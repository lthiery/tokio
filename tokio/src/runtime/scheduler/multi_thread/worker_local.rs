//! Worker-local task support for the multi-threaded scheduler.
//!
//! Worker-local tasks are `!Send` tasks bound to a single worker. They are
//! stored and polled only by the thread currently driving that worker, and
//! they never migrate: the `block_in_place` gate in `worker.rs` refuses to
//! hand a worker's core to another thread while any worker-local tasks are
//! alive on it, so "the thread driving this worker" cannot change while a
//! worker-local task exists.
//!
//! This enables use cases like io-uring where each worker thread owns a
//! reactor that must be polled from the same thread.

use crate::loom::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use crate::loom::sync::atomic::{fence, AtomicBool};
use crate::loom::sync::{Arc, Mutex};
use crate::runtime::scheduler::inject;
use crate::runtime::scheduler::multi_thread::Handle;
use crate::runtime::task::{
    LocalOwnedTasks, Notified, RawTask, Schedule, Task, TaskHarnessScheduleHooks,
};
use crate::util::metric_atomics::{MetricAtomicU64, MetricAtomicUsize};

use std::collections::VecDeque;
use std::panic::{self, AssertUnwindSafe};

/// Error returned by the internal worker-local spawn path. The public API
/// converts each variant into a panic with a specific message.
#[derive(Debug)]
pub(crate) enum SpawnWorkerLocalError {
    /// The current thread is not a multi-threaded runtime worker thread (or
    /// another runtime's handle has been entered on this thread).
    NotOnWorker,
    /// The current thread is a worker thread, but it does not currently hold
    /// its core (for example, inside `block_in_place`, a park callback, or
    /// shutdown).
    NoCore,
}

/// Scheduler for worker-local tasks.
///
/// Unlike `Arc<Handle>`, which routes woken tasks to any available worker,
/// this scheduler always routes tasks back to the worker they are bound to.
#[derive(Clone)]
pub(crate) struct WorkerLocalScheduler {
    /// Reference to the multi-thread scheduler handle.
    pub(super) handle: Arc<Handle>,

    /// The worker index this task is bound to.
    pub(super) worker_index: usize,
}

impl WorkerLocalScheduler {
    pub(super) fn new(handle: Arc<Handle>, worker_index: usize) -> Self {
        Self {
            handle,
            worker_index,
        }
    }
}

impl Schedule for WorkerLocalScheduler {
    fn release(&self, task: &Task<Self>) -> Option<Task<Self>> {
        // `release` runs either when the task completes (on the thread
        // polling it) or during `close_and_shutdown_all` in `pre_shutdown`.
        // Both run on the thread holding this worker's core, which is the
        // only thread permitted to touch `owned`.
        let worker_local = &self.handle.shared.worker_locals[self.worker_index];
        let released = worker_local.owned.remove(task);
        if released.is_some() {
            worker_local.dec_num_alive();
        }
        released
    }

    fn schedule(&self, task: Notified<Self>) {
        self.handle
            .schedule_worker_local_task(self.worker_index, task);
    }

    fn hooks(&self) -> TaskHarnessScheduleHooks {
        TaskHarnessScheduleHooks {
            task_terminate_callback: self.handle.task_hooks.task_terminate_callback.clone(),
        }
    }
}

/// Single-threaded intrusive FIFO queue of notified worker-local tasks,
/// reusing each task's `Header::queue_next` pointer (exclusively owned by
/// `Notified`). Avoids heap allocations and allows O(1) splicing from the
/// per-worker inject queue.
pub(super) struct LocalQueue {
    head: Option<RawTask>,
    tail: Option<RawTask>,
    len: usize,
}

// Safety: `LocalQueue` only holds `Notified<WorkerLocalScheduler>` raw task
// pointers (which are `Send` because `Notified` cannot poll or touch `!Send`
// task state without `LocalOwnedTasks::assert_owner`).
unsafe impl Send for LocalQueue {}

impl LocalQueue {
    pub(super) fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(super) fn push_back(&mut self, task: Notified<WorkerLocalScheduler>) {
        let task = task.into_raw();
        debug_assert!(unsafe { task.get_queue_next().is_none() });

        if let Some(tail) = self.tail {
            // safety: Holding the `Notified` guarantees exclusive access to
            // `queue_next`.
            unsafe { tail.set_queue_next(Some(task)) };
        } else {
            self.head = Some(task);
        }

        self.tail = Some(task);
        self.len += 1;
    }

    pub(super) fn pop_front(&mut self) -> Option<Notified<WorkerLocalScheduler>> {
        let task = self.head?;

        // safety: `task` came from a `Notified` pushed into this queue.
        self.head = unsafe { task.get_queue_next() };
        if self.head.is_none() {
            self.tail = None;
        }
        unsafe { task.set_queue_next(None) };
        self.len -= 1;

        // safety: `task` was pushed as a `Notified<WorkerLocalScheduler>`.
        Some(unsafe { Notified::from_raw(task) })
    }

    /// Appends a well-formed intrusive chain drained from `inject::Shared::drain_all`
    /// in O(1).
    ///
    /// # Safety
    ///
    /// `(head, tail, len)` must be a valid chain of `Notified<WorkerLocalScheduler>`
    /// tasks with `tail.queue_next == None`.
    pub(super) unsafe fn splice_from(
        &mut self,
        head: Option<RawTask>,
        tail: Option<RawTask>,
        len: usize,
    ) {
        if len == 0 {
            return;
        }
        debug_assert!(head.is_some() && tail.is_some());

        if let Some(cur_tail) = self.tail {
            unsafe { cur_tail.set_queue_next(head) };
        } else {
            self.head = head;
        }
        self.tail = tail;
        self.len += len;
    }

    pub(super) fn clear(&mut self) {
        while self.pop_front().is_some() {}
    }
}

impl Drop for LocalQueue {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Per-worker shared state for worker-local tasks. Stored in `Shared`,
/// indexed by worker (cache-padded to prevent false sharing between workers).
pub(crate) struct WorkerLocalShared {
    /// Task storage for this worker's worker-local (`!Send`) tasks.
    ///
    /// `LocalOwnedTasks` is `!Send + !Sync`: every method touches an inner
    /// `UnsafeCell` without synchronization. It must only be accessed by the
    /// thread currently holding this worker's core (see the `Send`/`Sync`
    /// impls below).
    pub(super) owned: LocalOwnedTasks<WorkerLocalScheduler>,

    /// Inject queue for worker-local tasks woken from other threads. Unlike
    /// the scheduler-wide inject queue, this queue has a single consumer:
    /// the thread holding this worker's core.
    pub(super) inject_shared: inject::Shared<WorkerLocalScheduler>,
    pub(super) inject_synced: Mutex<inject::Synced>,

    /// Closures waiting to run on this worker's thread. This is how
    /// worker-local tasks are created from other threads: the closure runs
    /// on the target worker and spawns from there, so that `owned.bind` is
    /// only ever called by the core holder.
    spawn_requests: Mutex<SpawnRequests>,

    /// Fast-path flag mirroring "`spawn_requests` is non-empty", letting the
    /// worker skip the lock on the common (empty) path. A racing enqueue
    /// that is missed here is corrected by the park/unpark protocol below.
    has_spawn_requests: AtomicBool,

    /// Set to `true` by the worker while parked (before its final check for
    /// worker-local work, separated by a `SeqCst` fence) and cleared on exit
    /// from `park`. Cross-thread pushers check this after a `SeqCst` fence
    /// so active workers are not spuriously unparked.
    is_parked: AtomicBool,

    /// Number of alive worker-local tasks bound to this worker, mirroring
    /// `owned` for `RuntimeMetrics::num_alive_tasks`. Written only by the
    /// thread holding this worker's core (bind, release, shutdown); read by
    /// metrics from any thread.
    num_alive: MetricAtomicUsize,

    /// Total number of worker-local tasks spawned on this worker, mirroring
    /// binds for `RuntimeMetrics::spawned_tasks_count`. Single writer (core
    /// holder); read by metrics from any thread.
    spawned_count: MetricAtomicU64,

    /// Length of this worker's `Core::worker_local_queue`, mirrored here for
    /// `RuntimeMetrics::worker_local_queue_depth`. Written only by the thread
    /// holding this worker's core (plain stores of the queue length after
    /// each mutation); read by metrics from any thread.
    queue_depth: MetricAtomicUsize,
}

/// The spawn-request queue and its closed bit, guarded by one mutex so
/// closing and rejecting racing pushes are atomic.
struct SpawnRequests {
    queue: VecDeque<Box<dyn FnOnce() + Send>>,

    /// Set at shutdown. Once closed, pushes are rejected and the pusher
    /// drops the closure. Without this, a request enqueued after the
    /// worker's shutdown drain would sit in the queue forever; a closure
    /// capturing a runtime handle would then form a reference cycle
    /// (`Shared` owns the closure, the closure keeps `Shared` alive) and
    /// leak the runtime.
    is_closed: bool,
}

// Safety: `WorkerLocalShared` lives in `Shared`, which is referenced from all
// worker threads, so it must be `Send + Sync`:
//
//  * `inject_shared`/`inject_synced` are synchronized by `inject_synced`'s
//    mutex, and `spawn_requests` by its own mutex (its closures are `Send`).
//  * `has_spawn_requests`, `is_parked`, `num_alive`, `spawned_count`, and
//    `queue_depth` are atomics.
//  * `owned` is `!Send + !Sync` and is only ever touched by the thread
//    currently holding this worker's core: `bind` requires the core to be in
//    the thread-local context (see `Context::spawn_worker_local`), polling
//    and `release` happen on the core holder by construction, and
//    `pre_shutdown` runs on the core holder. The core cannot move to another
//    thread while `owned` is non-empty because `block_in_place` panics in
//    that case, and an empty `LocalOwnedTasks` contains no `!Send` data.
//    One exception: the `debug_assert` in `Handle::shutdown_core` reads
//    `owned.is_empty()` from the thread that pushes the last core, which is
//    sound because each worker empties its `owned` in `pre_shutdown` before
//    pushing its core to `shutdown_cores`: the `shutdown_cores` mutex orders
//    every worker's push (and thus its `pre_shutdown`) before the final
//    pusher's read.
unsafe impl Send for WorkerLocalShared {}
unsafe impl Sync for WorkerLocalShared {}

impl WorkerLocalShared {
    pub(super) fn new() -> Self {
        let (inject_shared, inject_synced) = inject::Shared::new();
        Self {
            owned: LocalOwnedTasks::new(),
            inject_shared,
            inject_synced: Mutex::new(inject_synced),
            spawn_requests: Mutex::new(SpawnRequests {
                queue: VecDeque::new(),
                is_closed: false,
            }),
            has_spawn_requests: AtomicBool::new(false),
            is_parked: AtomicBool::new(false),
            num_alive: MetricAtomicUsize::new(0),
            spawned_count: MetricAtomicU64::new(0),
            queue_depth: MetricAtomicUsize::new(0),
        }
    }

    /// Number of alive worker-local tasks bound to this worker.
    pub(super) fn num_alive_tasks(&self) -> usize {
        self.num_alive.load(Relaxed)
    }

    /// Total number of worker-local tasks spawned on this worker.
    #[cfg(target_has_atomic = "64")]
    pub(super) fn spawned_tasks_count(&self) -> u64 {
        self.spawned_count.load(Relaxed)
    }

    /// Number of runnable worker-local tasks queued for this worker, in the
    /// core's run queue or the worker-local inject queue. Pending spawn
    /// requests are not counted: they are closures, not tasks, until they
    /// run.
    pub(super) fn queued_tasks(&self) -> usize {
        self.queue_depth.load(Relaxed) + self.inject_shared.len()
    }

    /// Records the current length of the core's worker-local run queue. Must
    /// be called from the thread holding this worker's core after each queue
    /// mutation.
    pub(super) fn set_queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth, Relaxed);
    }

    /// Records that a worker-local task was bound to this worker.
    ///
    /// Single writer (core holder): uses a plain relaxed load + store rather
    /// than a locked RMW.
    pub(super) fn record_spawn(&self) {
        let alive = self.num_alive.load(Relaxed);
        self.num_alive.store(alive + 1, Relaxed);

        #[cfg(target_has_atomic = "64")]
        {
            let spawned = self.spawned_count.load(Relaxed);
            self.spawned_count.store(spawned + 1, Relaxed);
        }
    }

    /// Records that a worker-local task was removed from `owned`.
    ///
    /// Single writer (core holder): uses a plain relaxed load + store rather
    /// than a locked RMW.
    pub(super) fn dec_num_alive(&self) {
        let alive = self.num_alive.load(Relaxed);
        self.num_alive.store(alive - 1, Relaxed);
    }

    /// Resets the alive count after `owned.close_and_shutdown_all` has shut
    /// down the remaining tasks: shutdown unlinks them without going through
    /// `remove`, so `dec_num_alive` never runs for them.
    pub(super) fn clear_num_alive(&self) {
        self.num_alive.store(0, Relaxed);
    }

    /// Marks whether this worker is currently in the parked state.
    pub(super) fn set_parked(&self, parked: bool) {
        self.is_parked.store(parked, Release);
    }

    /// Returns `true` if a cross-thread pusher must unpark this worker.
    ///
    /// The `SeqCst` fence pairs with the `SeqCst` fence in
    /// `Core::transition_to_parked` between `set_parked(true)` and
    /// `has_worker_local_work()`: either the pusher observes `is_parked == true`
    /// and unparks the worker, or the worker observes the pushed work before
    /// sleeping.
    pub(super) fn should_unpark(&self) -> bool {
        fence(SeqCst);
        self.is_parked.load(Relaxed)
    }

    /// Records a pending spawn request and returns `true`; the caller is
    /// responsible for unparking the target worker if `should_unpark()` is
    /// `true`.
    ///
    /// Returns `false` if the queue is closed (the runtime is shutting
    /// down); the request is dropped without running.
    pub(super) fn push_spawn_request(&self, f: Box<dyn FnOnce() + Send>) -> bool {
        let mut guard = self.spawn_requests.lock();
        if guard.is_closed {
            // Drop the closure outside the lock: its destructor may itself
            // call `run_on_worker`.
            drop(guard);
            drop(f);
            return false;
        }
        guard.queue.push_back(f);
        // Set the flag while holding the lock so the flag and the queue's
        // empty/non-empty state cannot disagree in a way that loses a
        // request.
        self.has_spawn_requests.store(true, Release);
        true
    }

    /// Returns `true` if there may be pending spawn requests, without
    /// locking.
    pub(super) fn has_spawn_requests(&self) -> bool {
        self.has_spawn_requests.load(Acquire)
    }

    /// Drains pending spawn requests into `buf` and returns them without
    /// dropping the underlying buffer capacity.
    ///
    /// Must be called from the thread holding this worker's core.
    pub(super) fn take_spawn_requests(&self, buf: &mut VecDeque<Box<dyn FnOnce() + Send>>) {
        debug_assert!(buf.is_empty());
        if !self.has_spawn_requests.load(Acquire) {
            return;
        }
        let mut guard = self.spawn_requests.lock();
        self.has_spawn_requests.store(false, Release);
        std::mem::swap(&mut guard.queue, buf);
    }

    /// Closes the queue and drops any pending spawn requests without
    /// running them. Used during shutdown.
    ///
    /// Closing and draining are atomic (one lock acquisition), so a racing
    /// push lands either before this drain or after the close, where the
    /// pusher drops it. Nothing can be left in the queue afterwards.
    ///
    /// Each closure's destructor is wrapped in `catch_unwind` so a panicking
    /// drop on shutdown cannot unwind out of `pre_shutdown` and leak the
    /// remaining tasks or wedge runtime shutdown.
    pub(super) fn close_spawn_requests(&self) {
        let requests = {
            let mut guard = self.spawn_requests.lock();
            guard.is_closed = true;
            self.has_spawn_requests.store(false, Release);
            std::mem::take(&mut guard.queue)
        };
        // Dropped outside the lock: a closure's destructor may itself call
        // `run_on_worker`.
        for f in requests {
            let _ = panic::catch_unwind(AssertUnwindSafe(move || drop(f)));
        }
    }
}
