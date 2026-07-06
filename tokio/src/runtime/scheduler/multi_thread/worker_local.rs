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

use crate::loom::sync::atomic::AtomicBool;
use crate::loom::sync::atomic::Ordering::{Acquire, Release};
use crate::loom::sync::{Arc, Mutex};
use crate::runtime::scheduler::inject;
use crate::runtime::scheduler::multi_thread::Handle;
use crate::runtime::task::{LocalOwnedTasks, Notified, Schedule, Task, TaskHarnessScheduleHooks};

use std::collections::VecDeque;

/// Error returned by the internal worker-local spawn path. The public API
/// converts each variant into a panic with a specific message.
#[derive(Debug)]
pub(crate) enum SpawnWorkerLocalError {
    /// The current thread is not a multi-threaded runtime worker thread.
    NotOnWorker,
    /// The current thread is a worker thread, but it does not hold its core
    /// (it is inside `block_in_place`).
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
        self.handle.shared.worker_locals[self.worker_index]
            .owned
            .remove(task)
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

/// Per-worker shared state for worker-local tasks. Stored in `Shared`,
/// indexed by worker.
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
    spawn_requests: Mutex<VecDeque<Box<dyn FnOnce() + Send>>>,

    /// Fast-path flag mirroring "`spawn_requests` is non-empty", letting the
    /// worker skip the lock on the common (empty) path. A racing enqueue
    /// that is missed here is corrected by the unpark that accompanies every
    /// `push_spawn_request`: the worker re-checks after unparking.
    has_spawn_requests: AtomicBool,
}

// Safety: `WorkerLocalShared` lives in `Shared`, which is referenced from all
// worker threads, so it must be `Send + Sync`:
//
//  * `inject_shared`/`inject_synced` are synchronized by `inject_synced`'s
//    mutex, and `spawn_requests` by its own mutex (its closures are `Send`).
//  * `owned` is `!Send + !Sync` and is only ever touched by the thread
//    currently holding this worker's core: `bind` requires the core to be in
//    the thread-local context (see `Context::spawn_worker_local`), polling
//    and `release` happen on the core holder by construction, and
//    `pre_shutdown` runs on the core holder. The core cannot move to another
//    thread while `owned` is non-empty because `block_in_place` panics in
//    that case, and an empty `LocalOwnedTasks` contains no `!Send` data.
unsafe impl Send for WorkerLocalShared {}
unsafe impl Sync for WorkerLocalShared {}

impl WorkerLocalShared {
    pub(super) fn new() -> Self {
        let (inject_shared, inject_synced) = inject::Shared::new();
        Self {
            owned: LocalOwnedTasks::new(),
            inject_shared,
            inject_synced: Mutex::new(inject_synced),
            spawn_requests: Mutex::new(VecDeque::new()),
            has_spawn_requests: AtomicBool::new(false),
        }
    }

    /// Returns `true` if there may be pending spawn requests, without
    /// locking.
    pub(super) fn has_spawn_requests(&self) -> bool {
        self.has_spawn_requests.load(Acquire)
    }

    /// Drains and executes all pending spawn requests.
    ///
    /// Must be called from the thread holding this worker's core, with the
    /// core stored in the thread-local context so the closures can spawn.
    pub(super) fn drain_spawn_requests(&self) {
        if !self.has_spawn_requests.load(Acquire) {
            return;
        }
        let requests: VecDeque<_> = {
            let mut guard = self.spawn_requests.lock();
            self.has_spawn_requests.store(false, Release);
            std::mem::take(&mut *guard)
        };
        for f in requests {
            f();
        }
    }

    /// Drops any pending spawn requests without running them. Used during
    /// shutdown.
    pub(super) fn clear_spawn_requests(&self) {
        let mut guard = self.spawn_requests.lock();
        self.has_spawn_requests.store(false, Release);
        guard.clear();
    }
}
