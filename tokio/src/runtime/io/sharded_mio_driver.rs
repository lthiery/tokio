//! Shared handle and cross-worker coordination for the sharded-mio
//! reactor backend.
//!
//! ## Lazy first-poll registration
//!
//! Originally this backend mirrored the legacy mio driver: every
//! `Registration::new_with_interest` call eagerly allocated
//! a [`ScheduledIo`] from a global [`RegistrationSet`] and called
//! `mio::Registry::register` on a round-robin-picked worker. That work
//! ran on the producer thread (often a single accept worker) and
//! serialised through one global mutex.
//!
//! The current shape is lazy: `Registration::new_with_interest`
//! does no driver work — it stashes `(fd, interest)` only. The first
//! `poll_ready` / `try_io` / `readiness` call on the registration runs
//! on whichever worker happens to be polling and triggers
//! [`ShardedMioHandle::register_local`], which:
//!
//! 1. Allocates `Arc::new(ScheduledIo::default())`.
//! 2. Inserts it into the polling worker's per-shard
//!    [`RegistrationSet`] (no global mutex involved).
//! 3. Calls `mio::Registry::register` on the polling worker's own
//!    `SharedRegistry` — entirely thread-local, lock-free against
//!    sibling workers.
//!
//! Under the single-owner registration model, each fd is registered
//! on exactly one worker's epoll fd; events surface only to that
//! worker. `mio::Registry` clones are `Send + Sync`, so register and
//! deregister calls run synchronously on the calling thread (no
//! cross-thread queue), but they target the owner's child epoll
//! whoever the caller is. Cross-worker readiness propagation —
//! readiness stealing under contention, idle-worker wake on owner
//! burn — is layered on top in subsequent commits via a runtime-wide
//! meta-epoll and a try-lock peer drain. Until those land, an event
//! whose owner is mid-burn is not visible to a peer.
//!
//! When `register_local` is called from off-runtime (no worker idx in
//! TLS), it picks a fallback worker for slab ownership via
//! [`fallback_worker`].

use std::cell::{Cell, RefCell};
use std::io;
use std::os::fd::RawFd;
use std::ptr;
#[cfg(target_os = "linux")]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use crate::io::interest::Interest;
use crate::loom::sync::Mutex;
use crate::runtime::io::registration_set;
use crate::runtime::io::sharded_mio_reactor::{
    DeregisterOutcome, ExternalWaker, Reactor, SharedRegistry,
};
use crate::runtime::io::{IoDriverMetrics, RegistrationSet, ScheduledIo};

/// Park-state atomic values. Shape mirrors the uring handle's
/// transitions so scheduler integration stays familiar.
pub(crate) const EMPTY: usize = 0;
pub(crate) const PARKED: usize = 1;
pub(crate) const NOTIFIED: usize = 2;

/// RAII handle for the meta-watcher slot. Constructed by
/// [`ShardedMioHandle::try_acquire_meta_watcher`]; releases the slot
/// on drop so the next idle worker can take over.
///
/// Holds an `Arc<ShardedMioHandle>` rather than borrowing it so the
/// guard is 'static — required to pass it across `&mut self` method
/// boundaries inside the parker without tripping the borrow checker.
/// The clone is one atomic-inc, dwarfed by the syscall the guard
/// protects.
///
/// **Currently unused.** See [`ShardedMioHandle::meta_watcher_busy`]
/// for why the gate is preserved as dead code.
#[cfg(target_os = "linux")]
#[allow(dead_code)]
pub(crate) struct MetaWatcherGuard {
    handle: Arc<ShardedMioHandle>,
    released: bool,
}

#[cfg(target_os = "linux")]
impl MetaWatcherGuard {
    /// Release the gate explicitly. Subsequent `Drop` becomes a no-op.
    /// The intended use was to release the gate as soon as
    /// `epoll_wait` returns so peers could enter `epoll_wait`
    /// themselves while this thread runs the dispatch loop — the
    /// herd-protection invariant the gate exists for only matters for
    /// the *syscall blocking* phase. See
    /// [`ShardedMioHandle::meta_watcher_busy`] for the experiment
    /// write-up explaining why this is currently unused.
    #[allow(dead_code)]
    pub(crate) fn release(mut self) {
        self.do_release();
    }

    fn do_release(&mut self) {
        if !self.released {
            self.handle
                .meta_watcher_busy
                .store(false, Ordering::Release);
            self.released = true;
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for MetaWatcherGuard {
    fn drop(&mut self) {
        self.do_release();
    }
}

/// Per-worker coordination slot. One per worker, indexed by worker id.
pub(crate) struct WorkerState {
    /// `EMPTY | PARKED | NOTIFIED`. Written by the owning worker on
    /// park/resume; read/CAS'd by unparkers.
    pub(crate) park_state: AtomicUsize,

    /// Cross-thread handle to this worker's mio `Registry` + slab +
    /// `Waker`. Published once during worker startup via
    /// [`ShardedMioHandle::register_worker`].
    pub(crate) shared_registry: OnceLock<SharedRegistry>,

    /// Waker clone for the fast-path `unpark`. Published alongside
    /// `shared_registry`; kept as a separate cheap-to-clone handle so
    /// `unpark` doesn't have to take any slab locks.
    pub(crate) external_waker: OnceLock<ExternalWaker>,

    /// Per-shard registration set: tracks the live
    /// `Arc<ScheduledIo>`s whose lazy first-poll registration landed on
    /// this worker, so [`Drop`] on the handle can shut them down. Lives
    /// here (not on [`ShardedMioHandle`]) so per-worker registration
    /// state never contends a global mutex.
    pub(super) registrations: RegistrationSet,

    /// Synced state for [`Self::registrations`]. Mutated under the
    /// owning worker's slab insert/remove paths. Cross-thread register
    /// / deregister calls take it briefly via [`Mutex::lock`] before
    /// touching the slab.
    pub(super) synced: Mutex<registration_set::Synced>,
}

impl WorkerState {
    fn new() -> Self {
        let (registrations, synced) = RegistrationSet::new();
        Self {
            park_state: AtomicUsize::new(EMPTY),
            shared_registry: OnceLock::new(),
            external_waker: OnceLock::new(),
            registrations,
            synced: Mutex::new(synced),
        }
    }
}

impl std::fmt::Debug for WorkerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerState").finish_non_exhaustive()
    }
}

/// Shared I/O handle for the sharded-mio backend.
///
/// Symmetry with [`UringHandle`][uh]: per-worker slots for unparking
/// and per-worker [`RegistrationSet`]s. There is no global registration
/// set — each shard owns its own, eliminating a single-mutex
/// serialization point under TCP connect churn. There is also no
/// cross-thread `pending_ops` queue: register/deregister calls run
/// synchronously on the calling thread, targeting the owner worker's
/// child epoll directly via the cloneable `mio::Registry`.
///
/// On Linux the handle additionally owns a *meta epoll fd*: every
/// worker's child epoll is registered onto it as level-triggered
/// `EPOLLIN`. A worker that is idle but not its target's owner can
/// park on the meta-epoll instead of its own child epoll and pick
/// up "some sibling has events" wake notifications, then drain the
/// firing child via [`SharedRegistry`]'s try-lock peer path. The
/// child epolls themselves remain the single-owner registration
/// surface — only the *parking* layer is hierarchical.
///
/// [uh]: super::uring_driver::UringHandle
pub(crate) struct ShardedMioHandle {
    workers: Box<[WorkerState]>,
    next_worker: AtomicUsize,

    pub(crate) metrics: IoDriverMetrics,

    /// Per-runtime startup barrier. Every worker waits here after
    /// building its reactor and before entering the scheduler loop so
    /// task polling begins at the same wall-clock moment across
    /// workers. Mirrors the same rationale as
    /// [`UringHandle::start_barrier`][uhb].
    ///
    /// [uhb]: super::uring_driver::UringHandle
    start_barrier: std::sync::Barrier,

    /// Runtime-wide *meta epoll fd*. Each worker's child epoll is
    /// added here once it publishes its `SharedRegistry`, with the
    /// child's `worker_idx` carried in `epoll_event.data.u64`. Used
    /// by the upcoming steal-mode park path to wait for "some
    /// sibling has events" and resolve back to the firing worker.
    ///
    /// Owned by this handle: created in [`Self::new`], closed in
    /// [`Drop`]. Not exposed publicly outside the sharded-mio module.
    #[cfg(target_os = "linux")]
    meta_epfd: RawFd,

    /// Userspace gate: at most one worker at a time blocks on the meta
    /// epoll. Workers that lose the gate fall back to parking on their
    /// own child epoll (which, for an empty slab, just waits for their
    /// own external waker — exactly the behavior an idle peer wants).
    ///
    /// # Motivation
    ///
    /// Without the gate, all idle workers `epoll_wait` on the same meta
    /// fd. Every child readiness event wakes them all (no
    /// `EPOLLEXCLUSIVE`, see below), they race for the steal-drain
    /// `try_lock`, the loser pays cache-cold migration cost, and IPC
    /// drops. `perf stat` on `busy_owner_idle` showed:
    ///
    /// - `cpu-migrations` 1.67k → 4.68k (+180%) vs. traditional
    /// - `task-clock` sys time +21%, user time -22%
    /// - `L1-dcache-miss-rate` 1.94% → 2.82%
    /// - sharded_mio +17% slower than traditional on the bench
    ///
    /// `EPOLLEXCLUSIVE` would have given us "wake exactly one waiter
    /// per readiness event" at the kernel layer, but `epoll_ctl(2)`
    /// explicitly rejects it with EINVAL when the target fd is itself
    /// an epoll instance — which is exactly our meta-of-children
    /// shape. So the gate has to live in userspace.
    ///
    /// # Why this gate is *not* currently wired up
    ///
    /// Two variants were spiked and benched against the no-gate
    /// baseline at commit `0025ef76` ("re-enable owner-burns
    /// regression test"):
    ///
    /// | variant            | `busy_owner_idle` | `busy_owner_3burners` |
    /// | ------------------ | ----------------- | --------------------- |
    /// | no gate (baseline) | sharded +17%      | sharded **−54%**      |
    /// | strict gate        | tied              | sharded +33%          |
    /// | early-release gate | sharded +14%      | sharded −12%          |
    ///
    /// Both variants improve `busy_owner_idle` (the herd they were
    /// designed to suppress) but regress `busy_owner_3burners`. The
    /// reason is symmetric: the same herd that wastes wakes in the
    /// idle case is what *parallelizes work-stealing* in the busy
    /// case. With the gate, all dispatched tasks land on the watcher's
    /// run queue (because `try_steal_drain` schedules to the calling
    /// thread's local queue) and the other peers stay parked on their
    /// own child epfd waiting for the runtime's
    /// `notify_parked_local`/`remote` to fire their external waker.
    /// That detour adds latency relative to "every peer sees every
    /// event and grabs work directly".
    ///
    /// `tcp_echo_throughput` (sharded -11%) and `tcp_connect_churn`
    /// (sharded -4%) are unaffected by the gate either way, so the
    /// trade-off is a micro-bench question. We chose to ship the
    /// no-gate design and accept the +17% `busy_owner_idle` cost in
    /// exchange for the -54% `busy_owner_3burners` win and the gain on
    /// real-world TCP load.
    ///
    /// # Possible follow-up directions
    ///
    /// To re-enable a gate without losing the `3burners` win, the
    /// dispatch path would need to redistribute work — e.g. push
    /// woken tasks to the *owner's* remote queue (so the burning
    /// owner's queue grows and idle peers steal from it via the
    /// regular work-stealing path), or split meta watching off onto
    /// a dedicated steward thread that signals per-worker inboxes.
    /// Both are larger reworks than this single-flag spike and are
    /// deferred until the idle case proves to matter for a real
    /// workload.
    ///
    /// The field, the guard type, and `try_acquire_meta_watcher` are
    /// retained for the next attempt. They are unused at runtime —
    /// no caller ever invokes the acquire — so the gate is a no-op
    /// today.
    ///
    /// [`epoll_ctl(2)`]: https://man7.org/linux/man-pages/man2/epoll_ctl.2.html
    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    meta_watcher_busy: AtomicBool,
}

impl std::fmt::Debug for ShardedMioHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedMioHandle")
            .field("num_workers", &self.workers.len())
            .finish_non_exhaustive()
    }
}

#[cfg(target_os = "linux")]
impl Drop for ShardedMioHandle {
    fn drop(&mut self) {
        // Close the meta epoll fd. Children remain registered until
        // each worker's `SharedRegistry` (and the underlying child
        // epoll fd) is dropped a moment later as part of runtime
        // teardown — the kernel removes the meta-side records
        // automatically when the meta fd closes, so we don't need to
        // walk the workers and `EPOLL_CTL_DEL` first.
        if self.meta_epfd >= 0 {
            // SAFETY: `meta_epfd` was created by `epoll_create1` in
            // `Self::new` and has not been closed elsewhere — Drop
            // runs at most once.
            unsafe { libc::close(self.meta_epfd) };
        }
    }
}

impl ShardedMioHandle {
    pub(crate) fn new(num_workers: usize) -> Self {
        let mut workers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            workers.push(WorkerState::new());
        }
        let barrier_count = num_workers.max(1);

        // Create the runtime-wide meta epoll fd up front so it is
        // available for `register_worker` to add child epolls onto.
        // `EPOLL_CLOEXEC` keeps the fd from leaking across `exec`.
        #[cfg(target_os = "linux")]
        let meta_epfd = {
            let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
            if fd < 0 {
                let err = io::Error::last_os_error();
                panic!("sharded-mio: epoll_create1 for meta epoll failed: {err}");
            }
            fd
        };

        Self {
            workers: workers.into_boxed_slice(),
            next_worker: AtomicUsize::new(0),
            metrics: IoDriverMetrics::default(),
            start_barrier: std::sync::Barrier::new(barrier_count),
            #[cfg(target_os = "linux")]
            meta_epfd,
            #[cfg(target_os = "linux")]
            meta_watcher_busy: AtomicBool::new(false),
        }
    }

    /// Try to become the runtime-wide *meta watcher*: the single
    /// thread allowed to block in `epoll_wait` on the meta epoll fd
    /// at any given moment. Returns `Some(guard)` on success — drop
    /// the guard (or call `MetaWatcherGuard::release`) to release the
    /// watcher slot. Returns `None` when another worker already holds
    /// it; the caller should fall back to parking on its own child
    /// epoll.
    ///
    /// Takes `self: &Arc<Self>` so the returned guard can carry an
    /// `Arc<Self>` clone, decoupling its lifetime from the caller and
    /// allowing it to cross `&mut self` boundaries on the parker side.
    ///
    /// See [`Self::meta_watcher_busy`] for why this gate exists and
    /// why no caller currently invokes this method.
    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    pub(crate) fn try_acquire_meta_watcher(
        self: &Arc<Self>,
    ) -> Option<MetaWatcherGuard> {
        match self.meta_watcher_busy.compare_exchange(
            false,
            true,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => Some(MetaWatcherGuard {
                handle: Arc::clone(self),
                released: false,
            }),
            Err(_) => None,
        }
    }

    /// Raw meta-epoll fd. Stable for the lifetime of `self` and closed
    /// in [`Drop`]. Currently only the parker module needs this — for
    /// the upcoming steal-mode `epoll_wait` on the meta fd. Not
    /// exposed publicly outside the sharded-mio backend.
    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    pub(crate) fn meta_epfd(&self) -> RawFd {
        self.meta_epfd
    }

    /// Number of workers this handle serves.
    #[allow(dead_code)]
    pub(crate) fn num_workers(&self) -> usize {
        self.workers.len()
    }

    /// Per-worker state slice. Reserved for the upcoming meta-epoll
    /// integration: a parker idle on the meta-epoll will need to
    /// resolve a wake notification (which carries the firing child
    /// epoll's worker idx) back to that worker's `SharedRegistry` to
    /// run the steal-drain pass. Not part of the public
    /// `IoDriverHandle` surface — strictly internal to the
    /// sharded-mio backend.
    #[allow(dead_code)]
    pub(crate) fn workers(&self) -> &[WorkerState] {
        &self.workers
    }

    /// Block until every sibling worker has also reached this call.
    /// Called exactly once per worker at startup after the reactor is
    /// built and its `SharedRegistry` published.
    pub(crate) fn wait_for_start(&self) {
        self.start_barrier.wait();
    }

    /// Publish a worker's `SharedRegistry` and `ExternalWaker`. Called
    /// exactly once per worker during scheduler startup, on that
    /// worker's thread. After this returns, other threads can target
    /// the worker via `add_source` / `unpark`.
    ///
    /// On Linux this also registers the worker's child epoll fd onto
    /// the runtime-wide meta epoll as level-triggered `EPOLLIN`, with
    /// `worker_idx` stamped into `epoll_event.data.u64`. The level-
    /// triggered shape is deliberate: we want the meta `epoll_wait`
    /// to keep reporting a child as ready until that child has been
    /// drained, so a peer parker that wakes on the meta but loses the
    /// try-lock race can be re-woken on the next attempt without any
    /// intervening event.
    pub(crate) fn register_worker(
        &self,
        worker_idx: usize,
        shared_registry: SharedRegistry,
        external_waker: ExternalWaker,
    ) {
        let slot = &self.workers[worker_idx];

        // Register the child epoll onto the meta epoll *before*
        // publishing `shared_registry` so that any thread that
        // subsequently observes the published registry can also rely
        // on the child being visible to the meta. Failure here is a
        // hard error — the runtime cannot honor steal-mode park
        // without the registration in place.
        #[cfg(target_os = "linux")]
        {
            let child_epfd = shared_registry.epoll_fd();
            let mut ev = libc::epoll_event {
                events: libc::EPOLLIN as u32,
                u64: worker_idx as u64,
            };
            // SAFETY: `self.meta_epfd` is owned by this handle and
            // stays open until `Drop`; `child_epfd` is owned by the
            // soon-to-be-published `SharedRegistry`, which itself
            // outlives the handle (the worker's `Reactor` is dropped
            // last on shutdown via `ShardedMioParker::shutdown`).
            // `&mut ev` is a fresh stack value the kernel reads but
            // does not retain.
            let ret = unsafe {
                libc::epoll_ctl(
                    self.meta_epfd,
                    libc::EPOLL_CTL_ADD,
                    child_epfd,
                    &mut ev,
                )
            };
            if ret != 0 {
                let err = io::Error::last_os_error();
                panic!(
                    "sharded-mio: epoll_ctl(meta, ADD, child={child_epfd}, \
                     worker={worker_idx}) failed: {err}",
                );
            }
        }

        if slot.shared_registry.set(shared_registry).is_err() {
            debug_assert!(false, "worker {worker_idx} published shared_registry twice");
        }
        if slot.external_waker.set(external_waker).is_err() {
            debug_assert!(false, "worker {worker_idx} published external_waker twice");
        }
    }

    /// Mark `worker_idx` as notified and — if the worker was parked —
    /// deliver an actual wake via its `mio::Waker`.
    ///
    /// Returns `true` if a wake was delivered to the kernel (for
    /// metrics). Mirrors [`UringHandle::unpark`][uu] but with only one
    /// wake mechanism (no MSG_RING vs. eventfd split).
    ///
    /// [uu]: super::uring_driver::UringHandle::unpark
    pub(crate) fn unpark(&self, worker_idx: usize) -> bool {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.unpark_calls);
        let slot = &self.workers[worker_idx];
        let prev = slot.park_state.swap(NOTIFIED, Ordering::Release);
        if prev != PARKED {
            if prev == EMPTY {
                bump(&COUNTERS.unpark_was_empty);
            } else {
                // prev == NOTIFIED
                bump(&COUNTERS.unpark_was_notified);
            }
            return false;
        }
        bump(&COUNTERS.unpark_was_parked);
        if let Some(waker) = slot.external_waker.get() {
            let _ = waker.wake();
        }
        true
    }

    /// Called by the worker on entry to park. Returns `true` if a wake
    /// was already pending, in which case park should skip the syscall
    /// and return immediately.
    pub(crate) fn begin_park(&self, worker_idx: usize) -> bool {
        use super::lazy_debug::{bump, bump_per_worker, COUNTERS, PER_WORKER};
        bump(&COUNTERS.begin_park_calls);
        bump_per_worker(&PER_WORKER.begin_park_calls, worker_idx);
        let slot = &self.workers[worker_idx];
        match slot.park_state.compare_exchange(
            EMPTY,
            PARKED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                bump(&COUNTERS.begin_park_parked);
                false
            }
            Err(NOTIFIED) => {
                bump(&COUNTERS.begin_park_fastpath);
                slot.park_state.store(EMPTY, Ordering::Release);
                true
            }
            Err(state) => panic!("inconsistent park_state on begin_park: {state}"),
        }
    }

    /// Called by the worker on park completion. Clears any notification
    /// that was consumed by the wake.
    pub(crate) fn end_park(&self, worker_idx: usize) {
        let slot = &self.workers[worker_idx];
        slot.park_state.store(EMPTY, Ordering::Release);
    }

    /// Allocate a [`ScheduledIo`] for a new fd without doing any
    /// driver-side work yet. The returned `Arc<ScheduledIo>` is owned
    /// by the caller's [`Registration`][reg]; the driver only learns
    /// about it on the first
    /// [`register_local`](Self::register_local) call.
    ///
    /// [reg]: super::registration::Registration
    pub(crate) fn allocate_scheduled_io(&self) -> Arc<ScheduledIo> {
        Arc::new(ScheduledIo::default())
    }

    /// Lazy register `(shared, fd, interest)` with the sharded-mio
    /// reactor. Called from
    /// [`Registration::ensure_registered`][reg-er] on the first poll
    /// of a registration.
    ///
    /// Synchronous on any thread. `mio::Registry` clones are
    /// `Send + Sync`, so the calling thread can drive the owner-side
    /// mio register directly without bouncing through the owner's
    /// park loop. The single per-shard decision is which worker owns
    /// the slab entry:
    ///
    /// * **On a worker:** the calling worker is the owner, picked via
    ///   [`current_worker_index`][cwi]. This is the common path —
    ///   `from_std` / `TcpListener::accept` on a worker triggers first
    ///   poll on that same worker, which keeps cache-warm dispatch
    ///   on the same core.
    /// * **Off-runtime:** [`fallback_worker`] picks an owner via
    ///   round-robin.
    ///
    /// **Throughput note.** The fast path concentrates fds on
    /// whichever worker happens to call `from_std`. In
    /// `tcp_connect_churn` (single accept-loop worker, many
    /// short-lived connections) this regresses throughput because
    /// the accepting worker becomes a single dispatch hot spot.
    /// Distributed-load workloads — where each worker accepts/spawns
    /// its own connections — benefit. See `docs/io-driver-vtable.md`
    /// for the bench results and the artifact discussion.
    ///
    /// Returns the worker index this registration is now bound to —
    /// the same index that ends up stored in
    /// `shared.sharded_mio_worker`.
    ///
    /// [reg-er]: super::registration::Registration::ensure_registered
    /// [cwi]: crate::runtime::scheduler::multi_thread::sharded_mio_park::current_worker_index
    pub(crate) fn register_local(
        &self,
        shared: &Arc<ScheduledIo>,
        fd: RawFd,
        interest: Interest,
    ) -> io::Result<usize> {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.register_local_calls);

        // Sync fast path: when the calling thread is itself a
        // sharded-mio worker, apply the registration inline on its
        // own `SharedRegistry`. The lazy-from-anywhere panic boundary
        // (no runtime in TLS) was already cleared by
        // `Registration::ensure_registered` calling
        // `Handle::current()`, so reaching this point with a worker
        // index always means we're on a live sharded-mio worker
        // belonging to the same runtime as `self`.
        if let Some(worker_idx) =
            crate::runtime::scheduler::multi_thread::sharded_mio_park::current_worker_index()
        {
            if worker_idx < self.workers.len() {
                return self.register_on_worker(worker_idx, shared, fd, interest);
            }
        }

        // Off-runtime path: pick a fallback owner round-robin.
        self.register_on_worker(self.fallback_worker(), shared, fd, interest)
    }

    /// Same-worker sync register. Mutates the owning worker's
    /// `RegistrationSet` and `SharedRegistry` directly. Caller must
    /// have established that `worker_idx` is the index of the
    /// currently-executing worker.
    fn register_on_worker(
        &self,
        worker_idx: usize,
        shared: &Arc<ScheduledIo>,
        fd: RawFd,
        interest: Interest,
    ) -> io::Result<usize> {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.register_on_worker_calls);

        // Publish the worker assignment first so a racing
        // `queue_deregister` (e.g. from a foreign Drop that just
        // received a clone of `shared`) routes to this same worker's
        // slab. Both calls run synchronously on their respective
        // threads; the slot's mutex serialises the Register-vs-
        // Deregister order on the slab side.
        shared
            .sharded_mio_worker
            .store(worker_idx as u32, Ordering::Relaxed);

        let slot = &self.workers[worker_idx];

        // Track in the per-shard set first.
        if slot
            .registrations
            .allocate_existing(&mut slot.synced.lock(), shared)
            .is_err()
        {
            bump(&COUNTERS.register_on_worker_shutdown);
            // Driver shutting down — surface a shutdown event so the
            // caller's first-poll waiter doesn't hang.
            shared.shutdown();
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "io driver shutting down",
            ));
        }

        let registry = match slot.shared_registry.get() {
            Some(r) => r,
            None => {
                bump(&COUNTERS.register_on_worker_no_registry);
                // Worker hasn't published its registry yet (shouldn't
                // happen post-startup; the start barrier guarantees
                // every worker publishes before any task runs). Be
                // safe: roll back the set insert and report.
                // SAFETY: `shared` was just inserted by
                // `allocate_existing`.
                unsafe {
                    slot.registrations
                        .remove(&mut slot.synced.lock(), shared);
                }
                shared.shutdown();
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "sharded-mio worker registry not yet published",
                ));
            }
        };

        let mut source = mio::unix::SourceFd(&fd);
        let ok = match registry.register(&mut source, fd, interest, shared) {
            Ok(ok) => ok,
            Err(e) => {
                bump(&COUNTERS.register_on_worker_errors);
                // SAFETY: just inserted by `allocate_existing` above.
                unsafe {
                    slot.registrations
                        .remove(&mut slot.synced.lock(), shared);
                }
                shared.shutdown();
                return Err(e);
            }
        };

        bump(&COUNTERS.register_on_worker_ok);
        super::lazy_debug::bump_per_worker(
            &super::lazy_debug::PER_WORKER.register_per_worker,
            worker_idx,
        );
        shared
            .sharded_mio_slab_key
            .store(ok.slab_key, Ordering::Relaxed);
        self.metrics.incr_fd_count();
        Ok(worker_idx)
    }

    fn fallback_worker(&self) -> usize {
        self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len()
    }

    /// Synchronously deregister `shared` from the sharded-mio reactor.
    ///
    /// Despite the historical "queue_" name, this call applies the
    /// deregistration directly on the calling thread:
    /// `mio::Registry` clones are `Send + Sync`, so there is no need
    /// to bounce through the owning worker's park loop. The owner
    /// identity (loaded from `shared.sharded_mio_worker`) identifies
    /// which shard's slab the entry lives in and which worker's child
    /// epoll holds the kernel-side registration.
    ///
    /// `fd` must be the same fd that was passed to
    /// [`Self::register_local`]; the caller (typically
    /// `Registration::deregister`) has it via
    /// `RegistrationSource::registration_raw_fd()`.
    pub(crate) fn queue_deregister(&self, shared: &Arc<ScheduledIo>, fd: RawFd) {
        use super::lazy_debug::{bump, maybe_dump_trial, COUNTERS};
        bump(&COUNTERS.queue_deregister_calls);
        // Per-trial delta hook (no-op unless `TOKIO_LAZY_DEBUG_TRIAL`
        // is set). Placed here because `io_busy_owner` deregisters
        // exactly once per probe task at end-of-trial — modulo the
        // configured `N` this gives one delta dump per bench iter.
        bump(&COUNTERS.apply_deregister_calls);
        maybe_dump_trial();
        let worker_idx = shared.sharded_mio_worker.load(Ordering::Relaxed) as usize;
        if worker_idx >= self.workers.len() {
            bump(&COUNTERS.queue_deregister_no_worker);
            // Either never registered (unlikely — caller should have
            // checked) or the field is the sentinel u32::MAX. Nothing
            // to deregister.
            return;
        }
        let slot = &self.workers[worker_idx];
        let slab_key = shared.sharded_mio_slab_key.load(Ordering::Relaxed);
        let gen = shared.sharded_mio_gen.load(Ordering::Relaxed);

        if slab_key != u32::MAX {
            if let Some(registry) = slot.shared_registry.get() {
                let mut source = mio::unix::SourceFd(&fd);
                match registry.deregister(&mut source, fd, slab_key, gen) {
                    DeregisterOutcome::Applied => {
                        self.metrics.dec_fd_count();
                    }
                    DeregisterOutcome::SkippedFdReused
                    | DeregisterOutcome::SkippedSlotReassigned => {
                        // The kernel-side registration we'd otherwise
                        // wipe with `epoll_ctl_del(fd)` belongs to a
                        // fresher registration. Skip; the kernel
                        // already auto-removed our original record
                        // when our fd was closed.
                        bump(&COUNTERS.apply_deregister_gen_mismatch);
                    }
                }
            }
        } else {
            bump(&COUNTERS.apply_deregister_no_key);
        }

        // Mark the registration for release. The per-shard
        // pending_release vec is drained on the owning worker's next
        // park via the `RegistrationSet::release` path inside
        // `RegistrationSet::deregister` itself once the threshold is
        // hit. Best-effort flush here too if needed.
        let _ = slot
            .registrations
            .deregister(&mut slot.synced.lock(), shared);
        if slot.registrations.needs_release() {
            slot.registrations.release(&mut slot.synced.lock());
        }
    }
}

// ===== LOCAL_REACTOR thread-local =====
//
// For the sharded-mio path this TLS is wired for symmetry with the
// uring reactor — the probe `local_reactor_installed()` is useful if we
// ever want to branch "am I on a sharded-mio worker?" the way
// `TcpStream::uring_send` branches on the uring TLS. Today nothing
// actually needs the reactor pointer (cross-worker add_source uses the
// `SharedRegistry` handle directly, no LOCAL_REACTOR dereference
// required). We keep the install/clear pair anyway so future work that
// wants to reach the local reactor has a uniform hook.

thread_local! {
    static LOCAL_REACTOR: Cell<*const RefCell<Reactor>> =
        const { Cell::new(ptr::null()) };
}

/// Install `ptr` as this thread's local reactor. See
/// [`uring_driver::install_local_reactor_raw`] for the safety contract.
///
/// [`uring_driver::install_local_reactor_raw`]: super::uring_driver::install_local_reactor_raw
pub(crate) unsafe fn install_local_reactor_raw(ptr: *const RefCell<Reactor>) {
    LOCAL_REACTOR.with(|slot| {
        let existing = slot.get();
        if !existing.is_null() {
            let tname = std::thread::current()
                .name()
                .unwrap_or("<unnamed>")
                .to_owned();
            panic!(
                "another sharded-mio Reactor is already installed on thread {tname:?}: \
                 existing={existing:p} new={ptr:p}",
            );
        }
        slot.set(ptr);
    });
}

/// Clear this thread's `LOCAL_REACTOR` slot. Idempotent.
pub(crate) fn clear_local_reactor() {
    LOCAL_REACTOR.with(|slot| slot.set(ptr::null()));
}

/// Cheap probe: is a sharded-mio `Reactor` currently installed on this
/// thread?
#[allow(dead_code)]
pub(crate) fn local_reactor_installed() -> bool {
    LOCAL_REACTOR.with(|slot| !slot.get().is_null())
}
