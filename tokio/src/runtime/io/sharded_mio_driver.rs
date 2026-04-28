//! Shared handle and cross-worker coordination for the sharded-mio
//! reactor backend.
//!
//! Mirror of [`uring_driver`] but dramatically simpler. Where the uring
//! path needs a `PendingOp` queue (because `io_uring` SQEs must be
//! submitted on the ring's owning thread), mio lets any thread mutate
//! the registry directly. So `add_source` / `deregister_source` just
//! register/deregister on the target worker's [`SharedRegistry`] in
//! place, then wake the worker via its [`mio::Waker`] so the next
//! `poll()` call sees the new registration.
//!
//! [`uring_driver`]: super::uring_driver

use std::cell::{Cell, RefCell};
use std::io;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use crate::io::interest::Interest;
use crate::loom::sync::Mutex;
use crate::runtime::io::registration::RegistrationSource;
use crate::runtime::io::registration_set;
use crate::runtime::io::sharded_mio_reactor::{ExternalWaker, Reactor, SharedRegistry};
use crate::runtime::io::{IoDriverMetrics, RegistrationSet, ScheduledIo};

/// Park-state atomic values. Shape mirrors the uring handle's
/// transitions so scheduler integration stays familiar.
pub(crate) const EMPTY: usize = 0;
pub(crate) const PARKED: usize = 1;
pub(crate) const NOTIFIED: usize = 2;

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

    /// Per-shard registration set: linked list of `Arc<ScheduledIo>`
    /// owned by this shard, plus its pending-release vec. Pulled out
    /// of the global handle so `add_source`/`deregister_source` only
    /// contend with operations targeting *this* shard, instead of
    /// every shard sharing one global mutex. The mutex is retained
    /// (rather than dropping to a thread-local cell) because it is
    /// the synchronization point that enables future readiness
    /// stealing — a foreign worker can `try_lock` a victim shard's
    /// `synced` to drain ready entries without contending with the
    /// owner most of the time.
    pub(super) registrations: RegistrationSet,
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
/// Symmetry with [`UringHandle`][uh]: per-worker slots for unparking +
/// a shared registration set. The key structural simplification vs.
/// uring: no `pending_ops` queue (direct cross-thread registration via
/// `mio::Registry`), no round-trip wake choreography (single `Waker`
/// per worker handles both external and peer paths).
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
}

impl std::fmt::Debug for ShardedMioHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedMioHandle")
            .field("num_workers", &self.workers.len())
            .finish_non_exhaustive()
    }
}

impl ShardedMioHandle {
    pub(crate) fn new(num_workers: usize) -> Self {
        let mut workers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            workers.push(WorkerState::new());
        }
        let barrier_count = num_workers.max(1);
        Self {
            workers: workers.into_boxed_slice(),
            next_worker: AtomicUsize::new(0),
            metrics: IoDriverMetrics::default(),
            start_barrier: std::sync::Barrier::new(barrier_count),
        }
    }

    /// Number of workers this handle serves.
    #[allow(dead_code)]
    pub(crate) fn num_workers(&self) -> usize {
        self.workers.len()
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
    pub(crate) fn register_worker(
        &self,
        worker_idx: usize,
        shared_registry: SharedRegistry,
        external_waker: ExternalWaker,
    ) {
        let slot = &self.workers[worker_idx];
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
        let slot = &self.workers[worker_idx];
        let prev = slot.park_state.swap(NOTIFIED, Ordering::Release);
        if prev != PARKED {
            return false;
        }
        if let Some(waker) = slot.external_waker.get() {
            let _ = waker.wake();
        }
        true
    }

    /// Called by the worker on entry to park. Returns `true` if a wake
    /// was already pending, in which case park should skip the syscall
    /// and return immediately.
    pub(crate) fn begin_park(&self, worker_idx: usize) -> bool {
        let slot = &self.workers[worker_idx];
        match slot.park_state.compare_exchange(
            EMPTY,
            PARKED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => false,
            Err(NOTIFIED) => {
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

    /// Register `source` for readiness notifications.
    ///
    /// Picks a worker via [`Self::placement_worker`] (caller-local
    /// affinity when invoked from a worker thread, round-robin
    /// otherwise), allocates a [`ScheduledIo`] from *that worker's*
    /// per-shard registration set, then registers the source on the
    /// same shard's `mio::Registry` in-place. No wakeup:
    /// `Registry::register` is `epoll_ctl_add` under the hood and is
    /// immediately effective on a concurrent `epoll_wait`.
    ///
    /// Affinity placement matters for the per-shard registration
    /// set: when a worker thread registers an fd on its own shard,
    /// the per-shard mutex is acquired by the same core that owns
    /// the cache line, so it's both uncontended and cache-hot. With
    /// pure round-robin, every register pulled a different shard's
    /// mutex cache line, defeating the locality the per-shard split
    /// is supposed to provide.
    pub(crate) fn add_source<S: RegistrationSource + ?Sized>(
        &self,
        source: &mut S,
        interest: Interest,
    ) -> io::Result<(Arc<ScheduledIo>, usize)> {
        let worker_idx = self.placement_worker();
        let slot = &self.workers[worker_idx];

        // Per-shard mutex: only contended with operations targeting
        // this same shard, not all sharded-mio traffic.
        let io = slot.registrations.allocate(&mut slot.synced.lock())?;

        // Publish assigned worker onto the ScheduledIo so `deregister`
        // can route to the same worker's registry + registration set
        // without a reverse lookup on the handle.
        io.sharded_mio_worker
            .store(worker_idx as u32, Ordering::Relaxed);

        let registry = slot
            .shared_registry
            .get()
            .expect("worker registry published before add_source");
        let slab_key = match registry.register(source, interest, &io) {
            Ok(k) => k,
            Err(e) => {
                // Roll back the RegistrationSet allocation so the
                // ScheduledIo isn't leaked when mio registration
                // fails. Matches the mio driver's cleanup path.
                unsafe {
                    slot.registrations
                        .remove(&mut slot.synced.lock(), &io);
                }
                return Err(e);
            }
        };
        io.sharded_mio_slab_key
            .store(slab_key, Ordering::Relaxed);

        self.metrics.incr_fd_count();
        Ok((io, worker_idx))
    }

    /// Choose which shard to place a new registration on.
    ///
    /// - When called from a worker thread, return that worker's
    ///   shard index. The new registration's per-shard mutex is then
    ///   acquired on the same core that owns its cache line, so it
    ///   is both uncontended and L1-resident. This is the common
    ///   case and the reason the registration set is per-shard.
    /// - When called from a non-worker thread (`block_on` caller,
    ///   `spawn` from main, an external thread driving the runtime
    ///   via `Handle`), fall back to round-robin so registrations
    ///   are spread across shards rather than piling on shard 0.
    fn placement_worker(&self) -> usize {
        if let Some(idx) = crate::runtime::scheduler::multi_thread::sharded_mio_park::current_worker_index() {
            if idx < self.workers.len() {
                return idx;
            }
        }
        self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len()
    }

    /// Deregister `source` from the worker it was registered with.
    ///
    /// Reads the owning shard out of the `ScheduledIo` and operates
    /// only on that shard's registry + registration set. The mio
    /// deregistration is a single `epoll_ctl_del` and does not need
    /// to wake the owning worker; the pending-release vec is drained
    /// by the owning worker at its next park.
    pub(crate) fn deregister_source<S: RegistrationSource + ?Sized>(
        &self,
        io: &Arc<ScheduledIo>,
        source: &mut S,
    ) -> io::Result<()> {
        let worker_idx = io.sharded_mio_worker.load(Ordering::Relaxed) as usize;
        let slab_key = io.sharded_mio_slab_key.load(Ordering::Relaxed);

        // Out-of-range worker_idx means the registration was never
        // fully published (e.g. add_source failed on the registry
        // step before storing the worker idx). Nothing to deregister.
        if worker_idx >= self.workers.len() {
            return Ok(());
        }

        let slot = &self.workers[worker_idx];

        let deregister_result = if let Some(registry) = slot.shared_registry.get() {
            registry.deregister::<S>(source, slab_key)
        } else {
            Ok(())
        };

        // Push onto the owning shard's pending-release vec. Drained
        // by that shard's worker at its next park; we deliberately
        // do not wake the worker just to free slab memory faster.
        let _ = slot.registrations.deregister(&mut slot.synced.lock(), io);

        self.metrics.dec_fd_count();
        deregister_result
    }

    /// Release any `ScheduledIo`s queued for removal on the given
    /// shard. Called by the owning worker after park; each worker
    /// drains only its own shard's pending-release vec.
    pub(crate) fn release_pending_registrations(&self, worker_idx: usize) {
        let slot = &self.workers[worker_idx];
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
