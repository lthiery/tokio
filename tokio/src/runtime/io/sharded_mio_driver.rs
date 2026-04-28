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
}

impl WorkerState {
    fn new() -> Self {
        Self {
            park_state: AtomicUsize::new(EMPTY),
            shared_registry: OnceLock::new(),
            external_waker: OnceLock::new(),
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

    /// Shared registration set (fd → ScheduledIo). Identical shape to
    /// the uring driver's `UringHandle::registrations` — the Arc-pinned
    /// `ScheduledIo` instances are also referenced from each
    /// per-worker reactor's slab while a registration is live.
    pub(super) registrations: RegistrationSet,
    pub(super) synced: Mutex<registration_set::Synced>,

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
        let (registrations, synced) = RegistrationSet::new();
        let barrier_count = num_workers.max(1);
        Self {
            workers: workers.into_boxed_slice(),
            next_worker: AtomicUsize::new(0),
            registrations,
            synced: Mutex::new(synced),
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
    /// Allocates a [`ScheduledIo`] from the shared registration set,
    /// picks a worker via round-robin, registers the source on that
    /// worker's `mio::Registry` in-place, and wakes the worker so its
    /// next `poll()` picks up the new registration.
    ///
    /// Round-robin placement matches uring's policy — fair ring load
    /// rather than task-affinity. See the uring design doc §5 for the
    /// rationale.
    pub(crate) fn add_source<S: RegistrationSource + ?Sized>(
        &self,
        source: &mut S,
        interest: Interest,
    ) -> io::Result<(Arc<ScheduledIo>, usize)> {
        let io = self.registrations.allocate(&mut self.synced.lock())?;

        let worker_idx = self.fallback_worker();

        // Publish assigned worker onto the ScheduledIo so `deregister`
        // can route to the same worker's registry without a reverse
        // lookup on the handle.
        io.sharded_mio_worker
            .store(worker_idx as u32, Ordering::Relaxed);

        let slot = &self.workers[worker_idx];
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
                    self.registrations
                        .remove(&mut self.synced.lock(), &io);
                }
                return Err(e);
            }
        };
        io.sharded_mio_slab_key
            .store(slab_key, Ordering::Relaxed);

        // No unpark on register: `Registry::register` (epoll_ctl_add
        // under the hood) is immediately effective on a concurrent
        // `epoll_wait`. The kernel updates the interest set live, so
        // the target shard's worker picks the new fd up on its
        // current or next poll without needing a wakeup. Waking
        // unconditionally was pure overhead on registration-heavy
        // workloads (TCP connect churn).

        self.metrics.incr_fd_count();
        Ok((io, worker_idx))
    }

    fn fallback_worker(&self) -> usize {
        self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len()
    }

    /// Deregister `source` from the worker it was registered with.
    pub(crate) fn deregister_source<S: RegistrationSource + ?Sized>(
        &self,
        io: &Arc<ScheduledIo>,
        source: &mut S,
    ) -> io::Result<()> {
        let worker_idx = io.sharded_mio_worker.load(Ordering::Relaxed) as usize;
        let slab_key = io.sharded_mio_slab_key.load(Ordering::Relaxed);

        let deregister_result = if worker_idx < self.workers.len() {
            let slot = &self.workers[worker_idx];
            if let Some(registry) = slot.shared_registry.get() {
                registry.deregister::<S>(source, slab_key)
            } else {
                Ok(())
            }
        } else {
            Ok(())
        };

        // We intentionally do NOT unpark the shard's worker even if
        // `RegistrationSet::deregister` reports pending releases.
        // Pending `ScheduledIo` releases are not latency-critical —
        // they are a few bytes of slab memory awaiting drain. The
        // owning worker drains them naturally at its next park
        // (see `release_pending_registrations`). Forcing a wake to
        // free slab memory faster trades a real syscall for a
        // microscopic memory-occupancy improvement, which is
        // exactly the wrong tradeoff under TCP connect churn.
        let _ = self.registrations.deregister(&mut self.synced.lock(), io);

        self.metrics.dec_fd_count();
        deregister_result
    }

    /// Release any `ScheduledIo`s queued for removal by
    /// [`RegistrationSet::deregister`]. Called by the owning worker
    /// after park.
    pub(crate) fn release_pending_registrations(&self) {
        if self.registrations.needs_release() {
            self.registrations.release(&mut self.synced.lock());
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
