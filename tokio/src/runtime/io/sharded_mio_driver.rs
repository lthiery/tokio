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
//! 4. Fans the registration out to every peer worker's epoll fd via
//!    raw `epoll_ctl(EPOLL_CTL_ADD, EPOLLEXCLUSIVE | EPOLLET)` so any
//!    idle worker's `epoll_wait` can pick the event up.
//!
//! Under the EPOLLEXCLUSIVE-fanout model, every register/deregister
//! call is **synchronous on the calling thread**, regardless of which
//! thread is making the call. `mio::Registry` clones are
//! `Send + Sync`, so the calling thread can issue `epoll_ctl` against
//! any worker's epoll fd directly. The "owning worker" concept
//! survives only on the slab side — the worker that allocates the
//! `ScheduledIo` slot is also the one that frees it on deregister —
//! while every dispatch is uniform: `Reactor::poll_and_dispatch`
//! routes each event by the token's `worker_idx` field to the slab on
//! that worker's `SharedRegistry`. There is no cross-thread `pending_ops`
//! queue, no park-time drain, and no readiness stealing.
//!
//! When `register_local` is called from off-runtime (no worker idx in
//! TLS), it picks a fallback worker for slab ownership via
//! [`fallback_worker`], then runs the same synchronous fanout-register
//! path as the on-worker fast path.

use std::cell::{Cell, RefCell};
use std::io;
use std::os::fd::RawFd;
use std::ptr;
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

    /// Synced state for [`Self::registrations`]. Mutated only from
    /// the owning worker's slab insert/remove paths; the underlying
    /// fanout register/deregister calls reach peer workers' epolls
    /// directly via cloned `mio::Registry` handles, so this lock is
    /// only ever taken under the owner's control.
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
/// cross-thread `pending_ops` queue: every register/deregister call
/// runs synchronously on the calling thread under the
/// EPOLLEXCLUSIVE-fanout model.
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

    /// Per-worker state slice. Exposed for `Reactor::poll_and_dispatch`
    /// (step 3 of the EPOLLEXCLUSIVE-fanout rollout) to look up the
    /// owning worker's `SharedRegistry` from the unpacked token's
    /// `worker_idx` field. Not part of the public `IoDriverHandle`
    /// surface — strictly internal to the sharded-mio backend.
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
    /// Synchronous on any thread. Under the EPOLLEXCLUSIVE-fanout
    /// model every register call must hit every worker's epoll fd, so
    /// there is no value in queuing the off-worker case onto a target
    /// worker — `mio::Registry` clones are `Send + Sync`, so the
    /// calling thread can drive both the owner-side mio register and
    /// the peer-side raw `epoll_ctl` calls directly. The only
    /// per-shard decision is which worker owns the slab entry:
    ///
    /// * **On a worker:** the calling worker is the owner, picked via
    ///   [`current_worker_index`][cwi]. This is the common path —
    ///   `from_std` / `TcpListener::accept` on a worker triggers first
    ///   poll on that same worker.
    /// * **Off-runtime:** [`fallback_worker`] picks an owner via
    ///   round-robin. Same fanout work as the on-worker path; only
    ///   the slab-owner identity differs.
    ///
    /// **Throughput note.** The synchronous fast path concentrates
    /// fds on whichever worker happens to call `from_std`. In
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

        // Off-runtime path: pick a fallback owner round-robin. The
        // fanout fan-out portion runs synchronously on the calling
        // thread (no cross-thread queue) just like the on-worker case.
        self.register_on_worker(self.fallback_worker(), shared, fd, interest)
    }

    /// Fan out a freshly-registered fd to every peer worker's epoll
    /// fd via raw `libc::epoll_ctl(EPOLL_CTL_ADD)` with
    /// `EPOLLEXCLUSIVE | EPOLLET | <interest_bits>`. The owner's
    /// epoll already received the registration through mio's
    /// `Registry::register`; this method only touches peers.
    ///
    /// Step 2 of the EPOLLEXCLUSIVE-fanout rollout: the helper is
    /// wired in but `Reactor::poll_and_dispatch` does not yet route
    /// peer-delivered events back to the owner's slab via the token's
    /// `worker_idx` field, so peer dispatch falls through as a slab
    /// miss for now. Step 3 closes that loop.
    ///
    /// `EPOLLEXCLUSIVE` (Linux 4.5+) instructs the kernel to wake at
    /// most one of the threads currently blocked in `epoll_wait` on
    /// epolls containing this fd's interest record with the flag set,
    /// across the whole fanout set. Combined with `EPOLLET`, that
    /// gives one wake per state-change with no thundering-herd.
    ///
    /// On any peer's `EPOLL_CTL_ADD` failure, we roll back the peers
    /// we already added (best-effort `EPOLL_CTL_DEL`) and surface the
    /// error to the caller, which is responsible for undoing the
    /// owner-side mio register and slab insert.
    #[cfg(target_os = "linux")]
    fn fanout_register_peers(
        &self,
        owner_idx: usize,
        fd: RawFd,
        interest: Interest,
        token: mio::Token,
    ) -> io::Result<()> {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.fanout_register_calls);

        // `EPOLLEXCLUSIVE` is the kernel's exact-once wake selector
        // across the fanout set. Per epoll_ctl(2), it is compatible
        // *only* with `EPOLLIN | EPOLLOUT | EPOLLWAKEUP | EPOLLET`;
        // adding `EPOLLRDHUP` or `EPOLLPRI` (which mio normally
        // requests on the owner-side register) would yield `EINVAL`.
        // The owner's own mio register still carries those bits, so
        // half-close / priority signals still reach user code via the
        // owner; peer-delivered events surface only the IN/OUT bits.
        let mio_int = interest.to_mio();
        let mut events: u32 = libc::EPOLLET as u32 | libc::EPOLLEXCLUSIVE as u32;
        if mio_int.is_readable() {
            events |= libc::EPOLLIN as u32;
        }
        if mio_int.is_writable() {
            events |= libc::EPOLLOUT as u32;
        }

        // Track which peer indices we successfully added so a partial
        // failure can be cleanly rolled back. `MAX_WORKERS` is bounded
        // by `lazy_debug::MAX_WORKERS = 16`, so a `u32` bitmask covers
        // every peer index without allocating.
        let mut added_mask: u32 = 0;

        for (i, slot) in self.workers.iter().enumerate() {
            if i == owner_idx {
                continue;
            }
            let peer_registry = match slot.shared_registry.get() {
                Some(r) => r,
                // Peer hasn't published its registry yet (early
                // startup, before its own `register_worker` call).
                // Skip — once it publishes, only fds registered after
                // that point will fan out to it. Pre-publish fds are
                // an acceptable best-effort gap because the start
                // barrier already guarantees publication completes
                // before any task runs.
                None => continue,
            };
            let peer_epfd = peer_registry.epoll_fd();
            let mut ev = libc::epoll_event {
                events,
                u64: token.0 as u64,
            };
            // SAFETY: `peer_epfd` is owned by the peer's
            // `SharedRegistry` (via `mio::Registry::try_clone`) and
            // remains live until runtime shutdown, which is gated by
            // `Drop` on `ShardedMioHandle`. `fd` is the just-
            // registered source's raw fd. `&mut ev` is a fresh stack
            // value the kernel reads but does not retain.
            let ret = unsafe {
                libc::epoll_ctl(peer_epfd, libc::EPOLL_CTL_ADD, fd, &mut ev)
            };
            if ret == 0 {
                added_mask |= 1u32 << i;
                continue;
            }
            let err = io::Error::last_os_error();
            bump(&COUNTERS.fanout_register_errors);

            // Roll back the peer adds we already committed. Each
            // `EPOLL_CTL_DEL` is best-effort: if the peer's epoll fd
            // closed mid-fanout we just continue. The owner-side
            // rollback (mio deregister + slab drop) is the caller's
            // responsibility.
            for (j, slot_j) in self.workers.iter().enumerate() {
                if added_mask & (1u32 << j) == 0 {
                    continue;
                }
                if let Some(reg_j) = slot_j.shared_registry.get() {
                    // SAFETY: same lifetime argument as the ADD above.
                    unsafe {
                        let _ = libc::epoll_ctl(
                            reg_j.epoll_fd(),
                            libc::EPOLL_CTL_DEL,
                            fd,
                            std::ptr::null_mut(),
                        );
                    }
                }
            }
            return Err(err);
        }

        bump(&COUNTERS.fanout_register_ok);
        Ok(())
    }

    /// Remove `fd` from every peer worker's epoll fd. Mirrors
    /// `fanout_register_peers` for the deregister path. Best-effort:
    /// `EBADF` (peer epoll closed during shutdown) and `ENOENT`
    /// (kernel auto-removed the entry when `fd` was closed) are
    /// counted and ignored.
    ///
    /// Caller must have already detected — via the owner's
    /// `DeregisterOutcome::Applied` return — that the kernel-side
    /// epoll record on the owner still belongs to this registration
    /// (i.e. the fd was not recycled). Skipping the peer fanout when
    /// the owner skipped is what protects a fresh registration on a
    /// recycled fd from being clobbered through one of the peer
    /// epolls.
    #[cfg(target_os = "linux")]
    fn fanout_deregister_peers(&self, owner_idx: usize, fd: RawFd) {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.fanout_deregister_calls);
        for (i, slot) in self.workers.iter().enumerate() {
            if i == owner_idx {
                continue;
            }
            let peer_registry = match slot.shared_registry.get() {
                Some(r) => r,
                None => continue,
            };
            // SAFETY: peer epoll fd lifetime extends to runtime
            // shutdown; null `event` pointer is permitted on
            // `EPOLL_CTL_DEL` since Linux 2.6.9 (we're far above that
            // floor — sharded-mio is gated to Linux generally).
            let ret = unsafe {
                libc::epoll_ctl(
                    peer_registry.epoll_fd(),
                    libc::EPOLL_CTL_DEL,
                    fd,
                    std::ptr::null_mut(),
                )
            };
            if ret == 0 {
                bump(&COUNTERS.fanout_deregister_ok);
                continue;
            }
            match io::Error::last_os_error().raw_os_error() {
                Some(libc::EBADF) => bump(&COUNTERS.fanout_deregister_ebadf),
                Some(libc::ENOENT) => bump(&COUNTERS.fanout_deregister_enoent),
                _ => bump(&COUNTERS.fanout_deregister_errors),
            }
        }
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

        // Owner-side mio register succeeded. Fan the registration out
        // to peer workers' epoll fds via raw `epoll_ctl(EPOLL_CTL_ADD,
        // EPOLLEXCLUSIVE | EPOLLET | <interest>)`. On any peer
        // failure, `fanout_register_peers` rolls back the peer adds
        // it already committed; we additionally undo the owner-side
        // mio register and the per-shard set insert here.
        #[cfg(target_os = "linux")]
        {
            if let Err(e) =
                self.fanout_register_peers(worker_idx, fd, interest, ok.token)
            {
                bump(&COUNTERS.register_on_worker_fanout_err);
                let mut undo_source = mio::unix::SourceFd(&fd);
                let _ = registry.deregister(&mut undo_source, fd, ok.slab_key, ok.gen);
                // SAFETY: just inserted by `allocate_existing` above.
                unsafe {
                    slot.registrations
                        .remove(&mut slot.synced.lock(), shared);
                }
                shared.shutdown();
                return Err(e);
            }
        }

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
    /// deregistration directly on the calling thread under the
    /// EPOLLEXCLUSIVE-fanout model: `mio::Registry` clones are
    /// `Send + Sync`, so there is no need to bounce through the owning
    /// worker's park loop. The owner identity (loaded from
    /// `shared.sharded_mio_worker`) is used only to identify which
    /// shard's slab the entry lives in. The owner's mio deregister
    /// runs first; on a successful (`Applied`) outcome the fanout
    /// `EPOLL_CTL_DEL` is mirrored to every peer worker's epoll fd.
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
                        // The owner-side epoll record was ours to
                        // remove; mirror that on every peer worker's
                        // epoll fd via the fanout-deregister helper.
                        // Done only on `Applied` so that an fd-reuse
                        // race (peer epoll entry now belongs to the
                        // recycled fd's fresh registration) doesn't
                        // clobber the new owner.
                        #[cfg(target_os = "linux")]
                        self.fanout_deregister_peers(worker_idx, fd);
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
