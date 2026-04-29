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
//! When the first poll happens off-runtime (rare), the producer
//! enqueues a [`DriverOp::Register`] op onto a round-robin-picked
//! worker's [`WorkerState::pending_ops`] queue and unparks the worker;
//! the worker drains the queue at park time.
//!
//! `deregister` always queues a [`DriverOp::Deregister`] op on the
//! owning worker so registry mutation stays on a single thread per
//! shard. The queue is FIFO, so the Drop-after-foreign-thread-Register
//! race is automatically resolved — Register is processed before the
//! matching Deregister.

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
///
/// `STEALING` is added to support readiness stealing: a peer worker
/// holds this state while it is mid-`epoll_wait` on our epoll fd. We
/// must not call our own `epoll_wait` while a stealer is active, or we
/// race the stealer for the kernel's exactly-once event delivery and
/// risk being starved. `begin_park` waits out a `STEALING` state via a
/// short spin before transitioning to `PARKED`.
pub(crate) const EMPTY: usize = 0;
pub(crate) const PARKED: usize = 1;
pub(crate) const NOTIFIED: usize = 2;
pub(crate) const STEALING: usize = 3;

/// Cross-thread driver op queued onto a worker's
/// [`WorkerState::pending_ops`].
///
/// Both variants carry an owning `Arc<ScheduledIo>` so the slab/registry
/// state can be mutated on the owning worker thread. `Deregister`
/// snapshots the fd at queue time because the original
/// [`mio::event::Source`] may be dropped before the worker drains the
/// op (mio's `Registry::deregister` only needs the fd, not the
/// originally-registered source value, on Linux/epoll).
pub(crate) enum DriverOp {
    Register {
        shared: Arc<ScheduledIo>,
        fd: RawFd,
        interest: Interest,
    },
    Deregister {
        shared: Arc<ScheduledIo>,
        fd: RawFd,
    },
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

    /// Synced state for [`Self::registrations`]. The cross-thread
    /// `pending_ops` queue is drained on the worker at park time so
    /// this lock is only ever taken from the owning worker thread.
    pub(super) synced: Mutex<registration_set::Synced>,

    /// Cross-thread MPSC queue of [`DriverOp`]s targeted at this
    /// worker. Drained at park time by the owning worker.
    ///
    /// `Mutex<Vec<DriverOp>>` rather than a lock-free queue because
    /// contention is bounded by registration rate (one push per fd
    /// register/deregister), not event rate. A simple mutex keeps the
    /// FIFO ordering Register-before-Deregister relies on for the
    /// foreign-thread-drop race.
    pub(super) pending_ops: Mutex<Vec<DriverOp>>,
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
            pending_ops: Mutex::new(Vec::new()),
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
/// Symmetry with [`UringHandle`][uh]: per-worker slots for unparking,
/// per-worker [`RegistrationSet`]s, plus a per-worker `pending_ops`
/// queue used only for the foreign-thread-first-poll and deregister
/// paths. There is no global registration set — each shard owns its
/// own, eliminating a single-mutex serialization point under TCP
/// connect churn.
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

    /// Try to harvest readiness from peer workers' epoll fds.
    ///
    /// Called from a worker's pre-park path when its own scheduler run
    /// queue is empty. For each peer (round-robin starting from the
    /// next index after `self_idx`) we issue a non-blocking
    /// `libc::epoll_wait(peer.epoll_fd, buf, 0)` and dispatch any
    /// returned events through the peer's [`SharedRegistry`].
    ///
    /// Why bypass mio's `Poll`: `Poll::poll` is `&mut self` and lives
    /// on the owning worker thread. The `Registry` clone, by contrast,
    /// is `Send + Sync` and gives us the same epoll fd. Calling raw
    /// `epoll_wait` on it is sound — Linux guarantees exactly-once
    /// delivery to `epoll_wait` callers, so a stealer racing the
    /// peer's own `Poll::poll` cannot cause a double-fire.
    ///
    /// Returns the total number of events dispatched. The caller
    /// generally doesn't need this — the wake side-effect is the
    /// product — but it's surfaced for tests/metrics.
    ///
    /// `buf` is a caller-owned scratch buffer; sized once on the stack
    /// (typically `[epoll_event; 32]`) to keep the steal pass
    /// allocation-free.
    #[cfg(target_os = "linux")]
    pub(crate) fn try_steal_pass(
        &self,
        self_idx: usize,
        buf: &mut [libc::epoll_event],
    ) -> usize {
        use super::lazy_debug::{bump, bump_per_worker, COUNTERS, PER_WORKER};
        bump(&COUNTERS.steal_pass_calls);
        bump_per_worker(&PER_WORKER.try_steal_pass_calls, self_idx);

        let n = self.workers.len();
        if n <= 1 || buf.is_empty() {
            return 0;
        }
        let mut total = 0usize;
        // Round-robin over peers starting after self_idx so two
        // adjacent stealers don't pile on the same victim every pass.
        'peers: for offset in 1..n {
            let victim_idx = (self_idx + offset) % n;
            let slot = &self.workers[victim_idx];
            let Some(registry) = slot.shared_registry.get() else {
                // Peer hasn't published its registry yet (early
                // startup). The start barrier prevents this in the
                // steady state, but be defensive.
                continue;
            };

            // Acquire the steal lock by CAS-ing peer.park_state into
            // STEALING. We accept *both* EMPTY and NOTIFIED as valid
            // entry states:
            //
            // * EMPTY: peer is running tasks (the common busy-burner
            //   case).
            // * NOTIFIED: peer was either previously running and got
            //   an `unpark` (no eventfd write — peer.park_state went
            //   EMPTY→NOTIFIED, harmless to steal from) OR was
            //   previously parked and got an `unpark` (eventfd byte
            //   written; peer's `poll.poll()` is about to return).
            //
            // We skip PARKED (peer is the right consumer for its own
            // events; the kernel delivers exactly-once and harvesting
            // here would starve `poll.poll()`) and STEALING (another
            // stealer beat us to this slot).
            //
            // The original-state we entered from is preserved on
            // release: if we entered from NOTIFIED, the slot returns
            // to NOTIFIED so the peer's next `begin_park` still
            // fastpaths. If we entered from EMPTY we restore EMPTY.
            //
            // Safety vs WAKER_TOKEN events on the peer's eventfd:
            // when entering from NOTIFIED there *may* be a queued
            // eventfd byte that our `epoll_wait` would consume,
            // racing the peer's own `poll.poll()` (kernel exactly-
            // once delivery). To stay safe we re-fire the peer's
            // external waker after the steal, putting a fresh byte
            // back on the eventfd — peer's poll either already
            // returned (and our re-fire becomes a harmless spurious
            // wake on its next park) or is still blocked (and our
            // re-fire is what unblocks it). EMPTY-original peers
            // have no eventfd byte queued by construction so no
            // re-fire is needed.
            let original = loop {
                let prev = slot.park_state.load(Ordering::Acquire);
                match prev {
                    PARKED => {
                        bump(&COUNTERS.steal_cas_fail_parked);
                        continue 'peers;
                    }
                    STEALING => {
                        bump(&COUNTERS.steal_cas_fail_stealing);
                        continue 'peers;
                    }
                    EMPTY | NOTIFIED => {
                        if slot
                            .park_state
                            .compare_exchange(
                                prev,
                                STEALING,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            if prev == NOTIFIED {
                                bump(&COUNTERS.steal_entered_notified);
                            }
                            break prev;
                        }
                        // CAS lost the race; reload and retry.
                        continue;
                    }
                    _ => continue 'peers,
                }
            };
            bump(&COUNTERS.steal_pass_visits);

            let epfd = registry.epoll_fd();
            // SAFETY: epfd is the live cloned epoll fd held by
            // `registry`; `buf` is a valid mutable buffer with `len`
            // entries. Timeout 0 = non-blocking.
            let rc = unsafe {
                libc::epoll_wait(
                    epfd,
                    buf.as_mut_ptr(),
                    buf.len() as libc::c_int,
                    0,
                )
            };

            // If we entered from NOTIFIED, re-fire peer's external
            // waker before releasing. See the WAKER_TOKEN safety
            // note on the entry CAS above. Cheap (one eventfd
            // write); only paid on NOTIFIED-original visits.
            if original == NOTIFIED {
                if let Some(waker) = slot.external_waker.get() {
                    let _ = waker.wake();
                }
            }

            // Release the steal lock before dispatch so a wake
            // delivered into the peer's queue can take effect via
            // `unpark` if the peer parks immediately after — we're
            // out of its hair after this point. Dispatch happens
            // outside the lock; the slab is independently protected
            // by `ops.lock()` inside `steal_dispatch`.
            //
            // CAS rather than store: while we held STEALING, an
            // `unpark` may have fired and `swap(NOTIFIED)`-ed past
            // us. In that case the slot is now NOTIFIED and the peer
            // worker should see it via its next `begin_park`. A naïve
            // `store(original)` here would silently clobber any such
            // racing notification.
            let _ = slot.park_state.compare_exchange(
                STEALING,
                original,
                Ordering::Release,
                Ordering::Relaxed,
            );

            if rc < 0 {
                let err = std::io::Error::last_os_error().raw_os_error();
                if err == Some(libc::EINTR) {
                    bump(&COUNTERS.steal_eintr);
                } else {
                    // EBADF can occur during shutdown; we just stop
                    // visiting this victim. Anything else (rare:
                    // EFAULT, EINVAL) is caller-bug territory and we
                    // also just skip — the next park pass retries.
                    bump(&COUNTERS.steal_errors);
                }
                continue;
            }
            if rc == 0 {
                bump(&COUNTERS.steal_eagain);
                continue;
            }
            let count = rc as usize;
            COUNTERS
                .steal_events_harvested
                .fetch_add(count as u64, Ordering::Relaxed);
            let woken = registry.steal_dispatch(&buf[..count]);
            // Attribute steal-side wakes to the *stealer* (this
            // worker), not the victim — matters for hypothesis 1
            // (probe registration concentrated on burner-pinned
            // workers means the stealer must do all the harvesting).
            super::lazy_debug::add_per_worker(
                &PER_WORKER.steal_events_woken,
                self_idx,
                woken as u64,
            );
            total += woken;
        }
        total
    }

    /// Called by the worker on entry to park. Returns `true` if a wake
    /// was already pending, in which case park should skip the syscall
    /// and return immediately.
    ///
    /// If a peer worker is currently in the [`STEALING`] state on this
    /// slot (mid `epoll_wait` on our epoll fd), we briefly spin waiting
    /// for it to release. The stealer holds the lock only for the
    /// duration of one non-blocking `epoll_wait` (~hundreds of ns), so
    /// the spin is bounded.
    pub(crate) fn begin_park(&self, worker_idx: usize) -> bool {
        use super::lazy_debug::{bump, bump_per_worker, COUNTERS, PER_WORKER};
        bump(&COUNTERS.begin_park_calls);
        bump_per_worker(&PER_WORKER.begin_park_calls, worker_idx);
        let slot = &self.workers[worker_idx];
        loop {
            match slot.park_state.compare_exchange(
                EMPTY,
                PARKED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    bump(&COUNTERS.begin_park_parked);
                    return false;
                }
                Err(NOTIFIED) => {
                    bump(&COUNTERS.begin_park_fastpath);
                    slot.park_state.store(EMPTY, Ordering::Release);
                    return true;
                }
                Err(STEALING) => {
                    // Peer is mid `epoll_wait(timeout=0)` on our
                    // epoll fd. Bounded duration; spin until they
                    // release back to EMPTY (or to NOTIFIED if a
                    // wake races in) and retry the CAS.
                    bump(&COUNTERS.begin_park_steal_spin);
                    while slot.park_state.load(Ordering::Acquire) == STEALING {
                        std::hint::spin_loop();
                    }
                }
                Err(state) => panic!("inconsistent park_state on begin_park: {state}"),
            }
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
    /// Two-tier dispatch:
    ///
    /// 1. **Same-worker sync path.** When the caller is itself a
    ///    sharded-mio worker (detected via
    ///    [`current_worker_index`][cwi]), the registration is applied
    ///    inline on the calling worker's own [`SharedRegistry`] —
    ///    no cross-thread queue, no unpark syscall, no pre-park
    ///    drain delay. This is the common path: `from_std` /
    ///    `TcpListener::accept` on a worker triggers first poll on
    ///    that same worker.
    ///
    /// 2. **Foreign-thread queued path.** When `current_worker_index`
    ///    returns `None` (off-runtime first poll, e.g. from a thread
    ///    spawned outside the multi-thread scheduler), or the
    ///    returned index is past the worker count (defensive),
    ///    round-robin picks a shard, pushes a
    ///    [`DriverOp::Register`] op, and unparks it.
    ///    The op is drained on the target worker's next park.
    ///
    /// `deregister` always queues — same-thread vs foreign-thread is
    /// a less interesting axis there because the deregister path is
    /// already happy to wait one park cycle. Centralising registry
    /// mutation on the owning worker keeps the FIFO Register-then-
    /// Deregister property the cross-thread Drop race relies on.
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

        self.queue_register(shared, fd, interest)
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
        // received a clone of `shared`) routes to this same worker
        // and the FIFO ordering on `pending_ops` resolves
        // Register-before-Deregister.
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

    /// Cross-thread first-poll path: enqueue a Register op onto a
    /// round-robin-picked shard and unpark it.
    fn queue_register(
        &self,
        shared: &Arc<ScheduledIo>,
        fd: RawFd,
        interest: Interest,
    ) -> io::Result<usize> {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.queue_register_calls);
        let worker_idx = self.fallback_worker();

        // Publish the worker assignment immediately so a racing
        // `deregister` enqueued behind this Register op routes to the
        // same shard.
        shared
            .sharded_mio_worker
            .store(worker_idx as u32, Ordering::Relaxed);

        let slot = &self.workers[worker_idx];
        let was_empty = {
            let mut q = slot.pending_ops.lock();
            let was_empty = q.is_empty();
            q.push(DriverOp::Register {
                shared: Arc::clone(shared),
                fd,
                interest,
            });
            was_empty
        };

        // Only unpark when transitioning the queue from empty → non-
        // empty: subsequent ops batch with no extra wake.
        if was_empty {
            self.unpark(worker_idx);
        }

        Ok(worker_idx)
    }

    fn fallback_worker(&self) -> usize {
        self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len()
    }

    /// Queue a deregister op for `shared` on the worker that originally
    /// registered it. All deregisters go through this queue —
    /// including from the owning worker — so registry mutation stays
    /// single-threaded per shard.
    ///
    /// `fd` must be the same fd that was passed to
    /// [`Self::register_local`]; the caller (typically
    /// `Registration::deregister`) has it via
    /// `RegistrationSource::registration_raw_fd()`.
    pub(crate) fn queue_deregister(&self, shared: &Arc<ScheduledIo>, fd: RawFd) {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.queue_deregister_calls);
        let worker_idx = shared.sharded_mio_worker.load(Ordering::Relaxed) as usize;
        if worker_idx >= self.workers.len() {
            bump(&COUNTERS.queue_deregister_no_worker);
            // Either never registered (unlikely — caller should have
            // checked) or the field is the sentinel u32::MAX. Nothing
            // to deregister.
            return;
        }
        let slot = &self.workers[worker_idx];
        let was_empty = {
            let mut q = slot.pending_ops.lock();
            let was_empty = q.is_empty();
            q.push(DriverOp::Deregister {
                shared: Arc::clone(shared),
                fd,
            });
            was_empty
        };
        if was_empty {
            self.unpark(worker_idx);
        }
    }

    /// Drain all pending [`DriverOp`]s targeted at `worker_idx`.
    ///
    /// Called by the owning worker on entry to park (before
    /// `reactor.park()`), so all queued Register/Deregister ops become
    /// visible to the kernel epoll set in the same syscall window
    /// that's about to block on it.
    pub(crate) fn drain_pending_ops(&self, worker_idx: usize) {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.drain_calls);
        if worker_idx >= self.workers.len() {
            return;
        }
        let slot = &self.workers[worker_idx];

        // Take ownership of the queue contents in one shot to
        // minimize lock-hold time. New ops pushed after this swap go
        // onto the next park's drain.
        let ops = {
            let mut q = slot.pending_ops.lock();
            std::mem::take(&mut *q)
        };

        for op in ops {
            match op {
                DriverOp::Register {
                    shared,
                    fd,
                    interest,
                } => {
                    bump(&COUNTERS.drain_register_drained);
                    self.apply_register(worker_idx, shared, fd, interest);
                }
                DriverOp::Deregister { shared, fd } => {
                    bump(&COUNTERS.drain_deregister_drained);
                    self.apply_deregister(worker_idx, shared, fd);
                }
            }
        }

        // Drain any deferred releases that landed during the drain
        // pass (or in-flight from prior parks). Keeps the per-shard
        // `pending_release` vec from growing unboundedly when the
        // NOTIFY_AFTER threshold isn't hit.
        if slot.registrations.needs_release() {
            slot.registrations.release(&mut slot.synced.lock());
        }
    }

    fn apply_register(
        &self,
        worker_idx: usize,
        shared: Arc<ScheduledIo>,
        fd: RawFd,
        interest: Interest,
    ) {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.apply_register_calls);
        let slot = &self.workers[worker_idx];

        // Track in the per-shard set first.
        if slot
            .registrations
            .allocate_existing(&mut slot.synced.lock(), &shared)
            .is_err()
        {
            bump(&COUNTERS.apply_register_shutdown);
            // Driver shutting down — surface a final shutdown event
            // to any waiter so they don't hang.
            shared.shutdown();
            return;
        }

        let registry = match slot.shared_registry.get() {
            Some(r) => r,
            None => {
                bump(&COUNTERS.apply_register_no_registry);
                // Worker hasn't published its registry yet (shouldn't
                // happen post-startup; just be safe). Roll back.
                // SAFETY: `shared` was just inserted by
                // `allocate_existing` above.
                unsafe {
                    slot.registrations
                        .remove(&mut slot.synced.lock(), &shared);
                }
                shared.shutdown();
                return;
            }
        };

        let mut source = mio::unix::SourceFd(&fd);
        let ok = match registry.register(&mut source, fd, interest, &shared) {
            Ok(ok) => ok,
            Err(_e) => {
                bump(&COUNTERS.apply_register_errors);
                // SAFETY: just inserted by `allocate_existing` above.
                unsafe {
                    slot.registrations
                        .remove(&mut slot.synced.lock(), &shared);
                }
                shared.shutdown();
                return;
            }
        };

        // Owner-side mio register succeeded. Fan out to peer epoll
        // fds; on partial failure the helper rolls back the peers it
        // already added, and we additionally undo the owner-side
        // register and per-shard set insert.
        #[cfg(target_os = "linux")]
        {
            if let Err(_e) =
                self.fanout_register_peers(worker_idx, fd, interest, ok.token)
            {
                bump(&COUNTERS.apply_register_fanout_err);
                let mut undo_source = mio::unix::SourceFd(&fd);
                let _ = registry.deregister(&mut undo_source, fd, ok.slab_key, ok.gen);
                // SAFETY: just inserted by `allocate_existing` above.
                unsafe {
                    slot.registrations
                        .remove(&mut slot.synced.lock(), &shared);
                }
                shared.shutdown();
                return;
            }
        }

        bump(&COUNTERS.apply_register_ok);
        super::lazy_debug::bump_per_worker(
            &super::lazy_debug::PER_WORKER.register_per_worker,
            worker_idx,
        );
        // `sharded_mio_gen` was stamped on `shared` by
        // `SharedRegistry::register`.
        shared
            .sharded_mio_slab_key
            .store(ok.slab_key, Ordering::Relaxed);
        self.metrics.incr_fd_count();
    }

    fn apply_deregister(&self, worker_idx: usize, shared: Arc<ScheduledIo>, fd: RawFd) {
        use super::lazy_debug::{bump, maybe_dump_trial, COUNTERS};
        bump(&COUNTERS.apply_deregister_calls);
        // Per-trial delta hook (no-op unless `TOKIO_LAZY_DEBUG_TRIAL`
        // is set). Placed here because `io_busy_owner` deregisters
        // exactly once per probe task at end-of-trial — modulo the
        // configured `N` this gives one delta dump per bench iter.
        maybe_dump_trial();
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

        // Mark the registration for release; the worker will run
        // through `pending_release` either at the NOTIFY_AFTER
        // threshold or at the end of `drain_pending_ops`.
        let _ = slot
            .registrations
            .deregister(&mut slot.synced.lock(), &shared);
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
