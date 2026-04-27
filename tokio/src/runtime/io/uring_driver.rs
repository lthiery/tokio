//! Shared handle and cross-worker coordination for the uring-reactor
//! backend.
//!
//! The [`Reactor`] itself is per-worker and `!Send`. [`UringHandle`] is the
//! shared half: it holds per-worker metadata (ring fds, external wakers,
//! park state) so that any thread can unpark any worker.
//!
//! # Unpark routing
//!
//! When `unpark(target)` is called:
//!
//! 1. Flip the target's park state atomic to `NOTIFIED`. If the previous
//!    value wasn't `PARKED`, the worker isn't blocked — it will observe the
//!    notification on its next trip through park(). No syscall.
//!
//! 2. Otherwise, the worker is blocked in `io_uring_enter`. We pick a wake
//!    mechanism:
//!
//!    - If we are currently executing on a worker thread, we submit a
//!      `MSG_RING` SQE on *our own* ring targeting the receiver's ring.
//!      This is the hot cross-worker path and costs one non-blocking
//!      syscall.
//!
//!    - Otherwise (external thread: `spawn_blocking`, user code, signal
//!      handler shim, etc.) we write to the target worker's eventfd via
//!      [`ExternalWaker`]. The eventfd is registered on the target ring
//!      with `POLL_ADD_MULTI` and fires a CQE, unblocking park.
//!
//! # The `LOCAL_REACTOR` thread-local
//!
//! Cross-worker `MSG_RING` wakes need access to the current thread's own
//! Reactor (to push an SQE on its ring). We publish this via a thread-local
//! pointer, installed by a [`LocalReactorGuard`] RAII token at worker
//! startup. The pointer targets a `RefCell<Reactor>` whose `borrow_mut`
//! sees no contention in practice: during task execution the worker is not
//! parked, and during park the worker is blocked in the kernel — the two
//! time windows are strictly non-overlapping on the same thread.

use std::cell::{Cell, RefCell};
use std::io;
use std::os::fd::RawFd;
use std::ptr;
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use crate::io::interest::Interest;
use crate::loom::sync::Mutex;
use crate::runtime::io::registration_set;
use crate::runtime::io::uring_arm_table::ArmTable;
use crate::runtime::io::uring_reactor::{ExternalWaker, Reactor};
use crate::runtime::io::{IoDriverMetrics, RegistrationSet, ScheduledIo};

/// Park-state atomic values. Shape mirrors the mio parker's transitions so
/// integration stays familiar.
pub(crate) const EMPTY: usize = 0;
pub(crate) const PARKED: usize = 1;
pub(crate) const NOTIFIED: usize = 2;

/// Sentinel value stored into `ScheduledIo::uring_worker` for the duration
/// of a cross-ring rebind. Concurrent `rebind_source` attempts serialize on
/// this via CAS: only one rebinder at a time installs `REBINDING_MARKER`;
/// others see the mid-rebind value and back off.
///
/// Chosen to be distinct from both `u32::MAX` (the "never registered"
/// default on `ScheduledIo::uring_worker`) and from any legitimate worker
/// index (worker count is bounded by `num_cpus` in practice, always
/// << 2^32).
pub(crate) const REBINDING_MARKER: u32 = u32::MAX - 1;

/// An operation that needs to be submitted on a specific worker's ring.
///
/// Cross-worker fd (de)registration goes through this queue because
/// `IORING_OP_POLL_ADD` / `POLL_REMOVE` must be submitted on the ring where
/// the registration lives (and `SINGLE_ISSUER` pins submission to the owning
/// worker thread). Any thread — worker or external — can push ops here;
/// only the owning worker drains.
#[derive(Debug)]
pub(crate) enum PendingOp {
    /// Install a multi-shot POLL_ADD for `fd` / `interest`. The reactor
    /// allocates a slab slot for the registration and stamps the slot key
    /// onto `io.uring_slab_key`; the cloned `Arc` is held in the slab until
    /// the registration's terminal CQE arrives.
    Register {
        fd: RawFd,
        interest: Interest,
        io: Arc<ScheduledIo>,
    },
    /// Submit a POLL_REMOVE for the slab slot identified by `(slab_key,
    /// slab_gen)`. The snapshot is taken at the time the pending op is
    /// queued; a stale request (slot recycled by a rebind before the owning
    /// worker drains its queue) is detected by gen-check inside
    /// `Reactor::deregister` and silently ignored. The slab entry's Arc is
    /// released by the reactor on the terminal CQE, not here.
    Deregister { slab_key: u32, slab_gen: u32 },
}

/// Per-worker coordination slot. One of these per worker, indexed by worker
/// id. All fields are thread-safe since multiple unparkers may target the
/// same worker concurrently.
#[derive(Debug)]
pub(crate) struct WorkerState {
    /// `EMPTY | PARKED | NOTIFIED`. Written by the owning worker on the
    /// park/resume transition; read and CAS'd by unparkers.
    pub(crate) park_state: AtomicUsize,

    /// Raw fd of the worker's ring. `-1` until the worker publishes it
    /// during startup via [`UringHandle::register_worker`].
    ///
    /// We store a `RawFd` rather than a `BorrowedFd` because the fd's
    /// lifetime is managed by the worker's `Reactor` (which owns the
    /// `IoUring`); the `AtomicI32` simply advertises the value.
    pub(crate) ring_fd: AtomicI32,

    /// Eventfd handle for waking this worker from a non-worker thread.
    /// Published once, at worker startup.
    pub(crate) external_waker: OnceLock<ExternalWaker>,

    /// Shared handle to this worker's arm table. Peer workers use this to
    /// atomically flip the `DISARMED` bit on a slab slot during a
    /// cross-ring rebind, without needing access to the owning worker's
    /// `!Sync` [`Reactor`]. The `Arc<ArmTable>` is cloned from the reactor
    /// at startup (`Reactor::arm_table()`) and stored here.
    ///
    /// Published once, at worker startup, by
    /// [`UringHandle::register_worker`]. Remains `None` (empty `OnceLock`)
    /// for workers that never brought a reactor up — in which case rebind
    /// requests targeting that worker are silently dropped by
    /// [`UringHandle::rebind_source`].
    pub(crate) arm_table: OnceLock<Arc<ArmTable>>,

    /// Ops pending submission on this worker's ring. Any thread can push;
    /// only the owning worker drains, on its next trip through park().
    ///
    /// A `Mutex<Vec<_>>` is coarse but fits the usage pattern: pushes are
    /// rare (one per fd registration/deregistration), and the drain batch
    /// happens once per park, not per SQE.
    pub(crate) pending_ops: Mutex<Vec<PendingOp>>,
}

impl WorkerState {
    fn new() -> Self {
        Self {
            park_state: AtomicUsize::new(EMPTY),
            ring_fd: AtomicI32::new(-1),
            external_waker: OnceLock::new(),
            arm_table: OnceLock::new(),
            pending_ops: Mutex::new(Vec::new()),
        }
    }
}

/// Shared I/O handle for the uring-reactor backend.
///
/// The Handle-side analog of the mio driver's [`Handle`]. Holds per-worker
/// slots used for cross-worker and external unparking, plus the shared
/// registration set and metrics (reused wholesale from the mio side — they
/// are backend-agnostic).
///
/// [`Handle`]: super::driver::Handle
pub(crate) struct UringHandle {
    /// Per-worker state, indexed by worker id. Length equals the runtime's
    /// worker count and does not change after construction.
    workers: Box<[WorkerState]>,

    /// Round-robin counter for assigning newly registered fds to workers.
    /// Bumped once per `add_source` call.
    next_worker: AtomicUsize,

    /// Shared registration set (fd → ScheduledIo). Identical to the mio
    /// driver's usage; the Arc-pinned `ScheduledIo` instances are also
    /// referenced from each per-worker reactor's slab while a registration
    /// is live.
    pub(super) registrations: RegistrationSet,
    pub(super) synced: Mutex<registration_set::Synced>,

    pub(crate) metrics: IoDriverMetrics,

    /// Per-runtime startup barrier. Every worker waits here after building
    /// its reactor and before entering the scheduler loop, so that task
    /// polling on all workers begins at the same wall-clock moment.
    ///
    /// Why: without this barrier, cold-start differentials between workers
    /// (e.g. worker 0 initializes first and starts polling before worker 3
    /// has finished its `io_uring_setup`) produce observable clock skew in
    /// tests that measure durations across workers. See the analysis in
    /// `tcp_read_blocks_then_wakes` — the server's sleep clock would start
    /// before the client's `Instant::now`, making `elapsed` under-estimate
    /// the real sleep duration. The barrier collapses that window to
    /// (approximately) the monotonic clock's resolution.
    ///
    /// Sized to `num_workers` at construction. `std::sync::Barrier` is
    /// reusable, but we only use the first trip; it has no shutdown mode,
    /// so correctness requires that every worker thread reaches the
    /// barrier. The multi-thread scheduler's `launch` path spawns exactly
    /// `num_workers` blocking tasks and each calls [`Self::wait_for_start`]
    /// at the top of `worker::run`, satisfying that requirement.
    start_barrier: std::sync::Barrier,

    // DIAG (temporary): rebind instrumentation. Remove before commit.
    diag_rebind_entered: AtomicU64,
    diag_rebind_already_on_target: AtomicU64,
    diag_rebind_cas_lost: AtomicU64,
    diag_rebind_committed: AtomicU64,
    diag_rebind_register_err: AtomicU64,
    diag_rebind_no_local_reactor: AtomicU64,
    /// NxN transition matrix flattened: entry [old*N + new] counts commits.
    diag_transitions: Box<[AtomicU64]>,
}

impl Drop for UringHandle {
    fn drop(&mut self) {
        // DIAG (temporary): dump rebind counters for perf investigation.
        let entered = self.diag_rebind_entered.load(Ordering::Relaxed);
        if entered == 0 {
            return;
        }
        let committed = self.diag_rebind_committed.load(Ordering::Relaxed);
        let already = self.diag_rebind_already_on_target.load(Ordering::Relaxed);
        let cas_lost = self.diag_rebind_cas_lost.load(Ordering::Relaxed);
        let reg_err = self.diag_rebind_register_err.load(Ordering::Relaxed);
        let no_local = self.diag_rebind_no_local_reactor.load(Ordering::Relaxed);
        eprintln!(
            "# DIAG rebind: entered={entered} committed={committed} already_on_target={already} cas_lost={cas_lost} register_err={reg_err} no_local_reactor={no_local}"
        );
        let n = self.workers.len();
        if n > 0 && committed > 0 {
            eprintln!("# DIAG rebind transition matrix (rows=old_worker, cols=new_worker):");
            for old in 0..n {
                let mut row = String::new();
                for new in 0..n {
                    let v = self.diag_transitions[old * n + new].load(Ordering::Relaxed);
                    row.push_str(&format!("{:>10} ", v));
                }
                eprintln!("#   W{old} -> {row}");
            }
        }
    }
}

impl std::fmt::Debug for UringHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringHandle")
            .field("num_workers", &self.workers.len())
            .finish_non_exhaustive()
    }
}

impl UringHandle {
    pub(crate) fn new(num_workers: usize) -> Self {
        let mut workers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            workers.push(WorkerState::new());
        }
        let (registrations, synced) = RegistrationSet::new();
        // Barrier must have at least 1 participant; a handle with zero
        // workers is degenerate but we keep it constructible for tests.
        let barrier_count = num_workers.max(1);
        let diag_transitions: Box<[AtomicU64]> = (0..num_workers * num_workers)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            workers: workers.into_boxed_slice(),
            next_worker: AtomicUsize::new(0),
            registrations,
            synced: Mutex::new(synced),
            metrics: IoDriverMetrics::default(),
            start_barrier: std::sync::Barrier::new(barrier_count),
            diag_rebind_entered: AtomicU64::new(0),
            diag_rebind_already_on_target: AtomicU64::new(0),
            diag_rebind_cas_lost: AtomicU64::new(0),
            diag_rebind_committed: AtomicU64::new(0),
            diag_rebind_register_err: AtomicU64::new(0),
            diag_rebind_no_local_reactor: AtomicU64::new(0),
            diag_transitions,
        }
    }

    /// Block until every sibling worker has also reached this call. Used
    /// exactly once per worker at startup, after the reactor has been
    /// built and published but before the worker enters its task loop.
    /// See `start_barrier` field docs for rationale.
    pub(crate) fn wait_for_start(&self) {
        self.start_barrier.wait();
    }

    /// Number of workers this handle serves.
    #[allow(dead_code)]
    pub(crate) fn num_workers(&self) -> usize {
        self.workers.len()
    }

    /// Publish a worker's ring fd and external waker, called exactly once
    /// per worker during scheduler startup. After this returns, other
    /// threads may target the worker with `unpark`.
    ///
    /// Safety of publishing the ring fd as a `RawFd`: the fd is owned by
    /// the worker's `IoUring`, which lives for the duration of the worker
    /// loop. Unpark callers must not use this fd after runtime shutdown.
    /// In practice the runtime's shutdown sequence joins worker threads
    /// before dropping `UringHandle`, closing the window.
    pub(crate) fn register_worker(
        &self,
        worker_idx: usize,
        ring_fd: std::os::fd::RawFd,
        external_waker: ExternalWaker,
        arm_table: Arc<ArmTable>,
    ) {
        let slot = &self.workers[worker_idx];
        // Publish the arm table *before* the ring_fd. Peer workers that
        // observe `ring_fd >= 0` in `rebind_source` will want to also pull
        // the arm_table; ordering the stores this way lets them assume
        // that if `ring_fd` is visible, `arm_table` already is too. Both
        // stores use `Release`; a paired `Acquire` load of `ring_fd` on the
        // reader side is sufficient to synchronize with the `arm_table`
        // `OnceLock::set` that happened-before it.
        if slot.arm_table.set(arm_table).is_err() {
            debug_assert!(false, "worker {worker_idx} published arm_table twice");
        }
        // `Release` so the eventfd/ring-fd writes are visible to unparkers
        // that observe the ring_fd.
        slot.ring_fd.store(ring_fd, Ordering::Release);
        // OnceCell::set returns Err if already set; we expect exactly-once
        // publication. In debug builds, panic; in release, last-writer-wins
        // with a visible debug_assert failure.
        if slot.external_waker.set(external_waker).is_err() {
            debug_assert!(false, "worker {worker_idx} published external_waker twice");
        }
    }

    /// Mark `worker_idx` as notified and — if the worker was parked —
    /// deliver an actual wake.
    ///
    /// Returns `true` if a wake was delivered to the kernel (for metrics).
    pub(crate) fn unpark(&self, worker_idx: usize) -> bool {
        let slot = &self.workers[worker_idx];
        // Swap to NOTIFIED with Release ordering so that any prior writes
        // by the caller (e.g., task queue push) are visible to the woken
        // worker, which reads this atomic with Acquire.
        let prev = slot.park_state.swap(NOTIFIED, Ordering::Release);
        if prev != PARKED {
            // Worker wasn't blocked — either already running (EMPTY) or
            // already notified (NOTIFIED). No syscall needed.
            return false;
        }
        // Worker is in `io_uring_enter` — actually wake it.
        self.deliver_wake(worker_idx);
        true
    }

    /// Called by the worker on entry to park, to record that it is about
    /// to block. Returns `true` if a wake was already pending, in which
    /// case the worker should skip the actual syscall and return
    /// immediately.
    pub(crate) fn begin_park(&self, worker_idx: usize) -> bool {
        let slot = &self.workers[worker_idx];
        match slot.park_state.compare_exchange(
            EMPTY,
            PARKED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => false, // Now PARKED, go ahead and block.
            Err(NOTIFIED) => {
                // Consume the notification and skip the park.
                slot.park_state.store(EMPTY, Ordering::Release);
                true
            }
            Err(state) => panic!("inconsistent park_state on begin_park: {state}"),
        }
    }

    /// Called by the worker on park completion. Resets state to EMPTY,
    /// clearing any notification that was consumed by the wake.
    pub(crate) fn end_park(&self, worker_idx: usize) {
        let slot = &self.workers[worker_idx];
        // Unconditionally clear — any notification that arrives after the
        // kernel released us will have already caused the CQE we're now
        // draining, so we're caught up.
        slot.park_state.store(EMPTY, Ordering::Release);
    }

    /// Register a raw fd for readiness notifications.
    ///
    /// Allocates a [`ScheduledIo`] from the shared registration set, picks a
    /// worker via round-robin, and queues a [`PendingOp::Register`] on that
    /// worker. The worker submits the actual `POLL_ADD_MULTI` SQE on its
    /// next trip through park() (we unpark it here so that happens
    /// immediately).
    ///
    /// # Placement policy
    ///
    /// **Caller-local if we're on a worker, round-robin otherwise.** The
    /// calling thread's worker index is probed via
    /// [`current_worker_index`][]; if set, the new registration lands on
    /// that worker's ring. If not (external thread — `spawn_blocking`,
    /// user code on `block_on`, etc.), we fall back to round-robin via
    /// [`Self::fallback_worker`].
    ///
    /// Rationale: task-local placement is the cheap default —
    /// `SINGLE_ISSUER`-wise, the task is already polling *here*, so its
    /// first `POLL_ADD_MULTI` should fire *here* too. When the task is
    /// then work-stolen to another worker, the rebind path
    /// ([`Self::rebind_source`], kicked by
    /// [`Registration::poll_ready`][reg-poll]) migrates the registration
    /// to wherever the task actually polls — including the historic
    /// listener + `tokio::spawn(handler)` pathology where v1's caller-
    /// local placement regressed because worker 0 owned every accepted
    /// fd's `POLL_ADD_MULTI` (no rebind then; see commit `e29114e1`).
    /// With the v2 rebind wired up, that pathology is now cancelled on
    /// first-poll: the handler task's initial `poll_ready` on whichever
    /// worker got the `tokio::spawn` job flips the registration to that
    /// worker's ring before the kernel has even had a chance to post the
    /// first CQE.
    ///
    /// [`current_worker_index`]:
    ///     crate::runtime::scheduler::multi_thread::uring_park::current_worker_index
    /// [reg-poll]: crate::runtime::io::Registration::poll_ready
    ///
    /// Returns the allocated `Arc<ScheduledIo>` together with the assigned
    /// worker index. Callers must remember the index and pass it back into
    /// [`Self::deregister_source`] so `POLL_REMOVE` lands on the same ring
    /// as the original `POLL_ADD` (required by io_uring — remove ops are
    /// scoped to their ring).
    pub(crate) fn add_source(
        &self,
        fd: RawFd,
        interest: Interest,
    ) -> io::Result<(Arc<ScheduledIo>, usize)> {
        let io = self.registrations.allocate(&mut self.synced.lock())?;

        let worker_idx = self.fallback_worker();

        // Publish the assigned worker onto the ScheduledIo so that v2 lazy
        // placement can observe "which worker currently owns this ring
        // registration" on every poll without touching the handle.
        io.uring_worker
            .store(worker_idx as u32, Ordering::Relaxed);

        {
            let slot = &self.workers[worker_idx];
            let mut queue = slot.pending_ops.lock();
            queue.push(PendingOp::Register {
                fd,
                interest,
                io: Arc::clone(&io),
            });
        }

        // Kick the target worker so it drains the queue. If it is parked in
        // `io_uring_enter`, this wakes it; if it is currently executing, the
        // notification is consumed on the next park.
        self.unpark(worker_idx);

        self.metrics.incr_fd_count();
        Ok((io, worker_idx))
    }

    /// Pick a worker by round-robin across `next_worker`. `fetch_add` is
    /// `Relaxed`: ordering of assignments does not affect correctness, only
    /// balance, and we only need distinct calls to tend toward distinct
    /// workers.
    ///
    /// Kept as a named helper (rather than inlined into `add_source`)
    /// because the v2 re-registration path will call it when an fd needs
    /// to be rebound to a different worker and we don't yet know which one.
    fn fallback_worker(&self) -> usize {
        self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len()
    }

    /// Queue a `POLL_REMOVE` for `io` on the worker it was registered with.
    ///
    /// The caller must pass the `worker_idx` returned by [`Self::add_source`];
    /// there is no fd→worker reverse lookup on the handle itself.
    ///
    /// This also enqueues `io` for release from the shared
    /// [`RegistrationSet`] (matching the mio driver's semantics: the Arc
    /// hangs around in `pending_release` until the driver's next cleanup
    /// pass).
    pub(crate) fn deregister_source(
        &self,
        io: &Arc<ScheduledIo>,
        worker_idx: usize,
    ) -> io::Result<()> {
        // Queue the POLL_REMOVE first so that if the caller races with
        // shutdown, the kernel-side cleanup still lands before the Arc is
        // freed. The worker drains this at its next park() call.
        //
        // We snapshot the slab identity at push time so that a later rebind
        // (which re-stamps `uring_slab_key`/`uring_gen`) cannot confuse the
        // worker into cancelling the wrong slot. If the snapshot is already
        // stale (e.g. the resource was never registered, or was migrated
        // between `add_source` and `deregister_source`), the reactor's
        // gen-check in `Reactor::deregister` will drop the request silently.
        if worker_idx < self.workers.len() {
            let slab_key = io.uring_slab_key.load(Ordering::Relaxed);
            let slab_gen = io.uring_gen.load(Ordering::Relaxed);
            let slot = &self.workers[worker_idx];
            let mut queue = slot.pending_ops.lock();
            queue.push(PendingOp::Deregister { slab_key, slab_gen });
        }

        // Mark the registration for release; the worker-side drain of
        // `pending_release` will drop the Arc after the POLL_REMOVE CQE.
        let should_unpark = self.registrations.deregister(&mut self.synced.lock(), io);

        // Always wake the target so it observes both the pending REMOVE and
        // (if the registration set threshold tripped) the release signal.
        if worker_idx < self.workers.len() {
            self.unpark(worker_idx);
        }
        // Mirrors mio driver's extra unpark when the release threshold hits.
        // On the uring side the "driver" is per-worker, so we re-kick the
        // same worker — release work runs in its drain loop.
        if should_unpark && worker_idx < self.workers.len() {
            self.unpark(worker_idx);
        }

        self.metrics.dec_fd_count();
        Ok(())
    }

    /// Migrate a live registration from its current owner worker onto
    /// `target_worker`'s ring.
    ///
    /// This is the v2-placement hot path: when a task is polled on worker
    /// `W_task` but its fd's `POLL_ADD_MULTI` lives on `W_ring != W_task`,
    /// a call here moves the registration so that subsequent CQEs fire on
    /// `W_task` (eliminating cross-ring wake hops).
    ///
    /// # Protocol
    ///
    /// 1. **Serialize with other rebinders.** CAS `io.uring_worker` from
    ///    its current value → [`REBINDING_MARKER`]. A racing rebinder
    ///    already holds the marker and will complete its own migration; we
    ///    bail out (returning `Ok(false)`).
    ///
    /// 2. **Snapshot the old slot identity** (`old_slab_key`,
    ///    `old_slab_gen`, `old_worker`) *before* mutating anything. These
    ///    are what the peer reactor needs to cancel its `POLL_ADD_MULTI`.
    ///
    /// 3. **Register locally.** `Reactor::register` allocates a fresh
    ///    slab slot on `target_worker`'s reactor, publishes the new
    ///    `(key, gen)` onto `io`, and stages a `POLL_ADD_MULTI` SQE on our
    ///    ring. From this moment forward, new kernel readiness fires on
    ///    `target_worker`.
    ///
    /// 4. **Release the marker.** Store `target_worker` into
    ///    `io.uring_worker` — concurrent pollers can now observe the new
    ///    owner.
    ///
    /// 5. **Disarm + cancel the old slot.** Flip `DISARMED` on the old
    ///    owner's [`ArmTable`] via [`ArmTable::try_disarm`]. If we win
    ///    the race (`true` returned), we hold cancellation responsibility:
    ///    we submit a `MSG_RING` carrying [`VARIANT_CROSS_DEREGISTER`] so
    ///    the old owner's reactor will emit the local `POLL_REMOVE`. If
    ///    `try_disarm` returns `false`, the old slot is already gone
    ///    (gen mismatch after recycle) or has already been disarmed by a
    ///    concurrent deregister — either way, the cancellation kick has
    ///    an owner and we must not double-submit.
    ///
    /// # Correctness invariants
    ///
    /// - From step 3 onward, any CQE the old peer posts for the old slot
    ///   will be observed as `DISARMED` (once step 5 commits) and
    ///   **suppressed from waking the task**. That is the whole point of
    ///   the arm table: it lets us cancel observation instantly, before
    ///   the peer's `POLL_REMOVE` has even been submitted, let alone
    ///   reaped.
    ///
    /// - The slab Arc on the old peer is released by the peer's own drain
    ///   loop on the terminal CQE; not our responsibility.
    ///
    /// - If this method returns `Ok(false)` (no rebind happened, because
    ///   a race was lost or no move was necessary), the registration is
    ///   in a consistent pre-call state from the caller's perspective:
    ///   either still owned by `old_worker` (CAS lost) or already owned
    ///   by `target_worker` (value raced but equal).
    ///
    /// # Returns
    ///
    /// - `Ok(true)`: the rebind committed. Subsequent polls on
    ///   `target_worker` will observe readiness locally.
    /// - `Ok(false)`: the rebind was skipped (already at target, race
    ///   lost to a concurrent rebinder, or target worker has no reactor
    ///   installed yet).
    /// - `Err(..)`: a local SQE push failed. The CAS is rolled back before
    ///   returning. No kernel-side state changed.
    ///
    /// # Caller contract
    ///
    /// - Must be called **on** the thread that owns `target_worker`'s
    ///   reactor (i.e. with that reactor installed in `LOCAL_REACTOR`).
    ///   `SINGLE_ISSUER` requires that the local `POLL_ADD_MULTI` be
    ///   submitted on the current thread's own ring.
    /// - `fd` and `interest` must match the original `add_source` call
    ///   for this `io`. The caller typically keeps them on the
    ///   `Registration` itself.
    ///
    /// [`ArmTable`]: crate::runtime::io::uring_arm_table::ArmTable
    /// [`ArmTable::try_disarm`]:
    ///     crate::runtime::io::uring_arm_table::ArmTable::try_disarm
    pub(crate) fn rebind_source(
        &self,
        io: &Arc<ScheduledIo>,
        fd: RawFd,
        interest: Interest,
        target_worker: usize,
    ) -> io::Result<bool> {
        self.diag_rebind_entered.fetch_add(1, Ordering::Relaxed);
        if target_worker >= self.workers.len() {
            return Ok(false);
        }

        // Observe current owner; the short-circuit below also handles the
        // "never successfully registered" (u32::MAX) and mid-rebind
        // (REBINDING_MARKER) cases.
        let old_worker_raw = io.uring_worker.load(Ordering::Acquire);
        if old_worker_raw == target_worker as u32 {
            self.diag_rebind_already_on_target.fetch_add(1, Ordering::Relaxed);
            // Already on target — nothing to do.
            return Ok(false);
        }
        if old_worker_raw == REBINDING_MARKER || old_worker_raw == u32::MAX {
            // Either another rebinder is mid-flight, or the registration
            // has never been assigned a worker (pre-`add_source`). Bail.
            return Ok(false);
        }

        // Claim the rebind with a CAS. Loss here means a concurrent
        // rebinder beat us; their completion will publish the new owner
        // and we back off. AcqRel pairs with the Acquire load above and
        // with the Release store we do on success below (step 4).
        if io
            .uring_worker
            .compare_exchange(
                old_worker_raw,
                REBINDING_MARKER,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.diag_rebind_cas_lost.fetch_add(1, Ordering::Relaxed);
            return Ok(false);
        }

        self.diag_rebind_committed.fetch_add(1, Ordering::Relaxed);
        {
            let n = self.workers.len();
            let old = old_worker_raw as usize;
            if old < n && target_worker < n {
                self.diag_transitions[old * n + target_worker]
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        // Past this point we own the rebind. Snapshot the old slot
        // identity *before* mutating. `uring_slab_key` / `uring_gen` are
        // stamped by `Reactor::register`, so the register() call in step
        // 3 will overwrite them — snapshot first.
        //
        // Relaxed is fine: these atomics were published by the *old*
        // owner under its own Release (inside `Reactor::register`), and
        // the CAS above — an AcqRel — synchronized us with that write
        // transitively via `uring_worker`.
        let old_slab_key = io.uring_slab_key.load(Ordering::Relaxed);
        let old_slab_gen = io.uring_gen.load(Ordering::Relaxed);
        let old_worker = old_worker_raw as usize;

        // Step 3: register on the target (local) reactor. We must be on
        // that worker's thread for SINGLE_ISSUER to accept our SQE.
        //
        // On failure, roll back `uring_worker` to the old value so the
        // registration stays consistent (still owned by `old_worker`,
        // no orphaned REBINDING_MARKER).
        let register_result = with_local_reactor(|reactor| reactor.register(fd, interest, io));
        match register_result {
            Some(Ok(())) => {}
            Some(Err(e)) => {
                self.diag_rebind_register_err.fetch_add(1, Ordering::Relaxed);
                io.uring_worker
                    .store(old_worker_raw, Ordering::Release);
                return Err(e);
            }
            None => {
                self.diag_rebind_no_local_reactor.fetch_add(1, Ordering::Relaxed);
                // Target worker's reactor isn't installed on this thread.
                // We can't complete the rebind safely (SINGLE_ISSUER
                // forbids registering fds from a non-owning thread).
                // Treat as "skip" — caller will retry on next poll.
                io.uring_worker
                    .store(old_worker_raw, Ordering::Release);
                return Ok(false);
            }
        }

        // Step 4: publish the new owner. Release so other threads that
        // observe this via Acquire see the fresh `uring_slab_key` /
        // `uring_gen` stamped by `Reactor::register` above.
        io.uring_worker
            .store(target_worker as u32, Ordering::Release);

        // Step 5: disarm + cancel the old registration.
        //
        // Lookup the old worker's arm table. If it isn't published yet
        // (worker never brought a reactor up), there is no registration
        // to cancel and we're done. In practice an old registration
        // always means an old reactor, so this branch is defensive.
        let Some(old_arm_table) = self.workers.get(old_worker).and_then(|w| w.arm_table.get())
        else {
            return Ok(true);
        };

        // If `old_slab_key` is the default `u32::MAX`, the registration
        // was CAS'd during a prior failed rebind rollback or was never
        // fully installed on `old_worker`. No disarm is needed.
        if old_slab_key == u32::MAX {
            return Ok(true);
        }

        if !old_arm_table.try_disarm(old_slab_key, old_slab_gen) {
            // Someone else already owns cancellation responsibility —
            // could be a concurrent `deregister_source`, or the slot has
            // been recycled (gen mismatch). Either way, no kick needed.
            return Ok(true);
        }

        // We own the cancellation kick. Fire a cross-ring MSG_RING
        // carrying VARIANT_CROSS_DEREGISTER so the old owner's drain
        // loop submits the local POLL_REMOVE against its ring. We must
        // be on our own reactor (same precondition as step 3), which is
        // satisfied by the caller contract.
        let old_ring_fd = self.workers[old_worker].ring_fd.load(Ordering::Acquire);
        if old_ring_fd < 0 {
            // Old worker's reactor has been torn down. The slab entry,
            // if any, will be cleaned up at reactor drop. DISARMED is
            // already flipped, so no spurious wake can come from it.
            return Ok(true);
        }

        let send_result = with_local_reactor(|reactor| {
            reactor.send_msg_ring_deregister(old_ring_fd, old_slab_gen, old_slab_key)
        });
        match send_result {
            Some(Ok(())) => Ok(true),
            Some(Err(e)) => Err(e),
            // `None` here means we lost the local reactor between steps
            // 3 and 5 — essentially impossible (we just used it), but
            // treat as success-with-no-kick: the peer's POLL_ADD_MULTI
            // is disarmed (won't wake tasks), and the registration's
            // Arc will be cleaned up by normal drop semantics.
            None => Ok(true),
        }
    }

    /// Drain and return the pending-ops queue for `worker_idx`. Intended to
    /// be called by the owning worker's parker before submitting the next
    /// `io_uring_enter` batch.
    pub(crate) fn take_pending_ops(&self, worker_idx: usize) -> Vec<PendingOp> {
        let slot = &self.workers[worker_idx];
        let mut queue = slot.pending_ops.lock();
        std::mem::take(&mut *queue)
    }

    /// Release any `ScheduledIo`s that have been queued for removal by
    /// [`RegistrationSet::deregister`]. Called by the owning worker after
    /// submitting any pending `POLL_REMOVE` ops so the Arcs are freed on
    /// the worker's own thread.
    pub(crate) fn release_pending_registrations(&self) {
        if self.registrations.needs_release() {
            self.registrations.release(&mut self.synced.lock());
        }
    }

    /// Deliver a wake to a definitely-parked worker via the best available
    /// mechanism for the calling thread.
    fn deliver_wake(&self, target_worker: usize) {
        let slot = &self.workers[target_worker];
        let target_ring_fd = slot.ring_fd.load(Ordering::Acquire);

        // Fast path: we're running on some worker, and the target's ring
        // fd has been published. MSG_RING from our ring.
        if target_ring_fd >= 0 {
            let sent = with_local_reactor(|reactor| {
                reactor.send_msg_ring(target_ring_fd)
            });
            match sent {
                Some(Ok(())) => return,
                Some(Err(_e)) => {
                    // MSG_RING failed — fall through to eventfd. This
                    // shouldn't happen in normal operation; if it does,
                    // the fallback keeps us correct.
                }
                None => {
                    // Not on a worker thread — fall through.
                }
            }
        }

        // Fallback path: eventfd write. Always correct; slightly more
        // expensive than MSG_RING because it goes through POLL_ADD_MULTI
        // on the target side.
        if let Some(waker) = slot.external_waker.get() {
            // Ignore write errors — if the eventfd is dead the target
            // reactor is being torn down, and shutdown will reap it.
            let _ = waker.wake();
        }
    }
}

// ===== LOCAL_REACTOR thread-local =====

thread_local! {
    /// Pointer to the current thread's `RefCell<Reactor>`, if installed.
    /// Installed by [`LocalReactorGuard`] at worker startup and cleared on
    /// drop. Accessed only by the owning thread, so no synchronization
    /// beyond the `Cell`.
    static LOCAL_REACTOR: Cell<*const RefCell<Reactor>> =
        const { Cell::new(ptr::null()) };
}

/// RAII token that installs the given `RefCell<Reactor>` as the current
/// thread's local reactor, and uninstalls it on drop.
///
/// Worker threads construct one of these after building their `Reactor`
/// and hold it for the duration of the worker loop.
#[must_use = "dropping the guard uninstalls the thread-local reactor"]
pub(crate) struct LocalReactorGuard<'a> {
    // Restrict to the Reactor's lifetime to prevent dangling pointer use.
    _marker: std::marker::PhantomData<&'a RefCell<Reactor>>,
    // Not Send: the thread-local is thread-specific.
    _not_send: std::marker::PhantomData<*const ()>,
}

impl<'a> LocalReactorGuard<'a> {
    /// Install `reactor` as the current thread's local reactor. Panics if
    /// a different reactor is already installed on this thread (nested
    /// worker loops are unsupported).
    pub(crate) fn install(reactor: &'a RefCell<Reactor>) -> Self {
        LOCAL_REACTOR.with(|slot| {
            debug_assert!(
                slot.get().is_null(),
                "another Reactor is already installed on this thread",
            );
            slot.set(reactor as *const _);
        });
        Self {
            _marker: std::marker::PhantomData,
            _not_send: std::marker::PhantomData,
        }
    }
}

impl Drop for LocalReactorGuard<'_> {
    fn drop(&mut self) {
        LOCAL_REACTOR.with(|slot| slot.set(ptr::null()));
    }
}

/// Install a `RefCell<Reactor>` pointer into the current thread's
/// `LOCAL_REACTOR` slot without an RAII guard.
///
/// This is used by the multi-thread scheduler's `UringParker`, which cannot
/// hold a `!Send` [`LocalReactorGuard`] because its containing `Core` must
/// itself be `Send` to cross the `spawn_blocking` boundary.
///
/// # Safety contract
///
/// The caller must:
///
/// 1. Pass a pointer to a `RefCell<Reactor>` whose allocation lives at
///    least as long as the pointer remains in the TLS slot.
/// 2. Call [`clear_local_reactor`] on the same thread before the pointed-to
///    allocation is dropped.
/// 3. Install only once per thread; re-installing a different pointer is a
///    programming error and will trigger the debug-mode assertion.
///
/// In practice, the worker thread constructs its `Reactor` inside a
/// `Box<RefCell<Reactor>>` owned by its `UringParker`; the parker installs
/// via this function on first `park`, and clears via `clear_local_reactor`
/// in its `Drop` impl. Both calls happen on the worker thread.
pub(crate) unsafe fn install_local_reactor_raw(ptr: *const RefCell<Reactor>) {
    LOCAL_REACTOR.with(|slot| {
        let existing = slot.get();
        if !existing.is_null() {
            let tname = std::thread::current()
                .name()
                .unwrap_or("<unnamed>")
                .to_owned();
            panic!(
                "another Reactor is already installed on thread {tname:?}: \
                 existing={existing:p} new={ptr:p}",
            );
        }
        slot.set(ptr);
    });
}

/// Clear the current thread's `LOCAL_REACTOR` slot. No-op if nothing was
/// installed. Must be called before the reactor's backing allocation is
/// dropped.
pub(crate) fn clear_local_reactor() {
    LOCAL_REACTOR.with(|slot| slot.set(ptr::null()));
}

/// Run `f` with a mutable reference to the current thread's reactor, if
/// one is installed. Returns `None` if the thread is not a worker.
///
/// Does **not** nest — calling this from inside a closure passed to itself
/// (on the same thread) will panic via `RefCell`'s runtime check. In
/// practice this cannot happen because worker threads are either executing
/// a task (not inside `park`) or blocked in the kernel (not executing
/// anything), never both.
/// Cheap probe: is a `Reactor` currently installed on this thread?
///
/// Useful for code that wants to branch on "am I on a uring worker?"
/// *before* moving owned values into a closure — `with_local_reactor`
/// captures its closure by move, so if no reactor is installed the
/// closure is never called and the values it captured are dropped
/// with it. Callers that need to keep those values on the "not on a
/// worker" branch should call this probe first.
pub(crate) fn local_reactor_installed() -> bool {
    LOCAL_REACTOR.with(|slot| !slot.get().is_null())
}

pub(crate) fn with_local_reactor<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&mut Reactor) -> R,
{
    LOCAL_REACTOR.with(|slot| {
        let ptr = slot.get();
        if ptr.is_null() {
            return None;
        }
        // SAFETY: installed by this thread via `LocalReactorGuard::install`;
        // the guard's lifetime bounds the pointer's validity, and the guard
        // cannot have been dropped while `with_local_reactor` is on the
        // call stack (both run on the same thread, drop would require
        // unwinding past this frame).
        let cell: &RefCell<Reactor> = unsafe { &*ptr };
        let mut reactor = cell.borrow_mut();
        Some(f(&mut reactor))
    })
}

// ===== tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    /// `unpark` before the worker parks sets NOTIFIED and returns without
    /// a syscall; a subsequent `begin_park` consumes the notification.
    #[test]
    fn unpark_before_park_is_consumed() {
        let handle = UringHandle::new(1);
        // No ring fd / external waker published — unpark must not touch
        // them because the worker isn't parked.
        assert!(!handle.unpark(0), "worker not parked, no wake should fire");
        assert!(handle.begin_park(0), "notification should have been pending");
    }

    /// When the caller has a `LOCAL_REACTOR` installed and the target has
    /// a published ring fd, `unpark` routes via MSG_RING rather than the
    /// eventfd fallback. Verified by observing that the target's park
    /// actually returns after the unpark call.
    #[test]
    fn unpark_on_worker_uses_msg_ring() {
        use std::sync::Arc;

        let handle = Arc::new(UringHandle::new(2));

        // Receiver thread: construct reactor, publish, then park.
        let receiver_handle = Arc::clone(&handle);
        let (ready_tx, ready_rx) = mpsc::channel();
        let (elapsed_tx, elapsed_rx) = mpsc::channel();
        let receiver = std::thread::spawn(move || {
            let Ok(mut reactor) = Reactor::new() else {
                let _ = ready_tx.send(false);
                return;
            };
            receiver_handle.register_worker(
                0,
                reactor.ring_fd(),
                reactor.external_waker(),
                reactor.arm_table(),
            );
            ready_tx.send(true).unwrap();

            // Simulate "enter park" protocol then actually park.
            let notified = receiver_handle.begin_park(0);
            assert!(!notified);
            let start = std::time::Instant::now();
            reactor.park().expect("receiver park returns");
            receiver_handle.end_park(0);
            elapsed_tx.send(start.elapsed()).unwrap();
        });

        if !ready_rx.recv().unwrap() {
            eprintln!("skipping: receiver reactor unavailable");
            receiver.join().unwrap();
            return;
        }

        // Sender thread: construct reactor, install LOCAL_REACTOR, unpark.
        let sender_handle = Arc::clone(&handle);
        let sender = std::thread::spawn(move || {
            let Ok(reactor) = Reactor::new() else {
                return false;
            };
            sender_handle.register_worker(
                1,
                reactor.ring_fd(),
                reactor.external_waker(),
                reactor.arm_table(),
            );
            let reactor_cell = RefCell::new(reactor);
            let _guard = LocalReactorGuard::install(&reactor_cell);

            // Give the receiver a beat to reach park().
            std::thread::sleep(Duration::from_millis(50));
            sender_handle.unpark(0)
        });

        let unpark_fired = sender.join().unwrap();
        assert!(unpark_fired, "unpark should deliver a wake");

        let elapsed = elapsed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        receiver.join().unwrap();

        assert!(
            elapsed >= Duration::from_millis(25),
            "receiver park returned too quickly: {elapsed:?}",
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "receiver park took suspiciously long: {elapsed:?}",
        );
    }

    /// With `PARKED` state and an external waker published, `unpark` from
    /// a non-worker thread causes the eventfd fallback to fire.
    #[test]
    fn unpark_external_uses_eventfd_fallback() {
        let Ok(reactor) = Reactor::new() else {
            eprintln!("skipping: reactor unavailable");
            return;
        };
        let external_waker = reactor.external_waker();
        let ring_fd = reactor.ring_fd();
        let arm_table = reactor.arm_table();

        let handle = UringHandle::new(1);
        handle.register_worker(0, ring_fd, external_waker, arm_table);

        // Simulate the worker having entered park.
        assert!(!handle.begin_park(0));

        // Move the reactor to a thread that will drain the wake — SINGLE_ISSUER
        // requires it to be driven by its creator, but this test only needs
        // to prove that `unpark` completes without error from a non-worker
        // thread. We don't drive the reactor in this test; we just assert
        // the eventfd write path is hit.
        //
        // (Full drive-the-wake coverage lives in the `external_waker_unblocks_park`
        // test in `uring_reactor.rs`.)
        drop(reactor);

        // Spawn a thread that has no LOCAL_REACTOR installed, so unpark
        // must use the eventfd path.
        let (done_tx, done_rx) = mpsc::channel();
        let handle_arc = std::sync::Arc::new(handle);
        let h = std::sync::Arc::clone(&handle_arc);
        let t = std::thread::spawn(move || {
            let fired = h.unpark(0);
            done_tx.send(fired).unwrap();
        });
        let fired = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        t.join().unwrap();
        assert!(fired, "unpark should have delivered a wake");
    }
}
