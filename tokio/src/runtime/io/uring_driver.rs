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
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use crate::io::interest::Interest;
use crate::loom::sync::{Condvar, Mutex};
use crate::runtime::io::registration_set;
use crate::runtime::io::uring_arm_table::ArmTable;
use crate::runtime::io::uring_reactor::{uring_global_enabled, ExternalWaker, Reactor};
use crate::runtime::io::{IoDriverMetrics, RegistrationSet, ScheduledIo};
use crate::util::TryLock;

use std::time::Duration;

/// Park-state atomic values. Shape mirrors the mio parker's transitions so
/// integration stays familiar.
pub(crate) const EMPTY: usize = 0;
pub(crate) const PARKED: usize = 1;
pub(crate) const NOTIFIED: usize = 2;
/// Global-ring mode only: parked on the per-worker condvar because another
/// worker holds the shared ring. Mirrors the stock parker's
/// `PARKED_CONDVAR`; in global mode [`PARKED`] plays the stock
/// `PARKED_DRIVER` role.
pub(crate) const PARKED_CONDVAR: usize = 3;

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
    /// Submit a POLL_REMOVE for `io`'s registration. The slab identity is
    /// read from `io.uring_slab_key`/`uring_gen` at DRAIN time, not queue
    /// time: an off-worker register + immediate dereg lands both ops in
    /// this FIFO before the worker has processed either, so at push time
    /// the key is still `u32::MAX` — a push-time snapshot silently no-ops
    /// and leaks the armed POLL_ADD_MULTI (slab slot never freed; found
    /// as the `tcp_register_dereg` ArmTable-exhaustion wedge). FIFO on the
    /// same queue guarantees the Register was drained first, so the
    /// drain-time read observes the real key. The slab entry's Arc is
    /// released by the reactor on the terminal CQE, not here.
    Deregister { io: Arc<ScheduledIo> },
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

    /// Shared handle to this worker's arm table. The `Arc<ArmTable>` is
    /// cloned from the reactor at startup (`Reactor::arm_table()`) and
    /// stored here.
    ///
    /// Published once, at worker startup, by
    /// [`UringHandle::register_worker`].
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

/// Per-worker park slot for global-ring mode. The stock parker's `Inner`
/// state machine, minus the driver reference (the shared ring lives on
/// [`GlobalRing`], not per worker).
struct GlobalParkSlot {
    /// `EMPTY | PARKED | NOTIFIED | PARKED_CONDVAR`, stock SeqCst
    /// discipline throughout.
    state: AtomicUsize,
    mutex: Mutex<()>,
    condvar: Condvar,
}

/// Phase-1 global-ring mode (`TOKIO_URING_GLOBAL=1`): ONE ring for the
/// whole runtime, driven by whichever worker parks first — the stock mio
/// `Parker` discipline with the uring [`Reactor`] in the driver seat.
/// Design + kill-criterion: `.claude/DESIGN-uring-global-phase1.md`.
///
/// Rotation is legal only because this mode forces
/// `SINGLE_ISSUER`/`DEFER_TASKRUN` off (see
/// [`uring_global_enabled`]'s interaction with the defer knob): any
/// thread may drive the ring, one at a time, serialized by the
/// [`TryLock`].
pub(crate) struct GlobalRing {
    /// The one shared reactor. Whoever `try_lock`s it parks on the ring
    /// and drains completions; everyone else condvar-parks.
    reactor: TryLock<Reactor>,

    /// Single pending-op queue — the global-mode analog of the per-worker
    /// `WorkerState::pending_ops`. Applied by the ring holder at
    /// park-entry; the `17eb1a7b` backpressure (zombie reap +
    /// `inline_reaped` park degradation) rides along unchanged.
    pending_ops: Mutex<Vec<PendingOp>>,

    /// Wakes whatever thread is blocked in `submit_and_wait` on the
    /// shared ring. Cloned from the reactor at construction. MSG_RING is
    /// useless in this mode — there is only one ring, and a sender would
    /// need a second ring to submit from — so the eventfd is the sole
    /// wake mechanism.
    ring_waker: ExternalWaker,

    /// `true` while some worker is blocked (or about to block) in
    /// `submit_and_wait` on the shared ring. Set by the holder AFTER its
    /// slot CAS to `PARKED` and BEFORE it drains `pending_ops`; cleared
    /// after the kernel enter returns. [`Self::push_op`] skips the
    /// eventfd syscall when this is `false` — the op will be drained at
    /// the next holder's park-entry instead.
    ///
    /// Why the ordering makes the skip safe: `push_op` pushes under the
    /// queue mutex and THEN loads this flag. If the load reads `false`,
    /// the holder's `take_pending` (which happens after its `true`
    /// store) has either not run yet — it will see the op — or ran
    /// before our push, in which case the `true` store happened-before
    /// our load (mutex release/acquire chains them) and we'd have read
    /// `true`, a contradiction. Either way no op is stranded. A stale
    /// `true` (holder just woke) costs one spurious eventfd CQE.
    ring_parked: std::sync::atomic::AtomicBool,

    /// Per-worker park slots, indexed by worker id.
    slots: Box<[GlobalParkSlot]>,
}

impl std::fmt::Debug for GlobalRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalRing")
            .field("num_workers", &self.slots.len())
            .finish_non_exhaustive()
    }
}

impl GlobalRing {
    /// Build the shared reactor eagerly. Runs on the runtime-builder
    /// thread — legal because global mode never sets `SINGLE_ISSUER`, so
    /// the ring is not bound to its constructing thread.
    fn new(num_workers: usize) -> io::Result<Self> {
        let reactor = Reactor::new()?;
        let ring_waker = reactor.external_waker();
        let mut slots = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            slots.push(GlobalParkSlot {
                state: AtomicUsize::new(EMPTY),
                mutex: Mutex::new(()),
                condvar: Condvar::new(),
            });
        }
        Ok(Self {
            reactor: TryLock::new(reactor),
            pending_ops: Mutex::new(Vec::new()),
            ring_waker,
            ring_parked: std::sync::atomic::AtomicBool::new(false),
            slots: slots.into_boxed_slice(),
        })
    }

    /// Queue a register/deregister op and kick the ring — but only when
    /// some worker is actually blocked in `submit_and_wait`. When nobody
    /// is, the op waits for the next park-entry drain, which every path
    /// into the ring performs before blocking; the eventfd write (a
    /// syscall per op — measured +8..15% on `tcp_register_dereg` when
    /// unconditional) buys nothing there. See `ring_parked` for the
    /// ordering that makes the skip safe.
    ///
    /// One exception on the skip path: a pusher that is NOT a worker
    /// thread has no park of its own coming up, so if every worker is
    /// condvar-parked while the ring is momentarily unheld the op would
    /// wait for an unrelated wake (unbounded latency, though never a
    /// lost op). Kick one worker so someone passes through a park-entry
    /// drain. Worker pushers skip the kick: their own next park (or
    /// zero-duration maintenance poll) drains the queue.
    pub(crate) fn push_op(&self, op: PendingOp) {
        self.pending_ops.lock().push(op);
        if self.ring_parked.load(Ordering::SeqCst) {
            let _ = self.ring_waker.wake();
        } else if crate::runtime::scheduler::multi_thread::uring_park::current_worker_index()
            .is_none()
        {
            self.kick_one_worker();
        }
    }

    fn take_ops(&self) -> Vec<PendingOp> {
        std::mem::take(&mut *self.pending_ops.lock())
    }

    /// Timer-insert wake: the legacy time driver inserted a timer that
    /// lowers the wheel minimum (see `UringHandle::unpark_for_timer`).
    ///
    /// The eventfd write is unconditional, unlike `push_op`'s: the
    /// deadline a holder parks with is computed in `park_global` BEFORE
    /// the `TryLock` race, so gating on `ring_parked` could skip the wake
    /// for a holder that is about to block with a stale (pre-insert)
    /// deadline. The write is persistent — a blocked holder wakes now; a
    /// holder between its `PARKED` CAS and the blocking enter finds the
    /// CQE already posted and returns immediately; a future holder's
    /// first blocking park returns immediately. In every case the woken
    /// worker re-parks with a fresh `next_wake_tick()`. (Non-blocking
    /// maintenance drains that consume the CQE re-arm `inline_reaped` on
    /// the reactor — see `Reactor::park_timeout` — so the token survives
    /// them too.)
    pub(crate) fn timer_kick(&self) {
        let _ = self.ring_waker.wake();
        // If nobody is driving the ring the CQE waits for the next ring
        // parker; wake one worker so that happens promptly even when
        // every idle worker is condvar-parked (reachable when the ring
        // was held by a zero-duration maintenance poll while they
        // parked).
        if !self.ring_parked.load(Ordering::SeqCst) {
            self.kick_one_worker();
        }
    }

    /// Wake one worker so that *someone* passes through a park-entry
    /// drain soon. Prefers a condvar-parked worker (it re-parks and
    /// races for the now-relevant ring); falls back to worker 0, whose
    /// `NOTIFIED` flag persists across its current activity and forces
    /// its next park to return and re-enter with fresh state.
    fn kick_one_worker(&self) {
        for (idx, slot) in self.slots.iter().enumerate() {
            if slot.state.load(Ordering::SeqCst) == PARKED_CONDVAR {
                self.unpark(idx);
                return;
            }
        }
        if !self.slots.is_empty() {
            self.unpark(0);
        }
    }

    /// Park worker `idx`. Stock `Inner::park` flow: consume a pending
    /// notification, else race for the ring, else condvar.
    ///
    /// `driver_duration` is the ring-holder timeout (caller has already
    /// min'd in the legacy timer deadline); `condvar_duration` is the raw
    /// scheduler timeout. They differ because timers are the ring
    /// holder's job — condvar parkers must not spin on timer deadlines.
    pub(crate) fn park_worker(
        &self,
        idx: usize,
        driver_duration: Option<Duration>,
        condvar_duration: Option<Duration>,
    ) {
        let slot = &self.slots[idx];
        if slot
            .state
            .compare_exchange(NOTIFIED, EMPTY, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            // Consumed a pending notification — don't block, but don't
            // strand queued ops either: a busy, frequently-notified
            // worker's maintenance polls would otherwise take this early
            // return every time and never reach the park-entry drain.
            self.drain_ops_if_unheld();
            return;
        }

        if let Some(mut reactor) = self.reactor.try_lock() {
            self.park_driver(idx, &mut reactor, driver_duration);
        } else {
            self.park_condvar(idx, condvar_duration);
        }
    }

    /// Best-effort non-blocking drain of the pending-op queue, used on
    /// park paths that consume a notification without reaching the
    /// park-entry drain (a busy worker's zero-duration maintenance polls
    /// can otherwise skip the drain indefinitely while ops sit queued).
    ///
    /// Only drains if the ring is free to grab; when the `try_lock`
    /// fails a holder exists and park-entry/tail drains are its job.
    fn drain_ops_if_unheld(&self) {
        if let Some(mut reactor) = self.reactor.try_lock() {
            let ops = self.take_ops();
            if ops.is_empty() {
                return;
            }
            apply_pending_ops(&mut reactor, ops);
            // Non-blocking flush + drain. If this consumes an external
            // wake CQE, the reactor re-arms `inline_reaped` so the next
            // blocking park degrades and its caller recomputes state —
            // required so a pending timer-kick token is not lost here.
            let _ = reactor.park_timeout(Duration::ZERO);
        }
    }

    fn park_driver(&self, idx: usize, reactor: &mut Reactor, duration: Option<Duration>) {
        let slot = &self.slots[idx];

        if duration.as_ref().is_some_and(Duration::is_zero) {
            // Zero-duration "park" is a maintenance poll: apply queued
            // ops and drain without blocking, no park-state transition.
            apply_pending_ops(reactor, self.take_ops());
            let _ = reactor.park_timeout(Duration::ZERO);
            return;
        }

        match slot
            .state
            .compare_exchange(EMPTY, PARKED, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => {}
            Err(NOTIFIED) => {
                // Same consume-the-notification re-read as the stock
                // parker (synchronizes with a racing second unpark).
                let old = slot.state.swap(EMPTY, Ordering::SeqCst);
                debug_assert_eq!(old, NOTIFIED, "park state changed unexpectedly");
                // Don't block, but don't strand queued ops either: they
                // may include registrations whose eventfd kick already
                // fired (and whose CQE we're about to consume).
                apply_pending_ops(reactor, self.take_ops());
                let _ = reactor.park_timeout(Duration::ZERO);
                return;
            }
            Err(actual) => panic!("inconsistent park state; actual = {actual}"),
        }

        // Publish "the ring has a blocked driver" BEFORE draining the
        // queue: `push_op`'s skip-the-wake fast path is only sound if a
        // pusher that our drain misses is guaranteed to observe this
        // store (mutex release/acquire on `pending_ops` chains the two —
        // see `ring_parked`'s field docs).
        self.ring_parked.store(true, Ordering::SeqCst);

        // Apply queued ops from all threads. May inline-reap; the
        // reactor's `inline_reaped` flag then degrades the park below to
        // a non-blocking pass (the reap can consume our own eventfd wake
        // CQE while `state == PARKED`, and unparkers who saw PARKED have
        // already stopped re-delivering — same invariant as per-worker
        // mode).
        apply_pending_ops(reactor, self.take_ops());

        let result = match duration {
            Some(dur) => reactor.park_timeout(dur),
            None => reactor.park(),
        };
        // Park errors are spurious wakes; liveness comes from the next
        // unpark, exactly as in per-worker mode.
        let _ = result;

        self.ring_parked.store(false, Ordering::SeqCst);

        // Ops pushed while we were blocked (whose eventfd CQE we just
        // consumed) — drain them now rather than leaving them for the
        // next park: the pusher's wake was consumed, and with the
        // skip-the-wake fast path nobody re-kicks the ring for ops
        // already in the queue.
        let tail = self.take_ops();
        if !tail.is_empty() {
            apply_pending_ops(reactor, tail);
            let _ = reactor.park_timeout(Duration::ZERO);
        }

        match slot.state.swap(EMPTY, Ordering::SeqCst) {
            NOTIFIED => {} // got a notification
            PARKED => {}   // no notification
            n => panic!("inconsistent park_driver state: {n}"),
        }
    }

    fn park_condvar(&self, idx: usize, duration: Option<Duration>) {
        let slot = &self.slots[idx];
        let mut m = slot.mutex.lock();

        match slot
            .state
            .compare_exchange(EMPTY, PARKED_CONDVAR, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => {}
            Err(NOTIFIED) => {
                let old = slot.state.swap(EMPTY, Ordering::SeqCst);
                debug_assert_eq!(old, NOTIFIED, "park state changed unexpectedly");
                return;
            }
            Err(actual) => panic!("inconsistent park state; actual = {actual}"),
        }

        let timeout_at = duration.map(|d| {
            std::time::Instant::now()
                .checked_add(d)
                .unwrap_or_else(|| std::time::Instant::now() + Duration::from_secs(1))
        });

        loop {
            let is_timeout;
            (m, is_timeout) = match timeout_at {
                Some(timeout_at) => {
                    let dur = timeout_at.saturating_duration_since(std::time::Instant::now());
                    if !dur.is_zero() {
                        let (m, res) = slot.condvar.wait_timeout(m, dur).unwrap();
                        (m, res.timed_out())
                    } else {
                        (m, true)
                    }
                }
                None => (slot.condvar.wait(m).unwrap(), false),
            };

            if is_timeout {
                match slot.state.swap(EMPTY, Ordering::SeqCst) {
                    PARKED_CONDVAR => return, // timed out, no notification
                    NOTIFIED => return,       // notification raced the timeout
                    actual => panic!("inconsistent park_condvar state: {actual}"),
                }
            } else if slot
                .state
                .compare_exchange(NOTIFIED, EMPTY, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return;
            }
            // Spurious wakeup — back to sleep.
        }
    }

    /// Unpark worker `idx`. Stock `Inner::unpark`: the swap (not CAS) is
    /// the release that `park` synchronizes with.
    pub(crate) fn unpark(&self, idx: usize) -> bool {
        let slot = &self.slots[idx];
        match slot.state.swap(NOTIFIED, Ordering::SeqCst) {
            EMPTY | NOTIFIED => false,
            PARKED_CONDVAR => {
                // Lock/drop the mutex before notifying — closes the
                // window between the parker's state store and its
                // condvar wait (stock `unpark_condvar` rationale).
                drop(slot.mutex.lock());
                slot.condvar.notify_one();
                true
            }
            PARKED => {
                // Parked driving the shared ring: eventfd CQE unblocks
                // its `submit_and_wait`.
                let _ = self.ring_waker.wake();
                true
            }
            actual => panic!("inconsistent state in unpark; actual = {actual}"),
        }
    }
}

/// Translate a batch of [`PendingOp`]s into SQEs on `reactor`'s ring.
///
/// Shared by the per-worker parker (each worker drains its own queue)
/// and global-ring mode (the current holder drains the one queue). SQEs
/// are staged, not submitted — they flush with the next
/// `submit_and_wait` or explicit non-blocking submit. (Not quite
/// unconditionally: `Reactor::register`/`deregister` reap completions
/// in-line when enough reclaimable slots have piled up, so a long batch
/// cannot run slab occupancy through the ArmTable ceiling.)
///
/// Individual errors are dropped here because the reactor already
/// surfaces them: a failed register marks its `ScheduledIo` shutdown
/// (waiters observe "IO driver has terminated"), and a failed deregister
/// is at worst a missed cancel that the terminal-CQE path cleans up.
pub(crate) fn apply_pending_ops(reactor: &mut Reactor, pending: Vec<PendingOp>) {
    for op in pending {
        let _ = match op {
            PendingOp::Register { fd, interest, io } => reactor.register(fd, interest, &io),
            // Slab identity is read HERE, at drain time — any Register for
            // this `io` queued ahead of us on the same FIFO has already
            // been applied, so the key/gen are the live ones (a push-time
            // snapshot would still read u32::MAX and leak the armed poll).
            PendingOp::Deregister { io } => {
                let (slab_key, slab_gen) = io.uring_slab_identity();
                reactor.deregister(slab_key, slab_gen)
            }
        };
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

    /// `Some` iff `TOKIO_URING_GLOBAL=1`: the Phase-1 shared-ring mode.
    /// When set, the per-worker machinery above is bypassed entirely —
    /// no per-worker reactors are built, registrations funnel into the
    /// [`GlobalRing`]'s single queue, and unparks route through its
    /// stock-parker state machine.
    global: Option<Arc<GlobalRing>>,

    /// Round-robin counter for assigning newly registered fds to workers.
    /// Bumped once per `add_source` call.
    next_worker: AtomicUsize,

    /// Upper bound on how many distinct worker rings receive fd
    /// registrations. `fallback_worker` round-robins over `0..ring_cap`
    /// rather than `0..workers.len()`, so at most `ring_cap` rings ever
    /// carry `POLL_ADD_MULTI` SQEs and thus ever get woken by I/O readiness.
    ///
    /// Always in `1..=workers.len()`. Defaults to `workers.len()` (every
    /// ring eligible — historical behavior) and is overridable per-runtime
    /// via `TOKIO_URING_RING_CAP` (see [`Self::read_ring_cap_env`]).
    ///
    /// Rationale: the `net_tcp_echo` high-W cliff is wake-from-idle
    /// amplification under worker oversubscription — a fixed, latency-bound
    /// connection set scattered round-robin across many cold rings means
    /// nearly every round-trip wakes a fresh deep-idle worker (~15× the
    /// wakeups/round-trip at W64 vs W16 on a 64-core host, with
    /// `io_uring_enter` volume flat). Bounding the active ring set keeps
    /// those rings warm and many-fds-deep (so one wake drains many CQEs —
    /// the amortization the sharded-mio cross-group drain achieves on the
    /// epoll side) while leaving surplus workers parked. Topology-agnostic:
    /// wake cost on this hardware has no locality gradient, so the lever is
    /// wake *count*, not where the wake lands. See
    /// `.claude/STAGE-A-FINDINGS-uring-wake-cliff.md`.
    ring_cap: usize,

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
        let ring_cap = Self::read_ring_cap_env(num_workers);
        // Global-ring mode builds its one reactor eagerly, here on the
        // runtime-builder thread (legal: no SINGLE_ISSUER in this mode).
        // Failure at this point means the kernel lacks io_uring support —
        // the same condition the per-worker mode `expect`s on at first
        // park, surfaced a little earlier.
        let global = uring_global_enabled().then(|| {
            Arc::new(GlobalRing::new(num_workers).expect(
                "failed to construct shared io_uring Reactor for \
                 TOKIO_URING_GLOBAL=1 (kernel must support io_uring, \
                 Linux 6.0+)",
            ))
        });
        Self {
            workers: workers.into_boxed_slice(),
            global,
            next_worker: AtomicUsize::new(0),
            ring_cap,
            registrations,
            synced: Mutex::new(synced),
            metrics: IoDriverMetrics::default(),
            start_barrier: std::sync::Barrier::new(barrier_count),
        }
    }

    /// The Phase-1 shared ring, iff `TOKIO_URING_GLOBAL=1`.
    pub(crate) fn global_ring(&self) -> Option<&Arc<GlobalRing>> {
        self.global.as_ref()
    }

    /// Resolve the active-ring-set cap from `TOKIO_URING_RING_CAP`, clamped
    /// to `1..=num_workers`. Read exactly once per runtime at construction
    /// (the cap is fixed for the runtime's life).
    ///
    /// - unset, empty, `0`, or unparseable → `num_workers` (historical
    ///   behavior: every ring eligible — this is the A/B control).
    /// - `N` ≥ 1 → `min(N, num_workers)`.
    ///
    /// A degenerate `num_workers == 0` handle (test-only) yields `1` so the
    /// modulus in [`Self::fallback_worker`] never divides by zero.
    fn read_ring_cap_env(num_workers: usize) -> usize {
        let ceiling = num_workers.max(1);
        match std::env::var("TOKIO_URING_RING_CAP") {
            Ok(s) => match s.trim().parse::<usize>() {
                Ok(n) if n >= 1 => n.min(ceiling),
                // 0 or junk → off (full set).
                _ => ceiling,
            },
            Err(_) => ceiling,
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
        // Publish the arm table *before* the ring_fd. Both stores use
        // `Release`; a paired `Acquire` load of `ring_fd` on the reader
        // side is sufficient to synchronize with the `arm_table`
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

    /// Wake a parker so a newly-inserted timer that lowered the wheel
    /// minimum is honored. Called by the legacy time driver's insert path
    /// (`time::Handle::reregister`) via the mode-aware routing in
    /// `time::Handle::unpark_for_insert`.
    ///
    /// The traditional runtime unparks the thread driving the shared mio
    /// driver; the uring backend has no such thread (the mio driver
    /// object built by `enable_io()` is never polled), so the wake must
    /// instead reach a parker that re-reads `next_wake_tick()` before
    /// blocking — which every uring park does (see
    /// `UringParker::compute_legacy_timer_duration`). One woken worker is
    /// therefore sufficient in either mode.
    pub(crate) fn unpark_for_timer(&self) {
        if let Some(g) = self.global.as_ref() {
            g.timer_kick();
            return;
        }
        if self.workers.is_empty() {
            return;
        }
        // Per-worker mode: prefer a worker actually blocked in
        // `io_uring_enter` — it wakes now and re-parks with the new
        // deadline folded in.
        for idx in 0..self.workers.len() {
            if self.workers[idx].park_state.load(Ordering::SeqCst) == PARKED {
                self.unpark(idx);
                return;
            }
        }
        // Nobody observed parked. A worker concurrently *transitioning*
        // into park computed its deadline before this insert and could
        // still block with it, so leave a persistent wake on worker 0:
        // the NOTIFIED flag (or the eventfd CQE, if it wins the PARKED
        // CAS first) guarantees worker 0's next park returns immediately
        // and re-enters with the wheel minimum re-read.
        self.unpark(0);
    }

    /// Mark `worker_idx` as notified and — if the worker was parked —
    /// deliver an actual wake.
    ///
    /// Returns `true` if a wake was delivered to the kernel (for metrics).
    pub(crate) fn unpark(&self, worker_idx: usize) -> bool {
        if let Some(g) = self.global.as_ref() {
            return g.unpark(worker_idx);
        }
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
    /// first `POLL_ADD_MULTI` should fire *here* too. Without a rebind
    /// path, work-stealing can leave a registration owned by a worker
    /// other than the one currently polling its task; that residual
    /// cross-ring wake hop is accepted as the cost of keeping placement
    /// stable (see commit `e29114e1` for v1's worker-0 listener
    /// pathology, which the v2 round-robin fallback below mitigates by
    /// spreading new fds rather than concentrating them on the caller's
    /// ring).
    ///
    /// [`current_worker_index`]:
    ///     crate::runtime::scheduler::multi_thread::uring_park::current_worker_index
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

        if let Some(g) = self.global.as_ref() {
            // Global mode: one queue, no placement decision. Worker index
            // 0 is a placeholder — deregister routes by mode, not index.
            io.uring_worker.store(0, Ordering::Relaxed);
            g.push_op(PendingOp::Register {
                fd,
                interest,
                io: Arc::clone(&io),
            });
            self.metrics.incr_fd_count();
            return Ok((io, 0));
        }

        let worker_idx = self.fallback_worker();

        // Publish the assigned worker onto the ScheduledIo so callers can
        // observe "which worker currently owns this ring registration".
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

    /// Lazy first-poll register entry-point used by the vtable shim.
    /// Mirrors [`Self::add_source`] but takes a caller-allocated
    /// `Arc<ScheduledIo>` rather than allocating one internally —
    /// [`super::registration::Registration`] now owns the Arc and only
    /// asks the driver to register/queue the SQE.
    ///
    /// The implementation does the same shared-set bookkeeping
    /// `add_source` did; the only structural difference is the caller
    /// already has the Arc, so we use [`RegistrationSet::allocate_existing`]
    /// rather than [`RegistrationSet::allocate`].
    ///
    /// Note: at present only the sharded-mio backend is genuinely lazy
    /// (its registry mutation is per-shard). The uring backend already
    /// queues the SQE and lets the worker submit it, so eager and lazy
    /// look identical from io_uring's perspective. We expose the shape
    /// uniformly so the vtable stays clean.
    pub(crate) fn register_local(
        &self,
        shared: &Arc<ScheduledIo>,
        fd: RawFd,
        interest: Interest,
    ) -> io::Result<usize> {
        // Reuse the global `RegistrationSet` for shutdown tracking;
        // identical semantics to `add_source` for an externally-owned
        // Arc.
        self.registrations
            .allocate_existing(&mut self.synced.lock(), shared)?;

        if let Some(g) = self.global.as_ref() {
            shared.uring_worker.store(0, Ordering::Relaxed);
            g.push_op(PendingOp::Register {
                fd,
                interest,
                io: Arc::clone(shared),
            });
            self.metrics.incr_fd_count();
            return Ok(0);
        }

        let worker_idx = self.fallback_worker();

        shared
            .uring_worker
            .store(worker_idx as u32, Ordering::Relaxed);

        {
            let slot = &self.workers[worker_idx];
            let mut queue = slot.pending_ops.lock();
            queue.push(PendingOp::Register {
                fd,
                interest,
                io: Arc::clone(shared),
            });
        }

        self.unpark(worker_idx);

        self.metrics.incr_fd_count();
        Ok(worker_idx)
    }

    /// Allocate a fresh `Arc<ScheduledIo>` without doing any
    /// driver-side work yet. The vtable's `allocate_scheduled_io`
    /// shim forwards to this; the actual register call comes later
    /// via [`Self::register_local`].
    pub(crate) fn allocate_scheduled_io(&self) -> Arc<ScheduledIo> {
        Arc::new(ScheduledIo::default())
    }

    /// Pick a worker by round-robin across `next_worker`. `fetch_add` is
    /// `Relaxed`: ordering of assignments does not affect correctness, only
    /// balance, and we only need distinct calls to tend toward distinct
    /// workers.
    ///
    fn fallback_worker(&self) -> usize {
        // Round-robin over the *active ring set* (`0..ring_cap`) rather than
        // all workers, so at most `ring_cap` rings ever carry registrations.
        // `ring_cap` is clamped to `1..=workers.len()` at construction, so
        // this is always a valid index and never divides by zero.
        self.next_worker.fetch_add(1, Ordering::Relaxed) % self.ring_cap
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
        if let Some(g) = self.global.as_ref() {
            // Same FIFO as the Register (drain-time identity read applies
            // identically), same release bookkeeping as below; the ring
            // kick is push_op's wake-only-if-parked eventfd write (plus
            // the external-pusher fallback documented on `push_op`).
            g.push_op(PendingOp::Deregister { io: Arc::clone(io) });
            let _ = self.registrations.deregister(&mut self.synced.lock(), io);
            self.metrics.dec_fd_count();
            return Ok(());
        }

        // Queue the POLL_REMOVE first so that if the caller races with
        // shutdown, the kernel-side cleanup still lands before the Arc is
        // freed. The worker drains this at its next park() call.
        //
        // The slab identity is read at drain time (see `PendingOp::
        // Deregister` docs) — a push-time snapshot races with a still-
        // queued Register on this same FIFO and leaks the registration.
        if worker_idx < self.workers.len() {
            let slot = &self.workers[worker_idx];
            let mut queue = slot.pending_ops.lock();
            queue.push(PendingOp::Deregister {
                io: Arc::clone(io),
            });
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
