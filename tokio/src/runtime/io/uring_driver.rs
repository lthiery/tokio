//! Shared handle for the single-ring uring reactor backend.
//!
//! There is one [`Reactor`] for the whole runtime, driven by whichever
//! worker parks first (holder rotation, the stock mio `Parker`
//! discipline). [`UringHandle`] is the shared half: it owns the
//! [`GlobalRing`] (the shared reactor plus its per-worker park slots and
//! op queue) and the fd → `ScheduledIo` registration set, so that any
//! thread can register fds, deregister them, and unpark whichever worker
//! is driving the ring.
//!
//! # Unpark routing
//!
//! `unpark(target)` flips the target park slot to `NOTIFIED`. If the
//! target was blocked driving the ring, an eventfd write (registered on
//! the ring with `POLL_ADD_MULTI`) fires a CQE and unblocks it; if it was
//! condvar-parked (another worker holds the ring), the condvar is
//! notified. See [`GlobalRing`] for the state machine.

use std::cell::Cell;
use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::io::interest::Interest;
use crate::loom::sync::{Condvar, Mutex};
use crate::runtime::io::registration_set;
use crate::runtime::io::uring_reactor::{ExternalWaker, Reactor};
use crate::runtime::io::{IoDriverMetrics, RegistrationSet, ScheduledIo};
use crate::util::TryLock;

use std::time::Duration;

/// Park-state atomic values. Shape mirrors the mio parker's transitions so
/// integration stays familiar.
pub(crate) const EMPTY: usize = 0;
pub(crate) const PARKED: usize = 1;
pub(crate) const NOTIFIED: usize = 2;
/// Parked on the per-worker condvar because another worker holds the
/// shared ring. Mirrors the stock parker's `PARKED_CONDVAR`; [`PARKED`]
/// plays the stock `PARKED_DRIVER` role (this worker is driving the ring).
pub(crate) const PARKED_CONDVAR: usize = 3;

/// An operation that needs to be submitted on the shared ring.
///
/// fd (de)registration goes through this queue because
/// `IORING_OP_POLL_ADD` / `POLL_REMOVE` must be submitted on the one ring
/// by whichever worker is currently holding (driving) it. Any thread,
/// worker or external, can push ops here; only the current holder drains.
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


/// Per-worker park slot. The stock parker's `Inner`
/// state machine, minus the driver reference (the shared ring lives on
/// [`GlobalRing`], not per worker).
struct GlobalParkSlot {
    /// `EMPTY | PARKED | NOTIFIED | PARKED_CONDVAR`, stock SeqCst
    /// discipline throughout.
    state: AtomicUsize,
    mutex: Mutex<()>,
    condvar: Condvar,
}

/// The shared ring: ONE `io_uring` for the whole runtime, driven by
/// whichever worker parks first — the stock mio `Parker` discipline with
/// the uring [`Reactor`] in the driver seat.
///
/// Rotation is legal because the reactor's ring is built without
/// `SINGLE_ISSUER` / `DEFER_TASKRUN`: any thread may drive it, one at a
/// time, serialized by the [`TryLock`].
pub(crate) struct GlobalRing {
    /// The one shared reactor. Whoever `try_lock`s it parks on the ring
    /// and drains completions; everyone else condvar-parks.
    reactor: TryLock<Reactor>,

    /// Single pending-op queue for fd (de)registration. Applied by the
    /// ring holder at park-entry; the backpressure (zombie reap +
    /// `inline_reaped` park degradation) rides along.
    pending_ops: Mutex<Vec<PendingOp>>,

    /// Wakes whatever thread is blocked in `submit_and_wait` on the
    /// shared ring. Cloned from the reactor at construction. The eventfd
    /// is the sole wake mechanism (there is only one ring).
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
    /// thread, which is legal because the ring is built without
    /// `SINGLE_ISSUER`, so it is not bound to its constructing thread and
    /// any worker can later drive it.
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
        } else if current_worker_index().is_none() {
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

    /// Wake one worker, but only when the ring is genuinely abandoned:
    /// EVERY worker is condvar-parked. That is the one state in which
    /// nobody will pass through a park-entry drain on their own — a
    /// condvar parker re-parks only when woken, and with the ring unheld
    /// there is no holder to drain for them.
    ///
    /// If ANY worker is awake (running, searching, or mid-transition),
    /// kicking buys nothing: an awake worker's next park either wins the
    /// free ring — draining the queue at park-entry — or condvar-parks
    /// because a holder exists, and every holder path from awake back to
    /// blocking re-drains (the `ring_parked` ordering argument). The old
    /// unconditional kick (wake first condvar parker, else `unpark(0)`)
    /// fired on virtually every external push in steady state and cost
    /// +35..85% on global `tcp_register_dereg` at W2–W8 — the `unpark(0)`
    /// fallback in particular only forced a spurious NOTIFIED pass on a
    /// worker whose next park-entry would have drained anyway.
    ///
    /// Check-then-act races are benign: a worker that wakes after the
    /// scan saw it condvar-parked makes the kick redundant (harmless); a
    /// worker the scan saw awake cannot reach condvar-park without the
    /// ring being held (park-entry wins a free ring), and a holder drains
    /// before blocking.
    fn kick_one_worker(&self) {
        let mut first_condvar = None;
        for (idx, slot) in self.slots.iter().enumerate() {
            if slot.state.load(Ordering::SeqCst) == PARKED_CONDVAR {
                if first_condvar.is_none() {
                    first_condvar = Some(idx);
                }
            } else {
                // Someone is awake or ring-parked — they'll drain.
                return;
            }
        }
        if let Some(idx) = first_condvar {
            self.unpark(idx);
        }
    }

    /// Park worker `idx`. Stock `Inner::park` flow: consume a pending
    /// notification, else race for the ring, else condvar.
    ///
    /// `driver_duration` is the ring-holder timeout (caller has already
    /// min'd in the legacy timer deadline); `condvar_duration` is the raw
    /// scheduler timeout. They differ because timers are the ring
    /// holder's job — condvar parkers must not spin on timer deadlines.
    ///
    /// Returns `true` iff this worker won the ring and drove it (the
    /// stock parker's `HadDriver` distinction).
    pub(crate) fn park_worker(
        &self,
        idx: usize,
        driver_duration: Option<Duration>,
        condvar_duration: Option<Duration>,
    ) -> bool {
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
            return false;
        }

        if let Some(mut reactor) = self.reactor.try_lock() {
            self.park_driver(idx, &mut reactor, driver_duration);
            true
        } else {
            self.park_condvar(idx, condvar_duration);
            false
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
        // already stopped re-delivering).
        apply_pending_ops(reactor, self.take_ops());

        let result = match duration {
            Some(dur) => reactor.park_timeout(dur),
            None => reactor.park(),
        };
        // Park errors are spurious wakes; liveness comes from the next
        // unpark.
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
/// The current holder of the shared ring drains the one queue. SQEs
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

/// Hybrid park flow helper: legacy timer + uring I/O.
///
/// When a uring-backed runtime is built without `enable_alt_timer()` (the
/// default), rings share the legacy single-mutex timer wheel. To make
/// sleeps fire on time, whoever is about to drive a ring computes its
/// `io_uring_enter` timeout as `min(scheduler_timeout,
/// time_until_next_timer)`.
///
/// Flavor-agnostic free function (formerly a `UringParker` method) so the
/// current_thread park path — which cannot reach the
/// `rt-multi-thread`-gated parker module — shares one copy.
pub(crate) fn compute_legacy_timer_duration(
    driver: &crate::runtime::driver::Handle,
    scheduler_timeout: Option<Duration>,
) -> Option<Duration> {
    #[cfg(feature = "time")]
    if let Some(time_handle) = driver.time_handle_opt() {
        if time_handle.is_traditional() {
            if let Some(when) = time_handle.next_wake_tick() {
                let now = time_handle.time_source().now(driver.clock());
                let time_dur = time_handle
                    .time_source()
                    .tick_to_duration(when.saturating_sub(now));
                return Some(match scheduler_timeout {
                    Some(s) => std::cmp::min(s, time_dur),
                    None => time_dur,
                });
            }
        }
    }
    scheduler_timeout
}

/// Mirror of [`compute_legacy_timer_duration`] for the post-park path:
/// process expired timers under the legacy flavor.
pub(crate) fn process_legacy_timer_after_park(driver: &crate::runtime::driver::Handle) {
    #[cfg(feature = "time")]
    if let Some(time_handle) = driver.time_handle_opt() {
        if time_handle.is_traditional() {
            time_handle.parker_process(driver.clock());
        }
    }
}

/// Shared I/O handle for the uring-reactor backend.
///
/// The Handle-side analog of the mio driver's [`Handle`]. Holds per-worker
/// park slots used for cross-thread and external unparking, plus the shared
/// registration set and metrics (reused wholesale from the mio side — they
/// are backend-agnostic).
///
/// [`Handle`]: super::driver::Handle
pub(crate) struct UringHandle {
    /// The one shared io_uring reactor for the whole runtime, driven by
    /// whichever worker parks first (holder rotation). Registrations
    /// funnel into its single queue and unparks route through its
    /// stock-parker state machine.
    global: Arc<GlobalRing>,

    /// Shared registration set (fd → ScheduledIo). Identical to the mio
    /// driver's usage; the Arc-pinned `ScheduledIo` instances are also
    /// referenced from the reactor's slab while a registration is live.
    pub(super) registrations: RegistrationSet,
    pub(super) synced: Mutex<registration_set::Synced>,

    pub(crate) metrics: IoDriverMetrics,

    /// Number of workers this handle serves. Fixed at construction.
    num_workers: usize,

    /// Per-runtime startup barrier. Every worker waits here before
    /// entering the scheduler loop, so that task polling on all workers
    /// begins at the same wall-clock moment.
    ///
    /// Why: without this barrier, cold-start differentials between workers
    /// produce observable clock skew in tests that measure durations
    /// across workers. See the analysis in `tcp_read_blocks_then_wakes` —
    /// the server's sleep clock would start before the client's
    /// `Instant::now`, making `elapsed` under-estimate the real sleep
    /// duration. The barrier collapses that window to (approximately) the
    /// monotonic clock's resolution.
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
            .field("num_workers", &self.num_workers)
            .finish_non_exhaustive()
    }
}

impl UringHandle {
    /// Construct the handle and its one shared reactor. Used by both the
    /// multi-thread scheduler (`num_workers` = worker count) and the
    /// current_thread scheduler (`num_workers` = 1).
    pub(crate) fn new(num_workers: usize) -> Self {
        let (registrations, synced) = RegistrationSet::new();
        // Barrier must have at least 1 participant; a handle with zero
        // workers is degenerate but we keep it constructible for tests.
        let barrier_count = num_workers.max(1);
        // Build the one reactor eagerly, here on the runtime-builder
        // thread (legal: the ring is not submitter-bound). Failure means
        // the kernel lacks io_uring support.
        let global = Arc::new(GlobalRing::new(num_workers).expect(
            "failed to construct the shared io_uring Reactor \
             (kernel must support io_uring, Linux 6.0+)",
        ));
        Self {
            global,
            registrations,
            synced: Mutex::new(synced),
            metrics: IoDriverMetrics::default(),
            num_workers,
            start_barrier: std::sync::Barrier::new(barrier_count),
        }
    }

    /// The one shared ring.
    pub(crate) fn global_ring(&self) -> &Arc<GlobalRing> {
        &self.global
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
        self.num_workers
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
        self.global.timer_kick();
    }

    /// Mark `worker_idx` as notified and — if the worker was parked —
    /// deliver an actual wake.
    ///
    /// Returns `true` if a wake was delivered to the kernel (for metrics).
    pub(crate) fn unpark(&self, worker_idx: usize) -> bool {
        self.global.unpark(worker_idx)
    }

    /// Register a raw fd for readiness notifications.
    ///
    /// Takes a caller-allocated `Arc<ScheduledIo>`
    /// ([`super::registration::Registration`] owns the Arc) and queues a
    /// [`PendingOp::Register`] onto the one shared ring's op queue. The
    /// ring holder submits the actual `POLL_ADD_MULTI` SQE at its next
    /// park-entry drain; [`GlobalRing::push_op`] kicks the ring when
    /// needed so that happens promptly.
    ///
    /// Returns a worker index placeholder (`0`) kept for signature
    /// compatibility with the vtable and the legacy driver; there is only
    /// one ring, so the index carries no routing meaning.
    pub(crate) fn register_local(
        &self,
        shared: &Arc<ScheduledIo>,
        fd: RawFd,
        interest: Interest,
    ) -> io::Result<usize> {
        self.registrations
            .allocate_existing(&mut self.synced.lock(), shared)?;

        shared.uring_worker.store(0, Ordering::Relaxed);
        self.global.push_op(PendingOp::Register {
            fd,
            interest,
            io: Arc::clone(shared),
        });
        self.metrics.incr_fd_count();
        Ok(0)
    }

    /// Allocate a fresh `Arc<ScheduledIo>` without doing any
    /// driver-side work yet. The vtable's `allocate_scheduled_io`
    /// shim forwards to this; the actual register call comes later
    /// via [`Self::register_local`].
    pub(crate) fn allocate_scheduled_io(&self) -> Arc<ScheduledIo> {
        Arc::new(ScheduledIo::default())
    }

    /// Queue a `POLL_REMOVE` for `io` onto the shared ring's op queue.
    ///
    /// The `worker_idx` argument is ignored (there is one ring); it is
    /// kept for signature compatibility with the vtable and the legacy
    /// driver.
    ///
    /// This also enqueues `io` for release from the shared
    /// [`RegistrationSet`] (matching the mio driver's semantics: the Arc
    /// hangs around in `pending_release` until the driver's next cleanup
    /// pass).
    pub(crate) fn deregister_source(
        &self,
        io: &Arc<ScheduledIo>,
        _worker_idx: usize,
    ) -> io::Result<()> {
        // Same FIFO as the Register (drain-time identity read applies
        // identically). The ring kick is push_op's wake-only-if-parked
        // eventfd write (plus the external-pusher fallback documented on
        // `push_op`).
        self.global.push_op(PendingOp::Deregister { io: Arc::clone(io) });
        let _ = self.registrations.deregister(&mut self.synced.lock(), io);
        self.metrics.dec_fd_count();
        Ok(())
    }

    /// Release any `ScheduledIo`s that have been queued for removal by
    /// [`RegistrationSet::deregister`]. Called by the ring holder after
    /// submitting any pending `POLL_REMOVE` ops so the Arcs are freed on
    /// a worker thread.
    pub(crate) fn release_pending_registrations(&self) {
        if self.registrations.needs_release() {
            self.registrations.release(&mut self.synced.lock());
        }
    }
}

// ===== CURRENT_WORKER thread-local =====

thread_local! {
    /// Current worker's index for the running thread, or `None` if this
    /// thread is not currently executing a uring worker loop (multi-thread
    /// scheduler) or a core-holding `block_on` (current_thread — planned).
    ///
    /// Lives here rather than in `multi_thread::uring_park` (its original
    /// home) because [`GlobalRing::push_op`] consults it and this module
    /// is compiled for `rt`-only builds where `multi_thread` does not
    /// exist. The multi-thread parker re-exports the accessors.
    ///
    /// Set by the multi-thread worker entry point / first park and cleared
    /// on worker exit (see `uring_park.rs` for the pairing discipline).
    ///
    /// Used by [`UringHandle::add_source`]-adjacent placement decisions
    /// and by [`GlobalRing::push_op`] to tell worker pushers (their own
    /// next park drains the queue) from external pushers (may need a
    /// kick). `None` means the caller is not on a worker thread.
    ///
    /// This is just the integer index consulted by
    /// [`GlobalRing::push_op`].
    static CURRENT_WORKER: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Index of the worker currently executing on this thread, or `None` if
/// this thread is not a uring worker.
pub(crate) fn current_worker_index() -> Option<usize> {
    CURRENT_WORKER.with(Cell::get)
}

/// Publish `idx` as this thread's current worker index. Must be paired
/// with [`clear_current_worker`] before the worker loop exits.
pub(crate) fn set_current_worker(idx: usize) {
    CURRENT_WORKER.with(|c| c.set(Some(idx)));
}

/// Clear this thread's `CURRENT_WORKER` slot. Idempotent.
pub(crate) fn clear_current_worker() {
    CURRENT_WORKER.with(|c| c.set(None));
}

// ===== tests =====

#[cfg(test)]
mod tests {
    use super::*;

    /// The handle builds its one shared reactor and reports the worker
    /// count it was constructed with.
    #[test]
    fn new_builds_shared_ring() {
        let handle = UringHandle::new(4);
        assert_eq!(handle.num_workers(), 4);
    }

    /// `unpark` on a worker that is not parked is a no-op (returns false):
    /// there is nothing blocked in the kernel to wake.
    #[test]
    fn unpark_idle_worker_is_noop() {
        let handle = UringHandle::new(2);
        assert!(!handle.unpark(0), "no worker parked, so no wake fires");
    }
}
