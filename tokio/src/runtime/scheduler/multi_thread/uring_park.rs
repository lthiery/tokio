//! Per-worker `io_uring` parker for the multi-thread scheduler.
//!
//! Each worker thread owns a [`UringParker`]. Unlike the traditional [`Parker`]
//! (which shares an `IoStack` behind a `TryLock`), uring parkers are fully
//! independent: each worker parks on its own ring, and cross-worker wakes are
//! routed via `IORING_OP_MSG_RING`.
//!
//! # Lifecycle
//!
//! The [`Reactor`] is lazily constructed on the worker's first `park` call,
//! **on the worker thread itself**. This is required by
//! `IORING_SETUP_SINGLE_ISSUER`: the kernel binds the ring's submitter at the
//! first `io_uring_enter` call, so the ring must be constructed on the thread
//! that will drive it.
//!
//! Once constructed, the [`Reactor`]'s ring fd and [`ExternalWaker`] are
//! published into the shared [`UringHandle`] so other threads can target us.
//! The reactor itself is published via the `LOCAL_REACTOR` thread-local using
//! a [`LocalReactorGuard`]; this lets *other* workers route `MSG_RING` SQEs
//! through their own rings.
//!
//! # Timer integration
//!
//! Same hybrid park flow as [`super::sharded_mio_park`]. When the runtime is
//! built with `enable_uring_reactor()` but without `enable_alt_timer()` (the
//! default since the `rt-alt-timer` feature gate landed), each worker still
//! owns its own ring but shares the legacy single-mutex timer wheel. The
//! parker computes its `io_uring_enter` timeout as `min(scheduler_timeout,
//! time_until_next_timer)` via [`Self::compute_legacy_timer_duration`] before
//! parking, then advances the wheel via [`Self::process_legacy_timer_after_park`]
//! after wake. With `enable_alt_timer()` the hybrid hooks short-circuit on
//! `is_traditional() == false` and wheel processing is left to the alt-timer
//! hooks in [`super::worker::run`].
//!
//! [`Parker`]: super::park::Parker
//! [`Reactor`]: crate::runtime::io::uring_reactor::Reactor
//! [`ExternalWaker`]: crate::runtime::io::uring_reactor::ExternalWaker
//! [`UringHandle`]: crate::runtime::io::uring_driver::UringHandle
//! [`LocalReactorGuard`]: crate::runtime::io::uring_driver::LocalReactorGuard

use crate::loom::sync::Arc;
use crate::runtime::driver;
use crate::runtime::io::uring_driver::{
    clear_local_reactor, install_local_reactor_raw, PendingOp, UringHandle,
};
use crate::runtime::io::uring_reactor::Reactor;
use crate::runtime::scheduler::multi_thread::park::HadDriver;

use std::cell::{Cell, RefCell};
use std::time::Duration;

thread_local! {
    /// Current worker's index for the running thread, or `None` if this
    /// thread is not currently executing a multi-thread uring worker loop.
    ///
    /// Set by [`UringParker::ensure_reactor_installed`] on first park (the
    /// same point where `LOCAL_REACTOR` is installed) and cleared by
    /// [`UringParker::shutdown`] / `Drop`, plus the `ClearUringTls` RAII
    /// guard in `worker.rs` (belt-and-braces for recycled blocking-pool
    /// threads across runtimes).
    ///
    /// Used by [`UringHandle::add_source`] to place new fd registrations on
    /// the current worker's ring — preferring `W_ring == W_task` locality
    /// over round-robin load balance — and by [`UringUnparker::unpark`] to
    /// short-circuit self-wakes. `None` means the caller is not on a worker
    /// thread; callers must fall back to a policy that doesn't assume
    /// worker-local state.
    ///
    /// Distinct from `LOCAL_REACTOR`: that TLS points at this worker's
    /// `RefCell<Reactor>` (required for `MSG_RING` routing), while this one
    /// is just the integer index — sufficient for routing decisions that
    /// don't touch the reactor itself.
    static CURRENT_WORKER: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Index of the worker currently executing on this thread, or `None` if
/// this thread is not a multi-thread uring worker.
///
/// Currently no callers — `unpark` no longer self-short-circuits (see
/// [`UringUnparker::unpark`]) and `UringHandle::add_source` uses pure
/// round-robin placement. Kept around for the planned task-local
/// placement path documented on [`UringHandle::add_source`].
#[allow(dead_code)]
pub(crate) fn current_worker_index() -> Option<usize> {
    CURRENT_WORKER.with(Cell::get)
}

/// Publish `idx` as this thread's current worker index. Must be paired with
/// [`clear_current_worker`] before the worker loop exits.
fn set_current_worker(idx: usize) {
    CURRENT_WORKER.with(|c| c.set(Some(idx)));
}

/// Publish the current worker index from the worker's `run` entry point,
/// before any task executes on this thread. Separate from the parker-side
/// install because the parker runs its lazy init on the *first park*, which
/// is too late for tasks that register fds during the pre-park burst at
/// startup.
///
/// Paired with the `ClearUringTls` teardown guard in `worker.rs`, which
/// already calls [`clear_current_worker`] on worker exit.
pub(crate) fn set_current_worker_early(idx: usize) {
    set_current_worker(idx);
}

/// Clear this thread's `CURRENT_WORKER` slot. Idempotent.
pub(crate) fn clear_current_worker() {
    CURRENT_WORKER.with(|c| c.set(None));
}

/// Per-worker parker for the `io_uring` backend.
///
/// Each [`UringParker`] is distinct and non-cloneable — worker threads own
/// exclusive rings. Unparking is done via a separate [`UringUnparker`] that
/// shares the [`UringHandle`] and the worker index.
pub(crate) struct UringParker {
    /// Zero-based worker index. Matches the slot in [`UringHandle::workers`].
    idx: usize,

    /// Shared coordination handle.
    handle: Arc<UringHandle>,

    /// Lazily-constructed per-worker reactor.
    ///
    /// Wrapped in a [`Box`] for address stability — we install a
    /// `*const RefCell<Reactor>` into the `LOCAL_REACTOR` thread-local, and
    /// that pointer must remain valid until cleared. `Option` tracks the
    /// lazy-init state.
    ///
    /// Access discipline: the worker thread takes `.borrow_mut()` during
    /// `park`; other threads observing via `LOCAL_REACTOR` only do so from
    /// contexts where the owning worker is **not** inside its own park
    /// (either mid-task on a different worker, or inside the kernel via
    /// `io_uring_enter` on this worker). The two time-windows are strictly
    /// non-overlapping on the same thread, so `borrow_mut` never contends.
    reactor: Option<Box<RefCell<Reactor>>>,

    /// `true` once we have installed `reactor` into the thread-local
    /// `LOCAL_REACTOR` slot on the worker thread. Tracked separately from
    /// `reactor.is_some()` because the install happens on *the worker
    /// thread*, which is not necessarily the thread that constructed the
    /// `UringParker`. Clearing the TLS on `Drop` must only happen if we
    /// actually installed it here.
    tls_installed: bool,
}

/// Unparker counterpart to [`UringParker`]. Cheap to clone — just an `Arc`
/// and a worker index.
#[derive(Clone)]
pub(crate) struct UringUnparker {
    idx: usize,
    handle: Arc<UringHandle>,
}

impl UringParker {
    /// Construct a parker for worker `idx`. The reactor is not built yet;
    /// that happens on first `park` so it lands on the worker's own thread
    /// (required by `IORING_SETUP_SINGLE_ISSUER`).
    pub(crate) fn new(idx: usize, handle: Arc<UringHandle>) -> Self {
        Self {
            idx,
            handle,
            reactor: None,
            tls_installed: false,
        }
    }

    /// Cheap handle to wake this worker from another thread.
    pub(crate) fn unparker(&self) -> UringUnparker {
        UringUnparker {
            idx: self.idx,
            handle: Arc::clone(&self.handle),
        }
    }

    /// Shared [`UringHandle`] — used by `Handle::add_source` et al. to route
    /// fd registrations.
    #[allow(dead_code)]
    pub(crate) fn handle(&self) -> &Arc<UringHandle> {
        &self.handle
    }

    /// Park the worker until woken.
    pub(crate) fn park(&mut self, driver: &driver::Handle) -> HadDriver {
        let park_dur = self.compute_legacy_timer_duration(driver, None);
        self.park_internal(park_dur);
        self.process_legacy_timer_after_park(driver);
        HadDriver::Yes
    }

    /// Park with a maximum duration.
    pub(crate) fn park_timeout(
        &mut self,
        driver: &driver::Handle,
        duration: Duration,
    ) -> HadDriver {
        let park_dur = self.compute_legacy_timer_duration(driver, Some(duration));
        self.park_internal(park_dur);
        self.process_legacy_timer_after_park(driver);
        HadDriver::Yes
    }

    /// Hybrid park flow: legacy timer + uring I/O.
    ///
    /// Mirror of [`super::sharded_mio_park::ShardedMioParker::compute_legacy_timer_duration`].
    /// When the runtime is built with `enable_uring_reactor()` but without
    /// `enable_alt_timer()` (the default since the rt-alt-timer feature gate
    /// landed), each worker still owns its own `io_uring` ring but shares the
    /// legacy single-mutex timer wheel. To make sleeps fire on time, each
    /// parker has to compute its `io_uring_enter` timeout as
    /// `min(scheduler_timeout, time_until_next_timer)`.
    fn compute_legacy_timer_duration(
        &self,
        driver: &driver::Handle,
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

    /// Mirror of [`Self::compute_legacy_timer_duration`] for the post-park
    /// path: process expired timers under the legacy flavor.
    fn process_legacy_timer_after_park(&self, driver: &driver::Handle) {
        #[cfg(feature = "time")]
        if let Some(time_handle) = driver.time_handle_opt() {
            if time_handle.is_traditional() {
                time_handle.parker_process(driver.clock());
            }
        }
    }

    /// Shutdown the parker. Clears the TLS install first (un-publishing the
    /// reactor), then drops the reactor itself. Idempotent.
    ///
    /// Must be called on the worker thread. In practice the `Drop` impl
    /// also clears the TLS as a belt-and-braces measure.
    pub(crate) fn shutdown(&mut self, _driver: &driver::Handle) {
        if self.tls_installed {
            clear_local_reactor();
            clear_current_worker();
            self.tls_installed = false;
        }
        self.reactor.take();
    }

    fn park_internal(&mut self, duration: Option<Duration>) {
        // CRITICAL: publish ring_fd + external_waker *before* transitioning
        // `park_state` to PARKED. An unparker that observes `park_state ==
        // PARKED` will attempt to deliver a wake via those channels; if they
        // are not yet published, the wake is silently dropped and the park
        // below blocks forever.
        //
        // Lazy-init on this thread, the first time we park.
        self.ensure_reactor_installed();

        // Consume any pending notification without going to the kernel.
        // It is critical that this happens *after* `ensure_reactor_installed`
        // so that the two atomics (`ring_fd`, `external_waker`) are already
        // visible by the time `park_state == PARKED` is observable.
        if self.handle.begin_park(self.idx) {
            // Even on the NOTIFIED fast path, we drain any fd (de)register
            // ops that landed on our queue. Otherwise a burst of
            // `Registration::new` followed immediately by an unpark would
            // skip the kernel round-trip and leave `POLL_ADD_MULTI` SQEs
            // un-submitted until the next real park.
            self.drain_pending_ops_and_submit();
            return;
        }

        // `reactor` is Some after `ensure_reactor_installed`.
        let cell: &RefCell<Reactor> = self
            .reactor
            .as_deref()
            .expect("reactor installed");
        let mut reactor = cell.borrow_mut();

        // Apply any fd registration / deregistration ops that peer threads
        // queued for us. These become SQEs on our ring and flush together
        // with the park's `submit_and_wait`.
        let pending = self.handle.take_pending_ops(self.idx);
        apply_pending_ops(&mut reactor, pending);

        let result = match duration {
            None => reactor.park(),
            Some(dur) if dur.is_zero() => reactor.park_timeout(Duration::ZERO),
            Some(dur) => reactor.park_timeout(dur),
        };

        // Park errors are treated as spurious — correctness does not depend
        // on them, just liveness (another unpark will arrive).
        let _ = result;

        drop(reactor);

        // Release any `ScheduledIo`s whose `Registration` was dropped while
        // we were parked; we do it on the owning thread so the drop runs
        // here (and so it interleaves naturally with the worker loop).
        self.handle.release_pending_registrations();

        self.handle.end_park(self.idx);
    }

    /// Fast-path variant used when `begin_park` consumed a NOTIFIED — we
    /// skip the syscall but still need to flush any fd-registration ops
    /// so they become visible to the kernel before we return to the task
    /// loop.
    fn drain_pending_ops_and_submit(&mut self) {
        let pending = self.handle.take_pending_ops(self.idx);
        if pending.is_empty() {
            return;
        }
        if let Some(cell) = self.reactor.as_deref() {
            let mut reactor = cell.borrow_mut();
            apply_pending_ops(&mut reactor, pending);
            // Non-blocking flush so the kernel sees the SQEs; we'll drain
            // their completions on the next real park.
            let _ = reactor.park_timeout(Duration::ZERO);
        }
    }

    /// Build the reactor on the current thread (required for
    /// `IORING_SETUP_SINGLE_ISSUER`) and then block until every sibling
    /// worker has done the same. Intended to be called once, at the top of
    /// the scheduler's worker entry point, before any task is polled.
    ///
    /// Without this, workers race to initialize their rings: worker 0 may
    /// start polling tasks while worker 3's `io_uring_setup` is still in
    /// progress. Tests that observe durations across workers (e.g.
    /// `tcp_read_blocks_then_wakes`, which times a server's sleep from the
    /// client's perspective) see the resulting wall-clock skew as spurious
    /// failures. The startup barrier in [`UringHandle`] collapses that
    /// skew to roughly the monotonic clock's resolution.
    ///
    /// `ensure_reactor_installed` remains callable from the lazy
    /// first-park path so a parker that was never given an eager-init
    /// opportunity (e.g. isolated unit tests that drive `park` directly)
    /// still initializes correctly on first use.
    pub(crate) fn eager_init_and_sync(&mut self) {
        self.ensure_reactor_installed();
        self.handle.wait_for_start();
    }

    /// Lazy-initialize the reactor and install it into the thread-local
    /// `LOCAL_REACTOR` slot. Idempotent — subsequent calls are no-ops.
    ///
    /// Must be called on the worker thread that will drive the reactor.
    fn ensure_reactor_installed(&mut self) {
        if self.reactor.is_some() {
            return;
        }

        let reactor = Reactor::new().expect(
            "failed to construct per-worker io_uring Reactor; \
             kernel must support io_uring with SINGLE_ISSUER + DEFER_TASKRUN \
             (Linux 6.0+)",
        );

        // Publish ring_fd + external_waker so other threads can target us.
        self.handle.register_worker(
            self.idx,
            reactor.ring_fd(),
            reactor.external_waker(),
            reactor.arm_table(),
        );

        let boxed = Box::new(RefCell::new(reactor));
        let cell_ptr: *const RefCell<Reactor> = &*boxed;
        self.reactor = Some(boxed);

        // SAFETY: `cell_ptr` points into the `Box` owned by `self.reactor`.
        // The `Box`'s address is stable for its lifetime. We clear the TLS
        // slot in `shutdown` and in `Drop` before the Box is dropped, so
        // the pointer never outlives the allocation.
        unsafe {
            install_local_reactor_raw(cell_ptr);
        }
        // Publish the worker index alongside the reactor pointer so that
        // `UringHandle::add_source` can prefer this worker for fds being
        // registered from tasks currently executing here. Paired with the
        // `clear_current_worker` calls in `shutdown` / `Drop`.
        set_current_worker(self.idx);
        self.tls_installed = true;
    }
}

/// Translate a batch of [`PendingOp`]s into SQEs on `reactor`'s ring.
///
/// The SQEs are staged but not submitted — they flush together with the
/// next `submit_and_wait` (park) or explicit non-blocking submit.
///
/// Individual errors are logged-and-dropped: a failed register/deregister
/// is at worst a missed readiness notification, which callers already have
/// to cope with via the spurious-wake rules on `poll_*_ready`.
fn apply_pending_ops(reactor: &mut Reactor, pending: Vec<PendingOp>) {
    for op in pending {
        let _ = match op {
            PendingOp::Register { fd, interest, io } => reactor.register(fd, interest, &io),
            // The caller snapshotted the slab identity at queue time.
            // `reactor.deregister` gen-checks this against the current slab
            // state: a stale snapshot is silently dropped rather than
            // risking a mis-cancel. The Arc held inside the slab slot is
            // released only when the kernel posts the terminal CQE for the
            // multi-shot poll (no `IORING_CQE_F_MORE`); see
            // `uring_reactor::Reactor::deregister`.
            PendingOp::Deregister { slab_key, slab_gen } => {
                reactor.deregister(slab_key, slab_gen)
            }
        };
    }
}

impl Drop for UringParker {
    fn drop(&mut self) {
        // Belt-and-braces TLS clear. If `shutdown` ran, this is a no-op; if
        // not, we clear on this thread. If the parker is being dropped on
        // a different thread than it was installed on (not expected in
        // normal runtime shutdown — workers drop their own parkers), the
        // `clear_local_reactor` / `clear_current_worker` calls affect this
        // thread's TLS, which is harmless because they were unset to begin
        // with.
        if self.tls_installed {
            clear_local_reactor();
            clear_current_worker();
            self.tls_installed = false;
        }
    }
}

impl UringUnparker {
    /// Unpark the associated worker. Fast path is an atomic flag flip; if
    /// the worker was actually parked, a `MSG_RING` (from a worker-thread
    /// caller) or `eventfd` (external) write delivers the wake.
    ///
    /// We always go through `handle.unpark`, even when the caller is the
    /// target worker itself. The `handle.unpark` path swaps `park_state`
    /// to `NOTIFIED` and only writes the eventfd / submits a `MSG_RING`
    /// SQE when the previous state was `PARKED`, so the self-wake case
    /// still costs only an atomic swap — never a syscall.
    ///
    /// An earlier version short-circuited on
    /// `current_worker_index() == Some(self.idx)` under the assumption
    /// that the worker was mid-task and would pick up whatever was just
    /// pushed once control returned to the worker loop. That assumption
    /// is unsound: `multi_thread::worker::transition_to_parked` calls
    /// `notify_if_work_pending` → `notify_parked_local`, which can pop
    /// the *calling* worker off the sleepers list and invoke its own
    /// unparker. Short-circuiting there leaves `park_state == EMPTY`,
    /// lets the imminent `begin_park` CAS succeed, the worker blocks in
    /// `io_uring_enter`, and `num_searching` stays at 1 so subsequent
    /// `notify_parked_remote` calls are skipped by
    /// `notify_should_wakeup`. The runtime deadlocks; the same bug bites
    /// the sharded-mio backend, see `tests/rt_sharded_mio_repro.rs`.
    pub(crate) fn unpark(&self, _driver: &driver::Handle) {
        self.handle.unpark(self.idx);
    }
}

impl std::fmt::Debug for UringParker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringParker")
            .field("idx", &self.idx)
            .field("reactor_installed", &self.reactor.is_some())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for UringUnparker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringUnparker")
            .field("idx", &self.idx)
            .finish_non_exhaustive()
    }
}
