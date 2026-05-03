//! Per-worker `mio::Poll` parker for the multi-thread scheduler.
//!
//! Mirrors [`uring_park`] but without the `PendingOp` drain dance —
//! cross-thread registration lands directly on the target worker's
//! `mio::Registry`, so the park loop has no pre-park op queue to
//! apply.
//!
//! # Timer integration
//!
//! Two flavors are supported (decoupled from this module via the
//! `rt-alt-timer` Cargo feature on [`Builder::enable_alt_timer`]):
//!
//! - **Legacy timer (default)** — the runtime owns a single shared timer
//!   wheel under a `Mutex<InnerState>` (`time::Handle::Inner::Traditional`).
//!   The standard time driver wraps the IO stack and runs its own
//!   `park_internal` (read `next_wake` → park IO → process expired wheel
//!   entries). Sharded-mio bypasses that wrapper because each worker owns
//!   its own [`mio::Poll`], so the parker has to drive the wheel itself.
//!   The hybrid path is implemented by [`Self::compute_legacy_timer_duration`]
//!   (read `next_wake_tick` → `min(scheduler_timeout, time_dur)`) before
//!   parking and [`Self::process_legacy_timer_after_park`] (call
//!   `parker_process(clock)`) after wake. See
//!   [`time::Handle::next_wake_tick`] for the pre-park query semantics.
//! - **Alt-timer (opt-in via `enable_alt_timer()`)** — per-worker timer
//!   wheels co-located with the parker, requires the `rt-alt-timer` Cargo
//!   feature. The parker leaves wheel processing to the alt-timer hooks
//!   in [`worker::run`]; the hybrid hooks here are no-ops in that case
//!   (selected via `time_handle.is_traditional()` at runtime).
//!
//! [`uring_park`]: super::uring_park
//! [`Builder::enable_alt_timer`]: crate::runtime::Builder::enable_alt_timer
//! [`time::Handle::next_wake_tick`]: crate::runtime::time::Handle::next_wake_tick
//! [`worker::run`]: super::worker

use crate::loom::sync::Arc;
use crate::runtime::driver;
use crate::runtime::io::sharded_mio_driver::{
    clear_local_handle, clear_local_reactor, install_local_handle_raw,
    install_local_reactor_raw, ParkMode, ShardedMioHandle, NOTIFIED,
};
use crate::runtime::io::sharded_mio_reactor::Reactor;
use crate::runtime::scheduler::multi_thread::park::HadDriver;

use std::cell::{Cell, RefCell};
use std::sync::OnceLock;
use std::time::Duration;

/// Default pre-park spin budget. **Zero (opt-in).**
///
/// The pre-park spin window is a workload-dependent optimisation:
/// it pays for itself on high-fanout cross-worker wake patterns
/// (e.g. `tokio::sync::watch::Sender::send` to many subscribers,
/// `broadcast::Sender` contention), and costs CPU/wall-time on
/// patterns where most workers never see a cross-worker wake (e.g.
/// `Notify::notify_one` from one external thread, single-tenant RPC
/// streams).
///
/// Because the right value depends on the workload, the substrate
/// ships with spin disabled and exposes it as an explicit knob. With
/// `spin_budget == 0` the parker uses a direct
/// `EMPTY → PARKED_<mode>` CAS (the original pre-SEARCHING path)
/// and the substrate cost is identical to the prior 4-state machine.
///
/// Opt in via [`Builder::enable_park_spin_budget`][bps] (per-runtime,
/// preferred) or the `TOKIO_PARK_SPIN_BUDGET=<u32>` env var
/// (process-wide). The Builder method takes precedence over the env
/// var. A budget around `256` iterations recovers ~10% on
/// `sync_watch/contention_resubscribe/100` against traditional mio;
/// see `bench-sweep-results/SUMMARY.txt` for a full sweep.
///
/// [bps]: crate::runtime::Builder::enable_park_spin_budget
const DEFAULT_SPIN_BUDGET: u32 = 0;

/// Read `TOKIO_PARK_SPIN_BUDGET` once per process. Each
/// `ShardedMioParker` snapshots the resolved budget into a local
/// field at construction so the spin loop has no atomic load per
/// park.
///
/// Resolution order (most-specific wins): `override_iters`
/// (`Builder::enable_park_spin_budget`) → `TOKIO_PARK_SPIN_BUDGET`
/// env var → [`DEFAULT_SPIN_BUDGET`].
fn resolve_spin_budget(override_iters: Option<u32>) -> u32 {
    if let Some(n) = override_iters {
        return n;
    }
    static ENV_BUDGET: OnceLock<Option<u32>> = OnceLock::new();
    if let Some(n) = *ENV_BUDGET.get_or_init(|| {
        std::env::var("TOKIO_PARK_SPIN_BUDGET")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
    }) {
        return n;
    }
    DEFAULT_SPIN_BUDGET
}

/// Tri-state futex-park override, parsed once per process from
/// `TOKIO_FUTEX_PARK`:
///
/// * `Some(true)`  — force futex park for every non-meta-watcher
///   worker, regardless of registered I/O. Useful only for
///   benchmarks of zero-I/O workloads (e.g. `sync_broadcast`,
///   `sync_notify`); **unsafe** for workloads with real I/O
///   sources because epoll readiness can't wake a futex.
/// * `Some(false)` — force epoll park (legacy behaviour, what the
///   driver did before the futex branch was added).
/// * `None`        — auto: per-worker, take the futex path iff
///   the worker has never had a `ScheduledIo` register on it
///   (`ShardedMioHandle::worker_has_io_registered(idx) == false`).
///   This is the production default — no-I/O workers (CPU-bound
///   tasks, channel/notify benches) avoid the ~9% kernel
///   epoll/eventfd overhead measured in `perf-investigation/`,
///   while workers that ever touched I/O stay on the epoll path.
#[cfg(target_os = "linux")]
fn futex_park_override() -> Option<bool> {
    static FLAG: OnceLock<Option<bool>> = OnceLock::new();
    *FLAG.get_or_init(|| {
        std::env::var("TOKIO_FUTEX_PARK").ok().and_then(|s| {
            if s == "1" || s.eq_ignore_ascii_case("true") {
                Some(true)
            } else if s == "0" || s.eq_ignore_ascii_case("false") {
                Some(false)
            } else {
                None
            }
        })
    })
}

/// Decide whether `worker_idx` should take the `PARKED_OWN_FUTEX`
/// path on this park entry. See [`futex_park_override`] for the
/// gate semantics.
#[cfg(target_os = "linux")]
fn use_futex_park(handle: &ShardedMioHandle, worker_idx: usize) -> bool {
    match futex_park_override() {
        Some(forced) => forced,
        None => !handle.worker_has_io_registered(worker_idx),
    }
}

thread_local! {
    /// Current worker's index for the running thread, or `None` if not
    /// on a sharded-mio worker. Mirrors the uring parker's
    /// [`uring_park::current_worker_index`][uci] TLS slot.
    ///
    /// Currently has no readers — `unpark` no longer short-circuits on
    /// `current == self.idx` (the previous self-wake optimization was
    /// unsound; see [`ShardedMioUnparker::unpark`]). Kept in place
    /// because step 2c is expected to introduce caller-local placement
    /// for fd registrations, mirroring uring's planned path.
    ///
    /// [uci]: super::uring_park::current_worker_index
    static CURRENT_WORKER: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Index of the worker currently executing on this thread, or `None`.
#[allow(dead_code)]
pub(crate) fn current_worker_index() -> Option<usize> {
    CURRENT_WORKER.with(Cell::get)
}

fn set_current_worker(idx: usize) {
    CURRENT_WORKER.with(|c| c.set(Some(idx)));
}

/// Publish the current worker index from the worker's `run` entry
/// point, before any task executes. Paired with [`clear_current_worker`]
/// in the teardown guard.
pub(crate) fn set_current_worker_early(idx: usize) {
    set_current_worker(idx);
}

/// Clear this thread's `CURRENT_WORKER` slot. Idempotent.
pub(crate) fn clear_current_worker() {
    CURRENT_WORKER.with(|c| c.set(None));
}

/// Per-worker parker for the sharded-mio backend.
pub(crate) struct ShardedMioParker {
    idx: usize,
    handle: Arc<ShardedMioHandle>,
    reactor: Option<Box<RefCell<Reactor>>>,
    tls_installed: bool,
    /// Snapshotted at construction from `TOKIO_PARK_SPIN_BUDGET`. The
    /// pre-park spin loop runs for at most this many `spin_loop()`
    /// iterations before committing to a kernel park. Each iteration
    /// reads `park_state`; if it observes `NOTIFIED` (a cross-worker
    /// `unpark` swapped against `prev = SEARCHING`) the parker yanks
    /// out without entering the kernel.
    spin_budget: u32,
}

/// Unparker counterpart. Cheap to clone — just an `Arc` + worker idx.
#[derive(Clone)]
pub(crate) struct ShardedMioUnparker {
    idx: usize,
    handle: Arc<ShardedMioHandle>,
}

impl ShardedMioParker {
    /// `spin_budget_override` is the per-runtime override sourced from
    /// [`Builder::enable_park_spin_budget`][bps]; pass `None` to fall
    /// back to env var / compiled-in default.
    ///
    /// [bps]: crate::runtime::Builder::enable_park_spin_budget
    pub(crate) fn new(
        idx: usize,
        handle: Arc<ShardedMioHandle>,
        spin_budget_override: Option<u32>,
    ) -> Self {
        // Eager reactor construction. Unlike the uring path (which is
        // pinned to the worker thread by `IORING_SETUP_SINGLE_ISSUER`),
        // `mio::Poll` can be built on any thread and then sent to the
        // worker — `Poll: Send`, `Registry: Send + Sync`. Building here
        // guarantees the `SharedRegistry` is published into
        // `UringHandle::workers[idx]` before any `add_source` call can
        // arrive, eliminating the startup race that TCP-bind-before-worker
        // setup would otherwise hit.
        let reactor = Reactor::new().expect(
            "failed to construct per-worker mio::Poll Reactor",
        );
        let shared_registry = reactor
            .shared_registry(idx)
            .expect("failed to clone mio::Registry for sharded-mio worker");
        let external_waker = reactor.external_waker();
        handle.register_worker(idx, shared_registry, external_waker);

        Self {
            idx,
            handle,
            reactor: Some(Box::new(RefCell::new(reactor))),
            tls_installed: false,
            spin_budget: resolve_spin_budget(spin_budget_override),
        }
    }

    pub(crate) fn unparker(&self) -> ShardedMioUnparker {
        ShardedMioUnparker {
            idx: self.idx,
            handle: Arc::clone(&self.handle),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn handle(&self) -> &Arc<ShardedMioHandle> {
        &self.handle
    }

    pub(crate) fn park(&mut self, driver: &driver::Handle) -> HadDriver {
        let park_dur = self.compute_legacy_timer_duration(driver, None);
        self.park_internal(park_dur);
        self.process_legacy_timer_after_park(driver);
        HadDriver::Yes
    }

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

    /// Hybrid park flow: legacy timer + sharded-mio I/O.
    ///
    /// When the runtime is built with `enable_sharded_mio()` but without
    /// `enable_alt_timer()` (the default since the rt-alt-timer feature
    /// gate landed), each worker still owns its own `mio::Poll` but shares
    /// the legacy single-mutex timer wheel. To make sleeps fire on time,
    /// each parker has to compute its `epoll_wait` timeout as
    /// `min(scheduler_timeout, time_until_next_timer)`.
    ///
    /// Returns the duration to pass to `park_internal`. `None` means "park
    /// indefinitely" (no scheduler timeout, no pending timer).
    fn compute_legacy_timer_duration(
        &self,
        driver: &driver::Handle,
        scheduler_timeout: Option<Duration>,
    ) -> Option<Duration> {
        // Only consult the time handle when timers are enabled AND the
        // runtime is using the legacy timer flavor (alt-timer manages its
        // own per-worker wheel and doesn't need parker involvement).
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

    pub(crate) fn shutdown(&mut self, _driver: &driver::Handle) {
        if self.tls_installed {
            clear_local_reactor();
            clear_local_handle();
            clear_current_worker();
            self.tls_installed = false;
        }
        self.reactor.take();
    }

    fn park_internal(&mut self, duration: Option<Duration>) {
        // Lazy-init on this thread, the first time we park. Keeps
        // parity with the uring parker's startup ordering so that
        // `SharedRegistry` is published before `park_state` can be
        // observed as `PARKED_*`.
        self.ensure_reactor_installed();

        // Cheap fast-path: if a notification is already pending we
        // can skip the spin gate and mode selection entirely.
        if self.handle.try_consume_notified(self.idx) {
            return;
        }

        // Two substrate paths:
        //
        // - `spin_budget > 0`: enter the pre-park spin window via
        //   `begin_searching` (CAS EMPTY → SEARCHING). A concurrent
        //   `unpark` swap against `prev = SEARCHING` is yanked back
        //   into userspace; we observe it from `park_state_load` in
        //   the spin loop or via `commit_park` failing with NOTIFIED.
        //
        // - `spin_budget == 0`: skip SEARCHING entirely and use the
        //   direct `EMPTY → PARKED_<mode>` CAS via `begin_park_direct`.
        //   This is one CAS instead of two and matches the original
        //   pre-SEARCHING substrate cost. Workloads that have not
        //   opted into spin pay zero substrate overhead vs the prior
        //   4-state machine.
        let spin = self.spin_budget;

        if spin > 0 {
            // `Err` here means we lost a race against an unpark from
            // EMPTY (state was already NOTIFIED on entry); the call
            // has already cleared state back to EMPTY for us.
            if self.handle.begin_searching(self.idx).is_err() {
                return;
            }

            // Spin window. Each iteration reads `park_state`; if a
            // cross-worker `unpark` ran during the spin (swapping
            // `prev = SEARCHING` to `NOTIFIED`) we observe it here and
            // bail without ever entering the kernel. The number of
            // iterations is sized by `TOKIO_PARK_SPIN_BUDGET`,
            // snapshotted into `self.spin_budget` at construction so
            // the inner loop does not pay an atomic load per park to
            // re-read it.
            //
            // `core::hint::spin_loop()` lowers to the architecture's
            // spin-pause hint (`PAUSE` on x86) so the SMT sibling and
            // the pipeline backend stay free for whatever cross-core
            // wake activity is in flight. The cost per iteration on
            // recent x86 is dominated by `PAUSE` itself (~100 cycles).
            for _ in 0..spin {
                if self.handle.park_state_load(self.idx) == NOTIFIED {
                    self.handle.abort_searching(self.idx);
                    return;
                }
                core::hint::spin_loop();
            }
        }

        // Mode selection — only runs on entries that are committing
        // to a kernel park. The hot `notify → wake → re-park → notify`
        // cycle is caught by `try_consume_notified` above, so the
        // meta-watcher CAS no longer fires on the steady-state
        // notification loop.
        //
        // The meta-watcher slot is the runtime-wide drain-of-last-
        // resort: exactly one worker parks on the meta-epoll (which
        // monitors ALL children level-triggered) and on wake drains
        // own events plus calls `try_steal_drain` against every peer.
        // Whichever worker wins the CAS becomes the meta-watcher,
        // *regardless of its own slab state*. This is critical for
        // `busy_owner_3burners`-style workloads: when N-1 workers are
        // burner-pinned and the only free worker happens to hold no
        // own registrations, that free worker MUST still serve as the
        // peer-drain — otherwise the queued events on the busy
        // children would have nobody to harvest them and probes would
        // stall until the burners' `BURNER_MS` deadline.
        //
        // Loser of the CAS: `park_on_own_child` — cache-warm
        // single-owner `mio::Poll::poll` on the worker's own child
        // epoll fd. The Poll always has the `WAKER_TOKEN` eventfd
        // registered, so it is wakeable even when the slab is
        // otherwise empty.
        #[cfg(target_os = "linux")]
        let (mode, _meta_guard) = {
            let guard = self.handle.try_acquire_meta_watcher();
            if guard.is_some() {
                (ParkMode::Meta, guard)
            } else if use_futex_park(&self.handle, self.idx) {
                // Direct futex park on `park_state` — skips the
                // `mio::Poll::poll` → `epoll_wait` → eventfd_write
                // → `ep_poll_callback` chain that costs ~9% of CPU
                // on no-I/O cross-worker-wake benches like
                // `sync_broadcast/contention/10`. Safe only when the
                // worker has no I/O sources on its child epoll;
                // the auto-gate consults the per-worker
                // `has_io_registered` latch — workers that ever held
                // a `ScheduledIo` registration drop back to the
                // epoll path permanently.
                (ParkMode::OwnChildFutex, None)
            } else {
                (ParkMode::OwnChild, None)
            }
        };
        #[cfg(not(target_os = "linux"))]
        let mode = ParkMode::OwnChild;

        // Commit to a kernel park. `Err` means we were yanked (state
        // observed NOTIFIED); the call has already cleared state back
        // to EMPTY for us. On linux, dropping `_meta_guard` releases
        // the meta-watcher slot so the next idle worker can take it.
        let commit = if spin > 0 {
            self.handle.commit_park(self.idx, mode)
        } else {
            self.handle.begin_park_direct(self.idx, mode)
        };
        if commit.is_err() {
            return;
        }

        #[cfg(target_os = "linux")]
        match mode {
            ParkMode::Meta => self.park_on_meta(duration),
            ParkMode::OwnChild => self.park_on_own_child(duration),
            ParkMode::OwnChildFutex => self.park_on_own_futex(duration),
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.park_on_own_child(duration);
        }

        self.handle.end_park(self.idx);
    }

    /// Direct-futex park on this worker's `park_state` atomic.
    /// Block in `SYS_futex(FUTEX_WAIT, &park_state,
    /// PARKED_OWN_FUTEX, timeout)`.
    ///
    /// Selected by `park_internal` only when (a) we lost the
    /// meta-watcher CAS, (b) the per-worker spin window is
    /// disabled or has elapsed, and (c) the futex-park path is
    /// enabled (currently env-var gated; will become slab-empty
    /// gated once the per-worker registration counter lands).
    ///
    /// Wake-side consistency: `ShardedMioHandle::unpark` always
    /// `swap(NOTIFIED)`s before issuing the wake; on `prev ==
    /// PARKED_OWN_FUTEX` it issues `FUTEX_WAKE` against the same
    /// atomic. The kernel's CAS-on-entry to FUTEX_WAIT closes the
    /// publish/sleep race — a publisher that swapped NOTIFIED
    /// before we entered the syscall causes the syscall to return
    /// `EAGAIN` immediately without queuing.
    #[cfg(target_os = "linux")]
    fn park_on_own_futex(&mut self, duration: Option<Duration>) {
        self.handle.park_on_own_futex(self.idx, duration);
    }

    /// Owner-mode park. Block in `mio::Poll::poll` on this worker's
    /// own child epoll fd and dispatch any returned events through
    /// the owner-side path that holds the slab lock under
    /// [`Reactor::poll_and_dispatch`]. This is the cache-warm path:
    /// the worker that registered the fd is the one that picks up
    /// the kernel notification and runs the wake. It is also where
    /// the worker's own `WAKER_TOKEN` eventfd is correctly drained,
    /// so the cross-worker `unpark` path (which writes to that
    /// eventfd) keeps producing fresh EPOLLET edges across parks.
    fn park_on_own_child(&mut self, duration: Option<Duration>) {
        let cell: &RefCell<Reactor> = self
            .reactor
            .as_deref()
            .expect("reactor installed");
        let mut reactor = cell.borrow_mut();
        let result = match duration {
            None => reactor.park(),
            Some(dur) => reactor.park_timeout(dur),
        };
        // Park errors are spurious; another wake will arrive.
        let _ = result;
    }

    /// Park on the runtime-wide meta-epoll fd. The meta-epoll monitors
    /// ALL workers' child epoll fds (level-triggered). When any child
    /// has events — whether our own or a peer's — the meta fires.
    ///
    /// On wake we use the kernel-returned `epoll_event[]` to selectively
    /// drain: each child fd was registered with `ev.u64 = worker_idx`,
    /// so the returned events tell us *exactly* which workers' children
    /// have pending readiness. We only drain those, avoiding redundant
    /// `epoll_wait(timeout=0)` syscalls on quiescent peers (which is
    /// almost everyone in steady-state idle workloads).
    ///
    /// This is how a free worker discovers events stuck on busy workers'
    /// child epolls (the `busy_owner_3burners` path). Without meta-epoll
    /// parking, the free worker only checks peers once per wake from its
    /// own child epoll — missing events that arrive on peers but not
    /// on self.
    #[cfg(target_os = "linux")]
    fn park_on_meta(&mut self, duration: Option<Duration>) {
        let meta_epfd = self.handle.meta_epfd();

        // Convert duration to epoll_wait timeout in ms.
        let timeout_ms: i32 = match duration {
            None => -1,
            Some(d) => {
                let ms = d.as_millis();
                if ms > i32::MAX as u128 {
                    i32::MAX
                } else {
                    ms as i32
                }
            }
        };

        // Block on the meta-epoll. Each returned event carries the
        // worker_idx in ev.u64 (stamped at register_worker time).
        // The buffer is sized to one slot per maximum supported
        // worker — `TOKEN_WORKER_BITS = 4` caps workers at 16 across
        // the rest of sharded-mio (see `pack_token`).
        const MAX_META_EVENTS: usize = 16;
        let mut meta_events: [libc::epoll_event; MAX_META_EVENTS] =
            unsafe { std::mem::zeroed() };
        let n = unsafe {
            libc::epoll_wait(
                meta_epfd,
                meta_events.as_mut_ptr(),
                MAX_META_EVENTS as i32,
                timeout_ms,
            )
        };

        // n <= 0 means timeout, EINTR, or error. Nothing to drain;
        // bounce back through the scheduler. The meta is level-
        // triggered, so any racing event will fire us again next park.
        if n <= 0 {
            return;
        }

        // Build a peer-mask from the returned events, and remember
        // whether our own child fired. `worker_idx` was stamped into
        // `ev.u64` at `register_worker` time, so the `widx <
        // num_workers` bound below is enforced by sharded-mio's own
        // registration path — but we still bounds-check defensively
        // because any malformed event would otherwise shift past the
        // top of `peer_mask`. The sentinel
        // `ShardedMioHandle::meta_waker_token()` identifies the
        // meta-waker eventfd that the cross-worker `unpark` path
        // writes to wake a meta-mode parker; drain it here so the
        // next wake produces a fresh edge.
        let num_workers = self.handle.workers().len();
        let meta_waker_token = ShardedMioHandle::meta_waker_token();
        let mut self_fired = false;
        let mut peer_mask: u64 = 0;
        for ev in &meta_events[..n as usize] {
            if ev.u64 == meta_waker_token {
                self.handle.drain_meta_waker();
                continue;
            }
            let widx = ev.u64 as usize;
            if widx == self.idx {
                self_fired = true;
            } else if widx < num_workers {
                peer_mask |= 1u64 << widx;
            }
        }

        // 1. Drain own child only if it fired. This skips a wasted
        //    `epoll_wait(timeout=0)` syscall in the (common) case
        //    where the meta woke us solely on a peer's events.
        if self_fired {
            self.park_on_own_child(Some(Duration::ZERO));
        }

        // 2. Steal only from peers whose children fired. Skips
        //    `try_steal_drain` (which costs a CAS + a non-blocking
        //    `epoll_wait`) on every quiescent peer.
        if peer_mask != 0 {
            self.steal_from_peers_masked(peer_mask);
        }
    }

    /// Non-blocking steal scan over a mask of peer workers. Each set
    /// bit is a peer whose child epoll has pending events (per the
    /// meta-epoll's last `epoll_wait` result). Each call does a
    /// non-blocking `epoll_wait(timeout=0)` under a `try_lock` of the
    /// peer's `OpsState`, so a miss costs only one atomic CAS.
    ///
    /// This is the readiness-stealing mechanism that closes the
    /// `busy_owner_3burners` gap: a free worker that has already
    /// dispatched its own events can pick up events queued on busy
    /// workers' child epolls while those workers are mid-task and
    /// unable to park.
    ///
    /// Safety against concurrent `epoll_wait` is provided by the
    /// CAS-based `epoll_guard` inside `try_steal_drain`: both the
    /// owner's `mio::Poll::poll` and the stealer's raw `epoll_wait`
    /// must CAS the guard `false→true` before entering; whoever
    /// loses the CAS defers. This closes the TOCTOU window that a
    /// plain flag could not.
    #[cfg(target_os = "linux")]
    fn steal_from_peers_masked(&self, mut mask: u64) {
        let workers = self.handle.workers();
        while mask != 0 {
            let peer_idx = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            // `peer_idx` came from a worker_idx stamped at
            // `register_worker` time; if the slot exists, the
            // child epoll registration has already been published.
            if let Some(slot) = workers.get(peer_idx) {
                if let Some(registry) = slot.shared_registry.get() {
                    registry.try_steal_drain();
                }
            }
        }
    }

    /// Build the reactor on the current thread and wait on the startup
    /// barrier for every sibling to do the same. Mirrors
    /// [`UringParker::eager_init_and_sync`][ueis]; called once per
    /// worker at the top of `worker::run`.
    ///
    /// [ueis]: super::uring_park::UringParker::eager_init_and_sync
    pub(crate) fn eager_init_and_sync(&mut self) {
        self.ensure_reactor_installed();
        self.handle.wait_for_start();
    }

    /// Install `LOCAL_REACTOR` TLS on the current (worker) thread.
    /// The reactor itself was constructed up front in `new()`; this
    /// only wires the thread-local pointer so in-thread code paths
    /// (e.g. peer wake routing in future revisions) can find it.
    fn ensure_reactor_installed(&mut self) {
        if self.tls_installed {
            return;
        }
        let boxed = self
            .reactor
            .as_deref()
            .expect("reactor constructed in ShardedMioParker::new");
        let cell_ptr: *const RefCell<Reactor> = boxed;

        // SAFETY: `cell_ptr` points into the Box owned by
        // `self.reactor`. The Box's address is stable; we clear the
        // TLS slot in `shutdown` / `Drop` before the Box is dropped.
        unsafe {
            install_local_reactor_raw(cell_ptr);
        }

        // Install the handle pointer so `ScheduledIo` waker register
        // sites can record cross-worker stake without an extra Arc on
        // every `ScheduledIo`. The pointer is valid for the lifetime
        // of `self.handle`, which is held by this parker until
        // `shutdown` / `Drop` clears the TLS.
        //
        // SAFETY: `&*self.handle` dereferences an `Arc<ShardedMioHandle>`
        // that we hold for the full duration the TLS pointer is
        // installed.
        let handle_ptr: *const ShardedMioHandle = &*self.handle;
        unsafe {
            install_local_handle_raw(handle_ptr);
        }
        set_current_worker(self.idx);

        self.tls_installed = true;
    }
}

impl Drop for ShardedMioParker {
    fn drop(&mut self) {
        if self.tls_installed {
            clear_local_reactor();
            clear_local_handle();
            clear_current_worker();
            self.tls_installed = false;
        }
    }
}

impl ShardedMioUnparker {
    /// Unpark the associated worker.
    ///
    /// We always go through `handle.unpark`, even when the caller is the
    /// target worker itself. The `handle.unpark` path swaps `park_state`
    /// to `NOTIFIED` and only issues a kernel wake when the previous
    /// state was `PARKED` (i.e. a syscall is genuinely in progress on
    /// some other thread), so the self-wake case still costs only an
    /// atomic swap — never a syscall.
    ///
    /// An earlier version of this function short-circuited on
    /// `current_worker_index() == Some(self.idx)` under the assumption
    /// that the worker was mid-task and would observe whatever was just
    /// pushed once control returned to the worker loop. That assumption
    /// is unsound: `multi_thread::worker::transition_to_parked` calls
    /// `notify_if_work_pending` → `notify_parked_local`, which can pop
    /// the *calling* worker off the sleepers list and invoke its own
    /// unparker. If we short-circuit there, `park_state` stays `EMPTY`,
    /// the next `begin_park` CAS succeeds, the worker blocks in
    /// `poll.poll`, and the idle state is left with
    /// `num_searching == 1` so subsequent `notify_parked_remote` calls
    /// from other workers are skipped by `notify_should_wakeup`. The
    /// runtime then deadlocks. See `tests/rt_sharded_mio_repro.rs`.
    pub(crate) fn unpark(&self, _driver: &driver::Handle) {
        self.handle.unpark(self.idx);
    }
}

impl std::fmt::Debug for ShardedMioParker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedMioParker")
            .field("idx", &self.idx)
            .field("reactor_installed", &self.reactor.is_some())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ShardedMioUnparker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedMioUnparker")
            .field("idx", &self.idx)
            .finish_non_exhaustive()
    }
}
