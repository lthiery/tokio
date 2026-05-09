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
    install_local_reactor_raw, ParkMode, ShardedMioHandle,
};
use crate::runtime::io::sharded_mio_reactor::Reactor;
use crate::runtime::scheduler::multi_thread::park::HadDriver;

use std::cell::{Cell, RefCell};
use std::time::Duration;

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

/// Hierarchical chiplet tier-A: cross-group fallback drain in
/// `park_on_meta`. Cached once at first park to keep
/// `std::env::var` (which allocates) off the hot park path.
///
/// Set `TOKIO_CHIPLET_XGROUP_DRAIN=1` to enable.
#[cfg(target_os = "linux")]
fn xgroup_drain_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("TOKIO_CHIPLET_XGROUP_DRAIN")
            .map(|s| s.trim() == "1")
            .unwrap_or(false)
    })
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
}

/// Unparker counterpart. Cheap to clone — just an `Arc` + worker idx.
#[derive(Clone)]
pub(crate) struct ShardedMioUnparker {
    idx: usize,
    handle: Arc<ShardedMioHandle>,
}

impl ShardedMioParker {
    pub(crate) fn new(
        idx: usize,
        handle: Arc<ShardedMioHandle>,
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
        // can skip mode selection entirely.
        if self.handle.try_consume_notified(self.idx) {
            return;
        }

        // Mode selection — only runs on entries that are committing
        // to a kernel park. The hot `notify → wake → re-park → notify`
        // cycle is caught by `try_consume_notified` above, so the
        // meta-watcher CAS no longer fires on the steady-state
        // notification loop.
        //
        // The meta-watcher slot is the chiplet-group drain-of-last-
        // resort: exactly one worker per group parks on that group's
        // meta-epoll (which monitors all *group-local* children
        // level-triggered) and on wake drains own events plus calls
        // `try_steal_drain` against in-group peers. Whichever worker
        // wins the per-group CAS becomes that group's meta-watcher,
        // *regardless of its own slab state*. Cross-group events are
        // not visible to this watcher — those workers' children are
        // registered on a *different* group's meta epoll. Cross-group
        // wakes ride the existing per-worker `external_waker`
        // (`PARKED_OWN`) path that `unpark` already uses; there is no
        // global watcher and no cross-group fanout substrate.
        //
        // Loser of the CAS: `park_on_own_child` — cache-warm
        // single-owner `mio::Poll::poll` on the worker's own child
        // epoll fd. The Poll always has the `WAKER_TOKEN` eventfd
        // registered, so it is wakeable even when the slab is
        // otherwise empty.
        //
        // **Gate on (num_workers > 1) && worker_has_io_registered &&
        // group_member_count > 1.** The meta-watcher's job is to
        // drain peer events when peers are CPU-stuck and can't park
        // their own children. That requires:
        //
        // - (a) at least one peer in the *runtime* (the W=1 escape
        //   hatch — the lone worker pays nothing extra),
        // - (b) the calling worker to hold *current* I/O of its own,
        //   so the meta CAS pays for itself,
        // - (c) at least one peer in the *group* (a single-member
        //   group's meta epoll only contains this worker's own child,
        //   so a meta park is no different from `park_on_own_child`
        //   minus a wasted CAS + meta-side `epoll_wait` overhead).
        //
        // For pure-sync workloads (notify/mpsc/rwlock/watch) no
        // worker ever registers a `ScheduledIo`, so condition (b)
        // fails and the meta CAS is skipped. For per-group runtimes
        // where workers haven't yet first-parked, condition (c) keeps
        // them on `park_on_own_child` until peers join the group.
        //
        // The `worker_has_io_registered` predicate tracks the *live*
        // registration count (74189589). The `group_member_count`
        // gate is new with per-chiplet sharding and replaces what
        // was implicitly handled by the runtime-wide
        // `meta_watcher_busy` CAS at W=1.
        #[cfg(target_os = "linux")]
        let (mode, _meta_guard) = {
            // Lazy first-park group assignment. After this returns,
            // `group_idx` is published on `WorkerState` with Release
            // and our child epfd is registered on the group's meta
            // epoll. Cheap on the steady-state path (one Acquire load).
            let group_idx = self.handle.ensure_group_assigned(self.idx);
            let want_meta = self.handle.num_workers() > 1
                && self.handle.worker_has_io_registered(self.idx)
                && self.handle.group_member_count(group_idx) > 1;
            let guard = if want_meta {
                self.handle.try_acquire_meta_watcher(group_idx)
            } else {
                None
            };
            if guard.is_some() {
                (ParkMode::Meta(group_idx), guard)
            } else {
                (ParkMode::OwnChild, None)
            }
        };
        #[cfg(not(target_os = "linux"))]
        let mode = ParkMode::OwnChild;

        // Commit to a kernel park via the direct
        // `EMPTY → PARKED_<mode>` CAS. `Err` means we were yanked
        // (state observed NOTIFIED); the call has already cleared
        // state back to EMPTY for us. On linux, dropping
        // `_meta_guard` releases the meta-watcher slot so the next
        // idle worker can take it.
        if self.handle.begin_park_direct(self.idx, mode).is_err() {
            return;
        }

        #[cfg(target_os = "linux")]
        match mode {
            ParkMode::Meta(group_idx) => {
                self.park_on_meta(duration, group_idx)
            }
            ParkMode::OwnChild => self.park_on_own_child(duration),
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.park_on_own_child(duration);
        }

        self.handle.end_park(self.idx);
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

    /// Park on this worker's chiplet-group meta-epoll fd. The meta-epoll
    /// monitors every group-local worker's child epoll fd
    /// (level-triggered). When any group-local child has events —
    /// whether our own or an in-group peer's — the meta fires.
    ///
    /// On wake we use the kernel-returned `epoll_event[]` to selectively
    /// drain: each child fd was registered with `ev.u64 = worker_idx`,
    /// so the returned events tell us *exactly* which workers' children
    /// have pending readiness. We only drain those, avoiding redundant
    /// `epoll_wait(timeout=0)` syscalls on quiescent peers (which is
    /// almost everyone in steady-state idle workloads).
    ///
    /// Cross-group children are by construction not in this group's
    /// meta epoll, so they cannot fire here. Cross-group wakes ride
    /// the existing per-worker `external_waker` (`PARKED_OWN`) path.
    ///
    /// `group_idx` is the assigned group (passed in to avoid a
    /// redundant Acquire load — caller already had it from the gate).
    #[cfg(target_os = "linux")]
    fn park_on_meta(&mut self, duration: Option<Duration>, group_idx: u8) {
        let meta_epfd = self.handle.meta_epfd(group_idx);

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
        // worker — `TOKEN_WORKER_BITS = 7` caps workers at 128 across
        // the rest of sharded-mio (see `pack_token`).
        const MAX_META_EVENTS: usize = 128;
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
        // `ev.u64` at `ensure_group_assigned` time, so the `widx <
        // num_workers` bound below is enforced by sharded-mio's own
        // registration path — but we still bounds-check defensively
        // because any malformed event would otherwise shift past the
        // top of `peer_mask`. The sentinel
        // `ShardedMioHandle::meta_waker_token()` identifies this
        // group's meta-waker eventfd that the cross-worker `unpark`
        // path writes to wake a meta-mode parker; drain it here so
        // the next wake produces a fresh edge. Per-group meta epolls
        // are disjoint, so this group's meta_waker can only fire
        // here.
        let num_workers = self.handle.workers().len();
        let meta_waker_token = ShardedMioHandle::meta_waker_token();
        let mut self_fired = false;
        let mut peer_mask: u128 = 0;
        for ev in &meta_events[..n as usize] {
            if ev.u64 == meta_waker_token {
                self.handle.drain_meta_waker(group_idx);
                continue;
            }
            let widx = ev.u64 as usize;
            if widx == self.idx {
                self_fired = true;
            } else if widx < num_workers {
                peer_mask |= 1u128 << widx;
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

        // 3. Cross-group fallback drain (chiplet hierarchical-tier-A).
        //    We just paid the wake cost (epoll_wait + futex +
        //    ctxswitch). Amortize it by attempting non-blocking
        //    drains on peers in *other* chiplet groups before
        //    returning to the scheduler. `try_steal_drain` is
        //    CAS-guarded against concurrent owner/stealer access
        //    (see `steal_from_peers_masked` doc comment), so each
        //    out-of-group attempt costs at most one atomic on a
        //    miss. A hit dispatches readiness that another group's
        //    quiescent meta-watcher would otherwise have had to
        //    wake to handle — reducing the *aggregate* wake rate
        //    across all groups.
        //
        //    Bound: scan up to N_OTHER_GROUP_PEERS workers, walking
        //    in (self.idx + 1) round-robin, skipping in-group peers
        //    (we already covered them above) and our own slot.
        //    For W=64 this is ~48 atomics worst case (under one
        //    microsecond) versus a >10µs wake cost — strictly
        //    profitable when any cross-group readiness exists.
        //
        //    Gated by `TOKIO_CHIPLET_XGROUP_DRAIN=1` for clean A/B,
        //    cached once via `OnceLock` to keep the hot park path
        //    free of `std::env::var` allocations.
        if xgroup_drain_enabled() {
            const N_OTHER_GROUP_PEERS: usize = 64;
            let workers = self.handle.workers();
            let num_workers = workers.len();
            if num_workers > 1 {
                let mut scanned = 0usize;
                let mut probe = (self.idx + 1) % num_workers;
                while scanned < N_OTHER_GROUP_PEERS && scanned < num_workers {
                    if probe != self.idx {
                        if let Some(slot) = workers.get(probe) {
                            // Skip in-group peers; we already
                            // handled those via peer_mask.
                            let g = slot
                                .group_idx
                                .load(std::sync::atomic::Ordering::Acquire);
                            if g != group_idx && g != u8::MAX {
                                if let Some(registry) =
                                    slot.shared_registry.get()
                                {
                                    registry.try_steal_drain();
                                }
                            }
                        }
                    }
                    probe = (probe + 1) % num_workers;
                    scanned += 1;
                }
            }
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
    fn steal_from_peers_masked(&self, mut mask: u128) {
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
