//! Per-worker `mio::Poll` parker for the multi-thread scheduler.
//!
//! Mirrors [`uring_park`] but without the `PendingOp` drain dance —
//! cross-thread registration lands directly on the target worker's
//! `mio::Registry`, so the park loop has no pre-park op queue to
//! apply.
//!
//! [`uring_park`]: super::uring_park

use crate::loom::sync::Arc;
use crate::runtime::driver;
use crate::runtime::io::sharded_mio_driver::{
    clear_local_reactor, install_local_reactor_raw, ShardedMioHandle, EMPTY as PARK_EMPTY,
};
use crate::runtime::io::sharded_mio_reactor::Reactor;
use crate::runtime::scheduler::multi_thread::park::HadDriver;

use std::cell::{Cell, RefCell};
#[cfg(target_os = "linux")]
use std::sync::atomic::Ordering;
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
    pub(crate) fn new(idx: usize, handle: Arc<ShardedMioHandle>) -> Self {
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

    pub(crate) fn park(&mut self, _driver: &driver::Handle) -> HadDriver {
        self.park_internal(None);
        HadDriver::Yes
    }

    pub(crate) fn park_timeout(
        &mut self,
        _driver: &driver::Handle,
        duration: Duration,
    ) -> HadDriver {
        self.park_internal(Some(duration));
        HadDriver::Yes
    }

    pub(crate) fn shutdown(&mut self, _driver: &driver::Handle) {
        if self.tls_installed {
            clear_local_reactor();
            clear_current_worker();
            self.tls_installed = false;
        }
        self.reactor.take();
    }

    fn park_internal(&mut self, duration: Option<Duration>) {
        // Lazy-init on this thread, the first time we park. Keeps
        // parity with the uring parker's startup ordering so that
        // `SharedRegistry` is published before `park_state` can be
        // observed as `PARKED`.
        self.ensure_reactor_installed();

        // Notified fast-path: no syscall needed.
        if self.handle.begin_park(self.idx) {
            return;
        }

        // Mode selection: park on our own child epoll if we have
        // any live registrations of our own (cache-warm dispatch on
        // owner thread); otherwise park on the runtime-wide meta
        // epoll so we can pick up siblings' work via steal-drain.
        //
        // The decision is made *per park call* rather than once at
        // startup so that workers transition naturally between modes
        // as their `OpsState` populates and drains. The lock acquire
        // in [`Reactor::slab_is_empty`] is uncontended on the hot
        // path under single-owner registration.
        let park_on_meta = {
            let cell: &RefCell<Reactor> = self
                .reactor
                .as_deref()
                .expect("reactor installed");
            cell.borrow().slab_is_empty()
        };

        if park_on_meta {
            self.park_on_meta(duration);
        } else {
            self.park_on_own_child(duration);
        }

        self.handle.end_park(self.idx);
    }

    /// Owner-mode park. Block in `mio::Poll::poll` on this worker's
    /// own child epoll fd and dispatch any returned events through
    /// the owner-side path that holds the slab lock under
    /// [`Reactor::poll_and_dispatch`]. This is the cache-warm path:
    /// the worker that registered the fd is the one that picks up
    /// the kernel notification and runs the wake.
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

    /// Steal-mode park. Block in `epoll_wait` on the runtime-wide
    /// meta epoll fd. On wake, the kernel returns one or more
    /// `worker_idx` values (level-triggered registration of each
    /// child epoll); for each, run [`SharedRegistry::try_steal_drain`]
    /// against that worker's registry to dispatch any queued events
    /// on its behalf. If the firing child is our own (e.g. our
    /// external waker fired), we drain it through the standard
    /// owner-side path with `timeout=0` so the slab lookup runs on
    /// our cache-warm copy of `OpsState`.
    ///
    /// Lost steal races are harmless: the meta is level-triggered,
    /// so any child still holding undrained events will keep firing
    /// until the owner or a future peer call drains it.
    #[cfg(target_os = "linux")]
    fn park_on_meta(&mut self, duration: Option<Duration>) {
        use crate::runtime::io::lazy_debug::{bump, COUNTERS};

        const META_BUDGET: usize = 16;
        let timeout_ms: i32 = match duration {
            None => -1,
            Some(d) => {
                // `epoll_wait` takes milliseconds as i32. Saturate to
                // i32::MAX-1 (~24 days) so the upper bound never
                // overflows; durations beyond that are a scheduler-
                // shutdown artifact, not a real wait.
                let ms = d.as_millis();
                if ms >= i32::MAX as u128 { i32::MAX - 1 } else { ms as i32 }
            }
        };

        // SAFETY: `epoll_event` is plain old data; `epoll_wait`
        // overwrites the slots it returns and we only read the
        // first `n` of them.
        let mut events: [libc::epoll_event; META_BUDGET] =
            unsafe { std::mem::zeroed() };

        bump(&COUNTERS.meta_park_calls);

        let meta_fd = self.handle.meta_epfd();
        let n = unsafe {
            libc::epoll_wait(
                meta_fd,
                events.as_mut_ptr(),
                META_BUDGET as i32,
                timeout_ms,
            )
        };
        if n < 0 {
            // EINTR is expected; other errors are logged and we let
            // the scheduler re-park if it still has nothing to do.
            bump(&COUNTERS.meta_park_err);
            return;
        }
        if n == 0 {
            bump(&COUNTERS.meta_park_timeout);
            return;
        }
        bump(&COUNTERS.meta_park_woken);

        let workers = self.handle.workers();
        for ev in events.iter().take(n as usize) {
            let widx = ev.u64 as usize;
            if widx >= workers.len() {
                continue;
            }
            if widx == self.idx {
                // Our own child fired (most commonly the external
                // waker). Drain it through the owner-side path —
                // we *are* the owner, so this takes the lock without
                // contending with anyone.
                let cell: &RefCell<Reactor> = self
                    .reactor
                    .as_deref()
                    .expect("reactor installed");
                let mut reactor = cell.borrow_mut();
                let _ = reactor.park_timeout(Duration::ZERO);
                drop(reactor);
                continue;
            }
            // Gate steal-drain on the owner being in `EMPTY` state
            // (running user code, not parked or transitioning).
            //
            // The peer-drain path raw-`epoll_wait`s on the owner's
            // child epoll fd. mio's `Waker` is registered on that same
            // epoll fd as an EPOLLET-tracked eventfd, so a concurrent
            // peer drain can consume the WAKER event posted by the
            // most recent `unpark(widx)` without ever reading the
            // eventfd. The owner's own `Poll::poll` then blocks
            // indefinitely: the eventfd counter stays at 1, no edge
            // transition fires, and subsequent `Waker::wake()` writes
            // never produce another EPOLLET event.
            //
            // Owner in `EMPTY` ⇒ not parked, no pending unpark, so the
            // WAKER eventfd cannot have a queued event for the peer
            // to swallow. Owner in `PARKED` or `NOTIFIED` ⇒ owner
            // (or its imminent return from `Poll::poll`) is the
            // designated drainer; peer skipping costs at most one
            // wasted meta wake. The meta is level-triggered, so a
            // missed steal re-fires until the owner consumes the
            // events itself.
            let owner_state =
                workers[widx].park_state.load(Ordering::Acquire);
            if owner_state != PARK_EMPTY {
                continue;
            }
            if let Some(reg) = workers[widx].shared_registry.get() {
                reg.try_steal_drain();
            }
        }
    }

    /// Non-Linux fallback: meta-epoll is Linux-only. Sharded-mio is
    /// gated to Linux by `scheduled_io.rs`, so this branch should
    /// be unreachable in steady state, but we provide an
    /// implementation that simply parks on the owner's child as a
    /// safety net rather than relying on a panic.
    #[cfg(not(target_os = "linux"))]
    fn park_on_meta(&mut self, duration: Option<Duration>) {
        self.park_on_own_child(duration);
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
        set_current_worker(self.idx);
        self.tls_installed = true;
    }
}

impl Drop for ShardedMioParker {
    fn drop(&mut self) {
        if self.tls_installed {
            clear_local_reactor();
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
