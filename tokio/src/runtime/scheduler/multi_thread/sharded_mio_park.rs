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
    clear_local_handle, clear_local_reactor, install_local_handle_raw,
    install_local_reactor_raw, ShardedMioHandle,
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
        // observed as `PARKED`.
        self.ensure_reactor_installed();

        // Notified fast-path: no syscall needed.
        if self.handle.begin_park(self.idx) {
            return;
        }

        // Mode selection. Workers with live registrations (slab
        // non-empty) park on their own child epoll via
        // `mio::Poll::poll` — the cache-warm path that correctly
        // drains the `WAKER_TOKEN` eventfd.
        //
        // Workers whose slab is empty (truly idle — no fds, no
        // active waiters) thread-park on a futex via
        // `std::thread::park_timeout`. This is cheaper than
        // `epoll_wait` on an empty child fd because `futex(WAIT)`
        // has lower overhead and `Thread::unpark` (futex WAKE) is
        // also cheaper than `mio::Waker::wake` (eventfd write).
        //
        // NOTE: the `busy_owner_idle` benchmark distributes probe
        // fds to ALL workers, so all slabs are non-empty and this
        // branch is never taken. The thread-park path only activates
        // for workloads where some workers genuinely have no fd
        // registrations — e.g. a service with one listener worker
        // and N-1 compute-only workers.
        let slab_empty = {
            let cell: &RefCell<Reactor> = self
                .reactor
                .as_deref()
                .expect("reactor installed");
            cell.borrow().slab_is_empty()
        };

        #[cfg(target_os = "linux")]
        {
            if slab_empty {
                self.thread_park(duration);
            } else {
                self.park_on_own_child(duration);
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = slab_empty;
            self.park_on_own_child(duration);
        }

        self.handle.end_park(self.idx);
    }

    /// Thread-park fallback for workers that lost the watcher CAS.
    /// `Thread::park_timeout` blocks on a futex (cheap park / cheap
    /// unpark) until either the timeout elapses or the unpark path
    /// fires `Thread::unpark` on this worker's stored handle. The
    /// `park_state` CAS already gates redundant unparks at the
    /// application level, so we don't need a separate "spurious wake"
    /// loop here — a spurious return just bounces back through the
    /// scheduler.
    #[cfg(target_os = "linux")]
    fn thread_park(&mut self, duration: Option<Duration>) {
        match duration {
            None => std::thread::park(),
            Some(d) => std::thread::park_timeout(d),
        }
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

        // Publish this thread's `Thread` handle so unparkers can
        // deliver `Thread::unpark` for the thread-park branch of the
        // watcher gate. Idempotent (`OnceLock::set` returns `Err` on
        // re-publish, which we ignore — the second caller would land
        // the same handle anyway).
        let _ = self.handle.workers()[self.idx]
            .park_thread
            .set(std::thread::current());

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
