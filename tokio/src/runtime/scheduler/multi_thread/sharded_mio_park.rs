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
    clear_local_reactor, install_local_reactor_raw, ShardedMioHandle,
};
use crate::runtime::io::sharded_mio_reactor::Reactor;
use crate::runtime::scheduler::multi_thread::park::HadDriver;

use std::cell::{Cell, RefCell};
use std::time::Duration;

thread_local! {
    /// Current worker's index for the running thread, or `None` if not
    /// on a sharded-mio worker. Used by [`ShardedMioUnparker::unpark`]
    /// to short-circuit self-wakes, matching the uring parker's
    /// [`uring_park::current_worker_index`][uci] pattern.
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
            .shared_registry()
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

        if self.handle.begin_park(self.idx) {
            // Notified fast-path: no syscall needed. Unlike the uring
            // parker we have no pending-ops queue to drain here —
            // `add_source` registered with mio directly when it ran.
            return;
        }

        let cell: &RefCell<Reactor> = self
            .reactor
            .as_deref()
            .expect("reactor installed");
        let mut reactor = cell.borrow_mut();

        let result = match duration {
            None => reactor.park(),
            Some(dur) => reactor.park_timeout(dur),
        };

        let _ = result; // park errors are spurious; another wake will arrive

        drop(reactor);

        self.handle.release_pending_registrations();
        self.handle.end_park(self.idx);
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
    /// Self-wake short-circuit: when the calling thread *is* the
    /// target worker, a wake is unnecessary. The worker is mid-task
    /// (otherwise it couldn't be calling unpark), its `park_state` is
    /// `EMPTY`, and whatever it just pushed to its own run queue will
    /// be picked up when control returns to the worker loop.
    /// Skipping the `unpark` call here avoids a needless CAS and —
    /// under `W_ring == W_task` placement — eliminates most post-wake
    /// overhead on the hot path. See commit `e29114e1` on the uring
    /// side for the original measurement motivating this.
    pub(crate) fn unpark(&self, _driver: &driver::Handle) {
        if current_worker_index() == Some(self.idx) {
            return;
        }
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
