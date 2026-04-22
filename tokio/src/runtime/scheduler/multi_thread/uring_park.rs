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
//! [`Parker`]: super::park::Parker
//! [`Reactor`]: crate::runtime::io::uring_reactor::Reactor
//! [`ExternalWaker`]: crate::runtime::io::uring_reactor::ExternalWaker
//! [`UringHandle`]: crate::runtime::io::uring_driver::UringHandle
//! [`LocalReactorGuard`]: crate::runtime::io::uring_driver::LocalReactorGuard

use crate::loom::sync::Arc;
use crate::runtime::driver;
use crate::runtime::io::uring_driver::{
    clear_local_reactor, install_local_reactor_raw, UringHandle,
};
use crate::runtime::io::uring_reactor::Reactor;
use crate::runtime::scheduler::multi_thread::park::HadDriver;

use std::cell::RefCell;
use std::time::Duration;

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
    pub(crate) fn park(&mut self, _driver: &driver::Handle) -> HadDriver {
        self.park_internal(None);
        HadDriver::Yes
    }

    /// Park with a maximum duration.
    pub(crate) fn park_timeout(
        &mut self,
        _driver: &driver::Handle,
        duration: Duration,
    ) -> HadDriver {
        self.park_internal(Some(duration));
        HadDriver::Yes
    }

    /// Shutdown the parker. Clears the TLS install first (un-publishing the
    /// reactor), then drops the reactor itself. Idempotent.
    ///
    /// Must be called on the worker thread. In practice the `Drop` impl
    /// also clears the TLS as a belt-and-braces measure.
    pub(crate) fn shutdown(&mut self, _driver: &driver::Handle) {
        if self.tls_installed {
            clear_local_reactor();
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
            return;
        }

        // `reactor` is Some after `ensure_reactor_installed`.
        let cell: &RefCell<Reactor> = self
            .reactor
            .as_deref()
            .expect("reactor installed");
        let mut reactor = cell.borrow_mut();

        let result = match duration {
            None => reactor.park(),
            Some(dur) if dur.is_zero() => reactor.park_timeout(Duration::ZERO),
            Some(dur) => reactor.park_timeout(dur),
        };

        // Park errors are treated as spurious — correctness does not depend
        // on them, just liveness (another unpark will arrive).
        let _ = result;

        drop(reactor);
        self.handle.end_park(self.idx);
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
        self.handle
            .register_worker(self.idx, reactor.ring_fd(), reactor.external_waker());

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
        self.tls_installed = true;
    }
}

impl Drop for UringParker {
    fn drop(&mut self) {
        // Belt-and-braces TLS clear. If `shutdown` ran, this is a no-op; if
        // not, we clear on this thread. If the parker is being dropped on
        // a different thread than it was installed on (not expected in
        // normal runtime shutdown — workers drop their own parkers), the
        // `clear_local_reactor` call affects this thread's TLS, which is
        // harmless because it was `null` to begin with.
        if self.tls_installed {
            clear_local_reactor();
            self.tls_installed = false;
        }
    }
}

impl UringUnparker {
    /// Unpark the associated worker. Fast path is an atomic flag flip; if
    /// the worker was actually parked, a `MSG_RING` (from a worker-thread
    /// caller) or `eventfd` (external) write delivers the wake.
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
