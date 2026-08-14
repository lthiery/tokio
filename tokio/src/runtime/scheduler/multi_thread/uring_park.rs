//! Worker parker for the single-ring `io_uring` reactor.
//!
//! There is one shared ring for the whole runtime. Each worker owns a
//! [`UringParker`] holding a shared [`UringHandle`]; on park, workers race
//! for the ring via [`crate::runtime::io::uring_driver::GlobalRing`] (the
//! stock mio `Parker` discipline): whoever wins drives the ring in
//! `io_uring_enter` and drains completions, everyone else condvar-parks
//! until woken. Cross-thread wakes reach a ring-driving worker through the
//! ring's eventfd and a condvar-parked worker through its condvar.
//!
//! # Timer integration
//!
//! When the runtime is built with `enable_uring_reactor()` but without
//! `enable_alt_timer()` (the default), the ring shares the legacy
//! single-mutex timer wheel. The ring holder computes its `io_uring_enter`
//! timeout as `min(scheduler_timeout, time_until_next_timer)` via
//! [`compute_legacy_timer_duration`] before parking, then advances the
//! wheel via [`process_legacy_timer_after_park`] after wake. Condvar
//! parkers leave timers to the holder.
//!
//! [`UringHandle`]: crate::runtime::io::uring_driver::UringHandle

use crate::loom::sync::Arc;
use crate::runtime::driver;
use crate::runtime::io::uring_driver::{
    compute_legacy_timer_duration, process_legacy_timer_after_park, set_current_worker, UringHandle,
};
use crate::runtime::scheduler::multi_thread::park::HadDriver;

use std::time::Duration;

// The `CURRENT_WORKER` thread-local these wrap lives in `uring_driver.rs`
// (it is consulted by `GlobalRing::push_op`, which must compile for
// `rt`-only builds where this module does not exist). The multi-thread
// worker code keeps addressing it through this module.
#[allow(unused_imports)]
pub(crate) use crate::runtime::io::uring_driver::{clear_current_worker, current_worker_index};

/// Publish the current worker index from the worker's `run` entry point,
/// before any task executes on this thread. `GlobalRing::push_op` reads it
/// to tell worker pushers (whose own next park drains the op queue) from
/// external pushers (which may need a kick).
///
/// Paired with the `ClearUringTls` teardown guard in `worker.rs`, which
/// calls [`clear_current_worker`] on worker exit.
pub(crate) fn set_current_worker_early(idx: usize) {
    set_current_worker(idx);
}

/// Per-worker parker for the single-ring `io_uring` backend.
///
/// Cheap: just a worker index and a shared [`UringHandle`]. All ring state
/// lives on the shared `GlobalRing` inside the handle.
pub(crate) struct UringParker {
    /// Zero-based worker index. Selects this worker's park slot in the
    /// shared `GlobalRing`.
    idx: usize,

    /// Shared coordination handle (owns the one ring).
    handle: Arc<UringHandle>,
}

/// Unparker counterpart to [`UringParker`]. Cheap to clone.
#[derive(Clone)]
pub(crate) struct UringUnparker {
    idx: usize,
    handle: Arc<UringHandle>,
}

impl UringParker {
    /// Construct a parker for worker `idx`.
    pub(crate) fn new(idx: usize, handle: Arc<UringHandle>) -> Self {
        Self { idx, handle }
    }

    /// Cheap handle to wake this worker from another thread.
    pub(crate) fn unparker(&self) -> UringUnparker {
        UringUnparker {
            idx: self.idx,
            handle: Arc::clone(&self.handle),
        }
    }

    /// Shared [`UringHandle`].
    #[allow(dead_code)]
    pub(crate) fn handle(&self) -> &Arc<UringHandle> {
        &self.handle
    }

    /// Park the worker until woken.
    pub(crate) fn park(&mut self, driver: &driver::Handle) -> HadDriver {
        self.park_global(driver, None)
    }

    /// Park with a maximum duration.
    pub(crate) fn park_timeout(
        &mut self,
        driver: &driver::Handle,
        duration: Duration,
    ) -> HadDriver {
        self.park_global(driver, Some(duration))
    }

    /// Race for the shared ring, drive it if won, condvar-park otherwise
    /// (the stock `Parker` discipline; see `GlobalRing`).
    ///
    /// Timer split: the ring holder parks with the legacy-timer-min'd
    /// deadline and processes the wheel after waking (it is "the driver"
    /// in the stock sense); condvar parkers use the raw scheduler timeout
    /// and leave timers to the holder.
    fn park_global(&mut self, driver: &driver::Handle, duration: Option<Duration>) -> HadDriver {
        let g = Arc::clone(self.handle.global_ring());
        let driver_duration = compute_legacy_timer_duration(driver, duration);
        let drove_ring = g.park_worker(self.idx, driver_duration, duration);
        // Release ScheduledIos queued for drop and advance the legacy
        // wheel. Both are cheap no-ops when there is nothing due, so we
        // run them regardless of whether we actually held the ring.
        self.handle.release_pending_registrations();
        process_legacy_timer_after_park(driver);
        // Report the stock parker's HadDriver distinction faithfully: a
        // condvar-parked (or notified-fast-path) worker did not hold the
        // driver, and saying it did makes the unstable
        // `enable_eager_driver_handoff` path spuriously notify.
        if drove_ring {
            HadDriver::Yes
        } else {
            HadDriver::No
        }
    }

    /// Shutdown the parker. Nothing thread-local to tear down in
    /// single-ring mode; the shared ring is dropped with the handle.
    pub(crate) fn shutdown(&mut self, _driver: &driver::Handle) {}

    /// Startup barrier. The one shared ring is built with the handle, so
    /// there is no per-worker reactor to construct here; we only wait so
    /// every worker starts polling at the same wall-clock moment (see the
    /// `start_barrier` rationale on `UringHandle`).
    pub(crate) fn eager_init_and_sync(&mut self) {
        self.handle.wait_for_start();
    }
}

/// Converts an unwinding uring worker thread into a loud process abort.
///
/// Armed on the worker's `run` stack (see `worker::run`) for the whole
/// worker lifetime. A uring worker that dies by panic while holding the
/// ring leaves a deaf ring behind: registrations still point at a CQ
/// nobody will drain, so sibling workers and `block_on` callers wait
/// forever on wakes that cannot arrive. That failure mode is strictly
/// worse than a crash.
///
/// Task panics never reach this guard (the task harness catches them);
/// only scheduler/driver invariant violations unwind through `run`.
pub(crate) struct AbortIfPanicking {
    pub(crate) worker: usize,
}

impl Drop for AbortIfPanicking {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "uring worker {} died by panic; aborting the process: \
                 a dead worker holding the shared ring cannot be drained \
                 and the runtime would otherwise hang silently",
                self.worker,
            );
            std::process::abort();
        }
    }
}

impl UringUnparker {
    /// Unpark the associated worker. Routes through `handle.unpark`, which
    /// swaps the worker's park slot to `NOTIFIED` and only performs a
    /// syscall / condvar notify when the worker was actually parked, so a
    /// self-wake costs only an atomic swap.
    ///
    /// We always go through `handle.unpark`, even when the caller is the
    /// target worker itself: short-circuiting on
    /// `current_worker_index() == Some(self.idx)` is unsound, because
    /// `transition_to_parked` can pop the calling worker off the sleepers
    /// list and invoke its own unparker; skipping the state transition
    /// there deadlocks the runtime.
    pub(crate) fn unpark(&self, _driver: &driver::Handle) {
        self.handle.unpark(self.idx);
    }
}

impl std::fmt::Debug for UringParker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringParker")
            .field("idx", &self.idx)
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
