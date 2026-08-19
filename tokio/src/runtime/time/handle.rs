use crate::runtime::time::TimeSource;
use std::fmt;

/// Handle to time driver instance.
pub(crate) struct Handle {
    pub(super) time_source: TimeSource,
    pub(super) inner: super::Inner,
}

impl Handle {
    /// Returns the time source associated with this handle.
    pub(crate) fn time_source(&self) -> &TimeSource {
        &self.time_source
    }

    /// Checks whether the driver has been shutdown.
    pub(super) fn is_shutdown(&self) -> bool {
        self.inner.is_shutdown()
    }

    /// True if this handle is using the traditional single-mutex timer
    /// wheel.
    /// Used by the uring parker to decide whether it has to drive the
    /// wheel itself; the alternative flavor manages per-worker wheels
    /// inside the worker loop and does not need parker involvement.
    #[cfg(all(tokio_unstable, feature = "io-uring-reactor", feature = "rt", target_os = "linux"))]
    pub(crate) fn is_traditional(&self) -> bool {
        match self.inner {
            super::Inner::Traditional { .. } => true,
            #[cfg(all(tokio_unstable, feature = "rt-multi-thread"))]
            super::Inner::Alternative { .. } => false,
        }
    }

    /// Track that the driver is being unparked
    pub(crate) fn unpark(&self) {
        #[cfg(feature = "test-util")]
        match self.inner {
            super::Inner::Traditional { ref did_wake, .. } => {
                did_wake.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            #[cfg(all(tokio_unstable, feature = "rt-multi-thread"))]
            super::Inner::Alternative { ref did_wake, .. } => {
                did_wake.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }
}

cfg_not_rt! {
    impl Handle {
        /// Tries to get a handle to the current timer.
        ///
        /// # Panics
        ///
        /// This function panics if there is no current timer set.
        ///
        /// It can be triggered when [`Builder::enable_time`] or
        /// [`Builder::enable_all`] are not included in the builder.
        ///
        /// It can also panic whenever a timer is created outside of a
        /// Tokio runtime. That is why `rt.block_on(sleep(...))` will panic,
        /// since the function is executed outside of the runtime.
        /// Whereas `rt.block_on(async {sleep(...).await})` doesn't panic.
        /// And this is because wrapping the function on an async makes it lazy,
        /// and so gets executed inside the runtime successfully without
        /// panicking.
        ///
        /// [`Builder::enable_time`]: crate::runtime::Builder::enable_time
        /// [`Builder::enable_all`]: crate::runtime::Builder::enable_all
        #[track_caller]
        pub(crate) fn current() -> Self {
            panic!("{}", crate::util::error::CONTEXT_MISSING_ERROR)
        }
    }
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Handle")
    }
}
