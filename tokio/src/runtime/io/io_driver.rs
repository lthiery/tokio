//! Backend-agnostic io-driver value, dispatched via a trait object.
//!
//! See `tokio/docs/io-driver-vtable.md` for the design doc.
//!
//! [`IoDriver`] decouples the code that *uses* the io driver
//! ([`Registration`]) from the concrete reactor implementation behind it.
//! Today exactly one backend implements [`IoDriverBackend`]: the shared
//! `mio::Poll` driver (`runtime::io::Handle`), so this module is a
//! behavior-neutral seam: every method forwards to the same code the
//! call sites used to reach directly. The trait covers the registration
//! boundary only; it is where an alternative reactor backend would slot
//! in, but a complete backend also involves parking, timer integration,
//! and shutdown, which stay concrete and arrive with the backend that
//! needs them.
//!
//! ## Representation
//!
//! `IoDriver` wraps `Arc<dyn IoDriverBackend>`: a two-word (data,
//! vtable) pointer, one function-pointer load per dispatched call,
//! plain `Arc` refcount traffic on clone/drop. The `Arc` is
//! `std::sync::Arc` rather than `loom::sync::Arc` for the same reason
//! `ScheduledIo` uses it: the io driver is never enabled under loom
//! (`create_io_stack` asserts this), so this type only needs to
//! compile there, and `std` unsized coercion keeps construction free
//! of any `cfg(loom)` special-casing.
//!
//! [`Registration`]: super::registration::Registration

use crate::io::interest::Interest;
use crate::runtime::io::registration::RegistrationSource;
use crate::runtime::io::ScheduledIo;

use std::io;
use std::sync::Arc;

/// The operations a reactor backend exposes to `Registration`.
///
/// Object-safe on purpose: the runtime's `driver::Handle` stores the
/// backend as `Arc<dyn IoDriverBackend>` inside [`IoDriver`].
pub(crate) trait IoDriverBackend: Send + Sync + std::fmt::Debug {
    /// Allocate a fresh `Arc<ScheduledIo>` without registering it with
    /// the kernel poller yet. Called from
    /// `Registration::new_with_interest_and_handle` just before
    /// [`register_local`][rl].
    ///
    /// The mio backend implements this as `Arc::new(ScheduledIo::default())`.
    /// Kept on the trait so backends with non-trivial initialisation
    /// (e.g. interior init that depends on driver state) have a hook.
    ///
    /// [rl]: Self::register_local
    fn allocate_scheduled_io(&self) -> Arc<ScheduledIo>;

    /// Register a previously-allocated `Arc<ScheduledIo>` for
    /// `source` / `interest`.
    ///
    /// `Ok` means kernel-side registration completed and the backend
    /// will deliver readiness to the `ScheduledIo`; registration
    /// failures reach the caller synchronously, from the resource
    /// constructor. For the mio backend this links the `ScheduledIo`
    /// into the `RegistrationSet` and registers the source with the
    /// shared `mio::Registry` (exactly the work `Handle::add_source`
    /// did). A backend that wants to complete registration
    /// asynchronously would need to widen this contract explicitly
    /// (where the deferred error surfaces, and when); that is out of
    /// scope here.
    ///
    /// The source is taken as `&mut dyn RegistrationSource`: mio-based
    /// backends use its `mio::event::Source` supertrait, fd-keyed
    /// backends read `registration_raw_fd()` and must fail
    /// registration if the source is not fd-backed.
    fn register_local(
        &self,
        shared: &Arc<ScheduledIo>,
        source: &mut dyn RegistrationSource,
        interest: Interest,
    ) -> io::Result<()>;

    /// Deregister a previously-registered `ScheduledIo`. The `source`
    /// is required by mio-based backends to call
    /// `mio::Registry::deregister`.
    ///
    /// Deregistration is synchronous at the kernel-poller boundary:
    /// after `Ok`, the backend delivers no new readiness to the
    /// `ScheduledIo` (readiness already dispatched may still be
    /// observed). The mio backend calls `Registry::deregister` before
    /// returning and then queues the `RegistrationSet` release.
    fn deregister(
        &self,
        io: &Arc<ScheduledIo>,
        source: &mut dyn RegistrationSource,
    ) -> io::Result<()>;
}

/// Backend-agnostic io-driver value: a shared handle to whichever
/// reactor backend the runtime was built with.
#[derive(Clone, Debug)]
pub(crate) struct IoDriver {
    backend: Arc<dyn IoDriverBackend>,
}

impl IoDriver {
    /// Allocate a fresh `Arc<ScheduledIo>` for a brand-new registration.
    /// Pairs with [`Self::register_local`]; the caller keeps ownership
    /// of the Arc between the two calls.
    pub(crate) fn allocate_scheduled_io(&self) -> Arc<ScheduledIo> {
        self.backend.allocate_scheduled_io()
    }

    /// Register `shared`/`source`/`interest` with the reactor.
    pub(crate) fn register_local(
        &self,
        shared: &Arc<ScheduledIo>,
        source: &mut dyn RegistrationSource,
        interest: Interest,
    ) -> io::Result<()> {
        self.backend.register_local(shared, source, interest)
    }

    pub(crate) fn deregister(
        &self,
        io: &Arc<ScheduledIo>,
        source: &mut dyn RegistrationSource,
    ) -> io::Result<()> {
        self.backend.deregister(io, source)
    }
}

// =====================================================================
// Mio (single shared `mio::Poll`) backend
// =====================================================================
//
// `runtime::io::Handle` (the single shared `mio::Poll` driver) is the
// backend: one registry, one waker, one reactor running inline on
// whichever worker holds the `Driver`.

use crate::runtime::io::Handle as MioHandle;

impl IoDriverBackend for MioHandle {
    fn allocate_scheduled_io(&self) -> Arc<ScheduledIo> {
        // Infallible allocation; the `RegistrationSet` linkage happens in
        // `register_local` via `Handle::register_existing`.
        Arc::new(ScheduledIo::default())
    }

    fn register_local(
        &self,
        shared: &Arc<ScheduledIo>,
        source: &mut dyn RegistrationSource,
        interest: Interest,
    ) -> io::Result<()> {
        // `RegistrationSource: mio::event::Source`, and
        // `Handle::register_existing` is generic over `S: Source + ?Sized`,
        // so the trait object routes through without a coercion. The fd
        // accessor is never read here.
        self.register_existing(shared, source, interest)
    }

    fn deregister(
        &self,
        io: &Arc<ScheduledIo>,
        source: &mut dyn RegistrationSource,
    ) -> io::Result<()> {
        // `Registry::deregister<S: Source + ?Sized>` accepts the unsized
        // trait object directly; `Handle::deregister_source` is likewise
        // generic over `S: Source + ?Sized`, so the dyn-call routes
        // through without an extra coercion.
        self.deregister_source(io, source)
    }
}

impl IoDriver {
    /// Construct an `IoDriver` from an owned `Arc<runtime::io::Handle>`
    /// (the mio backend).
    pub(crate) fn from_mio(handle: Arc<MioHandle>) -> Self {
        Self { backend: handle }
    }
}

// =====================================================================
// Tests
// =====================================================================

#[cfg(all(test, not(loom), not(miri)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A second, mio-free backend: proves `IoDriver` dispatches through
    /// the trait object and that backends observe the boundary types
    /// (`RegistrationSource`, error propagation) without needing a real
    /// reactor.
    #[derive(Debug, Default)]
    struct SyntheticBackend {
        registers: AtomicUsize,
        deregisters: AtomicUsize,
        fail_register: bool,
    }

    impl IoDriverBackend for SyntheticBackend {
        fn allocate_scheduled_io(&self) -> Arc<ScheduledIo> {
            Arc::new(ScheduledIo::default())
        }

        fn register_local(
            &self,
            _shared: &Arc<ScheduledIo>,
            source: &mut dyn RegistrationSource,
            _interest: Interest,
        ) -> io::Result<()> {
            // An fd-keyed backend reads the accessor through the trait
            // object; a non-fd source must fail registration.
            #[cfg(target_family = "unix")]
            if source.registration_raw_fd().is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "source is not fd-backed",
                ));
            }
            #[cfg(not(target_family = "unix"))]
            let _ = source;
            self.registers.fetch_add(1, Ordering::SeqCst);
            if self.fail_register {
                return Err(io::Error::new(io::ErrorKind::Other, "synthetic failure"));
            }
            Ok(())
        }

        fn deregister(
            &self,
            _io: &Arc<ScheduledIo>,
            _source: &mut dyn RegistrationSource,
        ) -> io::Result<()> {
            self.deregisters.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Minimal source usable on every platform. Unix builds get an
    /// explicit `RegistrationSource` impl; non-unix builds are covered
    /// by the blanket impl.
    struct TestSource(#[allow(dead_code)] i32);

    impl mio::event::Source for TestSource {
        fn register(
            &mut self,
            _registry: &mio::Registry,
            _token: mio::Token,
            _interests: mio::Interest,
        ) -> io::Result<()> {
            Ok(())
        }
        fn reregister(
            &mut self,
            _registry: &mio::Registry,
            _token: mio::Token,
            _interests: mio::Interest,
        ) -> io::Result<()> {
            Ok(())
        }
        fn deregister(&mut self, _registry: &mio::Registry) -> io::Result<()> {
            Ok(())
        }
    }

    #[cfg(target_family = "unix")]
    impl RegistrationSource for TestSource {
        fn registration_raw_fd(&self) -> Option<std::os::fd::RawFd> {
            Some(self.0)
        }
    }

    #[test]
    fn synthetic_backend_dispatch() {
        let backend = Arc::new(SyntheticBackend::default());
        let driver = IoDriver {
            backend: Arc::clone(&backend) as Arc<dyn IoDriverBackend>,
        };

        let shared = driver.allocate_scheduled_io();
        let mut source = TestSource(7);

        driver
            .register_local(&shared, &mut source, Interest::READABLE)
            .expect("synthetic register");
        assert_eq!(backend.registers.load(Ordering::SeqCst), 1);

        driver.deregister(&shared, &mut source).expect("synthetic deregister");
        assert_eq!(backend.deregisters.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn synthetic_backend_register_error_propagates() {
        let backend = Arc::new(SyntheticBackend {
            fail_register: true,
            ..Default::default()
        });
        let driver = IoDriver {
            backend: Arc::clone(&backend) as Arc<dyn IoDriverBackend>,
        };

        let shared = driver.allocate_scheduled_io();
        let mut source = TestSource(7);
        let err = driver
            .register_local(&shared, &mut source, Interest::READABLE)
            .expect_err("synthetic register must fail");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        // The backend was reached (dispatch happened) before failing.
        assert_eq!(backend.registers.load(Ordering::SeqCst), 1);
    }

    /// Constructing an `IoDriver` from an `Arc<runtime::io::Handle>`,
    /// cloning, and dropping must keep the refcount honest and must not
    /// leak the inner allocation.
    #[test]
    fn mio_backend_dispatch_and_refcount() {
        let (_drv, handle) = crate::runtime::io::Driver::new(1024).expect("io::Driver::new");
        let inner = Arc::new(handle);
        let weak = Arc::downgrade(&inner);
        assert_eq!(Arc::strong_count(&inner), 1);

        let driver = IoDriver::from_mio(Arc::clone(&inner));
        assert_eq!(Arc::strong_count(&inner), 2);

        let driver2 = driver.clone();
        assert_eq!(Arc::strong_count(&inner), 3);

        drop(driver);
        assert_eq!(Arc::strong_count(&inner), 2);
        drop(driver2);
        assert_eq!(Arc::strong_count(&inner), 1);

        drop(inner);
        assert!(weak.upgrade().is_none());
    }
}
