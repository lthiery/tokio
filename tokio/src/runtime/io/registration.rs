#![cfg_attr(not(feature = "net"), allow(dead_code))]

use crate::io::interest::Interest;
use crate::runtime::io::{Direction, Handle, ReadyEvent, ScheduledIo};
use crate::runtime::scheduler;

use mio::event::Source;
use std::io;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

/// Source trait bound used by `Registration::new_with_interest_and_handle`.
///
/// On Linux with the experimental `io-uring-reactor` or `io-sharded-mio`
/// feature enabled, the vtable-routed backends need a raw fd so they can
/// (uring) submit `POLL_ADD_MULTI` keyed on it, or (sharded-mio) call
/// `register_local` at first-poll without holding the original `Source`
/// reference. We expose that fd via a [`registration_raw_fd`] method
/// rather than a direct [`AsRawFd`] supertrait bound, because
/// [`mio::unix::SourceFd<'_>`] — used by `AsyncFd` — does not itself
/// implement `AsRawFd` even though it trivially holds a `RawFd`. A
/// hand-written impl below plugs that hole.
///
/// On non-Linux or with neither feature enabled, this trait is just a
/// blanket renaming of [`mio::event::Source`] and has no additional
/// requirements.
///
/// [`registration_raw_fd`]: RegistrationSource::registration_raw_fd
/// [`AsRawFd`]: std::os::fd::AsRawFd
pub(crate) trait RegistrationSource: Source {
    /// Return the raw fd to register with the vtable-routed reactor.
    /// Only called on Linux with `io-uring-reactor` or `io-sharded-mio`
    /// enabled; other builds dead-code-eliminate it.
    #[cfg(all(
        tokio_unstable,
        any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
        feature = "rt-multi-thread",
        target_os = "linux",
    ))]
    fn registration_raw_fd(&self) -> std::os::fd::RawFd;
}

// We deliberately avoid a blanket `impl<T: Source + AsRawFd>` here because it
// would conflict (coherence-wise) with a hand-written impl for
// `mio::unix::SourceFd<'_>`: the compiler notes that an upstream crate could
// add an `AsRawFd` impl for `SourceFd<'_>` in the future. Instead, we
// enumerate the concrete types Tokio actually wraps with `PollEvented` /
// `Registration`.
#[cfg(all(
    tokio_unstable,
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
))]
mod registration_source_impls {
    use super::RegistrationSource;
    use std::os::fd::{AsRawFd, RawFd};

    macro_rules! impl_registration_source_via_asrawfd {
        ($($ty:ty),* $(,)?) => {$(
            impl RegistrationSource for $ty {
                fn registration_raw_fd(&self) -> RawFd {
                    AsRawFd::as_raw_fd(self)
                }
            }
        )*};
    }

    // mio::net::* types used by tokio::net (gated behind `net` in Cargo.toml,
    // but this module is already under that same cfg via the wider feature
    // set).
    #[cfg(feature = "net")]
    impl_registration_source_via_asrawfd! {
        mio::net::TcpStream,
        mio::net::TcpListener,
        mio::net::UdpSocket,
        mio::net::UnixStream,
        mio::net::UnixListener,
        mio::net::UnixDatagram,
        mio::unix::pipe::Sender,
        mio::unix::pipe::Receiver,
    }

    // Hand-written impl for `mio::unix::SourceFd<'_>`, used by `AsyncFd`.
    // `SourceFd` does not implement `AsRawFd`, but holds a `&RawFd` directly.
    impl RegistrationSource for mio::unix::SourceFd<'_> {
        fn registration_raw_fd(&self) -> RawFd {
            *self.0
        }
    }
}

// Builds without the vtable-routed backends: no fd accessor, just a
// rename of `Source`.
#[cfg(not(all(
    tokio_unstable,
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
)))]
impl<T: Source> RegistrationSource for T {}

cfg_io_driver! {
    /// Associates an I/O resource with the reactor instance that drives it.
    ///
    /// A registration represents an I/O resource registered with a Reactor such
    /// that it will receive task notifications on readiness. This is the lowest
    /// level API for integrating with a reactor.
    ///
    /// The association between an I/O resource is made by calling
    /// [`new_with_interest_and_handle`].
    /// Once the association is established, it remains established until the
    /// registration instance is dropped.
    ///
    /// A registration instance represents two separate readiness streams. One
    /// for the read readiness and one for write readiness. These streams are
    /// independent and can be consumed from separate tasks.
    ///
    /// **Note**: while `Registration` is `Sync`, the caller must ensure that
    /// there are at most two tasks that use a registration instance
    /// concurrently. One task for [`poll_read_ready`] and one task for
    /// [`poll_write_ready`]. While violating this requirement is "safe" from a
    /// Rust memory safety point of view, it will result in unexpected behavior
    /// in the form of lost notifications and tasks hanging.
    ///
    /// ## Platform-specific events
    ///
    /// `Registration` also allows receiving platform-specific `mio::Ready`
    /// events. These events are included as part of the read readiness event
    /// stream. The write readiness event stream is only for `Ready::writable()`
    /// events.
    ///
    /// ## Lazy first-poll registration (vtable backends only)
    ///
    /// On the legacy mio driver path, `new_with_interest_and_handle`
    /// eagerly calls `add_source` so the kernel-side state is live by
    /// the time the constructor returns. On the vtable-routed backends
    /// (`io-uring-reactor` / `io-sharded-mio`) the constructor only
    /// stashes `(fd, interest)` and the actual driver work happens on
    /// the first `poll_ready` / `try_io` / `readiness` call, on
    /// whichever worker is polling. See `io-driver-vtable.md` for
    /// rationale.
    ///
    /// [`new_with_interest_and_handle`]: method@Self::new_with_interest_and_handle
    /// [`poll_read_ready`]: method@Self::poll_read_ready`
    /// [`poll_write_ready`]: method@Self::poll_write_ready`
    #[derive(Debug)]
    pub(crate) struct Registration {
        /// Handle to the associated runtime.
        ///
        /// TODO: this can probably be moved into `ScheduledIo`.
        handle: scheduler::Handle,

        /// Reference to state stored by the driver.
        ///
        /// On the legacy mio path this is populated at construction.
        /// On the vtable-routed backends it is populated lazily on the
        /// first poll once `register_local` succeeds; before that, the
        /// `OnceLock` is empty and the registration's poll/try_io
        /// methods route through `register_if_needed` first.
        shared: std::sync::OnceLock<Arc<ScheduledIo>>,

        /// Lazy registration state for the vtable-routed backends.
        /// `None` on the legacy mio path (registration is eager).
        #[cfg(all(
            tokio_unstable,
            any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
            feature = "rt-multi-thread",
            target_os = "linux",
        ))]
        lazy: Option<LazyState>,
    }

    /// Stash of the inputs needed to register on first poll.
    /// Only populated when the runtime uses a vtable-routed backend
    /// (sharded-mio or uring); the legacy mio path constructs
    /// `Registration` with `lazy: None` and `shared` already set.
    #[cfg(all(
        tokio_unstable,
        any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
        feature = "rt-multi-thread",
        target_os = "linux",
    ))]
    #[derive(Debug)]
    struct LazyState {
        fd: std::os::fd::RawFd,
        interest: Interest,
        /// Cached error kind from a failed first-poll register.
        /// Subsequent polls surface the same error rather than
        /// retrying registration.
        error: std::sync::OnceLock<io::ErrorKind>,
    }
}

unsafe impl Send for Registration {}
unsafe impl Sync for Registration {}

// ===== impl Registration =====

impl Registration {
    /// Registers the I/O resource with the reactor for the provided handle, for
    /// a specific `Interest`. This does not add `hup` or `error` so if you are
    /// interested in those states, you will need to add them to the readiness
    /// state passed to this function.
    ///
    /// # Return
    ///
    /// - `Ok` if the registration happened successfully
    /// - `Err` if an error was encountered during registration
    #[track_caller]
    pub(crate) fn new_with_interest_and_handle(
        io: &mut impl RegistrationSource,
        interest: Interest,
        handle: scheduler::Handle,
    ) -> io::Result<Registration> {
        // When the runtime was built with a non-traditional flavor
        // (`enable_uring_reactor()` / `enable_sharded_mio()`), route
        // the registration through the backend-agnostic vtable. The
        // vtable-routed backends are *lazy*: we don't actually call
        // `register_local` here — we just record `(fd, interest)` and
        // let the first poll do the registration on whichever worker
        // happens to be driving it.
        #[cfg(all(
            tokio_unstable,
            any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
            feature = "rt-multi-thread",
            target_os = "linux",
        ))]
        if handle.io_driver().is_some() {
            let fd = io.registration_raw_fd();
            return Ok(Registration {
                handle,
                shared: std::sync::OnceLock::new(),
                lazy: Some(LazyState {
                    fd,
                    interest,
                    error: std::sync::OnceLock::new(),
                }),
            });
        }

        let shared = handle.driver().io().add_source(io, interest)?;

        let once = std::sync::OnceLock::new();
        // `set` cannot fail on a fresh `OnceLock`.
        let _ = once.set(shared);
        Ok(Registration {
            handle,
            shared: once,
            #[cfg(all(
                tokio_unstable,
                any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
                feature = "rt-multi-thread",
                target_os = "linux",
            ))]
            lazy: None,
        })
    }

    /// Ensure `self.shared` is populated. On the legacy mio path this
    /// is a cheap `OnceLock::get` (always populated at construction).
    /// On the vtable-routed backends, the first call drives the
    /// driver-side registration via `register_local`.
    #[cfg(all(
        tokio_unstable,
        any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
        feature = "rt-multi-thread",
        target_os = "linux",
    ))]
    fn register_if_needed(&self) -> io::Result<&Arc<ScheduledIo>> {
        use crate::runtime::io::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.rin_calls);

        if let Some(shared) = self.shared.get() {
            bump(&COUNTERS.rin_shared_hit);
            return Ok(shared);
        }

        // Lazy path. `lazy` is `Some` whenever we constructed the
        // registration through the vtable-routed branch.
        let lazy = match &self.lazy {
            Some(l) => l,
            None => {
                bump(&COUNTERS.rin_no_lazy);
                // Defensive: the only way to reach `register_if_needed`
                // with `shared` empty is via the lazy path. If we got
                // here without `lazy`, surface a generic error rather
                // than panicking.
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "registration is empty (no driver routing)",
                ));
            }
        };

        if let Some(kind) = lazy.error.get() {
            bump(&COUNTERS.rin_error_cached);
            return Err(io::Error::from(*kind));
        }

        let driver = match self.handle.io_driver() {
            Some(d) => d,
            None => {
                bump(&COUNTERS.rin_no_driver);
                let kind = io::ErrorKind::Other;
                let _ = lazy.error.set(kind);
                return Err(io::Error::new(
                    kind,
                    "io driver not available for registration",
                ));
            }
        };

        bump(&COUNTERS.rin_register_call);
        let arc = driver.allocate_scheduled_io();
        if let Err(e) = driver.register_local(&arc, lazy.fd, lazy.interest) {
            bump(&COUNTERS.rin_register_err);
            let kind = e.kind();
            let _ = lazy.error.set(kind);
            return Err(e);
        }

        // Multiple poll callers can race here; whichever wins owns the
        // canonical Arc, the others discard their freshly-registered
        // alternate. To avoid a registered-but-unowned `ScheduledIo`,
        // a clean implementation would synchronize the registration
        // step itself, but in practice `Registration` is touched by
        // at most two tasks (read + write halves) and the first poll
        // is a single rare event, so a small probability of a duplicate
        // register that is immediately deregistered is acceptable.
        if let Err(_other) = self.shared.set(Arc::clone(&arc)) {
            bump(&COUNTERS.rin_race_loser);
            // Another caller beat us. Deregister our redundant
            // registration to avoid leaking it.
            //
            // Best-effort: build a `SourceFd` over the same fd and
            // route through the vtable's `deregister`. Errors are
            // ignored — at worst the kernel keeps a stale interest
            // entry until the fd is closed.
            let mut source = mio::unix::SourceFd(&lazy.fd);
            let _ = driver.deregister(&arc, &mut source);
        }

        bump(&COUNTERS.rin_success);
        Ok(self.shared.get().expect("shared populated above"))
    }

    /// Legacy-only variant: on builds without any vtable backend the
    /// registration is always eager, so `shared` is always populated.
    #[cfg(not(all(
        tokio_unstable,
        any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
        feature = "rt-multi-thread",
        target_os = "linux",
    )))]
    fn register_if_needed(&self) -> io::Result<&Arc<ScheduledIo>> {
        Ok(self.shared.get().expect("eager registration populated"))
    }

    /// Deregisters the I/O resource from the reactor it is associated with.
    ///
    /// This function must be called before the I/O resource associated with the
    /// registration is dropped.
    ///
    /// Note that deregistering does not guarantee that the I/O resource can be
    /// registered with a different reactor. Some I/O resource types can only be
    /// associated with a single reactor instance for their lifetime.
    ///
    /// # Return
    ///
    /// If the deregistration was successful, `Ok` is returned. Any calls to
    /// `Reactor::turn` that happen after a successful call to `deregister` will
    /// no longer result in notifications getting sent for this registration.
    ///
    /// `Err` is returned if an error is encountered.
    pub(crate) fn deregister(&mut self, io: &mut impl RegistrationSource) -> io::Result<()> {
        // Same backend-agnostic vtable path as `new_with_interest_and_handle`.
        // The uring shim ignores `io` (POLL_REMOVE is keyed off the
        // `ScheduledIo`'s `uring_worker` field, which the shim reads
        // internally) and the sharded-mio shim forwards `io`'s fd
        // through into a queued deregister op.
        #[cfg(all(
            tokio_unstable,
            any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
            feature = "rt-multi-thread",
            target_os = "linux",
        ))]
        if let Some(driver) = self.handle.io_driver() {
            // Lazy path: if `register_local` never ran, there's
            // nothing in the kernel registry to remove and no
            // ScheduledIo in the per-shard set. Just succeed.
            if let Some(shared) = self.shared.get() {
                return driver.deregister(shared, io);
            }
            return Ok(());
        }

        let shared = self
            .shared
            .get()
            .expect("legacy mio path always populates shared");
        self.handle().deregister_source(shared, io)
    }

    pub(crate) fn clear_readiness(&self, event: ReadyEvent) {
        if let Some(shared) = self.shared.get() {
            shared.clear_readiness(event);
        }
        // Lazy path with no registration yet: nothing to clear. The
        // first `poll_ready` call after this will kick off
        // registration; until then there is no readiness state.
    }

    // Uses the poll path, requiring the caller to ensure mutual exclusion for
    // correctness. Only the last task to call this function is notified.
    pub(crate) fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<ReadyEvent>> {
        self.poll_ready(cx, Direction::Read)
    }

    // Uses the poll path, requiring the caller to ensure mutual exclusion for
    // correctness. Only the last task to call this function is notified.
    pub(crate) fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<ReadyEvent>> {
        self.poll_ready(cx, Direction::Write)
    }

    // Uses the poll path, requiring the caller to ensure mutual exclusion for
    // correctness. Only the last task to call this function is notified.
    #[cfg(not(all(target_os = "wasi", target_env = "p1")))]
    pub(crate) fn poll_read_io<R>(
        &self,
        cx: &mut Context<'_>,
        f: impl FnMut() -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        self.poll_io(cx, Direction::Read, f)
    }

    // Uses the poll path, requiring the caller to ensure mutual exclusion for
    // correctness. Only the last task to call this function is notified.
    pub(crate) fn poll_write_io<R>(
        &self,
        cx: &mut Context<'_>,
        f: impl FnMut() -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        self.poll_io(cx, Direction::Write, f)
    }

    /// Polls for events on the I/O resource's `direction` readiness stream.
    ///
    /// If called with a task context, notify the task when a new event is
    /// received.
    fn poll_ready(
        &self,
        cx: &mut Context<'_>,
        direction: Direction,
    ) -> Poll<io::Result<ReadyEvent>> {
        ready!(crate::trace::trace_leaf(cx));
        // Keep track of task budget
        let coop = ready!(crate::task::coop::poll_proceed(cx));

        // First poll triggers lazy registration on the vtable-routed
        // backends. Subsequent polls hit the populated `OnceLock` and
        // skip the work.
        let shared = match self.register_if_needed() {
            Ok(s) => s,
            Err(e) => return Poll::Ready(Err(e)),
        };

        let ev = ready!(shared.poll_readiness(cx, direction));

        if ev.is_shutdown {
            return Poll::Ready(Err(gone()));
        }

        coop.made_progress();
        Poll::Ready(Ok(ev))
    }

    fn poll_io<R>(
        &self,
        cx: &mut Context<'_>,
        direction: Direction,
        mut f: impl FnMut() -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        loop {
            let ev = ready!(self.poll_ready(cx, direction))?;

            match f() {
                Ok(ret) => {
                    return Poll::Ready(Ok(ret));
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.clear_readiness(ev);
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    pub(crate) fn try_io<R>(
        &self,
        interest: Interest,
        f: impl FnOnce() -> io::Result<R>,
    ) -> io::Result<R> {
        // First call triggers lazy registration. After that the
        // populated `OnceLock` makes this a cheap pointer load.
        let shared = self.register_if_needed()?;

        let ev = shared.ready_event(interest);

        // Don't attempt the operation if the resource is not ready.
        if ev.ready.is_empty() {
            return Err(io::ErrorKind::WouldBlock.into());
        }

        match f() {
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.clear_readiness(ev);
                Err(io::ErrorKind::WouldBlock.into())
            }
            res => res,
        }
    }

    pub(crate) async fn readiness(&self, interest: Interest) -> io::Result<ReadyEvent> {
        let shared = self.register_if_needed()?;

        let ev = shared.readiness(interest).await;

        if ev.is_shutdown {
            return Err(gone());
        }

        Ok(ev)
    }

    pub(crate) async fn async_io<R>(
        &self,
        interest: Interest,
        mut f: impl FnMut() -> io::Result<R>,
    ) -> io::Result<R> {
        loop {
            let event = self.readiness(interest).await?;

            let coop = std::future::poll_fn(crate::task::coop::poll_proceed).await;

            match f() {
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.clear_readiness(event);
                }
                x => {
                    coop.made_progress();
                    return x;
                }
            }
        }
    }

    fn handle(&self) -> &Handle {
        self.handle.driver().io()
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        // It is possible for a cycle to be created between wakers stored in
        // `ScheduledIo` instances and `Arc<driver::Inner>`. To break this
        // cycle, wakers are cleared. This is an imperfect solution as it is
        // possible to store a `Registration` in a waker. In this case, the
        // cycle would remain.
        //
        // See tokio-rs/tokio#3481 for more details.
        if let Some(shared) = self.shared.get() {
            shared.clear_wakers();
        }
    }
}

fn gone() -> io::Error {
    io::Error::new(
        io::ErrorKind::Other,
        crate::util::error::RUNTIME_SHUTTING_DOWN_ERROR,
    )
}
