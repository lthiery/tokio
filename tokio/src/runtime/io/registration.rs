#![cfg_attr(not(feature = "net"), allow(dead_code))]

use crate::io::interest::Interest;
use crate::runtime::io::{Direction, ReadyEvent, ScheduledIo};
use crate::runtime::scheduler;

use mio::event::Source;
use std::io;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

/// Source trait bound used by `Registration::new_with_interest`.
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
    /// Required on unix; non-unix builds keep the eager `add_source`
    /// path in `Registration::new_with_interest` and never call this.
    #[cfg(target_family = "unix")]
    fn registration_raw_fd(&self) -> std::os::fd::RawFd;
}

// We deliberately avoid a blanket `impl<T: Source + AsRawFd>` here because it
// would conflict (coherence-wise) with a hand-written impl for
// `mio::unix::SourceFd<'_>`: the compiler notes that an upstream crate could
// add an `AsRawFd` impl for `SourceFd<'_>` in the future. Instead, we
// enumerate the concrete types Tokio actually wraps with `PollEvented` /
// `Registration`.
#[cfg(target_family = "unix")]
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

// Non-unix builds: no fd accessor, just a blanket rename of `Source`.
// `Registration::new_with_interest` stays on the eager `add_source`
// path on these targets (Windows etc.).
#[cfg(not(target_family = "unix"))]
impl<T: Source> RegistrationSource for T {}

cfg_io_driver! {
    /// Associates an I/O resource with the reactor instance that drives it.
    ///
    /// A registration represents an I/O resource registered with a Reactor such
    /// that it will receive task notifications on readiness. This is the lowest
    /// level API for integrating with a reactor.
    ///
    /// The association between an I/O resource is made by calling
    /// [`new_with_interest`].
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
    /// ## Lazy first-poll registration (unix targets)
    ///
    /// On unix, `new_with_interest` does **no** runtime lookup at
    /// all: it stashes `(fd, interest)` and the actual driver work
    /// — `Handle::current()`, allocation of `ScheduledIo`, and
    /// `register_local` — happens on the first `poll_ready` /
    /// `try_io` / `readiness` call, on whichever worker is polling.
    /// This is what allows `TcpStream::from_std` (and peers) to be
    /// constructed from any thread, with or without a runtime in
    /// scope. The panic-on-no-runtime is deferred to the first poll.
    ///
    /// On non-unix targets (Windows etc.) the legacy `add_source`
    /// path is still eager, since `mio::unix::SourceFd` is not
    /// available there. See `io-driver-vtable.md` for the design.
    ///
    /// [`new_with_interest`]: method@Self::new_with_interest
    /// [`poll_read_ready`]: method@Self::poll_read_ready`
    /// [`poll_write_ready`]: method@Self::poll_write_ready`
    #[derive(Debug)]
    pub(crate) struct Registration {
        /// Handle to the associated runtime. On unix this OnceLock
        /// is populated lazily by `ensure_registered` on the first
        /// poll, alongside `shared`. On non-unix targets it's
        /// populated eagerly by `new_with_interest`.
        ///
        /// We cache it (rather than calling `Handle::current()` again
        /// at deregister time) because `Drop` can run outside a
        /// runtime context (e.g. after `Runtime::shutdown`), so a TLS
        /// lookup at that point is unsafe. Invariant: if
        /// `shared.get().is_some()` then `handle.get().is_some()` —
        /// `ensure_registered` sets `handle` first, then `shared`,
        /// and the OnceLock release/acquire ordering carries the
        /// dependency.
        handle: HandleSlot,

        /// Reference to state stored by the driver.
        ///
        /// On unix this is populated lazily on the first poll once
        /// `register_local` succeeds; before that, the `OnceLock`
        /// is empty and the registration's poll/try_io methods route
        /// through `ensure_registered` first. On non-unix targets
        /// this is populated eagerly by `new_with_interest`.
        shared: std::sync::OnceLock<Arc<ScheduledIo>>,

        // ---- Unix-only lazy-path fields ----
        //
        // These fields are only present on unix targets where the
        // vtable's `register_local` (which takes a `RawFd`) is
        // reachable. Non-unix targets keep the eager `add_source`
        // path and don't need them.

        /// Raw fd captured at construction. The lazy path doesn't
        /// keep the original `Source` borrow around, so the fd is
        /// recorded directly so first-poll register and race-loser
        /// deregister can fabricate a `mio::unix::SourceFd` over
        /// the same fd.
        #[cfg(target_family = "unix")]
        fd: std::os::fd::RawFd,

        /// Interest captured at construction; used by first-poll
        /// register.
        #[cfg(target_family = "unix")]
        interest: Interest,

        /// Cached error kind from a failed first-poll register.
        /// Subsequent polls surface the same error rather than
        /// retrying registration. Unused on non-unix (errors there
        /// are surfaced eagerly from `new_with_interest`).
        #[cfg(target_family = "unix")]
        first_poll_error: std::sync::OnceLock<io::ErrorKind>,
    }

    /// Storage for the scheduler handle. Unified across targets as
    /// a `OnceLock`: the unix lazy first-poll path populates it on
    /// first poll, the non-unix eager constructor populates it at
    /// construction time. Either way, by the time `Drop` runs, the
    /// slot is `Some` iff the registration was ever attached to a
    /// runtime.
    type HandleSlot = std::sync::OnceLock<scheduler::Handle>;
}

unsafe impl Send for Registration {}
unsafe impl Sync for Registration {}

// ===== impl Registration =====

impl Registration {
    /// Registers the I/O resource with the reactor for a specific
    /// `Interest`. This does not add `hup` or `error` so if you are
    /// interested in those states, you will need to add them to the
    /// readiness state passed to this function.
    ///
    /// # Return
    ///
    /// - `Ok` if the registration was scheduled successfully. On
    ///   unix this only stashes `(fd, interest)` — the actual
    ///   driver work happens on the first poll. On non-unix this
    ///   means the kernel-side registration already ran.
    /// - `Err` if an error was encountered during registration. On
    ///   unix, registration errors are deferred to the first poll
    ///   along with the registration itself, so this constructor
    ///   never returns `Err` on unix targets.
    ///
    /// # Panics
    ///
    /// On unix targets, this constructor does **not** consult any
    /// runtime; if no Tokio runtime is set in thread-local storage,
    /// the panic is deferred to the first poll. This is true for
    /// every backend (legacy mio, sharded-mio, io-uring). On non-unix
    /// targets the legacy `add_source` path is still eager, so the
    /// panic remains at construction.
    #[track_caller]
    pub(crate) fn new_with_interest(
        io: &mut impl RegistrationSource,
        interest: Interest,
    ) -> io::Result<Registration> {
        // Unix: fully lazy. No runtime lookup, no allocation, no
        // driver-side work — just stash `(fd, interest)`; the first
        // poll's `ensure_registered` does the rest.
        #[cfg(target_family = "unix")]
        {
            let fd = io.registration_raw_fd();
            return Ok(Registration {
                handle: std::sync::OnceLock::new(),
                shared: std::sync::OnceLock::new(),
                fd,
                interest,
                first_poll_error: std::sync::OnceLock::new(),
            });
        }

        // Non-unix (Windows etc.): eager `add_source`. The vtable's
        // `register_local` shim relies on `mio::unix::SourceFd`,
        // which doesn't exist off-unix, so the legacy path stays as
        // the only option here. Internalises the `Handle::current()`
        // lookup callers used to do themselves.
        #[cfg(not(target_family = "unix"))]
        {
            let handle = scheduler::Handle::current();
            let shared = handle.driver().io().add_source(io, interest)?;
            let shared_once = std::sync::OnceLock::new();
            let handle_once = std::sync::OnceLock::new();
            // `set` cannot fail on a fresh `OnceLock`.
            let _ = shared_once.set(shared);
            let _ = handle_once.set(handle);
            Ok(Registration {
                handle: handle_once,
                shared: shared_once,
            })
        }
    }

    /// Ensure `self.shared` is populated. On unix this drives the
    /// vtable's `allocate_scheduled_io` + `register_local` on the
    /// first call; subsequent calls hit the cached `OnceLock`. On
    /// non-unix the `OnceLock` is always populated by the eager
    /// `new_with_interest` constructor, so this is a cheap get.
    ///
    /// Must be called from inside a poll context: on unix this is
    /// where the `Handle::current()` lookup happens, so calling
    /// `ensure_registered` on a registration whose home runtime has
    /// gone away will surface that as an error (or, if no runtime
    /// is in TLS at all, panic via `Handle::current`).
    #[cfg(target_family = "unix")]
    fn ensure_registered(&self) -> io::Result<&Arc<ScheduledIo>> {
        use crate::runtime::io::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.rin_calls);

        if let Some(shared) = self.shared.get() {
            bump(&COUNTERS.rin_shared_hit);
            return Ok(shared);
        }

        if let Some(kind) = self.first_poll_error.get() {
            bump(&COUNTERS.rin_error_cached);
            return Err(io::Error::from(*kind));
        }

        // First poll: locate the runtime in TLS. This is where the
        // construction-time-vs-first-poll panic boundary lives —
        // `Handle::current()` panics if no runtime is in scope.
        // Callers (TcpStream::from_std etc.) document this on their
        // public surface.
        let handle = crate::runtime::scheduler::Handle::current();

        // Stash the scheduler handle BEFORE publishing `shared`. Any
        // reader that observes `shared.get().is_some()` is guaranteed
        // to also see `handle.get().is_some()` thanks to the OnceLock
        // release/acquire pair. This invariant lets `Drop` deregister
        // without re-doing a `Handle::current()` lookup, which is
        // essential because Drop can run after `Runtime::shutdown`.
        //
        // The `OnceLock::set` may lose to a racing first-poll;
        // that's fine — the loser's clone is dropped and both
        // outcomes resolve to the same scheduler.
        let _ = self.handle.set(handle.clone());

        bump(&COUNTERS.rin_register_call);

        // Every io-enabled runtime now exposes an `IoDriver`:
        // multi_thread + `IoFlavor::Traditional` and current_thread
        // both carry `LEGACY_MIO_VTABLE`, the per-worker flavors carry
        // `URING_VTABLE` / `SHARDED_MIO_VTABLE`. If `io_driver()`
        // returns `None`, the runtime was built with io disabled —
        // that path panics on the way in (mirrors the pre-vtable
        // panic from `driver().io()`).
        let driver = handle
            .io_driver()
            .expect("io driver present when io is enabled");
        let arc = driver.allocate_scheduled_io();
        if let Err(e) = driver.register_local(&arc, self.fd, self.interest) {
            bump(&COUNTERS.rin_register_err);
            let kind = e.kind();
            let _ = self.first_poll_error.set(kind);
            return Err(e);
        }

        // Multiple poll callers can race here; whichever wins owns the
        // canonical Arc, the others discard their freshly-registered
        // alternate. The losers deregister their redundant
        // `ScheduledIo` to avoid a registered-but-unowned slot
        // lingering in the reactor. Same vtable as the register
        // call above — ignore any error, at worst the kernel keeps
        // a stale interest entry until the fd is closed.
        if let Err(_other) = self.shared.set(Arc::clone(&arc)) {
            bump(&COUNTERS.rin_race_loser);
            let mut source = mio::unix::SourceFd(&self.fd);
            let _ = driver.deregister(&arc, &mut source);
        }

        bump(&COUNTERS.rin_success);
        Ok(self.shared.get().expect("shared populated above"))
    }

    /// Non-unix variant: registration is always eager
    /// (`new_with_interest` populated `shared`), so this is a cheap
    /// get.
    #[cfg(not(target_family = "unix"))]
    fn ensure_registered(&self) -> io::Result<&Arc<ScheduledIo>> {
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
        // The legacy mio eager constructor populates both `handle`
        // and `shared` at construction time; the vtable-routed
        // lazy path populates them on first poll. Either way, if
        // both slots are `None` here, the registration was never
        // attached to a runtime — nothing in the reactor to remove,
        // just succeed.
        if let (Some(handle), Some(shared)) = (self.handle.get(), self.shared.get()) {
            let driver = handle
                .io_driver()
                .expect("io driver present when io is enabled");
            return driver.deregister(shared, io);
        }
        Ok(())
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
        let shared = match self.ensure_registered() {
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
        let shared = self.ensure_registered()?;

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
        let shared = self.ensure_registered()?;

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
