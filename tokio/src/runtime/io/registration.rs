#![cfg_attr(not(feature = "net"), allow(dead_code))]

use crate::io::interest::Interest;
use crate::runtime::io::{Direction, ReadyEvent, ScheduledIo};
// `Handle` (the io-driver handle, distinct from `scheduler::Handle`) is only
// referenced by the legacy-only `handle()` helper. Importing it
// unconditionally produces an unused-import warning under the vtable cfg.
#[cfg(not(all(
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
)))]
use crate::runtime::io::Handle;
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
    /// Only called on Linux with `io-uring-reactor` or `io-sharded-mio`
    /// enabled; other builds dead-code-eliminate it.
    #[cfg(all(
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
    /// ## Lazy first-poll registration (vtable backends only)
    ///
    /// On the legacy mio driver path, `new_with_interest`
    /// eagerly calls `add_source` so the kernel-side state is live by
    /// the time the constructor returns. On the vtable-routed backends
    /// (`io-uring-reactor` / `io-sharded-mio`) the constructor does
    /// **no** runtime lookup at all: it stashes `(fd, interest)` and
    /// the actual driver work — `Handle::current()`, allocation of
    /// `ScheduledIo`, and `register_local` — happens on the first
    /// `poll_ready` / `try_io` / `readiness` call, on whichever worker
    /// is polling. This is what allows `TcpStream::from_std` (and
    /// peers) to be constructed from any thread, with or without a
    /// runtime in scope, on vtable-routed builds. See
    /// `io-driver-vtable.md` for rationale.
    ///
    /// [`new_with_interest`]: method@Self::new_with_interest
    /// [`poll_read_ready`]: method@Self::poll_read_ready`
    /// [`poll_write_ready`]: method@Self::poll_write_ready`
    #[derive(Debug)]
    pub(crate) struct Registration {
        /// Handle to the associated runtime. Populated eagerly at
        /// construction on the legacy mio path; on the vtable-routed
        /// backends this OnceLock is populated lazily by
        /// `ensure_registered` on the first poll, alongside `shared`.
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
        /// On the legacy mio path this is populated at construction.
        /// On the vtable-routed backends it is populated lazily on the
        /// first poll once `register_local` succeeds; before that, the
        /// `OnceLock` is empty and the registration's poll/try_io
        /// methods route through `ensure_registered` first.
        shared: std::sync::OnceLock<Arc<ScheduledIo>>,

        // ---- Vtable-routed backend fields ----
        //
        // These fields are only present on builds with a vtable-routed
        // backend enabled. The legacy mio path doesn't carry them
        // (registration is eager and the backend handle is the
        // eagerly-stored scheduler handle).

        /// Raw fd captured at construction. The vtable-routed backends
        /// don't keep the original `Source` borrow around, so the fd
        /// is recorded directly so first-poll register and Drop-time
        /// deregister can fabricate a `mio::unix::SourceFd` over the
        /// same fd.
        #[cfg(all(
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
))]
        fd: std::os::fd::RawFd,

        /// Interest captured at construction; used by first-poll
        /// register.
        #[cfg(all(
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
))]
        interest: Interest,

        /// Cached error kind from a failed first-poll register.
        /// Subsequent polls surface the same error rather than
        /// retrying registration. Empty on the legacy mio path
        /// (errors there are surfaced eagerly from `new_with_interest`).
        #[cfg(all(
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
))]
        first_poll_error: std::sync::OnceLock<io::ErrorKind>,
    }

    /// Storage for the scheduler handle. On the legacy mio path the
    /// handle is captured eagerly at construction (so the handle is
    /// always available); on the vtable-routed backends it's populated
    /// lazily by `ensure_registered`. Encoded as a single field so
    /// `Registration` doesn't need to cfg-divide its layout.
    #[cfg(not(all(
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
)))]
    type HandleSlot = scheduler::Handle;

    #[cfg(all(
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
))]
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
    /// - `Ok` if the registration was scheduled successfully. On the
    ///   legacy mio path this means the kernel-side `epoll_ctl_add`
    ///   already ran. On the vtable-routed backends this only stashes
    ///   `(fd, interest)` — the actual driver work happens on the
    ///   first poll.
    /// - `Err` if an error was encountered during registration. On the
    ///   vtable-routed backends, registration errors are deferred to
    ///   the first poll along with the registration itself, so this
    ///   constructor never returns `Err` on those paths.
    ///
    /// # Panics
    ///
    /// On the legacy mio path, panics if no Tokio runtime is set in
    /// thread-local storage. On the vtable-routed backends
    /// (`io-uring-reactor` / `io-sharded-mio`), this constructor does
    /// **not** consult any runtime; the panic-on-no-runtime moves to
    /// the first poll of the registration.
    #[track_caller]
    pub(crate) fn new_with_interest(
        io: &mut impl RegistrationSource,
        interest: Interest,
    ) -> io::Result<Registration> {
        // Vtable-routed backends are fully lazy: no runtime lookup,
        // no allocation, no driver-side work. Just stash the inputs
        // first poll will need.
        #[cfg(all(
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
))]
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

        // Legacy mio path: eager `add_source`. Internalises the
        // `Handle::current()` lookup that callers used to do
        // themselves and pass in.
        #[cfg(not(all(
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
)))]
        {
            let handle = scheduler::Handle::current();
            let shared = handle.driver().io().add_source(io, interest)?;
            let once = std::sync::OnceLock::new();
            // `set` cannot fail on a fresh `OnceLock`.
            let _ = once.set(shared);
            Ok(Registration { handle, shared: once })
        }
    }

    /// Ensure `self.shared` is populated. On the legacy mio path this
    /// is a cheap `OnceLock::get` (always populated at construction).
    /// On the vtable-routed backends, the first call drives the
    /// driver-side registration via `register_local`.
    ///
    /// Must be called from inside a poll context: this is where the
    /// vtable-routed backends do their `Handle::current()` lookup, so
    /// calling `ensure_registered` on a registration whose home
    /// runtime has gone away will surface that as an error (or, if
    /// no runtime is in TLS at all, panic via `Handle::current`).
    #[cfg(all(
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
))]
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

        // Dispatch on whether the runtime selected a vtable-routed
        // backend. `io_driver()` returns `Some` for the experimental
        // `IoFlavor::UringPerWorker` / `IoFlavor::ShardedMio`
        // configurations, and `None` for the default `Traditional`
        // mio path. The latter still needs lazy first-poll
        // registration here (the eager path was removed in
        // `new_with_interest`), so fall through to `add_source` —
        // the same routine the legacy build calls eagerly at
        // construction.
        let arc = match handle.io_driver() {
            Some(driver) => {
                let arc = driver.allocate_scheduled_io();
                if let Err(e) = driver.register_local(&arc, self.fd, self.interest) {
                    bump(&COUNTERS.rin_register_err);
                    let kind = e.kind();
                    let _ = self.first_poll_error.set(kind);
                    return Err(e);
                }
                arc
            }
            None => {
                // Traditional/Legacy fallback: vtable feature was
                // compiled in, but the runtime selected the shared
                // mio reactor. Reconstruct a `SourceFd` over the
                // captured fd so we can call the same `add_source`
                // path the eager-registration build uses.
                let mut source = mio::unix::SourceFd(&self.fd);
                match handle.driver().io().add_source(&mut source, self.interest) {
                    Ok(arc) => arc,
                    Err(e) => {
                        bump(&COUNTERS.rin_register_err);
                        let kind = e.kind();
                        let _ = self.first_poll_error.set(kind);
                        return Err(e);
                    }
                }
            }
        };

        // Multiple poll callers can race here; whichever wins owns the
        // canonical Arc, the others discard their freshly-registered
        // alternate. The losers deregister their redundant
        // `ScheduledIo` to avoid a registered-but-unowned slot
        // lingering in the reactor.
        if let Err(_other) = self.shared.set(Arc::clone(&arc)) {
            bump(&COUNTERS.rin_race_loser);
            let mut source = mio::unix::SourceFd(&self.fd);
            // Route deregistration through the same backend that
            // performed the redundant register, ignoring any error
            // — at worst the kernel keeps a stale interest entry
            // until the fd is closed.
            match handle.io_driver() {
                Some(driver) => {
                    let _ = driver.deregister(&arc, &mut source);
                }
                None => {
                    let _ = handle.driver().io().deregister_source(&arc, &mut source);
                }
            }
        }

        bump(&COUNTERS.rin_success);
        Ok(self.shared.get().expect("shared populated above"))
    }

    /// Legacy-only variant: on builds without any vtable backend the
    /// registration is always eager, so `shared` is always populated.
    #[cfg(not(all(
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
)))]
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
        // Vtable-routed feature build: read the cached scheduler
        // handle from the OnceLock populated by the first poll's
        // `ensure_registered`. If the registration was never polled,
        // `shared` and `handle` are both empty — there's nothing in
        // the reactor to remove, so just succeed. The
        // construction-time runtime detection is gone: a registration
        // that is never polled is a no-op at deregister time,
        // regardless of whether the current thread has a runtime.
        #[cfg(all(
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
))]
        {
            if let (Some(handle), Some(shared)) = (self.handle.get(), self.shared.get()) {
                // Dispatch on the same axis as `ensure_registered`:
                // vtable backend if `io_driver()` is `Some`, legacy
                // mio reactor otherwise.
                return match handle.io_driver() {
                    Some(driver) => driver.deregister(shared, io),
                    None => handle.driver().io().deregister_source(shared, io),
                };
            }
            return Ok(());
        }

        #[cfg(not(all(
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
)))]
        {
            let shared = self
                .shared
                .get()
                .expect("legacy mio path always populates shared");
            self.handle().deregister_source(shared, io)
        }
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

    #[cfg(not(all(
    any(feature = "io-uring-reactor", feature = "io-sharded-mio"),
    feature = "rt-multi-thread",
    target_os = "linux",
)))]
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
