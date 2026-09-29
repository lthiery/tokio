#![cfg_attr(not(feature = "net"), allow(dead_code))]

use crate::io::interest::Interest;
use crate::io::ready::Ready;
use crate::runtime::io::{Direction, Handle, ReadyEvent, ScheduledIo};
use crate::runtime::scheduler;

use mio::event::Source;
use std::io;
use std::sync::{Arc, OnceLock};
use std::task::{ready, Context, Poll};

#[cfg(unix)]
use crate::loom::sync::Mutex;
#[cfg(unix)]
use mio::unix::SourceFd;
#[cfg(unix)]
use std::os::unix::io::RawFd;

cfg_io_driver! {
    /// Associates an I/O resource with the reactor instance that drives it.
    ///
    /// A registration represents an I/O resource registered with a Reactor such
    /// that it will receive task notifications on readiness. This is the lowest
    /// level API for integrating with a reactor.
    ///
    /// The association between an I/O resource is made by calling
    /// [`new_with_interest_and_handle`], or on first use for a registration
    /// created with [`deferred`].
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
    /// [`new_with_interest_and_handle`]: method@Self::new_with_interest_and_handle
    /// [`deferred`]: method@Self::deferred
    /// [`poll_read_ready`]: method@Self::poll_read_ready`
    /// [`poll_write_ready`]: method@Self::poll_write_ready`
    #[derive(Debug)]
    pub(crate) struct Registration {
        /// Handle to the associated runtime.
        ///
        /// TODO: this can probably be moved into `ScheduledIo`.
        handle: scheduler::Handle,

        /// Reference to state stored by the driver. Empty until the resource
        /// is registered, which a deferred registration does on first use.
        shared: OnceLock<Arc<ScheduledIo>>,

        /// How to register the resource, for a registration that does so on
        /// first use. `None` for a resource registered at construction. The
        /// lock serializes the registration between concurrent first users
        /// (the read and write halves of a split stream).
        #[cfg(unix)]
        deferred: Mutex<Option<Deferred>>,
    }
}

/// A resource whose registration with the driver is pending its first use.
#[cfg(unix)]
#[derive(Debug)]
struct Deferred {
    /// The resource is registered (and deregistered) through this descriptor
    /// rather than through `mio::event::Source`, which needs `&mut` and is
    /// not available from a poll.
    fd: RawFd,
    interest: Interest,
    /// Readiness to assume once registered; see `assume_ready`.
    assume_ready: Ready,
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
        io: &mut impl Source,
        interest: Interest,
        handle: scheduler::Handle,
    ) -> io::Result<Registration> {
        let shared = handle.driver().io().add_source(io, interest)?;

        Ok(Registration {
            handle,
            shared: OnceLock::from(shared),
            #[cfg(unix)]
            deferred: Mutex::new(None),
        })
    }

    /// Creates a registration that registers the resource with the reactor on
    /// first use instead of now.
    ///
    /// The first readiness poll, `try_io`, or `readiness` on this registration
    /// registers `fd` with the given handle's reactor, from the task that
    /// makes it. Errors from that registration are returned by the operation
    /// that triggered it, and the next operation tries again. A resource that
    /// is never used is never registered.
    ///
    /// `assume_ready` is applied as soon as the resource is registered, so the
    /// first operation tries the resource instead of waiting for the driver's
    /// first event; see [`assume_ready`](Self::assume_ready).
    ///
    /// # Panics
    ///
    /// Panics if the handle's runtime has no I/O driver, like
    /// [`new_with_interest_and_handle`](Self::new_with_interest_and_handle).
    #[cfg(unix)]
    #[track_caller]
    pub(crate) fn deferred(
        fd: RawFd,
        interest: Interest,
        assume_ready: Ready,
        handle: scheduler::Handle,
    ) -> Registration {
        // Keep the "IO is disabled" panic at construction time, where it is
        // documented, rather than at the first read.
        let _ = handle.driver().io();

        Registration {
            handle,
            shared: OnceLock::new(),
            deferred: Mutex::new(Some(Deferred {
                fd,
                interest,
                assume_ready,
            })),
        }
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
    pub(crate) fn deregister(&mut self, io: &mut impl Source) -> io::Result<()> {
        let Some(shared) = self.shared.get() else {
            // Never registered, so there is nothing to deregister.
            return Ok(());
        };

        // A deferred registration went through the raw descriptor, so
        // deregister the same way: in debug builds, mio checks that a source
        // is deregistered from the registry it was registered with.
        #[cfg(unix)]
        if let Some(deferred) = &*self.deferred.lock() {
            return self
                .handle()
                .deregister_source(shared, &mut SourceFd(&deferred.fd));
        }

        self.handle().deregister_source(shared, io)
    }

    pub(crate) fn clear_readiness(&self, event: ReadyEvent) {
        self.registered().clear_readiness(event);
    }

    /// Marks the resource ready without waiting for the driver's first event.
    /// Used for sockets whose state is known when they are created: accepted
    /// sockets are writable and usually already hold the peer's first bytes
    /// (under load that first event can queue behind every established
    /// connection's events), and both ends of a fresh `pair()` are writable.
    /// A wrong guess costs one `WouldBlock`, which clears the readiness again.
    ///
    /// For a deferred registration, pass the readiness to
    /// [`deferred`](Self::deferred) instead.
    #[cfg(feature = "net")]
    pub(crate) fn assume_ready(&self, ready: Ready) {
        self.registered().assume_ready(ready);
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
        let ev = ready!(self.shared()?.poll_readiness(cx, direction));

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
        let ev = self.shared()?.ready_event(interest);

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
        let ev = self.shared()?.readiness(interest).await;

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

    /// The driver state for this resource, registering the resource first if
    /// the registration was deferred.
    fn shared(&self) -> io::Result<&Arc<ScheduledIo>> {
        if let Some(shared) = self.shared.get() {
            return Ok(shared);
        }

        self.register_deferred()
    }

    #[cfg(unix)]
    #[cold]
    fn register_deferred(&self) -> io::Result<&Arc<ScheduledIo>> {
        let deferred = self.deferred.lock();

        // Another task may have registered the resource while this one
        // waited for the lock.
        if let Some(shared) = self.shared.get() {
            return Ok(shared);
        }

        let Deferred {
            fd,
            interest,
            assume_ready,
        } = deferred
            .as_ref()
            .expect("a registration is either registered or deferred");

        let shared = self.handle().add_source(&mut SourceFd(fd), *interest)?;
        shared.assume_ready(*assume_ready);

        // The lock is held and `shared` was empty above, so this stores ours.
        Ok(self.shared.get_or_init(|| shared))
    }

    #[cfg(not(unix))]
    fn register_deferred(&self) -> io::Result<&Arc<ScheduledIo>> {
        unreachable!("deferred registration is only available on unix")
    }

    /// The driver state for a resource known to be registered: readiness
    /// events only come from registered resources.
    fn registered(&self) -> &Arc<ScheduledIo> {
        self.shared
            .get()
            .expect("readiness event for an unregistered resource")
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
    io::Error::other(crate::util::error::RUNTIME_SHUTTING_DOWN_ERROR)
}
