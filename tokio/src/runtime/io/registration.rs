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
/// On Linux with the experimental `io-uring-reactor` feature enabled, the
/// uring backend needs a raw fd so it can submit `POLL_ADD_MULTI` keyed on
/// it. We expose that fd via a [`registration_raw_fd`] method rather than a
/// direct [`AsRawFd`] supertrait bound, because [`mio::unix::SourceFd<'_>`]
/// — used by `AsyncFd` — does not itself implement `AsRawFd` even though
/// it trivially holds a `RawFd`. A hand-written impl below plugs that hole.
///
/// On non-Linux or with the feature disabled, this trait is just a blanket
/// renaming of [`mio::event::Source`] and has no additional requirements.
///
/// [`registration_raw_fd`]: RegistrationSource::registration_raw_fd
/// [`AsRawFd`]: std::os::fd::AsRawFd
pub(crate) trait RegistrationSource: Source {
    /// Return the raw fd to register with the uring reactor. Only called on
    /// Linux with the `io-uring-reactor` feature enabled; other builds
    /// dead-code-eliminate it.
    #[cfg(all(
        tokio_unstable,
        feature = "io-uring-reactor",
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
    feature = "io-uring-reactor",
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

// Non-uring builds: no fd accessor, just a rename of `Source`.
#[cfg(not(all(
    tokio_unstable,
    feature = "io-uring-reactor",
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
        shared: Arc<ScheduledIo>,

        /// For the per-worker `io_uring` reactor: the raw fd under
        /// registration, cached so a later *rebind* (v2 lazy placement)
        /// can re-register the same fd on a different worker without
        /// reaching back through the `RegistrationSource`. Meaningful only
        /// when [`scheduler::Handle::uring_handle`] is `Some`.
        ///
        /// The underlying fd is owned by the caller's I/O resource (which
        /// outlives this `Registration`), so caching the raw value is
        /// sound.
        #[cfg(all(
            tokio_unstable,
            feature = "io-uring-reactor",
            feature = "rt-multi-thread",
            target_os = "linux",
        ))]
        uring_fd: std::os::fd::RawFd,

        /// Readiness interest this registration was installed with.
        /// Snapshotted because rebinds must re-issue `POLL_ADD_MULTI` with
        /// the same mask. `None` on the mio path / non-uring runtimes.
        #[cfg(all(
            tokio_unstable,
            feature = "io-uring-reactor",
            feature = "rt-multi-thread",
            target_os = "linux",
        ))]
        uring_interest: Option<Interest>,
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
        // When the runtime was built with `enable_uring_reactor()`, route
        // the registration through the per-worker uring handle instead of
        // mio. The uring path does not touch `mio::Registry`; the fd is
        // passed in raw and a `POLL_ADD_MULTI` SQE is queued for the
        // assigned worker.
        #[cfg(all(
            tokio_unstable,
            feature = "io-uring-reactor",
            feature = "rt-multi-thread",
            target_os = "linux",
        ))]
        if let Some(uring) = handle.uring_handle() {
            let fd = io.registration_raw_fd();
            let (shared, _worker_idx) = uring.add_source(fd, interest)?;
            return Ok(Registration {
                handle,
                shared,
                uring_fd: fd,
                uring_interest: Some(interest),
            });
        }

        let shared = handle.driver().io().add_source(io, interest)?;

        Ok(Registration {
            handle,
            shared,
            #[cfg(all(
                tokio_unstable,
                feature = "io-uring-reactor",
                feature = "rt-multi-thread",
                target_os = "linux",
            ))]
            uring_fd: -1,
            #[cfg(all(
                tokio_unstable,
                feature = "io-uring-reactor",
                feature = "rt-multi-thread",
                target_os = "linux",
            ))]
            uring_interest: None,
        })
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
        #[cfg(all(
            tokio_unstable,
            feature = "io-uring-reactor",
            feature = "rt-multi-thread",
            target_os = "linux",
        ))]
        if self.uring_interest.is_some() {
            // Unused `io` on the uring path: mio's `Registry::deregister`
            // is not involved — the worker will submit `POLL_REMOVE` on
            // its ring when it drains the pending-ops queue.
            let _ = io;
            if let Some(uring) = self.handle.uring_handle() {
                // Read the *current* owning worker off the `ScheduledIo`:
                // a prior rebind may have moved the registration. A sentinel
                // value (`u32::MAX` — not registered — or the in-flight
                // rebind marker) is ignored by `deregister_source`.
                let worker_idx = self
                    .shared
                    .uring_worker
                    .load(std::sync::atomic::Ordering::Relaxed)
                    as usize;
                return uring.deregister_source(&self.shared, worker_idx);
            }
            return Ok(());
        }

        self.handle().deregister_source(&self.shared, io)
    }

    pub(crate) fn clear_readiness(&self, event: ReadyEvent) {
        self.shared.clear_readiness(event);
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

        // v2 lazy placement hook. Gated by a hysteresis counter inside
        // the `maybe_rebind_to_current_worker` implementation so that
        // the common work-steal-for-one-poll pattern (which the
        // scheduler resolves on its own within the next poll) does not
        // trigger a cross-ring rebind. See `uring-fd-placement.md`.
        #[cfg(all(
            tokio_unstable,
            feature = "io-uring-reactor",
            feature = "rt-multi-thread",
            target_os = "linux",
        ))]
        self.maybe_rebind_to_current_worker();

        let ev = ready!(self.shared.poll_readiness(cx, direction));

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
        let ev = self.shared.ready_event(interest);

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
        let ev = self.shared.readiness(interest).await;

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

    /// v2 lazy placement hook. If this registration's `POLL_ADD_MULTI` is
    /// currently installed on a worker other than the one running the
    /// calling task, re-register on the current worker and schedule a
    /// cross-ring teardown of the old registration.
    ///
    /// Cheap in the common case — one atomic load plus a `usize` compare.
    /// Rebind itself goes through an atomic CAS to single-issuer the
    /// migration when two tasks share the same `Registration` across
    /// read+write halves.
    ///
    /// Errors from the local re-register are intentionally swallowed: a
    /// failed rebind leaves the resource on its previous worker, which
    /// remains fully functional (just not task-local). The poll call
    /// continues and will see readiness via the old ring's delivery path.
    #[cfg(all(
        tokio_unstable,
        feature = "io-uring-reactor",
        feature = "rt-multi-thread",
        target_os = "linux",
    ))]
    fn maybe_rebind_to_current_worker(&self) {
        use std::sync::atomic::Ordering::Relaxed;

        let Some(interest) = self.uring_interest else {
            // Mio-path registration under a non-uring runtime — nothing
            // to do.
            return;
        };
        let Some(current) =
            crate::runtime::scheduler::multi_thread::uring_park::current_worker_index()
        else {
            // Not executing on a uring worker thread (e.g., polled from a
            // LocalSet inside a blocking pool task). Leave the counter
            // untouched — it'll resume tracking on the next worker-thread
            // poll.
            return;
        };
        // Fast-path compare without CAS: current owner already matches?
        // Reset the hysteresis counter so that a transient ping does not
        // accumulate across unrelated wake-ups.
        let owner = self.shared.uring_worker.load(Relaxed);
        if owner as usize == current {
            self.shared.uring_rebind_hysteresis.store(0, Relaxed);
            return;
        }

        // Accumulate a mismatch. Only when the same worker has seen
        // `REBIND_HYSTERESIS_THRESHOLD` consecutive polls of this
        // registration — without the owner ever snapping back to it —
        // do we pay the cross-ring rebind cost.
        let prev = self.shared.uring_rebind_hysteresis.fetch_add(1, Relaxed);
        if prev + 1 < crate::runtime::io::scheduled_io::REBIND_HYSTERESIS_THRESHOLD {
            return;
        }
        // Reset the counter eagerly: the rebind either succeeds (owner
        // now matches `current`, so future polls take the fast-path
        // return above) or fails (old owner unchanged; we want a fresh
        // window before trying again rather than hammering every poll).
        self.shared.uring_rebind_hysteresis.store(0, Relaxed);

        let Some(uring) = self.handle.uring_handle() else {
            return;
        };
        // `rebind_source` internally handles stale / in-flight / mismatch
        // cases; we just forward and ignore the Ok/Err detail.
        let _ = uring.rebind_source(&self.shared, self.uring_fd, interest, current);
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
        self.shared.clear_wakers();
    }
}

fn gone() -> io::Error {
    io::Error::new(
        io::ErrorKind::Other,
        crate::util::error::RUNTIME_SHUTTING_DOWN_ERROR,
    )
}
