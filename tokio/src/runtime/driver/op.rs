use crate::io::blocking::Buf;
use crate::io::uring::open::Open;
use crate::io::uring::read::Read;
use crate::io::uring::rename::Rename;
use crate::io::uring::utils::ArcFd;
use crate::io::uring::write::Write;

use crate::runtime::Handle;

#[cfg(
    // libc::statx is only supported on these platforms
    // FIXME: Add musl target env when our minimum supported
    // rust version is 1.93. To clarify, statx support is
    // introduced to musl in 1.25 as mentioned officially here:
    // https://musl.libc.org/releases.html.
    // However, rustup target_env building for *-linux-musl
    // uses 1.25 musl on all *-linux-musl platforms starting
    // in 1.93 stable rust version.
    // https://blog.rust-lang.org/2025/12/05/Updating-musl-1.2.5/
    any(target_env = "gnu", target_os = "android")
)]
use crate::io::uring::statx::Statx;
use io_uring::cqueue;
use io_uring::squeue::Entry;
use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

// This field isn't accessed directly, but it holds cancellation data,
// so `#[allow(dead_code)]` is needed.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum CancelData {
    Open(Open),
    Write(Write),
    Rename(Rename),
    ReadVec(Read<Vec<u8>, OwnedFd>),
    ReadBuf(Read<Buf, ArcFd>),
    #[cfg(
        // libc::statx is only supported on these platforms
        // FIXME: Add musl target env when our minimum supported
        // rust version is 1.93. To clarify, statx support is
        // introduced to musl in 1.25 as mentioned officially here:
        // https://musl.libc.org/releases.html.
        // However, rustup target_env building for *-linux-musl
        // uses 1.25 musl on all *-linux-musl platforms starting
        // in 1.93 stable rust version.
        // https://blog.rust-lang.org/2025/12/05/Updating-musl-1.2.5/
        any(target_env = "gnu", target_os = "android")
    )]
    Statx(Statx),
}

#[derive(Debug)]
pub(crate) enum Lifecycle {
    /// The operation has been submitted to uring and is currently in-flight
    Submitted,

    /// The submitter is waiting for the completion of the operation
    Waiting(Waker),

    /// The submitter no longer has interest in the operation result. The state
    /// must be passed to the driver and held until the operation completes.
    Cancelled(
        // This field isn't accessed directly, but it holds cancellation data,
        // so `#[allow(dead_code)]` is needed.
        #[allow(dead_code)] CancelData,
    ),

    /// The operation has completed with a single cqe result
    Completed(cqueue::Entry),
}

pub(crate) enum State {
    Initialize(Option<Entry>),
    Polled(usize),
    Complete,
}

pub(crate) struct Op<T: Cancellable> {
    // Handle to the runtime
    handle: Handle,
    // State of this Op
    state: State,
    // Per operation data.
    data: Option<T>,
}

impl<T: Cancellable> Op<T> {
    /// # Safety
    ///
    /// Callers must ensure that parameters of the entry (such as buffer) are valid and will
    /// be valid for the entire duration of the operation, otherwise it may cause memory problems.
    pub(crate) unsafe fn new(entry: Entry, data: T) -> Self {
        let handle = Handle::current();
        Self {
            handle,
            data: Some(data),
            state: State::Initialize(Some(entry)),
        }
    }
    pub(crate) fn take_data(&mut self) -> Option<T> {
        self.data.take()
    }
}

impl<T: Cancellable> Drop for Op<T> {
    fn drop(&mut self) {
        match self.state {
            // We've already dropped this Op.
            State::Complete => (),
            // We will cancel this Op.
            State::Polled(index) => {
                let data = self.take_data();
                op_driver(&self.handle).cancel_op(index, data);
            }
            // This Op has not been polled yet.
            // We don't need to do anything here.
            State::Initialize(_) => (),
        }
    }
}

/// A single CQE result
pub(crate) struct CqeResult {
    pub(crate) result: io::Result<u32>,
}

impl From<cqueue::Entry> for CqeResult {
    fn from(cqe: cqueue::Entry) -> Self {
        let res = cqe.result();
        let result = if res >= 0 {
            Ok(res as u32)
        } else {
            Err(io::Error::from_raw_os_error(-res))
        };
        CqeResult { result }
    }
}

/// A trait that converts a CQE result into a usable value for each operation.
pub(crate) trait Completable {
    type Output;
    fn complete(self, cqe: CqeResult) -> Self::Output;

    // This is used when you want to terminate an operation with an error.
    //
    // The `Op` type that implements this trait can return the passed error
    // upstream by embedding it in the `Output`.
    fn complete_with_error(self, error: io::Error) -> Self::Output;
}

/// Extracts the `CancelData` needed to safely cancel an in-flight io_uring operation.
pub(crate) trait Cancellable {
    fn cancel(self) -> CancelData;
}

impl<T: Cancellable> Unpin for Op<T> {}

impl<T: Cancellable + Completable + Send> Future for Op<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let driver = op_driver(&this.handle);

        match &mut this.state {
            State::Initialize(entry_opt) => {
                let entry = entry_opt.take().expect("Entry must be present");
                let waker = cx.waker().clone();

                // SAFETY: entry is valid for the entire duration of the operation
                match unsafe { driver.register_op(entry, waker) } {
                    Ok(idx) => this.state = State::Polled(idx),
                    Err(err) => {
                        let data = this
                            .take_data()
                            .expect("Data must be present on Initialization");

                        this.state = State::Complete;

                        return Poll::Ready(data.complete_with_error(err));
                    }
                };

                Poll::Pending
            }

            State::Polled(idx) => match driver.poll_op(*idx, cx.waker()) {
                None => Poll::Pending,
                Some(cqe) => {
                    this.state = State::Complete;
                    let data = this
                        .take_data()
                        .expect("Data must be present on completion");
                    Poll::Ready(data.complete(cqe.into()))
                }
            },

            State::Complete => {
                panic!("Future polled after completion");
            }
        }
    }
}

/// The io_uring backend a given `Op<T>` drives. Both variants expose the
/// same submit / re-poll / cancel surface, so the `Op<T>` state machine is
/// backend-agnostic.
///
/// - `Sidecar` is the in-tree fs side-driver (its own ring, parked on the
///   mio driver). Selected on a mio runtime, and on any build without the
///   reactor feature.
/// - `Reactor` is the single shared io_uring reactor. Selected when the
///   runtime was built with `enable_uring_reactor()`, so fs completions ride
///   the same ring as readiness: one event system.
enum OpDriver<'a> {
    Sidecar(&'a crate::runtime::io::Handle),
    #[cfg(feature = "io-uring-reactor")]
    Reactor(&'a crate::runtime::io::uring_driver::UringHandle),
}

/// Resolve which io_uring backend this runtime's fs ops run on. Prefers the
/// uring reactor when it is the active io driver; otherwise the fs
/// side-driver, which is exactly the pre-reactor behavior.
fn op_driver(handle: &Handle) -> OpDriver<'_> {
    #[cfg(feature = "io-uring-reactor")]
    if let Some(uring) = handle.inner.driver().uring_handle() {
        return OpDriver::Reactor(uring);
    }
    OpDriver::Sidecar(handle.inner.driver().io())
}

/// Is a uring fs op available for `opcode` on the current runtime?
///
/// On a uring-reactor runtime the shared ring already handles the in-tree
/// fs ops, so this returns `true` without touching (and without lazily
/// spinning up) the fs side-driver's separate ring. Otherwise it defers to
/// the side-driver's lazy probe-and-init, i.e. the exact pre-reactor
/// behavior. Callers use the result to choose the uring path over the
/// `spawn_blocking` fallback.
pub(crate) async fn uring_fs_available(opcode: u8) -> io::Result<bool> {
    let handle = Handle::current();
    #[cfg(feature = "io-uring-reactor")]
    if handle.inner.driver().uring_handle().is_some() {
        return Ok(true);
    }
    handle.inner.driver().io().check_and_init(opcode).await
}

impl OpDriver<'_> {
    /// # Safety
    ///
    /// See [`Op::new`]: the entry's operands must stay valid for the whole op.
    unsafe fn register_op(&self, entry: Entry, waker: Waker) -> io::Result<usize> {
        match self {
            // SAFETY: forwarded from the caller.
            OpDriver::Sidecar(h) => unsafe { h.register_op(entry, waker) },
            #[cfg(feature = "io-uring-reactor")]
            // SAFETY: forwarded from the caller.
            OpDriver::Reactor(h) => unsafe { h.register_op(entry, waker) },
        }
    }

    fn poll_op(&self, index: usize, waker: &Waker) -> Option<cqueue::Entry> {
        match self {
            OpDriver::Sidecar(h) => h.poll_op(index, waker),
            #[cfg(feature = "io-uring-reactor")]
            OpDriver::Reactor(h) => h.poll_op(index, waker),
        }
    }

    fn cancel_op<T: Cancellable>(&self, index: usize, data: Option<T>) {
        match self {
            OpDriver::Sidecar(h) => h.cancel_op(index, data),
            #[cfg(feature = "io-uring-reactor")]
            OpDriver::Reactor(h) => h.cancel_op(index, data),
        }
    }
}
