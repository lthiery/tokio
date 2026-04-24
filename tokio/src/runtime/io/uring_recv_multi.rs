//! Multishot-recv stream API backed by a provided-buffer ring (POC).
//!
//! A single `RecvMulti` SQE stays armed on a socket for the whole
//! connection lifetime and produces one CQE per message/fragment the
//! kernel hands up. Each CQE draws a buffer from the registered
//! [`super::uring_buf_ring::BufferRing`] (via `IOSQE_BUFFER_SELECT`),
//! which we turn into an owned [`BufferLease`] and queue on a
//! per-socket [`RecvInbox`]. The user polls the inbox through
//! [`UringRecvMulti`], gets a `BufferLease`, reads from it, and drops
//! it when done — at which point the bid is recycled back to the
//! kernel.
//!
//! Contrast with the single-shot owned-`BytesMut` path
//! ([`super::uring_bytes_ops::RecvResult`]):
//!
//!   * No per-recv SQE: one SQE per socket lifetime, not per message.
//!   * No per-recv slab entry: one slot per socket, not per message.
//!   * No per-recv buffer allocation: the ring is shared across all
//!     sockets on the reactor.
//!
//! # POC scope
//!
//! * No backpressure on the inbox (unbounded VecDeque).
//! * Best-effort cancel on drop via AsyncCancel2-by-fd; the stream
//!   considers itself "ended" as soon as the drop fires, but in-
//!   flight CQEs may still deliver leases into the inbox (which are
//!   discarded when the Arc refcount hits zero).
//! * `ended` is only advanced by the reactor observing a CQE without
//!   `IORING_CQE_F_MORE` (natural termination or -ECANCELED).

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::task::{Context, Poll};

use crate::loom::sync::Arc;
use crate::sync::AtomicWaker;

use super::uring_buf_ring::BufferLease;

/// One delivery from the kernel's multishot recv.
pub(crate) enum InboxEntry {
    /// Non-zero byte delivery. The lease has `len() > 0`.
    Data(BufferLease),

    /// Peer closed the connection (kernel posted `res == 0`). The
    /// stream yields `Ok(None)` semantically; we encode it as an
    /// inbox entry so ordering with prior data deliveries is
    /// preserved.
    Eof,

    /// Kernel returned a negative `res`. For multishot recv the most
    /// common case is `-ENOBUFS` (pool drained — happens if the
    /// consumer isn't releasing leases fast enough) or `-ECANCELED`
    /// (after our AsyncCancel2). The stream surfaces the error once
    /// and ends.
    Err(io::Error),
}

/// Shared state between the reactor's drain loop and the
/// [`UringRecvMulti`] future.
///
/// SPSC in practice: the reactor that owns the socket's SQE is the
/// sole pusher (completions only land on that ring), and at most one
/// task polls the stream at any moment. The queue stays behind a
/// `Mutex` for correctness under task migration, but the waker slot
/// is handled by [`AtomicWaker`], saving one lock acquisition per
/// push and one per park/wake round-trip.
pub(crate) struct RecvInbox {
    /// Delivered entries, oldest first. Under SPSC use the lock is
    /// uncontended in the steady state; we keep it for portability
    /// and to avoid hand-rolling an intrusive SPSC queue for the POC.
    queue: Mutex<VecDeque<InboxEntry>>,

    /// Waker registered by the last `poll_next` call that saw an
    /// empty queue. Woken by the reactor after appending. Using
    /// tokio's lock-free [`AtomicWaker`] replaces the previous
    /// `Mutex<Option<Waker>>`: the hot path now does one atomic
    /// swap per push instead of a full lock acquire.
    waker: AtomicWaker,

    /// Set by the reactor when a CQE arrives without
    /// `IORING_CQE_F_MORE`. The consumer flushes any remaining queued
    /// entries and then returns `None`.
    ended: AtomicBool,
}

impl RecvInbox {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(VecDeque::with_capacity(8)),
            waker: AtomicWaker::new(),
            ended: AtomicBool::new(false),
        })
    }

    /// Reactor-side push. Appends `entry` and wakes any waiter.
    pub(crate) fn push(&self, entry: InboxEntry) {
        {
            let mut q = self.queue.lock().unwrap_or_else(|p| p.into_inner());
            q.push_back(entry);
        }
        // AtomicWaker::wake is a no-op if nothing is registered;
        // otherwise it atomically takes the waker and invokes
        // wake() on it.
        self.waker.wake();
    }

    /// Reactor-side: mark the stream as ended. Called on the terminal
    /// CQE (no `F_MORE`). Wakes any parked waiter so it can observe
    /// `None`.
    pub(crate) fn mark_ended(&self) {
        self.ended.store(true, Ordering::Release);
        self.waker.wake();
    }

    /// Consumer-side poll. Returns the next entry, `Ok(None)` on
    /// end-of-stream, or parks the waker.
    fn poll_next(&self, cx: &mut Context<'_>) -> Poll<Option<io::Result<BufferLease>>> {
        // Fast path: try to pop. Keep this before the waker register
        // so the steady-state path is lock-once.
        //
        // Drain order: always serve queued entries before surfacing
        // end-of-stream, so an Eof CQE that arrived after buffered
        // Data doesn't swallow the trailing bytes.
        let entry = {
            let mut q = self.queue.lock().unwrap_or_else(|p| p.into_inner());
            q.pop_front()
        };
        match entry {
            Some(InboxEntry::Data(lease)) => Poll::Ready(Some(Ok(lease))),
            Some(InboxEntry::Eof) => Poll::Ready(None),
            Some(InboxEntry::Err(e)) => Poll::Ready(Some(Err(e))),
            None => {
                if self.ended.load(Ordering::Acquire) {
                    return Poll::Ready(None);
                }
                // Register the waker lock-free, then re-check to
                // close the push-before-register race:
                //
                //   T(poll):     pop_front -> None
                //   T(reactor):  push_back(entry); waker.wake()  [no-op, not registered yet]
                //   T(poll):     waker.register(...)
                //   -> would sleep forever without the re-check below.
                //
                // The re-check under the queue lock ensures that
                // any push that completed before our `register`
                // either (a) observed a registered waker and woke
                // us, or (b) appended an entry we'll now observe.
                self.waker.register_by_ref(cx.waker());
                let q = self.queue.lock().unwrap_or_else(|p| p.into_inner());
                if !q.is_empty() || self.ended.load(Ordering::Acquire) {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
        }
    }
}

/// A stream-like type yielding [`BufferLease`]s from a multishot recv.
///
/// Held by user code; drop triggers a best-effort AsyncCancel2-by-fd
/// on the owning reactor. Uses an inherent `next()` method rather
/// than a `Stream` trait impl to keep this POC free of a `futures`
/// dependency.
pub struct UringRecvMulti {
    inbox: Arc<RecvInbox>,
    /// Socket fd, captured for the drop-time cancel-by-fd.
    fd: std::os::fd::RawFd,
    /// Flipped once the inbox drains and reports `None` or an error,
    /// so the `Drop` impl skips the cancel submission in the common
    /// path.
    terminal_observed: bool,
}

impl std::fmt::Debug for UringRecvMulti {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringRecvMulti")
            .field("fd", &self.fd)
            .field("terminal_observed", &self.terminal_observed)
            .finish()
    }
}

impl UringRecvMulti {
    /// Construct a stream backed by `inbox`. Called by
    /// `TcpStream::uring_recv_multi` after the reactor has accepted
    /// the registration.
    pub(crate) fn new(inbox: Arc<RecvInbox>, fd: std::os::fd::RawFd) -> Self {
        Self {
            inbox,
            fd,
            terminal_observed: false,
        }
    }

    /// Construct a stream that resolves to `Some(Err(err))` on its
    /// first poll and `None` thereafter. Used when submission failed
    /// or the caller wasn't on a uring worker.
    pub(crate) fn already_complete_err(err: io::Error) -> Self {
        let inbox = RecvInbox::new();
        inbox.push(InboxEntry::Err(err));
        inbox.mark_ended();
        Self {
            inbox,
            fd: -1,
            terminal_observed: false,
        }
    }

    /// Yield the next delivery. Returns `None` on end-of-stream.
    pub fn next(&mut self) -> NextRecv<'_> {
        NextRecv { parent: self }
    }
}

/// Future for [`UringRecvMulti::next`].
pub struct NextRecv<'a> {
    parent: &'a mut UringRecvMulti,
}

impl<'a> std::fmt::Debug for NextRecv<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NextRecv").finish()
    }
}

impl<'a> Future for NextRecv<'a> {
    type Output = Option<io::Result<BufferLease>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Safe: we only touch &mut through the destructured field; no
        // pinning invariants on `NextRecv` itself.
        let parent = unsafe { &mut self.get_unchecked_mut().parent };
        match parent.inbox.poll_next(cx) {
            Poll::Ready(None) => {
                parent.terminal_observed = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => {
                // Errors are terminal in the POC.
                parent.terminal_observed = true;
                Poll::Ready(Some(Err(e)))
            }
            other => other,
        }
    }
}

impl Drop for UringRecvMulti {
    fn drop(&mut self) {
        if self.terminal_observed {
            return;
        }
        if self.fd < 0 {
            return;
        }
        let fd = self.fd;
        let _ = super::uring_driver::with_local_reactor(|reactor| {
            let _ = reactor.submit_cancel_fd(fd);
        });
    }
}
