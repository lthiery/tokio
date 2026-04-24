//! Owned-buffer `Send`/`Recv` operations for the per-worker `io_uring`
//! reactor.
//!
//! # Ownership model
//!
//! Unlike the readiness-based `POLL_ADD_MULTI` path — which only tells the
//! task *when* to call `read`/`write` and leaves buffer ownership to the
//! caller — these ops hand a buffer *to the kernel* for the duration of
//! the SQE. The kernel reads from (`Send`) or writes into (`Recv`) the
//! buffer directly. While the SQE is in flight, the buffer MUST NOT be
//! dropped, freed, or mutated by user code: doing so would race with the
//! kernel's access and corrupt memory (for `Recv`) or invoke undefined
//! behaviour on a stale pointer (for `Send`).
//!
//! We enforce this by storing the buffer inside the reactor's op slab.
//! The buffer lives there for the entire op lifetime and is handed back
//! to the caller only when the terminal CQE arrives (success, error, or
//! `-ECANCELED` after an [`AsyncCancel`]).
//!
//! # Cancellation
//!
//! Dropping the returned future before it completes does not free the
//! buffer. It:
//!
//! 1. **Abandons** the shared completion state so the reactor's drain
//!    loop will not attempt to wake a freed task handle.
//! 2. **Best-effort submits an [`AsyncCancel`]** targeting the in-flight
//!    SQE's `user_data`. If we are on the same worker that owns the
//!    target ring, the cancel lands and the kernel posts a terminal
//!    `-ECANCELED` CQE promptly; the buffer is released then.
//! 3. **If the cancel cannot be submitted** (future dropped off a
//!    worker thread, or from a different worker than the one that
//!    submitted the op), the buffer simply stays alive in the slab
//!    until the op completes naturally. That may take arbitrarily long
//!    for a slow socket, but it is always safe: the kernel can never
//!    outrun the Arc-held lifetime of the backing allocation.
//!
//! The critical invariant — buffers are only dropped after the terminal
//! CQE for their op — is upheld regardless of which branch the
//! cancellation path takes.
//!
//! [`AsyncCancel`]: io_uring::opcode::AsyncCancel

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bytes::{Bytes, BytesMut};

/// Result type handed back to the `uring_send` caller: the original
/// buffer (always), paired with the kernel's `send()` result.
///
/// The buffer is returned in both the success and error cases so the
/// caller can reuse or resize it without an extra allocation — a
/// pattern borrowed from the `tokio-uring` crate. On success the
/// `usize` is the number of bytes consumed from the buffer; on error
/// the `usize` is meaningless (and is 0 by convention).
pub(crate) type SendResult = (io::Result<usize>, Bytes);

/// Result type handed back to the `uring_recv` caller: the original
/// buffer (always), paired with the kernel's `recv()` result.
///
/// On success the `usize` is the number of bytes the kernel wrote
/// into the buffer; the caller is responsible for `truncate(n)` or
/// `split_to(n)` if they want the logical length to reflect that. On
/// error the `usize` is meaningless (0 by convention).
pub(crate) type RecvResult = (io::Result<usize>, BytesMut);

/// State machine shared between the user-facing [`Future`] (the
/// "awaiter") and the reactor's completion path (the "completer").
///
/// The state transitions are:
///
/// ```text
///               future polled              reactor completes
///   Pending{None} ───────────▶ Pending{Some(Waker)} ─────▶ Ready(value)
///         │                                                    │
///         │                                                    │ future polled
///         │                                                    ▼
///         │                                                consumed: Arc dropped
///         │
///         │ future dropped
///         ▼
///     Abandoned ──── reactor completes ──▶ Done (value dropped)
/// ```
///
/// Every state is terminal from the perspective of exactly one actor:
/// `Ready` waits to be consumed by the awaiter, `Done` has already
/// swallowed its value.
pub(crate) struct CompleterShared<T> {
    state: Mutex<CompleterState<T>>,
}

enum CompleterState<T> {
    /// Op is in-flight and the awaiter is still interested. `waker` is
    /// set on the second+ poll (the first poll never has a waker to
    /// store yet because we submit eagerly before returning the future).
    Pending { waker: Option<Waker> },

    /// Completion arrived. Awaits `take()` by the awaiter's next poll.
    Ready(T),

    /// Awaiter dropped the future before completion. When the reactor
    /// eventually completes the op, it will find this state and discard
    /// the value rather than trying to wake.
    Abandoned,

    /// Reactor completed after abandonment. The value has been dropped
    /// (including any owned buffer). Terminal; the `Arc` is dropped
    /// shortly afterwards.
    Done,
}

impl<T> CompleterShared<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(CompleterState::Pending { waker: None }),
        })
    }

    /// Awaiter-side poll. Returns `Poll::Ready(value)` if the completion
    /// already landed, otherwise stores `waker` and returns `Pending`.
    fn poll(&self, cx: &mut Context<'_>) -> Poll<T> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match &mut *state {
            CompleterState::Pending { waker } => {
                // Update the stored waker only if the new one is
                // different; cheap cases where a future is re-polled
                // with the same waker skip the clone.
                match waker {
                    Some(existing) if existing.will_wake(cx.waker()) => {}
                    slot => *slot = Some(cx.waker().clone()),
                }
                Poll::Pending
            }
            CompleterState::Ready(_) => {
                // Extract the value by swapping in `Done`.
                let prev = std::mem::replace(&mut *state, CompleterState::Done);
                match prev {
                    CompleterState::Ready(v) => Poll::Ready(v),
                    // Unreachable: we just observed Ready under the lock.
                    _ => unreachable!(),
                }
            }
            CompleterState::Abandoned | CompleterState::Done => {
                // Cannot happen: only the awaiter transitions into
                // Abandoned, and it does so on drop — no further polls.
                // Done is likewise post-abandonment.
                unreachable!("CompleterShared::poll after abandonment")
            }
        }
    }

    /// Completer-side entry: the reactor delivers the op's result. If
    /// the awaiter is still listening, transitions to `Ready(value)` and
    /// wakes any parked waker; if the awaiter has abandoned, drops
    /// `value` (and any buffer it owns).
    pub(crate) fn complete(&self, value: T) {
        let waker_to_wake = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            match &mut *state {
                CompleterState::Pending { waker } => {
                    let waker = waker.take();
                    *state = CompleterState::Ready(value);
                    waker
                }
                CompleterState::Abandoned => {
                    *state = CompleterState::Done;
                    // `value` is dropped here, inside the lock.
                    // Acceptable: value drops are cheap (Bytes decrements
                    // an Arc refcount; usize is trivial).
                    None
                }
                CompleterState::Ready(_) | CompleterState::Done => {
                    // Double completion shouldn't happen: the reactor
                    // removes the slab slot atomically with the terminal
                    // CQE dispatch. Defensive no-op.
                    debug_assert!(false, "CompleterShared::complete called twice");
                    return;
                }
            }
        };
        if let Some(w) = waker_to_wake {
            w.wake();
        }
    }

    /// Awaiter-side: mark the future as dropped. Subsequent `complete`
    /// calls will silently drop the value.
    fn abandon(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match &*state {
            CompleterState::Pending { .. } => {
                *state = CompleterState::Abandoned;
            }
            CompleterState::Ready(_) => {
                // Completion arrived and we never polled again — discard
                // the result by transitioning to Done.
                *state = CompleterState::Done;
            }
            CompleterState::Abandoned | CompleterState::Done => {}
        }
    }
}

/// Information the future needs to request cancellation on drop. Held as
/// `Option<_>` on the future: `Some` while the op is in flight, `None`
/// once we observe `Ready` (no cancellation needed — already done).
struct CancelInfo {
    /// The `user_data` of the SQE we submitted, so `AsyncCancel` can
    /// reference it.
    target_user_data: u64,
}

/// Future returned by `TcpStream::uring_send`. See the [module-level
/// ownership and cancellation docs](self) for semantics.
pub struct UringSend {
    shared: Arc<CompleterShared<SendResult>>,
    cancel: Option<CancelInfo>,
}

impl std::fmt::Debug for UringSend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringSend")
            .field("in_flight", &self.cancel.is_some())
            .finish()
    }
}

impl UringSend {
    pub(crate) fn new(shared: Arc<CompleterShared<SendResult>>, target_user_data: u64) -> Self {
        Self {
            shared,
            cancel: Some(CancelInfo { target_user_data }),
        }
    }

    /// Construct a `UringSend` that resolves immediately to
    /// `(Err(err), buf)` on its first poll. Used when submission to
    /// the kernel failed (or never happened — e.g. the caller wasn't
    /// on a uring worker) and we need to surface the error while
    /// handing the buffer back.
    ///
    /// No kernel op exists, so there is nothing to cancel on drop.
    pub(crate) fn already_complete_err(err: io::Error, buf: Bytes) -> Self {
        let shared = CompleterShared::new();
        shared.complete((Err(err), buf));
        Self {
            shared,
            cancel: None,
        }
    }
}

impl Future for UringSend {
    type Output = SendResult;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.shared.poll(cx) {
            Poll::Ready(v) => {
                // Completion observed — nothing to cancel on drop.
                self.cancel = None;
                Poll::Ready(v)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for UringSend {
    fn drop(&mut self) {
        // Order matters: abandon first so the reactor won't try to wake
        // a dead task, then request cancellation so the kernel stops
        // touching the buffer ASAP. If the cancel submission fails, the
        // buffer still gets dropped when the natural CQE arrives — see
        // module-level docs.
        self.shared.abandon();
        if let Some(info) = self.cancel.take() {
            submit_local_cancel(info.target_user_data);
        }
    }
}

/// Future returned by `TcpStream::uring_recv`. See the [module-level
/// ownership and cancellation docs](self) for semantics.
pub struct UringRecv {
    shared: Arc<CompleterShared<RecvResult>>,
    cancel: Option<CancelInfo>,
}

impl std::fmt::Debug for UringRecv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringRecv")
            .field("in_flight", &self.cancel.is_some())
            .finish()
    }
}

impl UringRecv {
    pub(crate) fn new(shared: Arc<CompleterShared<RecvResult>>, target_user_data: u64) -> Self {
        Self {
            shared,
            cancel: Some(CancelInfo { target_user_data }),
        }
    }

    /// See [`UringSend::already_complete_err`].
    pub(crate) fn already_complete_err(err: io::Error, buf: BytesMut) -> Self {
        let shared = CompleterShared::new();
        shared.complete((Err(err), buf));
        Self {
            shared,
            cancel: None,
        }
    }
}

impl Future for UringRecv {
    type Output = RecvResult;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.shared.poll(cx) {
            Poll::Ready(v) => {
                self.cancel = None;
                Poll::Ready(v)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for UringRecv {
    fn drop(&mut self) {
        self.shared.abandon();
        if let Some(info) = self.cancel.take() {
            submit_local_cancel(info.target_user_data);
        }
    }
}

/// Best-effort submission of an `AsyncCancel` SQE on the current
/// worker's reactor. If we are not on a worker (e.g. the future was
/// dropped from a `spawn_blocking` or an external thread), or if the
/// target op lives on a different reactor, this is a no-op — the buffer
/// will still be safely released when the natural CQE arrives.
fn submit_local_cancel(target_user_data: u64) {
    let _ = super::uring_driver::with_local_reactor(|reactor| {
        // Errors here (SQ push failure, submit failure) are intentional
        // no-ops at this layer. The only correctness-critical part is
        // that we never drop the buffer before the real CQE; the cancel
        // is purely a latency optimisation.
        let _ = reactor.submit_async_cancel(target_user_data);
    });
}
