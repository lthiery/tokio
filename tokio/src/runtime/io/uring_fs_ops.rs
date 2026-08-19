//! Completion-op slab for the single shared io_uring reactor.
//!
//! This is the reactor-side home for the in-tree `tokio::fs` io_uring ops
//! (open/read/write). It holds one [`Lifecycle`] per in-flight op, keyed by
//! slab index, exactly like the fs side-driver's `UringContext.ops`
//! (`runtime::io::driver::uring`). The difference is purely transport: the
//! side-driver owns its own ring and submits/reaps inline, while here the
//! shared reactor's current holder submits the SQE (drained from the
//! [`GlobalRing`] op FIFO) and reaps the CQE (in `drain_completions`),
//! flipping the slot through this slab.
//!
//! The slab is guarded by its own `Mutex`, independent of the reactor's
//! ring `TryLock`. That independence is what lets an [`Op`] future poll for
//! its completion from any worker, even when a *different* worker is holding
//! (and blocked in the kernel on) the ring: the future only ever touches
//! this slab, never the ring.
//!
//! The op protocol (Waiting / Completed / Cancelled, drop-cancels-in-place,
//! `CancelData::Open` closing an fd delivered by a late CQE) is identical to
//! the side-driver's, so the `Op<T>` future, `Completable`, `Cancellable`,
//! and every `tokio::fs` call site are shared verbatim.
//!
//! [`Op`]: crate::runtime::driver::op::Op
//! [`GlobalRing`]: super::uring_driver::GlobalRing

use crate::loom::sync::Mutex;
use crate::runtime::driver::op::{Cancellable, CancelData, CqeResult, Lifecycle};

use io_uring::cqueue;
use slab::Slab;

use std::mem;
use std::os::fd::{FromRawFd, OwnedFd};
use std::task::Waker;

/// Slab of in-flight fs-op [`Lifecycle`]s for the shared reactor.
///
/// Shared (`Arc`) between the [`GlobalRing`] (submission / poll / cancel
/// side) and the [`Reactor`] (completion-drain side).
///
/// [`GlobalRing`]: super::uring_driver::GlobalRing
/// [`Reactor`]: super::uring_reactor::Reactor
pub(crate) struct FsOpSlab {
    ops: Mutex<Slab<Lifecycle>>,
}

impl std::fmt::Debug for FsOpSlab {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsOpSlab").finish_non_exhaustive()
    }
}

impl FsOpSlab {
    pub(crate) fn new() -> Self {
        Self {
            ops: Mutex::new(Slab::new()),
        }
    }

    /// Allocate a slot in the `Waiting` state and return its index. The
    /// caller stamps that index into the SQE's `user_data` (via the fs-op
    /// `user_data` encoding) before queuing it for submission.
    pub(crate) fn insert_waiting(&self, waker: Waker) -> usize {
        self.ops.lock().insert(Lifecycle::Waiting(waker))
    }

    /// `Op<T>` re-poll: inspect the slot at `idx`. Returns `Some(cqe)` and
    /// frees the slot once the completion has landed; otherwise refreshes
    /// the stored waker and returns `None`. Mirrors the fs side-driver's
    /// `Op::poll` `State::Polled` arm exactly.
    pub(crate) fn poll(&self, idx: usize, waker: &Waker) -> Option<cqueue::Entry> {
        let mut ops = self.ops.lock();
        let lifecycle = ops.get_mut(idx).expect("Lifecycle must be present");

        match mem::replace(lifecycle, Lifecycle::Submitted) {
            // Only replace the stored waker if it wouldn't wake the new one.
            Lifecycle::Waiting(prev) if !prev.will_wake(waker) => {
                *lifecycle = Lifecycle::Waiting(waker.clone());
                None
            }
            Lifecycle::Waiting(prev) => {
                *lifecycle = Lifecycle::Waiting(prev);
                None
            }
            Lifecycle::Completed(cqe) => {
                ops.remove(idx);
                Some(cqe)
            }
            Lifecycle::Submitted => {
                unreachable!("Submitted lifecycle should never be seen here");
            }
            Lifecycle::Cancelled(_) => {
                unreachable!("Cancelled lifecycle should never be seen here");
            }
        }
    }

    /// `Op<T>` drop-before-complete: move the op's data into the slot as
    /// [`CancelData`] so the buffers/fd stay alive until the kernel posts
    /// the terminal CQE. If the completion already landed, clean up now
    /// (closing a delivered fd for the `Open` case). Mirrors the fs
    /// side-driver's `Handle::cancel_op`.
    pub(crate) fn cancel<T: Cancellable>(&self, idx: usize, data: Option<T>) {
        let mut ops = self.ops.lock();
        let Some(lifecycle) = ops.get_mut(idx) else {
            // Already completed and removed.
            return;
        };

        let cancel_data = data.expect("Data should be present").cancel();
        match mem::replace(lifecycle, Lifecycle::Cancelled(cancel_data)) {
            Lifecycle::Submitted | Lifecycle::Waiting(_) => (),
            // The driver saw the completion, but it was never polled.
            Lifecycle::Completed(cqe) => {
                if let Lifecycle::Cancelled(CancelData::Open(_)) = lifecycle {
                    if let Ok(fd) = CqeResult::from(cqe).result {
                        // SAFETY: a successful Open CQE result is a real fd;
                        // nobody else owns it, so we close it here.
                        unsafe {
                            OwnedFd::from_raw_fd(fd as i32);
                        }
                    }
                }
                ops.remove(idx);
            }
            prev => panic!("Unexpected state: {prev:?}"),
        }
    }

    /// Completion drain: deliver the CQE for slot `idx`. A `Waiting` slot
    /// flips to `Completed` and wakes its task; a `Cancelled` slot is freed
    /// (closing a delivered fd for the `Open` case). Mirrors the fs
    /// side-driver's `dispatch_completions`. Called by the ring holder from
    /// `Reactor::drain_completions`.
    pub(crate) fn deliver(&self, idx: usize, cqe: cqueue::Entry) {
        let mut ops = self.ops.lock();
        match ops.get_mut(idx) {
            Some(Lifecycle::Waiting(waker)) => {
                waker.wake_by_ref();
                *ops.get_mut(idx).unwrap() = Lifecycle::Completed(cqe);
            }
            Some(Lifecycle::Cancelled(cancel_data)) => {
                if let CancelData::Open(_) = cancel_data {
                    if let Ok(fd) = CqeResult::from(cqe).result {
                        // SAFETY: as in `cancel`.
                        unsafe {
                            OwnedFd::from_raw_fd(fd as i32);
                        }
                    }
                }
                ops.remove(idx);
            }
            Some(other) => panic!("unexpected lifecycle for fs slot {idx}: {other:?}"),
            None => panic!("no fs op at index {idx}"),
        }
    }

    /// Shutdown helper: drop every `Completed` slot without waiting. A
    /// completed slot means the CQE already landed; its owned data (if any)
    /// went back to the `Op` when it was cancelled, so there is nothing to
    /// release here. Mirrors the fs side-driver `UringContext::drop`'s `retain`.
    pub(crate) fn reap_completed(&self) {
        self.ops
            .lock()
            .retain(|_, lifecycle| !matches!(lifecycle, Lifecycle::Completed(_)));
    }

    /// Shutdown helper: `true` while any slot still awaits a kernel CQE
    /// (`Waiting` / `Submitted` / `Cancelled`). Call [`Self::reap_completed`]
    /// first so already-landed completions do not count.
    pub(crate) fn has_pending_kernel_ops(&self) -> bool {
        !self.ops.lock().is_empty()
    }
}
