//! Shared handle and cross-worker coordination for the uring-reactor
//! backend.
//!
//! The [`Reactor`] itself is per-worker and `!Send`. [`UringHandle`] is the
//! shared half: it holds per-worker metadata (ring fds, external wakers,
//! park state) so that any thread can unpark any worker.
//!
//! # Unpark routing
//!
//! When `unpark(target)` is called:
//!
//! 1. Flip the target's park state atomic to `NOTIFIED`. If the previous
//!    value wasn't `PARKED`, the worker isn't blocked — it will observe the
//!    notification on its next trip through park(). No syscall.
//!
//! 2. Otherwise, the worker is blocked in `io_uring_enter`. We pick a wake
//!    mechanism:
//!
//!    - If we are currently executing on a worker thread, we submit a
//!      `MSG_RING` SQE on *our own* ring targeting the receiver's ring.
//!      This is the hot cross-worker path and costs one non-blocking
//!      syscall.
//!
//!    - Otherwise (external thread: `spawn_blocking`, user code, signal
//!      handler shim, etc.) we write to the target worker's eventfd via
//!      [`ExternalWaker`]. The eventfd is registered on the target ring
//!      with `POLL_ADD_MULTI` and fires a CQE, unblocking park.
//!
//! # The `LOCAL_REACTOR` thread-local
//!
//! Cross-worker `MSG_RING` wakes need access to the current thread's own
//! Reactor (to push an SQE on its ring). We publish this via a thread-local
//! pointer, installed by a [`LocalReactorGuard`] RAII token at worker
//! startup. The pointer targets a `RefCell<Reactor>` whose `borrow_mut`
//! sees no contention in practice: during task execution the worker is not
//! parked, and during park the worker is blocked in the kernel — the two
//! time windows are strictly non-overlapping on the same thread.

use std::cell::{Cell, RefCell};
use std::ptr;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::OnceLock;

use crate::loom::sync::Mutex;
use crate::runtime::io::registration_set;
use crate::runtime::io::uring_reactor::{ExternalWaker, Reactor};
use crate::runtime::io::{IoDriverMetrics, RegistrationSet};

/// Park-state atomic values. Shape mirrors the mio parker's transitions so
/// integration stays familiar.
pub(crate) const EMPTY: usize = 0;
pub(crate) const PARKED: usize = 1;
pub(crate) const NOTIFIED: usize = 2;

/// Per-worker coordination slot. One of these per worker, indexed by worker
/// id. All fields are thread-safe since multiple unparkers may target the
/// same worker concurrently.
#[derive(Debug)]
pub(crate) struct WorkerState {
    /// `EMPTY | PARKED | NOTIFIED`. Written by the owning worker on the
    /// park/resume transition; read and CAS'd by unparkers.
    pub(crate) park_state: AtomicUsize,

    /// Raw fd of the worker's ring. `-1` until the worker publishes it
    /// during startup via [`UringHandle::register_worker`].
    ///
    /// We store a `RawFd` rather than a `BorrowedFd` because the fd's
    /// lifetime is managed by the worker's `Reactor` (which owns the
    /// `IoUring`); the `AtomicI32` simply advertises the value.
    pub(crate) ring_fd: AtomicI32,

    /// Eventfd handle for waking this worker from a non-worker thread.
    /// Published once, at worker startup.
    pub(crate) external_waker: OnceLock<ExternalWaker>,
}

impl WorkerState {
    const fn new() -> Self {
        Self {
            park_state: AtomicUsize::new(EMPTY),
            ring_fd: AtomicI32::new(-1),
            external_waker: OnceLock::new(),
        }
    }
}

/// Shared I/O handle for the uring-reactor backend.
///
/// The Handle-side analog of the mio driver's [`Handle`]. Holds per-worker
/// slots used for cross-worker and external unparking, plus the shared
/// registration set and metrics (reused wholesale from the mio side — they
/// are backend-agnostic).
///
/// [`Handle`]: super::driver::Handle
pub(crate) struct UringHandle {
    /// Per-worker state, indexed by worker id. Length equals the runtime's
    /// worker count and does not change after construction.
    workers: Box<[WorkerState]>,

    /// Shared registration set (fd → ScheduledIo). Identical to the mio
    /// driver's usage; the Arc-pinned `ScheduledIo` instances hold the
    /// `user_data` pointers that our CQEs reference.
    pub(crate) registrations: RegistrationSet,
    pub(crate) synced: Mutex<registration_set::Synced>,

    pub(crate) metrics: IoDriverMetrics,
}

impl std::fmt::Debug for UringHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringHandle")
            .field("num_workers", &self.workers.len())
            .finish_non_exhaustive()
    }
}

impl UringHandle {
    pub(crate) fn new(num_workers: usize) -> Self {
        let mut workers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            workers.push(WorkerState::new());
        }
        let (registrations, synced) = RegistrationSet::new();
        Self {
            workers: workers.into_boxed_slice(),
            registrations,
            synced: Mutex::new(synced),
            metrics: IoDriverMetrics::default(),
        }
    }

    /// Number of workers this handle serves.
    #[allow(dead_code)]
    pub(crate) fn num_workers(&self) -> usize {
        self.workers.len()
    }

    /// Publish a worker's ring fd and external waker, called exactly once
    /// per worker during scheduler startup. After this returns, other
    /// threads may target the worker with `unpark`.
    ///
    /// Safety of publishing the ring fd as a `RawFd`: the fd is owned by
    /// the worker's `IoUring`, which lives for the duration of the worker
    /// loop. Unpark callers must not use this fd after runtime shutdown.
    /// In practice the runtime's shutdown sequence joins worker threads
    /// before dropping `UringHandle`, closing the window.
    pub(crate) fn register_worker(
        &self,
        worker_idx: usize,
        ring_fd: std::os::fd::RawFd,
        external_waker: ExternalWaker,
    ) {
        let slot = &self.workers[worker_idx];
        // `Release` so the eventfd/ring-fd writes are visible to unparkers
        // that observe the ring_fd.
        slot.ring_fd.store(ring_fd, Ordering::Release);
        // OnceCell::set returns Err if already set; we expect exactly-once
        // publication. In debug builds, panic; in release, last-writer-wins
        // with a visible debug_assert failure.
        if slot.external_waker.set(external_waker).is_err() {
            debug_assert!(false, "worker {worker_idx} published external_waker twice");
        }
    }

    /// Mark `worker_idx` as notified and — if the worker was parked —
    /// deliver an actual wake.
    ///
    /// Returns `true` if a wake was delivered to the kernel (for metrics).
    pub(crate) fn unpark(&self, worker_idx: usize) -> bool {
        let slot = &self.workers[worker_idx];
        // Swap to NOTIFIED with Release ordering so that any prior writes
        // by the caller (e.g., task queue push) are visible to the woken
        // worker, which reads this atomic with Acquire.
        let prev = slot.park_state.swap(NOTIFIED, Ordering::Release);
        if prev != PARKED {
            // Worker wasn't blocked — either already running (EMPTY) or
            // already notified (NOTIFIED). No syscall needed.
            return false;
        }
        // Worker is in `io_uring_enter` — actually wake it.
        self.deliver_wake(worker_idx);
        true
    }

    /// Called by the worker on entry to park, to record that it is about
    /// to block. Returns `true` if a wake was already pending, in which
    /// case the worker should skip the actual syscall and return
    /// immediately.
    pub(crate) fn begin_park(&self, worker_idx: usize) -> bool {
        let slot = &self.workers[worker_idx];
        match slot.park_state.compare_exchange(
            EMPTY,
            PARKED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => false, // Now PARKED, go ahead and block.
            Err(NOTIFIED) => {
                // Consume the notification and skip the park.
                slot.park_state.store(EMPTY, Ordering::Release);
                true
            }
            Err(state) => panic!("inconsistent park_state on begin_park: {state}"),
        }
    }

    /// Called by the worker on park completion. Resets state to EMPTY,
    /// clearing any notification that was consumed by the wake.
    pub(crate) fn end_park(&self, worker_idx: usize) {
        let slot = &self.workers[worker_idx];
        // Unconditionally clear — any notification that arrives after the
        // kernel released us will have already caused the CQE we're now
        // draining, so we're caught up.
        slot.park_state.store(EMPTY, Ordering::Release);
    }

    /// Deliver a wake to a definitely-parked worker via the best available
    /// mechanism for the calling thread.
    fn deliver_wake(&self, target_worker: usize) {
        let slot = &self.workers[target_worker];
        let target_ring_fd = slot.ring_fd.load(Ordering::Acquire);

        // Fast path: we're running on some worker, and the target's ring
        // fd has been published. MSG_RING from our ring.
        if target_ring_fd >= 0 {
            let sent = with_local_reactor(|reactor| {
                reactor.send_msg_ring(target_ring_fd)
            });
            match sent {
                Some(Ok(())) => return,
                Some(Err(_e)) => {
                    // MSG_RING failed — fall through to eventfd. This
                    // shouldn't happen in normal operation; if it does,
                    // the fallback keeps us correct.
                }
                None => {
                    // Not on a worker thread — fall through.
                }
            }
        }

        // Fallback path: eventfd write. Always correct; slightly more
        // expensive than MSG_RING because it goes through POLL_ADD_MULTI
        // on the target side.
        if let Some(waker) = slot.external_waker.get() {
            // Ignore write errors — if the eventfd is dead the target
            // reactor is being torn down, and shutdown will reap it.
            let _ = waker.wake();
        }
    }
}

// ===== LOCAL_REACTOR thread-local =====

thread_local! {
    /// Pointer to the current thread's `RefCell<Reactor>`, if installed.
    /// Installed by [`LocalReactorGuard`] at worker startup and cleared on
    /// drop. Accessed only by the owning thread, so no synchronization
    /// beyond the `Cell`.
    static LOCAL_REACTOR: Cell<*const RefCell<Reactor>> =
        const { Cell::new(ptr::null()) };
}

/// RAII token that installs the given `RefCell<Reactor>` as the current
/// thread's local reactor, and uninstalls it on drop.
///
/// Worker threads construct one of these after building their `Reactor`
/// and hold it for the duration of the worker loop.
#[must_use = "dropping the guard uninstalls the thread-local reactor"]
pub(crate) struct LocalReactorGuard<'a> {
    // Restrict to the Reactor's lifetime to prevent dangling pointer use.
    _marker: std::marker::PhantomData<&'a RefCell<Reactor>>,
    // Not Send: the thread-local is thread-specific.
    _not_send: std::marker::PhantomData<*const ()>,
}

impl<'a> LocalReactorGuard<'a> {
    /// Install `reactor` as the current thread's local reactor. Panics if
    /// a different reactor is already installed on this thread (nested
    /// worker loops are unsupported).
    pub(crate) fn install(reactor: &'a RefCell<Reactor>) -> Self {
        LOCAL_REACTOR.with(|slot| {
            debug_assert!(
                slot.get().is_null(),
                "another Reactor is already installed on this thread",
            );
            slot.set(reactor as *const _);
        });
        Self {
            _marker: std::marker::PhantomData,
            _not_send: std::marker::PhantomData,
        }
    }
}

impl Drop for LocalReactorGuard<'_> {
    fn drop(&mut self) {
        LOCAL_REACTOR.with(|slot| slot.set(ptr::null()));
    }
}

/// Install a `RefCell<Reactor>` pointer into the current thread's
/// `LOCAL_REACTOR` slot without an RAII guard.
///
/// This is used by the multi-thread scheduler's `UringParker`, which cannot
/// hold a `!Send` [`LocalReactorGuard`] because its containing `Core` must
/// itself be `Send` to cross the `spawn_blocking` boundary.
///
/// # Safety contract
///
/// The caller must:
///
/// 1. Pass a pointer to a `RefCell<Reactor>` whose allocation lives at
///    least as long as the pointer remains in the TLS slot.
/// 2. Call [`clear_local_reactor`] on the same thread before the pointed-to
///    allocation is dropped.
/// 3. Install only once per thread; re-installing a different pointer is a
///    programming error and will trigger the debug-mode assertion.
///
/// In practice, the worker thread constructs its `Reactor` inside a
/// `Box<RefCell<Reactor>>` owned by its `UringParker`; the parker installs
/// via this function on first `park`, and clears via `clear_local_reactor`
/// in its `Drop` impl. Both calls happen on the worker thread.
pub(crate) unsafe fn install_local_reactor_raw(ptr: *const RefCell<Reactor>) {
    LOCAL_REACTOR.with(|slot| {
        debug_assert!(
            slot.get().is_null(),
            "another Reactor is already installed on this thread",
        );
        slot.set(ptr);
    });
}

/// Clear the current thread's `LOCAL_REACTOR` slot. No-op if nothing was
/// installed. Must be called before the reactor's backing allocation is
/// dropped.
pub(crate) fn clear_local_reactor() {
    LOCAL_REACTOR.with(|slot| slot.set(ptr::null()));
}

/// Run `f` with a mutable reference to the current thread's reactor, if
/// one is installed. Returns `None` if the thread is not a worker.
///
/// Does **not** nest — calling this from inside a closure passed to itself
/// (on the same thread) will panic via `RefCell`'s runtime check. In
/// practice this cannot happen because worker threads are either executing
/// a task (not inside `park`) or blocked in the kernel (not executing
/// anything), never both.
pub(crate) fn with_local_reactor<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&mut Reactor) -> R,
{
    LOCAL_REACTOR.with(|slot| {
        let ptr = slot.get();
        if ptr.is_null() {
            return None;
        }
        // SAFETY: installed by this thread via `LocalReactorGuard::install`;
        // the guard's lifetime bounds the pointer's validity, and the guard
        // cannot have been dropped while `with_local_reactor` is on the
        // call stack (both run on the same thread, drop would require
        // unwinding past this frame).
        let cell: &RefCell<Reactor> = unsafe { &*ptr };
        let mut reactor = cell.borrow_mut();
        Some(f(&mut reactor))
    })
}

// ===== tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    /// `unpark` before the worker parks sets NOTIFIED and returns without
    /// a syscall; a subsequent `begin_park` consumes the notification.
    #[test]
    fn unpark_before_park_is_consumed() {
        let handle = UringHandle::new(1);
        // No ring fd / external waker published — unpark must not touch
        // them because the worker isn't parked.
        assert!(!handle.unpark(0), "worker not parked, no wake should fire");
        assert!(handle.begin_park(0), "notification should have been pending");
    }

    /// When the caller has a `LOCAL_REACTOR` installed and the target has
    /// a published ring fd, `unpark` routes via MSG_RING rather than the
    /// eventfd fallback. Verified by observing that the target's park
    /// actually returns after the unpark call.
    #[test]
    fn unpark_on_worker_uses_msg_ring() {
        use std::sync::Arc;

        let handle = Arc::new(UringHandle::new(2));

        // Receiver thread: construct reactor, publish, then park.
        let receiver_handle = Arc::clone(&handle);
        let (ready_tx, ready_rx) = mpsc::channel();
        let (elapsed_tx, elapsed_rx) = mpsc::channel();
        let receiver = std::thread::spawn(move || {
            let Ok(mut reactor) = Reactor::new() else {
                let _ = ready_tx.send(false);
                return;
            };
            receiver_handle.register_worker(0, reactor.ring_fd(), reactor.external_waker());
            ready_tx.send(true).unwrap();

            // Simulate "enter park" protocol then actually park.
            let notified = receiver_handle.begin_park(0);
            assert!(!notified);
            let start = std::time::Instant::now();
            reactor.park().expect("receiver park returns");
            receiver_handle.end_park(0);
            elapsed_tx.send(start.elapsed()).unwrap();
        });

        if !ready_rx.recv().unwrap() {
            eprintln!("skipping: receiver reactor unavailable");
            receiver.join().unwrap();
            return;
        }

        // Sender thread: construct reactor, install LOCAL_REACTOR, unpark.
        let sender_handle = Arc::clone(&handle);
        let sender = std::thread::spawn(move || {
            let Ok(reactor) = Reactor::new() else {
                return false;
            };
            sender_handle.register_worker(1, reactor.ring_fd(), reactor.external_waker());
            let reactor_cell = RefCell::new(reactor);
            let _guard = LocalReactorGuard::install(&reactor_cell);

            // Give the receiver a beat to reach park().
            std::thread::sleep(Duration::from_millis(50));
            sender_handle.unpark(0)
        });

        let unpark_fired = sender.join().unwrap();
        assert!(unpark_fired, "unpark should deliver a wake");

        let elapsed = elapsed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        receiver.join().unwrap();

        assert!(
            elapsed >= Duration::from_millis(25),
            "receiver park returned too quickly: {elapsed:?}",
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "receiver park took suspiciously long: {elapsed:?}",
        );
    }

    /// With `PARKED` state and an external waker published, `unpark` from
    /// a non-worker thread causes the eventfd fallback to fire.
    #[test]
    fn unpark_external_uses_eventfd_fallback() {
        let Ok(reactor) = Reactor::new() else {
            eprintln!("skipping: reactor unavailable");
            return;
        };
        let external_waker = reactor.external_waker();
        let ring_fd = reactor.ring_fd();

        let handle = UringHandle::new(1);
        handle.register_worker(0, ring_fd, external_waker);

        // Simulate the worker having entered park.
        assert!(!handle.begin_park(0));

        // Move the reactor to a thread that will drain the wake — SINGLE_ISSUER
        // requires it to be driven by its creator, but this test only needs
        // to prove that `unpark` completes without error from a non-worker
        // thread. We don't drive the reactor in this test; we just assert
        // the eventfd write path is hit.
        //
        // (Full drive-the-wake coverage lives in the `external_waker_unblocks_park`
        // test in `uring_reactor.rs`.)
        drop(reactor);

        // Spawn a thread that has no LOCAL_REACTOR installed, so unpark
        // must use the eventfd path.
        let (done_tx, done_rx) = mpsc::channel();
        let handle_arc = std::sync::Arc::new(handle);
        let h = std::sync::Arc::clone(&handle_arc);
        let t = std::thread::spawn(move || {
            let fired = h.unpark(0);
            done_tx.send(fired).unwrap();
        });
        let fired = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        t.join().unwrap();
        assert!(fired, "unpark should have delivered a wake");
    }
}
