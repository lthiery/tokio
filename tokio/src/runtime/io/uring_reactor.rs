//! Per-worker io_uring reactor (experimental).
//!
//! This is an alternative to the mio-backed [`Driver`] that uses io_uring in
//! readiness mode (via `POLL_ADD_MULTI`) for fd readiness events. Each worker
//! owns its own ring, eliminating submission contention. Cross-worker wakeups
//! use `MSG_RING`; external-thread wakeups use a per-worker eventfd registered
//! with `POLL_ADD_MULTI`.
//!
//! Scope for v1: readiness-model only — same semantics as the mio driver. Ops
//! like `RECV_MULTI`, `SEND_ZC`, and `ACCEPT_MULTI` are out of scope and will
//! layer on top later.
//!
//! # user_data encoding
//!
//! CQE `user_data` is a 64-bit value. We use specific sentinels for
//! internally-generated SQEs and reserve everything else for exposed
//! [`ScheduledIo`] pointers:
//!
//! - [`USER_DATA_EVENTFD`]  — an external-thread wake arrived on our eventfd.
//! - [`USER_DATA_MSG_RING`] — a cross-worker `MSG_RING` wake.
//! - [`USER_DATA_IGNORE`]   — the completion of a control SQE (POLL_REMOVE,
//!   TIMEOUT cancel, etc.) whose result we don't care about.
//! - Any other value is an exposed `*const ScheduledIo` via
//!   [`super::EXPOSE_IO`].
//!
//! The sentinels occupy the top of the 64-bit range, well outside the
//! canonical address range of any user-space pointer, so there is no
//! collision risk.
//!
//! [`Driver`]: super::driver::Driver
//! [`ScheduledIo`]: super::ScheduledIo

use io_uring::{opcode, types, IoUring};

use crate::io::{Interest, Ready};
use crate::loom::sync::Arc;
use crate::runtime::io::driver::Tick;
use crate::runtime::io::ScheduledIo;

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::Duration;

/// Submission queue depth. Per-worker, so small is fine.
const SQ_ENTRIES: u32 = 512;

/// Completion queue depth. Sized larger than the SQ to absorb bursts during
/// slow drains (4× the io_uring default of 2048).
const CQ_ENTRIES: u32 = 8192;

/// `user_data` sentinel for the per-worker eventfd's POLL_ADD_MULTI CQE.
pub(crate) const USER_DATA_EVENTFD: u64 = u64::MAX;

/// `user_data` sentinel for cross-worker `MSG_RING` wakes.
pub(crate) const USER_DATA_MSG_RING: u64 = u64::MAX - 1;

/// `user_data` sentinel for control SQEs whose completion we ignore.
pub(crate) const USER_DATA_IGNORE: u64 = u64::MAX - 2;

/// Smallest reserved sentinel — anything `<` this is treated as a
/// `ScheduledIo` pointer. Chosen to leave a comfortable gap above any
/// plausible user-space pointer on 64-bit Linux (canonical addresses are at
/// most 57 bits today).
const RESERVED_SENTINEL_FLOOR: u64 = u64::MAX - 15;

/// Per-worker io_uring reactor.
///
/// Owns a single [`IoUring`] instance. The owning worker is the sole submitter
/// (enforced by `IORING_SETUP_SINGLE_ISSUER`); cross-worker wakeups come in via
/// `MSG_RING` SQEs submitted on the sender's own ring; external-thread wakeups
/// come in via a per-reactor [`eventfd`] registered with `POLL_ADD_MULTI` on
/// this same ring.
///
/// [`eventfd`]: https://man7.org/linux/man-pages/man2/eventfd.2.html
pub(crate) struct Reactor {
    ring: IoUring,
    /// eventfd for external-thread wakeups. Held behind an `Arc` so
    /// [`ExternalWaker`]s can be handed out cheaply to non-worker threads.
    external_wake_fd: Arc<OwnedFd>,
}

/// Thread-safe handle for waking a [`Reactor`] from a non-worker thread.
///
/// A write to the underlying eventfd races with the reactor's `park()` call
/// and causes the POLL_ADD_MULTI registration on the eventfd to fire, which
/// posts a CQE with `USER_DATA_EVENTFD` and unblocks the park.
///
/// Cheap to clone — it's just an `Arc<OwnedFd>`.
#[derive(Clone, Debug)]
pub(crate) struct ExternalWaker {
    fd: Arc<OwnedFd>,
}

impl ExternalWaker {
    /// Wake the owning reactor. Thread-safe; may be called from any thread,
    /// any number of times. Coalesces: the eventfd's internal counter
    /// accumulates all writes until the reactor drains it on the next park.
    pub(crate) fn wake(&self) -> io::Result<()> {
        // eventfd writes are 8 bytes of a u64. Writing 1 increments the
        // counter by 1; any non-zero value works, the receive side only
        // cares that the fd is readable.
        let buf = 1u64.to_ne_bytes();
        // SAFETY: valid fd, valid buffer, correct length.
        let ret = unsafe {
            libc::write(
                self.fd.as_raw_fd(),
                buf.as_ptr().cast(),
                buf.len(),
            )
        };
        if ret < 0 {
            let err = io::Error::last_os_error();
            // EAGAIN on a non-blocking eventfd means the counter is already
            // at u64::MAX - 1, which implies the reactor is behind but a
            // wake is already pending. That's fine — treat it as success.
            if err.raw_os_error() == Some(libc::EAGAIN) {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }
}

impl Reactor {
    /// Create a new reactor, performing `io_uring_setup` with the flags
    /// required by the design:
    ///
    /// - `IORING_SETUP_SINGLE_ISSUER` — submission is worker-exclusive.
    /// - `IORING_SETUP_DEFER_TASKRUN` — task_work runs at `io_uring_enter`
    ///   time, avoiding cross-CPU IPIs. Requires SINGLE_ISSUER.
    /// - `IORING_SETUP_COOP_TASKRUN` — cooperative task_work scheduling.
    ///
    /// Kernel requirement: Linux 6.0+ (DEFER_TASKRUN landed in 5.19 but was
    /// not mature until 6.x; we target 6.0 as the minimum supported kernel).
    ///
    /// # Thread binding
    ///
    /// `new()` performs an initial `io_uring_enter` to submit the eventfd
    /// registration. Because `IORING_SETUP_SINGLE_ISSUER` binds the ring's
    /// submitter identity on the first `enter`, **`new()` must be called on
    /// the same thread that will drive the reactor** — typically, the worker
    /// thread itself during runtime spawn. Constructing a `Reactor` on one
    /// thread and moving it to another will cause subsequent `park()` calls
    /// to fail with `EEXIST`.
    pub(crate) fn new() -> io::Result<Self> {
        let mut ring = IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .setup_cqsize(CQ_ENTRIES)
            .build(SQ_ENTRIES)?;

        let external_wake_fd = make_eventfd()?;
        register_eventfd_multishot(&mut ring, external_wake_fd.as_raw_fd())?;

        Ok(Self {
            ring,
            external_wake_fd: Arc::new(external_wake_fd),
        })
    }

    /// Raw fd of the underlying ring. Needed by other workers so they can
    /// submit `MSG_RING` SQEs targeting this reactor's CQ.
    pub(crate) fn ring_fd(&self) -> RawFd {
        self.ring.as_raw_fd()
    }

    /// Obtain a thread-safe waker that can unblock this reactor's `park()`
    /// from any thread, including non-worker threads (spawn_blocking,
    /// external code calling `waker.wake()`).
    ///
    /// The returned `ExternalWaker` is cheap to clone and may be held across
    /// runtime shutdown — waking a dead reactor is a no-op error which
    /// callers should ignore.
    #[allow(dead_code)]
    pub(crate) fn external_waker(&self) -> ExternalWaker {
        ExternalWaker {
            fd: self.external_wake_fd.clone(),
        }
    }

    /// Send a `MSG_RING` wake to another reactor's ring.
    ///
    /// Must be called from the worker that owns **this** reactor —
    /// `SINGLE_ISSUER` requires submission on our own ring.
    ///
    /// Per the design, this flushes immediately (`io_uring_enter(submit,
    /// min_complete=0)`) rather than deferring to our next park, so the
    /// target worker receives the wake with low latency. Cost is one
    /// non-blocking syscall, comparable to an eventfd write.
    #[allow(dead_code)]
    pub(crate) fn send_msg_ring(&mut self, target_ring_fd: RawFd) -> io::Result<()> {
        let sqe = opcode::MsgRingData::new(
            types::Fd(target_ring_fd),
            0,                    // `result` — surfaces as CQE.result on receiver; unused.
            USER_DATA_MSG_RING,   // CQE user_data posted on the *target* ring.
            None,                 // no user_flags pass-through.
        )
        .build()
        .user_data(USER_DATA_IGNORE); // our own completion (MSG send ack) is discarded.

        // SAFETY: MsgRingData references no user buffers; always safe.
        unsafe { self.push_sqe(sqe)? };

        // Flush immediately — do not wait for park. Non-blocking submit.
        self.ring.submit()?;
        Ok(())
    }

    /// Register interest in readiness events for `fd`.
    ///
    /// Pushes a multi-shot `POLL_ADD` SQE with `user_data` set to the exposed
    /// pointer of `scheduled_io`. The SQE is staged in the submission ring but
    /// not submitted; it flushes at the next park, or sooner if the SQ fills
    /// up.
    ///
    /// # Safety of the `user_data` pointer
    ///
    /// `scheduled_io` is held behind an `Arc` by the [`RegistrationSet`]; it
    /// will not be freed until it is removed from that set *and* the I/O
    /// driver is not concurrently draining. Our deregister path issues
    /// `POLL_REMOVE` before the `Arc` is dropped, and park drains all CQEs
    /// synchronously, so the pointer remains valid for the lifetime of the
    /// registration.
    ///
    /// [`RegistrationSet`]: super::RegistrationSet
    pub(crate) fn register(
        &mut self,
        fd: RawFd,
        interest: Interest,
        scheduled_io: &ScheduledIo,
    ) -> io::Result<()> {
        let user_data = token_for(scheduled_io);
        debug_assert!(
            user_data < RESERVED_SENTINEL_FLOOR,
            "ScheduledIo pointer collides with a reserved user_data sentinel",
        );

        let mask = poll_mask_from_interest(interest);
        let sqe = opcode::PollAdd::new(types::Fd(fd), mask)
            .multi(true)
            .build()
            .user_data(user_data);

        // SAFETY: `sqe`'s operands are valid for the lifetime of the multi-shot
        // registration. The fd is owned by the caller and deregister() issues
        // a matching POLL_REMOVE before the ScheduledIo pointer is dropped.
        unsafe { self.push_sqe(sqe) }
    }

    /// Deregister a previously-registered fd by submitting a `POLL_REMOVE`
    /// keyed on the `ScheduledIo` pointer that was used as `user_data`.
    ///
    /// The REMOVE's own completion is tagged with [`USER_DATA_IGNORE`] and
    /// discarded during drain.
    pub(crate) fn deregister(&mut self, scheduled_io: &ScheduledIo) -> io::Result<()> {
        let target = token_for(scheduled_io);
        let sqe = opcode::PollRemove::new(target).build().user_data(USER_DATA_IGNORE);
        // SAFETY: `PollRemove` references no user buffers; it is always safe.
        unsafe { self.push_sqe(sqe) }
    }

    /// Block until at least one CQE is available, then drain completions.
    ///
    /// Performs one `io_uring_enter(submit=pending, min_complete=1,
    /// GETEVENTS)` call. Any SQEs staged since the last submit (by
    /// `register`/`deregister`) flush as part of the same syscall.
    pub(crate) fn park(&mut self) -> io::Result<()> {
        self.ring.submit_and_wait(1)?;
        self.drain_completions();
        Ok(())
    }

    /// Like [`park`] but bounded by `timeout`. A zero duration polls without
    /// blocking.
    ///
    /// Implemented by prepending a `TIMEOUT` SQE to the submission batch; the
    /// timeout's own CQE is discarded. Using a linked timeout SQE rather than
    /// the `io_uring_enter` `arg` parameter keeps the code path uniform.
    ///
    /// [`park`]: Reactor::park
    pub(crate) fn park_timeout(&mut self, timeout: Duration) -> io::Result<()> {
        if timeout.is_zero() {
            // Non-blocking drain: submit pending work without waiting.
            self.ring.submit()?;
            self.drain_completions();
            return Ok(());
        }

        let ts = types::Timespec::new()
            .sec(timeout.as_secs())
            .nsec(timeout.subsec_nanos());
        let sqe = opcode::Timeout::new(&ts as *const _)
            .build()
            .user_data(USER_DATA_IGNORE);

        // SAFETY: `ts` lives until submit_and_wait returns; the kernel copies
        // the Timespec value during submission.
        unsafe { self.push_sqe(sqe)? };

        self.ring.submit_and_wait(1)?;
        self.drain_completions();
        Ok(())
    }

    /// Drain the completion queue, dispatching readiness to [`ScheduledIo`]s
    /// and ignoring control-SQE completions.
    ///
    /// This is also the hook point for a future stealer: the logic here is
    /// purely CQE-driven and would work just as well against a victim ring's
    /// CQ once CAS-based head advancement is added.
    fn drain_completions(&mut self) {
        // Collect the eventfd fd up front so we can drain it without
        // borrowing `self` mutably while iterating the CQ.
        let external_fd = self.external_wake_fd.as_raw_fd();
        let mut saw_external_wake = false;

        let cq = self.ring.completion();
        for cqe in cq {
            match cqe.user_data() {
                USER_DATA_EVENTFD => {
                    // External-thread wake arrived. We drain the eventfd's
                    // counter below (outside the CQ borrow) so subsequent
                    // writes produce fresh CQEs. The POLL_ADD_MULTI
                    // registration auto-rearms — no resubmission needed.
                    saw_external_wake = true;
                }
                USER_DATA_MSG_RING | USER_DATA_IGNORE => {
                    // Cross-worker wake: nothing to dispatch here, the
                    // scheduler handles the "check task queues" logic
                    // *around* park(). Control SQE completions (POLL_REMOVE,
                    // TIMEOUT, MSG_RING send-ack) are discarded.
                }
                ptr_value => {
                    let flags = cqe.result();
                    if flags < 0 {
                        // Negative result on POLL_ADD_MULTI means the
                        // registration was cancelled (typically by our own
                        // POLL_REMOVE, or by the kernel on shutdown). No
                        // readiness to dispatch.
                        continue;
                    }
                    let ready = ready_from_poll_flags(flags);
                    // SAFETY: `ptr_value` is an exposed pointer published by
                    // [`ScheduledIo::token`]. Its target is kept alive by the
                    // `Arc` in the registration set until matching
                    // POLL_REMOVE completes; see the `register` safety note.
                    let io: &ScheduledIo =
                        unsafe { &*super::EXPOSE_IO.from_exposed_addr(ptr_value as usize) };
                    io.set_readiness(Tick::Set, |curr| curr | ready);
                    io.wake(ready);
                }
            }
        }
        // The iterator consumed `cq`, so its drop ran at the end of the
        // `for` loop and the updated head pointer has already been written
        // back to the kernel.

        if saw_external_wake {
            drain_eventfd(external_fd);
        }
    }

    /// Push an SQE into the submission ring, flushing to the kernel if the
    /// ring is full and retrying.
    ///
    /// # Safety
    ///
    /// Callers must ensure any operands referenced by `sqe` (buffers, fds,
    /// timespecs) remain valid for the duration of the operation.
    unsafe fn push_sqe(&mut self, sqe: io_uring::squeue::Entry) -> io::Result<()> {
        loop {
            // SAFETY: forwarded from the caller.
            if unsafe { self.ring.submission().push(&sqe) }.is_ok() {
                return Ok(());
            }
            // SQ is full — flush without waiting and retry.
            self.ring.submit()?;
        }
    }
}

impl std::fmt::Debug for Reactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reactor").finish_non_exhaustive()
    }
}

/// Convert a `ScheduledIo` reference into the `u64` `user_data` we stash on
/// its SQEs. Uses the same `EXPOSE_IO` mechanism as the mio path so both
/// drivers can coexist and share the readiness machinery.
fn token_for(scheduled_io: &ScheduledIo) -> u64 {
    super::EXPOSE_IO.expose_provenance(scheduled_io) as u64
}

/// Create a non-blocking, close-on-exec eventfd for external-thread wakeups.
///
/// The counter starts at zero; writes increment it, reads drain it to zero.
/// Non-blocking so our drain read never stalls the worker; close-on-exec so
/// it doesn't leak into child processes.
fn make_eventfd() -> io::Result<OwnedFd> {
    // SAFETY: eventfd2 is a straightforward syscall with no pointer args;
    // we check the return value for -1.
    let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: raw is a valid, owned fd (we just created it).
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

/// Register `fd` on `ring` with a multi-shot POLL_ADD for readable events
/// (the eventfd is only ever read-ready). Submits synchronously so the
/// registration is live before `new()` returns.
fn register_eventfd_multishot(ring: &mut IoUring, fd: RawFd) -> io::Result<()> {
    let sqe = opcode::PollAdd::new(types::Fd(fd), libc::POLLIN as u32)
        .multi(true)
        .build()
        .user_data(USER_DATA_EVENTFD);

    // SAFETY: fd outlives the registration (stored on the Reactor as an
    // OwnedFd); matching POLL_REMOVE is issued implicitly by ring teardown
    // when the reactor is dropped.
    while unsafe { ring.submission().push(&sqe) }.is_err() {
        ring.submit()?;
    }
    ring.submit()?;
    Ok(())
}

/// Drain an eventfd's counter. Non-blocking reads return either 8 bytes (the
/// counter value) or EAGAIN (counter was zero, nothing to drain). We
/// discard the value — we only care that the counter is reset so the next
/// write produces a fresh CQE.
fn drain_eventfd(fd: RawFd) {
    let mut buf = [0u8; 8];
    // SAFETY: valid fd, valid 8-byte buffer, correct length.
    let _ = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    // Errors are intentionally ignored: EAGAIN means already drained,
    // EBADF means the fd is gone (reactor shutting down). Either way the
    // wake has been acknowledged by virtue of the CQE reaching us.
}

/// Translate tokio's [`Interest`] into an epoll/poll event mask suitable for
/// `POLL_ADD`. We always include `POLLRDHUP` so that `READ_CLOSED` readiness
/// is surfaced, matching the mio driver's behavior.
fn poll_mask_from_interest(interest: Interest) -> u32 {
    let mut mask: u32 = 0;
    if interest.is_readable() {
        mask |= libc::POLLIN as u32 | libc::POLLRDHUP as u32;
    }
    if interest.is_writable() {
        mask |= libc::POLLOUT as u32;
    }
    // Errors and hangups are always reported.
    mask |= libc::POLLERR as u32 | libc::POLLHUP as u32;
    mask
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Ring construction succeeds on a supported kernel. Smoke test for the
    /// setup flags — if SINGLE_ISSUER/DEFER_TASKRUN aren't available we want
    /// the failure surfaced here, not deep inside park().
    #[test]
    fn reactor_new_succeeds() {
        let reactor = Reactor::new();
        match reactor {
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::ENOSYS) => {
                // Kernel without io_uring support — skip.
                eprintln!("skipping: io_uring not supported on this kernel");
            }
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                // Older kernel missing one of our setup flags.
                eprintln!("skipping: required io_uring setup flags unavailable ({e})");
            }
            Err(e) => panic!("unexpected error from Reactor::new: {e}"),
        }
    }

    /// A zero-timeout park on an idle reactor returns immediately without
    /// error. Exercises the submit + drain path against an empty ring.
    #[test]
    fn park_timeout_zero_is_noop() {
        let Ok(mut reactor) = Reactor::new() else {
            eprintln!("skipping: reactor unavailable");
            return;
        };
        reactor.park_timeout(Duration::ZERO).expect("park_timeout(0) should succeed");
    }

    /// Writing to the external waker from another thread unblocks a
    /// parked reactor. Exercises the eventfd → POLL_ADD_MULTI → CQE → drain
    /// path end-to-end.
    #[test]
    fn external_waker_unblocks_park() {
        let Ok(mut reactor) = Reactor::new() else {
            eprintln!("skipping: reactor unavailable");
            return;
        };
        let waker = reactor.external_waker();

        let handle = std::thread::spawn(move || {
            // Give the main thread a moment to enter park().
            std::thread::sleep(Duration::from_millis(50));
            waker.wake().expect("wake() should succeed");
        });

        let start = std::time::Instant::now();
        reactor.park().expect("park should return after external wake");
        let elapsed = start.elapsed();

        handle.join().unwrap();

        // Sanity: we should have blocked ~50ms, not 0 (which would mean the
        // wake was already pending before park) and not hit an internal
        // timeout (we have none). Allow wide slop for CI noise.
        assert!(
            elapsed >= Duration::from_millis(25),
            "park returned too quickly: {elapsed:?}",
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "park took suspiciously long: {elapsed:?}",
        );
    }

    /// A reactor can send a MSG_RING wake to another reactor and unblock
    /// its park. Exercises the cross-worker wake path.
    ///
    /// Each reactor must be constructed on the same thread that will drive
    /// it, because `IORING_SETUP_SINGLE_ISSUER` binds the ring's submitter
    /// identity at the first `io_uring_enter` call. We pass the target
    /// ring's fd across a channel rather than moving the receiver.
    #[test]
    fn msg_ring_wakes_peer() {
        use std::sync::mpsc;

        let (fd_tx, fd_rx) = mpsc::channel::<RawFd>();
        let (elapsed_tx, elapsed_rx) = mpsc::channel::<Duration>();

        // Receiver thread: owns its own reactor end-to-end.
        let receiver_thread = std::thread::spawn(move || {
            let Ok(mut receiver) = Reactor::new() else {
                // Propagate a dummy target so the sender side doesn't hang.
                fd_tx.send(-1).unwrap();
                return;
            };
            fd_tx.send(receiver.ring_fd()).unwrap();

            let start = std::time::Instant::now();
            receiver.park().expect("receiver park should return");
            elapsed_tx.send(start.elapsed()).unwrap();
        });

        let target_fd = fd_rx.recv().unwrap();
        if target_fd == -1 {
            eprintln!("skipping: receiver reactor unavailable");
            receiver_thread.join().unwrap();
            return;
        }

        let Ok(mut sender) = Reactor::new() else {
            eprintln!("skipping: sender reactor unavailable");
            // Send something to unblock the receiver (an external wake via
            // a fresh eventfd would also work, but we're already here).
            let _ = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(10));
            });
            return;
        };

        // Give the receiver time to enter park() before we send.
        std::thread::sleep(Duration::from_millis(50));
        sender.send_msg_ring(target_fd).expect("send_msg_ring should succeed");

        receiver_thread.join().unwrap();
        let elapsed = elapsed_rx.recv().unwrap();

        assert!(
            elapsed >= Duration::from_millis(25),
            "receiver park returned too quickly: {elapsed:?}",
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "receiver park took suspiciously long: {elapsed:?}",
        );
    }
}

/// Translate a CQE poll-result bitmask into tokio's [`Ready`].
fn ready_from_poll_flags(flags: i32) -> Ready {
    let flags = flags as i16;
    let mut ready = Ready::EMPTY;
    if flags & libc::POLLIN != 0 {
        ready |= Ready::READABLE;
    }
    if flags & libc::POLLOUT != 0 {
        ready |= Ready::WRITABLE;
    }
    if flags & libc::POLLRDHUP != 0 {
        ready |= Ready::READ_CLOSED;
    }
    if flags & libc::POLLHUP != 0 {
        // HUP fires on both sides of the connection; map to WRITE_CLOSED,
        // matching mio's behavior on Linux.
        ready |= Ready::WRITE_CLOSED;
    }
    if flags & libc::POLLERR != 0 {
        ready |= Ready::ERROR;
    }
    if flags & libc::POLLPRI != 0 {
        ready |= Ready::PRIORITY;
    }
    ready
}
