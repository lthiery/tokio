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
//! See the design notes in `docs/uring-reactor.md` (TODO).
//!
//! [`Driver`]: super::driver::Driver

use io_uring::IoUring;
use std::io;

/// Submission queue depth. Per-worker, so small is fine.
const SQ_ENTRIES: u32 = 512;

/// Completion queue depth. Sized larger than the SQ to absorb bursts during
/// slow drains (4× the io_uring default of 2048).
const CQ_ENTRIES: u32 = 8192;

/// Per-worker io_uring reactor.
///
/// Owns a single [`IoUring`] instance. The owning worker is the sole submitter
/// (enforced by `IORING_SETUP_SINGLE_ISSUER`); cross-worker wakeups come in via
/// `MSG_RING` SQEs submitted on the sender's own ring.
pub(crate) struct Reactor {
    /// The kernel ring. `None` would indicate uninitialized, but we initialize
    /// eagerly so this is always `Some` for a live reactor.
    ring: IoUring,
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
    pub(crate) fn new() -> io::Result<Self> {
        let ring = IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .setup_cqsize(CQ_ENTRIES)
            .build(SQ_ENTRIES)?;

        Ok(Self { ring })
    }

    /// Block until at least one CQE is available, then drain the completion
    /// queue. Flushes any pending SQEs as part of the same `io_uring_enter`
    /// call — this is the primary submission path (batched at park time).
    #[allow(dead_code)]
    pub(crate) fn park(&mut self) -> io::Result<()> {
        // TODO: batched submit + min_complete=1 enter, then drain CQEs and
        // dispatch readiness to ScheduledIo.
        unimplemented!("Reactor::park")
    }

    /// Like `park`, but with a timeout. Implemented by chaining a `TIMEOUT`
    /// SQE into the submission batch (or using the `timespec` argument to
    /// `io_uring_enter`, TBD based on benchmarks).
    #[allow(dead_code)]
    pub(crate) fn park_timeout(&mut self, _timeout: std::time::Duration) -> io::Result<()> {
        // TODO
        unimplemented!("Reactor::park_timeout")
    }

    /// Raw fd of the underlying ring. Needed by other workers so they can
    /// submit `MSG_RING` SQEs targeting this reactor's CQ.
    #[allow(dead_code)]
    pub(crate) fn ring_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.ring.as_raw_fd()
    }
}

impl std::fmt::Debug for Reactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reactor").finish_non_exhaustive()
    }
}
