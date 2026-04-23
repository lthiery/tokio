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
//! # `user_data` encoding
//!
//! CQE `user_data` is a 64-bit value encoded as
//! `(variant: u8, gen: u24, key: u32)`:
//!
//! ```text
//! bit 63       bit 56        bit 32                    bit 0
//!  │            │             │                         │
//!  ├────────────┼─────────────┼─────────────────────────┤
//!  │  variant   │     gen     │           key           │
//!  │  (8 bit)   │   (24 bit)  │        (32 bit)         │
//!  └────────────┴─────────────┴─────────────────────────┘
//! ```
//!
//! - `key` indexes into a per-reactor [`Slab`] of [`OpState`].
//! - `gen` is a per-reactor monotonic generation counter, bumped on every
//!   slab insert. Stale CQEs that arrive after a slot has been recycled are
//!   detected by gen-mismatch and silently dropped. 24 bits gives ~16M
//!   distinct generations; collision requires an in-flight CQE to survive
//!   that many subsequent slab inserts, which is not physically achievable.
//! - `variant` is a fast-path discriminator so the hot drain loop avoids an
//!   enum match per CQE:
//!     - [`VARIANT_POLL_MULTI`] — multi-shot `POLL_ADD` registration.
//!     - [`VARIANT_CONTROL`] — one-shot control op (POLL_REMOVE ack,
//!       TIMEOUT, MSG_RING send-ack) whose result we discard.
//!     - [`VARIANT_EVENTFD`] — external-thread wake delivered via eventfd.
//!     - [`VARIANT_MSG_RING_INCOMING`] — cross-worker wake delivered via
//!       `MSG_RING` from a peer reactor.
//!
//! The encoding replaces the legacy `EXPOSE_IO`-pointer scheme: the kernel
//! never sees a pointer, so pointer-reuse races are impossible. The
//! [`OpState::PollMulti`] arm holds the registration's `Arc<ScheduledIo>`
//! until the *terminal* CQE for that slot arrives (no `IORING_CQE_F_MORE`),
//! at which point the slab entry is removed and the `Arc` is dropped. This
//! is a deterministic, kernel-handshake-bounded lifetime — no time-based
//! retention pipeline.
//!
//! [`Driver`]: super::driver::Driver
//! [`ScheduledIo`]: super::ScheduledIo
//! [`Slab`]: slab::Slab

use io_uring::{cqueue, opcode, types, IoUring};
use slab::Slab;

use crate::io::{Interest, Ready};
use crate::loom::sync::Arc;
use crate::runtime::io::driver::Tick;
use crate::runtime::io::ScheduledIo;

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Submission queue depth. Per-worker, so small is fine: the steady-state
/// SQE rate is dominated by one POLL_ADD_MULTI per registered fd (submitted
/// once, lives until dereg) plus a handful of per-park control ops. A worker
/// with dozens of live registrations stages well under 64 SQEs between
/// submissions.
///
/// Sized at 128 — a comfortable 2–4× over observed peaks in
/// `net_uring_bench.rs`. The prior 512 was needlessly large and, combined
/// with the oversized CQ below, pinned ~160 KB per ring; under 4-way
/// parallel test execution (`cargo test --test-threads=4`, 8 concurrent
/// workers cold-starting rings together) that sizing pushed the kernel
/// slab allocator into contention, causing individual `io_uring_setup`
/// calls to take 10–18 ms and several to return `-ENOMEM` outright. See
/// the bpftrace analysis captured during the `tcp_read_blocks_then_wakes`
/// flake investigation for the evidence.
const SQ_ENTRIES: u32 = 128;

/// Completion queue depth. Sized to absorb peak CQE bursts without
/// overflowing — level-triggered `POLL_ADD_MULTI` can fire repeatedly for
/// the same fd while data is available, so CQE occupancy under fan-out
/// load (see `tcp_many_concurrent_connections`, 64 live sockets + rapid
/// read/write flips) can briefly exceed 1024 entries between park cycles.
/// 4096 keeps a comfortable overflow margin while still halving the prior
/// 8192 sizing (saving 64 KB of locked memory per ring). Kernel overflow
/// recovery via `IORING_FEAT_NODROP` is a correctness fallback, not a
/// performance one: once the CQ overflows, `submit` paths return `-EBUSY`
/// and we observe hangs in the multishot-POLL drain path — so we stay
/// generously above the working-set size.
const CQ_ENTRIES: u32 = 4096;

/// Process-wide permit for `io_uring_setup`.
///
/// Expressed as a 1-permit semaphore (a `Mutex<()>`). With one permit,
/// setup calls are fully serialized; raising the permit count to N would
/// let N setups proceed concurrently. One is the right default because
/// concurrent setup is exactly what drives the contention we want to
/// avoid — high-order page allocations (after our ring-size shrink:
/// order-2 for the SQ, order-5 for the CQ) via
/// `__get_free_pages(__GFP_NOWARN | __GFP_RETRY_MAYFAIL)` start failing
/// or stalling when several rings compete for the buddy allocator at the
/// same time. Under `cargo test --test-threads=4` we measured individual
/// `io_uring_setup` calls taking 10–18 ms and some returning `-ENOMEM`.
///
/// The permit is held only for the `build()` call itself. Setup happens
/// once per worker at runtime startup and never again, so there is no
/// steady-state cost. Serialization shifts the cold-start cost from
/// "concurrent and quadratic in worker count" to "serial and linear",
/// which is a win on both total wall time and tail latency.
///
/// Note that this serialization is deliberately process-wide, not
/// per-runtime: the contention is on kernel resources shared across all
/// io_uring instances on the host, so a per-runtime lock would not catch
/// the cross-runtime case (multiple `#[tokio::test]` suites, multiple
/// in-process runtimes, etc.).
static RING_SETUP_PERMIT: std::sync::Mutex<()> = std::sync::Mutex::new(());

// ===== user_data variant tags =====

/// Multi-shot `POLL_ADD` registration. The hot path.
const VARIANT_POLL_MULTI: u8 = 0x00;

/// One-shot control op (POLL_REMOVE ack, TIMEOUT, MSG_RING send-ack).
const VARIANT_CONTROL: u8 = 0x01;

/// External-thread wake delivered via eventfd `POLL_ADD_MULTI`.
const VARIANT_EVENTFD: u8 = 0x02;

/// Cross-worker wake delivered via a peer's `MSG_RING`.
const VARIANT_MSG_RING_INCOMING: u8 = 0x03;

// ===== well-known slab keys =====
//
// These are populated as the very first inserts in `Reactor::new`, in this
// order, so peers can encode the receiver's incoming-MSG_RING `user_data`
// without per-worker advertisement.

/// Slab key for the eventfd `POLL_ADD_MULTI` registration. Inserted first.
const KEY_EVENTFD: u32 = 0;

/// Slab key for the incoming-MSG_RING slot. Inserted second.
const KEY_MSG_RING_INCOMING: u32 = 1;

/// Encoded `user_data` value that peers stamp on `MsgRingData` SQEs targeting
/// our ring. The slot is reactor-lifetime (gen never advances), so this is a
/// universal constant — no per-peer advertisement is required.
const MSG_RING_INCOMING_UD: u64 = encode(VARIANT_MSG_RING_INCOMING, 0, KEY_MSG_RING_INCOMING);

/// Encoded `user_data` for the eventfd POLL_ADD_MULTI registration. Submitted
/// once at construction and never re-issued.
const EVENTFD_UD: u64 = encode(VARIANT_EVENTFD, 0, KEY_EVENTFD);

// ===== encoding helpers =====

/// Encode a `(variant, gen, key)` triple into a 64-bit `user_data`.
///
/// `gen` is masked to 24 bits; the caller is expected to keep it within
/// range (the per-reactor counter wraps modulo 2^24).
const fn encode(variant: u8, gen: u32, key: u32) -> u64 {
    ((variant as u64) << 56) | (((gen as u64) & 0x00FF_FFFF) << 32) | (key as u64)
}

/// Decode a 64-bit `user_data` back into `(variant, gen, key)`.
const fn decode(ud: u64) -> (u8, u32, u32) {
    let variant = (ud >> 56) as u8;
    let gen = ((ud >> 32) & 0x00FF_FFFF) as u32;
    let key = ud as u32;
    (variant, gen, key)
}

// ===== slab entry / op state =====

/// Per-slot entry in the reactor's [`Slab`].
struct SlotEntry {
    /// Generation at which this slot was last inserted into. Compared
    /// against the `gen` field decoded from incoming CQEs to detect stale
    /// CQEs that arrived after a slot was recycled.
    gen: u32,
    state: OpState,
}

/// State stored alongside each in-flight io_uring op.
enum OpState {
    /// Multi-shot `POLL_ADD`. Lives from `register` until the kernel posts a
    /// CQE without `IORING_CQE_F_MORE` (either `-ECANCELED` after our
    /// `POLL_REMOVE`, or autonomous kernel cleanup, e.g. on `POLLHUP`).
    PollMulti {
        io: Arc<ScheduledIo>,
        /// True once a local `deregister()` has scheduled this slot for
        /// teardown but the kernel has not yet posted the terminal CQE.
        /// The drain loop reads this flag in the `VARIANT_POLL_MULTI` arm
        /// and suppresses readiness delivery on any in-flight CQEs that
        /// land between `deregister` submission and the terminal
        /// `-ECANCELED` — the caller has already dropped interest, so
        /// delivering readiness would wake a stale task handle.
        removing: bool,
    },

    /// One-shot control op. Removed on first CQE.
    Control,

    /// The eventfd `POLL_ADD_MULTI` registration. Inserted at `Reactor::new`
    /// and never removed during the reactor's lifetime.
    Eventfd,

    /// Receive slot for incoming `MSG_RING` wakes from peer reactors.
    /// Inserted at `Reactor::new` and never removed. The slot itself has no
    /// kernel registration; peers encode this slot's key into the
    /// `MsgRingData` SQEs they push on their own rings.
    MsgRingIncoming,
}

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

    /// Active op slab keyed by `u32` (cast on insert; we cap at `u32::MAX`
    /// in practice since the slab is per-worker and bounded by active fd
    /// count).
    ops: Slab<SlotEntry>,

    /// Monotonic generation counter, bumped on every slab insert. 24-bit
    /// effective range (truncated by [`encode`]); wraparound is
    /// astronomically unlikely to collide with an outstanding CQE.
    next_gen: u32,
}

/// Thread-safe handle for waking a [`Reactor`] from a non-worker thread.
///
/// A write to the underlying eventfd races with the reactor's `park()` call
/// and causes the POLL_ADD_MULTI registration on the eventfd to fire, which
/// posts a CQE with [`VARIANT_EVENTFD`] and unblocks the park.
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
        // Acquire the process-wide `io_uring_setup` permit. Released when
        // the scoped guard drops at the end of this block. `PoisonError`
        // is ignored: the permit only guards the build call, so a prior
        // panicked holder leaves no partial state behind. See
        // `RING_SETUP_PERMIT` docs for rationale.
        let mut ring = {
            let _permit = RING_SETUP_PERMIT
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            IoUring::builder()
                .setup_single_issuer()
                .setup_defer_taskrun()
                .setup_coop_taskrun()
                .setup_cqsize(CQ_ENTRIES)
                .build(SQ_ENTRIES)?
        };

        let external_wake_fd = make_eventfd()?;

        // Pre-allocate the two reactor-lifetime slab slots in the order
        // required by the well-known-key constants. Slab fills the lowest
        // free index first, so a fresh slab gives us key=0 then key=1.
        let mut ops: Slab<SlotEntry> = Slab::new();
        let k_evt = ops.insert(SlotEntry { gen: 0, state: OpState::Eventfd });
        let k_msg = ops.insert(SlotEntry { gen: 0, state: OpState::MsgRingIncoming });
        debug_assert_eq!(k_evt as u32, KEY_EVENTFD, "well-known slab order changed");
        debug_assert_eq!(k_msg as u32, KEY_MSG_RING_INCOMING, "well-known slab order changed");

        register_eventfd_multishot(&mut ring, external_wake_fd.as_raw_fd())?;

        Ok(Self {
            ring,
            external_wake_fd: Arc::new(external_wake_fd),
            ops,
            // Start at 1; gen=0 is reserved for the never-recycled
            // well-known slots so they don't compete for the counter.
            next_gen: 1,
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
        // Allocate a Control slot for our own send-ack; it'll be removed on
        // first CQE in `drain_completions`.
        let (ack_ud, _ack_key) = self.alloc_control_slot();

        // The peer-side user_data is the universal MSG_RING_INCOMING_UD —
        // every reactor pre-allocates that slot at the same well-known key.
        let sqe = opcode::MsgRingData::new(
            types::Fd(target_ring_fd),
            0,                       // `result` — surfaces as CQE.result on receiver; unused.
            MSG_RING_INCOMING_UD,    // CQE user_data posted on the *target* ring.
            None,                    // no user_flags pass-through.
        )
        .build()
        .user_data(ack_ud);

        // SAFETY: MsgRingData references no user buffers; always safe.
        unsafe { self.push_sqe(sqe)? };

        // Flush immediately — do not wait for park. Non-blocking submit.
        self.ring.submit()?;
        Ok(())
    }

    /// Register interest in readiness events for `fd`.
    ///
    /// Allocates a slab slot holding `Arc::clone(scheduled_io)`, stamps the
    /// slot's key onto `scheduled_io.uring_slab_key`, and pushes a multi-shot
    /// `POLL_ADD` SQE whose `user_data` encodes the `(variant, gen, key)`
    /// tuple for that slot. The SQE is staged in the submission ring but
    /// not submitted; it flushes at the next park, or sooner if the SQ fills
    /// up.
    ///
    /// # Lifetime
    ///
    /// The cloned `Arc` lives in the slab until the kernel posts a CQE for
    /// this slot without `IORING_CQE_F_MORE` (terminal CQE), at which point
    /// the drain loop removes the slot and drops the `Arc`. There is no
    /// pointer-aliasing risk: the kernel only ever sees the encoded
    /// `user_data`, never the `ScheduledIo` address.
    pub(crate) fn register(
        &mut self,
        fd: RawFd,
        interest: Interest,
        scheduled_io: &Arc<ScheduledIo>,
    ) -> io::Result<()> {
        let gen = self.next_gen();
        let key = self.ops.insert(SlotEntry {
            gen,
            state: OpState::PollMulti {
                io: Arc::clone(scheduled_io),
                removing: false,
            },
        });
        let key_u32 = u32::try_from(key).expect("slab key exceeds u32");

        // Publish the (key, gen) onto the ScheduledIo so a later
        // deregister — local or cross-ring MSG_RING — can find this slot
        // and gen-check it against a possible slab recycle in between.
        // Both writes and reads for the local path happen on the owning
        // worker, so Relaxed is sufficient; cross-ring reads are ordered
        // by the MSG_RING CQE itself.
        scheduled_io.uring_slab_key.store(key_u32, Ordering::Relaxed);
        scheduled_io.uring_gen.store(gen, Ordering::Relaxed);

        let user_data = encode(VARIANT_POLL_MULTI, gen, key_u32);
        let mask = poll_mask_from_interest(interest);
        let sqe = opcode::PollAdd::new(types::Fd(fd), mask)
            .multi(true)
            .build()
            .user_data(user_data);

        // SAFETY: `sqe`'s operands (just an fd and a poll mask) are valid;
        // the multi-shot registration carries no user-buffer references.
        // The slab slot keeps the Arc alive until the terminal CQE.
        unsafe { self.push_sqe(sqe) }
    }

    /// Deregister a previously-registered fd.
    ///
    /// Looks up the slab key that `register` stored on `scheduled_io`,
    /// marks the slot as `removing`, and submits a `POLL_REMOVE` keyed on
    /// the slot's encoded `user_data`. The slot itself is **not** freed
    /// here: it stays alive (with its `Arc<ScheduledIo>` clone) until the
    /// kernel posts the terminal CQE for the multi-shot poll
    /// (`-ECANCELED` without `IORING_CQE_F_MORE`), at which point
    /// `drain_completions` removes it.
    ///
    /// The `POLL_REMOVE`'s own ack CQE is tagged with a fresh
    /// [`VARIANT_CONTROL`] slot which is freed on its single completion.
    ///
    /// The `(slab_key, slab_gen)` pair is snapshotted by the caller at the
    /// moment the deregister was scheduled. This survives slab recycles:
    /// if the slot has since been freed and re-inserted for a different
    /// registration, the gen will no longer match and we silently no-op
    /// rather than cancelling the wrong fd's poll.
    pub(crate) fn deregister(&mut self, slab_key: u32, slab_gen: u32) -> io::Result<()> {
        if slab_key == u32::MAX {
            // Never registered (or already deregistered). No-op.
            return Ok(());
        }

        // Find the slot, gen-check it against the caller's snapshot, and
        // flip `removing` for diagnostic visibility. If the slot is gone
        // or has been recycled to a different op, there's nothing to do.
        let target_ud = match self.ops.get_mut(slab_key as usize) {
            Some(entry) if entry.gen == slab_gen => match &mut entry.state {
                OpState::PollMulti { removing, .. } => {
                    *removing = true;
                    encode(VARIANT_POLL_MULTI, entry.gen, slab_key)
                }
                _ => {
                    // Gen matched but slot is not a PollMulti — shouldn't
                    // happen with disciplined caller usage.
                    debug_assert!(false, "deregister hit non-PollMulti slot");
                    return Ok(());
                }
            },
            // Stale key or slot vacant: silently drop.
            _ => return Ok(()),
        };

        let (ack_ud, _ack_key) = self.alloc_control_slot();
        let sqe = opcode::PollRemove::new(target_ud).build().user_data(ack_ud);

        // SAFETY: `PollRemove` references no user buffers; it is always safe.
        unsafe { self.push_sqe(sqe)? };
        Ok(())
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
    /// timeout's own CQE is treated as a [`VARIANT_CONTROL`] completion and
    /// freed in the drain.
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
        let (ud, _key) = self.alloc_control_slot();
        let sqe = opcode::Timeout::new(&ts as *const _).build().user_data(ud);

        // SAFETY: `ts` lives until submit_and_wait returns; the kernel copies
        // the Timespec value during submission.
        unsafe { self.push_sqe(sqe)? };

        self.ring.submit_and_wait(1)?;
        self.drain_completions();
        Ok(())
    }

    /// Drain the completion queue, dispatching readiness to [`ScheduledIo`]s
    /// and freeing slab slots as their terminal CQEs arrive.
    fn drain_completions(&mut self) {
        // Collect the eventfd fd up front so we can drain it without
        // borrowing `self` mutably while iterating the CQ.
        let external_fd = self.external_wake_fd.as_raw_fd();
        let mut saw_external_wake = false;

        // Stage slab removals after the CQ borrow drops; we cannot mutate
        // `self.ops` while the `cq` iterator borrows `self.ring`. Capacity
        // hint avoids re-allocs in the common per-park burst.
        let mut to_remove: Vec<u32> = Vec::with_capacity(16);
        // Stage readiness deliveries the same way — `ScheduledIo::wake` may
        // run arbitrary user code (waker callbacks), so we want it strictly
        // outside the CQ-iterator borrow.
        let mut readiness_deliveries: Vec<(Arc<ScheduledIo>, Ready)> = Vec::with_capacity(16);

        let cq = self.ring.completion();
        for cqe in cq {
            let (variant, gen, key) = decode(cqe.user_data());

            match variant {
                VARIANT_POLL_MULTI => {
                    // Look up the slot; reject stale CQEs whose gen no
                    // longer matches the slot's recorded gen (slot was
                    // recycled between submission and completion).
                    let entry = match self.ops.get(key as usize) {
                        Some(e) if e.gen == gen => e,
                        _ => continue, // stale CQE; drop.
                    };
                    let (io_arc, removing) = match &entry.state {
                        OpState::PollMulti { io, removing } => (io.clone(), *removing),
                        _ => continue, // defense-in-depth; gen check above should make this unreachable.
                    };

                    let result = cqe.result();
                    let flags = cqe.flags();
                    let has_more = cqueue::more(flags);

                    // Skip delivery if the slot has been scheduled for
                    // local teardown by `deregister()` — the caller has
                    // already dropped interest, and waking a stale task
                    // handle would be a spurious poll at best and a
                    // use-after-free scheduler hazard at worst. The
                    // terminal `-ECANCELED` CQE still frees the slot
                    // below via `!has_more`.
                    if result >= 0 && !removing {
                        let ready = ready_from_poll_flags(result);
                        readiness_deliveries.push((io_arc, ready));
                    }
                    // result < 0 is typically -ECANCELED (our POLL_REMOVE
                    // landed) or kernel-side autonomous cleanup. Either
                    // way, no readiness to dispatch.

                    if !has_more {
                        // Terminal CQE for this slot. The Arc is dropped
                        // when we remove it after the loop.
                        to_remove.push(key);
                    }
                }

                VARIANT_CONTROL => {
                    // One-shot: free the slot. Stale CQEs (gen mismatch)
                    // also clean up — we'd never reuse a Control slot for
                    // anything else, so removal is safe either way.
                    if let Some(entry) = self.ops.get(key as usize) {
                        if entry.gen == gen {
                            to_remove.push(key);
                        }
                    }
                }

                VARIANT_EVENTFD => {
                    // Multi-shot, reactor-lifetime slot. Slot stays.
                    saw_external_wake = true;
                }

                VARIANT_MSG_RING_INCOMING => {
                    // Cross-worker wake. No payload to dispatch — the
                    // scheduler's task-queue checks happen around park().
                    // Slot stays.
                }

                _ => {
                    // Unknown variant — ignore. Could happen if a future
                    // op type is introduced and an old binary sees its
                    // CQEs (won't happen in practice; reactor and CQE
                    // producers are versioned together).
                }
            }
        }
        // Iterator drop syncs the CQ head pointer back to the kernel.

        for key in to_remove {
            // Slab::try_remove tolerates already-vacant slots (which can
            // happen if a Control completion fires twice — defensive).
            let _ = self.ops.try_remove(key as usize);
        }

        for (io, ready) in readiness_deliveries {
            io.set_readiness(Tick::Set, |curr| curr | ready);
            io.wake(ready);
        }

        if saw_external_wake {
            drain_eventfd(external_fd);
        }
    }

    // ===== private helpers =====

    /// Allocate a one-shot Control slot and return its encoded `user_data`
    /// plus the slab key (caller may discard the key — it'll be freed when
    /// the CQE arrives).
    fn alloc_control_slot(&mut self) -> (u64, u32) {
        let gen = self.next_gen();
        let key = self.ops.insert(SlotEntry { gen, state: OpState::Control });
        let key_u32 = u32::try_from(key).expect("slab key exceeds u32");
        (encode(VARIANT_CONTROL, gen, key_u32), key_u32)
    }

    /// Bump and return the next generation. Wraps modulo 2^24 (the encoded
    /// width); collision with an outstanding CQE on the same slot would
    /// require ~16M intervening inserts and is not physically realizable.
    fn next_gen(&mut self) -> u32 {
        let g = self.next_gen;
        // Wrap into the 24-bit encoded range.
        self.next_gen = self.next_gen.wrapping_add(1) & 0x00FF_FFFF;
        // Reserve 0 for well-known slots; skip it on wrap.
        if self.next_gen == 0 {
            self.next_gen = 1;
        }
        g
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
        f.debug_struct("Reactor")
            .field("in_flight_ops", &self.ops.len())
            .finish_non_exhaustive()
    }
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
/// (the eventfd is only ever read-ready), tagged with [`EVENTFD_UD`].
/// Submits synchronously so the registration is live before `new()` returns.
fn register_eventfd_multishot(ring: &mut IoUring, fd: RawFd) -> io::Result<()> {
    let sqe = opcode::PollAdd::new(types::Fd(fd), libc::POLLIN as u32)
        .multi(true)
        .build()
        .user_data(EVENTFD_UD);

    // SAFETY: fd outlives the registration (stored on the Reactor as an
    // OwnedFd); the multi-shot registration is torn down by the kernel
    // when the ring is closed at reactor drop.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// `encode`/`decode` round-trips correctly across all variant tags and
    /// boundary values for `gen` and `key`.
    #[test]
    fn slot_encoding_roundtrip() {
        let cases = [
            (VARIANT_POLL_MULTI, 0u32, 0u32),
            (VARIANT_POLL_MULTI, 0x00FF_FFFF, u32::MAX),
            (VARIANT_CONTROL, 0x12_3456, 0xDEAD_BEEF),
            (VARIANT_EVENTFD, 0, KEY_EVENTFD),
            (VARIANT_MSG_RING_INCOMING, 0, KEY_MSG_RING_INCOMING),
        ];
        for (v, g, k) in cases {
            let ud = encode(v, g, k);
            let (v2, g2, k2) = decode(ud);
            assert_eq!((v, g, k), (v2, g2, k2), "round-trip failed for {ud:#x}");
        }
    }

    /// The well-known constants are derived from the well-known keys at
    /// compile time; verify the layout against the variant tags.
    #[test]
    fn well_known_constants_decode_correctly() {
        let (v, g, k) = decode(MSG_RING_INCOMING_UD);
        assert_eq!((v, g, k), (VARIANT_MSG_RING_INCOMING, 0, KEY_MSG_RING_INCOMING));
        let (v, g, k) = decode(EVENTFD_UD);
        assert_eq!((v, g, k), (VARIANT_EVENTFD, 0, KEY_EVENTFD));
    }

    /// Ring construction succeeds on a supported kernel. Smoke test for the
    /// setup flags — if SINGLE_ISSUER/DEFER_TASKRUN aren't available we want
    /// the failure surfaced here, not deep inside park().
    #[test]
    fn reactor_new_succeeds() {
        let reactor = Reactor::new();
        match reactor {
            Ok(r) => {
                // Two well-known slots pre-allocated.
                assert_eq!(r.ops.len(), 2);
            }
            Err(e) if e.raw_os_error() == Some(libc::ENOSYS) => {
                eprintln!("skipping: io_uring not supported on this kernel");
            }
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
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
    /// its park. Exercises the cross-worker wake path with the universal
    /// `MSG_RING_INCOMING_UD` constant.
    #[test]
    fn msg_ring_wakes_peer() {
        use std::sync::mpsc;

        let (fd_tx, fd_rx) = mpsc::channel::<RawFd>();
        let (elapsed_tx, elapsed_rx) = mpsc::channel::<Duration>();

        let receiver_thread = std::thread::spawn(move || {
            let Ok(mut receiver) = Reactor::new() else {
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
            return;
        };

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

    /// After register, the slab grows by one and the slab key is published
    /// onto the `ScheduledIo`. After deregister + park-until-terminal-CQE,
    /// the slab returns to its baseline size and the registration's `Arc`
    /// strong count drops back to the caller-only count.
    #[test]
    fn deregister_releases_arc_on_terminal_cqe() {
        use std::os::fd::{BorrowedFd, FromRawFd};

        let Ok(mut reactor) = Reactor::new() else {
            eprintln!("skipping: reactor unavailable");
            return;
        };

        // Build a pipe to register against — a real fd avoids any
        // -EBADF surprises and the read end is a legitimate POLLIN target.
        let mut fds = [0 as libc::c_int; 2];
        // SAFETY: standard pipe(2) call.
        let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
        assert_eq!(rc, 0, "pipe2 failed: {}", io::Error::last_os_error());
        // SAFETY: we own both ends of the pipe.
        let read_end = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let _write_end = unsafe { OwnedFd::from_raw_fd(fds[1]) };

        let baseline = reactor.ops.len();

        let io = Arc::new(ScheduledIo::default());
        // SAFETY: read_end is alive and owned for the duration of this scope.
        let borrowed = unsafe { BorrowedFd::borrow_raw(read_end.as_raw_fd()) };
        reactor
            .register(borrowed.as_raw_fd(), Interest::READABLE, &io)
            .expect("register");
        assert_eq!(reactor.ops.len(), baseline + 1, "register should add a slot");
        assert_ne!(
            io.uring_slab_key.load(Ordering::Relaxed),
            u32::MAX,
            "slab key should be published on ScheduledIo",
        );

        // Strong count: caller's `io` + the slab's clone = 2.
        assert_eq!(Arc::strong_count(&io), 2);

        // Snapshot (key, gen) before calling deregister — v2 signature
        // takes the snapshot rather than holding the Arc, so that stale
        // requests (after slab recycle) are detected by gen-check.
        let slab_key = io.uring_slab_key.load(Ordering::Relaxed);
        let slab_gen = io.uring_gen.load(Ordering::Relaxed);
        // Submit POLL_REMOVE; the terminal CQE will arrive on the next
        // park (the kernel posts -ECANCELED with F_MORE clear).
        reactor.deregister(slab_key, slab_gen).expect("deregister");

        // Drain until the PollMulti slot is gone. The Control ack and the
        // PollMulti terminal CQE may arrive on different submits depending
        // on kernel scheduling; loop with a bounded timeout to tolerate
        // either ordering.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while Arc::strong_count(&io) > 1 && std::time::Instant::now() < deadline {
            reactor
                .park_timeout(Duration::from_millis(100))
                .expect("park drain");
        }

        assert_eq!(
            Arc::strong_count(&io),
            1,
            "PollMulti slot should have been freed by the terminal CQE",
        );
    }

}
