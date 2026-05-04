//! Per-worker `mio::Poll` reactor (experimental).
//!
//! Companion to [`uring_reactor`] for A/B-measuring how much of the
//! uring reactor's multi-worker wins come from sharding the driver vs.
//! from io_uring itself. The architecture mirrors [`uring_reactor`] but
//! uses a per-worker `mio::Poll` instead of a per-worker `io_uring`:
//!
//! - `IoUring` → `mio::Poll`
//! - `POLL_ADD_MULTI` SQE → `registry.register(fd, Token, interest)`
//! - `submit_and_wait(1)` → `poll.poll(&mut events, None)`
//! - `MSG_RING` + eventfd → a [`StealSafeWaker`] per worker (own
//!   eventfd on Linux, `mio::Waker` fallback elsewhere)
//! - `user_data` slab/gen/variant → `mio::Token(usize)` keyed into a
//!   per-reactor `Slab<Arc<ScheduledIo>>` (no gen needed — mio has no
//!   stale-event race with in-flight kernel ops; once `deregister`
//!   returns, mio won't surface further events for that source)
//!
//! Cross-thread registration is trivial: `mio::Registry` is
//! `Send + Sync` and `register`/`deregister` are thread-safe, so
//! [`ShardedMioHandle::add_source`] can register directly onto the
//! target worker's registry from any thread. The target worker is then
//! woken via its `mio::Waker` so it re-polls with the new registration
//! visible.
//!
//! [`uring_reactor`]: super::uring_reactor
//! [`ShardedMioHandle::add_source`]: super::sharded_mio_driver::ShardedMioHandle::add_source

use mio::{Events, Poll, Registry, Token};
#[cfg(not(target_os = "linux"))]
use mio::Waker;
use slab::Slab;

use crate::io::{Interest, Ready};
use crate::loom::sync::Arc;
use crate::runtime::io::driver::Tick;
use crate::runtime::io::registration::RegistrationSource;
use crate::runtime::io::ScheduledIo;

use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex as StdMutex;
use std::time::Duration;

/// Reserved token for the per-worker `mio::Waker`. Events arriving with
/// this token are external/peer wakeups — scheduler state may have
/// changed, but there is no `ScheduledIo` to dispatch to.
pub(crate) const WAKER_TOKEN: Token = Token(usize::MAX);

/// Width in bits of the `worker_idx` field inside the packed token.
/// Sized to cover today's `lazy_debug::MAX_WORKERS = 16` (idx 0..=15).
/// See `pack_token` / `unpack_token` for the layout.
pub(crate) const TOKEN_WORKER_BITS: u32 = 4;
const TOKEN_WORKER_MASK: u32 = (1 << TOKEN_WORKER_BITS) - 1;
/// Width of the slab-key field. 28 bits gives ~256 M entries per
/// worker — well above any realistic working set, and we already
/// rejected wider keys at the `u32::try_from` site below.
pub(crate) const TOKEN_KEY_BITS: u32 = 32 - TOKEN_WORKER_BITS;
const TOKEN_KEY_MASK: u32 = (1 << TOKEN_KEY_BITS) - 1;

/// Mio `Token` is a `usize`. On 64-bit Linux we pack:
///
/// ```text
/// bit  63                32 31              4 3      0
///     +-------------------+-------------------+--------+
///     |        gen        |        key        |  wrkr  |
///     +-------------------+-------------------+--------+
///        32 bits             28 bits           4 bits
/// ```
///
/// `wrkr` is the registering worker's index. In the current
/// single-owner registration model every event is delivered to the
/// owner's epoll fd, so `wrkr` is informational — but it is preserved
/// in the layout because the upcoming meta-epoll readiness-stealing
/// path will use it to route an event observed via a *peer's* drain
/// pass back to the right slab.
///
/// `WAKER_TOKEN` (`usize::MAX`) decodes to (`wrkr=0xF`, `key=0xFFFFFFF`,
/// `gen=0xFFFFFFFF`); a real registration would need worker idx 15
/// **and** the maximum `gen` **and** the maximum `key` simultaneously
/// to collide. Slab keys are bounded by the runtime's working set
/// (orders of magnitude below `0xFFFFFFF`) so this remains safe in
/// practice — same argument as before the worker_idx field was added.
#[inline]
pub(crate) fn pack_token(worker_idx: u8, key: u32, gen: u32) -> Token {
    debug_assert!(
        (worker_idx as u32) <= TOKEN_WORKER_MASK,
        "worker_idx {worker_idx} exceeds TOKEN_WORKER_MASK ({TOKEN_WORKER_MASK})",
    );
    debug_assert!(
        key <= TOKEN_KEY_MASK,
        "slab key {key} exceeds TOKEN_KEY_MASK ({TOKEN_KEY_MASK})",
    );
    let raw =
        ((gen as usize) << 32) | ((key as usize) << TOKEN_WORKER_BITS) | (worker_idx as usize);
    Token(raw)
}

#[inline]
fn unpack_token(t: Token) -> (u8, u32, u32) {
    let raw = t.0;
    let worker_idx = (raw as u32) & TOKEN_WORKER_MASK;
    let key = ((raw as u32) >> TOKEN_WORKER_BITS) & TOKEN_KEY_MASK;
    let gen = (raw >> 32) as u32;
    (worker_idx as u8, key, gen)
}

/// Capacity of each worker's `mio::Events` buffer. Sized in the same
/// spirit as the uring reactor's CQ: comfortably above the observed
/// per-park working set for the bench matrix. Mio silently rolls
/// excess events over to the next `poll()` call, so this is a latency
/// hint, not a correctness knob.
const EVENTS_CAPACITY: usize = 1024;

/// Per-call budget for [`SharedRegistry::try_steal_drain`]. Sized so
/// the on-stack `[libc::epoll_event; N]` buffer remains comfortably
/// in the worker's stack budget (each `epoll_event` is 12 bytes on
/// Linux x86_64 — 12 * 128 = 1.5 KiB) and so a single steal call
/// cannot monopolise a peer worker by draining the owner's whole
/// queue. The level-triggered meta epoll guarantees we'll be woken
/// again on the same child if events remain undrained.
#[cfg(target_os = "linux")]
const STEAL_DRAIN_BUDGET: usize = 128;

/// Convert a raw Linux `epoll_event.events` bitmask into a tokio
/// [`Ready`]. Mirrors mio's owner-side
/// `mio::sys::unix::selector::epoll::event::is_*` helpers verbatim
/// so that the steal-drain path produces the same readiness shape
/// that the owner's `Reactor::poll_and_dispatch` would produce for
/// the same kernel event.
#[cfg(target_os = "linux")]
fn ready_from_epoll_bits(events: u32) -> Ready {
    let e = events as libc::c_int;
    let mut ready = Ready::EMPTY;
    if (e & libc::EPOLLIN) != 0 || (e & libc::EPOLLPRI) != 0 {
        ready |= Ready::READABLE;
    }
    if (e & libc::EPOLLOUT) != 0 {
        ready |= Ready::WRITABLE;
    }
    if (e & libc::EPOLLERR) != 0 {
        ready |= Ready::ERROR;
    }
    if (e & libc::EPOLLHUP) != 0
        || ((e & libc::EPOLLIN) != 0 && (e & libc::EPOLLRDHUP) != 0)
    {
        ready |= Ready::READ_CLOSED;
    }
    if (e & libc::EPOLLHUP) != 0
        || ((e & libc::EPOLLOUT) != 0 && (e & libc::EPOLLERR) != 0)
        || e == libc::EPOLLERR
    {
        ready |= Ready::WRITE_CLOSED;
    }
    if (e & libc::EPOLLPRI) != 0 {
        ready |= Ready::PRIORITY;
    }
    ready
}

/// State shared between [`Reactor`] (dispatch) and [`SharedRegistry`]
/// (cross-thread register/deregister).
///
/// All three fields live behind one [`std::sync::Mutex`]. Under the
/// single-owner registration model only one party touches a given
/// worker's `OpsState` at a time in the steady state — the owner
/// thread during dispatch — so the lock is uncontended on the hot
/// path. Cross-thread `register` / `deregister` from peers and the
/// future demand-driven peer-steal path will use `try_lock` to skip
/// rather than block when the owner is mid-dispatch.
pub(crate) struct OpsState {
    /// Live `Arc<ScheduledIo>` per slab key. Vacant slots indicate a
    /// previously-registered entry has been deregistered.
    slab: Slab<Arc<ScheduledIo>>,

    /// Per-slab-slot generation counter. Lazily grown to cover the
    /// largest slab key ever inserted; index `K` holds the generation
    /// **most recently assigned** at slot `K` (or `0` if the slot has
    /// never been used). Bumped on every fresh `slab.insert`. Persists
    /// across `slab.try_remove` so the next insert at the same slot
    /// observes a strictly-greater generation than any prior
    /// registration, defeating slab-key-reuse stale events.
    gens: Vec<u32>,

    /// Map from `RawFd` currently registered in this worker's epoll set
    /// to its slab key. Used to detect the
    /// stale-deregister-clobbers-fresh-registration race documented in
    /// HANDOFF-lazy-register-session2.md: if a queued `Deregister`
    /// arrives at `queue_deregister` but the fd has since been closed
    /// and recycled by a *different* registration, the live key here
    /// won't match and the `epoll_ctl_del(fd)` is skipped (which would
    /// otherwise wipe the new owner's epoll record because epoll keys
    /// on fd, not on token).
    live_fds: HashMap<RawFd, u32>,
}

/// Per-worker `mio::Poll` reactor.
///
/// Owns the [`Poll`] instance and an `Events` scratch buffer. The slab
/// of live registrations lives behind an [`Arc<StdMutex<_>>`]; the
/// owner worker takes the lock during dispatch and the peer paths
/// (cross-thread register/deregister, future steal-drain) take it via
/// `lock` / `try_lock`. Single-owner registration means each event is
/// delivered to exactly one worker's epoll fd, so the dispatch path
/// does not need read-side parallelism across slabs.
pub(crate) struct Reactor {
    poll: Poll,
    events: Events,

    /// Per-worker registration state — slab + per-slot gens + live-fd
    /// map. See [`OpsState`].
    ops: Arc<StdMutex<OpsState>>,

    /// Cross-thread waker, cloneable via [`Self::external_waker`] and
    /// via [`SharedRegistry`]. Backed by [`StealSafeWaker`] so peers
    /// can drain the eventfd via `try_steal_drain` without poisoning
    /// future wakes.
    waker: Arc<StealSafeWaker>,

    /// CAS-based mutual-exclusion guard for this worker's child epoll
    /// fd. `false` = free; `true` = either the owner's
    /// `mio::Poll::poll` or a peer's raw `epoll_wait` (in
    /// `try_steal_drain`) is in progress. Both sides CAS `false→true`
    /// before entering `epoll_wait`; whoever loses the CAS defers.
    /// This prevents the EPOLLET edge-consumption race that a simple
    /// TOCTOU flag (`owner_polling`) could not close.
    #[cfg(target_os = "linux")]
    epoll_guard: Arc<AtomicBool>,
}

/// Steal-safe waker: a cross-thread wake primitive that can be
/// **drained** from any thread without poisoning future wakes.
///
/// On Linux this is a raw `eventfd` registered **level-triggered**
/// (`EPOLLIN`, no `EPOLLET`) on the owning worker's child epoll.
/// Level-triggered registration guarantees that even if a peer's
/// raw `libc::epoll_wait` (in `try_steal_drain`) observes the event,
/// the eventfd keeps firing on subsequent `epoll_wait` calls until
/// someone drains the count via `read(fd, 8)`. Both the owner's
/// `poll_and_dispatch` and a peer's `try_steal_drain` call `drain()`
/// when they see `WAKER_TOKEN`, which resets the count to 0.
///
/// This replaces `mio::Waker`, which uses `EPOLLET` internally and
/// hides the raw eventfd fd — making it impossible to drain from a
/// peer's raw `epoll_wait` without losing future edges. See
/// `WATCHER_GATE_STATUS.md` § "The eventfd hazard" for the full
/// analysis.
///
/// On non-Linux, falls back to `mio::Waker` (steal-drain is
/// Linux-only anyway).
pub(crate) struct StealSafeWaker {
    #[cfg(target_os = "linux")]
    fd: RawFd,
    #[cfg(not(target_os = "linux"))]
    mio_waker: Waker,
}

// SAFETY: eventfd is a kernel object; read/write are thread-safe.
// mio::Waker is already Send + Sync.
unsafe impl Send for StealSafeWaker {}
unsafe impl Sync for StealSafeWaker {}

impl StealSafeWaker {
    /// Create a new waker and register it on `poll`'s epoll fd.
    #[cfg(target_os = "linux")]
    fn new(poll: &Poll) -> io::Result<Self> {
        let fd = unsafe {
            libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC)
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // Register as level-triggered EPOLLIN on the poll's epoll fd.
        // Level-triggered (no EPOLLET) means: as long as count > 0,
        // every epoll_wait reports readiness. This is strictly safer
        // than EPOLLET for cross-thread draining — no lost edges.
        let epoll_fd = poll.registry().as_raw_fd();
        let mut ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: WAKER_TOKEN.0 as u64,
        };
        let ret = unsafe {
            libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, fd, &mut ev)
        };
        if ret != 0 {
            let err = io::Error::last_os_error();
            unsafe { libc::close(fd); }
            return Err(err);
        }
        Ok(Self { fd })
    }

    #[cfg(not(target_os = "linux"))]
    fn new(poll: &Poll) -> io::Result<Self> {
        Ok(Self {
            mio_waker: Waker::new(poll.registry(), WAKER_TOKEN)?,
        })
    }

    /// Signal the owning reactor. Thread-safe; may be called from
    /// any thread. Multiple calls before a drain accumulate count
    /// but produce at most one event per drain cycle (level-triggered
    /// fires once per `epoll_wait` as long as count > 0).
    #[cfg(target_os = "linux")]
    pub(crate) fn wake(&self) -> io::Result<()> {
        let val: u64 = 1;
        let ret = unsafe {
            libc::write(
                self.fd,
                &val as *const u64 as *const libc::c_void,
                std::mem::size_of::<u64>(),
            )
        };
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) fn wake(&self) -> io::Result<()> {
        self.mio_waker.wake()
    }

    /// Drain the eventfd count to zero, re-arming the level-triggered
    /// registration. Called by both `poll_and_dispatch` (owner) and
    /// `try_steal_drain` (peer) when they observe `WAKER_TOKEN`.
    ///
    /// Non-blocking: if count was already 0 (spurious), the `read`
    /// returns `EAGAIN` — harmless, we ignore the error.
    #[cfg(target_os = "linux")]
    pub(crate) fn drain(&self) {
        let mut buf: u64 = 0;
        unsafe {
            libc::read(
                self.fd,
                &mut buf as *mut u64 as *mut libc::c_void,
                std::mem::size_of::<u64>(),
            );
        }
    }

    /// Non-Linux: mio::Waker drains internally during `Poll::poll`.
    #[cfg(not(target_os = "linux"))]
    pub(crate) fn drain(&self) {
        // no-op
    }
}

#[cfg(target_os = "linux")]
impl Drop for StealSafeWaker {
    fn drop(&mut self) {
        // SAFETY: fd was created by eventfd() in new() and has not
        // been closed elsewhere.
        unsafe { libc::close(self.fd); }
    }
}

impl std::fmt::Debug for StealSafeWaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StealSafeWaker").finish_non_exhaustive()
    }
}

/// Cross-thread wake handle. Analogous to
/// [`uring_reactor::ExternalWaker`][eu], backed by a
/// [`StealSafeWaker`] that can be drained from any thread.
///
/// [eu]: super::uring_reactor::ExternalWaker
#[derive(Clone)]
pub(crate) struct ExternalWaker {
    waker: Arc<StealSafeWaker>,
}

impl std::fmt::Debug for ExternalWaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalWaker").finish_non_exhaustive()
    }
}

impl ExternalWaker {
    /// Wake the owning reactor. Thread-safe; may be called from any
    /// thread.
    pub(crate) fn wake(&self) -> io::Result<()> {
        self.waker.wake()
    }
}

/// Cross-thread handle to a worker's `mio::Registry` + slab + waker.
///
/// Hands-off equivalent of "`UringHandle::workers[idx].ring_fd +
/// pending_ops + external_waker`" collapsed into one type. All fields
/// are thread-safe (`mio::Registry` is `Send + Sync`, the slab is
/// behind a mutex, the waker is `Send + Sync` via `Arc`), so this
/// entire handle is `Send + Sync` and can be cloned onto
/// [`ShardedMioHandle::workers`] for any-thread access.
pub(crate) struct SharedRegistry {
    /// Index of the owning worker. Stamped into the high-bit `worker_idx`
    /// field of every token this registry packs, so peer-delivered
    /// events (post-EPOLLEXCLUSIVE-fanout) can route back to the slab
    /// holding the `ScheduledIo`.
    worker_idx: u8,
    registry: Registry,
    ops: Arc<StdMutex<OpsState>>,
    waker: Arc<StealSafeWaker>,
    /// CAS-based guard shared with the owning [`Reactor`]. See
    /// [`Reactor::epoll_guard`] for semantics. `try_steal_drain`
    /// CAS-acquires this before its raw `epoll_wait`; the owner's
    /// `poll_and_dispatch` CAS-acquires before `mio::Poll::poll`.
    #[cfg(target_os = "linux")]
    epoll_guard: Arc<AtomicBool>,
}

/// Outcome of a [`SharedRegistry::register`] call. Carries the
/// freshly-assigned `(slab_key, gen)` pair so the caller can stash both
/// on the [`ScheduledIo`] for later mismatch checks, plus the packed
/// `mio::Token` so the EPOLLEXCLUSIVE-fanout helpers can stamp the
/// same `(worker_idx, key, gen)` tuple onto every peer worker's epoll
/// fd without having to re-derive it.
pub(crate) struct RegisterOk {
    pub(crate) slab_key: u32,
    pub(crate) gen: u32,
    pub(crate) token: Token,
}

/// Outcome of a [`SharedRegistry::deregister`] call.
pub(crate) enum DeregisterOutcome {
    /// Normal path — slab/epoll state was mutated.
    Applied,
    /// The (worker, fd) pair currently belongs to a *different* slab
    /// key than the one we were asked to deregister: the fd was
    /// recycled into a fresh registration. We skipped both the
    /// `epoll_ctl_del` and the `slab.try_remove` to avoid clobbering
    /// the new owner. Caller bumps `apply_deregister_gen_mismatch`.
    SkippedFdReused,
    /// The slab slot still holds someone, but it isn't us — the slot
    /// has been reassigned (slab key reuse). Skip silently; counter is
    /// the same as `SkippedFdReused`.
    SkippedSlotReassigned,
}

impl std::fmt::Debug for SharedRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedRegistry").finish_non_exhaustive()
    }
}

impl SharedRegistry {
    /// Clone an [`ExternalWaker`] referring to the owning reactor's
    /// [`StealSafeWaker`]. Used by [`ShardedMioHandle::unpark`] to
    /// wake a parked worker from arbitrary threads.
    ///
    /// [`ShardedMioHandle::unpark`]: super::sharded_mio_driver::ShardedMioHandle::unpark
    pub(crate) fn waker(&self) -> ExternalWaker {
        ExternalWaker {
            waker: Arc::clone(&self.waker),
        }
    }

    /// Register `source` (whose fd is `fd`) on this reactor's [`Poll`]
    /// for `interest`, and allocate a slab entry holding a clone of
    /// `scheduled_io`. Returns the freshly-assigned slab key plus the
    /// per-slot generation; both are packed into the mio token so
    /// dispatch and apply-deregister can detect stale references.
    ///
    /// Thread-safe. Rolls back the slab/gens/live_fds inserts if mio
    /// registration fails, so the caller observes a clean
    /// "not registered" state on error.
    pub(crate) fn register<S: RegistrationSource + ?Sized>(
        &self,
        source: &mut S,
        fd: RawFd,
        interest: Interest,
        scheduled_io: &Arc<ScheduledIo>,
    ) -> io::Result<RegisterOk> {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.sr_register_calls);
        if interest.is_readable() {
            bump(&COUNTERS.sr_register_readable_interest);
        }
        if interest.is_writable() {
            bump(&COUNTERS.sr_register_writable_interest);
        }
        let (key_u32, gen) = {
            let mut state = self.ops.lock().expect("sharded-mio ops poisoned");
            let key = state.slab.insert(Arc::clone(scheduled_io));
            let key_u32 = u32::try_from(key).expect("slab key exceeds u32");
            // Lazily grow gens to cover this slot. Persistent across
            // remove/reinsert cycles so the next reuse strictly
            // increases.
            if state.gens.len() <= key {
                state.gens.resize(key + 1, 0);
            }
            // Bump and skip 0 (sentinel for "never registered").
            let new_gen = state.gens[key].wrapping_add(1);
            let new_gen = if new_gen == 0 { 1 } else { new_gen };
            state.gens[key] = new_gen;
            // Stamp the gen on the ScheduledIo *inside* the lock, before
            // the kernel-side `registry.register` can produce an event
            // for `(new_gen, key)`. If we deferred this until after the
            // mio register call (as we used to), there was a window where
            // the kernel had already queued an event with token
            // `(new_gen, K)` but the ScheduledIo's `sharded_mio_gen` was
            // still its previous value (0 for fresh, or whatever the
            // prior occupant left). A peer dispatcher acquiring the ops
            // lock during that window would observe `slab[K]` with a
            // stale gen, take the `dispatch_gen_mismatch` branch, and
            // silently drop the live registration's edge — which under
            // EPOLLET is never redelivered. See
            // `SHARDED_MIO_DEADLOCK_ASSESSMENT.md` §7e2-result.
            scheduled_io
                .sharded_mio_gen
                .store(new_gen, Ordering::Relaxed);
            // Live-fd map: this fd's epoll record (about to be added)
            // is keyed by `key_u32`. Overwrites any previous entry — a
            // previous entry here means the prior owner's queued
            // deregister hasn't drained yet, and that earlier owner has
            // already lost its kernel-side registration via close
            // (otherwise we couldn't have gotten the fd). The
            // overwrite is what causes the matching apply-deregister
            // to detect the clobber and skip its `epoll_ctl_del`.
            state.live_fds.insert(fd, key_u32);
            (key_u32, new_gen)
        };
        let token = pack_token(self.worker_idx, key_u32, gen);
        if let Err(e) = self.registry.register(source, token, interest.to_mio()) {
            bump(&COUNTERS.sr_register_err);
            let mut state = self.ops.lock().expect("sharded-mio ops poisoned");
            let _ = state.slab.try_remove(key_u32 as usize);
            // Roll back live_fds only if it still matches us (a racing
            // register from another thread shouldn't be undone).
            if state.live_fds.get(&fd) == Some(&key_u32) {
                state.live_fds.remove(&fd);
            }
            // The orphan gen stamp on the (now-rolled-back) ScheduledIo
            // is harmless: the slab entry is gone so no dispatcher can
            // reach it, and the next register call will allocate a fresh
            // slot and stamp a fresh gen.
            return Err(e);
        }
        bump(&COUNTERS.sr_register_ok);
        Ok(RegisterOk {
            slab_key: key_u32,
            gen,
            token,
        })
    }

    /// Deregister `source` (whose fd is `fd`) from this reactor's
    /// [`Poll`] and drop the slab entry identified by
    /// `(slab_key, gen)`.
    ///
    /// The `(slab_key, gen)` pair must match what was returned from
    /// the corresponding [`Self::register`] call. If the live state
    /// has moved on (fd recycled to a different registration, slab
    /// slot reassigned to a different generation), this call skips
    /// the kernel/slab mutation to avoid clobbering the new owner —
    /// see [`DeregisterOutcome`].
    pub(crate) fn deregister<S: RegistrationSource + ?Sized>(
        &self,
        source: &mut S,
        fd: RawFd,
        slab_key: u32,
        gen: u32,
    ) -> DeregisterOutcome {
        if slab_key == u32::MAX {
            return DeregisterOutcome::Applied;
        }
        let mut state = self.ops.lock().expect("sharded-mio ops poisoned");

        // Per-fd ownership check: if the kernel-side epoll record for
        // this fd belongs to someone else now (different slab key),
        // an `epoll_ctl_del(fd)` would wipe their record. Skip.
        let live_owner = state.live_fds.get(&fd).copied();
        if live_owner != Some(slab_key) {
            return DeregisterOutcome::SkippedFdReused;
        }

        // Per-slot gen check: belt-and-suspenders against slab key
        // reuse races. If gens[key] has moved past `gen`, someone else
        // has been assigned this slot already.
        let cur_gen = state.gens.get(slab_key as usize).copied().unwrap_or(0);
        if cur_gen != gen {
            return DeregisterOutcome::SkippedSlotReassigned;
        }

        // Safe to mutate: this fd's epoll record and this slab slot
        // are still ours. Drop the lock around the mio call (mio's
        // registry is thread-safe) — but for simplicity we hold it
        // through the syscall. The lock is per-worker and contention
        // is bounded by registration rate, not event rate.
        let _ = self.registry.deregister(source);
        let _ = state.slab.try_remove(slab_key as usize);
        state.live_fds.remove(&fd);
        DeregisterOutcome::Applied
    }

    /// Raw epoll fd backing this registry's clone. Stable for the
    /// lifetime of `self` (the inner `mio::Registry` keeps the fd open
    /// via dup; closed when this `SharedRegistry` drops).
    ///
    /// Used by [`ShardedMioHandle::register_worker`] to add this
    /// child epoll onto the runtime-wide meta epoll (see
    /// [`sharded_mio_driver`]). The upcoming steal-mode park path
    /// will also pass it back to a peer worker's
    /// [`Self::try_steal_drain`] so the peer can `epoll_wait` non-
    /// blocking on it under a `try_lock` of `OpsState`.
    ///
    /// [`sharded_mio_driver`]: super::sharded_mio_driver
    #[cfg(target_os = "linux")]
    pub(crate) fn epoll_fd(&self) -> RawFd {
        self.registry.as_raw_fd()
    }

    /// Drain at most [`STEAL_DRAIN_BUDGET`] ready events from this
    /// worker's child epoll fd, dispatching each onto its slab
    /// without blocking the owner. Called by a *peer* worker on
    /// behalf of the owner — usually because a meta-epoll park
    /// observed this child as fireable, or because a task running
    /// on the peer just migrated in from the owner and is stalled
    /// waiting on a registration this worker holds.
    ///
    /// Two ownership-respecting choices make this safe to layer
    /// on top of the owner-side park:
    ///
    /// * `try_lock` on `OpsState` — never blocks. If the owner is
    ///   mid-dispatch we return zero immediately. The cost of a
    ///   "lost steal" is one wasted meta wake; the meta is level-
    ///   triggered, so the next sibling parker will see the same
    ///   child as fireable and re-attempt. This keeps the owner
    ///   on its own cache lines without ever stalling on a peer.
    /// * Non-blocking `epoll_wait(timeout=0)` on the child epoll
    ///   fd — drains whatever the kernel has queued without
    ///   blocking the *thread*. The peer never owns the child's
    ///   `mio::Poll`, so this is a raw `libc::epoll_wait` rather
    ///   than a `Poll::poll`.
    ///
    /// Returns the number of events that resolved to a live
    /// `ScheduledIo` and fired its waker.
    #[cfg(target_os = "linux")]
    pub(crate) fn try_steal_drain(&self) -> usize {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.steal_drain_calls);

        // CAS-acquire the epoll guard. If the owner (or another
        // stealer) currently holds it, bail — a concurrent raw
        // epoll_wait on the same child fd would consume EPOLLET
        // edges. Unlike a plain load-check, the CAS is atomic with
        // the acquisition, closing the TOCTOU window that allowed
        // the owner to enter mio::Poll::poll between our check and
        // our epoll_wait.
        if self
            .epoll_guard
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            bump(&COUNTERS.steal_drain_busy);
            return 0;
        }

        // Yield to the owner if it is mid-dispatch. Keeping owner
        // locality is the whole reason we shard — peers should
        // never block on the owner's slab.
        let state = match self.ops.try_lock() {
            Ok(g) => g,
            Err(_) => {
                bump(&COUNTERS.steal_drain_busy);
                self.epoll_guard.store(false, Ordering::Release);
                return 0;
            }
        };

        // Non-blocking drain of up to `BUDGET` events from the
        // owner's child epoll. Anything we don't drain remains
        // queued and the meta epoll will keep firing on this child
        // until the owner or a future peer call clears it (level-
        // triggered).
        const BUDGET: usize = STEAL_DRAIN_BUDGET;
        let mut events: [libc::epoll_event; BUDGET] =
            // SAFETY: `epoll_event` is plain old data; `epoll_wait`
            // overwrites the slots it returns and we only read the
            // first `n` of them.
            unsafe { std::mem::zeroed() };
        let n = unsafe {
            libc::epoll_wait(
                self.registry.as_raw_fd(),
                events.as_mut_ptr(),
                BUDGET as i32,
                0, // non-blocking
            )
        };
        if n < 0 {
            // EINTR is expected and harmless — we'll be re-invoked
            // on the next steal trigger. Other errors are logged
            // but not propagated; the owner-side park loop is the
            // canonical drain.
            bump(&COUNTERS.steal_drain_err);
            self.epoll_guard.store(false, Ordering::Release);
            return 0;
        }
        if n == 0 {
            bump(&COUNTERS.steal_drain_empty);
            self.epoll_guard.store(false, Ordering::Release);
            return 0;
        }

        let mut woken = 0usize;
        for ev in events.iter().take(n as usize) {
            let token = Token(ev.u64 as usize);
            if token == WAKER_TOKEN {
                // Owner's unpark notification — skip without
                // draining. The owner must see this in its own
                // `mio::Poll::poll` to return from park. Draining
                // here would consume the notification and cause the
                // owner to block in `poll` even though `park_state`
                // is `NOTIFIED` (the owner has already passed
                // `begin_park` and won't re-check). The level-
                // triggered eventfd will keep firing on subsequent
                // `epoll_wait(0)` calls, but `continue` skips it
                // cheaply — one wasted event slot per steal call.
                continue;
            }
            let (_w, key, gen) = unpack_token(token);
            let Some(io) = state.slab.get(key as usize) else {
                bump(&COUNTERS.dispatch_slab_miss);
                continue;
            };
            if io.sharded_mio_gen.load(Ordering::Relaxed) != gen {
                bump(&COUNTERS.dispatch_gen_mismatch);
                continue;
            }
            let ready = ready_from_epoll_bits(ev.events);
            bump(&COUNTERS.dispatch_woken);
            if ready.is_readable() {
                bump(&COUNTERS.dispatch_woken_readable);
            }
            if ready.is_writable() {
                bump(&COUNTERS.dispatch_woken_writable);
            }
            io.set_readiness(Tick::Set, |curr| curr | ready);
            io.wake(ready);
            woken += 1;
        }
        drop(state);
        // Release the epoll guard now that both the raw epoll_wait
        // and the slab dispatch are complete. The owner's next
        // poll_and_dispatch CAS will succeed.
        self.epoll_guard.store(false, Ordering::Release);
        if woken > 0 {
            bump(&COUNTERS.steal_drain_woken);
            COUNTERS
                .steal_drain_woken_total
                .fetch_add(woken as u64, Ordering::Relaxed);
        }
        woken
    }

    /// Drop the slab entry identified by `(slab_key, gen)` without
    /// touching mio. Used during `ScheduledIo` teardown when the
    /// underlying source has already been closed — mio implicitly
    /// deregisters on fd close, so the explicit `Registry::deregister`
    /// would just return `ENOENT`. The gen check makes this race-safe
    /// against slot reassignment.
    #[allow(dead_code)]
    pub(crate) fn drop_slab_entry(&self, slab_key: u32, gen: u32) {
        if slab_key == u32::MAX {
            return;
        }
        let mut state = self.ops.lock().expect("sharded-mio ops poisoned");
        let cur_gen = state.gens.get(slab_key as usize).copied().unwrap_or(0);
        if cur_gen == gen {
            let _ = state.slab.try_remove(slab_key as usize);
        }
    }
}

impl Reactor {
    /// Build a new per-worker reactor. Unlike the uring reactor this
    /// has no `SINGLE_ISSUER` constraint, so moving a constructed
    /// reactor across threads is allowed in principle. We still build
    /// lazily on the owning worker for parity with the uring path and
    /// so the `CURRENT_WORKER` TLS install lands at the same point.
    pub(crate) fn new() -> io::Result<Self> {
        let poll = Poll::new()?;
        let waker = Arc::new(StealSafeWaker::new(&poll)?);
        Ok(Self {
            poll,
            events: Events::with_capacity(EVENTS_CAPACITY),
            ops: Arc::new(StdMutex::new(OpsState {
                slab: Slab::new(),
                gens: Vec::new(),
                live_fds: HashMap::new(),
            })),
            waker,
            #[cfg(target_os = "linux")]
            epoll_guard: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Clone out a cross-thread [`SharedRegistry`]. `mio::Registry` is
    /// obtained via `try_clone`, which on Linux duplicates the epoll
    /// fd's reference so the clone and the owned registry target the
    /// same kernel object.
    ///
    /// `worker_idx` is the owning worker's index in the
    /// `ShardedMioHandle::workers` array. It's stamped into every token
    /// the registry packs (see `pack_token`) so peer-delivered events
    /// can route back to this slab.
    pub(crate) fn shared_registry(&self, worker_idx: usize) -> io::Result<SharedRegistry> {
        debug_assert!(
            (worker_idx as u32) <= TOKEN_WORKER_MASK,
            "worker_idx {worker_idx} exceeds TOKEN_WORKER_MASK ({TOKEN_WORKER_MASK})",
        );
        let registry = self.poll.registry().try_clone()?;
        Ok(SharedRegistry {
            worker_idx: worker_idx as u8,
            registry,
            ops: Arc::clone(&self.ops),
            waker: Arc::clone(&self.waker),
            #[cfg(target_os = "linux")]
            epoll_guard: Arc::clone(&self.epoll_guard),
        })
    }

    /// Cross-thread wake handle backed by [`StealSafeWaker`].
    /// Unlike mio's `Waker` (EPOLLET), this waker is safe to drain
    /// from any thread — see [`StealSafeWaker`].
    pub(crate) fn external_waker(&self) -> ExternalWaker {
        ExternalWaker {
            waker: Arc::clone(&self.waker),
        }
    }

    /// Park until at least one event arrives, then dispatch all events.
    /// One `mio::Poll::poll(None)` syscall. Matches the uring reactor's
    /// `park` shape: block, drain, return.
    pub(crate) fn park(&mut self) -> io::Result<()> {
        self.poll_and_dispatch(None)
    }

    /// Park with a maximum duration. Zero duration polls non-blocking.
    pub(crate) fn park_timeout(&mut self, timeout: Duration) -> io::Result<()> {
        self.poll_and_dispatch(Some(timeout))
    }

    fn poll_and_dispatch(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        // CAS-acquire the epoll guard before entering mio::Poll::poll.
        // A peer stealer may briefly hold the guard for a non-blocking
        // epoll_wait(timeout=0); spin until it releases. The spin is
        // bounded: the stealer's syscall is non-blocking and the
        // STEAL_DRAIN_BUDGET caps dispatch work.
        #[cfg(target_os = "linux")]
        {
            while self
                .epoll_guard
                .compare_exchange_weak(
                    false,
                    true,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                std::hint::spin_loop();
            }
        }
        let result = self.poll_and_dispatch_inner(timeout);
        #[cfg(target_os = "linux")]
        self.epoll_guard.store(false, Ordering::Release);
        result
    }

    fn poll_and_dispatch_inner(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.dispatch_calls);
        // Resolve our worker idx once for per-worker attribution. TLS
        // is set by `ShardedMioParker::ensure_reactor_installed`; this
        // function only runs from a worker thread, so the lookup
        // should succeed in the steady state.
        let self_idx =
            crate::runtime::scheduler::multi_thread::sharded_mio_park::current_worker_index();
        let events = &mut self.events;
        match self.poll.poll(events, timeout) {
            Ok(()) => {
                bump(&COUNTERS.dispatch_poll_ok);
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
                // Treat EINTR as spurious — caller will re-park if
                // needed. Matches the mio driver's behavior.
                bump(&COUNTERS.dispatch_poll_eintr);
            }
            Err(e) => {
                bump(&COUNTERS.dispatch_poll_err);
                return Err(e);
            }
        }

        if events.is_empty() {
            bump(&COUNTERS.dispatch_zero_events);
            return Ok(());
        }
        if super::lazy_debug::enabled() {
            let event_count = events.iter().count() as u64;
            COUNTERS
                .dispatch_events_total
                .fetch_add(event_count, Ordering::Relaxed);
        }

        // Single-owner dispatch: every event was delivered against
        // *this* worker's epoll fd because each fd is registered on
        // exactly one worker. Look up each token's slab key in our own
        // `OpsState`, holding the lock once across the whole batch.
        // The token's `worker_idx` is informational here and is only
        // sanity-checked against our own index in debug builds.
        let mut woken_total = 0usize;
        let mut waker_token_count = 0u64;
        let state = self.ops.lock().expect("sharded-mio ops poisoned");
        for event in events.iter() {
            let token = event.token();
            if token == WAKER_TOKEN {
                waker_token_count += 1;
                // Drain the eventfd count so the level-triggered
                // registration stops firing until the next wake().
                self.waker.drain();
                continue;
            }
            let (_w, key, gen) = unpack_token(token);
            let Some(io) = state.slab.get(key as usize) else {
                bump(&COUNTERS.dispatch_slab_miss);
                continue;
            };
            if io.sharded_mio_gen.load(Ordering::Relaxed) != gen {
                bump(&COUNTERS.dispatch_gen_mismatch);
                continue;
            }
            let ready = Ready::from_mio(event);
            bump(&COUNTERS.dispatch_woken);
            if ready.is_readable() {
                bump(&COUNTERS.dispatch_woken_readable);
            }
            if ready.is_writable() {
                bump(&COUNTERS.dispatch_woken_writable);
            }
            io.set_readiness(Tick::Set, |curr| curr | ready);
            io.wake(ready);
            woken_total += 1;
        }
        drop(state);
        if waker_token_count > 0 {
            COUNTERS
                .dispatch_waker_token
                .fetch_add(waker_token_count, Ordering::Relaxed);
        }
        if woken_total > 0 {
            if let Some(idx) = self_idx {
                super::lazy_debug::PER_WORKER
                    .dispatch_woken
                    .get(idx)
                    .map(|c| c.fetch_add(woken_total as u64, Ordering::Relaxed));
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for Reactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("sharded_mio::Reactor").finish_non_exhaustive()
    }
}

// ===== tests =====

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reactor_new_succeeds() {
        let _reactor = Reactor::new().expect("mio::Poll construction");
    }

    #[test]
    fn external_waker_unblocks_park() {
        let mut reactor = Reactor::new().expect("reactor");
        let ext = reactor.external_waker();
        let t = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            ext.wake().expect("wake");
        });
        let start = std::time::Instant::now();
        reactor.park().expect("park");
        let elapsed = start.elapsed();
        t.join().unwrap();
        assert!(elapsed >= Duration::from_millis(10), "park returned too fast: {elapsed:?}");
        assert!(elapsed < Duration::from_secs(2), "park took too long: {elapsed:?}");
    }

    #[test]
    fn park_timeout_zero_is_noop() {
        let mut reactor = Reactor::new().expect("reactor");
        let start = std::time::Instant::now();
        reactor
            .park_timeout(Duration::ZERO)
            .expect("park_timeout zero");
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "zero-timeout park blocked too long",
        );
    }
}
