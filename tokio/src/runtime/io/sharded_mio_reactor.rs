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
//! - `MSG_RING` + eventfd → a single `mio::Waker` per worker
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

use mio::{Events, Poll, Registry, Token, Waker};
use slab::Slab;

use crate::io::{Interest, Ready};
use crate::loom::sync::Arc;
use crate::runtime::io::driver::Tick;
use crate::runtime::io::registration::RegistrationSource;
use crate::runtime::io::ScheduledIo;

use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::Ordering;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

/// Reserved token for the per-worker `mio::Waker`. Events arriving with
/// this token are external/peer wakeups — scheduler state may have
/// changed, but there is no `ScheduledIo` to dispatch to.
pub(crate) const WAKER_TOKEN: Token = Token(usize::MAX);

/// Mio `Token` is a `usize`. On 64-bit Linux we pack the slab key into
/// the low 32 bits and a per-slot generation counter into the high
/// 32 bits. The two combined never produce `usize::MAX` because slab
/// keys are practically bounded well below `u32::MAX` and no real gen
/// reaches `u32::MAX` simultaneously, leaving `WAKER_TOKEN` distinct.
#[inline]
fn pack_token(key: u32, gen: u32) -> Token {
    Token(((gen as usize) << 32) | (key as usize))
}

#[inline]
fn unpack_token(t: Token) -> (u32, u32) {
    let raw = t.0;
    (raw as u32, (raw >> 32) as u32)
}

/// Mirror of `Ready::from_mio` operating on the raw `epoll_event.events`
/// bitmask returned by a direct `libc::epoll_wait` call.
///
/// Used by the readiness-stealing path: peer-worker steals call
/// `epoll_wait` directly (bypassing mio) and need to translate the raw
/// epoll bits the same way mio's `event::Event::is_*` methods would.
#[cfg(target_os = "linux")]
fn ready_from_epoll_events(events: u32) -> crate::io::Ready {
    use crate::io::Ready;
    let mut ready = Ready::EMPTY;
    let ev = events as i32;

    // EPOLLIN | EPOLLPRI -> readable. (Mio also surfaces priority
    // separately below; here we follow mio's `is_readable` which folds
    // EPOLLPRI into READABLE.)
    if ev & (libc::EPOLLIN | libc::EPOLLPRI) != 0 {
        ready |= Ready::READABLE;
    }
    if ev & libc::EPOLLOUT != 0 {
        ready |= Ready::WRITABLE;
    }
    // EPOLLRDHUP -> half-close on the read side.
    if ev & libc::EPOLLRDHUP != 0 {
        ready |= Ready::READ_CLOSED;
    }
    // EPOLLHUP -> full hang-up: both directions closed.
    if ev & libc::EPOLLHUP != 0 {
        ready |= Ready::READ_CLOSED | Ready::WRITE_CLOSED;
    }
    if ev & libc::EPOLLERR != 0 {
        ready |= Ready::ERROR;
        // Mio surfaces WRITE_CLOSED when EPOLLOUT registrations get
        // EPOLLERR — preserve that semantics.
        if ev & libc::EPOLLOUT != 0 {
            ready |= Ready::WRITE_CLOSED;
        }
    }
    if ev & libc::EPOLLPRI != 0 {
        ready |= Ready::PRIORITY;
    }
    ready
}

/// Capacity of each worker's `mio::Events` buffer. Sized in the same
/// spirit as the uring reactor's CQ: comfortably above the observed
/// per-park working set for the bench matrix. Mio silently rolls
/// excess events over to the next `poll()` call, so this is a latency
/// hint, not a correctness knob.
const EVENTS_CAPACITY: usize = 1024;

/// State shared between [`Reactor`] (dispatch) and [`SharedRegistry`]
/// (cross-thread register/deregister).
///
/// All three fields move together under one mutex: we want a single
/// lock-acquire for the dispatch pass (slab lookup), and the writes
/// from register/deregister have to keep the slab/gens/live_fds views
/// consistent.
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
    /// arrives at `apply_deregister` but the fd has since been closed
    /// and recycled by a *different* registration, the live key here
    /// won't match and the `epoll_ctl_del(fd)` is skipped (which would
    /// otherwise wipe the new owner's epoll record because epoll keys
    /// on fd, not on token).
    live_fds: HashMap<RawFd, u32>,
}

/// Per-worker `mio::Poll` reactor.
///
/// Owns the [`Poll`] instance and an `Events` scratch buffer. The slab
/// of live registrations lives behind an [`Arc<StdMutex<_>>`] because
/// registration happens from arbitrary threads via
/// [`SharedRegistry::register`] while dispatch happens on the owning
/// worker inside [`Self::park`] / [`Self::park_timeout`]. Lock
/// contention stays low: register/deregister are per-fd-lifecycle
/// (rare); dispatch holds the lock once per park (batch) rather than
/// once per event.
pub(crate) struct Reactor {
    poll: Poll,
    events: Events,

    /// Per-worker registration state — slab + per-slot gens + live-fd
    /// map. See [`OpsState`].
    ops: Arc<StdMutex<OpsState>>,

    /// Cross-thread waker, cloneable via [`Self::external_waker`] and
    /// via [`SharedRegistry`]. Mio collapses both the "external thread"
    /// and "peer worker" wake paths into one [`Waker`] — unlike the
    /// uring reactor which splits them into eventfd vs. `MSG_RING`.
    waker: Arc<Waker>,
}

/// Cross-thread wake handle. Analogous to
/// [`uring_reactor::ExternalWaker`][eu], but backed by a `mio::Waker`
/// rather than an eventfd. Cheap to clone.
///
/// [eu]: super::uring_reactor::ExternalWaker
#[derive(Clone)]
pub(crate) struct ExternalWaker {
    waker: Arc<Waker>,
}

impl std::fmt::Debug for ExternalWaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalWaker").finish_non_exhaustive()
    }
}

impl ExternalWaker {
    /// Wake the owning reactor. Thread-safe; may be called from any
    /// thread. Mio coalesces pending wakes: multiple calls before the
    /// target observes the event produce one event.
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
    registry: Registry,
    ops: Arc<StdMutex<OpsState>>,
    waker: Arc<Waker>,
}

/// Outcome of a [`SharedRegistry::register`] call. Carries the
/// freshly-assigned `(slab_key, gen)` pair so the caller can stash both
/// on the [`ScheduledIo`] for later mismatch checks.
pub(crate) struct RegisterOk {
    pub(crate) slab_key: u32,
    pub(crate) gen: u32,
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
    /// `mio::Waker`. Used by [`ShardedMioHandle::unpark`] to wake a
    /// parked worker from arbitrary threads.
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
        let token = pack_token(key_u32, gen);
        if let Err(e) = self.registry.register(source, token, interest.to_mio()) {
            bump(&COUNTERS.sr_register_err);
            let mut state = self.ops.lock().expect("sharded-mio ops poisoned");
            let _ = state.slab.try_remove(key_u32 as usize);
            // Roll back live_fds only if it still matches us (a racing
            // register from another thread shouldn't be undone).
            if state.live_fds.get(&fd) == Some(&key_u32) {
                state.live_fds.remove(&fd);
            }
            return Err(e);
        }
        // Stamp the gen on the ScheduledIo so the apply-deregister
        // path can compare against `state.gens[key]` later. The
        // slab_key is stamped by the caller (driver) on success.
        scheduled_io
            .sharded_mio_gen
            .store(gen, Ordering::Relaxed);
        bump(&COUNTERS.sr_register_ok);
        Ok(RegisterOk { slab_key: key_u32, gen })
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
    /// Used by the readiness-stealing path: a peer worker calls
    /// `libc::epoll_wait(epoll_fd(), ..., 0)` non-blocking to harvest
    /// events queued for this worker, then dispatches them through
    /// [`Self::steal_dispatch`].
    #[cfg(target_os = "linux")]
    pub(crate) fn epoll_fd(&self) -> RawFd {
        self.registry.as_raw_fd()
    }

    /// Dispatch a batch of raw epoll events harvested from this
    /// registry's epoll fd by a peer worker.
    ///
    /// `events` should be the populated prefix of the buffer the caller
    /// passed to `libc::epoll_wait(self.epoll_fd(), ..., 0)`. Returns
    /// the number of events that successfully resolved to a live
    /// `ScheduledIo` and fired its waker. The remaining count covers
    /// the WAKER_TOKEN, slab miss, or gen-mismatch cases.
    ///
    /// Concurrency: the kernel guarantees exactly-once delivery to
    /// `epoll_wait` callers, so a stealer/owner-park race resolves at
    /// the syscall layer with no double-fire. Slab access is serialised
    /// with the owning worker via the existing `ops` mutex.
    #[cfg(target_os = "linux")]
    pub(crate) fn steal_dispatch(&self, events: &[libc::epoll_event]) -> usize {
        use super::lazy_debug::{bump, StealDispatchGuard, COUNTERS};
        let state = self.ops.lock().expect("sharded-mio ops poisoned");
        // Mark the calling thread as being inside steal_dispatch so the
        // scheduler's `schedule_task` can attribute its local-vs-remote
        // branch to this path. Dropped at end of function scope.
        let _steal_guard = StealDispatchGuard::enter();
        let mut woken = 0usize;
        for ev in events {
            let token = Token(ev.u64 as usize);
            if token == WAKER_TOKEN {
                bump(&COUNTERS.steal_waker_token);
                continue;
            }
            let (key, gen) = unpack_token(token);
            let Some(io) = state.slab.get(key as usize) else {
                bump(&COUNTERS.steal_slab_miss);
                continue;
            };
            if io.sharded_mio_gen.load(Ordering::Relaxed) != gen {
                bump(&COUNTERS.steal_gen_mismatch);
                continue;
            }
            let ready = ready_from_epoll_events(ev.events);
            io.set_readiness(Tick::Set, |curr| curr | ready);
            io.wake(ready);
            bump(&COUNTERS.steal_events_woken);
            woken += 1;
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
        let waker = Arc::new(Waker::new(poll.registry(), WAKER_TOKEN)?);
        Ok(Self {
            poll,
            events: Events::with_capacity(EVENTS_CAPACITY),
            ops: Arc::new(StdMutex::new(OpsState {
                slab: Slab::new(),
                gens: Vec::new(),
                live_fds: HashMap::new(),
            })),
            waker,
        })
    }

    /// Clone out a cross-thread [`SharedRegistry`]. `mio::Registry` is
    /// obtained via `try_clone`, which on Linux duplicates the epoll
    /// fd's reference so the clone and the owned registry target the
    /// same kernel object.
    pub(crate) fn shared_registry(&self) -> io::Result<SharedRegistry> {
        let registry = self.poll.registry().try_clone()?;
        Ok(SharedRegistry {
            registry,
            ops: Arc::clone(&self.ops),
            waker: Arc::clone(&self.waker),
        })
    }

    /// Cross-thread wake handle. Unlike uring (which has distinct
    /// eventfd and MSG_RING paths), mio collapses both into one
    /// `Waker`, so this is the single cross-thread wake mechanism.
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

        let event_count = events.iter().count() as u64;
        COUNTERS
            .dispatch_events_total
            .fetch_add(event_count, Ordering::Relaxed);
        if event_count == 0 {
            bump(&COUNTERS.dispatch_zero_events);
        }

        // Hold the ops lock for the whole dispatch pass: lock-once vs.
        // lock-per-event is the right tradeoff given contention is
        // bounded by registration/deregistration rate rather than
        // event rate. Waking scheduled tasks can invoke arbitrary
        // waker callbacks, but those callbacks cannot themselves
        // re-enter the reactor's lock on this thread (they go
        // through the scheduler, not back into `register`).
        let state = self.ops.lock().expect("sharded-mio ops poisoned");

        for event in events.iter() {
            let token = event.token();
            if token == WAKER_TOKEN {
                bump(&COUNTERS.dispatch_waker_token);
                // Cross-thread wake. Scheduler-state checks happen
                // around the park call; nothing to dispatch here.
                continue;
            }
            let (key, gen) = unpack_token(token);
            let Some(io) = state.slab.get(key as usize) else {
                bump(&COUNTERS.dispatch_slab_miss);
                // Slab slot already vacated (e.g. a deregister raced
                // ahead of a poll tick that had already captured the
                // event). Safe to drop; the caller dropped interest.
                continue;
            };
            // Per-slot gen check: an event for a slot that has since
            // been reassigned would otherwise wake the wrong waiter
            // with a stale readiness mask. Skip.
            if io.sharded_mio_gen.load(Ordering::Relaxed) != gen {
                bump(&COUNTERS.dispatch_gen_mismatch);
                continue;
            }
            let ready = Ready::from_mio(event);
            bump(&COUNTERS.dispatch_woken);
            if let Some(idx) = self_idx {
                super::lazy_debug::bump_per_worker(
                    &super::lazy_debug::PER_WORKER.dispatch_woken,
                    idx,
                );
            }
            if ready.is_readable() {
                bump(&COUNTERS.dispatch_woken_readable);
            }
            if ready.is_writable() {
                bump(&COUNTERS.dispatch_woken_writable);
            }
            io.set_readiness(Tick::Set, |curr| curr | ready);
            io.wake(ready);
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
