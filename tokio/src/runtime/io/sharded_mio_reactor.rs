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

use std::io;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

/// Reserved token for the per-worker `mio::Waker`. Events arriving with
/// this token are external/peer wakeups — scheduler state may have
/// changed, but there is no `ScheduledIo` to dispatch to.
pub(crate) const WAKER_TOKEN: Token = Token(usize::MAX);

/// Capacity of each worker's `mio::Events` buffer. Sized in the same
/// spirit as the uring reactor's CQ: comfortably above the observed
/// per-park working set for the bench matrix. Mio silently rolls
/// excess events over to the next `poll()` call, so this is a latency
/// hint, not a correctness knob.
const EVENTS_CAPACITY: usize = 1024;

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

    /// Slab of live registrations, keyed by the token handed to mio.
    /// The `Arc<ScheduledIo>` is held here for the full lifetime of the
    /// mio registration. On `deregister` we both deregister the source
    /// from mio **and** remove the slab entry, dropping the Arc. Mio
    /// guarantees no further events will fire on a deregistered source,
    /// so there is no stale-event race that would require a gen bit.
    ops: Arc<StdMutex<Slab<Arc<ScheduledIo>>>>,

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
    ops: Arc<StdMutex<Slab<Arc<ScheduledIo>>>>,
    waker: Arc<Waker>,
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

    /// Register `source` on this reactor's [`Poll`] for `interest`, and
    /// allocate a slab entry holding a clone of `scheduled_io`.
    /// Returns the slab key (which is also the token value handed to
    /// mio).
    ///
    /// Thread-safe. Rolls back the slab insert if mio registration
    /// fails, so the caller observes a clean "not registered" state on
    /// error.
    pub(crate) fn register<S: RegistrationSource + ?Sized>(
        &self,
        source: &mut S,
        interest: Interest,
        scheduled_io: &Arc<ScheduledIo>,
    ) -> io::Result<u32> {
        let key = {
            let mut slab = self.ops.lock().expect("sharded-mio slab poisoned");
            slab.insert(Arc::clone(scheduled_io))
        };
        let key_u32 = u32::try_from(key).expect("slab key exceeds u32");
        let token = Token(key);
        if let Err(e) = self.registry.register(source, token, interest.to_mio()) {
            let mut slab = self.ops.lock().expect("sharded-mio slab poisoned");
            let _ = slab.try_remove(key);
            return Err(e);
        }
        Ok(key_u32)
    }

    /// Deregister `source` from this reactor's [`Poll`] and drop the
    /// slab entry identified by `slab_key`.
    ///
    /// Mio guarantees no further events will be surfaced for the
    /// deregistered source, so it is safe to drop the
    /// `Arc<ScheduledIo>` immediately.
    pub(crate) fn deregister<S: RegistrationSource + ?Sized>(
        &self,
        source: &mut S,
        slab_key: u32,
    ) -> io::Result<()> {
        let mio_result = self.registry.deregister(source);
        if slab_key != u32::MAX {
            let mut slab = self.ops.lock().expect("sharded-mio slab poisoned");
            let _ = slab.try_remove(slab_key as usize);
        }
        mio_result
    }

    /// Drop the slab entry identified by `slab_key` without touching
    /// mio. Used during `ScheduledIo` teardown when the underlying
    /// source has already been closed — mio implicitly deregisters on
    /// fd close, so the explicit `Registry::deregister` would just
    /// return `ENOENT`.
    #[allow(dead_code)]
    pub(crate) fn drop_slab_entry(&self, slab_key: u32) {
        if slab_key == u32::MAX {
            return;
        }
        let mut slab = self.ops.lock().expect("sharded-mio slab poisoned");
        let _ = slab.try_remove(slab_key as usize);
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
            ops: Arc::new(StdMutex::new(Slab::new())),
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
        let events = &mut self.events;
        match self.poll.poll(events, timeout) {
            Ok(()) => {}
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
                // Treat EINTR as spurious — caller will re-park if
                // needed. Matches the mio driver's behavior.
            }
            Err(e) => return Err(e),
        }

        // Hold the slab lock for the whole dispatch pass: lock-once vs.
        // lock-per-event is the right tradeoff given contention is
        // bounded by registration/deregistration rate rather than
        // event rate. Waking scheduled tasks can invoke arbitrary
        // waker callbacks, but those callbacks cannot themselves
        // re-enter the reactor's slab lock on this thread (they go
        // through the scheduler, not back into `register`).
        let slab = self.ops.lock().expect("sharded-mio slab poisoned");

        for event in events.iter() {
            let token = event.token();
            if token == WAKER_TOKEN {
                // Cross-thread wake. Scheduler-state checks happen
                // around the park call; nothing to dispatch here.
                continue;
            }
            let Some(io) = slab.get(token.0) else {
                // Slab slot already vacated (e.g. a deregister raced
                // ahead of a poll tick that had already captured the
                // event). Safe to drop; the caller dropped interest.
                continue;
            };
            let ready = Ready::from_mio(event);
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
