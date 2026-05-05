//! Shared handle and cross-worker coordination for the sharded-mio
//! reactor backend.
//!
//! ## Lazy first-poll registration
//!
//! Originally this backend mirrored the legacy mio driver: every
//! `Registration::new_with_interest` call eagerly allocated
//! a [`ScheduledIo`] from a global [`RegistrationSet`] and called
//! `mio::Registry::register` on a round-robin-picked worker. That work
//! ran on the producer thread (often a single accept worker) and
//! serialised through one global mutex.
//!
//! The current shape is lazy: `Registration::new_with_interest`
//! does no driver work — it stashes `(fd, interest)` only. The first
//! `poll_ready` / `try_io` / `readiness` call on the registration runs
//! on whichever worker happens to be polling and triggers
//! [`ShardedMioHandle::register_local`], which:
//!
//! 1. Allocates `Arc::new(ScheduledIo::default())`.
//! 2. Inserts it into the polling worker's per-shard
//!    [`RegistrationSet`] (no global mutex involved).
//! 3. Calls `mio::Registry::register` on the polling worker's own
//!    `SharedRegistry` — entirely thread-local, lock-free against
//!    sibling workers.
//!
//! Under the single-owner registration model, each fd is registered
//! on exactly one worker's epoll fd; events surface only to that
//! worker. `mio::Registry` clones are `Send + Sync`, so register and
//! deregister calls run synchronously on the calling thread (no
//! cross-thread queue), but they target the owner's child epoll
//! whoever the caller is. Cross-worker readiness propagation —
//! readiness stealing under contention, idle-worker wake on owner
//! burn — is layered on top in subsequent commits via a runtime-wide
//! meta-epoll and a try-lock peer drain. Until those land, an event
//! whose owner is mid-burn is not visible to a peer.
//!
//! When `register_local` is called from off-runtime (no worker idx in
//! TLS), it picks a fallback worker for slab ownership via
//! [`fallback_worker`].

use std::cell::{Cell, RefCell};
use std::io;
use std::os::fd::RawFd;
use std::ptr;
#[cfg(target_os = "linux")]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use crate::io::interest::Interest;
use crate::loom::sync::Mutex;
use crate::runtime::io::registration_set;
use crate::runtime::io::sharded_mio_reactor::{
    DeregisterOutcome, ExternalWaker, Reactor, SharedRegistry,
};
use crate::runtime::io::{IoDriverMetrics, RegistrationSet, ScheduledIo};
use crate::util::cacheline::CachePadded;

/// Park-state atomic values. Encodes which wake mechanism the unpark
/// path should use so [`ShardedMioHandle::unpark`] can route to exactly
/// one wake mechanism instead of firing both an eventfd write and a
/// `Thread::unpark` for every cross-worker unpark.
///
/// The parker publishes which branch it took into `park_state` *before*
/// entering the blocking syscall (via [`ShardedMioHandle::commit_park`]
/// taking a [`ParkMode`]); the unpark path single-matches on `prev` and
/// only issues the wake mechanism that is load-bearing for the branch
/// the parker is in. The intermediate `SEARCHING` state covers the
/// pre-syscall spin window — an unpark in that window suppresses the
/// kernel wake entirely, and the parker's spin or its
/// `SEARCHING → PARKED_<mode>` CAS observes the resulting `NOTIFIED`
/// and bails. See the unpark module docs for the full race argument.
// `park_state` is an `AtomicU32`. The state values trivially fit
// in `u32` (max value is `SEARCHING = 4`).
pub(crate) const EMPTY: u32 = 0;
pub(crate) const PARKED_OWN: u32 = 1;
pub(crate) const PARKED_META: u32 = 2;
pub(crate) const NOTIFIED: u32 = 3;
/// Pre-park spin window. The parker has committed to *try* to park
/// but has not yet entered the kernel; a concurrent `unpark` swap
/// observes `prev == SEARCHING` and skips the kernel wake (the
/// parker will see the resulting `NOTIFIED` from inside its spin
/// loop or via its `commit_park` CAS).
pub(crate) const SEARCHING: u32 = 4;
/// Park branch the worker is about to block in. Passed to
/// [`ShardedMioHandle::begin_park`] so the published `park_state`
/// records which wake mechanism the unpark path should use. The
/// non-Linux variant `Meta` is unreachable on non-Linux but kept in
/// the enum so callers don't need cfg-fences around the match
/// expression.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ParkMode {
    /// `park_on_own_child` — worker blocks in `mio::Poll::poll` on
    /// its own child epoll fd. Wake by writing the worker's
    /// `external_waker` eventfd (registered in the child Poll).
    /// This is the only branch on non-meta-watcher workers; even
    /// when the worker's slab is empty, the child Poll still has
    /// the `external_waker` eventfd registered, so `mio::Poll::poll`
    /// is wakeable by the unpark path.
    OwnChild,
    /// `park_on_meta` — worker blocks in `epoll_wait` on the
    /// runtime-wide meta epoll. Wake by writing the meta-waker
    /// eventfd (registered on the meta epoll with
    /// `META_WAKER_TOKEN`).
    #[cfg(target_os = "linux")]
    Meta,
}

impl ParkMode {
    #[inline]
    fn as_state(self) -> u32 {
        match self {
            ParkMode::OwnChild => PARKED_OWN,
            #[cfg(target_os = "linux")]
            ParkMode::Meta => PARKED_META,
        }
    }
}

/// `epoll_event.u64` token used to identify the meta-waker eventfd
/// when it fires on the meta epoll. Children carry their `worker_idx`
/// as their u64; this sentinel is well outside any plausible worker
/// index (`MAX_WORKERS` is 16 on this branch, capped by
/// `TOKEN_WORKER_BITS`).
#[cfg(target_os = "linux")]
const META_WAKER_TOKEN: u64 = u64::MAX;

/// Standalone eventfd registered on the runtime-wide meta epoll fd
/// with `data.u64 = META_WAKER_TOKEN`. The unpark path writes 1 to
/// the eventfd to wake whichever worker is currently parked on the
/// meta epoll (the meta-watcher); the watcher drains it on the way
/// out of `epoll_wait`.
///
/// Sized identical to the per-worker `WAKER_TOKEN` eventfd that
/// `ExternalWaker` uses, but registered on the meta epoll instead
/// of any child Poll, so the meta-watcher branch has a wake target
/// that does not require the watcher to also be the slab owner of
/// any particular worker.
#[cfg(target_os = "linux")]
struct MetaWaker {
    fd: RawFd,
}

#[cfg(target_os = "linux")]
impl MetaWaker {
    fn new() -> io::Result<Self> {
        // SAFETY: passing well-defined libc flag constants to
        // `eventfd(2)` which has no preconditions on the caller.
        let fd = unsafe {
            libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC)
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { fd })
    }

    fn fd(&self) -> RawFd {
        self.fd
    }

    /// Increment the eventfd counter, edging the meta epoll
    /// awake. Idempotent — multiple writes between drains coalesce
    /// into a single readable edge.
    fn wake(&self) {
        let val: u64 = 1;
        // SAFETY: `self.fd` is a valid eventfd owned by `self` for
        // the lifetime of `MetaWaker`; `&val` is stack-resident for
        // the duration of the call. `libc::write` returns `-1` on
        // failure which we ignore (idempotent best-effort wake; a
        // spurious EAGAIN cannot happen on a level-counting eventfd
        // unless the counter would overflow `u64::MAX - 1`, which
        // we never reach because the watcher drains on every wake).
        unsafe {
            let _ = libc::write(
                self.fd,
                &val as *const u64 as *const libc::c_void,
                std::mem::size_of::<u64>(),
            );
        }
    }

    /// Read the eventfd counter back to zero so the next wake
    /// edge fires `epoll_wait` again. Called by the meta-watcher
    /// after observing the meta-waker token in the returned
    /// events.
    fn drain(&self) {
        let mut buf: u64 = 0;
        // SAFETY: same fd lifetime as `wake`; `&mut buf` is stack
        // resident; `read` may fail with `EAGAIN` if the counter is
        // already zero (spurious / racing drain), in which case we
        // simply ignore the error.
        unsafe {
            let _ = libc::read(
                self.fd,
                &mut buf as *mut u64 as *mut libc::c_void,
                std::mem::size_of::<u64>(),
            );
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for MetaWaker {
    fn drop(&mut self) {
        // SAFETY: `self.fd` was created by `eventfd(2)` in `new()`
        // and not closed elsewhere; `Drop` runs at most once.
        unsafe { libc::close(self.fd) };
    }
}

/// RAII handle for the meta-watcher slot. Constructed by
/// [`ShardedMioHandle::try_acquire_meta_watcher`]; releases the slot
/// on drop so the next idle worker can take over.
///
/// Holds an `Arc<ShardedMioHandle>` rather than borrowing it so the
/// guard is 'static — required to pass it across `&mut self` method
/// boundaries inside the parker without tripping the borrow checker.
/// The clone is one atomic-inc, dwarfed by the syscall the guard
/// protects.
///
/// Owned by the worker that won the gate; consumed by dropping the
/// guard (or via `release()`) once the gate-protected syscall has
/// returned and the dispatch loop is ready to release the slot.
#[cfg(target_os = "linux")]
#[allow(dead_code)]
pub(crate) struct MetaWatcherGuard {
    handle: Arc<ShardedMioHandle>,
    released: bool,
}

#[cfg(target_os = "linux")]
impl MetaWatcherGuard {
    /// Release the gate explicitly. Subsequent `Drop` becomes a no-op.
    /// Currently unused — the parker relies on RAII drop — but kept
    /// as a public API for callers that want to release before
    /// the borrow ends.
    #[allow(dead_code)]
    pub(crate) fn release(mut self) {
        self.do_release();
    }

    fn do_release(&mut self) {
        if !self.released {
            self.handle
                .meta_watcher_busy
                .store(false, Ordering::Release);
            self.released = true;
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for MetaWatcherGuard {
    fn drop(&mut self) {
        self.do_release();
    }
}

/// Per-worker coordination slot. One per worker, indexed by worker id.
///
/// The owning `WorkerSet` stores these as `CachePadded<WorkerState>` so
/// each slot sits on its own coherence unit; without that, `park_state`
/// from adjacent workers shares a cache line with neighbouring slots'
/// hot atomics and produces false-sharing traffic on the parallel-fanout
/// dispatch path.
pub(crate) struct WorkerState {
    /// `EMPTY | PARKED_OWN | PARKED_META | NOTIFIED | SEARCHING`.
    /// Written by the owning worker on park/resume; read/CAS'd by
    /// unparkers.
    pub(crate) park_state: AtomicU32,

    /// Cross-thread handle to this worker's mio `Registry` + slab +
    /// `Waker`. Published once during worker startup via
    /// [`ShardedMioHandle::register_worker`].
    pub(crate) shared_registry: OnceLock<SharedRegistry>,

    /// Waker clone for the fast-path `unpark`. Published alongside
    /// `shared_registry`; kept as a separate cheap-to-clone handle so
    /// `unpark` doesn't have to take any slab locks.
    pub(crate) external_waker: OnceLock<ExternalWaker>,

    /// Per-shard registration set: tracks the live
    /// `Arc<ScheduledIo>`s whose lazy first-poll registration landed on
    /// this worker, so [`Drop`] on the handle can shut them down. Lives
    /// here (not on [`ShardedMioHandle`]) so per-worker registration
    /// state never contends a global mutex.
    pub(super) registrations: RegistrationSet,

    /// Synced state for [`Self::registrations`]. Mutated under the
    /// owning worker's slab insert/remove paths. Cross-thread register
    /// / deregister calls take it briefly via [`Mutex::lock`] before
    /// touching the slab.
    pub(super) synced: Mutex<registration_set::Synced>,

    /// One-shot latch: set to `true` the first time a `ScheduledIo`
    /// successfully registers on this worker's child epoll. Once set,
    /// never reset — it is a permanent "this worker has touched I/O"
    /// flag, used to gate the meta-watcher CAS so pure-sync workloads
    /// don't pay the global CAS contention on `meta_watcher_busy` per
    /// park.
    ///
    /// Conservative by design: a worker that ever held a registration
    /// stays gated `true` for the rest of its life, even after the
    /// `ScheduledIo` is deregistered. Tracking exact emptiness would
    /// require a refcount and a re-check race against in-flight
    /// `register_on_worker` calls — not worth the complexity for the
    /// common "one worker handles all the I/O, others are CPU-bound"
    /// shape this gate is designed to optimise.
    pub(crate) has_io_registered: AtomicBool,
}

impl WorkerState {
    fn new() -> Self {
        let (registrations, synced) = RegistrationSet::new();
        Self {
            park_state: AtomicU32::new(EMPTY),
            shared_registry: OnceLock::new(),
            external_waker: OnceLock::new(),
            registrations,
            synced: Mutex::new(synced),
            has_io_registered: AtomicBool::new(false),
        }
    }
}

impl std::fmt::Debug for WorkerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerState").finish_non_exhaustive()
    }
}

/// Shared I/O handle for the sharded-mio backend.
///
/// Symmetry with [`UringHandle`][uh]: per-worker slots for unparking
/// and per-worker [`RegistrationSet`]s. There is no global registration
/// set — each shard owns its own, eliminating a single-mutex
/// serialization point under TCP connect churn. There is also no
/// cross-thread `pending_ops` queue: register/deregister calls run
/// synchronously on the calling thread, targeting the owner worker's
/// child epoll directly via the cloneable `mio::Registry`.
///
/// On Linux the handle additionally owns a *meta epoll fd*: every
/// worker's child epoll is registered onto it as level-triggered
/// `EPOLLIN`. A worker that is idle but not its target's owner can
/// park on the meta-epoll instead of its own child epoll and pick
/// up "some sibling has events" wake notifications, then drain the
/// firing child via [`SharedRegistry`]'s try-lock peer path. The
/// child epolls themselves remain the single-owner registration
/// surface — only the *parking* layer is hierarchical.
///
/// [uh]: super::uring_driver::UringHandle
pub(crate) struct ShardedMioHandle {
    workers: Box<[CachePadded<WorkerState>]>,
    next_worker: AtomicUsize,

    pub(crate) metrics: IoDriverMetrics,

    /// Per-runtime startup barrier. Every worker waits here after
    /// building its reactor and before entering the scheduler loop so
    /// task polling begins at the same wall-clock moment across
    /// workers. Mirrors the same rationale as
    /// [`UringHandle::start_barrier`][uhb].
    ///
    /// [uhb]: super::uring_driver::UringHandle
    start_barrier: std::sync::Barrier,

    /// Runtime-wide *meta epoll fd*. Each worker's child epoll is
    /// added here once it publishes its `SharedRegistry`, with the
    /// child's `worker_idx` carried in `epoll_event.data.u64`. Used
    /// by the upcoming steal-mode park path to wait for "some
    /// sibling has events" and resolve back to the firing worker.
    ///
    /// Owned by this handle: created in [`Self::new`], closed in
    /// [`Drop`]. Not exposed publicly outside the sharded-mio module.
    #[cfg(target_os = "linux")]
    meta_epfd: RawFd,

    /// Userspace gate: at most one worker at a time blocks on the
    /// meta epoll. Workers that lose the gate park on their own
    /// child epoll fd via [`ShardedMioParker::park_on_own_child`]
    /// for the duration the caller requested; the meta-watcher
    /// covers any peer events for them.
    ///
    /// # Motivation
    ///
    /// Without the gate, every idle worker `epoll_wait`s on the meta
    /// fd. With four workers and one fd producing events, sharded-mio
    /// pays `4 × epoll_wait` overhead vs. traditional's
    /// `1 × epoll_wait + 3 × futex` — `perf stat` localised the
    /// `busy_owner_idle` regression to that excess kernel-side syscall
    /// volume. `EPOLLEXCLUSIVE` would have provided kernel-side
    /// fan-in but `epoll_ctl(2)` rejects it with `EINVAL` when the
    /// target fd is itself an epoll instance, which is the meta-of-
    /// children shape we use. So the gate lives in userspace.
    ///
    /// # Why earlier gate spikes regressed `busy_owner_3burners`
    ///
    /// A naive gate funnels every wake into the watcher's run queue
    /// (via `try_steal_drain`'s call to `io.wake(ready)` on the
    /// watcher thread), leaving peers parked until the scheduler's
    /// own `notify_parked_remote` fires later. That extra hop costs
    /// us the cache-locality advantage we get on `busy_owner_3burners`.
    ///
    /// The current design avoids the regression by relying on the
    /// in-band `io.wake(ready)` push performed by `try_steal_drain`:
    /// after dispatching a peer's events the freshly-woken tasks are
    /// already on the owner's run queue, so the owner peer itself
    /// re-enters the scheduler loop and picks them up directly. (The
    /// previous design also OR'd a per-worker stake bitset into the
    /// owner's `WorkerState` for a post-drain fan-out unpark, but that
    /// substrate was never wired to a consumer and produced
    /// false-sharing on the owner's hot park line — removed.)
    ///
    /// [`epoll_ctl(2)`]: https://man7.org/linux/man-pages/man2/epoll_ctl.2.html
    #[cfg(target_os = "linux")]
    meta_watcher_busy: AtomicBool,

    /// Eventfd registered on `meta_epfd` so the unpark path can wake a
    /// worker that is currently parked in the meta-watcher branch.
    /// Owned by the handle: created in [`Self::new`] alongside
    /// `meta_epfd`, registered on the meta epoll with sentinel
    /// `META_WAKER_TOKEN`, drained by the meta-watcher in
    /// [`super::sharded_mio_park::ShardedMioParker::park_on_meta`],
    /// closed in [`Drop`].
    #[cfg(target_os = "linux")]
    meta_waker: MetaWaker,
}

impl std::fmt::Debug for ShardedMioHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedMioHandle")
            .field("num_workers", &self.workers.len())
            .finish_non_exhaustive()
    }
}

#[cfg(target_os = "linux")]
impl Drop for ShardedMioHandle {
    fn drop(&mut self) {
        // Close the meta epoll fd. Children remain registered until
        // each worker's `SharedRegistry` (and the underlying child
        // epoll fd) is dropped a moment later as part of runtime
        // teardown — the kernel removes the meta-side records
        // automatically when the meta fd closes, so we don't need to
        // walk the workers and `EPOLL_CTL_DEL` first.
        if self.meta_epfd >= 0 {
            // SAFETY: `meta_epfd` was created by `epoll_create1` in
            // `Self::new` and has not been closed elsewhere — Drop
            // runs at most once.
            unsafe { libc::close(self.meta_epfd) };
        }
    }
}

impl ShardedMioHandle {
    pub(crate) fn new(num_workers: usize) -> Self {
        let mut workers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            workers.push(CachePadded::new(WorkerState::new()));
        }
        let barrier_count = num_workers.max(1);

        // Create the runtime-wide meta epoll fd up front so it is
        // available for `register_worker` to add child epolls onto.
        // `EPOLL_CLOEXEC` keeps the fd from leaking across `exec`.
        #[cfg(target_os = "linux")]
        let meta_epfd = {
            let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
            if fd < 0 {
                let err = io::Error::last_os_error();
                panic!("sharded-mio: epoll_create1 for meta epoll failed: {err}");
            }
            fd
        };

        // Create the meta-waker eventfd and register it on the meta
        // epoll. The unpark path writes to this fd to wake a worker
        // currently parked in the meta-watcher branch (see
        // `ParkMode::Meta`). `META_WAKER_TOKEN` is the sentinel
        // `epoll_event.u64` the watcher uses to identify the wake
        // (vs. a child epoll firing on real I/O).
        #[cfg(target_os = "linux")]
        let meta_waker = {
            let mw = match MetaWaker::new() {
                Ok(mw) => mw,
                Err(err) => panic!(
                    "sharded-mio: eventfd for meta_waker failed: {err}",
                ),
            };
            let mut ev = libc::epoll_event {
                events: libc::EPOLLIN as u32,
                u64: META_WAKER_TOKEN,
            };
            // SAFETY: `meta_epfd` is freshly created above and
            // unshared; `mw.fd()` is owned by `mw` for the duration
            // of this handle (we move it into `Self` below); `&mut
            // ev` is a fresh stack value the kernel reads but does
            // not retain.
            let ret = unsafe {
                libc::epoll_ctl(
                    meta_epfd,
                    libc::EPOLL_CTL_ADD,
                    mw.fd(),
                    &mut ev,
                )
            };
            if ret != 0 {
                let err = io::Error::last_os_error();
                panic!(
                    "sharded-mio: epoll_ctl(meta, ADD, meta_waker) failed: {err}",
                );
            }
            mw
        };

        Self {
            workers: workers.into_boxed_slice(),
            next_worker: AtomicUsize::new(0),
            metrics: IoDriverMetrics::default(),
            start_barrier: std::sync::Barrier::new(barrier_count),
            #[cfg(target_os = "linux")]
            meta_epfd,
            #[cfg(target_os = "linux")]
            meta_watcher_busy: AtomicBool::new(false),
            #[cfg(target_os = "linux")]
            meta_waker,
        }
    }

    /// Try to become the runtime-wide *meta watcher*: the single
    /// thread allowed to block in `epoll_wait` on the meta epoll fd
    /// at any given moment. Returns `Some(guard)` on success — drop
    /// the guard (or call `MetaWatcherGuard::release`) to release the
    /// watcher slot. Returns `None` when another worker already holds
    /// it; the caller should fall back to parking on its own child
    /// epoll.
    ///
    /// Takes `self: &Arc<Self>` so the returned guard can carry an
    /// `Arc<Self>` clone, decoupling its lifetime from the caller and
    /// allowing it to cross `&mut self` boundaries on the parker side.
    ///
    /// See [`Self::meta_watcher_busy`] for the gate's motivation.
    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    pub(crate) fn try_acquire_meta_watcher(
        self: &Arc<Self>,
    ) -> Option<MetaWatcherGuard> {
        match self.meta_watcher_busy.compare_exchange(
            false,
            true,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => Some(MetaWatcherGuard {
                handle: Arc::clone(self),
                released: false,
            }),
            Err(_) => None,
        }
    }

    /// Raw meta-epoll fd. Stable for the lifetime of `self` and closed
    /// in [`Drop`]. Currently only the parker module needs this — for
    /// the upcoming steal-mode `epoll_wait` on the meta fd. Not
    /// exposed publicly outside the sharded-mio backend.
    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    pub(crate) fn meta_epfd(&self) -> RawFd {
        self.meta_epfd
    }

    /// Sentinel `epoll_event.u64` that identifies the meta-waker
    /// eventfd in the events buffer returned by `epoll_wait` on
    /// `meta_epfd`. The meta-watcher matches on this token to drain
    /// the meta-waker (re-arm it for the next wake) instead of
    /// treating it as a worker child.
    #[cfg(target_os = "linux")]
    #[inline]
    pub(crate) fn meta_waker_token() -> u64 {
        META_WAKER_TOKEN
    }

    /// Drain the meta-waker eventfd. Called by
    /// `ShardedMioParker::park_on_meta` after observing
    /// `META_WAKER_TOKEN` in the kernel-returned events.
    #[cfg(target_os = "linux")]
    #[inline]
    pub(crate) fn drain_meta_waker(&self) {
        self.meta_waker.drain();
    }

    /// Number of workers this handle serves.
    #[allow(dead_code)]
    pub(crate) fn num_workers(&self) -> usize {
        self.workers.len()
    }

    /// Per-worker state slice. Reserved for the upcoming meta-epoll
    /// integration: a parker idle on the meta-epoll will need to
    /// resolve a wake notification (which carries the firing child
    /// epoll's worker idx) back to that worker's `SharedRegistry` to
    /// run the steal-drain pass. Not part of the public
    /// `IoDriverHandle` surface — strictly internal to the
    /// sharded-mio backend.
    #[allow(dead_code)]
    pub(crate) fn workers(&self) -> &[CachePadded<WorkerState>] {
        &self.workers
    }

    /// Block until every sibling worker has also reached this call.
    /// Called exactly once per worker at startup after the reactor is
    /// built and its `SharedRegistry` published.
    pub(crate) fn wait_for_start(&self) {
        self.start_barrier.wait();
    }

    /// Publish a worker's `SharedRegistry` and `ExternalWaker`. Called
    /// exactly once per worker during scheduler startup, on that
    /// worker's thread. After this returns, other threads can target
    /// the worker via `add_source` / `unpark`.
    ///
    /// On Linux this also registers the worker's child epoll fd onto
    /// the runtime-wide meta epoll as level-triggered `EPOLLIN`, with
    /// `worker_idx` stamped into `epoll_event.data.u64`. The level-
    /// triggered shape is deliberate: we want the meta `epoll_wait`
    /// to keep reporting a child as ready until that child has been
    /// drained, so a peer parker that wakes on the meta but loses the
    /// try-lock race can be re-woken on the next attempt without any
    /// intervening event.
    pub(crate) fn register_worker(
        &self,
        worker_idx: usize,
        shared_registry: SharedRegistry,
        external_waker: ExternalWaker,
    ) {
        let slot = &self.workers[worker_idx];

        // Register the child epoll onto the meta epoll *before*
        // publishing `shared_registry` so that any thread that
        // subsequently observes the published registry can also rely
        // on the child being visible to the meta. Failure here is a
        // hard error — the runtime cannot honor steal-mode park
        // without the registration in place.
        #[cfg(target_os = "linux")]
        {
            let child_epfd = shared_registry.epoll_fd();
            let mut ev = libc::epoll_event {
                events: libc::EPOLLIN as u32,
                u64: worker_idx as u64,
            };
            // SAFETY: `self.meta_epfd` is owned by this handle and
            // stays open until `Drop`; `child_epfd` is owned by the
            // soon-to-be-published `SharedRegistry`, which itself
            // outlives the handle (the worker's `Reactor` is dropped
            // last on shutdown via `ShardedMioParker::shutdown`).
            // `&mut ev` is a fresh stack value the kernel reads but
            // does not retain.
            let ret = unsafe {
                libc::epoll_ctl(
                    self.meta_epfd,
                    libc::EPOLL_CTL_ADD,
                    child_epfd,
                    &mut ev,
                )
            };
            if ret != 0 {
                let err = io::Error::last_os_error();
                panic!(
                    "sharded-mio: epoll_ctl(meta, ADD, child={child_epfd}, \
                     worker={worker_idx}) failed: {err}",
                );
            }
        }

        if slot.shared_registry.set(shared_registry).is_err() {
            debug_assert!(false, "worker {worker_idx} published shared_registry twice");
        }
        if slot.external_waker.set(external_waker).is_err() {
            debug_assert!(false, "worker {worker_idx} published external_waker twice");
        }
    }

    /// Mark `worker_idx` as notified and — if the worker was parked —
    /// deliver an actual wake to the one mechanism the parker is blocked
    /// in.
    ///
    /// `park_state` carries the [`ParkMode`] the parker took
    /// ([`PARKED_OWN`] or [`PARKED_META`]) so this method can issue
    /// exactly one kernel wake per cross-worker unpark instead of
    /// fanning out an `external_waker` eventfd write *and* a
    /// `Thread::unpark` for every notification.
    ///
    /// Race argument: the parker transitions through three CAS-published
    /// states before entering the kernel —
    /// [`begin_searching`] (`EMPTY → SEARCHING`), then
    /// [`commit_park`] (`SEARCHING → PARKED_<mode>`), then the
    /// blocking syscall. All CASes use `AcqRel` ordering. An
    /// unparker's `swap(NOTIFIED, Release)` against `prev`:
    ///
    /// - `prev = PARKED_<X>` → parker is in syscall `X`; deliver one
    ///   kernel wake on `X`'s mechanism.
    /// - `prev = SEARCHING` → parker is in the pre-park spin window;
    ///   no kernel wake (the spin loop or the `commit_park` CAS will
    ///   observe NOTIFIED and bail).
    /// - `prev = EMPTY` → parker is mid-task; no kernel wake (the
    ///   next `try_consume_notified` will short-circuit).
    /// - `prev = NOTIFIED` → already pending; idempotent.
    ///
    /// The single wake mechanism for the parker's actual branch is
    /// the only one that needs to fire.
    ///
    /// Returns `true` if a wake was delivered to the kernel (for
    /// metrics). Mirrors [`UringHandle::unpark`][uu], specialising
    /// the wake target on the parker's branch.
    ///
    /// [uu]: super::uring_driver::UringHandle::unpark
    pub(crate) fn unpark(&self, worker_idx: usize) -> bool {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.unpark_calls);
        let slot = &self.workers[worker_idx];
        let prev = slot.park_state.swap(NOTIFIED, Ordering::Release);
        match prev {
            PARKED_OWN => {
                bump(&COUNTERS.unpark_was_parked);
                // Worker is in `mio::Poll::poll` on its own child
                // epoll. The child has its `WAKER_TOKEN` eventfd
                // registered; writing it edges `epoll_wait` awake.
                if let Some(waker) = slot.external_waker.get() {
                    let _ = waker.wake();
                }
                true
            }
            #[cfg(target_os = "linux")]
            PARKED_META => {
                bump(&COUNTERS.unpark_was_parked);
                // Worker is in `epoll_wait` on the runtime-wide meta
                // epoll. Wake by writing the meta-waker eventfd that
                // is registered on the meta epoll with sentinel
                // `META_WAKER_TOKEN`.
                self.meta_waker.wake();
                true
            }
            SEARCHING => {
                bump(&COUNTERS.unpark_was_searching);
                // Parker is in the pre-park spin window. The swap
                // above already published NOTIFIED; the spin will
                // observe it (or its `commit_park` CAS will fail
                // with NOTIFIED) and return without ever entering
                // a syscall. No kernel wake needed.
                false
            }
            EMPTY => {
                bump(&COUNTERS.unpark_was_empty);
                false
            }
            // `NOTIFIED` (or any unexpected state — defensive
            // fallthrough; the swap above guarantees we never see
            // `PARKED_*` from a previously-finished cycle).
            _ => {
                bump(&COUNTERS.unpark_was_notified);
                false
            }
        }
    }

    /// Cheap pre-park fast-path. If a notification is already
    /// pending (`park_state == NOTIFIED`), consume it and return
    /// `true`; otherwise leave state untouched and return `false`.
    ///
    /// Called by the parker before mode selection so that the
    /// hot-loop `notify → wake → re-park → notify` cycle does not
    /// pay the mode-selection cost (in particular the
    /// `try_acquire_meta_watcher` `AtomicBool` CAS) on every
    /// trip through the loop. Workers that pass this check still
    /// perform the full slow-path [`Self::begin_park`] CAS, so the
    /// transition `EMPTY → PARKED_<mode>` (and the racing-unparker
    /// short-circuit) remain atomic.
    ///
    /// Race-safe because only the owning worker thread calls this
    /// method: an unparker can only `swap(NOTIFIED)` and observe
    /// `prev`; if it observes `prev = NOTIFIED` it does not issue
    /// a kernel wake (idempotent). If we observe `cur = NOTIFIED`
    /// here and store `EMPTY`, any unparker swap from `NOTIFIED`
    /// or `EMPTY` after our store re-arms a fresh `NOTIFIED` for
    /// the next park (correctly).
    pub(crate) fn try_consume_notified(&self, worker_idx: usize) -> bool {
        use super::lazy_debug::{bump, COUNTERS};
        let slot = &self.workers[worker_idx];
        // CAS so we don't race with concurrent unparkers that may
        // be transitioning EMPTY ↔ NOTIFIED. Failure leaves state
        // untouched; success consumes exactly one notification.
        if slot
            .park_state
            .compare_exchange(NOTIFIED, EMPTY, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            bump(&COUNTERS.begin_park_calls);
            bump(&COUNTERS.begin_park_fastpath);
            true
        } else {
            false
        }
    }

    /// Enter the pre-park spin window: CAS `EMPTY → SEARCHING`. After
    /// this call returns `Ok`, the worker is bookkept as "about to
    /// park" — a concurrent [`Self::unpark`] swap observes
    /// `prev == SEARCHING` and skips the kernel wake; the parker is
    /// expected to either notice the resulting `NOTIFIED` from inside
    /// its spin loop or fail its [`Self::commit_park`] CAS.
    ///
    /// Returns `Err(())` if a notification was already pending on
    /// entry (race against an unpark from `EMPTY`); the call clears
    /// `park_state` back to `EMPTY` and the caller should return
    /// without parking.
    ///
    /// Replaces the old `begin_park(EMPTY → PARKED_<mode>)` CAS,
    /// splitting the transition into two steps so the unpark path can
    /// observe a parker that has committed to wake handling but not
    /// yet to a syscall.
    pub(crate) fn begin_searching(&self, worker_idx: usize) -> Result<(), ()> {
        use super::lazy_debug::{bump, bump_per_worker, COUNTERS, PER_WORKER};
        bump(&COUNTERS.begin_park_calls);
        bump_per_worker(&PER_WORKER.begin_park_calls, worker_idx);
        let slot = &self.workers[worker_idx];
        match slot.park_state.compare_exchange(
            EMPTY,
            SEARCHING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(NOTIFIED) => {
                bump(&COUNTERS.begin_park_fastpath);
                slot.park_state.store(EMPTY, Ordering::Release);
                Err(())
            }
            Err(state) => panic!("inconsistent park_state on begin_searching: {state}"),
        }
    }

    /// Direct one-step park CAS: `EMPTY → PARKED_<mode>`. Used by the
    /// no-spin substrate path (`spin_budget == 0`) so workloads that
    /// opt out of pre-park spin do not pay the two-CAS
    /// (`EMPTY → SEARCHING → PARKED_*`) transition cost.
    ///
    /// Returns `Err(())` if a notification was already pending on
    /// entry (`park_state == NOTIFIED`); the call clears state back
    /// to `EMPTY` and the caller should return without parking.
    ///
    /// Cross-worker `unpark` correctness is unaffected: the unpark
    /// path's `swap(NOTIFIED)` produces `prev == PARKED_<mode>` (kernel
    /// wake) or `prev == EMPTY` (no-op) the same way it did before
    /// SEARCHING was introduced.
    pub(crate) fn begin_park_direct(
        &self,
        worker_idx: usize,
        mode: ParkMode,
    ) -> Result<(), ()> {
        use super::lazy_debug::{bump, bump_per_worker, COUNTERS, PER_WORKER};
        bump(&COUNTERS.begin_park_calls);
        bump_per_worker(&PER_WORKER.begin_park_calls, worker_idx);
        let slot = &self.workers[worker_idx];
        let parked_state = mode.as_state();
        match slot.park_state.compare_exchange(
            EMPTY,
            parked_state,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                bump(&COUNTERS.begin_park_parked);
                Ok(())
            }
            Err(NOTIFIED) => {
                bump(&COUNTERS.begin_park_fastpath);
                slot.park_state.store(EMPTY, Ordering::Release);
                Err(())
            }
            Err(state) => {
                panic!("inconsistent park_state on begin_park_direct: {state}")
            }
        }
    }

    /// Commit to a kernel park: CAS `SEARCHING → PARKED_<mode>`. The
    /// caller must already hold the SEARCHING token via a successful
    /// [`Self::begin_searching`].
    ///
    /// Returns `Err(())` if the parker was yanked during the spin
    /// window (state observed `NOTIFIED`); the call clears
    /// `park_state` back to `EMPTY` and the caller should return
    /// without entering the kernel.
    pub(crate) fn commit_park(&self, worker_idx: usize, mode: ParkMode) -> Result<(), ()> {
        use super::lazy_debug::{bump, COUNTERS};
        let slot = &self.workers[worker_idx];
        let parked_state = mode.as_state();
        match slot.park_state.compare_exchange(
            SEARCHING,
            parked_state,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                bump(&COUNTERS.begin_park_parked);
                Ok(())
            }
            Err(NOTIFIED) => {
                bump(&COUNTERS.commit_park_yanked);
                slot.park_state.store(EMPTY, Ordering::Release);
                Err(())
            }
            Err(state) => panic!("inconsistent park_state on commit_park: {state}"),
        }
    }

    /// Has this worker ever held a successful I/O registration?
    ///
    /// Permanent latch (`AtomicBool`, set-once). `false` means the
    /// worker has never had a `ScheduledIo` registered against its
    /// child epoll. Used to gate the meta-watcher CAS: a worker with
    /// no I/O of its own and no peers with I/O has nothing to drain
    /// from the meta epoll, so we skip the global CAS contention on
    /// pure-sync workloads. `true` means at least one registration
    /// has landed.
    ///
    /// Acquire-load pairs with the Release-store in
    /// `register_on_worker`'s success tail, so observing `true`
    /// implies the registration is fully published into the slab and
    /// epoll interest set.
    #[inline]
    pub(crate) fn worker_has_io_registered(&self, worker_idx: usize) -> bool {
        self.workers[worker_idx]
            .has_io_registered
            .load(Ordering::Acquire)
    }

    /// Snapshot the current `park_state` for the spin loop. Relaxed
    /// because the only consumer is the owning worker on its own
    /// slot — the cross-worker writer (`unpark`'s `swap`) provides
    /// release ordering, paired with this load's acquire ordering on
    /// observed transitions to `NOTIFIED` (handled in the caller's
    /// CAS sequence). For pure spin checks, relaxed is sufficient
    /// because the next CAS will re-validate.
    #[inline]
    pub(crate) fn park_state_load(&self, worker_idx: usize) -> u32 {
        self.workers[worker_idx]
            .park_state
            .load(Ordering::Relaxed)
    }

    /// Abort the current spin window without entering the kernel:
    /// store `EMPTY`. Used when the parker observes a `NOTIFIED` from
    /// inside the spin loop (or any other reason to bail before
    /// committing to a kernel park). Idempotent w.r.t. a subsequent
    /// `unpark` because the unpark swap will simply re-arm
    /// `NOTIFIED` for the next park entry.
    #[inline]
    pub(crate) fn abort_searching(&self, worker_idx: usize) {
        self.workers[worker_idx]
            .park_state
            .store(EMPTY, Ordering::Release);
    }

    /// Called by the worker on park completion. Clears any notification
    /// that was consumed by the wake.
    pub(crate) fn end_park(&self, worker_idx: usize) {
        let slot = &self.workers[worker_idx];
        slot.park_state.store(EMPTY, Ordering::Release);
    }

    /// Allocate a [`ScheduledIo`] for a new fd without doing any
    /// driver-side work yet. The returned `Arc<ScheduledIo>` is owned
    /// by the caller's [`Registration`][reg]; the driver only learns
    /// about it on the first
    /// [`register_local`](Self::register_local) call.
    ///
    /// [reg]: super::registration::Registration
    pub(crate) fn allocate_scheduled_io(&self) -> Arc<ScheduledIo> {
        Arc::new(ScheduledIo::default())
    }

    /// Lazy register `(shared, fd, interest)` with the sharded-mio
    /// reactor. Called from
    /// [`Registration::ensure_registered`][reg-er] on the first poll
    /// of a registration.
    ///
    /// Synchronous on any thread. `mio::Registry` clones are
    /// `Send + Sync`, so the calling thread can drive the owner-side
    /// mio register directly without bouncing through the owner's
    /// park loop. The single per-shard decision is which worker owns
    /// the slab entry:
    ///
    /// * **On a worker:** the calling worker is the owner, picked via
    ///   [`current_worker_index`][cwi]. This is the common path —
    ///   `from_std` / `TcpListener::accept` on a worker triggers first
    ///   poll on that same worker, which keeps cache-warm dispatch
    ///   on the same core.
    /// * **Off-runtime:** [`fallback_worker`] picks an owner via
    ///   round-robin.
    ///
    /// **Throughput note.** The fast path concentrates fds on
    /// whichever worker happens to call `from_std`. In
    /// `tcp_connect_churn` (single accept-loop worker, many
    /// short-lived connections) this regresses throughput because
    /// the accepting worker becomes a single dispatch hot spot.
    /// Distributed-load workloads — where each worker accepts/spawns
    /// its own connections — benefit. See `docs/io-driver-vtable.md`
    /// for the bench results and the artifact discussion.
    ///
    /// Returns the worker index this registration is now bound to —
    /// the same index that ends up stored in
    /// `shared.sharded_mio_worker`.
    ///
    /// [reg-er]: super::registration::Registration::ensure_registered
    /// [cwi]: crate::runtime::scheduler::multi_thread::sharded_mio_park::current_worker_index
    pub(crate) fn register_local(
        &self,
        shared: &Arc<ScheduledIo>,
        fd: RawFd,
        interest: Interest,
    ) -> io::Result<usize> {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.register_local_calls);

        // Sync fast path: when the calling thread is itself a
        // sharded-mio worker, apply the registration inline on its
        // own `SharedRegistry`. The lazy-from-anywhere panic boundary
        // (no runtime in TLS) was already cleared by
        // `Registration::ensure_registered` calling
        // `Handle::current()`, so reaching this point with a worker
        // index always means we're on a live sharded-mio worker
        // belonging to the same runtime as `self`.
        if let Some(worker_idx) =
            crate::runtime::scheduler::multi_thread::sharded_mio_park::current_worker_index()
        {
            if worker_idx < self.workers.len() {
                return self.register_on_worker(worker_idx, shared, fd, interest);
            }
        }

        // Off-runtime path: pick a fallback owner round-robin.
        self.register_on_worker(self.fallback_worker(), shared, fd, interest)
    }

    /// Same-worker sync register. Mutates the owning worker's
    /// `RegistrationSet` and `SharedRegistry` directly. Caller must
    /// have established that `worker_idx` is the index of the
    /// currently-executing worker.
    fn register_on_worker(
        &self,
        worker_idx: usize,
        shared: &Arc<ScheduledIo>,
        fd: RawFd,
        interest: Interest,
    ) -> io::Result<usize> {
        use super::lazy_debug::{bump, COUNTERS};
        bump(&COUNTERS.register_on_worker_calls);

        // Publish the worker assignment first so a racing
        // `queue_deregister` (e.g. from a foreign Drop that just
        // received a clone of `shared`) routes to this same worker's
        // slab. Both calls run synchronously on their respective
        // threads; the slot's mutex serialises the Register-vs-
        // Deregister order on the slab side.
        shared
            .sharded_mio_worker
            .store(worker_idx as u32, Ordering::Relaxed);

        let slot = &self.workers[worker_idx];

        // Track in the per-shard set first.
        if slot
            .registrations
            .allocate_existing(&mut slot.synced.lock(), shared)
            .is_err()
        {
            bump(&COUNTERS.register_on_worker_shutdown);
            // Driver shutting down — surface a shutdown event so the
            // caller's first-poll waiter doesn't hang.
            shared.shutdown();
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "io driver shutting down",
            ));
        }

        let registry = match slot.shared_registry.get() {
            Some(r) => r,
            None => {
                bump(&COUNTERS.register_on_worker_no_registry);
                // Worker hasn't published its registry yet (shouldn't
                // happen post-startup; the start barrier guarantees
                // every worker publishes before any task runs). Be
                // safe: roll back the set insert and report.
                // SAFETY: `shared` was just inserted by
                // `allocate_existing`.
                unsafe {
                    slot.registrations
                        .remove(&mut slot.synced.lock(), shared);
                }
                shared.shutdown();
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "sharded-mio worker registry not yet published",
                ));
            }
        };

        let mut source = mio::unix::SourceFd(&fd);
        let ok = match registry.register(&mut source, fd, interest, shared) {
            Ok(ok) => ok,
            Err(e) => {
                bump(&COUNTERS.register_on_worker_errors);
                // SAFETY: just inserted by `allocate_existing` above.
                unsafe {
                    slot.registrations
                        .remove(&mut slot.synced.lock(), shared);
                }
                shared.shutdown();
                return Err(e);
            }
        };

        bump(&COUNTERS.register_on_worker_ok);
        super::lazy_debug::bump_per_worker(
            &super::lazy_debug::PER_WORKER.register_per_worker,
            worker_idx,
        );
        shared
            .sharded_mio_slab_key
            .store(ok.slab_key, Ordering::Relaxed);
        // Permanent latch: this worker has now touched I/O, so all
        // future parks on it must use the epoll path (events fire
        // through the child epoll, not through `park_state`).
        // Release-store pairs with the Acquire-load on the parker side
        // (`ShardedMioHandle::worker_has_io_registered`).
        slot.has_io_registered.store(true, Ordering::Release);
        self.metrics.incr_fd_count();
        Ok(worker_idx)
    }

    fn fallback_worker(&self) -> usize {
        self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len()
    }

    /// Synchronously deregister `shared` from the sharded-mio reactor.
    ///
    /// Despite the historical "queue_" name, this call applies the
    /// deregistration directly on the calling thread:
    /// `mio::Registry` clones are `Send + Sync`, so there is no need
    /// to bounce through the owning worker's park loop. The owner
    /// identity (loaded from `shared.sharded_mio_worker`) identifies
    /// which shard's slab the entry lives in and which worker's child
    /// epoll holds the kernel-side registration.
    ///
    /// `fd` must be the same fd that was passed to
    /// [`Self::register_local`]; the caller (typically
    /// `Registration::deregister`) has it via
    /// `RegistrationSource::registration_raw_fd()`.
    pub(crate) fn queue_deregister(&self, shared: &Arc<ScheduledIo>, fd: RawFd) {
        use super::lazy_debug::{bump, maybe_dump_trial, COUNTERS};
        bump(&COUNTERS.queue_deregister_calls);
        // Per-trial delta hook (no-op unless `TOKIO_LAZY_DEBUG_TRIAL`
        // is set). Placed here because `io_busy_owner` deregisters
        // exactly once per probe task at end-of-trial — modulo the
        // configured `N` this gives one delta dump per bench iter.
        bump(&COUNTERS.apply_deregister_calls);
        maybe_dump_trial();
        let worker_idx = shared.sharded_mio_worker.load(Ordering::Relaxed) as usize;
        if worker_idx >= self.workers.len() {
            bump(&COUNTERS.queue_deregister_no_worker);
            // Either never registered (unlikely — caller should have
            // checked) or the field is the sentinel u32::MAX. Nothing
            // to deregister.
            return;
        }
        let slot = &self.workers[worker_idx];
        let slab_key = shared.sharded_mio_slab_key.load(Ordering::Relaxed);
        let gen = shared.sharded_mio_gen.load(Ordering::Relaxed);

        if slab_key != u32::MAX {
            if let Some(registry) = slot.shared_registry.get() {
                let mut source = mio::unix::SourceFd(&fd);
                match registry.deregister(&mut source, fd, slab_key, gen) {
                    DeregisterOutcome::Applied => {
                        self.metrics.dec_fd_count();
                    }
                    DeregisterOutcome::SkippedFdReused
                    | DeregisterOutcome::SkippedSlotReassigned => {
                        // The kernel-side registration we'd otherwise
                        // wipe with `epoll_ctl_del(fd)` belongs to a
                        // fresher registration. Skip; the kernel
                        // already auto-removed our original record
                        // when our fd was closed.
                        bump(&COUNTERS.apply_deregister_gen_mismatch);
                    }
                }
            }
        } else {
            bump(&COUNTERS.apply_deregister_no_key);
        }

        // Mark the registration for release. The per-shard
        // pending_release vec is drained on the owning worker's next
        // park via the `RegistrationSet::release` path inside
        // `RegistrationSet::deregister` itself once the threshold is
        // hit. Best-effort flush here too if needed.
        let _ = slot
            .registrations
            .deregister(&mut slot.synced.lock(), shared);
        if slot.registrations.needs_release() {
            slot.registrations.release(&mut slot.synced.lock());
        }
    }
}

// ===== LOCAL_REACTOR thread-local =====
//
// For the sharded-mio path this TLS is wired for symmetry with the
// uring reactor — the probe `local_reactor_installed()` is useful if we
// ever want to branch "am I on a sharded-mio worker?" the way
// `TcpStream::uring_send` branches on the uring TLS. Today nothing
// actually needs the reactor pointer (cross-worker add_source uses the
// `SharedRegistry` handle directly, no LOCAL_REACTOR dereference
// required). We keep the install/clear pair anyway so future work that
// wants to reach the local reactor has a uniform hook.

thread_local! {
    static LOCAL_REACTOR: Cell<*const RefCell<Reactor>> =
        const { Cell::new(ptr::null()) };
}

/// Install `ptr` as this thread's local reactor. See
/// [`uring_driver::install_local_reactor_raw`] for the safety contract.
///
/// [`uring_driver::install_local_reactor_raw`]: super::uring_driver::install_local_reactor_raw
pub(crate) unsafe fn install_local_reactor_raw(ptr: *const RefCell<Reactor>) {
    LOCAL_REACTOR.with(|slot| {
        let existing = slot.get();
        if !existing.is_null() {
            let tname = std::thread::current()
                .name()
                .unwrap_or("<unnamed>")
                .to_owned();
            panic!(
                "another sharded-mio Reactor is already installed on thread {tname:?}: \
                 existing={existing:p} new={ptr:p}",
            );
        }
        slot.set(ptr);
    });
}

/// Clear this thread's `LOCAL_REACTOR` slot. Idempotent.
pub(crate) fn clear_local_reactor() {
    LOCAL_REACTOR.with(|slot| slot.set(ptr::null()));
}

/// Cheap probe: is a sharded-mio `Reactor` currently installed on this
/// thread?
#[allow(dead_code)]
pub(crate) fn local_reactor_installed() -> bool {
    LOCAL_REACTOR.with(|slot| !slot.get().is_null())
}

// ===== LOCAL_HANDLE thread-local =====
//
// Per-thread `*const ShardedMioHandle` slot, installed by the parker
// alongside `LOCAL_REACTOR` and cleared on shutdown / Drop. The handle
// lets any thread holding a registration on a sharded-mio
// `ScheduledIo` reach back into the runtime's worker set without
// threading the `Arc` through the I/O API surface.
//
// Originally consumed by the "interested workers" stake-tracking spike
// (peer wake fan-out via `interested_workers: AtomicU64` on each
// `WorkerState`); the bitset substrate has been removed (see
// INVESTIGATION-sharded-mio-perf.md, "Patch applied + WorkerState
// audit") because it produced false-sharing on the owner's hot park
// line and was never wired to a consumer. The TLS slot itself is kept
// because `install_local_handle_raw` / `clear_local_handle` callers
// remain in the parker's lifecycle; if a future meta-watcher fan-out
// implementation needs cross-worker handle access, it can read this
// slot via [`with_local_handle`].
//
// The slot stores a raw `*const ShardedMioHandle`, not an `Arc`, to keep
// the install/clear path zero-allocation. Lifetime is bounded by the
// parker that installs it: the parker holds an `Arc<ShardedMioHandle>`
// for as long as it is live, and clears the TLS slot before dropping
// that `Arc`.

thread_local! {
    static LOCAL_HANDLE: Cell<*const ShardedMioHandle> =
        const { Cell::new(ptr::null()) };
}

/// Install `ptr` as this thread's local sharded-mio handle.
///
/// # Safety
///
/// `ptr` must remain valid until [`clear_local_handle`] is called on the
/// same thread. The caller (typically [`ShardedMioParker`]) guarantees
/// this by holding an `Arc<ShardedMioHandle>` for the duration of the
/// installation.
pub(crate) unsafe fn install_local_handle_raw(ptr: *const ShardedMioHandle) {
    LOCAL_HANDLE.with(|slot| {
        let existing = slot.get();
        if !existing.is_null() {
            let tname = std::thread::current()
                .name()
                .unwrap_or("<unnamed>")
                .to_owned();
            panic!(
                "another sharded-mio handle is already installed on thread {tname:?}: \
                 existing={existing:p} new={ptr:p}",
            );
        }
        slot.set(ptr);
    });
}

/// Clear this thread's `LOCAL_HANDLE` slot. Idempotent.
pub(crate) fn clear_local_handle() {
    LOCAL_HANDLE.with(|slot| slot.set(ptr::null()));
}

/// Run `f` against this thread's installed sharded-mio handle, if any.
/// Returns `None` off-runtime (no handle installed) or if the polling
/// thread predates handle install (shouldn't happen in steady state but
/// the bench TLS slot is empty before `eager_init_and_sync`).
#[inline]
pub(crate) fn with_local_handle<R>(f: impl FnOnce(&ShardedMioHandle) -> R) -> Option<R> {
    LOCAL_HANDLE.with(|slot| {
        let p = slot.get();
        if p.is_null() {
            None
        } else {
            // SAFETY: The pointer was installed via
            // `install_local_handle_raw` and remains valid until
            // `clear_local_handle` is called by the same parker, which
            // happens only after dropping all task work on this thread.
            // We do not store the reference past this scope.
            Some(f(unsafe { &*p }))
        }
    })
}
