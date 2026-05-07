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
#[cfg(target_os = "linux")]
use std::sync::atomic::AtomicU8;
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
/// entering the blocking syscall (via
/// [`ShardedMioHandle::begin_park_direct`] taking a [`ParkMode`]); the
/// unpark path single-matches on `prev` and only issues the wake
/// mechanism that is load-bearing for the branch the parker is in.
/// See the unpark module docs for the full race argument.
// `park_state` is an `AtomicU32`. The state values trivially fit
// in `u32` (max value is `NOTIFIED = 3`).
pub(crate) const EMPTY: u32 = 0;
pub(crate) const PARKED_OWN: u32 = 1;
pub(crate) const PARKED_META: u32 = 2;
pub(crate) const NOTIFIED: u32 = 3;
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
    /// `park_on_meta` — worker blocks in `epoll_wait` on its
    /// chiplet group's meta epoll. Wake by writing that group's
    /// `meta_waker` eventfd (registered on the group's meta epoll
    /// with `META_WAKER_TOKEN`). The carried `u8` is the group
    /// index, threaded through to `park_on_meta` and `unpark`.
    #[cfg(target_os = "linux")]
    Meta(u8),
}

impl ParkMode {
    #[inline]
    fn as_state(self) -> u32 {
        match self {
            ParkMode::OwnChild => PARKED_OWN,
            #[cfg(target_os = "linux")]
            ParkMode::Meta(_) => PARKED_META,
        }
    }
}

/// `epoll_event.u64` token used to identify the meta-waker eventfd
/// when it fires on the meta epoll. Children carry their `worker_idx`
/// as their u64; this sentinel is well outside any plausible worker
/// index (`MAX_WORKERS` is 128 on this branch, capped by
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

/// One chiplet-local meta-watcher group. The runtime-wide
/// `meta_epfd` + `meta_watcher_busy` + `meta_waker` triple was
/// replaced with an array of these to localise the meta-watcher CAS
/// line and the kernel-side fan-in to a CCX-aligned slice of workers.
/// See [`ShardedMioHandle`] field docs for the partitioning rationale
/// and topology source.
///
/// Each group owns one `epoll_create1` fd, one `eventfd`, and one
/// `AtomicBool`. Group count `G` is determined by [`probe_chiplet_groups`]
/// at handle construction. Workers are assigned to a group at
/// first-park (best-effort, kernel-migration-tolerant) by reading
/// `sched_getcpu()` and looking up the L3-shared CCX it belongs to.
#[cfg(target_os = "linux")]
struct ChipletGroup {
    /// Per-group meta epoll fd. Only this group's worker child
    /// epolls + this group's `meta_waker` are registered on it.
    /// Closed in [`Drop`].
    meta_epfd: RawFd,

    /// Per-group meta-watcher gate. At most one worker per group
    /// blocks in `epoll_wait(meta_epfd)` at any moment. Replaces the
    /// runtime-wide `AtomicBool` so the CAS line is partitioned `G`
    /// ways instead of one — this is the targeted fix for the
    /// `tcp_connect_churn` regression at lourip W=64/128 where the
    /// global CAS bounced across 8 CCDs.
    meta_watcher_busy: AtomicBool,

    /// Per-group meta-waker eventfd, registered on `meta_epfd` with
    /// sentinel `META_WAKER_TOKEN`. The unpark path writes this fd to
    /// wake a worker currently parked in `ParkMode::Meta` for *this*
    /// group; cross-group unparks use the per-worker `external_waker`
    /// path (`PARKED_OWN`) — there is no global meta-waker any more.
    meta_waker: MetaWaker,

    /// Live count of workers that have first-parked into this group.
    /// Read by the gate to skip the meta CAS when the group has only
    /// the calling worker as a member (no fan-out target). Workers
    /// only ever join, never leave (group assignment is permanent
    /// for the runtime's life).
    member_count: AtomicU8,
}

#[cfg(target_os = "linux")]
impl ChipletGroup {
    fn new() -> io::Result<Self> {
        // SAFETY: passing well-defined libc flag constants to
        // `epoll_create1`; no caller preconditions.
        let meta_epfd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if meta_epfd < 0 {
            return Err(io::Error::last_os_error());
        }
        let meta_waker = match MetaWaker::new() {
            Ok(mw) => mw,
            Err(err) => {
                // SAFETY: `meta_epfd` was just created above and not
                // shared; closing on the error path before returning.
                unsafe { libc::close(meta_epfd) };
                return Err(err);
            }
        };
        let mut ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: META_WAKER_TOKEN,
        };
        // SAFETY: `meta_epfd` is freshly created and unshared;
        // `meta_waker.fd()` is owned by `meta_waker` for the duration
        // of this group; `&mut ev` is a stack value the kernel reads
        // but does not retain.
        let r = unsafe {
            libc::epoll_ctl(
                meta_epfd,
                libc::EPOLL_CTL_ADD,
                meta_waker.fd(),
                &mut ev,
            )
        };
        if r != 0 {
            let err = io::Error::last_os_error();
            // SAFETY: `meta_epfd` is owned by this scope and not yet
            // moved into a `ChipletGroup`; close before the error
            // returns. `meta_waker` drops normally and closes its fd.
            unsafe { libc::close(meta_epfd) };
            return Err(err);
        }
        Ok(Self {
            meta_epfd,
            meta_watcher_busy: AtomicBool::new(false),
            meta_waker,
            member_count: AtomicU8::new(0),
        })
    }

    /// Register a worker's child epoll fd onto this group's
    /// `meta_epfd` as level-triggered `EPOLLIN`, with `worker_idx`
    /// stamped into `epoll_event.data.u64` (matching the watcher's
    /// dispatch in `park_on_meta`). Called once per worker on its
    /// first park, after the worker has been assigned to this group.
    fn add_child(&self, child_epfd: RawFd, worker_idx: u64) -> io::Result<()> {
        let mut ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: worker_idx,
        };
        // SAFETY: `self.meta_epfd` is owned by `self` and stays open
        // until `Drop`; `child_epfd` is owned by the worker's
        // `SharedRegistry`, which outlives this handle (worker
        // reactor is dropped last on shutdown); `&mut ev` is a stack
        // value the kernel reads but does not retain.
        let r = unsafe {
            libc::epoll_ctl(
                self.meta_epfd,
                libc::EPOLL_CTL_ADD,
                child_epfd,
                &mut ev,
            )
        };
        if r != 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for ChipletGroup {
    fn drop(&mut self) {
        if self.meta_epfd >= 0 {
            // SAFETY: `meta_epfd` was created by `epoll_create1` in
            // `Self::new` and not closed elsewhere; `Drop` runs at
            // most once. Children remain registered until their
            // owning `SharedRegistry` is dropped a moment later as
            // part of runtime teardown — the kernel removes the
            // meta-side records automatically when the meta fd
            // closes. `meta_waker` drops afterwards via field-drop
            // order and closes its eventfd.
            unsafe { libc::close(self.meta_epfd) };
        }
    }
}

/// Probe `/sys/devices/system/cpu/cpuN/cache/index3/shared_cpu_list`
/// to build a CPU → group-index lookup table aligned to L3 cache
/// boundaries. On Zen, the L3 cache is the CCX boundary, so an
/// L3-aligned partition is a CCX-aligned partition.
///
/// **Why not `cluster_id`?** The canonical kernel-exposed CCX source
/// would be `/sys/devices/system/cpu/cpuN/topology/cluster_id`, but on
/// the kernels we run (and the Zen 2 / 3 hardware in this fleet) it
/// reads as `65535` (= unset / -1) for every CPU. `index3/shared_cpu_list`
/// IS populated and gives the same partition (one L3 == one CCX on
/// Zen 2, e.g. 4C/8T per CCX on EPYC 7H12 lourip; 2C/4T per CCX on
/// the partial-CCX EPYC 7302 lounas).
///
/// Returns `(cpu_to_group, group_count)`:
/// - `cpu_to_group[cpu_id] = group_idx` for every CPU we successfully
///   read; `u8::MAX` for CPUs whose sysfs entry was missing or
///   unparseable (callers must default to group 0 for those).
/// - `group_count >= 1` always; falls back to `1` (single-group)
///   when every read failed (e.g. sysfs missing in a sandbox).
///
/// Group index is `u8` because no realistic system has more than
/// 255 distinct L3 caches; the largest current AMD parts are
/// dual-socket Genoa with ~24 L3s, well within `u8`.
#[cfg(target_os = "linux")]
fn probe_chiplet_groups() -> (Box<[u8]>, u8) {
    // SAFETY: `sysconf` is async-signal-safe and takes a single
    // well-defined integer constant; no caller preconditions.
    let max_cpu = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_CONF) };
    let max_cpu = if max_cpu <= 0 { 1usize } else { max_cpu as usize };

    let mut cpu_to_group = vec![u8::MAX; max_cpu];
    let mut next_group: u8 = 0;

    for cpu in 0..max_cpu {
        if cpu_to_group[cpu] != u8::MAX {
            continue; // already assigned via a peer CPU's L3 list
        }
        let path = format!(
            "/sys/devices/system/cpu/cpu{cpu}/cache/index3/shared_cpu_list"
        );
        match std::fs::read_to_string(&path) {
            Ok(s) => {
                if next_group == u8::MAX {
                    // Saturated (>= 255 L3 groups). Should be
                    // impossible on real hardware. Bucket every
                    // remaining CPU into the last available group.
                    cpu_to_group[cpu] = u8::MAX - 1;
                    continue;
                }
                let g = next_group;
                next_group = next_group.saturating_add(1);
                for peer in parse_cpu_list(&s) {
                    if peer < cpu_to_group.len()
                        && cpu_to_group[peer] == u8::MAX
                    {
                        cpu_to_group[peer] = g;
                    }
                }
            }
            Err(_) => {
                // sysfs missing for this CPU (offline, or not exposed
                // in a sandbox). Give it its own group so it isn't
                // silently funnelled with an arbitrary peer.
                if next_group != u8::MAX {
                    cpu_to_group[cpu] = next_group;
                    next_group = next_group.saturating_add(1);
                } else {
                    cpu_to_group[cpu] = u8::MAX - 1;
                }
            }
        }
    }

    let group_count = if next_group == 0 {
        // Every read failed. Single-group fallback: every CPU -> 0.
        // Behaviour collapses to the pre-sharding runtime-wide
        // meta-watcher.
        for g in cpu_to_group.iter_mut() {
            *g = 0;
        }
        1
    } else {
        next_group
    };

    (cpu_to_group.into_boxed_slice(), group_count)
}

/// Parse a `/sys` cpu-list (e.g. `"0-3,64-67"`, `"7"`) into the
/// concrete CPU IDs it covers. Tolerant of malformed input — returns
/// the empty iterator on parse failures rather than panicking; a
/// stray sysfs read on an unfamiliar kernel must not bring the
/// runtime down.
#[cfg(target_os = "linux")]
fn parse_cpu_list(s: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for tok in s.trim().split(',') {
        let tok = tok.trim();
        if tok.is_empty() {
            continue;
        }
        if let Some((lo, hi)) = tok.split_once('-') {
            let lo: usize = match lo.trim().parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let hi: usize = match hi.trim().parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            if lo <= hi {
                for c in lo..=hi {
                    out.push(c);
                }
            }
        } else if let Ok(c) = tok.parse() {
            out.push(c);
        }
    }
    out
}

/// RAII handle for a per-group meta-watcher slot. Constructed by
/// [`ShardedMioHandle::try_acquire_meta_watcher`]; releases this
/// group's `meta_watcher_busy` on drop so the next idle worker in
/// the same group can take over.
///
/// Holds an `Arc<ShardedMioHandle>` rather than borrowing it so the
/// guard is 'static — required to pass it across `&mut self` method
/// boundaries inside the parker without tripping the borrow checker.
/// The clone is one atomic-inc, dwarfed by the syscall the guard
/// protects.
#[cfg(target_os = "linux")]
#[allow(dead_code)]
pub(crate) struct MetaWatcherGuard {
    handle: Arc<ShardedMioHandle>,
    /// Index into `handle.groups`; identifies whose `meta_watcher_busy`
    /// the drop path must clear.
    group_idx: u8,
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
            self.handle.groups[self.group_idx as usize]
                .meta_watcher_busy
                .store(false, Ordering::Release);
            self.released = true;
        }
    }

    /// Group index this guard owns the watcher slot of. Used by the
    /// parker's `park_on_meta` to dispatch to the correct meta epfd.
    #[allow(dead_code)]
    pub(crate) fn group_idx(&self) -> u8 {
        self.group_idx
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

    /// Live count of `ScheduledIo`s currently registered on this
    /// worker's child epoll. Incremented on the success tail of
    /// `register_on_worker`, decremented on the matching arm of
    /// `queue_deregister`. Used to gate the meta-watcher CAS so
    /// pure-sync workloads — and workers that *used to* hold I/O
    /// but no longer do — skip the global CAS contention on
    /// `meta_watcher_busy` per park.
    ///
    /// Replaces an earlier sticky `AtomicBool` latch (set-once, never
    /// reset). The sticky version was conservative-correct but kept
    /// workers paying the meta-watcher CAS + extra `epoll_wait` per
    /// park indefinitely after they finished their last I/O — wasted
    /// work for workloads that move I/O between workers over the
    /// runtime's lifetime.
    ///
    /// The gate is purely advisory: the real synchronization with
    /// peer parkers is the `meta_watcher_busy` CAS itself. Stale
    /// reads of this counter cannot cause incorrect behaviour, only
    /// a wasted park-mode decision (one extra meta cycle if read
    /// stale-non-zero, or one missed meta-cycle if read stale-zero —
    /// the next park rechecks).
    pub(crate) registered_count: AtomicUsize,

    /// Index into [`ShardedMioHandle::groups`] this worker is
    /// assigned to, or `u8::MAX` while still unassigned.
    ///
    /// **Lazy first-park assignment.** Workers are assigned at their
    /// first park entry by reading `libc::sched_getcpu()` and looking
    /// up the L3-sharing CCX of that CPU in
    /// [`ShardedMioHandle::cpu_to_group`]. Tokio doesn't pin workers,
    /// so a kernel migration after first-park leaves the worker's
    /// `group_idx` pointing at its *initial* CCX — best-effort
    /// routing, never wrong (the meta-watcher's drain locality just
    /// becomes imperfect, not unsound).
    ///
    /// Synchronisation: written exactly once with `Release` ordering
    /// by the owning worker thread before the first park enters
    /// `try_acquire_meta_watcher` (which can only succeed for
    /// `group_idx != u8::MAX`). Read with `Acquire` by the unpark path
    /// when routing a `PARKED_META` wake to the right group's
    /// `meta_waker`. Read with `Relaxed` on the owner's own park path
    /// (no cross-thread visibility concern — the owner wrote it).
    ///
    /// Invariant: by the time `park_state` becomes `PARKED_META`,
    /// `group_idx` has been published. Cross-thread `unpark` observers
    /// of `PARKED_META` are therefore guaranteed to see a valid group
    /// index.
    #[cfg(target_os = "linux")]
    pub(crate) group_idx: AtomicU8,
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
            registered_count: AtomicUsize::new(0),
            #[cfg(target_os = "linux")]
            group_idx: AtomicU8::new(u8::MAX),
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

    /// Per-chiplet meta-watcher groups. Replaces the runtime-wide
    /// `meta_epfd` + `meta_watcher_busy` + `meta_waker` triple with
    /// an array of [`ChipletGroup`]s, each owning its own
    /// `meta_epfd`, `meta_watcher_busy` CAS line, and `meta_waker`
    /// eventfd. Group count `G` is the number of distinct L3 caches
    /// reported by `/sys` (= number of CCXs on Zen) at runtime
    /// build; on lourip (EPYC 7H12) that's 16 groups of 8 logical
    /// CPUs, on lounas (EPYC 7302) 8 groups of 4. Single-group
    /// fallback when sysfs reads fail.
    ///
    /// Each worker is assigned to exactly one group at first-park
    /// (see [`WorkerState::group_idx`]). Cross-group I/O wakes ride
    /// the existing per-worker `external_waker` path
    /// (`PARKED_OWN`) — there is no global meta-waker any more,
    /// and there is no cross-group fan-out bitset (the
    /// `interested_workers: AtomicU64` substrate that would have
    /// supported it was removed in `9eb7bed9` for false-sharing on
    /// the owner's park line and is *not* reintroduced here).
    ///
    /// # Motivation
    ///
    /// Without per-group partitioning, every idle worker's
    /// `try_acquire_meta_watcher` CAS targets a single
    /// runtime-wide `AtomicBool`. At W=64/128 on lourip the
    /// register/deregister-heavy `tcp_connect_churn` benchmark
    /// regressed +2.35% / +5.20% after the live-counter gate
    /// refinement (`74189589`) — the suspected cause was the global
    /// CAS line bouncing across CCDs of fabric on every park entry.
    /// Splitting the CAS line `G` ways localises the contention to
    /// CCX-local cache traffic.
    ///
    /// `EPOLLEXCLUSIVE` would have provided kernel-side fan-in but
    /// `epoll_ctl(2)` rejects it with `EINVAL` when the target fd is
    /// itself an epoll instance, which is the meta-of-children shape
    /// we use. So the gate lives in userspace.
    ///
    /// [`epoll_ctl(2)`]: https://man7.org/linux/man-pages/man2/epoll_ctl.2.html
    #[cfg(target_os = "linux")]
    groups: Box<[ChipletGroup]>,

    /// CPU-id → group-index lookup table built by
    /// [`probe_chiplet_groups`] at handle construction. Indexed by
    /// the result of `libc::sched_getcpu()` on a worker's first park
    /// to assign that worker to its CCX-local group. `u8::MAX` for
    /// CPUs whose sysfs entry was missing or unparseable; callers
    /// default to group 0 in that case.
    ///
    /// Stable for the lifetime of `self`; never written after
    /// construction.
    #[cfg(target_os = "linux")]
    cpu_to_group: Box<[u8]>,
}

impl std::fmt::Debug for ShardedMioHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedMioHandle")
            .field("num_workers", &self.workers.len())
            .finish_non_exhaustive()
    }
}

// Note: explicit `Drop` impl on `ShardedMioHandle` is no longer
// needed — `groups: Box<[ChipletGroup]>` field-drops every group's
// `meta_epfd` + `meta_waker` automatically. Children remain
// registered until each worker's `SharedRegistry` is dropped a
// moment later as part of runtime teardown; the kernel removes the
// meta-side records automatically when each group's meta fd closes.

impl ShardedMioHandle {
    pub(crate) fn new(num_workers: usize) -> Self {
        let mut workers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            workers.push(CachePadded::new(WorkerState::new()));
        }
        let barrier_count = num_workers.max(1);

        // Probe L3-cache topology and build one ChipletGroup per
        // distinct L3 (= one per CCX on Zen). On hosts where every
        // `/sys` read fails (e.g. sandboxes without sysfs), the probe
        // falls back to a single group and behaviour collapses to the
        // pre-sharding runtime-wide meta-watcher. `EPOLL_CLOEXEC` keeps
        // each group's meta epoll fd from leaking across `exec`.
        #[cfg(target_os = "linux")]
        let (cpu_to_group, group_count) = probe_chiplet_groups();
        #[cfg(target_os = "linux")]
        let groups: Box<[ChipletGroup]> = {
            let mut v = Vec::with_capacity(group_count as usize);
            for g in 0..group_count {
                match ChipletGroup::new() {
                    Ok(grp) => v.push(grp),
                    Err(err) => panic!(
                        "sharded-mio: ChipletGroup::new for group {g} failed: {err}",
                    ),
                }
            }
            v.into_boxed_slice()
        };

        Self {
            workers: workers.into_boxed_slice(),
            next_worker: AtomicUsize::new(0),
            metrics: IoDriverMetrics::default(),
            start_barrier: std::sync::Barrier::new(barrier_count),
            #[cfg(target_os = "linux")]
            groups,
            #[cfg(target_os = "linux")]
            cpu_to_group,
        }
    }

    /// Try to become the *meta watcher* for `group_idx`: the single
    /// thread allowed to block in `epoll_wait` on that group's meta
    /// epoll fd at any given moment. Returns `Some(guard)` on success
    /// — drop the guard (or call `MetaWatcherGuard::release`) to
    /// release the slot for that group. Returns `None` when another
    /// worker already holds the slot for this group; the caller
    /// should fall back to parking on its own child epoll.
    ///
    /// Takes `self: &Arc<Self>` so the returned guard can carry an
    /// `Arc<Self>` clone, decoupling its lifetime from the caller and
    /// allowing it to cross `&mut self` boundaries on the parker side.
    ///
    /// See [`Self::groups`] for the partitioning rationale.
    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    pub(crate) fn try_acquire_meta_watcher(
        self: &Arc<Self>,
        group_idx: u8,
    ) -> Option<MetaWatcherGuard> {
        let group = self.groups.get(group_idx as usize)?;
        match group.meta_watcher_busy.compare_exchange(
            false,
            true,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => Some(MetaWatcherGuard {
                handle: Arc::clone(self),
                group_idx,
                released: false,
            }),
            Err(_) => None,
        }
    }

    /// Raw meta-epoll fd for `group_idx`. Stable for the lifetime of
    /// `self`; field-dropped via the `groups` `Box<[..]>`. Used by the
    /// parker's [`ParkMode::Meta`] branch.
    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    pub(crate) fn meta_epfd(&self, group_idx: u8) -> RawFd {
        self.groups[group_idx as usize].meta_epfd
    }

    /// Sentinel `epoll_event.u64` that identifies a group's
    /// meta-waker eventfd in the events buffer returned by
    /// `epoll_wait` on the group's `meta_epfd`. Children carry their
    /// `worker_idx` as their u64; this sentinel is well outside any
    /// plausible worker index. Same constant for every group — the
    /// per-group meta epolls are disjoint, so collision-free.
    #[cfg(target_os = "linux")]
    #[inline]
    pub(crate) fn meta_waker_token() -> u64 {
        META_WAKER_TOKEN
    }

    /// Drain `group_idx`'s meta-waker eventfd. Called by
    /// `ShardedMioParker::park_on_meta` after observing
    /// `META_WAKER_TOKEN` in the kernel-returned events.
    #[cfg(target_os = "linux")]
    #[inline]
    pub(crate) fn drain_meta_waker(&self, group_idx: u8) {
        self.groups[group_idx as usize].meta_waker.drain();
    }

    /// Group-local member set as a `u128` bitmask: bit `i` set iff
    /// worker `i` is assigned to `group_idx`. Used by the parker's
    /// `park_on_meta` to bound `peer_mask` to in-group members and
    /// by the gate to skip the meta CAS when this worker is the only
    /// member of its group (no fan-out target). Cheap to recompute
    /// per call: it's a tight loop over `self.workers` reading one
    /// `AtomicU8` each. Stable after first-park assignment of every
    /// member.
    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    pub(crate) fn group_member_mask(&self, group_idx: u8) -> u128 {
        let mut mask: u128 = 0;
        for (i, slot) in self.workers.iter().enumerate() {
            if i >= 128 {
                break;
            }
            if slot.group_idx.load(Ordering::Acquire) == group_idx {
                mask |= 1u128 << i;
            }
        }
        mask
    }

    /// Live count of workers currently assigned to `group_idx`. Read
    /// by the gate to short-circuit the meta CAS for single-member
    /// groups (no fan-out value). Maintained by `ensure_group_assigned`
    /// on each first-park.
    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    pub(crate) fn group_member_count(&self, group_idx: u8) -> u8 {
        self.groups[group_idx as usize]
            .member_count
            .load(Ordering::Acquire)
    }

    /// Ensure `worker_idx` has been assigned to a chiplet group. On
    /// first call (cold path) this reads `libc::sched_getcpu()` to
    /// pick a CCX-local group, registers the worker's child epoll fd
    /// onto that group's meta epoll, increments the group's
    /// `member_count`, and publishes the group index on
    /// `WorkerState::group_idx` with `Release`. Subsequent calls are
    /// a single `Acquire` load on the worker's own slot.
    ///
    /// Returns the assigned `group_idx`. Always called from the
    /// owning worker thread (in the parker's first-park path), so no
    /// concurrent assignment for the same worker_idx is possible.
    #[cfg(target_os = "linux")]
    pub(crate) fn ensure_group_assigned(&self, worker_idx: usize) -> u8 {
        let slot = &self.workers[worker_idx];
        // Hot path: already assigned. Acquire pairs with the Release
        // store below and with cross-thread unpark observers.
        let cur = slot.group_idx.load(Ordering::Acquire);
        if cur != u8::MAX {
            return cur;
        }

        // Cold path: pick group from the CPU we are currently
        // running on. `sched_getcpu` may return -1 in heavily
        // restricted sandboxes; bucket those into group 0.
        // SAFETY: `sched_getcpu` has no caller preconditions and is
        // signal-safe; failure modes are <0 returns.
        let cpu = unsafe { libc::sched_getcpu() };
        let group_idx = if cpu < 0 {
            0u8
        } else {
            let cpu = cpu as usize;
            let g = self
                .cpu_to_group
                .get(cpu)
                .copied()
                .unwrap_or(u8::MAX);
            if g == u8::MAX {
                0u8
            } else {
                g
            }
        };
        // Defensive bound: if topology probe somehow reported a
        // group beyond what we built, fall back to group 0. Should
        // not happen — groups[] was sized from probe's group_count.
        let group_idx = if (group_idx as usize) >= self.groups.len() {
            0u8
        } else {
            group_idx
        };

        // Register own child epfd onto the group's meta epoll. Must
        // happen before the Release store of `group_idx` so any peer
        // that observes our published group_idx (e.g. via the
        // unpark path's PARKED_META branch) is guaranteed the meta
        // epoll has already been wired to wake on our child.
        let registry = slot
            .shared_registry
            .get()
            .expect("shared_registry published before first park");
        let child_epfd = registry.epoll_fd();
        if let Err(err) = self.groups[group_idx as usize]
            .add_child(child_epfd, worker_idx as u64)
        {
            panic!(
                "sharded-mio: epoll_ctl(group {group_idx} meta, ADD, \
                 child={child_epfd}, worker={worker_idx}) failed: {err}",
            );
        }

        // Bump member_count then publish group_idx. The order of
        // these two is irrelevant for correctness — both are
        // Release/Acquire-paired with their respective readers and
        // single-writer (this thread is the only writer for both
        // this slot's group_idx and a unique increment of the
        // group's member_count).
        self.groups[group_idx as usize]
            .member_count
            .fetch_add(1, Ordering::Release);
        slot.group_idx.store(group_idx, Ordering::Release);
        group_idx
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
    /// **Per-chiplet meta epoll registration is deferred to first-park.**
    /// Originally this method also registered the worker's child epoll
    /// fd onto a runtime-wide meta epoll. With per-chiplet sharding the
    /// worker doesn't yet know its group at scheduler startup — group
    /// assignment depends on `sched_getcpu()` and is best done once
    /// the worker has actually started running tasks. The meta-side
    /// `epoll_ctl(ADD)` therefore lives in
    /// [`Self::ensure_group_assigned`], called from the parker on its
    /// first park.
    ///
    /// Acceptable hole: between scheduler startup and first park, this
    /// worker is invisible to every group's meta-watcher. Events
    /// queued on its child during that window are still picked up by
    /// the worker itself on its first `mio::Poll::poll`. Peers can't
    /// fan-in for it during that window, but the worker is by
    /// definition not yet CPU-bound (it hasn't started polling
    /// anything), so there are no peer-stuck events to harvest.
    pub(crate) fn register_worker(
        &self,
        worker_idx: usize,
        shared_registry: SharedRegistry,
        external_waker: ExternalWaker,
    ) {
        let slot = &self.workers[worker_idx];

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
    /// Race argument: the parker transitions through one CAS-published
    /// state before entering the kernel —
    /// [`begin_park_direct`] (`EMPTY → PARKED_<mode>`), then the
    /// blocking syscall. The CAS uses `AcqRel` ordering. An
    /// unparker's `swap(NOTIFIED, Release)` against `prev`:
    ///
    /// - `prev = PARKED_<X>` → parker is in syscall `X`; deliver one
    ///   kernel wake on `X`'s mechanism.
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
                // Worker is in `epoll_wait` on its chiplet group's
                // meta epoll. Wake by writing that group's
                // `meta_waker` eventfd (registered on the group's
                // meta epoll with sentinel `META_WAKER_TOKEN`).
                //
                // `group_idx` is published with `Release` before the
                // park CAS that publishes `PARKED_META`; observing
                // `PARKED_META` here therefore guarantees a valid
                // `group_idx` (the swap above is `Release`/`Acquire`
                // and pairs with the parker's `begin_park_direct`
                // CAS).
                let g = slot.group_idx.load(Ordering::Acquire);
                if let Some(group) = self.groups.get(g as usize) {
                    group.meta_waker.wake();
                } else {
                    debug_assert!(
                        false,
                        "unpark: PARKED_META observed without valid group_idx \
                         (worker_idx={worker_idx}, group_idx={g})",
                    );
                }
                true
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

    /// Direct one-step park CAS: `EMPTY → PARKED_<mode>`. The
    /// substrate's only park-CAS path: `try_consume_notified` already
    /// cleared the fast-path `NOTIFIED` case, so by the time we get
    /// here we expect either `EMPTY` (we win the CAS, parker commits
    /// to a syscall) or a fresh `NOTIFIED` from a racing `unpark`
    /// (we yank back).
    ///
    /// Returns `Err(())` if a notification was already pending on
    /// entry (`park_state == NOTIFIED`); the call clears state back
    /// to `EMPTY` and the caller should return without parking.
    ///
    /// Cross-worker `unpark` correctness: the unpark path's
    /// `swap(NOTIFIED)` produces `prev == PARKED_<mode>` (kernel wake)
    /// or `prev == EMPTY` (no-op).
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

    /// Does this worker currently hold any live I/O registrations?
    ///
    /// Live counter (`AtomicUsize`): incremented on the success tail
    /// of `register_on_worker`, decremented on the matching arm of
    /// `queue_deregister`. Returns `true` when the count is `> 0`,
    /// meaning at least one `ScheduledIo` is currently registered on
    /// this worker's child epoll. Used to gate the meta-watcher CAS:
    /// a worker with no current I/O and no peers with I/O has
    /// nothing to drain from the meta epoll, so we skip the global
    /// CAS contention on `meta_watcher_busy`.
    ///
    /// Acquire-load pairs with the Release fetch_add in
    /// `register_on_worker`'s success tail, so observing `> 0`
    /// implies the registration is fully published into the slab and
    /// epoll interest set. Decrement uses Release for symmetry with
    /// post-deregister cleanup; stale reads in either direction are
    /// safe (see field doc on `WorkerState::registered_count`).
    #[inline]
    pub(crate) fn worker_has_io_registered(&self, worker_idx: usize) -> bool {
        self.workers[worker_idx]
            .registered_count
            .load(Ordering::Acquire)
            > 0
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
        // Live count: bump so future parks on this worker take the
        // meta-watcher path (events for our fds fire through the
        // child epoll, drained by whichever worker is the meta
        // watcher). Release fetch_add pairs with the Acquire-load on
        // the parker side (`ShardedMioHandle::worker_has_io_registered`),
        // ensuring the slab insert + kernel epoll registration are
        // visible to any worker that observes count > 0. Matched by
        // a Release fetch_sub in `queue_deregister`.
        slot.registered_count.fetch_add(1, Ordering::Release);
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
            // Decrement the live registration count for this worker.
            // Symmetric with the Release fetch_add in
            // `register_on_worker` — we only reach this branch when
            // `slab_key` was set there, which is the same path that
            // bumped the counter. Release ordering matches the
            // increment side; the gate that reads this counter
            // (`worker_has_io_registered`) is purely advisory, so a
            // stale Acquire-load of `> 0` after this decrement is
            // harmless (one extra meta-watcher cycle, then re-park).
            slot.registered_count.fetch_sub(1, Ordering::Release);
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
