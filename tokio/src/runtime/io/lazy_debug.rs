//! Lightweight, process-wide counters for the lazy-on-first-poll
//! registration path on the sharded-mio backend.
//!
//! Compiled in only when the runtime is built with `tokio_unstable` +
//! `io-sharded-mio` (or `io-uring-reactor`) on Linux. Activated at
//! runtime by setting the `TOKIO_LAZY_DEBUG` environment variable to a
//! non-empty value before the runtime starts. The counters are always
//! incremented (cheap relaxed atomic ops); the env var only gates the
//! periodic stderr dump thread, so the overhead when not enabled is one
//! `Once::call_once` plus a no-op spawn.
//!
//! The dump thread prints the full counter set to stderr every 250 ms.
//! It exits when the process exits — we don't try to join it.

use std::env;
use std::io::Write;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, Once};
use std::thread;
use std::time::Duration;

macro_rules! counters {
    ( $( $(#[$meta:meta])* $name:ident ),* $(,)? ) => {
        pub(crate) struct LazyDebugCounters {
            $( $(#[$meta])* pub(crate) $name: AtomicU64, )*
        }

        impl LazyDebugCounters {
            const fn new() -> Self {
                Self {
                    $( $name: AtomicU64::new(0), )*
                }
            }

            fn snapshot(&self) -> Vec<(&'static str, u64)> {
                vec![
                    $( (stringify!($name), self.$name.load(Ordering::Relaxed)), )*
                ]
            }
        }
    };
}

counters! {
    // ---- registration.rs::register_if_needed ----
    rin_calls,
    rin_shared_hit,
    rin_error_cached,
    rin_no_lazy,
    rin_no_driver,
    rin_register_call,
    rin_register_err,
    rin_race_loser,
    rin_success,

    // ---- sharded_mio_driver register dispatch ----
    register_local_calls,

    // ---- same-worker sync register fast path ----
    /// `register_on_worker` entered (sync path on the calling worker).
    register_on_worker_calls,
    /// `register_on_worker` saw the registration set already shutting
    /// down; the registration was rejected.
    register_on_worker_shutdown,
    /// `register_on_worker` found no `SharedRegistry` published on the
    /// calling worker yet — startup race; should not happen
    /// post-barrier.
    register_on_worker_no_registry,
    /// `register_on_worker` succeeded: kernel-side epoll registration
    /// installed and `(slab_key, gen)` stamped on `ScheduledIo`.
    register_on_worker_ok,
    /// `register_on_worker` saw `mio::Registry::register` fail (e.g.
    /// EBADF). Rolled back the per-shard set entry and surfaced the
    /// error.
    register_on_worker_errors,

    // ---- deregister bookkeeping ----
    /// Total number of `queue_deregister` calls that reached the
    /// synchronous deregister body (i.e. were not short-circuited by
    /// the `worker_idx >= workers.len()` no-op check). Kept under the
    /// historical `apply_deregister_calls` name because
    /// `maybe_dump_trial` keys its delta-dump cadence off this counter.
    apply_deregister_calls,
    apply_deregister_no_key,
    /// Dispatch-time observation that a slab lookup hit but the entry's
    /// generation didn't match the one packed into the kernel token —
    /// the slot was reassigned between epoll queueing the event and
    /// dispatch reading it.
    dispatch_gen_mismatch,
    /// Apply-deregister was skipped because either the fd in the kernel
    /// epoll set belongs to a fresher registration (recycled fd) or the
    /// slab slot has been reassigned. Skipping `epoll_ctl_del` here is
    /// what protects the new registration from being clobbered. See
    /// `DeregisterOutcome` in `sharded_mio_reactor.rs`.
    apply_deregister_gen_mismatch,

    // ---- deregister entry from Drop ----
    queue_deregister_calls,
    queue_deregister_no_worker,

    // ---- sharded_mio_reactor::poll_and_dispatch ----
    /// `poll_and_dispatch` was entered (one per `reactor.park*` call).
    dispatch_calls,
    /// `mio::Poll::poll` returned successfully (no error/EINTR).
    dispatch_poll_ok,
    /// `mio::Poll::poll` returned `Interrupted`.
    dispatch_poll_eintr,
    /// `mio::Poll::poll` returned some other error.
    dispatch_poll_err,
    /// Total number of `mio::Event`s seen across all dispatches
    /// (sum of `events.iter().count()` per pass).
    dispatch_events_total,
    /// Dispatches that returned with zero events (timeout/spurious wake).
    dispatch_zero_events,
    /// Events whose token was the reserved `WAKER_TOKEN` (cross-thread
    /// wake; nothing to dispatch).
    dispatch_waker_token,
    /// Events whose token had no live slab entry (deregistered between
    /// kernel queueing and dispatch).
    dispatch_slab_miss,
    /// Events that successfully resolved to a `ScheduledIo` and were
    /// woken via `io.wake(ready)`.
    dispatch_woken,
    /// Subset of `dispatch_woken` where the event included `READABLE`.
    dispatch_woken_readable,
    /// Subset of `dispatch_woken` where the event included `WRITABLE`.
    dispatch_woken_writable,

    // ---- unpark observability ----
    /// `ShardedMioHandle::unpark` was called.
    unpark_calls,
    /// `unpark` saw `prev == PARKED` and delivered a kernel wake.
    unpark_was_parked,
    /// `unpark` saw `prev == EMPTY` (worker mid-task; no syscall needed,
    /// state set to NOTIFIED and the next `begin_park` will fast-path).
    unpark_was_empty,
    /// `unpark` saw `prev == NOTIFIED` (already pending; idempotent).
    unpark_was_notified,

    /// `begin_park` entered.
    begin_park_calls,
    /// `begin_park` parked (CAS EMPTY → PARKED succeeded).
    begin_park_parked,
    /// `begin_park` fast-pathed (state was NOTIFIED on entry; skip
    /// kernel park).
    begin_park_fastpath,

    // ---- mio register call observability ----
    /// `SharedRegistry::register` entered.
    sr_register_calls,
    /// `SharedRegistry::register` returned Ok.
    sr_register_ok,
    /// `SharedRegistry::register` returned Err.
    sr_register_err,
    /// Subset of `sr_register_calls` where the requested interest
    /// included `READABLE`.
    sr_register_readable_interest,
    /// Subset of `sr_register_calls` where the requested interest
    /// included `WRITABLE`.
    sr_register_writable_interest,

    /// `Handle::schedule_task` took the local-queue branch (current
    /// thread is on a worker of this scheduler and holds its core).
    /// Counted regardless of steal-dispatch context.
    schedule_local_total,
    /// `Handle::schedule_task` fell through to the inject queue
    /// because `with_current` returned `None` — caller is not on any
    /// worker thread (cross-thread waker, blocking pool, etc.).
    schedule_remote_no_cx,
    /// `Handle::schedule_task` fell through to the inject queue
    /// because the current worker belongs to a *different*
    /// scheduler (multi-runtime case).
    schedule_remote_other_scheduler,
    /// `Handle::schedule_task` fell through to the inject queue
    /// because the current worker no longer holds its core (mid
    /// hand-off / blocking-pool transition).
    schedule_remote_no_core,

    // ---- meta-epoll readiness stealing ----
    /// `SharedRegistry::try_steal_drain` entered. One call ≈ one
    /// peer attempt to drain a sibling worker's child epoll on
    /// behalf of a stalled task or an idle parker.
    steal_drain_calls,
    /// Steal attempt skipped because `try_lock` on `OpsState`
    /// failed — the owner is mid-dispatch and we yield to it
    /// rather than block.
    steal_drain_busy,
    /// Steal attempt found the owner's child epoll empty
    /// (kernel returned 0 events). Either another peer just
    /// drained it or the level-trigger fired spuriously.
    steal_drain_empty,
    /// `epoll_wait` on the child epoll returned an error from
    /// the steal path. Counted but not fatal — the owner-side
    /// park loop is the canonical drain.
    steal_drain_err,
    /// Steal attempt drained at least one event and fired its
    /// waker. `steal_drain_woken_total` accumulates the per-call
    /// counts.
    steal_drain_woken,
    /// Sum of events fired across all steal calls. Useful for
    /// end-of-trial dumps to compare with `dispatch_woken`.
    steal_drain_woken_total,
}

pub(crate) static COUNTERS: LazyDebugCounters = LazyDebugCounters::new();

/// Maximum worker index supported by the per-worker counter arrays.
/// Bench scenarios use 4 workers; 16 covers reasonable headroom while
/// keeping the static foot-print trivial.
pub(crate) const MAX_WORKERS: usize = 16;

/// Per-worker counter arrays. Indexed by worker idx as published by
/// `ShardedMioParker` / `current_worker_index()`. Out-of-range indices
/// are silently dropped at the bump sites.
pub(crate) struct PerWorkerCounters {
    /// Worker dispatched ≥1 event during its own
    /// `Reactor::poll_and_dispatch` pass (the [`dispatch_woken`]
    /// counter, attributed by worker). Under EPOLLEXCLUSIVE-fanout
    /// the dispatching worker is whichever one received the event in
    /// `epoll_wait`, not necessarily the slab owner.
    ///
    /// [`dispatch_woken`]: LazyDebugCounters
    pub(crate) dispatch_woken: [AtomicU64; MAX_WORKERS],
    /// `begin_park` was called for this worker idx.
    pub(crate) begin_park_calls: [AtomicU64; MAX_WORKERS],
    /// fd registered via `register_on_worker` onto this worker as the
    /// slab owner.
    pub(crate) register_per_worker: [AtomicU64; MAX_WORKERS],
}

impl PerWorkerCounters {
    const fn new() -> Self {
        Self {
            dispatch_woken: [const { AtomicU64::new(0) }; MAX_WORKERS],
            begin_park_calls: [const { AtomicU64::new(0) }; MAX_WORKERS],
            register_per_worker: [const { AtomicU64::new(0) }; MAX_WORKERS],
        }
    }
}

pub(crate) static PER_WORKER: PerWorkerCounters = PerWorkerCounters::new();

/// Bump a per-worker counter slot. Out-of-range indices are no-ops
/// (defensive — should not happen with `MAX_WORKERS=16` bench setups).
#[inline]
pub(crate) fn bump_per_worker(arr: &[AtomicU64; MAX_WORKERS], idx: usize) {
    if let Some(slot) = arr.get(idx) {
        slot.fetch_add(1, Ordering::Relaxed);
    }
    ensure_dumper();
}

static INIT: Once = Once::new();

/// Bump a counter and ensure the periodic dumper is running (no-op if
/// `TOKIO_LAZY_DEBUG` is unset). Cheap to call: one relaxed add plus an
/// already-completed `Once` check on the hot path.
#[inline]
pub(crate) fn bump(c: &AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
    ensure_dumper();
}

#[inline]
fn ensure_dumper() {
    INIT.call_once(spawn_dumper);
}

fn spawn_dumper() {
    let enabled = env::var_os("TOKIO_LAZY_DEBUG")
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    if !enabled {
        return;
    }
    let _ = thread::Builder::new()
        .name("lazy-debug-dumper".into())
        .spawn(|| loop {
            thread::sleep(Duration::from_millis(250));
            dump_to_stderr();
        });
}

fn dump_to_stderr() {
    let snap = COUNTERS.snapshot();
    let stderr = std::io::stderr();
    let mut out = stderr.lock();
    let _ = writeln!(out, "---- lazy-debug counters ----");
    for (name, val) in snap {
        let _ = writeln!(out, "  {name:32} = {val}");
    }
}

// ---- per-trial delta dumper ----
//
// Activated by `TOKIO_LAZY_DEBUG_TRIAL=N`: every `N`th increment of
// `apply_deregister_calls` triggers a delta snapshot. Designed for
// the `io_busy_owner` bench, which deregisters one fd at the end of
// every probe task; with `N=16` (the bench's `NUM_PROBES`), each
// dump is exactly one trial-worth of activity.
//
// Independent of the periodic `TOKIO_LAZY_DEBUG=1` dumper. Safe to
// enable both at once; output is interleaved on stderr.

/// `-1` = env var not yet read; `0` = disabled; `>0` = trial size.
static TRIAL_SIZE: AtomicI64 = AtomicI64::new(-1);

/// Highest `count / trial_size` already dumped. Guards against
/// double-dumping the same boundary when multiple threads are
/// racing through `apply_deregister`.
static TRIAL_NUM: AtomicU64 = AtomicU64::new(0);

/// Counter values from the previous boundary, parallel to
/// `LazyDebugCounters::snapshot()`'s iteration order. Empty before
/// the first dump.
static PREV_SNAPSHOT: Mutex<Vec<u64>> = Mutex::new(Vec::new());

/// Per-worker previous snapshot for delta computation. Populated on
/// first dump.
static PREV_PER_WORKER: Mutex<Option<PerWorkerSnapshot>> = Mutex::new(None);

#[derive(Default, Clone)]
struct PerWorkerSnapshot {
    dispatch_woken: [u64; MAX_WORKERS],
    begin_park_calls: [u64; MAX_WORKERS],
    register_per_worker: [u64; MAX_WORKERS],
}

impl PerWorkerSnapshot {
    fn capture() -> Self {
        let load_arr = |arr: &[AtomicU64; MAX_WORKERS]| {
            let mut out = [0u64; MAX_WORKERS];
            for (i, slot) in arr.iter().enumerate() {
                out[i] = slot.load(Ordering::Relaxed);
            }
            out
        };
        Self {
            dispatch_woken: load_arr(&PER_WORKER.dispatch_woken),
            begin_park_calls: load_arr(&PER_WORKER.begin_park_calls),
            register_per_worker: load_arr(&PER_WORKER.register_per_worker),
        }
    }
}

fn trial_size() -> u64 {
    let cur = TRIAL_SIZE.load(Ordering::Relaxed);
    if cur >= 0 {
        return cur as u64;
    }
    let parsed = env::var("TOKIO_LAZY_DEBUG_TRIAL")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    TRIAL_SIZE.store(parsed as i64, Ordering::Relaxed);
    parsed
}

/// Hook called from `apply_deregister`. Cheap when the env var is
/// unset (one relaxed load + an `i64::cmp` after first call).
pub(crate) fn maybe_dump_trial() {
    let size = trial_size();
    if size == 0 {
        return;
    }
    let count = COUNTERS.apply_deregister_calls.load(Ordering::Relaxed);
    if count == 0 || count % size != 0 {
        return;
    }
    let trial_num = count / size;

    // Take the snapshot first so cross-thread race observers see a
    // consistent view; serialise dumping on PREV_SNAPSHOT's lock.
    let snap = COUNTERS.snapshot();
    let mut prev = match PREV_SNAPSHOT.lock() {
        Ok(g) => g,
        Err(_) => return,
    };

    // Idempotency: only the first thread to reach this boundary
    // emits the delta; later racers see TRIAL_NUM already advanced.
    let last = TRIAL_NUM.load(Ordering::Relaxed);
    if last >= trial_num {
        return;
    }
    TRIAL_NUM.store(trial_num, Ordering::Relaxed);

    let stderr = std::io::stderr();
    let mut out = stderr.lock();
    let _ = writeln!(
        out,
        "---- lazy-debug trial #{} (apply_deregister_calls = {}) ----",
        trial_num, count,
    );

    if prev.is_empty() {
        // First boundary: dump absolute values for any non-zero
        // counter so the reader can see the cumulative startup cost.
        for (name, val) in &snap {
            if *val != 0 {
                let _ = writeln!(out, "  {name:32} = {val}");
            }
        }
    } else {
        for (i, (name, val)) in snap.iter().enumerate() {
            let prev_val = prev.get(i).copied().unwrap_or(0);
            let delta = val.wrapping_sub(prev_val);
            if delta != 0 {
                let _ = writeln!(out, "  {name:32} +{delta}");
            }
        }
    }
    *prev = snap.into_iter().map(|(_, v)| v).collect();

    // Per-worker deltas. Same boundary semantics: first dump shows
    // absolute non-zero values, subsequent dumps show deltas.
    let cur_pw = PerWorkerSnapshot::capture();
    let mut prev_pw_guard = match PREV_PER_WORKER.lock() {
        Ok(g) => g,
        Err(_) => return,
    };
    let render = |out: &mut std::io::StderrLock<'_>,
                  name: &str,
                  cur: &[u64; MAX_WORKERS],
                  prev: Option<&[u64; MAX_WORKERS]>| {
        // Only emit a line per worker idx if the value (delta or
        // absolute) is non-zero. Keeps output compact.
        for w in 0..MAX_WORKERS {
            let v = match prev {
                Some(p) => cur[w].wrapping_sub(p[w]),
                None => cur[w],
            };
            if v != 0 {
                let _ = writeln!(out, "  {name:32}[w{w}] {sign}{v}",
                    sign = if prev.is_some() { "+" } else { "=" });
            }
        }
    };

    let prev_pw = prev_pw_guard.as_ref();
    render(&mut out, "dispatch_woken_pw",
        &cur_pw.dispatch_woken, prev_pw.map(|p| &p.dispatch_woken));
    render(&mut out, "begin_park_calls_pw",
        &cur_pw.begin_park_calls, prev_pw.map(|p| &p.begin_park_calls));
    render(&mut out, "register_per_worker_pw",
        &cur_pw.register_per_worker, prev_pw.map(|p| &p.register_per_worker));

    *prev_pw_guard = Some(cur_pw);
}
