//! Lightweight, process-wide counters for the lazy-on-first-poll
//! registration path on the io-uring backend.
//!
//! Compiled in only when the runtime is built with `tokio_unstable` +
//! `io-uring-reactor` on Linux. Activated at
//! runtime by setting the `TOKIO_LAZY_DEBUG` environment variable to a
//! non-empty value before the runtime starts. The env var gates both
//! the periodic stderr dump thread and the per-call counter increments,
//! so the steady-state overhead when not enabled is a single relaxed
//! atomic load on a read-shared cache line.
//!
//! The dump thread prints the full counter set to stderr every 250 ms.
//! It exits when the process exits — we don't try to join it.

use std::env;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
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

    // ---- register dispatch ----
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
    /// what protects the new registration from being clobbered.
    apply_deregister_gen_mismatch,

    // ---- deregister entry from Drop ----
    queue_deregister_calls,
    queue_deregister_no_worker,

    // ---- poll_and_dispatch ----
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
    /// `unpark` was called.
    unpark_calls,
    /// `unpark` saw `prev == PARKED` and delivered a kernel wake.
    unpark_was_parked,
    /// `unpark` saw `prev == EMPTY` (worker mid-task; no syscall needed,
    /// state set to NOTIFIED and the next `begin_park` will fast-path).
    unpark_was_empty,
    /// `unpark` saw `prev == NOTIFIED` (already pending; idempotent).
    unpark_was_notified,

    /// `begin_park` entered (counts `begin_park_direct` entries plus
    /// `try_consume_notified` fast-paths).
    begin_park_calls,
    /// Parker committed to a kernel park (CAS EMPTY → PARKED_*
    /// succeeded).
    begin_park_parked,
    /// Parker fast-pathed (state was NOTIFIED on entry; skip kernel
    /// park).
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

    /// `park_on_meta` entered. One call per steal-mode park; mode is
    /// selected per-park based on whether the worker's own slab is empty.
    meta_park_calls,
    /// Steal-mode park returned 0 events because the
    /// `epoll_wait(timeout)` deadline expired without any sibling
    /// child firing.
    meta_park_timeout,
    /// `epoll_wait` on the meta fd returned an error. EINTR is the
    /// most common reason and is harmless; others are logged for
    /// visibility.
    meta_park_err,
    /// Steal-mode park observed at least one firing child and
    /// invoked `try_steal_drain` (or self-drain) for it.
    meta_park_woken,
}

pub(crate) static COUNTERS: LazyDebugCounters = LazyDebugCounters::new();

/// Bump a counter (no-op if `TOKIO_LAZY_DEBUG` is unset).
#[inline]
pub(crate) fn bump(c: &AtomicU64) {
    if !enabled() {
        return;
    }
    c.fetch_add(1, Ordering::Relaxed);
}

/// Cheap runtime gate over `TOKIO_LAZY_DEBUG`.
///
/// On the steady-state hot path this is a single read-shared atomic
/// load with no contention bouncing — unlike `fetch_add` on the
/// counter atomics, which serializes a cache line across cores. The
/// first call probes the env var and (if enabled) starts the periodic
/// dumper thread.
#[inline]
pub(crate) fn enabled() -> bool {
    *ENABLED.get_or_init(|| {
        let on = env::var_os("TOKIO_LAZY_DEBUG")
            .map(|v| !v.is_empty())
            .unwrap_or(false);
        if on {
            spawn_dumper();
        }
        on
    })
}

static ENABLED: OnceLock<bool> = OnceLock::new();

fn spawn_dumper() {
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
