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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;
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
    queue_register_calls,

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

    // ---- park-side drain ----
    drain_calls,
    drain_register_drained,
    drain_deregister_drained,

    apply_register_calls,
    apply_register_shutdown,
    apply_register_no_registry,
    apply_register_errors,
    apply_register_ok,

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

    // ---- readiness stealing ----
    /// `try_steal_pass` entered (one per pre-park steal attempt).
    steal_pass_calls,
    /// Number of peer epoll fds visited across all steal passes.
    steal_pass_visits,
    /// Total events harvested across all steal passes.
    steal_events_harvested,
    /// Subset of harvested events where dispatch fired a waker.
    steal_events_woken,
    /// `epoll_wait(timeout=0)` returned 0 (no events ready on the peer).
    steal_eagain,
    /// `epoll_wait(timeout=0)` returned a negative errno other than EINTR.
    steal_errors,
    /// `epoll_wait(timeout=0)` returned EINTR (rare; treat as 0).
    steal_eintr,
    /// Steal observed an event whose slab entry was vacated.
    steal_slab_miss,
    /// Steal observed an event whose gen disagrees with the slab entry.
    steal_gen_mismatch,
    /// Steal observed the peer's WAKER_TOKEN (peer self-wake; ignored).
    steal_waker_token,
    /// `begin_park` observed a peer holding STEALING; spun until released.
    begin_park_steal_spin,
    /// Pre-park steal harvested >0 events; we skipped the actual park.
    park_skip_after_steal,
}

pub(crate) static COUNTERS: LazyDebugCounters = LazyDebugCounters::new();

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
