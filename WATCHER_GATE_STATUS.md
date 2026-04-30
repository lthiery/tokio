# Meta-Watcher Gate — Status (abandoned, WIP committed)

Companion to `WATCHER_GATE_DESIGN.md`. Read that first.

This document records what was attempted, what failed, and what would
be needed to actually close the residual `busy_owner_idle` gap. Written
after a single ~3-hour implementation session that did not ship a win.

## TL;DR

Two gate variants tried; neither produced a benchmark improvement:

| variant                                | `busy_owner_idle`     | `busy_owner_3burners`        | verdict |
|----------------------------------------|-----------------------|------------------------------|---------|
| traditional (baseline)                 | ~273 µs               | ~336 µs                      |         |
| sharded-mio, no gate (commit `fede15a6`) | ~289 µs (~6% gap)   | ~271 µs (clear winner)       | starting point |
| sharded-mio, *unconditional* gate       | hung at 100% CPU     | n/a (test never returned)    | abandoned |
| sharded-mio, *slab-empty* gate (this WIP) | ~322 µs (~18% gap, **regressed** from no-gate) | ~48,000 µs (**142×** regression vs no-gate) | abandoned |

Both gate variants make the runtime worse. The unconditional gate
deadlocks; the slab-conditional gate is correct (all 18 sharded-mio
integration tests pass) but shifts cost in the wrong direction.

The WIP commit preserves the substrate (`park_thread`,
`record_owner_interest`, `interested_workers`, `try_acquire_meta_watcher`,
`meta_epfd`, etc.) so a future attempt does not need to redo the
plumbing. The gate path is wired but its bench result is a regression,
so I am leaving it on disk under WIP rather than shipping it on
`worktree-io-driver-vtable`.

## What was implemented

Reasonably faithful to design doc §7 steps 1-4, with deviations:

1. **`park_thread: OnceLock<Thread>` on `WorkerState`** — populated
   from `ensure_reactor_installed` in the parker, consumed in
   `ShardedMioHandle::unpark` so a thread-parked peer can be woken via
   `Thread::unpark()`. Issued in addition to (not instead of)
   `external_waker.wake()` so we do not regress the non-watcher path.

2. **Watcher CAS via `MetaWatcherGuard`** — RAII lock around an
   `AtomicBool` slot. Single watcher at a time; the rest of the idle
   workers fall back to `std::thread::park`/`park_timeout`.

3. **`meta_watcher_park`** — pivoted away from the design doc's
   "drain peers + fan-out via `interested_workers`" plan; current
   implementation only:
     - pre-drains the watcher's *own* child via
       `Reactor::park_timeout(Duration::ZERO)` to re-arm the
       `WAKER_TOKEN` eventfd,
     - blocks in raw `libc::epoll_wait` on `meta_epfd`,
     - post-drains the watcher's *own* child only,
     - never touches peer child fds.
   The peer-stealing dispatch path was abandoned mid-flight; see "the
   eventfd hazard" below.

4. **Mode selection in `park_internal`** — workers with non-empty
   slabs (`slab_is_empty() == false`) keep the unchanged
   `park_on_own_child` path. Only workers with empty slabs compete
   for the watcher slot. This is the deviation that makes the gate
   safe but also gives up most of its theoretical win, because the
   `busy_owner_idle` bench has the *owner* (slab non-empty) on the
   `mio::Poll::poll` path and N-1 idle peers fighting for the
   watcher slot — which is exactly the shape the gate was supposed
   to optimize, except now the dispatcher is also slab-non-empty so
   it skips the gate too.

5. **`record_sharded_mio_stake`** — added in `scheduled_io.rs` at the
   three Waker-register sites. Currently unused by the simplified
   `meta_watcher_park` (no fan-out), but the substrate is in place
   for a future attempt that drains peers.

## The eventfd hazard

The single most important finding from this attempt, not flagged in
the design doc:

`mio::Waker` registers an `eventfd(EFD_NONBLOCK | EFD_CLOEXEC)` with
`EPOLLET | EPOLLIN` against the per-worker `mio::Poll`'s child epoll.
A peer that does a *raw* `libc::epoll_wait` on the owner's child fd
(e.g. the design's `try_steal_drain` path) will consume the EPOLLET
edge but **not** drain the eventfd's count — only `mio::Poll::poll`
does that, via its registered token handlers.

Once the edge is consumed and the count is left at >0, future
`external_waker.wake()` calls `write(eventfd, 1)`, which increments
the count but produces *no new edge* — the kernel only fires EPOLLET
on the 0→non-zero transition. Subsequent `epoll_wait`s (own *or*
meta) silently miss the wake.

This is what caused the unconditional-gate hang in
`tcp_multi_worker_round_trip`: a thread-parked owner whose external
waker was already in count > 0 from a previous wake never got a
fresh edge, and the watcher-mode peer kept seeing meta-epoll fire on
the level-triggered (peer-side) registration but skipped the drain
because of the busy-state filter.

**Implication:** any design that wants peers to drain owner child
fds *cannot* use `libc::epoll_wait` directly on those child fds.
Either:

- Bypass `mio::Waker` entirely with a runtime-owned eventfd that the
  watcher can read directly (custom registration, no EPOLLET trick),
  or
- Do the steal-drain via `mio::Poll::poll` on the owner's reactor,
  but that requires giving the watcher mutable access to the owner's
  `Reactor`, which races with the owner's own `mio::Poll::poll` if
  the owner ever re-enters its park path.

Neither variant is small. Both are incompatible with the slab/registry
ownership invariants the rest of the sharded-mio backend relies on.

## Why the current WIP regresses 3burners by 142×

I did not finish diagnosing this — abandoned at the bench result per
the "stuck protocol". A reasonable hypothesis:

`busy_owner_3burners` has 3 workers actively producing events plus 1
mostly-idle dispatcher. The dispatcher has empty slab → goes through
the watcher gate. The 3 burners have non-empty slabs → keep the
`mio::Poll::poll` path. The dispatcher's wakes from the burners
arrive on its child epoll (via `tokio::sync::mpsc` notifications,
which go through `Thread::unpark`), but the dispatcher is in
`libc::epoll_wait` on the *meta* fd, not its own. Meta is
level-triggered on each child, so the wake should still propagate —
but the cost of a meta `epoll_wait` followed by a own-child
`mio::Poll::poll(0)` and dispatch-and-bookkeeping per event is
clearly much higher than just a direct `mio::Poll::poll` on the own
child fd, especially when the burst rate is high.

The 142× number suggests something is also live-locking, not just
slow — possibly the meta-epoll level-triggered mode keeps firing on
the burners' (registered-but-unhandled) entries while the dispatcher
ignores them. That is consistent with the design doc's warning that
the watcher must drain peers if it observes their events, and we
deliberately skip that drain in the simplified `meta_watcher_park`.

## What would be needed to actually close the gap

In rough order of effort:

1. **Replace `mio::Waker` with a runtime-owned eventfd** registered
   directly on each child epoll *without* `EPOLLET`. Then a peer's
   raw `epoll_wait` can drain the eventfd via `read(8)` and the
   level-triggered registration produces fresh wakes for any
   subsequent `write`. This unblocks the design doc's `try_steal_drain`
   plan.

2. **Make `meta_watcher_park` actually dispatch peer events** —
   either via point (1), or via per-worker `mio::Registry` access
   that the watcher takes for the duration of the steal-drain.
   Combine with `interested_workers` fan-out so the unparker only
   wakes peers with stake.

3. **Reconsider whether the gate is the right shape**: an alternative
   is to remove the watcher entirely and instead have the *owner*
   batch-wake all `interested_workers` after each `mio::Poll::poll`
   wave. That keeps every park syscall on its own `mio::Poll::poll`
   (no meta-epoll, no raw epoll_wait) and just amortizes wakeups.
   Worth measuring before committing to (1) + (2).

## Files in this WIP

```
tokio/src/runtime/io/sharded_mio_driver.rs        — gate substrate (Watcher slot, meta_epfd, interested_workers, park_thread, record_owner_interest, take_interested_workers)
tokio/src/runtime/io/scheduled_io.rs              — record_sharded_mio_stake hook at 3 Waker-register sites
tokio/src/runtime/scheduler/multi_thread/sharded_mio_park.rs — slab-conditional gate, meta_watcher_park (own-child only), thread_park fallback
WATCHER_GATE_DESIGN.md                            — original design (some sections now superseded — see this file)
```

All sharded-mio tests pass:

```
runtime::io::*                          5/5
net_sharded_mio_tcp                     4/4
net_sharded_mio_bench                   5/5
rt_sharded_mio                          7/7
rt_sharded_mio_fanout                   2/2
```

The bench regression is a perf problem, not a correctness problem.

## Recommended next move

Before another implementation pass, **decide whether to keep going**.
The non-gate baseline (`fede15a6`) is *already the winner* on
`busy_owner_3burners` and most other shapes in the sharded-mio bench
matrix; the residual gap is on the single-busy-owner-many-idle-peers
shape, which is also the shape least representative of real
workloads. It may be cheaper to call the existing 6% gap acceptable
and put the time elsewhere.

If we do keep going: do option (3) above (owner-side fan-out, no
watcher) first, since it sidesteps the eventfd hazard entirely and
reuses the `interested_workers` substrate this WIP already wires up.
