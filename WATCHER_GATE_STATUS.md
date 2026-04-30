# Meta-Watcher Gate — Status (abandoned, WIP committed)

Companion to `WATCHER_GATE_DESIGN.md`. Read that first.

This document records what was attempted, what failed, and why the
`busy_owner_idle` gap cannot be closed by a parking optimization.
Written across two implementation sessions.

## TL;DR

**The `busy_owner_idle` benchmark distributes probe fds to ALL
workers.** Every worker has a non-empty slab. No worker is "idle" in
the slab sense. Any gate conditioned on `slab_is_empty()` never
activates. The ~20% gap between sharded-mio and traditional on this
benchmark is the inherent cost of N separate `epoll_wait` syscalls
vs traditional's single shared epoll. No parking optimization can
close it — the fix is readiness stealing (a worker that has no events
polls a peer's child epoll on its behalf), not parking strategy.

| variant                                | `busy_owner_idle` | `busy_owner_3burners` |
|----------------------------------------|-------------------|------------------------|
| traditional                            | ~264 µs           | ~326 µs                |
| sharded-mio, no gate (`fede15a6`)      | ~325 µs (~23%)    | ~48 ms (no steal)      |
| sharded-mio, unconditional gate        | hung (deadlock)   | n/a                    |
| sharded-mio, slab-empty gate (this WIP)| ~328 µs (=baseline)| ~48 ms (=baseline)   |
| sharded-mio, slab-empty gate + fan-out | ~324 µs (=baseline)| ~48 ms (=baseline)   |

**Earlier numbers citing a 6-9% gap and sharded-mio winning on
3burners (271 µs) were artifacts of criterion's cached baseline
from a previous run.** The true baseline gap is ~23% on idle and
~150× on 3burners (the 48 ms is the 50 ms burner spin time — probes
stuck on busy workers' child epolls can't be drained without
readiness stealing).

## What was tried

### Attempt 1: Unconditional watcher gate

All workers (regardless of slab state) go through the meta-watcher
CAS. One winner blocks on `libc::epoll_wait(meta_epfd)`, the rest
`std::thread::park`.

**Result:** Deadlocked `tcp_multi_worker_round_trip` at 100% CPU.
Root cause: the eventfd hazard (see below). Owner workers with
active fds were thread-parking instead of calling `mio::Poll::poll`,
so their `WAKER_TOKEN` eventfd was never drained. The watcher's raw
`epoll_wait` on the meta fd observed the level-triggered child but
couldn't dispatch the peer's events. Spin loop.

### Attempt 2: Slab-conditional watcher gate

Workers with `slab_is_empty() == true` compete for the watcher slot;
workers with non-empty slabs keep `park_on_own_child` (unchanged).
`meta_watcher_park` pre-drains own child, blocks on meta fd, post-
drains own child. Peer events not dispatched.

**Result:** All 23 sharded-mio tests pass. But the gate never
activates for `busy_owner_idle` because every worker has ~4 probe
fds — no slab is empty. Performance: identical to no-gate baseline.

### Attempt 3: Owner-side fan-out

After `park_on_own_child`, owner calls `take_interested_workers()`
and `unpark()` on every peer with stake. No watcher, no meta-epoll.
Empty-slab workers just `thread_park`.

**Result:** All tests pass. But adds measurable overhead from the
atomic swap on every park return. No improvement on `busy_owner_idle`
(peers have their own fds to poll anyway). +31% regression in one
bench run (likely noise, but certainly no improvement).

### Final state: simplified slab-conditional thread_park

Cleanest version: empty-slab workers `thread_park`; non-empty-slab
workers `park_on_own_child`. No watcher, no meta-epoll-wait, no
fan-out. `meta_watcher_park` removed from hot path. Substrate kept
(MetaWatcherGuard, meta_epfd, interested_workers, park_thread,
record_sharded_mio_stake) behind `#[allow(dead_code)]` for future
use.

## The eventfd hazard

The single most important finding, not flagged in the design doc:

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

## Why no parking optimization can close `busy_owner_idle`

The benchmark creates 16 socketpair probes distributed across 4
workers (~4 per worker). With 0 burners, all 4 workers are free.
Each must call `epoll_wait` on its own child fd to pick up its ~4
probe events. That's 4 `epoll_wait` syscalls.

Traditional uses a single shared epoll: 1 `epoll_wait` returns all
16 events. The ~23% gap is `4 × epoll_wait` vs `1 × epoll_wait`.

A parking optimization changes *where* idle workers block (epoll vs
futex). But in this benchmark, no worker is idle — every worker has
events queued on its child fd. They all NEED to `epoll_wait` to
pick up their events. Thread-parking an "idle" worker saves nothing
because there are no idle workers.

## What would actually close the gap

1. **Readiness stealing:** A free worker that has already drained its
   own child calls `epoll_wait` (or `mio::Poll::poll`) on a busy
   peer's child fd and dispatches the peer's events. This is the
   design doc's `try_steal_drain` plan, blocked by the eventfd
   hazard. Unblocking it requires point (2).

2. **Replace `mio::Waker` with a runtime-owned eventfd** registered
   on each child epoll *without* `EPOLLET`. Then a peer's raw
   `epoll_wait` can drain the eventfd via `read(8)` and the
   level-triggered registration produces fresh wakes for any
   subsequent `write`.

3. **Or:** accept the `busy_owner_idle` gap and optimize the
   `busy_owner_3burners` path (where the gap is 150×) via readiness
   stealing. That's the higher-value target anyway: real servers
   have hot workers that monopolize the scheduler, and probes stuck
   on their child epolls is the actual latency problem.

## Files in this WIP

```
tokio/src/runtime/io/sharded_mio_driver.rs        — gate substrate (MetaWatcherGuard, meta_epfd, interested_workers, park_thread, record_owner_interest, take_interested_workers)
tokio/src/runtime/io/scheduled_io.rs              — record_sharded_mio_stake hook at 3 Waker-register sites
tokio/src/runtime/scheduler/multi_thread/sharded_mio_park.rs — slab-conditional thread_park, park_on_own_child (unchanged)
WATCHER_GATE_DESIGN.md                            — original design (superseded by this file)
```

All sharded-mio tests pass (23/23):

```
runtime::io::*                          5/5
net_sharded_mio_tcp                     4/4
net_sharded_mio_bench                   5/5
rt_sharded_mio                          7/7
rt_sharded_mio_fanout                   2/2
```
