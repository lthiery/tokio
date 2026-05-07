# Meta-Watcher Gate — Status

Companion to `WATCHER_GATE_DESIGN.md`. Read that first.

> **Status (current):** the gate **shipped**, in a shape distinct from
> both the original design and the abandoned WIP described below. See
> `INVESTIGATION-sharded-mio-perf.md` for the resurrection narrative.
> Live in `sharded_mio_park.rs` as:
>
> ```rust
> let want_meta = self.handle.num_workers() > 1
>     && self.handle.worker_has_io_registered(self.idx);
> ```
>
> The two-condition gate sidesteps both failure modes from the
> original attempts (the eventfd hazard at §"The eventfd hazard"
> below was avoided by keeping the watcher's drain on `mio::Poll`
> rather than raw `libc::epoll_wait`; the `busy_owner_idle` gap is
> still open per §"Why no parking optimization can close
> `busy_owner_idle`", and the path forward there is readiness
> stealing — see `tokio/docs/readiness-stealing-fanout.md`).
>
> **Refinement at `74189589` (2026-05-07):** the
> `worker_has_io_registered` predicate switched from a sticky
> `AtomicBool` set-once latch to a live `AtomicUsize` counter
> (`registered_count`, increment on `register_on_worker` success,
> decrement on `queue_deregister`). Workers that used to own I/O and
> have since dropped all of it now fall out of the gate immediately
> instead of paying the meta CAS + extra `epoll_wait(meta_epfd)` per
> park indefinitely. Headline perf delta vs baseline at `17f8863b`
> (5 reps, 8s measure):
>
> | Bench / case                                | lounas (W=4..32)                    | lourip (W=6..128)                   |
> |---------------------------------------------|--------------------------------------|--------------------------------------|
> | `net_tcp_echo / sharded_mio/tcp_echo_throughput` | −2.84%, −5.19%, −2.82%, −2.81%, −4.98% | −0.48%, −2.92%, −2.30%, −0.69%, −1.11% |
> | `net_tcp_echo / sharded_mio/tcp_connect_churn`   | +0.53%, −41.83%, −31.65%, +0.80%, −6.01%  | −4.84%, −10.51%, −24.14%, +2.35%, +5.20% |
> | `sync_notify`, `sync_mpsc`                       | within rep noise (gate already short-circuits) | within rep noise                  |
>
> The mild regression on `tcp_connect_churn` at high W on lourip
> (+2.35%, +5.20% at W=64, 128) is the `fetch_add` / `fetch_sub`
> overhead vs the sticky bool's single Release-store, dominated by
> the W=16-32 wins (-10% to -24%). See §"Open follow-ups" for the
> per-chiplet watcher sharding idea that should remove that
> high-W tail entirely.

---

## History (abandoned WIP, kept for the eventfd-hazard write-up)

The remainder of this document records the original two-session
abandoned attempt. It is **not** describing the current code — see
the box above for what shipped. The `eventfd hazard` section
(§"The eventfd hazard") and the `busy_owner_idle` analysis
(§"Why no parking optimization can close `busy_owner_idle`") remain
correct and continue to constrain future work; the rest is history.

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

---

## Open follow-ups

### Shard the watcher / steal-drain per chiplet (8 threads)

The current shipped gate has a **single, runtime-wide**
`meta_watcher_busy: AtomicBool` and a single `meta_epfd` fanning in
every worker's child epoll. That global CAS line and shared epoll fd
become a bottleneck at high worker counts:

- **lourip** (EPYC 7H12, 64C/128T, 8 CCDs × 2 CCX × 4C = 16 CCXs of
  8 threads each on Zen 2) shows a mild `tcp_connect_churn`
  regression at W=64 (+2.35%) and W=128 (+5.20%) after the live-
  counter refinement. The throughput case still wins, but the
  connect-churn path — heavy on `register_on_worker` /
  `queue_deregister` cycles — pays for cross-CCX cache traffic on
  `meta_watcher_busy` and the `registered_count` fields.
- The gate's behavioural premise is "one of the workers blocks in
  `epoll_wait(meta_epfd)` and fans events out to peers." That
  premise is correct globally but **wasteful** when peers are far
  apart on the topology — a Linux `epoll_wait` return on a CCX-3
  watcher delivering an event to a CCX-12 peer crosses two CCDs
  worth of fabric.

**Proposed structure:** partition workers into chiplet-local groups
of 8 threads (1 CCX on Zen 2; on Zen 4 / Zen 5 with 8C CCXs, 1 CCX
== 16T but 8T groupings still align with L3 boundaries on Zen 2 / 3
hardware we run today). Each group owns:

- Its own `meta_watcher_busy: AtomicBool`.
- Its own `meta_epfd` containing only that group's worker child
  epolls.
- Its own `registered_count` aggregation if we want a fast
  group-level "any I/O active here?" gate.

Cross-group fanout becomes opt-in: the group meta-watcher fans
events to its 8 group-local peers via `unpark` / waker; events on
fds owned by another group are **not** seen by this group's
watcher (they landed on the other group's `meta_epfd`). This is
analogous to per-CCX run-queues in CPU schedulers — locality
trumps load-balancing for the common case.

Sketch of the data model:

```text
ShardedMioHandle
├── workers: [WorkerState; N]               // unchanged
├── chiplet_groups: [ChipletGroup; G]       // new; G = ceil(N / 8)
│   └── ChipletGroup { meta_epfd, meta_watcher_busy, member_idxs: [u32; <=8] }
└── (no more global meta_epfd / meta_watcher_busy)
```

Open design questions before this lands:

1. **Topology source.** `sched_getaffinity` + `/sys/devices/system/cpu`
   per-cpu `topology/{cluster_id, package_cpus, core_cpus}` reads
   give us core→CCX mapping. Read once at runtime build, store the
   group assignment alongside `WorkerState`.
2. **Pinning policy.** Do workers stay on their starting CPU
   (existing tokio behaviour: no pinning by default), or do we
   pin to chiplet-local CPUs at scheduler init? If unpinned, the
   group assignment is best-effort; the kernel may move a worker
   to another CCX between scheduling decisions, in which case the
   group routing is just a heuristic and not a correctness
   constraint.
3. **Cross-group I/O.** When a peer registers a `Waker` on a
   `ScheduledIo` owned by a worker in a different group, the
   existing `record_sharded_mio_stake` substrate (already in the
   tree, currently unused per `WATCHER_GATE_DESIGN.md` §3) is the
   natural place to record cross-group interest and route the
   wake. Same atom (`interested_workers: AtomicU64`) suffices —
   just consumed at group boundaries instead of globally.
4. **Bench gate.** Re-run the same `net_tcp_echo` sweep matrix
   (lounas W=4..32, lourip W=6..128) and check whether the
   `tcp_connect_churn` regression at lourip W=64/128 closes
   without giving up the W=16-32 wins. Targets:
   - lourip W=64/128 `sharded_mio/tcp_connect_churn`: at parity
     with sticky-bool baseline (no regression).
   - lourip W=16-32 `sharded_mio/tcp_connect_churn`: keep the
     -10% to -24% win.
   - lounas W=6/8 `sharded_mio/tcp_connect_churn`: keep the -32%
     to -42% win (these are well within a single CCX on a 15-core
     host so per-chiplet partitioning is a no-op for lounas).

Status: not started. Substrate (`interested_workers`,
`record_sharded_mio_stake`) is in tree from the original
design's spike. The chiplet partitioning is the missing piece.
