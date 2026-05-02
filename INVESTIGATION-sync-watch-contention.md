# Investigation: `sync_watch/contention_resubscribe` regression on sharded-mio

## Status

**Investigated, deferred.** Not actionable under the timer-regression work
plan that produced this branch state. Documented here so a follow-up can
pick up the same evidence without re-deriving it.

## What was previously reported

After the wide post-Step-1 bench sweep, `sync_watch/contention_resubscribe/500`
was flagged as `+9.87%` slower with `bench-sharded-mio` enabled. The plan
added it as a stretch task: "investigate `sync_watch/contention_resubscribe/500`
+9.87%."

## Why this is flagged separately from `time_timeout`

`sync_watch::Sender::send` does not interact with the time driver at all.
The bench wakes 6 workers + N subscribers in a notification storm with no
sleeps, no timeouts, no `Instant`-based work. Whatever overhead it measures
must come from the **scheduler park/unpark path**, not the timer flavor.

This rules out the entire Step 4 hybrid-park scope as a possible fix — the
hybrid park flow only changes timer behavior (wheel processing under lock
after wake; `next_wake_tick()` query under lock before park). Both of those
short-circuit when no timer is registered (`next_wake_tick()` returns `None`,
the post-park `process` finds no expired entries). On a pure `Notify`/`watch`
workload they are no-ops.

## Re-bench post-Step-4 (HEAD of this worktree)

Pairwise criterion comparison, default-features baseline:

```
trad_legacy → shard_legacy (hybrid park flow, no alt-timer)
contention_resubscribe/10    1.94 ms → 1.97 ms   +2.4%
contention_resubscribe/100   7.75 ms → 8.54 ms   +10.1%   ← new worst case
contention_resubscribe/500   32.45 ms → 33.95 ms +4.6%   (CI [+0.4%, +8.8%])
contention_resubscribe/1000  61.48 ms → 64.46 ms +4.85%
```

The /500 number on this machine has wide CI ([+0.4%, +8.8%]); the +9.87%
midpoint reported earlier is well within that band. Other task counts show
that the regression exists across the bench family, not just at /500. /100
is now the worst case at +10.1%.

## Diagnosis

The dominant cost is sharded-mio's per-worker park-state machine. Every
park (whether or not it actually blocks in `mio::Poll::poll`) walks this
chain:

```
park_internal()
  ├─ try_consume_notified()              ← AtomicU8 load + CAS to clear
  ├─ try_acquire_meta_watcher()          ← runtime-wide AtomicBool CAS
  ├─ begin_park()                        ← AtomicU8 CAS publishing PARKED_<mode>
  ├─ <syscall: mio::Poll::poll OR epoll_wait OR fast-path return>
  └─ end_park()                          ← AtomicU8 CAS clearing PARKED_<mode>
```

In a notify-storm bench, the bottleneck is the *unpark* path, which
*itself* is at least one extra AtomicU8 swap + (when the prior state was
PARKED) an `eventfd` write to drive the worker's child epoll. Trad-mio
amortizes wake delivery across one shared waker; sharded-mio splits it
into per-worker eventfds, paying one syscall per cross-worker target.

`PLAN-fix-messaging-regression.md` already analyzed an extreme version of
this: an unconditional double-wake in `ShardedMioHandle::unpark` that
fires both `external_waker.wake()` and `park_thread.unpark()` on every
cross-worker wake. That was already simplified down to a single wake
(commit `8e7f77e6` "drop thread-park branch; route slab-empty workers
through OwnChild" on this branch). The remaining `+5–10%` on
`contention_resubscribe` is the residual cost of the per-worker parker
itself, after that simplification.

## Why this is out of scope for the current branch

1. **Different system**. The work plan that produced this branch state was
   "remove the unfair-cfg-driven `+14%` regression on `time_timeout`." That
   was achieved (`-11.77%` on `multi_thread_timeout-8` after Step 4). The
   `sync_watch` bench is testing a different subsystem.
2. **No quick fix**. The four-CAS-per-park cost is the design of the
   sharded-mio park-state machine; reducing it requires either:
   - merging the meta-watcher and own-child branches into a single park
     path (loses the `busy_owner_3burners` peer-stealing win — see
     `WATCHER_GATE_DESIGN.md`),
   - moving the `try_acquire_meta_watcher` CAS out of the per-park hot
     path (e.g. by binding meta-watcher to a single fixed worker, with
     fallback rotation on shutdown), or
   - going back to a single shared `mio::Poll` with caller-side dispatch
     (loses sharding wins on IO-driven benches).
3. **Bench is not load-bearing**. `sync_watch/contention_resubscribe` is
   a microbench; production users with this access pattern would normally
   not enable `enable_sharded_mio()` (it's tagged unstable + experimental).

## Suggested next steps for a follow-up

If someone picks this up:

1. **Confirm the diagnosis with `perf record`** on
   `sync_watch-shard_legacy contention_resubscribe/100`. Expect to see hot
   samples in `try_acquire_meta_watcher`, `begin_park`/`end_park`, and the
   `ShardedMioHandle::unpark` swap.
2. **Try meta-watcher pinning**. Bind meta-watcher to worker 0 by
   construction; remove the per-park CAS. Measure both
   `contention_resubscribe/*` (expect win) and `io_busy_owner/*` (expect
   no regression — meta is still drained, just always by the same worker
   when worker 0 is parked; `try_steal_drain` covers the case where it
   isn't).
3. **Try fold meta into own-child**. Each worker registers all peers'
   child epoll fds into its own `mio::Poll` with worker-idx tokens. The
   per-park CAS goes away because there's no longer a special "meta
   parker" role. Cost: every fd registration/deregistration replicates
   to all workers' Polls — likely too expensive for non-burner workloads.
   Probably a non-starter, but worth listing as the simplest "no
   special role" alternative.
4. **Don't touch the Step 4 hybrid park flow** for any of this — the
   timer integration is independent and clean.

## Files of record

- `benches/sync_watch.rs` — bench source, 6 workers fixed, N_TASKS ∈
  {10, 100, 500, 1000}, 100 iterations of `send` + wait-for-N-acks.
- `PLAN-fix-messaging-regression.md` — earlier analysis of the
  unconditional-double-wake bug in `ShardedMioHandle::unpark`, already
  fixed.
- `WATCHER_GATE_DESIGN.md` — design doc for the meta-watcher + steal
  scheme; explains why the per-park CAS exists.
- `tokio/src/runtime/io/sharded_mio_driver.rs::ShardedMioHandle::unpark`
  — current cross-worker wake delivery path (single `external_waker`
  write).
- `tokio/src/runtime/scheduler/multi_thread/sharded_mio_park.rs` — park
  state machine; `park_internal` is the function to optimize.
