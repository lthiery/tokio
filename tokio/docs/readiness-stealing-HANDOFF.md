# Readiness Stealing P1 — Handoff

**Status:** P1 landed at `293324f2` on `worktree-io-driver-vtable`.
**Worktree:** `/home/louis/tokio/.claude/worktrees/io-driver-vtable`.
**Design doc:** `tokio/docs/readiness-stealing.md`.

## What works

* Steal mechanism wired end-to-end:
  * `SharedRegistry::epoll_fd()` (Linux) → exposes the cloned mio epoll fd.
  * `ShardedMioHandle::try_steal_pass()` → round-robin non-blocking
    `libc::epoll_wait(timeout=0)` on each peer.
  * `SharedRegistry::steal_dispatch()` → decode tokens, gen-check, fire
    ScheduledIo wakers under the existing `ops` lock.
* Race-free park/steal interlock via a new `STEALING: usize = 3` park
  state (lock CAS `EMPTY → STEALING`, release CAS `STEALING → EMPTY`
  preserving any racing `NOTIFIED`). `begin_park` spins on `STEALING`.
* Steal hook in `ShardedMioParker::park_internal` runs between
  `drain_pending_ops` and `begin_park`; if events were harvested the
  worker skips its own `poll.poll()` (`park_skip_after_steal`).
* All sharded-mio integration tests pass under repeated runs:
  * `rt_sharded_mio` — 7 / 7
  * `net_sharded_mio_tcp` — 4 / 4 (parallel and `--test-threads=1`,
    5×5 cycles each)
  * No regressions vs. main.
* Counters added (lazy_debug): `steal_pass_calls`, `steal_pass_visits`,
  `steal_events_harvested`, `steal_events_woken`, `steal_eagain`,
  `steal_errors`, `steal_eintr`, `steal_slab_miss`,
  `steal_gen_mismatch`, `steal_waker_token`, `begin_park_steal_spin`,
  `park_skip_after_steal`.
* `io_busy_owner` bench wired (`benches/io_busy_owner.rs`,
  `bench-sharded-mio` feature gates the sharded backend group).

## Bench numbers

`io_busy_owner`, 4 workers / 16 fds / 3 burners (50 ms each):

| Backend / scenario                | latency       | vs idle |
|-----------------------------------|---------------|---------|
| traditional / idle                | ~baseline     | 1.0×    |
| traditional / 3 burners           | ~baseline     | ~1.0×   |
| sharded-mio / idle                | ~300 µs       | 1.0×    |
| sharded-mio / busy, **trial 0**   | **392 µs**    | ~1.3×   |
| sharded-mio / busy, trial 1+      | ~48 ms        | ~150×   |

The cold-start trial proves the design hits its Phase-0 target
(<1 ms vs the original 45.6 ms = ~115× win). Steady-state trials
regress to BURNER_MS — see open issue below.

## Open issue: trial-1+ steady-state regression

After the first iter wins, every subsequent iter takes exactly the
50 ms BURNER_MS. **It is not a deadlock** — the bench completes — but
the steal-induced speedup only fires on the cold runtime.

### What I confirmed

* Counters show stealing IS firing on later trials:
  * `steal_pass_calls = 209` across 10 trials (~21 / trial).
  * `steal_events_harvested = 78` (~8 / trial — about half of the 16
    events that the kicker fires per trial).
  * `park_skip_after_steal = 11` (~1 / trial — the steal-then-skip
    fast-path is taken once per trial).
  * `begin_park_steal_spin = 7` (peer-vs-stealer interlock fires
    rarely; bounded as designed).
* `dispatch_woken = 2` across 10 trials — i.e. essentially no wakes
  flow through the normal `poll.poll() → dispatch_events` path; almost
  everything routes through steal.
* All sharded_mio integration tests pass (so the steal logic itself is
  not the regression).
* Pattern holds with both per-trial `block_on` and a single outer
  `block_on` containing 10 sub-iters.
* Pattern holds at 4-worker / 16-fd / 3-burner *and* at
  2-worker / 2-fd / 1-burner (in the latter, ~50 % of trials still
  hit the 50 ms slow path — partial win).

### Hypotheses I have NOT yet verified

1. **Half-harvested events.** Counters say only ~8 of 16 events get
   stolen per trial. Where do the other 8 go?
   * `dispatch_woken = 2` across 10 trials, so they don't reach normal
     dispatch either. Yet probes complete (else `h.await` would hang).
   * Suspect: events are still on a busy peer's epoll fd when the
     burner exits at 50 ms; the peer then enters its own park flow
     and either dispatches them via `poll.poll()` (but counter says
     no) or somehow short-circuits. Worth instrumenting
     `dispatch_events_total` per-trial to disambiguate.

2. **`schedule_local` placement.** Inside `steal_dispatch`,
   `with_current()` should resolve to the stealer's worker context
   and `schedule_local` should push to the *stealer's* LIFO/run
   queue — verified via reading `multi_thread::worker::schedule_task`.
   But if `cx.core.borrow_mut()` ever sees `None` during steal, we'd
   fall through to `push_remote_task` (inject queue) and the task
   would land somewhere idle workers can pick it up. With 3 burners,
   only one idle worker exists — the stealer itself, which is mid-
   `park_internal` and can't drain inject until it returns. **Worth
   adding a counter for `schedule_local-vs-push_remote` choice during
   our steal pass.**

3. **`registers_local_calls` counter mismatch.** Across 10 trials we
   should see 160 fd registrations; counter showed 96. Either the
   dumper is racing `process exit` (250 ms cycle, total run ≈500 ms),
   or AsyncFd::with_interest is short-circuiting in some trials.
   Needs verification.

4. **Cold-start specific path.** Trial 0 always wins (~400 µs).
   Difference vs trial 1+: in trial 0 all 4 workers start parked, so
   spawn-via-inject + notify_parked_remote distributes the 16 probes
   roughly 4-per-worker — every worker's epoll fd has events, the
   free worker's own `poll.poll()` handles its 4, and pre-park steal
   harvests the other 12 in one batch. In trial 1, runtime is "warm"
   and registration distribution may collapse onto one worker (the
   one fastest to drain inject), which alters the steal arithmetic.

### Recommended first investigation

Add 2 cheap counters and re-run:

```rust
// in steal_dispatch / wherever io.wake fires
COUNTERS.steal_dispatch_local_schedule    // schedule_local taken
COUNTERS.steal_dispatch_remote_schedule   // push_remote_task taken
```

Plus per-trial counter snapshot via a small extension to
`lazy_debug`:

```rust
pub(crate) fn snapshot_pub() -> Vec<(&'static str, u64)> {
    COUNTERS.snapshot()
}
```

Then dump deltas around each trial in a private repro (don't ship
the snapshot API).

If `steal_dispatch_remote_schedule` is non-zero, that's the bug:
fix is to bias `with_current` lookup or call `schedule_local`
directly with the stealer's `core`.

If both counters show local placement and the mystery persists,
instrument `transition_from_parked` — possible the stealer's
`has_tasks()` is observing stale state due to a missed memory
barrier between `schedule_local`'s `push_lifo` and the read in
`worker.rs::park`.

### Result of the schedule-routing investigation (session 2)

Counters added in lazy_debug:

```
steal_dispatch_local_schedule
steal_dispatch_remote_schedule
```

Plus a thread-local `IN_STEAL_DISPATCH` flag (RAII guard
`StealDispatchGuard::enter()` set inside `steal_dispatch`'s wake
loop) so `Handle::schedule_task` in `multi_thread/worker.rs` can
attribute its local-vs-remote branch to this path only.

Full `cargo bench --bench io_busy_owner -- 'sharded_mio/busy_owner_3burners'`
run, 163 iters (criterion warmup + 100 measurements), 16 fds/iter
= 2608 fd registrations. Aggregate counters:

| counter                          | total | per-trial |
|----------------------------------|-------|-----------|
| sr_register_calls                | 2608  | 16.0      |
| dispatch_woken                   |  253  |  1.6      |
| steal_events_woken               | 2355  | 14.4      |
| **steal_dispatch_local_schedule**| **2355**| **14.4** |
| **steal_dispatch_remote_schedule**| **0**  | **0**    |
| steal_pass_calls                 | 6560  | 40.2      |
| steal_pass_visits                | 2883  | 17.7      |
| steal_events_harvested           | 2357  | 14.5      |
| park_skip_after_steal            |  337  |  2.1      |
| begin_park_steal_spin            |  217  |  1.3      |

Two clean conclusions:

1. **Hypothesis #2 (push_remote_task fall-through) is rejected.**
   100 % of steal-woken tasks take the `schedule_local` branch.
   Every harvested event lands on the stealing worker's local
   queue, exactly as the design intends. No
   `with_current`/core-borrow miss is happening.

2. **Hypothesis #1 (half-harvested events) is also rejected at
   full bench length.** 14.4 stolen + 1.6 dispatched = ~16.0 fd
   events accounted for per trial. The earlier "8/trial" figure
   came from the `--quick` run (10 trials only). Steady-state
   harvesting works.

So the steal mechanism is correct end-to-end: events get stolen,
decoded, fired, and routed to the stealing worker's local run
queue. **And yet trial 1+ still measures ~42 ms per iter
(criterion mean 41.877 ms).** The bug is downstream of
`schedule_task` — somewhere between "task on stealing worker's
local run queue" and "probe handle's `await` resolving."

### Next hypotheses (session 3)

Working hypotheses for where the 42 ms is now hiding:

* **A. Steal pass timing.** All 16 events get stolen per trial,
  but maybe across many parks spread over the full BURNER_MS, not
  in one batch. If the stealer (the only free worker, also the
  one running `block_on`'s main task) is itself busy running
  freshly-stolen probes between parks, peer events accumulate
  briefly but the stealer doesn't get back to a park to harvest
  them until the main task awaits the *next* probe handle.
  Verify: dump per-trial deltas of `steal_pass_calls` /
  `park_skip_after_steal` — if `park_skip_after_steal ≈ 2/trial`
  but events come 2 at a time, that's the steady-state tax.
  The `lazy_debug::snapshot_pub` extension this doc mentions is
  the right next step.

* **B. `transition_from_parked` ordering.** Tasks land on
  `core.run_queue` from inside `steal_dispatch`, then the parker
  returns up through `park_internal` and `park`'s while-loop calls
  `transition_from_parked(&worker)`. If that check uses an idle
  state field (e.g. `num_searching`) that doesn't reflect the
  newly-pushed tasks until a separate barrier, the worker may
  loop back into another `park_internal` with non-empty queue.
  Instrument `transition_from_parked`'s return value across
  back-to-back parks where steal happened.

* **C. Probe-handle wake routing.** When a probe completes, its
  `JoinHandle`'s waker fires `schedule_task` for the *main bench
  task* (the one in `block_on`). If the main task's home-worker
  association steers it to the inject queue (because the firing
  thread's `cx` doesn't match), each probe completion costs a
  full `notify_parked_remote` round-trip. Check
  `scheduler_metrics.remote_schedule_count` per trial.

The `IN_STEAL_DISPATCH` guard + `steal_dispatch_*_schedule`
counters live in tree (293324f2…HEAD). Reuse for hypothesis-C
investigation by widening the guard to also cover
`JoinHandle::poll` or by adding a separate flag.

### Per-trial delta dump (session 2 follow-up)

`lazy_debug` now has a per-trial delta dumper triggered by
`TOKIO_LAZY_DEBUG_TRIAL=N`. Every Nth `apply_deregister_calls`
increment, the dumper takes a snapshot, computes deltas vs the
previous boundary, and prints to stderr. With `N=16` (the bench's
`NUM_PROBES`), each dump is exactly one bench iter.

`TOKIO_LAZY_DEBUG_TRIAL=16 io_busy_owner --bench --quick
'sharded_mio/busy_owner_3burners'` (excluding trial 1 which
includes startup):

| trial | dispatch_woken | steal_woken | local_sched | park_skip | begin_park_parked | dispatch_calls | steal_pass_visits | steal_eagain |
|-------|---------------:|------------:|------------:|----------:|------------------:|---------------:|------------------:|-------------:|
| 2     | 12             | 4           | 4           | 1         | 11                | 11             | 15                | 12           |
| 3     | 5              | 11          | 11          | 1         | 10                | 10             | 15                | 13           |
| 4     | 4              | 12          | 12          | 1         | 12                | 12             | 22                | 20           |
| 5     | 9              | 7           | 7           | 1         | 12                | 12             | 22                | 20           |
| 6     | 6              | 10          | 10          | 1         | 12                | 12             | 19                | 17           |
| 7     | 8              | 8           | 8           | 1         | 11                | 11             | 17                | 15           |

Three **decisive** observations:

1. **`steal_pass_visits` ≈ 15-22 of `steal_pass_calls × 3 ≈ 45`
   per trial — only ~30-50 % of CAS attempts succeed.** The CAS in
   `try_steal_pass` is `EMPTY → STEALING`; it fails on `PARKED`,
   `STEALING`, *or `NOTIFIED`*. With `unpark_was_empty ≈ 5/trial`
   converting peer state to `NOTIFIED`, a burner-running peer that
   received an unpark (e.g. a `notify_parked_remote` from a probe
   spawn) sticks at `NOTIFIED` indefinitely — the burner never
   reaches `begin_park` to consume it. **The stealer cannot harvest
   from `NOTIFIED` peers, even though epoll-stealing is perfectly
   safe in that state** (peer is not in `epoll_wait`).

2. **`steal_eagain` ≈ `steal_pass_visits` − a few**: among the
   ~15-22 successful CAS visits per trial, only 2-3 actually find
   events on the peer's epoll. The rest of the events are stuck
   on `NOTIFIED`-stuck peers we can't even probe.

3. **`park_skip_after_steal = 1/trial`** — exactly one steal pass
   per trial harvests > 0 events. Subsequent parks within the
   trial keep failing the CAS on the same `NOTIFIED` peers and
   come back empty.

So the trial-1+ regression isn't about local-vs-remote scheduling
or about half-harvested events — it's a **liveness bug in the
park-state CAS**: stealing is gated on `EMPTY`, but a busy peer's
`park_state` accumulates `NOTIFIED` from cross-thread unparks and
never drains because the burner doesn't yield to its parker. The
fix is to relax the CAS to accept both `EMPTY` *and* `NOTIFIED`,
preserving the `NOTIFIED` flag on release.

### Recommended fix (session 3 — not yet applied)

In `sharded_mio_driver.rs::try_steal_pass`, replace the single
`compare_exchange(EMPTY, STEALING)` with a load-and-CAS loop that
accepts `EMPTY | NOTIFIED`, remembers the original, and on
release CAS-restores `STEALING → original` (still using CAS, not
store, to preserve any *new* `NOTIFIED` that raced in during our
syscall):

```rust
let original = loop {
    let prev = slot.park_state.load(Ordering::Acquire);
    if prev == PARKED || prev == STEALING { continue 'peers; }
    if slot.park_state
        .compare_exchange(prev, STEALING, AcqRel, Acquire)
        .is_ok() { break prev; }
};
// ... epoll_wait + dispatch ...
let _ = slot.park_state.compare_exchange(
    STEALING, original, Release, Relaxed,
);
```

Validate first by adding three sub-counters
(`steal_cas_fail_parked`, `steal_cas_fail_notified`,
`steal_cas_fail_stealing`) to confirm `notified` dominates the
failure mode. If yes, apply the fix and re-measure — expect trial
1+ to drop to single-digit-ms (matching trial 0).

Hypotheses A/B/C from the previous section may now be moot if
the CAS relaxation alone moves the bench to <1 ms steady state.
If a residual ~40 ms tail remains after the CAS fix, return to
those.

## File map

```
benches/Cargo.toml                                              [+5]
benches/io_busy_owner.rs                                       [+235] new
tokio/docs/readiness-stealing.md                               [+257] new (design)
tokio/src/runtime/io/lazy_debug.rs                              [+25]
tokio/src/runtime/io/sharded_mio_driver.rs                    [+196]
tokio/src/runtime/io/sharded_mio_reactor.rs                   [+100]
tokio/src/runtime/scheduler/multi_thread/sharded_mio_park.rs   [+35]
```

## Re-running everything

```sh
cd /home/louis/tokio/.claude/worktrees/io-driver-vtable
. ~/.cargo/env

# integration tests
RUSTFLAGS="--cfg tokio_unstable" cargo test --release -p tokio \
  --features full,io-sharded-mio --test rt_sharded_mio
RUSTFLAGS="--cfg tokio_unstable" cargo test --release -p tokio \
  --features full,io-sharded-mio --test net_sharded_mio_tcp

# bench
RUSTFLAGS="--cfg tokio_unstable" cargo build --release -p benches \
  --bench io_busy_owner --features bench-sharded-mio
BENCH=$(ls -t target/release/deps/io_busy_owner-* \
        | grep -v '\.d$' | head -1)
"$BENCH" --quick                       # all 4 groups
TOKIO_LAZY_DEBUG=70 "$BENCH" --quick   # with counters

# net_sharded_mio_tcp under high parallelism (was the original
# concern about per-worker epoll behaviour)
TCP=$(ls -t target/release/deps/net_sharded_mio_tcp-* \
      | grep -v '\.d$' | head -1)
for i in 1 2 3 4 5; do timeout 20 "$TCP" 2>&1 | tail -3; done
```

## Phases NOT yet started

* **P2 — adaptive cadence.** Heuristic: only steal every Nth park, or
  only when the per-worker `dispatch_woken` rate is below a threshold.
  Reduces steal syscall overhead in the steady-state-busy idle case
  where every worker is genuinely working its own queue.
* **P3 — victim selection.** Replace round-robin with a
  recently-active-but-not-parking heuristic (e.g. workers whose
  `unpark_was_empty` count is climbing — proxy for "has events but
  isn't polling").
* **P4 — steal-on-block.** Allow the parked worker's `poll.poll()` to
  be interrupted by a peer that wants to take over its epoll fd
  during a long burner-induced stall. Requires the timeout-park dance
  outlined in the design doc §4.4.

The trial-1+ regression is in scope for "tightening P1", not P2/P3 —
P1's contract is "always-on round-robin steal works correctly."

## Loose ends

* The dump thread is started lazily on first `bump`. For benches that
  exit in <250 ms there's no dump. Consider an explicit
  `lazy_debug::dump_now()` flushed on `Drop`.
* `dispatch_woken = 2` over 10 trials in the bench is suspicious. May
  indicate a counter-bump-site mismatch (only writeable bits get
  counted? — unlikely, code increments unconditionally) or that
  `poll.poll()` is genuinely never producing fd events in this
  workload (everything was already stolen). Re-check after the
  trial-1+ fix.
* Earlier iteration tried gating steal via `peer.park_state == EMPTY`
  load (without CAS) — that race is now closed by the STEALING lock.
  No remnants in tree.

## Session 4 — CAS relaxation + outer-task hypothesis

**Change applied:** `try_steal_pass` no longer rejects `NOTIFIED`
peers. The entry CAS is a load-and-CAS loop that accepts both
`EMPTY` and `NOTIFIED` as valid origin states; the release CAS
restores the original. After stealing from a `NOTIFIED`-origin
peer we re-fire the peer's `external_waker` to compensate for any
queued `WAKER_TOKEN` byte we may have consumed (kernel
exactly-once delivery on the shared epoll fd). The
`steal_cas_fail_notified` counter was repurposed/renamed to
`steal_entered_notified` (success-path).

**Tests:** `rt_sharded_mio` 7/7 + `net_sharded_mio_tcp` 4/4 still
pass.

**Bench result:** Negligible. `sharded_mio/busy_owner_3burners`
remains pinned at the burner deadline (~43 ms across two 20-sample
runs, vs. ~45 ms pre-fix). One 10-sample run measured ~21 ms but
did not reproduce.

**Per-trial counter deltas (BURNER_MS=50, 16 probes, 3 burners),
trial #118:**

| counter                       | delta |
|-------------------------------|-------|
| steal_pass_calls              | 48    |
| steal_pass_visits             | 73    |
| steal_events_harvested        | 50    |
| steal_events_woken            | 16    |
| steal_entered_notified        | 38    |
| steal_cas_fail_parked         | 62    |
| steal_cas_fail_stealing       | 10    |
| steal_dispatch_local_schedule | 16    |

Across the full run: `steal_events_woken=1483 + dispatch_woken=405
= 1888 = rin_calls`. **Every probe is being woken**, ~78 % via
the steal path and ~22 % via the stealer's own `poll.poll()`.
None are stuck waiting for a burner to die.

**So why is the iter still 43 ms?** New hypothesis: the bottleneck
is not probe wake — it's the *outer iter task*. `one_iter` awaits
`probe_handles` sequentially:

```rust
for h in probe_handles {
    h.await.expect("probe");
}
```

The outer task lives on whichever worker `block_on` parks on. If
that worker is one of the three running a burner, every
`h.await` resume is scheduled onto a burner-pinned queue.
`schedule_task` → `with_current` finds the current cx (the worker
running the *probe* task on the stealer), `ptr_eq` succeeds (same
scheduler), and `schedule_local` pushes the outer task to the
*stealer's* queue — which should be fine. But if the wake fires
from outside a worker context (e.g., the `JoinHandle`'s waker is
invoked from a non-Tokio thread, or the probe task is mid-tear-
down on a different worker), it falls through to
`push_remote_task` → inject queue → next worker to drain inject
picks it up. With 3/4 workers spinning, that next-worker pickup
could easily be 50 ms away.

This is consistent with the bench shape (always = burner deadline)
and with the counters (every probe wakes). It also explains why
the cold trial is fast (no burner pressure) but warm trials are
not.

**Suggested next step (still inside "tighten P1" scope):**
instrument `Handle::schedule_task`'s remote fallback with a
counter for the *non-steal-dispatch* case — i.e. how often the
ordinary scheduler routes a task to the inject queue under this
bench. If that counter is 16-ish per trial, the outer-task
hypothesis is confirmed and the fix is to prefer local-or-stealer
scheduling when the original owner is `STEALING|PARKED|busy`.

If the counter says the outer-task wake is also local, the
bottleneck is something more subtle (queue drain ordering inside
the stealer when it has 17+ tasks queued, perhaps).
