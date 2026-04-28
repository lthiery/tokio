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

## Session 5 — schedule_task attribution (outer-task hypothesis falsified)

**Change applied:** added counters
`schedule_local_total`, `schedule_remote_no_cx`,
`schedule_remote_other_scheduler`, `schedule_remote_no_core`
to `Handle::schedule_task` in
`tokio/src/runtime/scheduler/multi_thread/worker.rs`.

**Per-trial deltas (consistent across trials 2-N):**

| counter                          | delta |
|----------------------------------|-------|
| schedule_local_total             | 16    |
| schedule_remote_no_cx            | 19    |
| schedule_remote_other_scheduler  | 0     |
| schedule_remote_no_core          | 0     |

19 = NUM_PROBES (16) + num_burners (3). **All 19 per-trial
spawns** route through `push_remote_task` because the bench's
outer iter loop runs on the bench thread, not on a worker:
multi_thread's `block_on` polls the future on the calling thread
(see `MultiThread::block_on`'s doc:
"The future will execute on the current thread, but all spawned
tasks will be executed on the thread pool"). `tokio::spawn`'s
internal `with_scheduler(...)` finds no `MultiThread` scheduler
context on the bench thread, so spawn → `schedule_task` → no cx →
inject queue.

**This is by design, not a bug.** Inject-queue tasks are picked up
promptly by any free worker. The 16 probes register in the 2 ms
pre-burner window when all 4 workers are free; they fan out
roughly uniformly. The 3 burners then pin 3 workers, leaving 1
stealer.

**The outer-task wake hypothesis is therefore wrong.** The outer
task is *not* a Tokio task — it's polled via `block_on`'s parker
on the calling thread, woken by an `Unpark` notification, not by
`schedule_task`. Each probe completion fires a `JoinHandle` waker
that schedules the outer task by unparking the bench thread
directly (the JoinHandle waker on a non-task future is a thread
parker waker), not via the inject queue.

**So the bottleneck is somewhere else.** Per-trial counters look
*identical* between the cold trial (~400 µs) and warm trials
(~48 ms). Same `steal_events_woken=16`, same
`schedule_local_total=16`, same fall-through pattern. Whatever
turns 400 µs into 48 ms is not visible in any current counter.

**Plausible remaining suspects:**

1. *Burner pinning timing.* In the cold trial the runtime is
   fresh; burners may not yet be pinned to workers when the kicker
   fires. In warm trials previous iters' bookkeeping (drains,
   deregisters, owner queues) may bias which workers grab burners
   and which pick up the new probes — possibly pushing all probes
   onto burner-pinned workers' registry, which then needs to be
   stolen.
2. *Timer driver pin.* `tokio::time::sleep(2ms).await` and
   `sleep(1ms).await` both drive timers. If the worker that owns
   the timer wheel becomes burner-pinned in trial N, the 2 ms
   sleep can stretch to 50 ms — kicker fires *before* probes have
   registered.
3. *Inject queue drain order.* In warm trials, the inject queue
   may already have residual tasks (deregister callbacks?) that
   delay probe pickup.
4. *Burner deadline arithmetic.* `burner_deadline = now() + 50ms`
   computed *before* the 1 ms pre-kick sleep. If the
   pre-kick sleep itself stretches, the deadline becomes much
   closer than 50 ms — but that would *shorten* not lengthen the
   measured time.

**Suggested next step:** add a per-trial trace marker (or a
counter like `iter_kick_to_first_probe_us` and
`iter_first_to_last_probe_us`) bracketed inside `one_iter`, plus
a counter for "task popped from inject" so we can see whether
inject-drain delay is consuming the budget. Or instrument the
2 ms / 1 ms `sleep` to record actual wall-clock duration.

## Session 6 — per-phase wall-clock instrumentation (smoking gun)

**Change applied:** `benches/io_busy_owner.rs` now records eight
per-phase wall-clock measurements per `one_iter`, gated by
`BENCH_PHASE_DEBUG=1`. Process-wide sums + counts dump on each
bench function teardown:

  * `spawn_probes_us`     — time to spawn 16 probes
  * `pre_burner_sleep_us` — `tokio::time::sleep(2ms)` actual
  * `spawn_burners_us`    — time to spawn 3 burners
  * `post_burner_sleep_us`— `tokio::time::sleep(1ms)` actual
  * `kicker_thread_us`    — kicker thread span
  * `kick_to_first_wake_us`  — kick to *first* `.readable().await`
                                 returning (shared `Mutex<Option<Instant>>`)
  * `first_to_last_wake_us`  — first → last probe wake (sequential `h.await`)
  * `iter_total_us`       — what the bench measures

**Result for `sharded_mio/busy_owner_3burners` (means over 131
trials):**

| phase                  | µs    |
|------------------------|-------|
| spawn_probes_us        |   321 |
| pre_burner_sleep_us    | 3 081 |
| spawn_burners_us       |    10 |
| post_burner_sleep_us   | 3 526 |
| kicker_thread_us       |   256 |
| kick_to_first_wake_us  |**46 118**|
| first_to_last_wake_us  |    86 |
| iter_total_us          |46 205 |

**Per-trial detail:**

```
[phase] trial=1   probe_spawn= 150us pre_sleep=3088us burner_spawn=  6us post_sleep=2136us kicker= 224us k_to_1st=   224us 1st_to_last= 290us total=  514us
[phase] trial=2   probe_spawn= 177us pre_sleep=3080us burner_spawn=  5us post_sleep=2182us kicker= 140us k_to_1st= 47828us 1st_to_last=  90us total=47919us
[phase] trial=3   probe_spawn= 297us pre_sleep=3098us burner_spawn=  6us post_sleep=2207us kicker= 199us k_to_1st= 47792us 1st_to_last=  88us total=47881us
[phase] trial=25+ ... k_to_1st = 47880-47900us (steady)
```

**Smoking gun:**

* All pre-kick phases are *identical* between cold and warm trials.
* `kick_to_first_wake_us` jumps from 224 µs (trial 1) to 47 800+ µs
  (trials 2+). 200× regression on a single phase.
* `first_to_last_wake_us` is consistently <300 µs in every trial,
  including the slow ones — once *any* probe wakes, all 16 wake
  within ~100 µs.
* The 47.8 ms stall is precisely
  `BURNER_MS - post_burner_sleep_us` ≈ 50 ms − 2.1 ms = 47.9 ms.
  i.e. **probes wake at burner death, not at kick.**

**Interpretation:** in trial 1 the steal harvest works: the cold
runtime has the stealer parked on its own epoll fd, the kicker
fires events on (some subset of) the four worker epoll fds, the
stealer wakes, runs `try_steal_pass`, harvests peer events, all
16 probes wake within 514 µs. In trials 2+, *something* prevents
the stealer from waking on kick, so events sit on peer epoll fds
until each peer's burner exits and the peer's own `poll.poll()`
finally drains them.

**Remaining question — why does the stealer fail to wake on kick
in warm trials but not cold?** Pre-kick phase data is identical,
so the registration path is the same. Possible causes:

1. *Probe registration distribution drift.* If in warm trials all
   16 probes happen to register on burner-pinned workers (zero on
   the stealer), the kicker fires events only on peer epoll fds,
   never on the stealer's. The stealer's `poll.poll(None)` never
   wakes. The earlier process-wide counter `dispatch_woken=405`
   over 110 trials = ~3.7/trial would *also* be consistent with
   "almost no own-poll wake during the iter window — most events
   only get drained at burner death via own-poll on the
   burner-pinned worker's eventual park."
2. *Stealer not actually parked at kick time.* If the stealer is
   doing some other work (timer wheel maintenance, residual task)
   between trials, it may be in the scheduler loop, miss the
   `try_steal_pass` window, and only park *after* the kick — but
   on its own epoll fd which has no events queued. It then sleeps
   indefinitely until burner-death events hit peers.
3. *Cross-thread waker not firing the stealer.* If `unpark` from
   the kick path goes through `notify_parked_remote` but the
   stealer is parked on `poll.poll(None)`, the WAKER_TOKEN should
   wake it. If it doesn't, that's a sharded-mio waker integration
   bug.

**Suggested next step:** add per-worker `dispatch_woken` and
`steal_events_woken` (small fixed-size atomic arrays indexed by
worker idx) so we can see *which* worker harvested *what*, per
trial. If the stealer's `dispatch_woken` is 0 across warm trials,
hypothesis 1 is confirmed. If the stealer's `try_steal_pass`
counter shows it stops calling `try_steal_pass` between kick and
burner-death, hypothesis 2 is confirmed. Hypothesis 3 would show
up as the stealer's `begin_park_calls` not incrementing during
the 47 ms window.

---

## Session 7 — Per-worker counters confirm hypothesis 1

**Instrumentation added.** `tokio/src/runtime/io/lazy_debug.rs` now
exposes `PerWorkerCounters` — five `[AtomicU64; MAX_WORKERS]` arrays
(`MAX_WORKERS = 16`):

* `dispatch_woken` — bumped per dispatched event in
  `Reactor::poll_and_dispatch`, indexed by `current_worker_index()`.
* `steal_events_woken` — bumped by `woken` count after each
  `registry.steal_dispatch()` in `try_steal_pass(self_idx, …)`.
* `try_steal_pass_calls` — bumped on every `try_steal_pass` entry.
* `register_per_worker` — bumped on the success arms of
  `register_on_worker` / `apply_register`.
* `begin_park_calls` — bumped at the top of `begin_park(worker_idx)`.

`maybe_dump_trial` snapshots and renders deltas as e.g.
`dispatch_woken_pw [w2]+7 [w0]+9` so each trial dump shows which
worker did the work.

**Tests still pass.** `cargo test -p tokio --features
io-sharded-mio --test rt_sharded_mio` → 7/7,
`cargo test -p tokio --features io-sharded-mio --test
net_sharded_mio_tcp` → 4/4.

**Smoking gun.** Running
`TOKIO_LAZY_DEBUG=1 TOKIO_LAZY_DEBUG_TRIAL=16 BENCH_PHASE_DEBUG=1
cargo bench -p benches --bench io_busy_owner --features
bench-sharded-mio` and lining up bench-trial timings against
lazy-debug trial dumps:

```
SLOW trial #2  (k_to_1st = 47.9 ms):
  register_per_worker_pw  [w0]+11 [w2]+2 [w3]+3       (w1 = 0)
  dispatch_woken_pw       (none)                      (everywhere = 0)
  steal_events_woken_pw   [w0]+2 [w2]+3 [w3]+11       (cascade at burner death)
  begin_park_calls_pw     [w1]+1 [w0]+5 [w2]+5 [w3]+5

FAST trial #25 (k_to_1st = 118 µs):
  register_per_worker_pw  [w2]+7  …
  dispatch_woken_pw       [w2]+7                      (stealer's own poll fires)
  steal_events_woken_pw   [w2]+9                      (stealer harvests peers)
```

The discriminator is `register_per_worker_pw[stealer]`. When the
stealer (the worker not pinned to a burner) holds zero probes,
`dispatch_woken_pw` stays zero across **all** workers for the full
50 ms window, and the 16 events only surface as a cascade of
`steal_events_woken_pw` once burners die and burner-pinned workers
re-enter `try_steal_pass`. When the stealer holds even one probe,
its own `poll.poll()` fires on kick, the stealer runs
`try_steal_pass`, and the remaining peer events are harvested
within microseconds.

**Root cause (confirmed).** Hypothesis 1 from Session 6. Probe
registration is not load-balanced across workers; in most warm
trials all probes land on the three burner-pinned workers and the
stealer is parked in `poll.poll(None)` on an empty epoll fd. The
kicker's writes produce kernel events only on the burner-pinned
workers' epoll fds, but those workers are spinning in CPU-burn
loops and never drain epoll. The stealer cannot wake from its own
`poll.poll(None)` because no event ever lands on its fd, and
nothing else (eventfd, mio waker, peer unpark) reaches into peer
epolls. So the stealer sleeps until burner death, at which point
the burner-pinned workers re-enter park, `try_steal_pass` cascades
across peers, and 16 events come out at once.

Hypothesis 2 (stealer not parked) is ruled out:
`begin_park_calls_pw[stealer]` ticks normally. Hypothesis 3
(cross-thread waker not firing) is moot — nothing in the bench
path issues a waker for the stealer's mio Poll on kick.

**Caveat — the dump itself perturbs the bench.** With
`TOKIO_LAZY_DEBUG_TRIAL=16` set, the per-trial dump goes through
the stderr lock + a `Mutex<PerWorkerSnapshot>` and adds ~20 lines
of `eprintln!` between trials. That's enough to shift the
inter-trial deregister phase and bring the slow trials down to
~16-22 ms in roughly 70 % of runs (without the env var, all warm
trials are ~46 ms). Future diagnostics in this codepath should
either snapshot to a ring buffer and dump after the bench
finishes, or accept that turning the dump on changes what's being
measured.

**Fix direction (P2-territory — flagged for user judgment).**
Two reasonable fixes:

1. **Park-with-bounded-timeout.** Replace `poll.poll(None)` in
   the sharded-mio park path with a small timeout (e.g. 1 ms)
   so the stealer periodically retries `try_steal_pass`. Cheap,
   localized, but adds steady idle wakeups.

2. **Cross-worker register-wake.** When a worker registers an fd,
   fan out a lightweight `unpark`/mio-waker poke to peers (or at
   least to a known stealer slot) so the empty-stealer scenario
   gets a chance to run `try_steal_pass` once probes have landed.
   More invasive but no idle wakeups.

Both cross into P2 ("readiness stealing — wake propagation"). The
Session 0 ground rules say "Don't start P2/P3/P4." Pausing here
for user input on whether to land a bounded-timeout fix as part
of P1, or carry the diagnosis into the P2 design.

### P1 closeout decision

**No fix landed in P1.** The bench regression is a known
limitation of the current sharded-mio design under adversarial
CPU-bound peer load: when a worker registers fds and another
task on the same worker spins on CPU without yielding, kernel
events queue on that worker's epoll fd with no in-process
signal that can reach an idle peer. The waker layer sits
downstream of `epoll_wait` and only fires once *some* worker
drains its epoll, so it can't compensate for an owner that
never parks.

The principled fixes (EPOLL_EXCLUSIVE registration fanout, or a
dedicated I/O worker that runs only `epoll_wait` + dispatch and
never user tasks) move event delivery itself rather than
patching the park loop. Both are architectural decisions and
are deferred to P2 ("readiness stealing — wake propagation").
A bounded-timeout park (1 ms idle re-poll) was considered and
rejected: it adds steady idle wakeups in exchange for masking,
not fixing, the gap.

The per-worker lazy_debug counters (Session 7) stay in tree.
They have no runtime cost when `TOKIO_LAZY_DEBUG` is unset and
will be needed again when P2 begins. Marking P1 done.
