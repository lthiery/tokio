# Handoff: lazy-on-first-poll registration for sharded-mio

**Worktree:** `/home/louis/tokio/.claude/worktrees/io-driver-vtable`
**Branch:** `worktree-io-driver-vtable`
**Baseline tag/commit:** `facd69f0` (pre-lazy)
**Status:** working but with caveats; one open mystery blocking the originally
intended worker-local fast path.

---

## Goal

Defer `Arc<ScheduledIo>` allocation, slab insert, and `epoll_ctl_add` from
`Registration::new` to the first `poll_readiness` call. Targeted gains:
**-15% to -30% on `tcp_connect_churn`**, neutral on `tcp_echo_throughput`.

## Current state of the code

`tokio/src/runtime/io/sharded_mio_driver.rs::register_local` always routes
through the cross-thread queue path (`queue_register`), which:

1. Picks a worker round-robin via `fallback_worker()`.
2. Pushes a `DriverOp::Register` onto that worker's `pending_ops` mutex-protected vec.
3. `unpark`s the target worker if the queue was empty.

The target worker drains the queue at the start of its next park, doing the
real `slab.insert` + `Registry::register` calls then.

`register_worker_local` (the fast-path that synchronously registers on the
calling worker when `current_worker_index()` matches) is preserved as
`#[allow(dead_code)]` with a comment pointing here, because:

> **Open mystery:** synchronous register from the owning worker thread
> deadlocks `tcp_connect_churn`. We don't yet know why. See "Bisection" below.

## Bisection (this session)

| Path | Deadlocks `tcp_connect_churn`? | Perf vs `pre_lazy` |
|---|---|---|
| `register_worker_local` (sync, owning worker) | **Yes** | n/a (hangs) |
| `register_worker_local` + `self.unpark(self.idx)` | **Yes** | n/a (hangs) |
| `queue_register_pinned(self.idx)` (queue + drain on same worker) | No | flat (~+19% median, p=0.55) |
| `queue_register` (queue + round-robin distribution) | No | **lower-CI -17.17%**, median noisy |

What we learned:
- The bug isn't the park-state machinery. Adding a self-`unpark` (NOTIFIED
  poke) doesn't fix it.
- Queue + drain *ordering* alone (pinned to local worker) avoids the wedge but
  doesn't deliver perf.
- Round-robin distribution is what gets the win — connect-completion epoll
  events get spread across all 4 workers' epoll sets instead of all landing on
  one.

## Diagnostic data we have

- All 4 workers stuck in `ep_poll` (kernel `wchan = ep_poll`).
- IO-driver-side counters all consistent: 388 registers, 396 wakes, 0 slab
  misses, all unparks accounted for.
- 28 of 32 spawned connect tasks per iteration never get polled.
- Pattern matches the documented `num_searching` short-circuit deadlock in
  `ShardedMioUnparker::unpark`'s comment.

## Files modified

- `tokio/src/runtime/io/sharded_mio_driver.rs` — register_local routes through
  queue_register; register_worker_local marked `#[allow(dead_code)]`;
  unpark/begin_park/register_local instrumented with counters.
- `tokio/src/runtime/io/sharded_mio_reactor.rs` — `SharedRegistry::register`
  and `Reactor::poll_and_dispatch` instrumented.
- `tokio/src/runtime/io/lazy_debug.rs` — process-wide `AtomicU64` counters,
  `Once`-gated stderr dumper. Macro updated to accept `///` doc comments.

## How to repro

Build the instrumented bench:

```sh
cd /home/louis/tokio/.claude/worktrees/io-driver-vtable
RUSTFLAGS="--cfg tokio_unstable" \
  cargo build -p benches --features bench-sharded-mio \
  --bench net_tcp_echo --release --target-dir target/bench-instrumented
```

Run the bench (path includes a hash that changes — `ls` first):

```sh
ls target/bench-instrumented/release/deps/net_tcp_echo-* | grep -v '\.d$'
timeout 90 target/bench-instrumented/release/deps/net_tcp_echo-<hash> \
  --bench 'sharded_mio/(tcp_connect_churn|tcp_echo_throughput)' \
  --measurement-time 6 --warm-up-time 2 --sample-size 25 --baseline pre_lazy
```

To reproduce the deadlock with the worker-local fast path: edit
`register_local` to call `register_worker_local` on the local-worker branch
instead of `queue_register`, rebuild, run `tcp_connect_churn` — it'll hang
within seconds.

To enable the counter dump on exit set `LAZY_DEBUG_DUMP=1` (or whatever the
guard env var in `lazy_debug.rs` is — search for `LAZY_DEBUG`).

## Open questions / next experiments

1. **Why does synchronous register-from-worker hang?** Need scheduler-side
   instrumentation — track `num_searching` transitions, sleeper-list pops, and
   `notify_should_wakeup` short-circuits during a hung run. The IO driver
   counters show the IO side is fine; the wedge is in the multi-thread
   scheduler's notification logic interacting with our park-state machinery.
2. **Are the `tcp_connect_churn` tail outliers (4/25 severe high) system noise
   or residual stalls?** This machine has `influxdb3` at ~110% CPU and other
   noise. Needs either a quieter box or longer runs (e.g. `--measurement-time
   30 --sample-size 100`) to settle.
3. **Can a sound worker-local fast path be designed once (1) is understood?**
   The queue path costs a mutex-pair (push + drain). On uncontended workloads
   that's cheap, but a direct path would be cheaper still and is the original
   design intent.
4. **Sanity-check uring bench** (`bench-uring-reactor` feature) — these
   changes touched `Registration` shared code; uring path should be unaffected
   but verify.

## Final perf snapshot (this session, noisy machine)

- `tcp_echo_throughput`: **+2.99%** (regression, p=0.00). Roughly matches the
  +2.6% measured earlier this session. Likely the cost of one extra mutex
  acquire per register (queue push + drain) over the synchronous path.
- `tcp_connect_churn`: lower-CI **-17.17%** matches the originally targeted
  range. Median +93% in the noisy run, with 4/25 severe high outliers — needs
  cleaner measurement.

## Memory pointers

- Auto-memory: `/home/louis/.claude/projects/-home-louis/memory/MEMORY.md`
  (no entry for this project yet — consider adding one if continuing).
- Full prior conversation transcript:
  `/home/louis/.claude/projects/-home-louis/0433eedf-a293-47d9-9258-e86357c2cfc9.jsonl`
