# Session 2: scheduler-side instrumentation of the worker-local fast-path hang

**Worktree:** `/home/louis/tokio/.claude/worktrees/io-driver-vtable`
**Branch:** `worktree-io-driver-vtable`
**Picks up from:** [`HANDOFF-lazy-register.md`](./HANDOFF-lazy-register.md), open
question #1 ("Why does synchronous register-from-worker hang?").

## What this session did

1. Extended `runtime::io::lazy_debug` with 33 new scheduler-side
   counters covering `Idle::*` and `Handle::notify_*` /
   `Worker::transition_*`.
2. Added a cfg-gated `sched_dbg` shim in
   `runtime::scheduler::multi_thread::counters` that forwards to
   `lazy_debug` when `io-sharded-mio` is on and is no-ops otherwise, so
   call sites in `idle.rs` / `worker.rs` are clean.
3. Instrumented `idle.rs` (`worker_to_notify`, `notify_should_wakeup`,
   `transition_worker_to_searching`, `transition_worker_from_searching`,
   `transition_worker_to_parked`, `unpark_worker_by_id`) and `worker.rs`
   (`notify_parked_local`/`_remote`, `notify_if_work_pending`,
   `transition_to_parked`, `transition_from_parked`).
4. Added a runtime toggle in `sharded_mio_driver::register_local`:
   setting `TOKIO_LAZY_FASTPATH=1` re-enables the synchronous
   `register_worker_local` fast path on the local-worker branch. With
   the env var unset, the safe `queue_register` path runs unchanged.
5. Reproduced the hang under `tcp_connect_churn` with the fast path
   enabled and captured per-thread `wchan` + counter snapshots.

## Repro

```sh
cd /home/louis/tokio/.claude/worktrees/io-driver-vtable
PATH="$HOME/.cargo/bin:$PATH" RUSTFLAGS="--cfg tokio_unstable" \
  cargo build -p benches --features bench-sharded-mio \
  --bench net_tcp_echo --release --target-dir target/bench-instrumented

# Hangs within ~1 s. TOKIO_LAZY_DEBUG=1 makes the lazy-debug dumper
# print every 250 ms to stderr.
TOKIO_LAZY_FASTPATH=1 TOKIO_LAZY_DEBUG=1 \
  timeout 30 \
  ./target/bench-instrumented/release/deps/net_tcp_echo-<hash> \
  --bench 'sharded_mio/tcp_connect_churn$' \
  --measurement-time 3 --warm-up-time 1 --sample-size 10
```

Without `TOKIO_LAZY_FASTPATH=1`, the bench completes normally
(`time = [1.35 ms 1.37 ms 1.39 ms]`).

## Per-thread state at the wedge (5 s after launch)

```
PID  TID    COMMAND          STATE  WCHAN
... 1120447 net_tcp_echo-e2  S      futex_do_wait
... 1120454 tokio-rt-worker  S      ep_poll
... 1120455 tokio-rt-worker  S      ep_poll
... 1120456 tokio-rt-worker  S      ep_poll
... 1120457 tokio-rt-worker  S      ep_poll
```

All 4 workers parked in `ep_poll`. Main thread blocked in
`block_on`'s parker. Same shape the prior session described.

## Frozen counter snapshot (steady state)

The counters quoted below were identical across every 250 ms dump for
the full 30 s timeout window — every counter has zero forward motion
once the wedge sets in.

```
register_worker_local_calls        163        # all registrations took the fastpath
register_worker_local_errors       0
queue_register_calls               0          # never used (env var on)
queue_deregister_calls             161
drain_calls                        87
drain_register_drained             0
drain_deregister_drained           161        # only deregisters drained

dispatch_calls                     63
dispatch_poll_ok                   59
dispatch_zero_events               2          # plus 4 returns w/ events == 0?
dispatch_events_total              200
dispatch_woken                     163
dispatch_woken_writable            159
dispatch_woken_readable            28

unpark_calls                       71
unpark_was_parked                  37
unpark_was_empty                   24
unpark_was_notified                10
begin_park_calls                   87
begin_park_parked                  63
begin_park_fastpath                24

# --- Idle::notify_should_wakeup (the prime suspect from the handoff) ---
idle_notify_should_wakeup_calls            339
idle_notify_should_wakeup_yes              103   # 30%
idle_notify_should_wakeup_no_searching     165   # 49%  ← short-circuit
idle_notify_should_wakeup_no_full           71   # 21%  ← short-circuit
idle_worker_to_notify_calls                285
idle_worker_to_notify_some                  49   # 17% actually pop a sleeper
idle_worker_to_notify_none_pre_lock        231
idle_worker_to_notify_none_post_lock         5

idle_transition_to_searching_calls         117
idle_transition_to_searching_yes           117   # never capped
idle_transition_to_searching_no_capped       0
idle_transition_from_searching_calls       101
idle_transition_from_searching_was_last     99   # 98% are "last searcher"
idle_transition_to_parked_calls             65
idle_transition_to_parked_was_last_searcher 59   # 91%
idle_transition_to_parked_was_searching     65   # all transitions came in searching
idle_unpark_worker_by_id_calls              19
idle_unpark_worker_by_id_hit                 8
idle_unpark_worker_by_id_miss               11

# --- worker.rs notify outcomes ---
worker_notify_parked_local_woke             40
worker_notify_parked_local_no_one           81   # 67% miss rate
worker_notify_parked_remote_woke             9
worker_notify_parked_remote_no_one         155   # 94% miss rate

# --- the smoking gun ---
worker_notify_if_work_pending_calls         59
worker_notify_if_work_pending_via_steal      0   # ←
worker_notify_if_work_pending_via_inject     0   # ←
worker_notify_if_work_pending_no_op         59   # ← every single call

# --- park-state transitions ---
worker_transition_to_parked_calls           65
worker_transition_to_parked_yes             65   # never bailed via has_tasks
worker_transition_from_parked_calls         83
worker_transition_from_parked_has_tasks     19
worker_transition_from_parked_not_in_sleeper 38
worker_transition_from_parked_still_in_sleeper 26   # spurious-wake reparks
```

## What this rules out

- **The `num_searching` cap is not the cause.**
  `idle_transition_to_searching_no_capped == 0` — workers never hit the
  `2*num_searching >= num_workers` cap on `transition_worker_to_searching`.
  The "limited searchers" optimization isn't squeezing anyone out.

- **The `notify_should_wakeup` SeqCst guard isn't lying.** It does
  short-circuit a lot (70% of calls), but it does so when a) someone
  is genuinely searching, or b) all workers are genuinely running. Both
  are valid reasons to skip the wake under the rule "an existing
  searcher will discover any submitted work."

- **The wedge is not "no events arrive."** Workers parked in `ep_poll`
  and counters frozen across 30 s shows *zero* kernel events delivered
  during the wedge — but registrations completed (163 alive at peak,
  161 deregistered, **2 still alive at freeze**: very likely the
  listener fd plus one other long-lived registration). Whichever
  worker holds those remaining FDs is parked in `ep_poll` waiting for
  events that the kernel won't generate without new client-side
  activity.

- **Each FD is bound to exactly one worker's `mio::Poll`.** With the
  fastpath, every `register_worker_local` lands the FD on whichever
  worker first polled the registering future. The listener FD is
  registered exactly once during server-task first poll; its
  accept-completion events fire on **only that worker's epoll**.

## What the data points to

The smoking gun is `worker_notify_if_work_pending` finding **0/59**
calls with steal or inject work despite the runtime obviously having
in-flight work that should rebalance.

`notify_if_work_pending` is the *final-searcher's* last-line check
when transitioning to parked. Its job is to catch tasks that landed
between the searcher's "I'm out" decision and its actual park. It
inspects:

```rust
for remote in &self.shared.remotes[..] {
    if !remote.steal.is_empty() { ... }
}
if !self.shared.inject.is_empty() { ... }
```

It does **not** look at any worker's LIFO slot, which is private to
its owner and not stealable.

Combined with the FD-binding asymmetry (`register_worker_local`):

1. Worker X spawns N tasks → first goes to LIFO, prev displaces to
   run-queue back. After spawn, LIFO holds 1, run-queue back holds N-1.
2. Tasks 1..N-1 in run-queue back are stealable. Worker X polls the
   LIFO task: a connect future. `register_worker_local` puts its fd on
   worker X. Connect returns Pending. X picks next from LIFO/run-queue.
3. Workers Y, Z, W steal connect tasks from X's run-queue. Their
   `register_worker_local` puts those fds on Y/Z/W's epoll.
4. As iterations proceed, the **listener FD** stays on whichever
   worker first polled the server task — call that worker L.
5. The bench task (block_on body) runs somewhere, awaits handle 0.
   When handle 0's task completes, its JoinHandle waker fires → bench
   task is rescheduled. The bench task can land in any worker's LIFO
   slot.

The wedge is reachable when:

- The bench task is sitting in some worker M's LIFO slot, scheduled
  but not yet polled (e.g., M was in the middle of finishing dispatch
  and the bench-task wake is queued behind a few connect-task wakes).
- Worker M finishes the dispatched batch, returns from park,
  `transition_from_parked` sees `has_tasks=true` (LIFO non-empty), pops
  the LIFO bench task, polls it, the bench task does some sync work
  and yields/awaits. Worker M tries to find more work, finds none in
  its run-queue / steal targets / inject, transitions to parked.
- At `transition_to_parked`, worker M is the *last searcher*, so
  `notify_if_work_pending` runs. It sees nothing in any steal queue
  and nothing in inject. (Real next work — maybe new spawned tasks
  from the bench body — is in M's *own* LIFO. But M is the one
  parking; LIFO inspection here would just confirm "yes I have work,"
  which is contradicted by the fact M just decided to park. So this
  isn't the LIFO blind spot.)
- Actually re-examining: if M genuinely has nothing in its LIFO and
  nothing else does either, then everyone is correctly idle.

So the LIFO blind spot isn't the right framing.

## A more precise hypothesis

Looking at the exact arithmetic:

- `dispatch_woken = 163`. That's the total wakes the IO driver ever
  issued. Once that count stops growing, *no more FD events will fire*
  until something changes the kernel state.
- `queue_deregister_calls = 161`, `drain_deregister_drained = 161`.
  All but 2 FDs were cleanly deregistered.
- The 2 remaining FDs at freeze are the listener (1) plus most likely
  one in-flight connect socket whose task got stuck mid-poll. Or both
  are listener-shape: with this bench, only the listener is long-lived.
  The "2nd alive FD" is most likely a connect socket whose task hit a
  state where it was woken, partially polled, then suspended again
  before its drop ran.

The actual stuck condition: at iteration ~5 of the bench (163/32 ≈ 5.1
iterations), one or more of the 32 spawned connect futures got into a
state where:

- Its socket is still registered (no Drop has happened).
- Its task is waiting for a wake.
- The wake source it's waiting on — either a kernel event on its FD,
  or a scheduler wake from a `JoinHandle::wake` propagation — is never
  going to fire because nothing is going to change the kernel state.

The `bench_task.handle.await` cycle: bench task awaits handle 0,
handle 0's wake propagates from task 0 completion. If task 0
*completes* but its completion isn't propagated to bench task's
waker, bench task hangs.

**Where could a wake be lost?** With `register_worker_local`:

- Connect task T runs on worker A. Registers fd on A. Pending.
- Connect-completion event fires on A's epoll. A wakes T. T runs,
  drops socket, exits. Task completion notifies the JoinHandle waker.
- JoinHandle waker is the bench task's continuation. Schedules bench
  task onto wherever the JoinHandle was last polled, *or* into inject
  if cross-thread. With `register_worker_local`, the bench task could
  have been polled on any worker depending on stealing/dispatch
  history.

Hmm, this still doesn't obviously deadlock. The wake should fire and
schedule the bench task; the bench task should then poll handle 1, etc.

The frozen-counters fact suggests a *missed wake*, not a stuck-running
worker. Once everyone is parked and no schedule_task fires, no one
wakes. So the wake must have been issued (somewhere) but consumed by
a worker that didn't actually have a task to wake — or it must have
hit a path that flipped park_state to NOTIFIED on a worker that
wasn't holding the task that needed waking.

## Concrete finding

`worker_notify_parked_local_no_one = 81` and
`worker_notify_parked_remote_no_one = 155`. **236 wake attempts
(64% of all wake attempts) were absorbed without delivering a wake**,
because `worker_to_notify` returned `None` due to `num_searching > 0`
(165) or `num_unparked == num_workers` (71).

The 165 `no_searching` short-circuits are the path the existing
`ShardedMioUnparker::unpark` comment warned about. They occur when
some worker is currently in the searcher state. The contract is:
"that searcher will find any submitted work." The contract holds *for
work in steal queues and inject*. It **does not hold for work that
becomes visible only via a kernel FD event whose epoll is bound to
one specific worker** — because the searcher will never call
`epoll_wait` on the *target* worker's epoll fd.

This is the actual mismatch that the synchronous-register-from-worker
fast path violates:

> The scheduler's "searcher will find any submitted work" invariant
> assumes work is task-shaped (steal or inject queue). The
> `register_worker_local` fast path produces work that is
> *fd-shaped*: the only thread that can dispatch it is the worker
> whose epoll holds the FD. A searcher running on worker A cannot
> dispatch FD events queued on worker B's epoll, no matter how hard
> it spins.

The queue-register path side-steps this because:

1. Round-robin distribution puts each FD on a different worker, so no
   one worker accumulates a critical mass of FDs.
2. Every `queue_register` ends with `unpark(target)` when the queue
   was empty — that wake is delivered to the *target worker
   directly*, bypassing the `worker_to_notify` short-circuits.

## Why "self-unpark on local register" doesn't fix it (handoff bisection row 2)

Even with `register_worker_local` calling `self.unpark(self.idx)`, the
problem is the same: an extra notification on the calling worker
itself doesn't help when the *root issue* is that some other FD's
events are queued on a third worker's epoll and that third worker is
in `ep_poll` waiting for events on FDs that the bench has stopped
generating activity for.

## Why `queue_register_pinned(self.idx)` (bisection row 3) doesn't deadlock but doesn't perform

Pinning the queue path to the local worker still concentrates FDs on
one worker. But unlike the synchronous fast path, it always issues an
explicit `unpark(target)` when the queue transitions empty→non-empty,
which guarantees the target worker leaves `ep_poll` and drains. It's
the *unpark* that breaks the stall, not the queueing per se. Round-
robin distribution is what gets the perf win on top.

## What this implies for the original goal

A "sound worker-local fast path" needs at minimum to guarantee one of:

- **Distribute FDs across workers anyway.** Pick the target worker
  not by `current_worker_index()` but by load (round-robin or
  least-loaded). This sacrifices the cache-locality argument for the
  fast path entirely — at which point the fast path is just a
  mutex-skip optimization over `queue_register`. Worth measuring
  whether even that mutex pair is the dominant cost.

- **Wake all parked siblings on every register.** Costly and defeats
  the whole "no syscall" intent.

- **Periodically migrate FDs across workers.** Adds a non-trivial
  rebalancing layer.

None of these recover both halves of the original intent ("no
synchronization, no sibling wake"). The cleanest follow-up is
probably **measuring how much of the queue path's ~3% throughput
regression is the mutex pair vs. the round-robin pick vs. the unpark
itself**, and deciding whether the mutex itself can be removed (e.g.
per-worker MPSC of bounded capacity that falls back to mutex on
overflow) without changing the distribution policy.

## Files modified this session

```
tokio/src/runtime/io/lazy_debug.rs                    +98   (new counters)
tokio/src/runtime/scheduler/multi_thread/counters.rs  +180  (sched_dbg shim)
tokio/src/runtime/scheduler/multi_thread/idle.rs       +30  (counter calls)
tokio/src/runtime/scheduler/multi_thread/worker.rs     +25  (counter calls)
tokio/src/runtime/io/sharded_mio_driver.rs            ~25   (TOKIO_LAZY_FASTPATH toggle)
```

Default behaviour unchanged (safe `queue_register` path still runs in
the absence of `TOKIO_LAZY_FASTPATH`).

## Follow-up experiment within this session: rr-distributed sync fastpath

To test the "all FDs on one worker is the real bug" framing, two
additional `TOKIO_LAZY_FASTPATH` modes were added to
`register_local`:

- `rr` — synchronous register on a `fallback_worker()`-picked target,
  i.e. distribute FDs round-robin like `queue_register` does, but skip
  the `pending_ops` mutex pair.
- `rr+unpark` — same as `rr`, plus an explicit `self.unpark(target)`
  after the synchronous register, mirroring the wake the queue path
  issues on its empty→non-empty transition.

**Both modes hang.** Same wchan signature — 4 workers in `ep_poll`,
main in `futex_do_wait`, counters frozen across multiple 250 ms dumps.

Counter snapshot at the `rr` wedge (truncated to the relevant rows):

```
register_worker_local_calls            195
queue_register_calls                     0
drain_deregister_drained               192
dispatch_calls                          76
dispatch_woken                         196
dispatch_slab_miss                       0       # no stale-slab wakes
worker_notify_if_work_pending_calls     69
worker_notify_if_work_pending_no_op     68       # 99% no-op (one via_steal)
idle_notify_should_wakeup_no_searching 167
idle_notify_should_wakeup_no_full      118
worker_transition_to_parked_yes         74
```

Per-thread state during the `rr+unpark` wedge:

```
PID  TID    COMMAND          STATE  WCHAN
... 1156253 net_tcp_echo-e2  S      futex_do_wait
... 1156260 tokio-rt-worker  S      ep_poll
... 1156261 tokio-rt-worker  S      ep_poll
... 1156262 tokio-rt-worker  S      ep_poll
... 1156263 tokio-rt-worker  S      ep_poll
```

## Updated bisection table

| variant                                | distribution | per-register wake          | hangs? |
|----------------------------------------|--------------|----------------------------|--------|
| sync, local (`local`)                  | local        | none                       | **YES** |
| sync, local + self-unpark              | local        | `self.unpark(self.idx)`    | **YES** (session 1) |
| sync, rr (`rr`)                        | round-robin  | none                       | **YES** (this session) |
| sync, rr + target-unpark (`rr+unpark`) | round-robin  | `unpark(target)` per call  | **YES** (this session) |
| queue, pinned-local                    | local        | `unpark(local)` when empty | no (session 1) |
| queue, rr (default)                    | round-robin  | `unpark(target)` when empty| no (session 1) |

## Revised root-cause framing

The discriminator is **synchronous vs queued mutation of the shared
slab/registry**, full stop:

- **Distribution doesn't matter** — round-robin synchronous still hangs.
- **Per-register wake doesn't matter** — explicit `unpark(target)` per
  registration doesn't help either.
- **It is not slab-lock contention.** All wedged workers are in
  `ep_poll`, not in `futex_do_wait` on the slab mutex. They are
  legitimately parked, kernel-side, with no events to deliver.

So the "all FDs on one worker" framing from the first write-up of
this session was wrong; the FD-distribution skew is a *consequence*
of the local-sync configuration, not the cause of the wedge. The real
cause is that synchronous mutation of the per-shard slab from outside
its owning worker thread breaks something the queue path's
single-threaded-per-shard discipline preserves.

## Speculative culprits to investigate next

The wedge shape ("counters frozen, all workers in ep_poll, kernel has
nothing to deliver") implies a **lost wake**, not a stuck thread. Two
plausible mechanisms remain:

1. **Slab key reuse races.** `slab.insert` returns the next free
   slot. The `slab` crate does **not** carry generation tags. Sequence:
   - Tick T: FD `f1` deregistered, slot `k` freed, but a stale epoll
     event for token=`k` is already in the kernel-side ready list.
   - Tick T+ε: synchronous register from another thread inserts FD
     `f2` at the same slot `k` (because `slab.insert` reuses).
   - Tick T+2ε: target worker's `poll_and_dispatch` returns events,
     looks up token `k`, finds `f2`'s `ScheduledIo`, calls
     `io.wake(ready)` — wakes the **wrong task** with stale readiness.
     The original `f1` task got its wake delivered to nobody (already
     deregistered, not waiting); the `f2` task gets a spurious wake.

   In the queue path this is impossible because slab mutation
   serializes against dispatch on the same thread, so a deregister
   *cannot* land between an `epoll_wait` returning event for `k` and
   the dispatch loop reading slab[k] — they're on the same thread,
   no reordering window.

2. **Publish-order race in `register_worker_local`.** Look at the
   sequence (driver source):
   ```rust
   shared.sharded_mio_worker.store(worker_idx, Relaxed);   // (1)
   slot.registrations.allocate_existing(...);              // (2)
   registry.register(source, interest, shared) -> Ok(key); // (3) returns key
   shared.sharded_mio_slab_key.store(key, Relaxed);        // (4)
   ```
   A concurrent deregister sees `sharded_mio_worker = idx` (from 1)
   and reads `sharded_mio_slab_key`. If it observes the pre-(4) value
   `u32::MAX`, `apply_deregister` skips the `registry.deregister` call
   (the `slab_key != u32::MAX` guard at sharded_mio_driver.rs:610).
   Result: the FD stays registered in the kernel epoll forever, the
   `ScheduledIo` is dropped, and any subsequent kernel event for that
   FD lands on `dispatch_slab_miss` — except `dispatch_slab_miss = 0`
   in our snapshot. So this is *not* what's happening here either,
   but the publish-order race is real and worth fixing on its own.

The high-information next experiment is **direct observation of which
FD's wake is missing at the wedge**. Concretely:

- Add a per-worker ring buffer recording the last N `(fd, slab_key,
  event_kind)` triples seen by `dispatch`. Dump on hang via
  `TOKIO_LAZY_DEBUG`.
- Add a per-`ScheduledIo` "last waker pointer" field updated on every
  `register_waker` call; on shutdown / hang, scan the slab for entries
  whose waker is set but whose readiness has never been observed.
- Confirm whether the 3 still-alive FDs at wedge-time are all
  connect-side sockets, all listener+something, etc.

## Long-term implications

A worker-local fast path that preserves the queue path's
single-threaded-per-shard mutation invariant is possible, but it can
only fire when `current_worker_index() == target_worker_index`:

- Pick `target = current_worker_index()` (so the calling thread *is*
  the target's worker thread; mutation is serialized against
  dispatch).
- Synchronously do `slot.registrations.allocate_existing` +
  `registry.register` on this thread, on this worker.
- *Don't* round-robin. The local-sync variant *also hangs* in the
  current bisection, but the prior session's test of that mode is
  worth re-running with the slab-key-reuse hypothesis specifically in
  mind — because local-sync from the current worker should *not*
  contend dispatch on the same worker, yet it still wedges. That
  contradicts the slab-key-reuse story above unless events targeting
  *other* workers' slabs are involved.

Concrete question that disambiguates: **does local-sync still hang
with `worker_threads(1)`?** With one worker, slab mutation can only
race with dispatch on the same thread, which is impossible. If
local-sync hangs even with 1 worker, the bug is in the registration
sequence itself, not in the cross-thread interaction.

## Session-3 result: single-worker confirms the wedge

Repro: `TOKIO_BENCH_WORKERS=1 TOKIO_LAZY_FASTPATH=1` with
`sharded_mio/tcp_connect_churn` — exit 124 (timeout). Wchan inventory
during the hang:

```
tid=<main>  state=S comm=net_tcp_echo-e2 wchan=futex_do_wait
tid=<wkr0>  state=S comm=tokio-rt-worker  wchan=ep_poll
```

The single worker is parked in `epoll_wait`; the main thread is
parked on a `JoinHandle`; only the parked worker can ever mutate the
registry or call `epoll_ctl`. **No cross-thread mutation, no
slab-mutex contention, no `num_searching`-style lost-wakeup state.**
A single thread issued `epoll_ctl_add`s, then blocked on
`epoll_wait`, and the kernel has nothing to deliver.

Counter snapshot at wedge:

```
register_worker_local_calls   = 163   (sync path, expected)
queue_register_calls          = 0     (no queue path runs)
apply_deregister_calls        = 161
dispatch_woken_writable       = 159   (vs ≈163 connects — 1–2 missed)
dispatch_woken_readable       =   8   (listener; far below the
                                       ≈iters×accept count)
```

`register_worker_local_calls − apply_deregister_calls ≈ 1–2` is
exactly the count of stuck connect tasks.

## The actual bug: stale-deregister destroys the next registration

The asymmetric routing of register vs. deregister is the bug:

- `register_local` (with the env-var fastpath set) runs **synchronously**
  on the calling worker — `slab.insert` + `epoll_ctl_add` happen
  immediately during task poll.
- `deregister` (`io_driver.rs:407`) **always** queues onto
  `slot.pending_ops` and is only applied at the next park's
  `drain_pending_ops`.

Combined with kernel fd reuse, this is enough to silently dismantle a
fresh registration on the same thread:

1. Iter N, task A_i sync-registers fd_X at slab key K_A. epoll holds
   `(fd_X, K_A)`.
2. A_i finishes, drops `TcpStream`. Order inside the drop chain:
   - `Registration::drop` →
     `sharded_mio_deregister(io_driver.rs:395)` →
     `let fd = source.registration_raw_fd(); handle.queue_deregister(io, fd)`
     pushes `DriverOp::Deregister { fd: fd_X, shared: A_i }` onto
     `pending_ops` and returns. **No mio call, no `epoll_ctl_del`
     yet.**
   - mio's source then drops, calling `close(fd_X)`. Kernel
     auto-drops the epoll registration when the last file ref dies,
     so `(fd_X, K_A)` is gone from the epoll set as a side effect of
     close.
3. **Same worker**, still pre-park: iter N+1 task B_j calls
   `socket()`. Linux returns the just-freed fd number — a brand new
   socket at the same fd value `fd_X`.
4. B_j sync-registers: `slab.insert` → key K_B; `epoll_ctl_add(fd_X,
   token=K_B)`. New `(fd_X, K_B)` is in the epoll set.
5. Worker finally parks. `drain_pending_ops` replays the iter-N
   `DriverOp::Deregister { fd: fd_X, … }`, which calls
   `Registry::deregister(SourceFd(&fd_X), …)` →
   `epoll_ctl_del(fd_X)`. **This wipes B_j's `(fd_X, K_B)` line
   item.** epoll has no idea that the fd value has been recycled —
   it's keyed on the fd number in the user's table, not on file*.
6. B_j has a connected socket whose epoll registration was just
   deleted. EPOLLOUT for connect never fires. Task hangs forever;
   `JoinHandle.await` on the main thread hangs forever.

Consistent with every observation:

- Single-worker hangs (no cross-thread races needed; ordering on one
  thread suffices).
- All sync variants (`local`, `rr`, `rr+unpark`) hang — they all
  register synchronously and rely on the queued deregister catching
  up later.
- Both queued variants don't hang — register and deregister both
  flow through `pending_ops` and apply in queued order. By the time
  iter N+1's register is *applied* (next park's drain), iter N's
  `Deregister` has already been applied earlier in the same drain
  pass: the post-close `epoll_ctl_del(fd_X)` either succeeds
  harmlessly on the kernel-already-dropped registration or returns
  ENOENT (which is ignored). Then iter N+1's `Register` re-adds
  `(fd_X, K_B)` and no later op disturbs it.
- The asymmetry is invisible at low fd-reuse rates — the bug needs
  fd-number recycling between drain cycles. `tcp_connect_churn`
  exercises this maximally; `tcp_echo_throughput` doesn't (long-lived
  connections), which is why the previous bisection didn't pin it
  down.

## Chosen direction: per-slot generation marker

Stamp every slab slot with a `u32` generation that bumps on every
`slab.insert`. Pack `(key, gen)` into the mio token; carry both on
`Arc<ScheduledIo>` (replace `sharded_mio_slab_key: AtomicU32` with
`AtomicU64` or add a sibling `sharded_mio_gen: AtomicU32`). Then the
fastpath wedge dissolves *without* needing to make deregister
synchronous and *without* changing the close-time semantics:

### How it kills the wedge

Recycle scenario from above plays out like this:

1. Iter N: A registers at slot K_A. Slot gen `0 → 1`. Stored on
   `shared_A`: `(K_A, 1)`. Token in epoll: `(K_A | 1)`.
2. A drops; `queue_deregister` captures `(fd_X, K_A, 1)` (the gen
   read from `shared_A` at queue time). mio closes fd_X — kernel
   auto-removes the registration when the last file ref dies.
3. Iter N+1, same worker, before park: B `socket()` returns recycled
   fd_X. Sync register: `slab.insert(B)` returns K_A again, gen
   `1 → 2`. Stored on `shared_B`: `(K_A, 2)`. Token in epoll:
   `(K_A | 2)`.
4. Worker parks. `drain_pending_ops` replays A's deregister with
   captured `(fd_X, K_A, 1)`. **`apply_deregister` checks
   `slab[K_A].gen` → 2. Mismatch. Skip both `epoll_ctl_del(fd_X)` and
   `slab.try_remove(K_A)`.**
5. B's `(fd_X, K_A | 2)` registration is intact. EPOLLOUT fires.
   Connect completes. No hang.

### What it also fixes

A matching gen check in `poll_and_dispatch` makes the dispatch loop
robust to genuinely stale events (e.g. an event surfacing for a slot
that was deregistered and reassigned between the kernel queueing the
event and the dispatcher consuming it):

```rust
let Some(io) = slab.get(token.key()) else { miss; continue };
if io.gen() != token.gen() { stale; continue };
io.wake(...)
```

This is a real correctness improvement on the queue path too —
today's dispatch only checks slot occupancy, not entry identity.

### Skipping `epoll_ctl_del` is correct

`epoll_ctl_del(fd)` is keyed on fd, not token; it would clobber
whoever's currently registered at that fd. Skipping on gen-mismatch
is safe because:

- **fd was actually closed** (the recycle scenario): kernel
  auto-removed the registration via the close path. Explicit
  `epoll_ctl_del` is redundant; skipping is a no-op against the
  kernel.
- **fd is still open** (rare: dup, fd-passing): no recycle has
  happened, so nobody else has registered against this slot. Slab
  gen still matches → we run the deregister.

### What changes

1. **Slot data layout**: each slot needs a generation that lives
   *across* vacate→reinsert. The `slab` crate stores `T` only in
   occupied slots; vacant slots carry no user data. Two options:
   - Replace `Slab<Arc<ScheduledIo>>` with `slotmap::HopSlotMap`
     (or a hand-rolled equivalent) — generation built in.
   - Keep `slab::Slab<(u32, Arc<ScheduledIo>)>` and add a parallel
     `Vec<u32>` of generations indexed by slab key, grown lazily on
     out-of-range read. The vec stores the *next* generation to use
     on the next `insert` at this slot.
   The slotmap swap is cleaner; the parallel-vec approach is a
   smaller diff.

2. **Token packing**: e.g. low 24 bits = key, next 40 bits = gen,
   reserve `usize::MAX` for `WAKER_TOKEN`. Token <-> (key, gen)
   conversions in `SharedRegistry::register` and
   `Reactor::poll_and_dispatch`.

3. **`ScheduledIo`**: replace `sharded_mio_slab_key: AtomicU32` with
   either `AtomicU64` carrying a packed `(key, gen)` or add a
   sibling `sharded_mio_gen: AtomicU32`. Bench latency with both;
   the `AtomicU64` is one less load/store on the hot dispatch path.

4. **`DriverOp::Deregister`**: no shape change needed if the gen is
   stored on `shared` — `apply_deregister` just reads
   `(shared.slab_key, shared.gen)` like it reads `slab_key` today.

5. **Dispatch path**: insert the gen check after the `slab.get`
   miss path. Add a `dispatch_gen_mismatch` counter for visibility.

6. **Apply-deregister path**: read gen from shared; compare to
   `slab[key].gen` (or "next gen at this slot" in the parallel-vec
   layout, with the convention that *next != stored* means the slot
   has been reassigned). Skip on mismatch and bump
   `apply_deregister_gen_mismatch` counter.

### Why this beats the other candidates

- vs. **sync deregister too**: still requires cross-worker sync
  paths (the slab mutex must remain). Generations let cross-worker
  deregisters keep being queued safely.
- vs. **drain-before-sync-register**: avoids burning a slab + synced
  lock + drain pass on every registration in the hot path.
- vs. **defer close until apply_deregister**: doesn't extend fd
  lifetime; doesn't risk fd exhaustion under churn.

The cost: one extra `u32` per slot (or one extra atomic load on
ScheduledIo) and one cmp on dispatch + apply_deregister.

## Confirmation experiment (run before committing the slotmap swap)

Cheap scaffolding to *prove* the diagnosis from a hung run before
investing in the slotmap refactor:

1. Log `(fd, slab_key)` from each sync `register_worker_local`
   success and from each `queue_deregister` queue-time.
2. At `apply_deregister`, compare `(fd, slab_key)` against any sync
   register that landed *after* this op was queued for the same fd.
   Bump a `apply_deregister_clobber_observed` counter when a
   collision is detected. Exit (or at least dump) on first hit.

If the counter trips during a hung run with `TOKIO_LAZY_FASTPATH=1`,
diagnosis is confirmed. Then proceed with the generation-marker
implementation.

## Other follow-ups (unchanged from before)

- **Replace `pending_ops: Mutex<Vec<DriverOp>>` with a lock-free
  MPSC.** Attacks the +3% `tcp_echo_throughput` regression directly.
- **Cleaner perf rerun on a quieter box.** Lower-CI -17% on
  `tcp_connect_churn` is solid; the +3% throughput regress would
  benefit from being measured cleanly.
- **Sanity-check uring bench (`bench-uring-reactor`).** This session's
  `sched_dbg` shim no-ops on the uring build path, but the bench was
  not run to verify.

## Memory pointers

- Auto-memory: `/home/louis/.claude/projects/-home-louis/memory/MEMORY.md`
  (still no entry for this project — consider adding one).
- Prior session handoff: `HANDOFF-lazy-register.md` (this file picks
  up from "Open questions / next experiments" #1).
- Prior session full transcript:
  `/home/louis/.claude/projects/-home-louis/0433eedf-a293-47d9-9258-e86357c2cfc9.jsonl`.
