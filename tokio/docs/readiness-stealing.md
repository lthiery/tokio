# Readiness stealing for sharded-mio

## Motivation

The sharded-mio backend gives each multi-thread worker its own
`mio::Poll` (epoll fd). Registrations land on the worker that first
polls the fd, and `epoll_wait` is only called by that worker — and only
when its scheduler run queue is empty.

This shape is fine when load is roughly even. It is **catastrophic**
when one worker gets pinned to a CPU-bound task: that worker stops
calling `epoll_wait`, so every fd registered on it becomes invisible
to the runtime until the busy task yields. Idle peer workers cannot
help: each peer only sees its own epoll fd.

Phase 0 microbench (`benches/io_busy_owner.rs`) quantifies the
phenomenon on a 4-worker runtime, 16 fds, 50 ms burner deadline:

| Backend       | Idle  | 3 burners | Ratio |
|---------------|-------|-----------|-------|
| `traditional` | 281µs | 376µs     | 1.34× |
| `sharded_mio` | 301µs | 45.6 ms   |  151× |

With three of four workers spin-bound, sharded-mio's tail latency is
bounded by `BURNER_MS` (50 ms) — the kernel has events queued, but no
one is reading them. Traditional shows almost no degradation because
its single global epoll fd is read by whichever worker happens to be
parked.

**Readiness stealing** closes this gap: an idle worker harvests events
directly from a busy peer's epoll fd via a non-blocking `epoll_wait`,
dispatches them through the peer's `SharedRegistry`, and lets the
ordinary waker chain reschedule the awaiting tasks.

## Design

### Kernel semantics (Linux)

epoll fds are shareable across threads. The kernel delivers each
event to exactly one `epoll_wait` waiter — whichever calls first wins.
Edge-triggered registrations (Tokio's mode) are reported once per
state change. A racing steal-and-own-park therefore can never
double-fire an event: either we get it or the owner does.

mio's "one `Poll` per thread" rule is library convention, not a
kernel constraint. We bypass the `Poll` API on the steal path and
call `libc::epoll_wait` directly on the cloned `Registry`'s raw fd.

### Component additions

**1. `SharedRegistry::epoll_fd() -> RawFd`** — expose the cloned
registry's underlying fd. Implementation reads `Registry::as_raw_fd()`;
mio exposes this on Linux.

**2. `SharedRegistry::steal_dispatch(events: &mut [libc::epoll_event]) -> usize`**

```rust
pub(crate) fn steal_dispatch(
    &self,
    events: &mut [libc::epoll_event],
) -> usize {
    let epfd = self.registry.as_raw_fd();
    // SAFETY: epfd is a live epoll fd for as long as `self` lives;
    // events is a valid mutable buffer.
    let n = unsafe {
        libc::epoll_wait(
            epfd,
            events.as_mut_ptr(),
            events.len() as i32,
            0, // non-blocking
        )
    };
    if n <= 0 { return 0; }
    let n = n as usize;

    let state = self.ops.lock().expect("sharded-mio ops poisoned");
    for ev in &events[..n] {
        let token = Token(ev.u64 as usize);
        if token == WAKER_TOKEN { continue; }
        let (key, gen) = unpack_token(token);
        let Some(io) = state.slab.get(key as usize) else { continue };
        if io.sharded_mio_gen.load(Ordering::Relaxed) != gen { continue; }
        let ready = ready_from_epoll_events(ev.events);
        io.set_readiness(Tick::Set, |curr| curr | ready);
        io.wake(ready);
    }
    n
}
```

The dispatch logic is the same as `Reactor::poll_and_dispatch` — slab
lookup, gen check, set readiness, fire waker — just operating on raw
`libc::epoll_event` instead of `mio::event::Event`.

**3. `ShardedMioHandle::try_steal_pass(self_idx, buf)`** — round-robin
over peers, call each one's `steal_dispatch`. Returns total events
harvested.

**4. Park-loop hook (`ShardedMioParker::park_internal`)** — between
`drain_pending_ops` and `begin_park`:

```rust
self.handle.drain_pending_ops(self.idx);

// Readiness stealing: try to harvest events from peers' epoll fds
// before parking. If any harvested event wakes a task on our
// scheduler, the wake chain will set our park_state to NOTIFIED,
// which begin_park's CAS catches as the fast-path return.
let mut steal_buf = [libc::epoll_event { events: 0, u64: 0 };
    STEAL_BATCH];
self.handle.try_steal_pass(self.idx, &mut steal_buf);

if self.handle.begin_park(self.idx) {
    return; // notified — possibly by our own steal
}
// ...rest as before
```

`STEAL_BATCH` = 32 (small; we don't want a steal pass to drain a peer
faster than the peer itself would, just to take the edge off).

### Concurrency argument

- **Slab lock**: `steal_dispatch` takes the peer's `ops.lock()` for
  the dispatch pass. The peer's own `Reactor::poll_and_dispatch` also
  takes it. Contention is bounded by event rate × stealer count;
  worst case all idle workers pile on one busy peer, but then the
  per-event critical section is microscopic (slab get, atomic load,
  set_readiness, wake). Mutex is fine here; if profiling later shows
  contention, a per-event RCU is feasible.

- **Event ownership**: kernel guarantees exactly-once delivery to
  `epoll_wait` callers. Race between stealer and owner-park is
  resolved by the kernel — no Tokio-side coordination needed.

- **Lifetime**: `SharedRegistry` is held in `WorkerState::shared_registry:
  OnceLock<SharedRegistry>` and lives for the runtime's lifetime. The
  cloned epoll fd in the `Registry` is closed when the registry drops
  at runtime shutdown — well after the last possible `steal_dispatch`
  call.

- **Drop / shutdown**: a stealer racing with shutdown is fine: if
  shutdown closes the epoll fd before our `epoll_wait`, the syscall
  returns `EBADF`; we return 0 and move on. We don't synchronise
  beyond what mio's `Drop for Poll` already provides.

### What the user sees (perf)

After this lands, the busy-owner case in `io_busy_owner.rs` should
drop from ~45 ms to within ~5× of the idle case (still bounded by
how often the idle worker runs its park loop, but no longer bounded
by `BURNER_MS`). The Phase 0 bench is the regression gate.

The idle case should be essentially unchanged: one extra round-robin
of `epoll_wait(0)` per park. Empirically `epoll_wait(0)` on an empty
epoll fd is ~80–150 ns; for a 4-worker runtime that's ≤ 0.5 µs
overhead per park, a fraction of a percent of the 281 µs idle
baseline.

### Phased rollout

1. **P1: Always-on round-robin** (the design above). Simple, correct,
   adds 3 syscalls per park on a 4-worker runtime. Land behind a
   `tokio_unstable` cfg to keep it off the default path until
   measured.

2. **P2: Adaptive cadence**. Track per-worker `parks_since_last_steal`;
   only steal every K parks unless the previous park slept past T µs
   (hot path on truly idle worker, no overhead on busy mixed
   workloads).

3. **P3: Targeted victim selection**. Don't round-robin: pick the peer
   whose scheduler `is_searching == 0` and whose run queue length is
   largest (most CPU-bound). Requires cross-worker visibility into
   scheduler state, which Tokio's `multi_thread` already exposes via
   `idle.is_parked` etc. P1 numbers will tell us whether the round-
   robin overhead actually warrants this.

4. **P4: Steal-on-block**. Instead of (or in addition to)
   pre-park stealing, hook the worker's search loop so an idle worker
   that's about to park first does N rounds of "try a peer's epoll
   fd"; if events arrive, never actually parks. Subsumes P1 and gives
   even lower latency. Larger refactor — touches the multi_thread
   worker, not just the parker.

P1 is the smallest patch that proves the concept; we ship it, run
the bench, decide whether P2/P3/P4 are worth doing.

### Files touched (P1)

- `tokio/src/runtime/io/sharded_mio_reactor.rs`
  - Add `SharedRegistry::epoll_fd`, `SharedRegistry::steal_dispatch`.
  - Helper `ready_from_epoll_events(u32) -> Ready` (mirrors
    `Ready::from_mio` but on raw epoll bits).

- `tokio/src/runtime/io/sharded_mio_driver.rs`
  - Add `ShardedMioHandle::try_steal_pass(self_idx, buf) -> usize`.
  - Counters: `steal_calls`, `steal_events_harvested`,
    `steal_eagain`, `steal_errors`.

- `tokio/src/runtime/scheduler/multi_thread/sharded_mio_park.rs`
  - In `park_internal`, after `drain_pending_ops`: call
    `try_steal_pass`. Stack-allocated 32-event buffer.

- `tokio/src/runtime/io/lazy_debug.rs`
  - New counters defined.

- `benches/io_busy_owner.rs` — already in tree; rerun to confirm
  busy/idle ratio collapses.

### Risks

- **Slab-lock contention on a busy peer.** Mitigation: steal dispatches
  in batch under one lock acquire, same as the owner does. Worst
  case is a hot fd that the owner can't process due to spin-burn —
  exactly the scenario we want stealers to handle, so the contention
  is paying for itself.

- **Cross-worker waker storms.** An event harvested from peer N
  whose target task is on N's queue triggers
  `notify_parked_local(N)`, which calls `unpark(N)`. N is busy and
  doesn't park, so this is just an atomic store on `park_state`.
  No syscall. Cheap.

- **`steal_dispatch` from off-runtime threads.** Not exposed; only
  the parker's worker-thread context calls it.

- **EPOLL_CLOEXEC / dup semantics.** mio's `Registry::try_clone` already
  handles the dup; we operate on the cloned fd. No change needed.

### Testing

- **Loom**: extend `loom_registration.rs` with a model where two
  workers race `register_local` and `steal_dispatch` on the same
  `ScheduledIo`. Invariant: `wake()` count matches the number of
  state-change events the kernel would have delivered (at-most-once,
  at-least-once-if-anyone-polls).

- **Stress**: add `tests/rt_sharded_mio_steal.rs`:
  - 4-worker runtime, 1 burner spinning forever, fd registered on
    burner's worker, peer-thread does the wake.
  - Assert `.readable().await` returns within 100 ms (without steal
    it would never return).

- **Bench gate**: `io_busy_owner.rs` `sharded_mio/busy_owner_3burners`
  drops from 45.6 ms to < 1 ms.

## Decision point

If you greenlight, P1 is roughly:
- ~80 lines in `sharded_mio_reactor.rs`
- ~40 lines in `sharded_mio_driver.rs`
- ~10 lines in `sharded_mio_park.rs`
- one stress test
- one counter-bumping audit

— ready to write on your go.
