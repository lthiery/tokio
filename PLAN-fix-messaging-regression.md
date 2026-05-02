# Plan: keep sharded-mio IO wins, fix messaging-side regressions

## Status entering this plan

- Branch `worktree-io-driver-vtable` at `240d14ab` — selective-drain commit on top of `d8c93f1a` (always-serve-meta-watcher) on top of `ee6768eb` (initial readiness-stealing wiring).
- Validated wins (multi-sample, not `--quick`): `io_busy_owner/busy_owner_idle` parity with traditional (~291 µs), `busy_owner_3burners` ~21% faster than traditional. 18/18 sharded-mio tests passing. Non-sharded code paths binary-identical to baseline.
- Wide head-to-head sweep across all 19 benches (`/tmp/bench-comparison.txt`) shows median Δ=+8.6%, with sharded-mio ~10–35% slower than traditional on most non-IO synchronization & cross-worker spawn workloads:
  - `sync_mpsc/contention/*`, `sync_broadcast/contention/*`, `sync_notify/notify_one/*`, `sync_watch/contention_resubscribe/*`
  - `spawn_blocking/concurrency/{4,8,16,32}`, `remote_spawn/threads/{1..8}`
  - `time_timeout/multi_thread_*`
- These benches **do not exercise the I/O driver**. They only stress the runtime parker.

The question this plan answers: **can we keep the IO wins while making the parker no slower than the traditional path on IO-less workloads?**

## Diagnosis: the smoking gun

`tokio/src/runtime/io/sharded_mio_driver.rs::ShardedMioHandle::unpark` (lines 550-572) issues **two kernel wakes per cross-worker unpark, unconditionally**:

```rust
pub(crate) fn unpark(&self, worker_idx: usize) -> bool {
    let slot = &self.workers[worker_idx];
    let prev = slot.park_state.swap(NOTIFIED, Ordering::Release);
    if prev != PARKED { return false; }
    if let Some(waker) = slot.external_waker.get() {
        let _ = waker.wake();   // <-- kernel write() to eventfd
    }
    if let Some(thr) = slot.park_thread.get() {
        thr.unpark();           // <-- kernel futex wake
    }
    true
}
```

The comment justifies the unconditional double-wake:
> *Calling both unconditionally avoids a mode-check race (the worker can transition between watcher and thread-parker between two parks).*

This is the cost we're paying. Every cross-worker `Notify::notify_one`, every `tx.send` that wakes a remote `rx`, every `Semaphore::add_permits` that releases a waiter — each pays **two syscalls** vs traditional's **one** (write to the global Waker eventfd). The traditional driver has a single park mode (mio::Poll on a single `Poll`), so its `unpark` is a single wake. We bifurcated the park modes (meta-epoll / own-child-epoll / thread-park) and kept the unpark path mode-agnostic by firing both possible wakes.

For benches with millions of cross-worker unparks per second (`sync_mpsc/contention/unbounded`, `sync_notify/notify_one/500`), an extra syscall per wake plausibly accounts for the 10–35% regressions we see.

## Hypotheses to validate before redesigning

Each is cheap to confirm and de-risks the plan. Run *before* changing code.

1. **H1 — Double-syscall is the dominant cost.** Measure `strace -c -f -p <runtime-pid>` over a steady-state `sync_mpsc/contention/bounded` run, sharded vs traditional. Predict sharded shows ~2× the rate of `write` (eventfd) + `futex` (FUTEX_WAKE) calls per logical wake. If the syscall ratio is closer to 1:1, the regression is somewhere else (cache traffic on `park_state`, meta-watcher CAS, etc.) and Strategy A below won't fully recover.

2. **H2 — Meta-watcher is rarely active in IO-less workloads.** Add a counter for `try_acquire_meta_watcher` success rate (already partially exists via `lazy_debug::COUNTERS`). For sync benches we expect the watcher slot to be vacant most of the time because slabs are empty. If true, we can almost-always use the cheaper path; if false, the meta-epoll itself is sitting in the hot loop.

3. **H3 — `external_waker` fast path is the bottleneck per-wake, not `Thread::unpark`.** Replace `let _ = waker.wake();` with a no-op (under a debug feature flag) and re-run sync benches. If sync_mpsc speeds up by ~10-15%, the eventfd write is the cost we want to shed in the empty-slab case. If futex turns out to be the heavier of the two, invert the priority.

4. **H4 — `WAKER_TOKEN` eventfd writes by *peers* contribute too.** When worker B does `tx.send` to wake worker A's task, B calls `unparker.unpark` which writes A's eventfd. Even on the traditional path B does *one* write to the global Waker. The shard cost is the *second* wake (futex), not the first. Confirms that single-wake unpark recovers parity.

## Recommended strategy: encode park-mode in `park_state`, single-wake unpark

### Idea

Replace the 3-state `park_state` (`EMPTY` / `PARKED` / `NOTIFIED`) with a 5-state encoding that records *which* park branch the worker took, so `unpark` can route to exactly one wake:

```text
EMPTY        = 0
PARKED_OWN   = 1  // park_on_own_child  → wake by writing WAKER_TOKEN eventfd
PARKED_META  = 2  // park_on_meta       → wake by writing meta-watcher eventfd (NEW)
PARKED_THREAD= 3  // thread_park        → wake by Thread::unpark (futex)
NOTIFIED     = 4
```

Park entry (under `begin_park`) CASes `EMPTY → PARKED_*` after the mode is chosen but *before* any blocking syscall is issued. Unpark CASes the slot to `NOTIFIED` and switches on the prior state to choose the wake mechanism.

Race that the current code feared: "the worker can transition between watcher and thread-parker between two parks." This is fine because each park starts with a CAS that publishes the new mode atomically. The unparker reads `prev`, observes the mode that was active at the moment of the swap, and that mode is exactly the one the parker is currently blocked in (or about to block in, in which case the NOTIFIED check on the parker side aborts the syscall).

### Concrete edit map

1. **`tokio/src/runtime/io/sharded_mio_driver.rs`**
   - Replace the `park_state` constants (currently `EMPTY=0`, `PARKED=1`, `NOTIFIED=2`) with the 5-state encoding.
   - Add `meta_waker: OnceLock<Box<dyn Wake>>` (or just an eventfd) to `Inner` for the meta-watcher path. Register it on the meta-epoll fd alongside the children, level-triggered. The watcher branch's `epoll_wait` will return it when written, allowing the watcher to break out of the syscall.
   - Replace `begin_park(idx)` → `begin_park(idx, mode: ParkMode)`. Caller in `park_internal` passes `OwnChild`, `Meta`, or `Thread`.
   - Rewrite `unpark`: single match on `prev`:
     ```rust
     match prev {
         PARKED_OWN     => write_eventfd(slot.external_waker),  // existing path
         PARKED_META    => write_eventfd(self.meta_waker),      // NEW eventfd
         PARKED_THREAD  => slot.park_thread.unwrap().unpark(),  // futex
         _ => {}
     }
     ```

2. **`tokio/src/runtime/scheduler/multi_thread/sharded_mio_park.rs::park_internal`**
   - At each branch entry, call `begin_park(idx, mode)` with the appropriate `ParkMode` *before* the blocking syscall.
   - `park_on_meta` must additionally drain its meta-waker eventfd if the kernel returns it (one of the meta_events).
   - Keep the existing `try_acquire_meta_watcher` CAS — orthogonal to this change.

3. **Tests**: add a stress test that hammers cross-worker `Notify::notify_one` from many threads while one worker is the meta-watcher and others are split between own-child and thread-park, asserting no deadlock and observed wake counts match logical wake counts (one syscall per wake).

### Risk register

- **Breaks invariant in `unpark` comment** (lines 549-563 of the existing module): the comment explicitly justifies double-wake. The replacement must preserve liveness when the parker is mid-mode-transition. Mitigation: the parker's `begin_park` CAS publishes the new mode *before* the syscall, so an unpark that reads `prev = PARKED_OLD` will wake the OLD mode's mechanism — which is exactly what's needed because the parker is still in the OLD mode's syscall (or about to enter it, in which case NOTIFIED short-circuits). The window where mode has changed but the new syscall hasn't started yet is closed by `begin_park`'s CAS+store.
- **Drop of meta-watcher slot mid-park**: if a worker that won the meta-watcher CAS is asked to shut down while parked on meta, we need a clean wake. `meta_waker` write from the runtime shutdown path handles it.
- **Rust toolchain**: no MSRV impact; uses only AtomicUsize CAS.

## Alternative: simpler "always-park-on-epoll" approach

If H2 confirms the meta-watcher CAS is essentially uncontended in IO-less workloads, an even simpler change works:

- **Drop `thread_park` branch entirely.** Every worker always parks in either `park_on_meta` (winner) or `park_on_own_child` (everyone else). The "slab empty" case still parks on the child epoll, which has only the `WAKER_TOKEN` eventfd registered — `epoll_wait` on a single-eventfd set is essentially a futex wait under the hood (kernel uses the eventfd's wait queue), with negligible extra cost.
- This collapses the park modes to 2 (`OwnChild` and `Meta`), eliminating `park_thread` and `Thread::unpark` from the hot path.
- Unpark is now: write to `WAKER_TOKEN` eventfd (for own-child branch) or write to `meta_waker` eventfd (for meta branch). Still need the 2-bit mode encoding from Strategy A above.

Pros: even less code than Strategy A; no `Thread::unpark` futex call ever.
Cons: own-child epoll on an empty slab might be ~1 µs slower than `park_thread` futex per park. Need to benchmark.

**Recommended order:** validate H1-H4, then do Strategy A. If H2 also confirms, fold in the simpler "no thread_park" form.

## Validation criteria for the next agent

The next pass is **done** when, on the same hardware as `/tmp/bench-comparison.txt`:

1. All 18 sharded-mio tests still pass (`tokio_unstable + io-sharded-mio + rt-multi-thread + linux`).
2. `io_busy_owner/busy_owner_idle` and `busy_owner_3burners` retain their wins from `240d14ab` (parity-or-better vs traditional). Sample size: criterion default (not `--quick`).
3. Median Δ across the messaging benches (`sync_mpsc/contention/*`, `sync_broadcast/contention/*`, `sync_notify/*`, `sync_watch/contention_resubscribe/*`, `spawn_blocking/concurrency/{4,8,16,32}`) drops from current +13.7% to ≤ +3% (within criterion noise floor at default sample size).
4. `strace -c` shows ~1 wake-syscall per logical cross-worker unpark in steady state (down from current ~2).
5. Non-sharded-mio code paths still binary-identical to `HEAD~3` (the gate verification).

## Out of scope for this plan

- **Hardware**: leave the EPYC 7H12 / `MAX_PARALLEL=16` follow-up alone — orthogonal.
- **`bench-sharded-mio` patch automation** of `/tmp/patch_benches.py`: that script worked but is throwaway; do not commit it. Future runs should recreate it on demand if cross-bench sweeps are needed.
- **MSRV / non-Linux**: change is `#[cfg(target_os = "linux")]` gated, same as everything else in sharded-mio.

## Files & artifacts the next agent will need

- `tokio/src/runtime/io/sharded_mio_driver.rs:550-606` — `unpark` / `begin_park` / `end_park`.
- `tokio/src/runtime/scheduler/multi_thread/sharded_mio_park.rs:137-201` — `park_internal` mode selection.
- `tokio/src/runtime/scheduler/multi_thread/sharded_mio_park.rs:259-331` — `park_on_meta`, the harvest path.
- `/tmp/bench-trad/*.txt` and `/tmp/bench-shard2/*.txt` — current head-to-head numbers to beat.
- `/tmp/parse_bench.py` — comparison-table generator.

## TL;DR

The IO wins come from selective drain across worker children. The messaging losses come from `unpark` issuing two kernel wakes (eventfd + futex) per logical wake because `park_state` doesn't know which park-branch the target worker took. **Encode the park-mode into `park_state`** so `unpark` can route to exactly one wake. Validate with strace + the 19-bench sweep. Optionally fold in dropping the `thread_park` branch entirely.
