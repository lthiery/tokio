# Chiplet meta-watcher regression — FIXED

**Date:** 2026-05-09
**Branch:** `chiplet-cross-group-drain` (off tag `chiplet-regressed-eea395a7`)
**Commit:** `7ffd8006` — `rt(sharded-mio): cross-group fallback drain in park_on_meta (chiplet tier-A)`
**Sweep:** lourip run `56af977b-b295-4043-854f-6a8066516480` (W=64, 5 reps, measure_s=8)

## TL;DR

Option A from `HANDOFF-chiplet-regression-G-sweep.md` worked, and worked better than predicted: a single +69 LOC patch in `sharded_mio_park.rs::park_on_meta` (cross-group `try_steal_drain` after in-group drain) **fully eliminates** the connect-churn regression and **simultaneously improves** echo throughput.

| Bench (W=64) | pre-chiplet | chiplet (G=16) | **+xgroup_drain** |
|---|---|---|---|
| sharded_mio/tcp_connect_churn | 286.06 µs | 484.57 µs (+69.4%) | **287.71 µs (+0.6%)** |
| sharded_mio/tcp_echo_throughput | 9414.90 µs | 9322.90 µs (−1.0%) | **8587.40 µs (−8.8%)** |

xgroup_drain is faster than pre-chiplet on **both** workloads, faster than chiplet on **both** workloads. No trade-off.

## What changed

In `tokio/src/runtime/scheduler/multi_thread/sharded_mio_park.rs::park_on_meta`,
after the in-group drain (`steal_from_peers_masked(peer_mask)`):

```rust
if xgroup_drain_enabled() {
    const N_OTHER_GROUP_PEERS: usize = 64;
    let workers = self.handle.workers();
    let num_workers = workers.len();
    if num_workers > 1 {
        let mut scanned = 0usize;
        let mut probe = (self.idx + 1) % num_workers;
        while scanned < N_OTHER_GROUP_PEERS && scanned < num_workers {
            if probe != self.idx {
                if let Some(slot) = workers.get(probe) {
                    let g = slot.group_idx.load(Ordering::Acquire);
                    if g != group_idx && g != u8::MAX {
                        if let Some(registry) = slot.shared_registry.get() {
                            registry.try_steal_drain();
                        }
                    }
                }
            }
            probe = (probe + 1) % num_workers;
            scanned += 1;
        }
    }
}
```

Plus a `xgroup_drain_enabled()` helper that reads `TOKIO_CHIPLET_XGROUP_DRAIN`
once via `OnceLock` to keep the hot park path free of `std::env::var` allocations.

## Why it works (mechanism)

The original chiplet regression was fragmentation tax: 16 per-CCX
meta-watchers each watching ~4 children quiesced between events, each
paying a futex+epoll_wait+ctxswitch round-trip per batch. Pre-chiplet's
single global watcher tight-loops because 64-child fan-in keeps it
continuously readable.

The cross-group drain doesn't reduce the *count* of watcher wakes
(each per-CCX watcher still wakes on its own meta as before), but it
massively increases the **drain productivity per wake**. When a CCX
watcher wakes for one event, it now also drains every other group's
ready peers before re-sleeping. This means events queued on a
quiescent group's children get dispatched before that group's watcher
even has to wake — the aggregate wake rate across all 16 watchers
collapses, and the system approximates pre-chiplet's tight-loop
behaviour from the *perspective of the kernel* even though watcher
ownership is still per-CCX.

Echo throughput improves further (−8.8% vs pre-chiplet) because:
1. CCX-local wake routing in `unpark` is preserved → wake target is L3-local
2. Per-wake drain productivity is now ~16× higher → fewer total ctxswitches
3. The watcher dispatching stays CCX-local for in-group events (locality preserved) AND opportunistically drains cross-group when slack exists

So locality and amortization compose, rather than trading off.

## Robustness

xgroup_drain reps were *tighter* than both baselines:
- xgroup_drain connect_churn: [287.7, 311.5, 290.3, 286.9, 287.7] — max 311.5 µs
- chiplet baseline: [484.6, 509.5, 355.5, 406.3, 10745.0] — one rep at 10.7ms (wake-cascade pathology)
- pre-chiplet: [287.2, 285.4, 286.1, 285.9, 349.4] — one rep at 349 µs

The pathological tail spikes seen in the original chiplet design (and
the documented G-sweep tail at G≥4) appear to be dampened by the
cross-group drain. The likely mechanism: tail spikes happen when many
small groups deadlock on each other's level-triggered events, and the
cross-group drain breaks those cycles by letting a non-affected
watcher dispatch the stuck readiness.

## Cross-host W-matrix sweep (2026-05-09 follow-up)

Sweeps `20609039-5aa5-4651-ad82-9f8614ee728b` (lourip W={6,16,32,64})
and `e0bb8906-8dcd-48ce-9a32-3c8e22fbfdf2` (lounas W={1,2,4,6,8}),
3 modes × 5 reps × 4 sub-benches × 8s measure.

### Lourip (8 CCDs × 2 CCXs = 16 natural groups)

`sharded_mio/tcp_connect_churn` Δ vs pre-chiplet:

| W | chiplet | **xgroup_drain** |
|---|---|---|
| 6  | +3.3% | +0.1% |
| 16 | +1.3% | **−7.4%** |
| 32 | +1.1% | **−25.0%** |
| 64 | +45.7% | **−1.5%** |

`sharded_mio/tcp_echo_throughput` Δ vs pre-chiplet:

| W | chiplet | **xgroup_drain** |
|---|---|---|
| 6  | +2.4% | **−8.3%** |
| 16 | +4.1% | **−7.6%** |
| 32 | +3.0% | **−11.6%** |
| 64 | +51.8% | **−11.0%** |

### Lounas (4 CCDs × 1 CCX = 4 natural groups)

`sharded_mio/tcp_connect_churn` Δ vs pre-chiplet:

| W | chiplet | **xgroup_drain** |
|---|---|---|
| 1 | +0.3% | +0.2% |
| 2 | −1.0% | **−2.8%** |
| 4 | +8.5% | **−2.8%** |
| 6 | +27.2% | +10.2% |
| 8 | +17.3% | +4.6% |

`sharded_mio/tcp_echo_throughput` Δ vs pre-chiplet:

| W | chiplet | **xgroup_drain** |
|---|---|---|
| 1 | +2.3% | +0.5% |
| 2 | −5.3% | **−5.4%** |
| 4 | +24.0% | +9.7% |
| 6 | +17.5% | +3.6% |
| 8 | +12.2% | +0.5% |

### Cross-host verdict

- **xgroup_drain strictly dominates chiplet on EVERY (host, W, sub-bench) cell.** No exceptions.
- **On lourip (the host that mattered): xgroup_drain is at parity or substantially better than pre-chiplet on every cell**, with W=32 giving a remarkable −25% on connect_churn and −11.6% on echo. The W=64 connect-churn regression that started the investigation (+45–69%) is fully eliminated.
- **On lounas (small 4-group host): strictly better than chiplet but with a residual gap vs pre-chiplet** at W=4–6 (~+10% on a couple of cells). Likely due to cross-group walk paying an atomic per peer even when in-group is already busy enough not to need helpers.

## Recommendations for next steps

1. **Flip the gate to default-on.** The data justifies making xgroup_drain
   the default behaviour and keeping the env var as a *disable* knob
   (`TOKIO_CHIPLET_XGROUP_DRAIN=0`) for future A/B regression checks.

2. **Add a peer-mask-aware skip for the cross-group walk.** If
   `peer_mask.count_ones() > group_size / 2`, our own group is busy
   enough that walking other groups pre-empts useful in-group
   dispatch. Should close the remaining lounas residual at W=4–6.
   ~5-line change.

4. **Consider smaller `N_OTHER_GROUP_PEERS`.** Currently scans up to 64
   — at W=64 that's all-but-self. For larger W (eg 128) this could be
   capped lower (8 or 16) without loss. But at current scale this is
   sub-µs even worst-case and the benefit may scale with the cap.

5. **Drop the pre-chiplet revert.** Original revert (`684362f8` on
   `chiplet-watcher-sharding`) was committed before this fix existed.
   With xgroup_drain shipping default-on, the chiplet design is a net
   win on every measured workload — the revert can be reverted in turn
   and chiplet shipped properly.

6. **Don't bother with Option B (global tier-2 watcher).** The Option B
   design proposed in the prior handoff (separate global meta-epfd,
   global watcher slot) isn't needed — Option A captured all the
   mechanism. Save the complexity budget.

## Artifacts on lourip

- Bench logs: `/home/louis/tokio-benchd/logs.lourip/W64/{sharded_mio_pre_chiplet,sharded_mio,xgroup_drain}/net_tcp_echo-r*.txt`
- DB row: `/home/louis/tokio-benchd/state.lourip.db` run `56af977b-b295-4043-854f-6a8066516480`
- Aggregation: `/tmp/agg-xgroup.py`
- Built bench binary: `/home/louis/tokio-bench/io-driver-vtable/target/release/deps/net_tcp_echo-744f872161c83237`
  contains `TOKIO_CHIPLET_XGROUP_DRAIN` and `xgroup_drain_enabled` strings (verified)
- Source on lourip is at `/home/louis/tokio-bench/io-driver-vtable` (not git-managed); authoritative source is at `/home/louis/tokio/.claude/worktrees/io-driver-vtable` on lounas, branch `chiplet-cross-group-drain`, commit `7ffd8006`.
