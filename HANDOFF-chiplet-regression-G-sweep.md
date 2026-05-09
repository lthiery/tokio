# Chiplet meta-watcher G-sweep — empirical confirmation

**Date:** 2026-05-09 (follow-up to `HANDOFF-chiplet-regression.md`)
**Branch:** `chiplet-G-knob` (off tag `chiplet-regressed-eea395a7`)
**Host:** lourip (EPYC 7H12, 64C/128T, 8 CCDs / 16 CCXs)
**Sweep run:** `9a183a0d-1eee-4978-8636-1c6dfdb41aa8` (state.lourip.db)

## TL;DR

The original handoff posited two candidate mechanisms (MetaWaker fan-out;
cross-CCX wake hop) that I was able to **falsify by code reading** —
`MetaWaker::wake` is a single eventfd write, and the wake-dispatch chain
is structurally identical to pre-chiplet. The actual mechanism is **per-
group meta-watcher under-amortization**: with G=16 groups each watching
only N/G ≈ 4 child epolls, each group quiesces between event batches
and pays a futex+epoll_wait+ctxswitch round-trip per batch instead of
amortizing across all 64 children like the pre-chiplet single watcher does.

Confirmed empirically with a G-knob (`TOKIO_CHIPLET_G` env var that
coalesces natural CCX groups via integer division). Connect-churn
regression is **monotonic in G**:

| G | sharded_mio/tcp_connect_churn (W=64) | vs pre-chiplet |
|---|--------------------------------------|----------------|
| pre-chiplet (single global watcher) | 287.13 µs | baseline |
| G=1  (knob collapses to 1 group)    | 290.96 µs | **+1.3%** |
| G=2                                  | 309.84 µs | +7.9% |
| G=4                                  | 321.18 µs | +11.9% |
| G=8                                  | 366.69 µs | +27.7% |
| G=16 (natural per-CCX)               | 466.07 µs | **+62.3%** |

Monotonic and clean. G=1 is parity (within noise) — confirming the knob
works and that `MetaWaker`/`probe_chiplet_groups`/`ChipletGroup`
construction overhead is not itself the regression. The regression is
the *number of watchers*.

## Echo throughput tells the opposite story

| G | sharded_mio/tcp_echo_throughput (W=64) | vs pre-chiplet |
|---|----------------------------------------|----------------|
| pre-chiplet | 9364.20 µs | baseline |
| G=1   | 9404.70 µs | +0.4% |
| G=2   | 9698.90 µs | +3.6% |
| G=4   | 9542.40 µs | +1.9% |
| G=8   | 9607.00 µs | +2.6% |
| G=16  | 8102.20 µs | **−13.5% (faster)** |

So chiplet is a real architectural trade-off, not a pure regression:

- **Connect-churn loses** because every iteration creates fresh sockets
  with no time to develop CCX affinity → events scatter across all
  G=16 watchers → all 16 watchers bounce between idle and active →
  worst-case fragmentation tax.
- **Echo-throughput wins** because long-lived sockets bind to one CCX,
  the watcher in that CCX stays busy, and all related task scheduling
  benefits from staying within CCX-local L3.

A correct fix needs to keep the echo gain while avoiding the
connect-churn tax — a single env knob can't do that. G=1 fully restores
parity but defeats chiplet's purpose entirely; G=16 keeps the design
but bleeds 62% on connect-churn.

## Stability tail at high G

G≥4 produced occasional outlier reps where individual benchmark
samples ballooned 1000× (single sample at 681 ms vs 466 µs median for
G=16 connect-churn rep 4). Median is robust; mean is destroyed. These
look like transient stalls — possibly the level-triggered cascade
described in `MetaWaker::wake` going pathological when many tiny groups
all bounce concurrently. Worth investigating but not the headline.

## Knob implementation

In `tokio/src/runtime/io/sharded_mio_driver.rs::probe_chiplet_groups()`,
after the natural per-CCX group probe sets `(cpu_to_group, group_count)`:

```rust
let (cpu_to_group, group_count) = if let Ok(s) = std::env::var("TOKIO_CHIPLET_G") {
    if let Ok(target) = s.trim().parse::<u32>() {
        if target >= 1 && target < u8::MAX as u32 && target <= group_count as u32 {
            let target = target as u8;
            let bucket = (group_count + target - 1) / target;
            let mut max_new: u8 = 0;
            let mut overridden = cpu_to_group;
            for g in overridden.iter_mut() {
                if *g != u8::MAX && *g != u8::MAX - 1 {
                    let new = *g / bucket;
                    if new > max_new { max_new = new; }
                    *g = new;
                }
            }
            (overridden, max_new + 1)
        } else { (cpu_to_group, group_count) }
    } else { (cpu_to_group, group_count) }
} else { (cpu_to_group, group_count) };
```

Branch is preserved. Not for merging — purely a diagnostic tool.

## What this rules in / rules out

- ✅ Fragmentation tax is the regression mechanism (monotonic in G)
- ✅ Knob G=1 ≈ pre-chiplet (no hidden chiplet overhead in struct layout)
- ✅ Echo workload genuinely benefits from CCX-local watcher locality
- ❌ MetaWaker fan-out (already ruled out by code review — single eventfd
   write, no fan-out)
- ❌ Cross-CCX wake hop as originally posited (chain is structurally
   identical to pre-chiplet)
- ❌ Single env knob fix (the knob trades workloads, doesn't fix the
   underlying tension)

## Recommended next steps for whoever picks this up

1. **Don't merge `chiplet-G-knob`.** It's a diagnostic. The data is
   recorded; the branch can be deleted after this handoff is read.

2. **The real fix has to be hierarchical.** A per-CCX watcher gives
   echo its locality win; a fallback that lets ANY worker drain ANY
   CCX's children when its own group is quiet would prevent the
   fragmentation tax during connect-churn (because under churn, child
   distribution is uniform — all groups are non-quiet and the global
   coalescer would just behave like pre-chiplet's single watcher).

3. **Consider a gate threshold instead.** Currently `group_member_count
   > 1` is always true at W=64. Raising it (e.g. only enter chiplet
   mode when sustained per-group event rate exceeds threshold X)
   would dynamically fall back to pre-chiplet behaviour for churn-
   shaped workloads without code-time configuration.

4. **The G=8 (CCD-grouped) data point** (+27.7%) suggests grouping by
   CCD instead of CCX is *not* a free win — fragmentation tax dominates
   even at G=8. Don't bother trying CCD-only as a cheap alternative.

5. The `chiplet-watcher-sharding` branch in `tokio.git` already has
   the proper revert (`684362f8`); ship that for now. The chiplet
   commit is preserved at tag `chiplet-regressed-eea395a7` and the
   benchd `worktree-pre-chiplet` tree is set up on both lounas and
   lourip for ongoing A/B comparison work.

## Artifacts on lourip

- Bench logs: `/home/louis/tokio-benchd/logs.lourip/W64/{sharded_mio_pre_chiplet,G1,G2,G4,G8,G16}/net_tcp_echo-r*.txt`
- DB: `/home/louis/tokio-benchd/state.lourip.db` (run id above)
- Aggregation: `/tmp/agg-gsweep.py`
- Built bench binary (chiplet-G-knob): `/home/louis/tokio-bench/io-driver-vtable/target/release/deps/net_tcp_echo-744f872161c83237`
  contains `TOKIO_CHIPLET_G` and `shared_cpu_list` strings (verified)
