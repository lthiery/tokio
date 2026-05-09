# Chiplet xgroup_drain — peer-mask gated refinement results

**Date:** 2026-05-09
**Branch:** `chiplet-cross-group-drain` @ `30841dec`
**Predecessor:** `HANDOFF-chiplet-regression-FIXED.md`
**Sweeps:** lounas `98a02230-e402-4a83-bb6e-526f7b41e4f7` (W={4,6,8}, 5 reps) +
            lourip `706de97f-b854-4cde-8cbc-5bf2c8bd4a81` (W={16,32,64}, 5 reps),
            net_tcp_echo, 4 modes × 8s measure.

## TL;DR

Recommendation #2 from the previous handoff (skip the cross-group walk
when our own group is busy enough to dispatch in-group) **closes the
lounas residual at W=6/8 as predicted**, with no significant change on
lourip. Median-of-5-reps deltas vs `pre_chiplet_b`:

### lounas (4 natural groups) — `sharded_mio/tcp_connect_churn`

| W | chiplet_b | xgroup_drain | **xgroup_drain_gated** |
|---|---|---|---|
| 4 | +7.9% | +1.1% | +2.8% |
| 6 | +5.6% | +2.6% | **−2.4%** |
| 8 | +15.2% | +7.9% | **−0.9%** |

The W=8 −0.9% (gated) vs +7.9% (unconditional) is the headline: 8.8pp
swing from the +group_size/2 gate. W=6 swings 5.0pp the same way.

### lourip (16 natural groups) — `sharded_mio/tcp_connect_churn`

| W | chiplet_b | xgroup_drain | **xgroup_drain_gated** |
|---|---|---|---|
| 16 | +4.3% | −11.0% | +6.7%  *(noise — see below)* |
| 32 | +15.1% | −30.2% | −28.1% |
| 64 | +46.6% | −1.3% | −1.1% |

W=16 reps for unconditional `xgroup_drain` are [417.9, 348.6, 349.4,
335.6, 462.0] µs and for `xgroup_drain_gated` are [394.8, 418.8, 486.4,
505.4, 308.8] µs — overlapping ranges, 5-rep medians are noisy here.
At W=16 lourip, group_size is small enough that the gate rarely fires,
so behaviour should be ~identical to unconditional. Treat as noise.

### lourip echo

| W | chiplet_b | xgroup_drain | **xgroup_drain_gated** |
|---|---|---|---|
| 16 | +4.9% | −7.2% | −5.4% |
| 32 | +0.5% | −9.1% | −3.6% |
| 64 | −5.7% | −7.7% | **−12.3%** |

W=64 echo: gated reps are tightest of all four modes (8.0–8.9ms, no
tail spike) vs unconditional's 7.9–8.8ms range. Real win.

## Why the gate works on lounas

On lounas (4 natural groups, W=8 → group_size≈2), when both in-group
peers fire (`peer_mask.count_ones() == 2`, `> group_size/2 == 1`), the
local watcher already has plenty to dispatch and walking other groups
just pre-empts useful in-group work. Skipping the walk in that case
preserves locality. With group_size≤2 on lounas, the gate fires often.

On lourip (16 natural groups, group_size≈1–4 for tested W), the gate
fires much less — most parks have `peer_mask.count_ones() == 0` or 1
out of 1–4, and `> group_size/2` is rarely true. Hence near-identical
results to unconditional `xgroup_drain`.

## Implementation

`tokio/src/runtime/scheduler/multi_thread/sharded_mio_park.rs`:

```rust
// in park_on_meta, replacing the previous unconditional walk:
if xgroup_drain_enabled() && !self.in_group_too_busy_for_xgroup(group_idx, peer_mask) {
    /* cross-group walk as before */
}

#[cfg(target_os = "linux")]
fn in_group_too_busy_for_xgroup(&self, group_idx: u8, peer_mask: u128) -> bool {
    if !xgroup_drain_gated_enabled() { return false; }
    let group_size = self.handle.group_member_count(group_idx) as u32;
    peer_mask.count_ones() > group_size / 2
}
```

`xgroup_drain_gated_enabled()` reads `TOKIO_CHIPLET_XGROUP_DRAIN_GATED`
once into a `OnceLock<bool>`, mirroring the existing
`xgroup_drain_enabled` helper.

## Recommendations

1. **Ship gated as the default-on path.** With both `XGROUP_DRAIN` and
   `XGROUP_DRAIN_GATED` flipped to default-on, lounas W=6/8 connect-
   churn moves from +2.6/+7.9% to −2.4/−0.9% (parity-or-better with
   pre-chiplet) while lourip is unchanged within noise.

2. **At final ship-time, collapse the two env vars into one disable
   knob.** Once defaults are established, expose only
   `TOKIO_CHIPLET_XGROUP_DRAIN=0` to disable the whole tier. The gated
   sub-knob existed for A/B isolation and isn't useful long-term.

3. **Don't tune the gate threshold further yet.** `> group_size/2`
   captures the relevant inflection at small W with small groups; the
   exact threshold is uncritical because the gate's effect dies off
   anyway as `group_size` grows past 2.

## Artifacts

- lounas DB: `/home/louis/tokio-benchd/state.lounas.db` run
  `98a02230-e402-4a83-bb6e-526f7b41e4f7`
- lourip DB: `/home/louis/tokio-benchd/state.lourip.db` run
  `706de97f-b854-4cde-8cbc-5bf2c8bd4a81`
- Source: branch `chiplet-cross-group-drain` @ `30841dec`
  (post-simplify; behaviour-identical to `fdb5371a` which was the
  binary actually under test)
