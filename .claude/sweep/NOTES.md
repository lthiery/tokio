# Placement sweep notes (io_shards #8450 + deferred TcpStream registration)

Tree: `lazy-on-io-shards` (pr-8450 head c4dcb3e5 + lazy 2 commits + EXPERIMENT
knobs). Knobs: TOKIO_EAGER_TCP (1 = register at construction), TOKIO_IO_SHARD_LOCAL
(1 = register on the current worker's group's shard, else #8450 round-robin).
Counter: RuntimeMetrics::io_shard_poll_stats = readiness polls split by polling
worker's group == source's shard group / other group / off-worker; plus
io_shard_registered = sources per shard. Tabulate with tabulate.py <dir>.

## lounas 2026-10-13 (EPYC 7302, 8 workers pinned 1/CPU on 0-7 = 4 CCX of 2
cores; client 8 workers on 8-15; 64 conns x 100 rounds; 3 alternating rounds)

Dir: lounas-1013-1610. Read with the table; the points that matter:

1. net_accept task_per_conn/4 workers, 4 shards: lazy 14.5-14.8 us vs eager
   15.8-17.4 (same direction and size as the plain-master A/B, -12.7%).
   Cross-group polls: rr 75% (3 of 4 shards are foreign, as expected for a
   coin flip), lazy+local 0% (burst of queued accepts -> tasks stolen before
   first poll -> registered where they run), eager+local 91% (everything on
   the acceptor's shard, tasks elsewhere). Timing between rr and local: noise.
   The 1-worker rows are NOT a 4-shard config: io_shards(4) is clamped to the
   worker count, registered_per_shard shows one shard; those rows are the
   known-noisy 1-worker shape, ignore.

2. echo/steady 8 workers 4 shards vs 1 shard: neutral on this box (18.0-18.5
   vs 18.2 ms). Cross-group fraction with BALANCED round-robin placement is
   41-45%: the work-stealing scheduler keeps moving tasks between groups
   (a readiness wake pulls the task to the shard's group, stealing pushes it
   away), so no static placement policy sticks.

3. echo/steady lazy+local under a TRICKLE of accepts (connect_par=1):
   registered_per_shard=[65,0,0,0], 65% cross, 10-15% slower than rr. The
   accepted stream's task sits in the acceptor's LIFO slot and makes its
   first poll on the acceptor's worker, so "register at first poll" follows
   the acceptor exactly like eager does. Under a BURST (connect_par=64) the
   spawned tasks overflow into the run queue, idle workers steal them, lazy
   spreads [3,34,12,16] and cross drops to 50%; still not balanced.

4. echo/fresh (connect + 8 rounds + drop): all policies and shard counts
   within noise (14.3-14.7 ms trickle, 4.7-4.8 ms burst).

Conclusion so far: lazy registration keeps its standalone merits (accept path
syscalls, never-used streams) and is a prerequisite for sane placement on a
sharded driver, but it is NOT sufficient: under work stealing the first poll
has no stable home and tasks keep migrating. The 3-way leg (worker-local
affinity + lazy + local pick) tests whether giving the task a home fixes both
the cross-group fraction and the imbalance. Branch `lazy-shards-worker-local`.

## hades (9950X3D, 2 CCDs of 8 cores, governor powersave: no passwordless
sudo, within-host comparisons only)
A = server 0-7 (one CCD), client 8-15. B = server 0-3,8-11 (2 shards -> one
group per CCD), client 4-7,12-15. Dirs hades-A-sameccd, hades-B-crossccd.

## 3-way leg: worker-local + lazy + local pick (branch `lazy-shards-worker-local`)

Stack: pr-8450 + lazy (2) + EXPERIMENT (2) + worker-local v3 (5 commits,
clean cherry-pick except a benches/Cargo.toml hunk) + 1 integration fix.
All three suites pass together (io_shards 10, tcp_deferred 7, worker_local 25).
WL bench mode: TOKIO_BENCH_WL=1 -> accept loop does run_on_worker(round
robin) + spawn_worker_local(serve), needs --cfg tokio_unstable + feature
worker-local (binary in ~/tokio-lazy-shards-wl/target/release/deps).

INTEGRATION BUG found and fixed (commit "rt: count worker-local work in
the pre-park shard sweep probe"): #8450's Parker::sweep asks `has_tasks()`
(regular queue only) before a worker blocks; a readiness event for the
worker's OWN worker-local task is fast-pathed into its worker-local queue
with no unpark; the worker then parks, finds its shard held by its
sibling, and sleeps on the standby condvar with NO timeout. Idle deadlock:
4 blockers cycling at the 10 ms sweep timeout, 4 standby sleepers, client
in epoll_wait(-1). ~1 in 3 runs. Bisect: eager stalls too (lazy not
involved), 1 shard never (no standby path), WL=0 never. Fix: probe uses
`has_any_work(&worker)`. 10/10 clean after. Lesson for both PR texts: any
new per-worker work source must hook every "is there work" probe,
including ones added by concurrent PRs.

First numbers (smoke, 8 workers, 4 shards, lazy+local, WL=1): cross-group
polls 0.0% and registered_per_shard [17,16,16,16] in BOTH trickle and
burst regimes (vs 40-65% cross and [65,0,0,0] without affinity). Steady
echo ~18 ms, same as 1 shard. The sweep (lounas-wl1-*) + same-binary WL=0
control (lounas-wl0ctl-*) give the full table.

## RESULTS of the 3-way leg (lounas, dirs lounas-wl1-1013-1646 / lounas-wl0ctl-1013-1646)

Same unstable binary; WL=1 = serve tasks worker-local (run_on_worker round
robin), WL=0 = stock tokio::spawn. 8 workers 1/CPU on 4 CCX, 4 shards unless
noted, 64 conns, steady = 100 rounds/conn, medians of 3 alternating rounds,
echo/steady ms per iteration (lower is better), cross = % of readiness polls
on a worker outside the source's shard group, reg = sockets per shard.

| config (4 shards)            | WL=1 steady trickle / burst | cross | reg            | WL=0 steady trickle / burst | cross | reg          |
|------------------------------|-----------------------------|-------|----------------|-----------------------------|-------|--------------|
| lazy + local pick            | 18.65 / 18.47               | 0.0%  | 17,16,16,16    | 20.93 / 19.10               | 64/47%| 65,0,0,0 / 4,19,20,22 |
| lazy + round-robin (#8450)   | 20.73 / 20.11               | 75%   | 17,16,16,16    | 17.24 / 19.19               | 43%   | 17,16,16,16  |
| eager + local pick           | 18.48 / 19.60               | 75%   | 65,0,0,0       | 21.91 / 20.50               | 63/65%| 65,0,0,0     |
| eager + round-robin          | 19.07 / 18.84               | 75%   | 17,16,16,16    | 18.21 / 18.23               | 44%   | 17,16,16,16  |
| 1 shard (lazy / eager)       | 17.67 / 19.00               | 0     | 65             | 18.96 / 18.79               | 0     | 65           |

fresh (connect + 8 rounds + drop): all 4-shard configs 14.3-14.9 ms trickle /
4.74-4.76 ms burst, EXCEPT WL=1 eager+local burst 5.29 ms (+11%: 61121
registrations on one shard). Cross-group fractions as in steady.

Reading:
1. Stock scheduler + #8450 round-robin = 43% cross-group polls, balanced. No
   static placement fixes it; tasks migrate (readiness pulls them to the
   shard's group, stealing pushes them off).
2. Affinity alone (WL=1, rr): 75% cross-group FOREVER (3 of 4 shards foreign,
   and a pinned task cannot migrate to fix it); +10% latency vs the fixed
   config. Affinity NEEDS placement.
3. Placement alone (lazy+local, stock): follows the acceptor (LIFO slot), 64%
   cross, [65,0,0,0], +21% latency in the trickle regime.
4. Affinity + lazy + local pick: 0.0% cross, balanced, in all four regimes;
   fastest 4-shard config and equal to 1 shard (18.5 vs 17.7-19.0). On this
   single-socket EPYC the cross-group wake costs ~10% at most, so the
   structural result (0% vs 43-75%, balanced vs one-shard pileup) is the
   headline; a multi-socket/NUMA box would be where the latency shows.
   Each of the three pieces is necessary: lazy moves the decision to first
   poll, the local pick uses it (#8450's own "acceptor's shard" follow-up
   done properly), worker-local gives the task the home that makes the
   first poll meaningful.
5. For the standalone lazy PR: it is the hook, not the fix. Say so.
6. Integration bug (see above) is a review note for #8450: a pre-park "any
   work?" probe is new surface every per-worker queue must hook.

## lourip leg 2026-11-05 (EPYC 7H12, 16 CCX of 4 cores, 1 NUMA node)

Same unstable binary as the lounas 3-way leg (net_shard_echo-7eb63e0bf8b226a3,
copied to lourip:~/placement-sweep/bin/net_shard_echo; glibc 2.39 on both, no
target-cpu). Server 32 workers pinned 1/CPU on 0-31 = 8 CCX, so with 8 shards
one shard group = one CCX; 32 shards = per-worker grain. Client 32 workers on
32-63. 256 conns, steady 100 rounds/conn, burst = 256. net_accept rows skipped
(WITH_ACCEPT=0). run.sh grew SHARDS/BURST/WITH_ACCEPT knobs for this.
Driver: lourip:~/placement-sweep/all.sh as user unit `placement-sweep`
(WL=1 sweep then WL=0 control, 3 alternating rounds each, ~45 min each);
marker ALLDONE-<ts>; results dirs lourip-wl1-1105-1555 / lourip-wl0-1105-1555,
tabulate with tabulate.py. Smoke (8 shards, lazy+local, WL=1): steady 54 ms,
fresh 47 ms, cross 0.0%, reg [33,32x7].

## Reference branches rebuilt 2026-11-05 (public pointers for the PR texts)

On lthiery, built on #8450's CURRENT head 3ce02b2e (his 10-24 master merge):
- `io-shards-worker-local`: 3ce02b2e + lazy (2, the rebased
  tcp-deferred-registration commits) + worker-local v3 (5, from 94f17d4b) +
  probe fix. Every pick clean. fmt, clippy (upstream warnings only), and
  io_shards 10 / worker_local 25 / tcp_deferred 7 / tcp_accept_ready 9 green
  TOGETHER with --features full,worker-local + tokio_unstable.
- `io-shards-worker-local-bench`: = that + the 3 EXPERIMENT commits (knobs,
  counters, net_shard_echo, TOKIO_BENCH_WL). One benches/Cargo.toml hunk
  (both sides add a [[bench]]), kept both.
`lazy-shards-worker-local` (5bc8bed9, on his 10-13 head) stays as the
provenance of the 10-13 tables; do not delete.
Deadlock repro on the NEW head (8 workers, 4 shards, WL=1, rr, trickle;
`--bench --warm-up-time 1 --measurement-time 3`, 90 s timeout each):
fix-reverted control eager 3/8 HANG, lazy 0/8 (10-13 said both hung; today
only eager did in 8 runs, treat lazy as "less often", not "never"); fixed
binary 10/10 clean, same invocation. #8450's sweep/standby code did not change between
his 10-13 and 10-24 heads (his only own commits: a time test + a flaky-test
fix), so the finding stands on the current head. GOTCHA: a steady-only 2 s
run never hangs in 10 tries; the repro needs the fresh case (connect/drop
churn) in the same process.


## 2026-11-08: stack rebuilt on the mio `register_shared` lazy commits

Branches (local worktrees ~/tokio-shards-shared-reg and ~/tokio-shards-shared-reg-bench):

- `io-shards-shared-reg` @ f5d9866b = #8450 @ 3ce02b2e + DO NOT MERGE mio path
  patch (../mio-shared-registration, branch shared-registration-v3) + rt/net
  from `tcp-shared-registration` (cherry-picked; one conflict: #8450's
  add_source picks a shard, now done inside `add_source_with`) + worker-local
  v3 (5) + probe fix. Supersedes `io-shards-worker-local` (SourceFd lazy).
- `io-shards-shared-reg-bench` = that + 4 EXPERIMENT commits. Changes vs the
  old EXPERIMENT: record_poll moved onto `self.registered()`; the
  TOKIO_EAGER_TCP override makes `new_deferred` return `io::Result` again (2
  `?` in stream.rs); `eager_override` no longer unix-only. Sweep driver knobs
  (SHARDS/BURST/WITH_ACCEPT) committed.

Build: `RUSTFLAGS=--cfg tokio_unstable cargo bench -p benches --features
worker-local --bench net_shard_echo --bench net_accept --no-run` (the benches
crate feature `worker-local` turns on tokio's; without it net_shard_echo does
not compile).

Everything is platform-neutral now (no cfg(unix) in the lazy path), so this
tree is what runs on hades/Windows (`ssh hades-win`, toolchain + clones ready).
Question for Windows: one IOCP per shard, a rearm syscall per WouldBlock; does
cross-group placement cost more than on epoll, and does hades' 2x96MB L3 turn
the 0% vs 43-75% cross-group difference into latency.

Smoke (lounas, 4+4 workers on 0-3/4-7, 2 shards, 16 conns, 20 rounds, suite
compiling on the box at the same time, so timings are NOT clean): knobs all
behave, WL=1 gives 0% cross-group again. lazy+rr steady came out 11.3 ms vs
1.8 ms for the other three configs; re-check on a quiet box before believing
it.
