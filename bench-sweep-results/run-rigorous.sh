#!/usr/bin/env bash
# Rigorous 2-way sweep — stable-vs-stable, no --cfg tokio_unstable.
#
#   mainline     — upstream tokio at /home/louis/tokio/.claude/worktrees/mainline-baseline
#   sharded_mio  — sharded-mio runtime; the runtime picks futex vs epoll
#                  per-park internally (sharded_mio_park.rs).
#
# Each (mode, bench) pair is run REPS times into separate files; the
# aggregator (aggregate-rigorous.py) takes the per-bench median across reps.
#
# Output layout:
#   bench-sweep-results/rigorous/<mode>/<bench>-r<N>.txt
#
# History: this script previously ran a 4-way matrix
# (mainline / shard_epoll / shard_auto / shard_futex) by toggling
# TOKIO_FUTEX_PARK. That env var was removed at tokio commit ca7537a1
# ("rt(sharded-mio): remove direct-futex park branch") on
# worktree-nuke-spin, so the three shard modes produced byte-identical
# runs. Collapsed to a single sharded_mio mode.
#
# Wall time estimate: ~1.2 hours.

set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.."

REPS="${REPS:-5}"
MEASURE="${MEASURE:-8}"
WARMUP="${WARMUP:-2}"

BENCHES=(
  sync_watch
  sync_broadcast
  sync_notify
  sync_mpsc
  sync_mpsc_oneshot
  remote_spawn
  spawn_blocking
  time_timeout
)

OUT=bench-sweep-results/rigorous
mkdir -p "$OUT/mainline" "$OUT/sharded_mio"

CRIT_FLAGS="--warm-up-time $WARMUP --measurement-time $MEASURE"

MAINLINE_DIR=/home/louis/tokio/.claude/worktrees/mainline-baseline

# Pin to physical cores 1-7 to reduce scheduler-placement noise (the bench
# thread plus six worker threads need 7 cores).
TASKSET="taskset -c 1-7"

# --- Pre-build (no --cfg tokio_unstable for either tree) ---
echo "==> Pre-build: mainline (stable)"
( cd "$MAINLINE_DIR/benches" && cargo build --release --benches ) \
  > "$OUT/build-mainline.log" 2>&1

echo "==> Pre-build: sharded-mio (stable)"
( cd benches && cargo build --release --benches --features bench-sharded-mio ) \
  > "$OUT/build-shard.log" 2>&1

run_mainline_rep() {
  local bench=$1; local rep=$2
  ( cd "$MAINLINE_DIR/benches" && \
      $TASKSET cargo bench --bench "$bench" -- $CRIT_FLAGS )
}

run_shard_rep() {
  local bench=$1; local rep=$2
  ( cd benches && \
      $TASKSET cargo bench --bench "$bench" --features bench-sharded-mio -- $CRIT_FLAGS )
}

# --- Run sweep ---
for r in $(seq 1 $REPS); do
  echo "===== rep $r/$REPS ($(date)) ====="
  for b in "${BENCHES[@]}"; do
    echo "  ==> mainline / $b (rep $r)"
    run_mainline_rep "$b" "$r" > "$OUT/mainline/$b-r$r.txt" 2>&1

    echo "  ==> sharded_mio / $b (rep $r)"
    run_shard_rep "$b" "$r" > "$OUT/sharded_mio/$b-r$r.txt" 2>&1
  done
done

echo "==> rigorous sweep done ($(date))"
