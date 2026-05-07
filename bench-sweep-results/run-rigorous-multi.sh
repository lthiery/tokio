#!/usr/bin/env bash
# Multi-machine W-axis rigorous sweep.
#
# Splits the (W, bench, mode) matrix between two machines:
#   lounas (16C/32T) — low W:  1, 2, 4, 6, 8
#   lourip (64C/128T) — high W: 6, 16, 32, 64
#
# W=6 is in BOTH lists as an overlap sanity check (small expected machine
# effect; large unexpected gap → discard cross-machine comparisons).
#
# Modes: mainline / sharded_mio
#
# (History: previously ran four modes — mainline / shard_epoll /
# shard_auto / shard_futex — by toggling TOKIO_FUTEX_PARK. That env
# var was removed at tokio commit ca7537a1 on worktree-nuke-spin, so
# the three shard modes produced byte-identical runs. Collapsed to one.)
#
# Bench layout:
#   W-axis benches run at every W in WORKER_LIST (TOKIO_BENCH_WORKERS=W)
#   Fixed-W benches run once per machine under "Wfixed" (W=6 taskset),
#     because their hardcoded fanout is coupled to a specific W and
#     doesn't honor TOKIO_BENCH_WORKERS.
#
# Output: bench-sweep-results/rigorous-multi/W{N}/<mode>/<bench>-r{R}.txt
#         bench-sweep-results/rigorous-multi/Wfixed/<mode>/<bench>-r{R}.txt
#
# Run as:
#   MACHINE=lounas bash bench-sweep-results/run-rigorous-multi.sh
#   MACHINE=lourip bash bench-sweep-results/run-rigorous-multi.sh
#
# Wall time estimate (REPS=5, MEASURE=8s, WARMUP=2s):
#   lounas: 5 W × 4 benches × 2 modes × 5 reps × ~15s ≈ 50 min
#         + fixed: 6 benches × 2 modes × 5 reps × ~15s ≈ 15 min
#         ≈ 1.1h
#   lourip: 4 W × 4 benches × 2 modes × 5 reps × ~15s ≈ 40 min
#         + fixed: 6 benches × 2 modes × 5 reps × ~15s ≈ 15 min
#         ≈ 0.9h
#   Run in parallel → wall time ~1.1h.

set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.."

MACHINE="${MACHINE:?MACHINE must be set to lounas or lourip}"
REPS="${REPS:-5}"
MEASURE="${MEASURE:-8}"
WARMUP="${WARMUP:-2}"

case "$MACHINE" in
  lounas) WORKER_LIST_DEFAULT="1 2 4 6 8" ;;
  lourip) WORKER_LIST_DEFAULT="6 16 32 64" ;;
  *) echo "MACHINE must be lounas or lourip; got $MACHINE" >&2; exit 1 ;;
esac
WORKER_LIST="${WORKER_LIST:-$WORKER_LIST_DEFAULT}"

# W-axis benches: rebuilt to honor TOKIO_BENCH_WORKERS via the workers()
# helper. Run for every W in WORKER_LIST.
W_AXIS_BENCHES=(sync_watch sync_broadcast sync_notify sync_mpsc)

# Fixed-W benches: hardcoded fanout (W=6, W=1, etc.); ignore
# TOKIO_BENCH_WORKERS. Run once per machine under "Wfixed".
FIXED_BENCHES=(sync_mpsc_oneshot sync_rwlock sync_semaphore remote_spawn spawn_blocking time_timeout)
FIXED_TASKSET="taskset -c 1-7"

OUT=bench-sweep-results/rigorous-multi
MODES=(mainline sharded_mio)

# Mainline path differs per machine
case "$MACHINE" in
  lounas) MAINLINE_DIR="${MAINLINE_DIR:-/home/louis/tokio/.claude/worktrees/mainline-baseline}" ;;
  lourip) MAINLINE_DIR="${MAINLINE_DIR:-/home/louis/tokio-bench/mainline-baseline}" ;;
esac

# Choose taskset range based on W (W workers + 1 bench/criterion thread).
# Cap at machine's physical cores so we don't oversubscribe SMT.
case "$MACHINE" in
  lounas) MAX_CORES=15 ;;   # 16 physical, leave 1 for OS
  lourip) MAX_CORES=63 ;;   # 64 physical, leave 1 for OS
esac
taskset_for_w() {
  local w=$1
  local end=$((w + 1))
  if [ "$end" -gt "$MAX_CORES" ]; then end=$MAX_CORES; fi
  echo "taskset -c 1-$end"
}

# Preflight: every cpu must be on the `performance` governor for stable
# numbers (the default `schedutil`/`ondemand` adds ramp-up jitter that
# corrupts criterion's noise estimate, especially at low W). Set with:
#   for c in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
#     echo performance | sudo tee "$c" >/dev/null
#   done
require_performance_governor() {
  local bad=0
  for f in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    [ "$(cat "$f")" = performance ] || bad=$((bad + 1))
  done
  if [ "$bad" -ne 0 ]; then
    echo "preflight: $bad cpu(s) not on 'performance' governor — refusing to run." >&2
    echo "  fix: for c in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do" >&2
    echo "         echo performance | sudo tee \"\$c\" >/dev/null" >&2
    echo "       done" >&2
    exit 3
  fi
}
require_performance_governor

# Pre-build both trees once (release, no --cfg tokio_unstable for either)
mkdir -p "$OUT/build-logs"

# Snapshot host state for reproducibility (one file per sweep run).
{
  echo "machine: $MACHINE  ($(uname -n))"
  echo "kernel:  $(uname -r)"
  echo "date:    $(date -Iseconds)"
  echo "governor (cpu0/63/last): \
$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor) \
$(cat /sys/devices/system/cpu/cpu63/cpufreq/scaling_governor 2>/dev/null || echo n/a) \
$(cat /sys/devices/system/cpu/cpu$(($(nproc) - 1))/cpufreq/scaling_governor)"
  echo "smt:     $(cat /sys/devices/system/cpu/smt/control 2>/dev/null || echo n/a)"
  echo "thp:     $(cat /sys/kernel/mm/transparent_hugepage/enabled)"
  echo "turbo:   $(cat /sys/devices/system/cpu/cpufreq/boost 2>/dev/null \
            || cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null \
            || echo n/a)"
  echo "WORKER_LIST=$WORKER_LIST  REPS=$REPS  MEASURE=$MEASURE  WARMUP=$WARMUP"
} > "$OUT/build-logs/host-$MACHINE.txt"

echo "==> Pre-build: mainline (stable) [$MAINLINE_DIR]" >&2
( cd "$MAINLINE_DIR/benches" && cargo build --release --benches ) \
  > "$OUT/build-logs/mainline-$MACHINE.log" 2>&1 || {
    echo "mainline build FAILED — see $OUT/build-logs/mainline-$MACHINE.log" >&2
    exit 2
  }
echo "==> Pre-build: sharded-mio (stable)" >&2
( cd benches && cargo build --release --benches --features bench-sharded-mio ) \
  > "$OUT/build-logs/shard-$MACHINE.log" 2>&1 || {
    echo "shard build FAILED — see $OUT/build-logs/shard-$MACHINE.log" >&2
    exit 2
  }

run_mainline() {
  local bench=$1 w=$2
  local TS; TS=$(taskset_for_w "$w")
  ( cd "$MAINLINE_DIR/benches" && \
      TOKIO_BENCH_WORKERS=$w \
      $TS cargo bench --bench "$bench" -- --warm-up-time "$WARMUP" --measurement-time "$MEASURE" )
}

run_shard() {
  local bench=$1 w=$2
  local TS; TS=$(taskset_for_w "$w")
  ( cd benches && \
      TOKIO_BENCH_WORKERS=$w \
      $TS cargo bench --bench "$bench" --features bench-sharded-mio -- --warm-up-time "$WARMUP" --measurement-time "$MEASURE" )
}

run_fixed_mainline() {
  local bench=$1
  ( cd "$MAINLINE_DIR/benches" && \
      $FIXED_TASKSET cargo bench --bench "$bench" -- --warm-up-time "$WARMUP" --measurement-time "$MEASURE" )
}

run_fixed_shard() {
  local bench=$1
  ( cd benches && \
      $FIXED_TASKSET cargo bench --bench "$bench" --features bench-sharded-mio -- --warm-up-time "$WARMUP" --measurement-time "$MEASURE" )
}

mkdir -p "$OUT"
echo "==> $MACHINE: WORKER_LIST=$WORKER_LIST  REPS=$REPS  ($(date))" >&2

# Phase 1: W-axis benches
for r in $(seq 1 "$REPS"); do
  echo "===== W-axis rep $r/$REPS on $MACHINE ($(date)) =====" >&2
  for w in $WORKER_LIST; do
    for b in "${W_AXIS_BENCHES[@]}"; do
      for mode in "${MODES[@]}"; do
        mkdir -p "$OUT/W$w/$mode"
        LOG="$OUT/W$w/$mode/$b-r$r.txt"
        echo "  W=$w $mode/$b r$r" >&2
        case "$mode" in
          mainline)    run_mainline "$b" "$w" > "$LOG" 2>&1 ;;
          sharded_mio) run_shard    "$b" "$w" > "$LOG" 2>&1 ;;
        esac
        [ $? -eq 0 ] || echo "  ! cell FAILED: W=$w $mode/$b r$r — see $LOG" >&2
      done
    done
  done
done

# Phase 2: fixed-W benches (TOKIO_BENCH_WORKERS unused here)
for r in $(seq 1 "$REPS"); do
  echo "===== Wfixed rep $r/$REPS on $MACHINE ($(date)) =====" >&2
  for b in "${FIXED_BENCHES[@]}"; do
    for mode in "${MODES[@]}"; do
      mkdir -p "$OUT/Wfixed/$mode"
      LOG="$OUT/Wfixed/$mode/$b-r$r.txt"
      echo "  Wfixed $mode/$b r$r" >&2
      case "$mode" in
        mainline)    run_fixed_mainline "$b" > "$LOG" 2>&1 ;;
        sharded_mio) run_fixed_shard    "$b" > "$LOG" 2>&1 ;;
      esac
      [ $? -eq 0 ] || echo "  ! cell FAILED: Wfixed $mode/$b r$r — see $LOG" >&2
    done
  done
done

echo "==> multi sweep on $MACHINE done ($(date))" >&2
