#!/usr/bin/env bash
# Single-mode sharded-mio sweep.
#
# History: this script previously ran a 3-way comparison
# (epoll / auto / futex) of the sharded-mio park substrate by toggling
# TOKIO_FUTEX_PARK. That env var was removed at tokio commit ca7537a1
# ("rt(sharded-mio): remove direct-futex park branch") on
# worktree-nuke-spin — substrate selection now lives entirely inside
# the runtime (sharded_mio_park.rs), so the three modes produced
# byte-identical runs. Collapsed to a single sharded_mio mode.
#
# Output: bench-sweep-results/sharded_mio/<bench>.txt

set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.."

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

OUT=bench-sweep-results
mkdir -p "$OUT/sharded_mio"

CRIT_FLAGS="--warm-up-time 2 --measurement-time 4"

run_shard() {
  local bench=$1
  echo "==> sharded_mio / $bench"
  ( cd benches && \
      RUSTFLAGS="--cfg tokio_unstable" \
      cargo bench --bench "$bench" --features bench-sharded-mio -- $CRIT_FLAGS ) \
    > "$OUT/sharded_mio/$bench.txt" 2>&1
}

echo "==> Pre-build: sharded mio"
( cd benches && RUSTFLAGS="--cfg tokio_unstable" cargo build --release --benches --features bench-sharded-mio ) \
  > "$OUT/build-shard-autogate.log" 2>&1

for b in "${BENCHES[@]}"; do
  run_shard "$b"
done

echo "==> sharded_mio sweep done"
date
