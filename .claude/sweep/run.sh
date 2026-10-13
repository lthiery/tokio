#!/usr/bin/env bash
# Placement sweep: {eager,lazy} x {rr,local} x shards, alternating rounds.
# Usage: run.sh <outdir> <server_cpus> <client_cpus> <rounds> [echo_bin] [accept_bin]
set -u
out=$1; scpu=$2; ccpu=$3; rounds=$4
echo_bin=${5:-$(ls ~/tokio-lazy-shards/target/release/deps/net_shard_echo-* | grep -v '\.d$' | head -1)}
acc_bin=${6:-$(ls ~/tokio-lazy-shards/target/release/deps/net_accept-* | grep -v '\.d$' | head -1)}
mkdir -p "$out"
export TOKIO_BENCH_WORKERS=${TOKIO_BENCH_WORKERS:-8} TOKIO_BENCH_SERVER_CPUS=$scpu TOKIO_BENCH_CLIENT_CPUS=$ccpu
export TOKIO_BENCH_CLIENT_WORKERS=${TOKIO_BENCH_CLIENT_WORKERS:-8} TOKIO_BENCH_PIN_PER_WORKER=${TOKIO_BENCH_PIN_PER_WORKER:-1}
export TOKIO_BENCH_CONNS=${TOKIO_BENCH_CONNS:-64} TOKIO_BENCH_ROUNDS=${TOKIO_BENCH_ROUNDS:-100}
# configs: shards eager local
# shards eager local connect_par
cfgs=("1 1 0 1" "1 0 0 1" "4 1 0 1" "4 0 0 1" "4 1 1 1" "4 0 1 1" \
      "1 1 0 64" "1 0 0 64" "4 1 0 64" "4 0 0 64" "4 1 1 64" "4 0 1 64")
echo "start $(date -Is) host=$(hostname) workers=$TOKIO_BENCH_WORKERS server=$scpu client=$ccpu conns=$TOKIO_BENCH_CONNS rounds=$TOKIO_BENCH_ROUNDS" | tee "$out/meta.txt"
for r in $(seq 1 "$rounds"); do
  for cfg in "${cfgs[@]}"; do
    set -- $cfg
    tag="s$1-e$2-l$3-p$4-r$r"
    TOKIO_BENCH_IO_SHARDS=$1 TOKIO_EAGER_TCP=$2 TOKIO_IO_SHARD_LOCAL=$3 TOKIO_BENCH_CONNECT_PAR=$4 \
      "$echo_bin" --bench --warm-up-time 1 --measurement-time 3 --noplot 2>&1 \
      | grep -E 'time:|io_shard_poll_stats|panicked|error' > "$out/echo-$tag.txt"
    [ "$4" = 1 ] && TOKIO_BENCH_IO_SHARDS=$1 TOKIO_EAGER_TCP=$2 TOKIO_IO_SHARD_LOCAL=$3 \
      taskset -c "$scpu,$ccpu" "$acc_bin" --bench 'task_per_conn/multi_thread_4|task_per_conn/multi_thread_1' \
      --warm-up-time 1 --measurement-time 3 --noplot 2>&1 \
      | grep -E '^[a-z_/0-9]+ *$|time:|task_per_conn/multi_thread_[14]: io_shard|panicked|error' > "$out/accept-$tag.txt"
    echo "done $tag $(date +%T)" >> "$out/meta.txt"
  done
done
echo "end $(date -Is)" >> "$out/meta.txt"
