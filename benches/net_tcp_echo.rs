//! TCP echo throughput bench. Exercises the io-driver registration and
//! readiness path. Used to compare backends (traditional / sharded-mio /
//! uring) and to check for regressions across the IoDriver vtable refactor.
//!
//! Build matrix:
//! - default features: traditional mio only
//! - `--features bench-sharded-mio` + `--cfg tokio_unstable`: adds sharded-mio
//! - `--features bench-uring-reactor` + `--cfg tokio_unstable`: adds uring
//!
//! Each backend's bench function exists only when its feature is built.

use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};

const NUM_WORKERS: usize = 4;
const NUM_CONNS: usize = 32;
const MSGS_PER_CONN: usize = 64;
const MSG_BYTES: usize = 1;

/// Override `NUM_WORKERS` at runtime via `TOKIO_BENCH_WORKERS=N`.
/// Used during the lazy-register hang investigation to compare 1-worker
/// vs multi-worker behavior of the synchronous fastpath.
fn workers() -> usize {
    env_usize("TOKIO_BENCH_WORKERS", NUM_WORKERS)
}

/// Concurrent connections per criterion iteration. Override via
/// `TOKIO_BENCH_CONNS=N`. Default 32. Raising this is how the
/// RING_CAP high-concurrency probe forces enough offered concurrency
/// to keep all cores busy (so a low ring cap can actually throttle).
fn conns() -> usize {
    env_usize("TOKIO_BENCH_CONNS", NUM_CONNS)
}

/// Round-trips per connection. Override via `TOKIO_BENCH_MSGS=N`.
/// Default 64. Raise to make connections long-lived (sustained
/// throughput, minimal connect/close churn).
fn msgs_per_conn() -> usize {
    env_usize("TOKIO_BENCH_MSGS", MSGS_PER_CONN)
}

/// Payload bytes per message. Override via `TOKIO_BENCH_MSG_BYTES=N`.
/// Default 1 (the historical latency-bound ping-pong). Raise to move
/// real data so post-wake userspace work has a cache-coherency cost —
/// the regime where topology-blind ring placement could matter.
fn msg_bytes() -> usize {
    env_usize("TOKIO_BENCH_MSG_BYTES", MSG_BYTES).max(1)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

fn rt_traditional() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(workers())
        .enable_all()
        .build()
        .unwrap()
}

#[cfg(all(feature = "bench-sharded-mio", target_os = "linux"))]
fn rt_sharded_mio() -> Runtime {
    let mut b = Builder::new_multi_thread();
    b.worker_threads(workers()).enable_all();
    b.enable_sharded_mio();
    b.build().unwrap()
}

#[cfg(all(tokio_unstable, feature = "bench-uring-reactor", target_os = "linux"))]
fn rt_uring() -> Runtime {
    let mut b = Builder::new_multi_thread();
    b.worker_threads(workers()).enable_all();
    b.enable_uring_reactor();
    b.build().unwrap()
}

fn run_tcp_echo(rt: &Runtime, b: &mut Bencher) {
    let num_conns = conns();
    let msgs = msgs_per_conn();
    let bytes = msg_bytes();
    b.iter_custom(|iters| {
        rt.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let server = tokio::spawn(async move {
                let total = num_conns * iters as usize;
                for _ in 0..total {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; bytes];
                        for _ in 0..msgs {
                            if sock.read_exact(&mut buf).await.is_err() {
                                return;
                            }
                            if sock.write_all(&buf).await.is_err() {
                                return;
                            }
                        }
                    });
                }
            });

            let start = Instant::now();
            for _ in 0..iters {
                let mut handles = Vec::with_capacity(num_conns);
                for _ in 0..num_conns {
                    handles.push(tokio::spawn(async move {
                        let mut s = TcpStream::connect(addr).await.unwrap();
                        s.set_nodelay(true).unwrap();
                        let mut buf = vec![0u8; bytes];
                        for i in 0..msgs {
                            buf[0] = (i & 0xff) as u8;
                            s.write_all(&buf).await.unwrap();
                            s.read_exact(&mut buf).await.unwrap();
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            }
            let elapsed = start.elapsed();
            server.abort();
            let _ = server.await;
            elapsed
        })
    });
}

fn run_tcp_connect_churn(rt: &Runtime, b: &mut Bencher) {
    b.iter_custom(|iters| {
        rt.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let server = tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((sock, _)) => drop(sock),
                        Err(_) => return,
                    }
                }
            });

            let start = Instant::now();
            for _ in 0..iters {
                let mut handles = Vec::with_capacity(NUM_CONNS);
                for _ in 0..NUM_CONNS {
                    handles.push(tokio::spawn(async move {
                        let _ = TcpStream::connect(addr).await;
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            }
            let elapsed = start.elapsed();
            server.abort();
            let _ = server.await;
            elapsed
        })
    });
}

fn bench_traditional(c: &mut Criterion) {
    let rt = rt_traditional();
    c.bench_function("traditional/tcp_echo_throughput", |b| run_tcp_echo(&rt, b));
    c.bench_function("traditional/tcp_connect_churn", |b| {
        run_tcp_connect_churn(&rt, b)
    });
}

#[cfg(all(feature = "bench-sharded-mio", target_os = "linux"))]
fn bench_sharded_mio(c: &mut Criterion) {
    let rt = rt_sharded_mio();
    c.bench_function("sharded_mio/tcp_echo_throughput", |b| run_tcp_echo(&rt, b));
    c.bench_function("sharded_mio/tcp_connect_churn", |b| {
        run_tcp_connect_churn(&rt, b)
    });
}

#[cfg(not(all(feature = "bench-sharded-mio", target_os = "linux")))]
fn bench_sharded_mio(_c: &mut Criterion) {}

#[cfg(all(tokio_unstable, feature = "bench-uring-reactor", target_os = "linux"))]
fn bench_uring(c: &mut Criterion) {
    let rt = rt_uring();
    c.bench_function("uring/tcp_echo_throughput", |b| run_tcp_echo(&rt, b));
    c.bench_function("uring/tcp_connect_churn", |b| {
        run_tcp_connect_churn(&rt, b)
    });
}

#[cfg(not(all(tokio_unstable, feature = "bench-uring-reactor", target_os = "linux")))]
fn bench_uring(_c: &mut Criterion) {}

criterion_group!(net, bench_traditional, bench_sharded_mio, bench_uring);
criterion_main!(net);
