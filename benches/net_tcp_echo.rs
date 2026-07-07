//! TCP echo throughput bench on the standard multi-thread runtime.
//! Used as the regression leg for the worker-local A/B: the workload never
//! touches worker-local APIs, so any delta between builds is pure run-loop
//! tax from the feature being compiled in.
//!
//! Trimmed from the io-driver bench harness: traditional runtime only.

use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};

const NUM_WORKERS: usize = 4;
const NUM_CONNS: usize = 32;
const MSGS_PER_CONN: usize = 64;

/// Override `NUM_WORKERS` at runtime via `TOKIO_BENCH_WORKERS=N`.
fn workers() -> usize {
    std::env::var("TOKIO_BENCH_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(NUM_WORKERS)
}

fn rt_traditional() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(workers())
        .enable_all()
        .build()
        .unwrap()
}

fn run_tcp_echo(rt: &Runtime, b: &mut Bencher) {
    b.iter_custom(|iters| {
        rt.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let server = tokio::spawn(async move {
                let total = NUM_CONNS * iters as usize;
                for _ in 0..total {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    tokio::spawn(async move {
                        let mut buf = [0u8; 1];
                        for _ in 0..MSGS_PER_CONN {
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
                let mut handles = Vec::with_capacity(NUM_CONNS);
                for _ in 0..NUM_CONNS {
                    handles.push(tokio::spawn(async move {
                        let mut s = TcpStream::connect(addr).await.unwrap();
                        s.set_nodelay(true).unwrap();
                        let mut buf = [0u8; 1];
                        for i in 0..MSGS_PER_CONN {
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

criterion_group!(net, bench_traditional);
criterion_main!(net);
