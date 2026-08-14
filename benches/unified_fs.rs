//! Unified-driver fs benchmark: measures `tokio::fs` op throughput and a
//! mixed net + fs workload across the three io backends the unified-driver
//! story compares.
//!
//! Build/run matrix (the arm is chosen by build features + which runtime
//! the case builds, NOT by an env knob, so each arm benches a fixed
//! workload):
//!
//! - **unified**: `--features bench-uring-reactor` + `--cfg tokio_unstable`.
//!   The `reactor/*` cases build an `enable_uring_reactor()` runtime, so
//!   `tokio::fs` open/read/write ride the single shared reactor ring: one
//!   event system for both net readiness and fs completions.
//! - **sidecar**: `--features bench-uring-sidecar` + `--cfg tokio_unstable`.
//!   The `traditional/*` cases build a stock multi-thread runtime; with the
//!   `io-uring` feature compiled but no reactor, `tokio::fs` uses the legacy
//!   fs side-driver's separate ring while net readiness stays on mio: two
//!   event systems.
//! - **baseline**: default features (no io-uring). The `traditional/*` cases
//!   fall back to `spawn_blocking` fs, the pre-uring world.
//!
//! So `unified vs sidecar` is `reactor/*` (build A) vs `traditional/*`
//! (build A), and `baseline` is `traditional/*` (build B). Self-contained
//! and overlay-friendly like `net_tcp_echo.rs`.

use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};

const NUM_WORKERS: usize = 4;
/// Concurrent fs op chains per iteration.
const FS_TASKS: usize = 32;
/// Bytes per file (a page-ish payload; big enough that the copy is real,
/// small enough that syscall/submission overhead dominates, which is what
/// the driver comparison is about).
const FILE_BYTES: usize = 4096;
/// Concurrent echo connections in the mixed case. Multiple conns (not one
/// sequential conn) so the mixed number reflects the realistic readiness
/// win, not the reactor's known single-conn W1/W2 per-event tax.
const ECHO_CONNS: usize = 16;
/// Echo round-trips per connection, run concurrently with the fs churn.
const ECHO_MSGS: usize = 64;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn workers() -> usize {
    env_usize("TOKIO_BENCH_WORKERS", NUM_WORKERS)
}

fn fs_tasks() -> usize {
    env_usize("TOKIO_BENCH_FS_TASKS", FS_TASKS)
}

fn echo_conns() -> usize {
    env_usize("TOKIO_BENCH_ECHO_CONNS", ECHO_CONNS)
}

fn rt_traditional() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(workers())
        .enable_all()
        .build()
        .unwrap()
}

#[cfg(all(tokio_unstable, feature = "bench-uring-reactor", target_os = "linux"))]
fn rt_uring() -> Runtime {
    let mut b = Builder::new_multi_thread();
    b.worker_threads(workers()).enable_all();
    b.enable_uring_reactor();
    b.build().unwrap()
}

/// One fs op chain: write `FILE_BYTES` then read them back. Both ops route
/// through `Op::write_at` / `Op::read_at` on a uring build, or
/// `spawn_blocking` on a baseline build.
async fn fs_chain(path: PathBuf, payload: &'static [u8]) {
    tokio::fs::write(&path, payload).await.unwrap();
    let got = tokio::fs::read(&path).await.unwrap();
    assert_eq!(got.len(), payload.len());
}

/// Files-only workload: `fs_tasks()` concurrent write+read chains per
/// iteration, into a fresh temp dir.
fn fs_only(b: &mut Bencher<'_>, rt: &Runtime) {
    static PAYLOAD: [u8; FILE_BYTES] = [0xABu8; FILE_BYTES];
    b.iter(|| {
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let n = fs_tasks();
            let mut tasks = Vec::with_capacity(n);
            for i in 0..n {
                let path = dir.path().join(format!("f{i}.bin"));
                tasks.push(tokio::spawn(fs_chain(path, &PAYLOAD)));
            }
            for t in tasks {
                t.await.unwrap();
            }
        });
    });
}

/// One echo connection: `ECHO_MSGS` sequential 8-byte round-trips.
async fn echo_conn(addr: std::net::SocketAddr) {
    let mut client = TcpStream::connect(addr).await.unwrap();
    let msg = [0x5Au8; 8];
    let mut got = [0u8; 8];
    for _ in 0..ECHO_MSGS {
        client.write_all(&msg).await.unwrap();
        client.read_exact(&mut got).await.unwrap();
    }
}

/// Mixed workload: `echo_conns()` concurrent echo connections run alongside
/// the fs churn, so both event paths (net readiness and fs completions) are
/// exercised on the same runtime at once. On the unified arm both ride one
/// ring; on the sidecar arm they ride two. Multiple concurrent conns (not a
/// single sequential one) so the readiness path is at the worker counts
/// where uring wins, not pinned to the single-conn per-event tax.
fn mixed(b: &mut Bencher<'_>, rt: &Runtime) {
    static PAYLOAD: [u8; FILE_BYTES] = [0xCDu8; FILE_BYTES];
    b.iter(|| {
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let n_echo = echo_conns();

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            // Accept loop: one echo task per accepted connection.
            let server = tokio::spawn(async move {
                let mut accepted = Vec::with_capacity(n_echo);
                for _ in 0..n_echo {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    accepted.push(tokio::spawn(async move {
                        let mut buf = [0u8; 8];
                        for _ in 0..ECHO_MSGS {
                            sock.read_exact(&mut buf).await.unwrap();
                            sock.write_all(&buf).await.unwrap();
                        }
                    }));
                }
                for t in accepted {
                    t.await.unwrap();
                }
            });

            // fs churn concurrent with the echo traffic.
            let n_fs = fs_tasks();
            let mut fs = Vec::with_capacity(n_fs);
            for i in 0..n_fs {
                let path = dir.path().join(format!("m{i}.bin"));
                fs.push(tokio::spawn(fs_chain(path, &PAYLOAD)));
            }

            let mut clients = Vec::with_capacity(n_echo);
            for _ in 0..n_echo {
                clients.push(tokio::spawn(echo_conn(addr)));
            }
            for c in clients {
                c.await.unwrap();
            }
            server.await.unwrap();
            for t in fs {
                t.await.unwrap();
            }
        });
    });
}

fn bench_traditional(c: &mut Criterion) {
    let rt = rt_traditional();
    // `traditional/*` = sidecar fs when io-uring is compiled (sidecar arm),
    // spawn_blocking fs otherwise (baseline arm).
    c.bench_function("traditional/fs_only", |b| fs_only(b, &rt));
    c.bench_function("traditional/mixed", |b| mixed(b, &rt));
}

#[cfg(all(tokio_unstable, feature = "bench-uring-reactor", target_os = "linux"))]
fn bench_uring(c: &mut Criterion) {
    let rt = rt_uring();
    // `reactor/*` = the unified single-ring driver (fs on the reactor).
    c.bench_function("reactor/fs_only", |b| fs_only(b, &rt));
    c.bench_function("reactor/mixed", |b| mixed(b, &rt));
}

#[cfg(not(all(tokio_unstable, feature = "bench-uring-reactor", target_os = "linux")))]
fn bench_uring(_c: &mut Criterion) {}

criterion_group!(unified_fs, bench_traditional, bench_uring);
criterion_main!(unified_fs);
