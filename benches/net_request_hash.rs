//! Request/response server with per-session keyed hashing — a proxy for a
//! "read request, do crypto on it, respond" service where session state is
//! stateful (each request's key depends on the previous digest).
//!
//! Compares two implementations of the same wire protocol:
//!  - `shared_arc`: `tokio::spawn` handlers, sessions in `Arc<Mutex<HashMap>>`
//!  - `worker_local`: connections dispatched round-robin to workers, handlers
//!    spawned via `spawn_worker_local`, sessions in per-worker
//!    `Rc<RefCell<HashMap>>` (no atomics or locks on the session path)
//!
//! The `worker_local` variant exists only when built with
//! `--features worker-local` + `--cfg tokio_unstable`.
//!
//! Wire protocol: request = [8B session id][REQ_BYTES payload],
//! response = [8B digest]. digest = keyed FNV-1a over the payload for
//! HASH_ROUNDS rounds; the session key rolls to the digest after each
//! request.
//!
//! Knobs (env): TOKIO_BENCH_WORKERS, TOKIO_BENCH_HASH_ROUNDS,
//! TOKIO_BENCH_REQ_BYTES, TOKIO_BENCH_CONNS, TOKIO_BENCH_MSGS.

use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};

const NUM_WORKERS: usize = 4;
const NUM_CONNS: usize = 32;
const MSGS_PER_CONN: usize = 32;
const REQ_BYTES: usize = 512;
const HASH_ROUNDS: usize = 8;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

fn workers() -> usize {
    env_usize("TOKIO_BENCH_WORKERS", NUM_WORKERS)
}

fn hash_rounds() -> usize {
    env_usize("TOKIO_BENCH_HASH_ROUNDS", HASH_ROUNDS)
}

fn req_bytes() -> usize {
    env_usize("TOKIO_BENCH_REQ_BYTES", REQ_BYTES)
}

fn num_conns() -> usize {
    env_usize("TOKIO_BENCH_CONNS", NUM_CONNS)
}

fn msgs_per_conn() -> usize {
    env_usize("TOKIO_BENCH_MSGS", MSGS_PER_CONN)
}

fn rt() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(workers())
        .enable_all()
        .build()
        .unwrap()
}

/// Keyed FNV-1a over the payload, `rounds` times. Stands in for a
/// per-request crypto operation whose cost scales with payload size.
fn keyed_hash(key: u64, payload: &[u8], rounds: usize) -> u64 {
    let mut digest = key ^ 0xcbf2_9ce4_8422_2325;
    for _ in 0..rounds {
        for &b in payload {
            digest ^= b as u64;
            digest = digest.wrapping_mul(0x100_0000_01b3);
        }
    }
    digest
}

/// Client half, shared by both variants: open NUM_CONNS connections, each
/// sending MSGS_PER_CONN requests and reading the digest back. Returns the
/// elapsed time for `iters` full rounds.
async fn run_clients(addr: std::net::SocketAddr, iters: u64) -> std::time::Duration {
    let conns = num_conns();
    let msgs = msgs_per_conn();
    let payload_len = req_bytes();

    let start = Instant::now();
    for _ in 0..iters {
        let mut handles = Vec::with_capacity(conns);
        for session in 0..conns {
            handles.push(tokio::spawn(async move {
                let mut s = TcpStream::connect(addr).await.unwrap();
                s.set_nodelay(true).unwrap();
                let mut req = vec![0u8; 8 + payload_len];
                req[..8].copy_from_slice(&(session as u64).to_le_bytes());
                let mut resp = [0u8; 8];
                for i in 0..msgs {
                    req[8] = (i & 0xff) as u8;
                    s.write_all(&req).await.unwrap();
                    s.read_exact(&mut resp).await.unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }
    start.elapsed()
}

/// Serve one connection: read requests, hash with the session's rolling key
/// via `lookup` (which owns the variant-specific session store access), and
/// respond with the digest.
async fn serve_conn<F>(mut sock: TcpStream, mut roll: F)
where
    F: FnMut(u64, &[u8]) -> u64,
{
    let payload_len = req_bytes();
    let mut req = vec![0u8; 8 + payload_len];
    loop {
        if sock.read_exact(&mut req).await.is_err() {
            return;
        }
        let session = u64::from_le_bytes(req[..8].try_into().unwrap());
        let digest = roll(session, &req[8..]);
        if sock.write_all(&digest.to_le_bytes()).await.is_err() {
            return;
        }
    }
}

/// Sessions behind a global `Arc<Mutex>`; handlers spawned onto the shared
/// work-stealing pool.
fn bench_shared_arc(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("shared_arc/request_hash", |b: &mut Bencher| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let sessions = Arc::new(Mutex::new(HashMap::<u64, u64>::new()));
                let rounds = hash_rounds();

                let server = tokio::spawn(async move {
                    loop {
                        let (sock, _) = match listener.accept().await {
                            Ok(pair) => pair,
                            Err(_) => return,
                        };
                        sock.set_nodelay(true).unwrap();
                        let sessions = sessions.clone();
                        tokio::spawn(serve_conn(sock, move |session, payload| {
                            let mut sessions = sessions.lock().unwrap();
                            let key = sessions.entry(session).or_insert(session);
                            let digest = keyed_hash(*key, payload, rounds);
                            *key = digest;
                            digest
                        }));
                    }
                });

                let elapsed = run_clients(addr, iters).await;
                server.abort();
                let _ = server.await;
                elapsed
            })
        })
    });
}

/// Sessions sharded per worker in `Rc<RefCell<HashMap>>`; connections
/// dispatched round-robin to a worker-local dispatcher task per worker.
#[cfg(all(tokio_unstable, feature = "worker-local"))]
fn bench_worker_local(c: &mut Criterion) {
    use std::cell::RefCell;
    use std::rc::Rc;
    use tokio::sync::mpsc;

    let rt = rt();
    c.bench_function("worker_local/request_hash", |b: &mut Bencher| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let n_workers = workers();
                let rounds = hash_rounds();

                // One dispatcher per worker: receives sockets and spawns a
                // worker-local handler per connection. Session state is a
                // per-worker Rc<RefCell<..>> — no locks, no atomics.
                let mut txs = Vec::with_capacity(n_workers);
                for w in 0..n_workers {
                    let (tx, mut rx) = mpsc::unbounded_channel::<TcpStream>();
                    txs.push(tx);
                    tokio::task::run_on_worker(w, move || {
                        tokio::task::spawn_worker_local(async move {
                            let sessions = Rc::new(RefCell::new(HashMap::<u64, u64>::new()));
                            while let Some(sock) = rx.recv().await {
                                let sessions = sessions.clone();
                                tokio::task::spawn_worker_local(serve_conn(
                                    sock,
                                    move |session, payload| {
                                        let mut sessions = sessions.borrow_mut();
                                        let key = sessions.entry(session).or_insert(session);
                                        let digest = keyed_hash(*key, payload, rounds);
                                        *key = digest;
                                        digest
                                    },
                                ));
                            }
                        });
                    });
                }

                let server = tokio::spawn(async move {
                    let mut next = 0usize;
                    loop {
                        let (sock, _) = match listener.accept().await {
                            Ok(pair) => pair,
                            Err(_) => return,
                        };
                        sock.set_nodelay(true).unwrap();
                        // Round-robin conn placement across workers.
                        if txs[next % txs.len()].send(sock).is_err() {
                            return;
                        }
                        next += 1;
                    }
                });

                let elapsed = run_clients(addr, iters).await;
                server.abort();
                let _ = server.await;
                elapsed
            })
        })
    });
}

#[cfg(not(all(tokio_unstable, feature = "worker-local")))]
fn bench_worker_local(_c: &mut Criterion) {}

criterion_group!(net, bench_shared_arc, bench_worker_local);
criterion_main!(net);
