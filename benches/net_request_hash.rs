//! Request/response server with per-session keyed hashing: a proxy for a
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
//!
//! Runtime-config knobs, applied to the shared runtime all variants run on:
//! TOKIO_BENCH_GLOBAL_QUEUE_INTERVAL and TOKIO_BENCH_EVENT_INTERVAL set the
//! corresponding builder options; TOKIO_BENCH_DISABLE_LIFO=1 disables the
//! LIFO slot (needs --cfg tokio_unstable). The LIFO knob matters for
//! fairness: the stealing variants' handlers ride the LIFO slot on the
//! read->respond wake path, while worker-local tasks have no LIFO slot.
//!
//! The `*_churn` cases are the open-loop variants: `TOKIO_BENCH_CONCURRENCY`
//! (default 32) connections stay in flight continuously, each connection is
//! a globally unique session (created on first request, evicted on
//! disconnect), and a completed connection's slot immediately opens the
//! next, so accept, spawn, and session-store churn overlap with in-flight
//! serving, like a real server. `stealing_local/request_hash_churn` keeps
//! the rolling key in the handler task (no store, no locks, stealable
//! tasks): the pure-scheduling comparator for worker_local under churn.
//!
//! Asymmetric-load knobs: TOKIO_BENCH_HASH_ROUNDS_MIN / _MAX draw the
//! per-hash round count uniformly from [min, max] instead of the fixed
//! TOKIO_BENCH_HASH_ROUNDS. TOKIO_BENCH_ROUNDS_GRAIN picks the draw grain:
//! "request" (default; each request draws independently, so imbalance
//! averages out over a connection) or "session" (each connection draws
//! once for all its requests, so a heavy session pins its entire cost to
//! one worker under worker-local dispatch, the shape work stealing can
//! rescue). Draws are deterministic (splitmix64 of session/request id),
//! so both server variants hash identical work.
//!
//! Variable request sizes: TOKIO_BENCH_REQ_BYTES_MIN / _MAX draw each
//! request's payload length uniformly from [min, max] (default: the fixed
//! TOKIO_BENCH_REQ_BYTES), with the same grain as the rounds draw. Client
//! and server both derive the length from (session, request index), so the
//! wire format is unchanged. Hash cost scales with payload length, so this
//! varies per-request work through the receive path as well as the hash.
//!
//! `worker_local_reuseport/*` (Linux): one `SO_REUSEPORT` listener per
//! worker, each listening and accepting from a worker-local task on its own
//! worker, and every accepted connection served worker-local in place. The
//! kernel spreads connections across the listeners, so there is no
//! dispatcher and no cross-worker hand-off: the connection is accepted,
//! registered, and served on one worker.

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

#[derive(Clone, Copy)]
enum Grain {
    Request,
    Session,
}

/// Uniform [min, max] draw, deterministic in (session, request index), so
/// every server variant sees identical work. `salt` keeps independent draws
/// (rounds, payload length) uncorrelated.
#[derive(Clone, Copy)]
struct Draw {
    min: u64,
    max: u64,
    grain: Grain,
    salt: u64,
}

impl Draw {
    fn from_env(min_var: &str, max_var: &str, fixed: usize, salt: u64) -> Self {
        let min = env_usize(min_var, fixed) as u64;
        let max = env_usize(max_var, fixed) as u64;
        assert!(min <= max, "{min_var} must be <= {max_var}");
        let grain = match std::env::var("TOKIO_BENCH_ROUNDS_GRAIN").as_deref() {
            Ok("session") => Grain::Session,
            _ => Grain::Request,
        };
        Self {
            min,
            max,
            grain,
            salt,
        }
    }

    fn fixed(&self) -> Option<usize> {
        (self.min == self.max).then_some(self.min as usize)
    }

    fn get(&self, session: u64, msg_idx: u64) -> usize {
        if let Some(fixed) = self.fixed() {
            return fixed;
        }
        let key = match self.grain {
            Grain::Request => session.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ msg_idx,
            Grain::Session => session,
        };
        (self.min + splitmix64(key ^ self.salt) % (self.max - self.min + 1)) as usize
    }
}

/// Per-request work: hash rounds and payload length.
#[derive(Clone, Copy)]
struct Workload {
    rounds: Draw,
    bytes: Draw,
}

impl Workload {
    fn from_env() -> Self {
        Self {
            rounds: Draw::from_env(
                "TOKIO_BENCH_HASH_ROUNDS_MIN",
                "TOKIO_BENCH_HASH_ROUNDS_MAX",
                hash_rounds(),
                0,
            ),
            bytes: Draw::from_env(
                "TOKIO_BENCH_REQ_BYTES_MIN",
                "TOKIO_BENCH_REQ_BYTES_MAX",
                req_bytes(),
                0x5eed_5eed_5eed_5eed,
            ),
        }
    }
}

/// splitmix64: cheap, stateless, well-distributed. Deterministic draw so
/// every run (and both server variants) sees the same workload.
fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn req_bytes() -> usize {
    env_usize("TOKIO_BENCH_REQ_BYTES", REQ_BYTES)
}

fn num_conns() -> usize {
    env_usize("TOKIO_BENCH_CONNS", NUM_CONNS)
}

/// In-flight connection count for the churn cases.
fn concurrency() -> usize {
    env_usize("TOKIO_BENCH_CONCURRENCY", NUM_CONNS)
}

fn msgs_per_conn() -> usize {
    env_usize("TOKIO_BENCH_MSGS", MSGS_PER_CONN)
}

fn rt() -> Runtime {
    let mut b = Builder::new_multi_thread();
    b.worker_threads(workers()).enable_all();
    if let Ok(v) = std::env::var("TOKIO_BENCH_GLOBAL_QUEUE_INTERVAL") {
        b.global_queue_interval(v.parse().expect("GLOBAL_QUEUE_INTERVAL: u32"));
    }
    if let Ok(v) = std::env::var("TOKIO_BENCH_EVENT_INTERVAL") {
        b.event_interval(v.parse().expect("EVENT_INTERVAL: u32"));
    }
    #[cfg(tokio_unstable)]
    if std::env::var("TOKIO_BENCH_DISABLE_LIFO").as_deref() == Ok("1") {
        b.disable_lifo_slot();
    }
    b.build().unwrap()
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
    let work = Workload::from_env();

    let start = Instant::now();
    for _ in 0..iters {
        let mut handles = Vec::with_capacity(conns);
        for session in 0..conns {
            handles.push(tokio::spawn(async move {
                send_requests(addr, session as u64, msgs, work).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }
    start.elapsed()
}

/// One client connection: `msgs` requests on `session`, each waiting for
/// its digest. Request `i` carries `work.bytes.get(session, i)` payload bytes.
async fn send_requests(addr: std::net::SocketAddr, session: u64, msgs: usize, work: Workload) {
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.set_nodelay(true).unwrap();
    let mut req = vec![0u8; 8 + work.bytes.max as usize];
    req[..8].copy_from_slice(&session.to_le_bytes());
    let mut resp = [0u8; 8];
    for i in 0..msgs {
        let len = work.bytes.get(session, i as u64);
        req[8] = (i & 0xff) as u8;
        s.write_all(&req[..8 + len]).await.unwrap();
        s.read_exact(&mut resp).await.unwrap();
    }
}

/// Reads the next request into `buf` (sized for the largest payload) and
/// returns its session and payload length, or `None` at EOF. With a fixed
/// payload length the whole request is one read, as the client sends it;
/// with variable lengths the header comes first, since the length is a
/// function of the session.
async fn read_request(
    sock: &mut TcpStream,
    buf: &mut [u8],
    work: Workload,
    msg_idx: u64,
) -> Option<(u64, usize)> {
    if let Some(len) = work.bytes.fixed() {
        sock.read_exact(&mut buf[..8 + len]).await.ok()?;
        return Some((u64::from_le_bytes(buf[..8].try_into().unwrap()), len));
    }
    sock.read_exact(&mut buf[..8]).await.ok()?;
    let session = u64::from_le_bytes(buf[..8].try_into().unwrap());
    let len = work.bytes.get(session, msg_idx);
    sock.read_exact(&mut buf[8..8 + len]).await.ok()?;
    Some((session, len))
}

/// Serve one connection: read requests, hash with the session's rolling key
/// via `roll` (which owns the variant-specific session store access), and
/// respond with the digest. Per-request work is drawn from `dist`.
async fn serve_conn<F>(mut sock: TcpStream, work: Workload, mut roll: F)
where
    F: FnMut(u64, &[u8], usize) -> u64,
{
    let mut req = vec![0u8; 8 + work.bytes.max as usize];
    let mut msg_idx = 0u64;
    loop {
        let Some((session, len)) = read_request(&mut sock, &mut req, work, msg_idx).await else {
            return;
        };
        let rounds = work.rounds.get(session, msg_idx);
        msg_idx += 1;
        let digest = roll(session, &req[8..8 + len], rounds);
        if sock.write_all(&digest.to_le_bytes()).await.is_err() {
            return;
        }
    }
}

/// Open-loop churn client: maintain `concurrency()` connections in flight,
/// each with a globally unique session id; as one completes, its slot
/// immediately opens the next. Total connections per timed round stays
/// `iters * num_conns()` so criterion scaling matches the batch cases.
/// Arrivals are staggered by construction (no thundering herd) and overlap
/// with in-flight serving (a realistic accept-while-serving server load).
async fn run_churn_clients(addr: std::net::SocketAddr, iters: u64) -> std::time::Duration {
    use std::sync::atomic::{AtomicU64, Ordering};

    let conc = concurrency();
    let msgs = msgs_per_conn();
    let work = Workload::from_env();
    let total = iters * num_conns() as u64;
    // Session ids restart at 0 each timed round, so every round (and both
    // server variants) sees the identical deterministic workload.
    let next = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut slots = tokio::task::JoinSet::new();
    for _ in 0..conc.min(total as usize) {
        let next = next.clone();
        slots.spawn(async move {
            loop {
                let session = next.fetch_add(1, Ordering::Relaxed);
                if session >= total {
                    return;
                }
                send_requests(addr, session, msgs, work).await;
            }
        });
    }
    while slots.join_next().await.is_some() {}
    start.elapsed()
}

/// Churn-case connection handler: like `serve_conn`, plus a cleanup hook
/// invoked with the session id when the connection closes, so session
/// stores see realistic insert -> use -> evict churn.
async fn serve_conn_churn<F, C>(mut sock: TcpStream, work: Workload, mut roll: F, cleanup: C)
where
    F: FnMut(u64, &[u8], usize) -> u64,
    C: FnOnce(u64),
{
    let mut req = vec![0u8; 8 + work.bytes.max as usize];
    let mut msg_idx = 0u64;
    let mut last_session = None;
    loop {
        let Some((session, len)) = read_request(&mut sock, &mut req, work, msg_idx).await else {
            break;
        };
        last_session = Some(session);
        let rounds = work.rounds.get(session, msg_idx);
        msg_idx += 1;
        let digest = roll(session, &req[8..8 + len], rounds);
        if sock.write_all(&digest.to_le_bytes()).await.is_err() {
            break;
        }
    }
    if let Some(session) = last_session {
        cleanup(session);
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
                let dist = Workload::from_env();

                let server = tokio::spawn(async move {
                    loop {
                        let (sock, _) = match listener.accept().await {
                            Ok(pair) => pair,
                            Err(_) => return,
                        };
                        sock.set_nodelay(true).unwrap();
                        let sessions = sessions.clone();
                        tokio::spawn(serve_conn(sock, dist, move |session, payload, rounds| {
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

    c.bench_function("shared_arc/request_hash_churn", |b: &mut Bencher| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let sessions = Arc::new(Mutex::new(HashMap::<u64, u64>::new()));
                let dist = Workload::from_env();

                let server = tokio::spawn(async move {
                    loop {
                        let (sock, _) = match listener.accept().await {
                            Ok(pair) => pair,
                            Err(_) => return,
                        };
                        sock.set_nodelay(true).unwrap();
                        let sessions = sessions.clone();
                        let evict = sessions.clone();
                        tokio::spawn(serve_conn_churn(
                            sock,
                            dist,
                            move |session, payload, rounds| {
                                let mut sessions = sessions.lock().unwrap();
                                let key = sessions.entry(session).or_insert(session);
                                let digest = keyed_hash(*key, payload, rounds);
                                *key = digest;
                                digest
                            },
                            move |session| {
                                evict.lock().unwrap().remove(&session);
                            },
                        ));
                    }
                });

                let elapsed = run_churn_clients(addr, iters).await;
                server.abort();
                let _ = server.await;
                elapsed
            })
        })
    });
}

/// No session store at all: sessions are 1:1 with connections under churn,
/// so the rolling key lives in the handler task and the tasks stay `Send`
/// and stealable. This is the pure-scheduling comparator for
/// `worker_local/request_hash_churn`: byte-identical hash work, zero
/// synchronization, work-stealing placement.
fn bench_stealing_local(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("stealing_local/request_hash_churn", |b: &mut Bencher| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let dist = Workload::from_env();

                let server = tokio::spawn(async move {
                    loop {
                        let (sock, _) = match listener.accept().await {
                            Ok(pair) => pair,
                            Err(_) => return,
                        };
                        sock.set_nodelay(true).unwrap();
                        let mut key: Option<u64> = None;
                        tokio::spawn(serve_conn_churn(
                            sock,
                            dist,
                            move |session, payload, rounds| {
                                let k = key.get_or_insert(session);
                                let digest = keyed_hash(*k, payload, rounds);
                                *k = digest;
                                digest
                            },
                            |_| {},
                        ));
                    }
                });

                let elapsed = run_churn_clients(addr, iters).await;
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
                let dist = Workload::from_env();

                // One dispatcher per worker: receives sockets and spawns a
                // worker-local handler per connection. Session state is a
                // per-worker Rc<RefCell<..>>: no locks, no atomics.
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
                                    dist,
                                    move |session, payload, rounds| {
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

    c.bench_function("worker_local/request_hash_churn", |b: &mut Bencher| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let n_workers = workers();
                let dist = Workload::from_env();

                let mut txs = Vec::with_capacity(n_workers);
                for w in 0..n_workers {
                    let (tx, mut rx) = mpsc::unbounded_channel::<TcpStream>();
                    txs.push(tx);
                    tokio::task::run_on_worker(w, move || {
                        tokio::task::spawn_worker_local(async move {
                            let sessions = Rc::new(RefCell::new(HashMap::<u64, u64>::new()));
                            while let Some(sock) = rx.recv().await {
                                let sessions = sessions.clone();
                                let evict = sessions.clone();
                                tokio::task::spawn_worker_local(serve_conn_churn(
                                    sock,
                                    dist,
                                    move |session, payload, rounds| {
                                        let mut sessions = sessions.borrow_mut();
                                        let key = sessions.entry(session).or_insert(session);
                                        let digest = keyed_hash(*key, payload, rounds);
                                        *key = digest;
                                        digest
                                    },
                                    move |session| {
                                        evict.borrow_mut().remove(&session);
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
                        if txs[next % txs.len()].send(sock).is_err() {
                            return;
                        }
                        next += 1;
                    }
                });

                let elapsed = run_churn_clients(addr, iters).await;
                server.abort();
                let _ = server.await;
                elapsed
            })
        })
    });
}

#[cfg(not(all(tokio_unstable, feature = "worker-local")))]
fn bench_worker_local(_c: &mut Criterion) {}

/// Starts one `SO_REUSEPORT` listener per worker, each listening and
/// accepting from a worker-local task on its own worker, serving every
/// accepted connection worker-local in place with that worker's session
/// store. Returns the shared address and a handle that stops the accept
/// loops and waits for them.
#[cfg(all(tokio_unstable, feature = "worker-local", target_os = "linux"))]
async fn start_reuseport_servers(
    work: Workload,
    churn: bool,
) -> (std::net::SocketAddr, ReuseportServers) {
    use std::cell::RefCell;
    use std::rc::Rc;
    use tokio::net::TcpSocket;
    use tokio::sync::{mpsc, watch};

    fn reuseport_socket(addr: std::net::SocketAddr) -> TcpSocket {
        let sock = TcpSocket::new_v4().unwrap();
        sock.set_reuseport(true).unwrap();
        sock.bind(addr).unwrap();
        sock
    }

    let n_workers = workers();
    let first = reuseport_socket("127.0.0.1:0".parse().unwrap());
    let addr = first.local_addr().unwrap();
    let mut sockets = vec![first];
    sockets.extend((1..n_workers).map(|_| reuseport_socket(addr)));

    let (stop_tx, stop_rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = mpsc::unbounded_channel::<()>();
    let (done_tx, done_rx) = mpsc::unbounded_channel::<()>();
    for (w, sock) in sockets.into_iter().enumerate() {
        let mut stop = stop_rx.clone();
        let ready = ready_tx.clone();
        let done = done_tx.clone();
        tokio::task::run_on_worker(w, move || {
            // `listen` registers the listener from this worker.
            let listener = sock.listen(1024).unwrap();
            let _ = ready.send(());
            tokio::task::spawn_worker_local(async move {
                let sessions = Rc::new(RefCell::new(HashMap::<u64, u64>::new()));
                loop {
                    let sock = tokio::select! {
                        res = listener.accept() => match res {
                            Ok((sock, _)) => sock,
                            Err(_) => break,
                        },
                        _ = stop.changed() => break,
                    };
                    sock.set_nodelay(true).unwrap();
                    let roll_sessions = sessions.clone();
                    let roll = move |session, payload: &[u8], rounds| {
                        let mut sessions = roll_sessions.borrow_mut();
                        let key = sessions.entry(session).or_insert(session);
                        let digest = keyed_hash(*key, payload, rounds);
                        *key = digest;
                        digest
                    };
                    if churn {
                        let evict = sessions.clone();
                        tokio::task::spawn_worker_local(serve_conn_churn(
                            sock,
                            work,
                            roll,
                            move |session| {
                                evict.borrow_mut().remove(&session);
                            },
                        ));
                    } else {
                        tokio::task::spawn_worker_local(serve_conn(sock, work, roll));
                    }
                }
                let _ = done.send(());
            });
        });
    }
    drop(ready_tx);
    // Every listener must be listening before the clients connect, or the
    // kernel only spreads connections over the ones that are.
    for _ in 0..n_workers {
        ready_rx.recv().await.unwrap();
    }
    (
        addr,
        ReuseportServers {
            stop: stop_tx,
            done: done_rx,
            n: n_workers,
        },
    )
}

#[cfg(all(tokio_unstable, feature = "worker-local", target_os = "linux"))]
struct ReuseportServers {
    stop: tokio::sync::watch::Sender<bool>,
    done: tokio::sync::mpsc::UnboundedReceiver<()>,
    n: usize,
}

#[cfg(all(tokio_unstable, feature = "worker-local", target_os = "linux"))]
impl ReuseportServers {
    async fn stop(mut self) {
        let _ = self.stop.send(true);
        for _ in 0..self.n {
            self.done.recv().await.unwrap();
        }
    }
}

/// Worker-local serving without a dispatcher: the kernel spreads
/// connections over one `SO_REUSEPORT` listener per worker.
#[cfg(all(tokio_unstable, feature = "worker-local", target_os = "linux"))]
fn bench_worker_local_reuseport(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("worker_local_reuseport/request_hash", |b: &mut Bencher| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let (addr, servers) = start_reuseport_servers(Workload::from_env(), false).await;
                let elapsed = run_clients(addr, iters).await;
                servers.stop().await;
                elapsed
            })
        })
    });

    c.bench_function(
        "worker_local_reuseport/request_hash_churn",
        |b: &mut Bencher| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let (addr, servers) = start_reuseport_servers(Workload::from_env(), true).await;
                    let elapsed = run_churn_clients(addr, iters).await;
                    servers.stop().await;
                    elapsed
                })
            })
        },
    );
}

#[cfg(not(all(tokio_unstable, feature = "worker-local", target_os = "linux")))]
fn bench_worker_local_reuseport(_c: &mut Criterion) {}

criterion_group!(
    net,
    bench_shared_arc,
    bench_stealing_local,
    bench_worker_local,
    bench_worker_local_reuseport
);
criterion_main!(net);
