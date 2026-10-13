//! EXPERIMENT (not for upstream): TCP echo round trips on a multi-thread
//! server runtime, with the server's worker count, I/O shard count, and CPU
//! sets taken from the environment, to compare socket placement policies.
//!
//! Server: `TOKIO_BENCH_WORKERS` workers (default 8), `TOKIO_BENCH_IO_SHARDS`
//! shards (default 1), threads pinned to `TOKIO_BENCH_SERVER_CPUS` (e.g.
//! `0-7`); with `TOKIO_BENCH_PIN_PER_WORKER=1` the k-th worker thread gets
//! the k-th slice of that set, so shard groups line up with cache groups.
//! Client: a multi-thread runtime with `TOKIO_BENCH_CLIENT_WORKERS` workers
//! (default 4) pinned to `TOKIO_BENCH_CLIENT_CPUS`, one task per connection;
//! `TOKIO_BENCH_CONNS` connections (default 64), each doing
//! `TOKIO_BENCH_ROUNDS` 64-byte round trips per iteration (default 100).
//! `TOKIO_BENCH_CONNECT_PAR` connections are opened at a time (default 1, so
//! the server sees a trickle of accepts; set it to the connection count for
//! one burst).
//!
//! - `echo/steady`: connections are made once; an iteration is all of them
//!   doing their rounds concurrently.
//! - `echo/fresh`: an iteration connects, does 8 rounds per connection, and
//!   drops the connections, so registration is inside the timed section.
//!
//! Placement policy comes from the runtime's env knobs (`TOKIO_EAGER_TCP`,
//! `TOKIO_IO_SHARD_LOCAL`); the per-case `io_shard_poll_stats` line shows
//! how many readiness polls ran on a worker of the source's shard group.

#![cfg(unix)]

use std::net::SocketAddr;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{self, Runtime};

const PAYLOAD: usize = 64;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// "0-7,16" -> cpu list; empty when unset.
fn env_cpus(name: &str) -> Vec<usize> {
    let Ok(v) = std::env::var(name) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for part in v.split(',').filter(|p| !p.is_empty()) {
        match part.split_once('-') {
            Some((a, b)) => {
                let (a, b): (usize, usize) = (a.parse().unwrap(), b.parse().unwrap());
                out.extend(a..=b);
            }
            None => out.push(part.parse().unwrap()),
        }
    }
    out
}

fn pin_to(cpus: &[usize]) {
    if cpus.is_empty() {
        return;
    }
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for &c in cpus {
            libc::CPU_SET(c, &mut set);
        }
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

/// Pins runtime threads to `cpus`: all of them to the whole set, or with
/// `per_worker`, the k-th started thread to the k-th of `workers` slices.
fn pinner(cpus: Vec<usize>, workers: usize, per_worker: bool) -> impl Fn() + Send + Sync {
    let next = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    move || {
        if cpus.is_empty() {
            return;
        }
        if per_worker {
            let k = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % workers;
            let lo = k * cpus.len() / workers;
            let hi = ((k + 1) * cpus.len() / workers).max(lo + 1).min(cpus.len());
            pin_to(&cpus[lo..hi]);
        } else {
            pin_to(&cpus);
        }
    }
}

fn server_rt() -> Runtime {
    let workers = env_usize("TOKIO_BENCH_WORKERS", 8);
    let per_worker = env_usize("TOKIO_BENCH_PIN_PER_WORKER", 0) == 1;
    let mut b = runtime::Builder::new_multi_thread();
    b.worker_threads(workers)
        .io_shards(env_usize("TOKIO_BENCH_IO_SHARDS", 1))
        .enable_all()
        .on_thread_start(pinner(
            env_cpus("TOKIO_BENCH_SERVER_CPUS"),
            workers,
            per_worker,
        ));
    b.build().unwrap()
}

async fn serve(mut s: TcpStream) {
    let mut buf = [0u8; PAYLOAD];
    loop {
        match s.read_exact(&mut buf).await {
            Ok(_) => {}
            Err(_) => return,
        }
        if s.write_all(&buf).await.is_err() {
            return;
        }
    }
}

/// Starts the echo server on `rt` and returns its address.
fn start_server(rt: &Runtime) -> SocketAddr {
    let listener = rt.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
    let addr = listener.local_addr().unwrap();
    rt.spawn(async move {
        loop {
            let (s, _) = listener.accept().await.unwrap();
            tokio::spawn(serve(s));
        }
    });
    addr
}

/// A client runtime driven from its own thread; `run` executes a job on it
/// and returns the wall time the job reports.
struct Client {
    tx: mpsc::Sender<Box<dyn FnOnce(&Runtime) -> Duration + Send>>,
    rx: mpsc::Receiver<Duration>,
}

impl Client {
    fn new() -> Client {
        let (tx, jobs) = mpsc::channel::<Box<dyn FnOnce(&Runtime) -> Duration + Send>>();
        let (done, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let cpus = env_cpus("TOKIO_BENCH_CLIENT_CPUS");
            pin_to(&cpus);
            let workers = env_usize("TOKIO_BENCH_CLIENT_WORKERS", 4);
            let rt = runtime::Builder::new_multi_thread()
                .worker_threads(workers)
                .enable_all()
                .on_thread_start(pinner(cpus, workers, false))
                .build()
                .unwrap();
            for job in jobs {
                let _ = done.send(job(&rt));
            }
        });
        Client { tx, rx }
    }

    fn run(&self, job: impl FnOnce(&Runtime) -> Duration + Send + 'static) -> Duration {
        self.tx.send(Box::new(job)).unwrap();
        self.rx.recv().unwrap()
    }
}

async fn rounds(mut s: TcpStream, n: usize) -> TcpStream {
    let out = [7u8; PAYLOAD];
    let mut buf = [0u8; PAYLOAD];
    for _ in 0..n {
        s.write_all(&out).await.unwrap();
        s.read_exact(&mut buf).await.unwrap();
    }
    s
}

/// One task per connection; returns the connections in order.
async fn all_rounds(streams: Vec<TcpStream>, n: usize) -> Vec<TcpStream> {
    let handles: Vec<_> = streams
        .into_iter()
        .map(|s| tokio::spawn(rounds(s, n)))
        .collect();
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(h.await.unwrap());
    }
    out
}

/// Connects `n` streams, `TOKIO_BENCH_CONNECT_PAR` at a time (default 1: a
/// trickle of accepts on the server; `n`: one burst).
async fn connect_all(addr: SocketAddr, n: usize) -> Vec<TcpStream> {
    let par = env_usize("TOKIO_BENCH_CONNECT_PAR", 1).max(1);
    let mut v = Vec::with_capacity(n);
    while v.len() < n {
        let batch = par.min(n - v.len());
        let handles: Vec<_> = (0..batch)
            .map(|_| {
                tokio::spawn(async move {
                    let s = TcpStream::connect(addr).await.unwrap();
                    s.set_nodelay(true).unwrap();
                    s
                })
            })
            .collect();
        for h in handles {
            v.push(h.await.unwrap());
        }
    }
    v
}

fn report(name: &str, rt: &Runtime) {
    let [same, other, off] = rt.metrics().io_shard_poll_stats();
    let total = (same + other + off).max(1);
    eprintln!(
        "  {name}: io_shard_poll_stats same={same} other={other} off_worker={off} \
         (other/total {:.1}%) registered_per_shard={:?}",
        100.0 * other as f64 / total as f64,
        rt.metrics().io_shard_registered(),
    );
}

fn echo_steady(c: &mut Criterion) {
    let conns = env_usize("TOKIO_BENCH_CONNS", 64);
    let rounds_n = env_usize("TOKIO_BENCH_ROUNDS", 100);
    let rt = server_rt();
    let addr = start_server(&rt);
    let client = Client::new();

    // Connect once, keep the streams on the client thread.
    let streams = std::sync::Arc::new(std::sync::Mutex::new(Some(Vec::new())));
    {
        let streams = streams.clone();
        client.run(move |rt| {
            let v = rt.block_on(connect_all(addr, conns));
            *streams.lock().unwrap() = Some(v);
            Duration::ZERO
        });
    }

    let mut g = c.benchmark_group("echo");
    g.throughput(Throughput::Elements((conns * rounds_n) as u64));
    g.bench_function("steady", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let streams = streams.clone();
                total += client.run(move |rt| {
                    let v = streams.lock().unwrap().take().unwrap();
                    let start = Instant::now();
                    let v = rt.block_on(all_rounds(v, rounds_n));
                    let el = start.elapsed();
                    *streams.lock().unwrap() = Some(v);
                    el
                });
            }
            total
        })
    });
    g.finish();
    report("echo/steady", &rt);
}

fn echo_fresh(c: &mut Criterion) {
    let conns = env_usize("TOKIO_BENCH_CONNS", 64);
    let rt = server_rt();
    let addr = start_server(&rt);
    let client = Client::new();

    let mut g = c.benchmark_group("echo");
    g.throughput(Throughput::Elements((conns * 8) as u64));
    g.bench_function("fresh", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += client.run(move |rt| {
                    let start = Instant::now();
                    rt.block_on(async {
                        let v = connect_all(addr, conns).await;
                        drop(all_rounds(v, 8).await);
                    });
                    start.elapsed()
                });
            }
            total
        })
    });
    g.finish();
    report("echo/fresh", &rt);
}

criterion_group!(net_shard_echo, echo_steady, echo_fresh);
criterion_main!(net_shard_echo);
