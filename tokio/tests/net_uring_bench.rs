//! TCP echo throughput benchmark for the per-worker io_uring reactor.
//!
//! The same binary is compiled twice — once with `io-uring-reactor` enabled,
//! once without — and the results are compared. The benchmark itself is
//! backend-agnostic: it just exercises `TcpStream` / `TcpListener` with N
//! concurrent clients doing M round-trips of S-byte payloads.
//!
//! Run the uring backend:
//!   RUSTFLAGS='--cfg tokio_unstable' cargo test --release -p tokio \
//!     --features 'rt,rt-multi-thread,net,io-uring-reactor,time,io-util' \
//!     --test net_uring_bench -- --nocapture --test-threads=1
//!
//! Run the mio (default) backend:
//!   cargo test --release -p tokio \
//!     --features 'rt,rt-multi-thread,net,time,io-util' \
//!     --test net_uring_bench -- --nocapture --test-threads=1

#![cfg(target_os = "linux")]
#![warn(rust_2018_idioms)]

use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime;

/// Build the runtime, enabling the uring reactor if the feature is active.
fn build_rt(workers: usize) -> runtime::Runtime {
    let mut b = runtime::Builder::new_multi_thread();
    b.worker_threads(workers);
    b.enable_all();
    // DIAG (temporary): raise stickiness intervals to starve work-stealing.
    b.event_interval(1024);
    b.global_queue_interval(1024);

    #[cfg(all(tokio_unstable, feature = "io-uring-reactor"))]
    {
        b.enable_uring_reactor();
        eprintln!("# backend: io-uring-reactor");
    }

    #[cfg(not(all(tokio_unstable, feature = "io-uring-reactor")))]
    eprintln!("# backend: mio (default)");

    b.build().expect("runtime builds")
}

struct BenchParams {
    workers: usize,
    clients: usize,
    msgs_per_client: usize,
    msg_size: usize,
    label: &'static str,
}

fn run_bench(p: BenchParams) {
    let rt = build_rt(p.workers);
    rt.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let msg_size = p.msg_size;
        let msgs_per_client = p.msgs_per_client;
        let clients = p.clients;

        // Server: accept `clients` connections, each echoing
        // `msgs_per_client` messages. Each connection runs on its own task.
        let server = tokio::spawn(async move {
            let mut handles = Vec::with_capacity(clients);
            for _ in 0..clients {
                let (mut sock, _) = listener.accept().await.unwrap();
                let h = tokio::spawn(async move {
                    let mut buf = vec![0u8; msg_size];
                    for _ in 0..msgs_per_client {
                        sock.read_exact(&mut buf).await.unwrap();
                        sock.write_all(&buf).await.unwrap();
                    }
                });
                handles.push(h);
            }
            for h in handles {
                h.await.unwrap();
            }
        });

        // Brief warm-up so the runtime reaches steady state before we
        // start the timer.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let start = Instant::now();

        let mut client_handles = Vec::with_capacity(clients);
        for _ in 0..clients {
            client_handles.push(tokio::spawn(async move {
                let mut sock = TcpStream::connect(addr).await.unwrap();
                sock.set_nodelay(true).unwrap();
                let payload = vec![0xABu8; msg_size];
                let mut buf = vec![0u8; msg_size];
                for _ in 0..msgs_per_client {
                    sock.write_all(&payload).await.unwrap();
                    sock.read_exact(&mut buf).await.unwrap();
                }
            }));
        }
        for h in client_handles {
            h.await.unwrap();
        }

        let elapsed = start.elapsed();
        server.await.unwrap();

        let total_round_trips = clients * msgs_per_client;
        let rt_per_sec = total_round_trips as f64 / elapsed.as_secs_f64();
        // Each round-trip moves 2*msg_size bytes end-to-end.
        let bytes_per_sec = (total_round_trips * msg_size * 2) as f64 / elapsed.as_secs_f64();

        eprintln!(
            "{label:<32} workers={w:2} clients={c:4} msgs/c={m:5} size={s:5}B  \
             elapsed={e:>10.3?}  rt/s={rps:>10.0}  {mib:>7.1} MiB/s",
            label = p.label,
            w = p.workers,
            c = clients,
            m = msgs_per_client,
            s = msg_size,
            e = elapsed,
            rps = rt_per_sec,
            mib = bytes_per_sec / (1024.0 * 1024.0),
        );
    });
}

// --- matrix of scenarios ---
//
// Each `#[test]` is a separate scenario; running the suite with
// `--test-threads=1 --nocapture` prints a readable side-by-side.

#[test]
fn bench_small_msgs_many_clients() {
    run_bench(BenchParams {
        workers: 4,
        clients: 64,
        msgs_per_client: 2_000,
        msg_size: 64,
        label: "small_msgs_many_clients",
    });
}

#[test]
fn bench_small_msgs_few_clients() {
    run_bench(BenchParams {
        workers: 4,
        clients: 8,
        msgs_per_client: 10_000,
        msg_size: 64,
        label: "small_msgs_few_clients",
    });
}

#[test]
fn bench_large_msgs_many_clients() {
    run_bench(BenchParams {
        workers: 4,
        clients: 32,
        msgs_per_client: 500,
        msg_size: 16 * 1024,
        label: "large_msgs_many_clients",
    });
}

#[test]
fn bench_single_worker_saturation() {
    run_bench(BenchParams {
        workers: 1,
        clients: 16,
        msgs_per_client: 4_000,
        msg_size: 256,
        label: "single_worker_saturation",
    });
}

#[test]
fn bench_many_workers_low_concurrency() {
    run_bench(BenchParams {
        workers: 8,
        clients: 16,
        msgs_per_client: 4_000,
        msg_size: 256,
        label: "many_workers_low_concurrency",
    });
}
