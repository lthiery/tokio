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

// ======================================================================
// Owned-buffer variant: exercises `TcpStream::uring_send` /
// `uring_recv`, which bypass `PollEvented` entirely and hand buffers
// directly to the io_uring reactor. Only compiled when the uring
// reactor feature is active — the mio-only build has no such methods.
//
// Side-by-side interpretation when running --test-threads=1 --nocapture:
//
//   mio run                      → 5 `bench_*` scenarios (readiness/mio)
//   uring readiness run          → 5 `bench_*` scenarios (readiness/uring)
//   uring owned-buffer run       → same 5 scenarios as `uring_owned_*`
//
// Same workloads, same binary, different submission path. Any
// `uring_owned_X` vs `bench_X` delta on the uring build is the
// effect of owning the buffer inside the reactor slab versus going
// through `PollEvented` + a ready syscall.
// ======================================================================

#[cfg(all(tokio_unstable, feature = "io-uring-reactor"))]
mod uring_owned {
    use super::{build_rt, BenchParams};
    use bytes::{Bytes, BytesMut};
    use std::time::{Duration, Instant};
    use tokio::net::{TcpListener, TcpStream};

    /// Send `payload` in full, looping across partial sends. The
    /// buffer is returned on every completion (success or partial) so
    /// we re-submit the unsent tail without re-allocating — mirroring
    /// the readiness path's `write_all` which loops internally on one
    /// buffer.
    async fn send_all(sock: &TcpStream, mut remaining: Bytes) {
        while !remaining.is_empty() {
            let (r, returned) = sock.uring_send(remaining).await;
            let n = r.expect("uring_send ok");
            assert!(n > 0, "uring_send returned 0 — peer closed");
            remaining = returned.slice(n..);
        }
    }

    /// Receive exactly `want` bytes, looping across short reads.
    /// Reuses a single caller-owned `BytesMut` across every
    /// iteration — allocator-apples-to-apples with the readiness
    /// path, which reads into a single `vec![0u8; msg_size]` per
    /// connection. `recv_buf` is passed in by `&mut Option<_>` so
    /// the caller keeps ownership across calls; we `take` it,
    /// hand it to the kernel, and restore it from the completion.
    async fn recv_exact(
        sock: &TcpStream,
        recv_buf: &mut Option<BytesMut>,
        mut want: usize,
    ) {
        while want > 0 {
            let buf = recv_buf.take().expect("recv_buf vacant — prior call panicked");
            let (r, returned) = sock.uring_recv(buf).await;
            let n = r.expect("uring_recv ok");
            *recv_buf = Some(returned);
            assert!(n > 0, "uring_recv returned 0 — peer closed mid-read");
            // Note: the kernel may have written up to `buf.len()`
            // bytes, which could exceed `want` if we ever sized the
            // buffer larger than the remaining window. We never do
            // — `buf.len() == msg_size` throughout — and the echo
            // peer only sends exactly msg_size bytes per iteration,
            // so in this benchmark `n <= want` always.
            want = want.saturating_sub(n);
        }
    }

    fn run_bench_owned(p: BenchParams) {
        let rt = build_rt(p.workers);
        rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let msg_size = p.msg_size;
            let msgs_per_client = p.msgs_per_client;
            let clients = p.clients;

            // Server: accept `clients` connections, echo `msgs_per_client`
            // messages each via owned-buffer send/recv.
            let server = tokio::spawn(async move {
                // A single reusable echo template — every iteration
                // `clone()`s it (cheap: Arc refcount bump only).
                let echo_template = Bytes::from(vec![0xABu8; msg_size]);
                let mut handles = Vec::with_capacity(clients);
                for _ in 0..clients {
                    let (sock, _) = listener.accept().await.unwrap();
                    let echo = echo_template.clone();
                    let h = tokio::spawn(async move {
                        // One recv buffer per connection, reused for
                        // all msgs_per_client iterations.
                        let mut recv_buf = Some(BytesMut::zeroed(msg_size));
                        for _ in 0..msgs_per_client {
                            recv_exact(&sock, &mut recv_buf, msg_size).await;
                            send_all(&sock, echo.clone()).await;
                        }
                    });
                    handles.push(h);
                }
                for h in handles {
                    h.await.unwrap();
                }
            });

            // Warm-up so the runtime reaches steady state before we time.
            tokio::time::sleep(Duration::from_millis(20)).await;

            let start = Instant::now();

            let mut client_handles = Vec::with_capacity(clients);
            for _ in 0..clients {
                client_handles.push(tokio::spawn(async move {
                    let sock = TcpStream::connect(addr).await.unwrap();
                    sock.set_nodelay(true).unwrap();
                    // Both buffers allocated once per connection and
                    // reused for every iteration — matching the
                    // readiness-path bench's pre-allocated vec.
                    let payload_template = Bytes::from(vec![0xABu8; msg_size]);
                    let mut recv_buf = Some(BytesMut::zeroed(msg_size));
                    for _ in 0..msgs_per_client {
                        send_all(&sock, payload_template.clone()).await;
                        recv_exact(&sock, &mut recv_buf, msg_size).await;
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
            let bytes_per_sec =
                (total_round_trips * msg_size * 2) as f64 / elapsed.as_secs_f64();

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

    #[test]
    fn uring_owned_small_msgs_many_clients() {
        run_bench_owned(BenchParams {
            workers: 4,
            clients: 64,
            msgs_per_client: 2_000,
            msg_size: 64,
            label: "uring_owned_small_msgs_many_clients",
        });
    }

    #[test]
    fn uring_owned_small_msgs_few_clients() {
        run_bench_owned(BenchParams {
            workers: 4,
            clients: 8,
            msgs_per_client: 10_000,
            msg_size: 64,
            label: "uring_owned_small_msgs_few_clients",
        });
    }

    #[test]
    fn uring_owned_large_msgs_many_clients() {
        run_bench_owned(BenchParams {
            workers: 4,
            clients: 32,
            msgs_per_client: 500,
            msg_size: 16 * 1024,
            label: "uring_owned_large_msgs_many_clients",
        });
    }

    #[test]
    fn uring_owned_single_worker_saturation() {
        run_bench_owned(BenchParams {
            workers: 1,
            clients: 16,
            msgs_per_client: 4_000,
            msg_size: 256,
            label: "uring_owned_single_worker_saturation",
        });
    }

    #[test]
    fn uring_owned_many_workers_low_concurrency() {
        run_bench_owned(BenchParams {
            workers: 8,
            clients: 16,
            msgs_per_client: 4_000,
            msg_size: 256,
            label: "uring_owned_many_workers_low_concurrency",
        });
    }
}

// ======================================================================
// Multishot-recv + provided-buffer-ring variant (POC).
//
// Same echo workload, but the recv side arms a single `uring_recv_multi`
// per connection — one SQE for the whole connection's lifetime, with
// each incoming message drawn from the reactor's registered buffer
// pool. Sends still go through `uring_send` (owned-buffer path); we
// only vary the recv side.
//
// The interesting comparison points:
//
//   uring_owned_*  : single-shot owned recv (1 SQE + 1 slab entry / msg)
//   uring_multi_*  : multishot recv         (1 SQE + 1 slab entry / conn)
//
// If the owned-buffer path is SQE/slab-bound (hypothesis from prior
// benchmarking), `uring_multi_*` should recover the lost throughput.
// ======================================================================

#[cfg(all(tokio_unstable, feature = "io-uring-reactor"))]
mod uring_multi {
    use super::{build_rt, BenchParams};
    use bytes::Bytes;
    use std::time::{Duration, Instant};
    use tokio::net::{TcpListener, TcpStream};

    /// Send `payload` in full. Same implementation as the uring_owned
    /// path — this benchmark only varies the recv side.
    async fn send_all(sock: &TcpStream, mut remaining: Bytes) {
        while !remaining.is_empty() {
            let (r, returned) = sock.uring_send(remaining).await;
            let n = r.expect("uring_send ok");
            assert!(n > 0, "uring_send returned 0 — peer closed");
            remaining = returned.slice(n..);
        }
    }

    // Helpers intentionally inlined: `UringRecvMulti` is only
    // reachable via `impl TcpStream` return types through
    // `pub(crate) mod io`, so naming it from an external test module
    // would require a wider re-export than the POC warrants.
    // Instead each consumer writes a `while want > 0` loop that
    // calls `stream.next().await` and drops each lease promptly.

    fn run_bench_multi(p: BenchParams) {
        let rt = build_rt(p.workers);
        rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let msg_size = p.msg_size;
            let msgs_per_client = p.msgs_per_client;
            let clients = p.clients;

            // Server: one task per connection. Arms `uring_recv_multi`
            // at connection start; loops `msgs_per_client` times
            // receiving `msg_size` bytes then echoing back.
            let server = tokio::spawn(async move {
                let echo_template = Bytes::from(vec![0xABu8; msg_size]);
                let mut handles = Vec::with_capacity(clients);
                for _ in 0..clients {
                    let (sock, _) = listener.accept().await.unwrap();
                    let echo = echo_template.clone();
                    let h = tokio::spawn(async move {
                        let mut stream = sock.uring_recv_multi();
                        for _ in 0..msgs_per_client {
                            let mut want = msg_size;
                            while want > 0 {
                                match stream.next().await {
                                    Some(Ok(lease)) => {
                                        let n = lease.len() as usize;
                                        assert!(n > 0);
                                        want = want.saturating_sub(n);
                                    }
                                    Some(Err(e)) => panic!("server recv_multi: {e}"),
                                    None => panic!("server recv_multi ended early"),
                                }
                            }
                            send_all(&sock, echo.clone()).await;
                        }
                    });
                    handles.push(h);
                }
                for h in handles {
                    h.await.unwrap();
                }
            });

            tokio::time::sleep(Duration::from_millis(20)).await;

            let start = Instant::now();

            let mut client_handles = Vec::with_capacity(clients);
            for _ in 0..clients {
                client_handles.push(tokio::spawn(async move {
                    let sock = TcpStream::connect(addr).await.unwrap();
                    sock.set_nodelay(true).unwrap();
                    let mut stream = sock.uring_recv_multi();
                    let payload_template = Bytes::from(vec![0xABu8; msg_size]);
                    for _ in 0..msgs_per_client {
                        send_all(&sock, payload_template.clone()).await;
                        let mut want = msg_size;
                        while want > 0 {
                            match stream.next().await {
                                Some(Ok(lease)) => {
                                    let n = lease.len() as usize;
                                    assert!(n > 0);
                                    want = want.saturating_sub(n);
                                }
                                Some(Err(e)) => panic!("client recv_multi: {e}"),
                                None => panic!("client recv_multi ended early"),
                            }
                        }
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
            let bytes_per_sec =
                (total_round_trips * msg_size * 2) as f64 / elapsed.as_secs_f64();

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

    #[test]
    fn uring_multi_small_msgs_many_clients() {
        run_bench_multi(BenchParams {
            workers: 4,
            clients: 64,
            msgs_per_client: 2_000,
            msg_size: 64,
            label: "uring_multi_small_msgs_many_clients",
        });
    }

    #[test]
    fn uring_multi_small_msgs_few_clients() {
        run_bench_multi(BenchParams {
            workers: 4,
            clients: 8,
            msgs_per_client: 10_000,
            msg_size: 64,
            label: "uring_multi_small_msgs_few_clients",
        });
    }

    #[test]
    fn uring_multi_large_msgs_many_clients() {
        run_bench_multi(BenchParams {
            workers: 4,
            clients: 32,
            msgs_per_client: 500,
            msg_size: 16 * 1024,
            label: "uring_multi_large_msgs_many_clients",
        });
    }

    #[test]
    fn uring_multi_single_worker_saturation() {
        run_bench_multi(BenchParams {
            workers: 1,
            clients: 16,
            msgs_per_client: 4_000,
            msg_size: 256,
            label: "uring_multi_single_worker_saturation",
        });
    }

    #[test]
    fn uring_multi_many_workers_low_concurrency() {
        run_bench_multi(BenchParams {
            workers: 8,
            clients: 16,
            msgs_per_client: 4_000,
            msg_size: 256,
            label: "uring_multi_many_workers_low_concurrency",
        });
    }
}
