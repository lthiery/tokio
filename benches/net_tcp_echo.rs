//! TCP echo throughput bench. Exercises the io-driver registration and
//! readiness path. Used to compare backends (traditional / uring) and to
//! check for regressions across the IoDriver vtable refactor.
//!
//! Build matrix:
//! - default features: traditional mio only
//! - `--features bench-uring-reactor` + `--cfg tokio_unstable`: adds uring
//!
//! Each backend's bench function exists only when its feature is built.
//!
//! This file is the program's cross-backend bench SOURCE OF TRUTH and is
//! deliberately self-contained (no Cargo.toml changes, no new bench
//! binaries): "bench-anchor" arms are produced by overlaying exactly this
//! file onto the target runtime commit. Keep it that way — a knob that
//! exists on one arm but not another silently benches different workloads
//! (see .claude/BENCH-PLAN-uring-global.md).

use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};

/// Sets SO_LINGER=0 so close sends RST instead of FIN, bypassing TIME_WAIT.
///
/// Why: the churn cases open tens of thousands of short-lived loopback
/// connections per rep. With normal FIN close, each 4-tuple sits in TIME_WAIT
/// for ~60s, exhausting the ephemeral port pool mid-sweep (observed: rep4/5
/// losing both arms during W=64 A/B runs). RST close skips TIME_WAIT entirely.
///
/// Trade-off: RST is semantically harsher than FIN (no graceful drain of
/// in-flight data), but this is a wake/registration microbench — we don't
/// care about graceful shutdown. Echo conns drain all rounds before close,
/// so the RST only fires after the application-level exchange is complete.
fn linger_zero(s: &TcpStream) {
    // Tokio deprecated `set_linger` over the concern that positive linger
    // values block the thread on drop. Linger=0 doesn't block — it sends
    // RST immediately — so the deprecation doesn't apply to this usage.
    #[allow(deprecated)]
    let _ = s.set_linger(Some(Duration::ZERO));
}

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

/// Server-side CPU work per message, in microseconds of calibrated
/// spin. Override via `TOKIO_BENCH_CPU_PER_MSG=N`. Default 0 (pure
/// ping-pong). Non-zero moves the bench from "workers idle, reactor
/// dominates" to "workers busy" — the regime where a single-drainer
/// reactor actually contends with task work, so pure ping-pong can't
/// flatter it.
fn cpu_per_msg_us() -> usize {
    env_usize("TOKIO_BENCH_CPU_PER_MSG", 0)
}

/// Established-but-inactive connections held open for the duration of
/// every echo iteration. Override via `TOKIO_BENCH_IDLE_CONNS=N`.
/// Default 0. Each ballast conn exchanges one setup byte (forcing real
/// reactor registration even on lazy-registration branches) and then
/// goes quiet. Tests registration-table scale and poll efficiency —
/// historically epoll-LT home turf, i.e. adversarial for uring.
fn idle_conns() -> usize {
    env_usize("TOKIO_BENCH_IDLE_CONNS", 0)
}

/// `TOKIO_BENCH_EXTRA=1` registers the extra cases (`*_rtt_p99`,
/// `*_accept_rate`, `*_register_dereg`). Off by default because the
/// bench daemon runs every case compiled into the binary per cell —
/// extras would multiply the cost of sweeps that don't want them.
fn extras_enabled() -> bool {
    std::env::var("TOKIO_BENCH_EXTRA").is_ok_and(|v| v.trim() == "1")
}

/// `TOKIO_BENCH_CT=1` swaps the binary to the current_thread case set:
/// the same workloads on `new_current_thread()` runtimes, registered
/// under `traditional_ct/` and `uring_ct/` prefixes, with the
/// multi-thread cases skipped entirely. A switch rather than an addition so a ct
/// sweep cell doesn't also pay for the full multi-thread set; both ct
/// arms live in the same binary, keeping the workload identical by
/// construction. `TOKIO_BENCH_WORKERS` is meaningless here — sweep
/// with workers=[1].
fn ct_enabled() -> bool {
    std::env::var("TOKIO_BENCH_CT").is_ok_and(|v| v.trim() == "1")
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// Calibrated spin: how many `spin_iter` rounds fit in 1µs on this
/// host. Measured once, outside any measured region; stable under the
/// performance governor the bench hosts pin. Shared by every case so
/// all backends pay identical per-message CPU.
fn spin_rounds_per_us() -> u64 {
    use std::sync::OnceLock;
    static ROUNDS: OnceLock<u64> = OnceLock::new();
    *ROUNDS.get_or_init(|| {
        spin_iter(10_000); // warm-up
        let start = Instant::now();
        spin_iter(1_000_000);
        let ns_per_round = start.elapsed().as_nanos() as u64 / 1_000_000;
        (1_000 / ns_per_round.max(1)).max(1)
    })
}

#[inline(never)]
fn spin_iter(rounds: u64) {
    let mut acc = 0u64;
    for i in 0..rounds {
        acc = std::hint::black_box(acc.wrapping_add(i).rotate_left(7));
    }
    std::hint::black_box(acc);
}

/// Burn `us` microseconds of CPU on the current thread.
fn spin_us(us: usize) {
    if us > 0 {
        spin_iter(spin_rounds_per_us() * us as u64);
    }
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

fn rt_traditional_ct() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

/// current_thread + uring: forced global-ring mode with one worker slot
/// (no env knob involved; `TOKIO_URING_GLOBAL` is multi-thread-only).
#[cfg(all(tokio_unstable, feature = "bench-uring-reactor", target_os = "linux"))]
fn rt_uring_ct() -> Runtime {
    let mut b = Builder::new_current_thread();
    b.enable_all();
    b.enable_uring_reactor();
    b.build().unwrap()
}

/// First-byte marker distinguishing ballast conns from echo conns at
/// the server. Echo clients never send it as byte 0 (they remap).
const BALLAST_MARKER: u8 = 0xB5;

/// Ballast: `idle_conns()` established connections that exchange one
/// setup byte and then sit registered-but-silent until dropped. The
/// server halves park in tasks awaiting a read that only completes at
/// teardown (client drop → RST). Returns the client halves; keep them
/// alive for the measured region.
async fn spawn_idle_ballast(addr: std::net::SocketAddr) -> Vec<TcpStream> {
    let n = idle_conns();
    let mut ballast = Vec::with_capacity(n);
    for _ in 0..n {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.set_nodelay(true).unwrap();
        linger_zero(&s);
        // One byte each way forces reactor registration + read interest
        // on both halves, on eager AND lazy registration branches.
        s.write_all(&[BALLAST_MARKER]).await.unwrap();
        let mut one = [0u8; 1];
        s.read_exact(&mut one).await.unwrap();
        ballast.push(s);
    }
    ballast
}

/// Echo server shared by the throughput and percentile cases. Echoes
/// `msgs` messages of `bytes` per accepted conn, burning `cpu_us` of
/// spin per message. Conns opening with `BALLAST_MARKER` get the byte
/// echoed, then park on a read that resolves only at client drop.
fn spawn_echo_server(
    listener: TcpListener,
    msgs: usize,
    bytes: usize,
    cpu_us: usize,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => return,
            };
            linger_zero(&sock);
            tokio::spawn(async move {
                let mut first = [0u8; 1];
                if sock.read_exact(&mut first).await.is_err() {
                    return;
                }
                if first[0] == BALLAST_MARKER {
                    if sock.write_all(&first).await.is_err() {
                        return;
                    }
                    let mut park = [0u8; 1];
                    let _ = sock.read_exact(&mut park).await;
                    return;
                }
                // Echo conn: `first` already holds byte 0 of message 0.
                let mut buf = vec![0u8; bytes];
                buf[0] = first[0];
                if bytes > 1 && sock.read_exact(&mut buf[1..]).await.is_err() {
                    return;
                }
                for i in 0..msgs {
                    spin_us(cpu_us);
                    if sock.write_all(&buf).await.is_err() {
                        return;
                    }
                    if i + 1 == msgs {
                        return;
                    }
                    if sock.read_exact(&mut buf).await.is_err() {
                        return;
                    }
                }
            });
        }
    })
}

/// Byte 0 of echo message `i`: the message counter, remapped off the
/// ballast marker so the server's first-byte sniff can't misroute.
fn msg_byte0(i: usize) -> u8 {
    let b = (i & 0xff) as u8;
    if b == BALLAST_MARKER {
        BALLAST_MARKER.wrapping_add(1)
    } else {
        b
    }
}

fn run_tcp_echo(rt: &Runtime, b: &mut Bencher) {
    let num_conns = conns();
    let msgs = msgs_per_conn();
    let bytes = msg_bytes();
    let cpu_us = cpu_per_msg_us();
    spin_rounds_per_us(); // calibrate outside the measured region
    b.iter_custom(|iters| {
        rt.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = spawn_echo_server(listener, msgs, bytes, cpu_us);
            let ballast = spawn_idle_ballast(addr).await;

            let start = Instant::now();
            for _ in 0..iters {
                let mut handles = Vec::with_capacity(num_conns);
                for _ in 0..num_conns {
                    handles.push(tokio::spawn(async move {
                        let mut s = TcpStream::connect(addr).await.unwrap();
                        s.set_nodelay(true).unwrap();
                        linger_zero(&s);
                        let mut buf = vec![0u8; bytes];
                        for i in 0..msgs {
                            buf[0] = msg_byte0(i);
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
            drop(ballast);
            server.abort();
            let _ = server.await;
            elapsed
        })
    });
}

/// p99 round-trip latency under the same offered load as
/// `tcp_echo_throughput`. One sampler connection times every
/// individual round-trip while `conns()-1` background connections run
/// the normal workload. Reported value = p99 RTT × msgs-per-conn
/// (scaled so criterion's divide-by-iters yields a stable,
/// cross-arm-comparable number; it is NOT a wall time). Median-only
/// reporting is how a 30%-span cell looks "fine" — this case exists
/// to catch tail-for-median trades.
fn run_tcp_echo_rtt_p99(rt: &Runtime, b: &mut Bencher) {
    let num_conns = conns().max(1);
    let msgs = msgs_per_conn();
    let bytes = msg_bytes();
    let cpu_us = cpu_per_msg_us();
    spin_rounds_per_us();
    b.iter_custom(|iters| {
        rt.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = spawn_echo_server(listener, msgs, bytes, cpu_us);
            let ballast = spawn_idle_ballast(addr).await;

            let mut samples: Vec<u64> = Vec::with_capacity(iters as usize * msgs);
            for _ in 0..iters {
                let mut handles = Vec::with_capacity(num_conns - 1);
                for _ in 0..num_conns - 1 {
                    handles.push(tokio::spawn(async move {
                        let mut s = TcpStream::connect(addr).await.unwrap();
                        s.set_nodelay(true).unwrap();
                        linger_zero(&s);
                        let mut buf = vec![0u8; bytes];
                        for i in 0..msgs {
                            buf[0] = msg_byte0(i);
                            s.write_all(&buf).await.unwrap();
                            s.read_exact(&mut buf).await.unwrap();
                        }
                    }));
                }
                // Sampler: same workload, individually timed round-trips.
                let mut s = TcpStream::connect(addr).await.unwrap();
                s.set_nodelay(true).unwrap();
                linger_zero(&s);
                let mut buf = vec![0u8; bytes];
                for i in 0..msgs {
                    buf[0] = msg_byte0(i);
                    let t = Instant::now();
                    s.write_all(&buf).await.unwrap();
                    s.read_exact(&mut buf).await.unwrap();
                    samples.push(t.elapsed().as_nanos() as u64);
                }
                drop(s);
                for h in handles {
                    h.await.unwrap();
                }
            }
            drop(ballast);
            server.abort();
            let _ = server.await;

            samples.sort_unstable();
            let idx = (samples.len().saturating_sub(1)) * 99 / 100;
            let p99_ns = samples.get(idx).copied().unwrap_or(0);
            Duration::from_nanos(p99_ns * msgs as u64 * iters)
        })
    });
}

fn run_tcp_connect_churn(rt: &Runtime, b: &mut Bencher) {
    let num_conns = conns();
    b.iter_custom(|iters| {
        rt.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let server = tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((sock, _)) => {
                            linger_zero(&sock);
                            drop(sock);
                        }
                        Err(_) => return,
                    }
                }
            });

            let start = Instant::now();
            for _ in 0..iters {
                let mut handles = Vec::with_capacity(num_conns);
                for _ in 0..num_conns {
                    handles.push(tokio::spawn(async move {
                        if let Ok(s) = TcpStream::connect(addr).await {
                            linger_zero(&s);
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

/// Churn decomposition (i): accept()→drop rate, connects excluded.
/// The kernel completes handshakes into the listen backlog without
/// accept(), so all connects finish BEFORE the clock starts; the
/// measured region is purely the accept loop: listener readiness +
/// accept + register + drop/dereg per conn. Fresh listener per
/// iteration so backlog state can't carry over.
fn run_accept_rate(rt: &Runtime, b: &mut Bencher) {
    // Stay under the listen backlog so pre-connects can't stall.
    let num_conns = conns().min(512);
    b.iter_custom(|iters| {
        rt.block_on(async {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();

                let mut pre = Vec::with_capacity(num_conns);
                for _ in 0..num_conns {
                    let s = TcpStream::connect(addr).await.unwrap();
                    linger_zero(&s);
                    pre.push(s);
                }

                let start = Instant::now();
                for _ in 0..num_conns {
                    let (sock, _) = listener.accept().await.unwrap();
                    linger_zero(&sock);
                    drop(sock);
                }
                total += start.elapsed();
                drop(pre);
            }
            total
        })
    });
}

/// Churn decomposition (ii): pure register/deregister cycling. One
/// established connection; each round dups the fd and wraps it in a
/// fresh tokio `TcpStream` (registers with the driver), then drops it
/// (deregisters). No connects, no accepts, no data — isolates the
/// driver's registration-table cost, i.e. the vtable
/// `register_local`/`deregister` path by itself.
fn run_register_dereg(rt: &Runtime, b: &mut Bencher) {
    let cycles = conns().max(32);
    b.iter_custom(|iters| {
        rt.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let accept = tokio::spawn(async move { listener.accept().await });
            let conn = TcpStream::connect(addr).await.unwrap();
            let _server_half = accept.await.unwrap().unwrap();
            let std_conn = conn.into_std().unwrap();

            let start = Instant::now();
            for _ in 0..iters {
                for _ in 0..cycles {
                    let dup = std_conn.try_clone().unwrap();
                    dup.set_nonblocking(true).unwrap();
                    let registered = TcpStream::from_std(dup).unwrap();
                    // Lazy-registration branches don't touch the driver
                    // until first poll — force it, or this case measures
                    // only allocation. WouldBlock expected (peer silent).
                    let _ = registered.try_read(&mut [0u8; 1]);
                    drop(registered);
                }
            }
            start.elapsed()
        })
    });
}

/// Register the standard pair of cases plus (behind
/// `TOKIO_BENCH_EXTRA=1`) the decomposition/percentile cases for one
/// backend prefix.
fn bench_backend(c: &mut Criterion, prefix: &str, rt: &Runtime) {
    c.bench_function(&format!("{prefix}/tcp_echo_throughput"), |b| {
        run_tcp_echo(rt, b)
    });
    c.bench_function(&format!("{prefix}/tcp_connect_churn"), |b| {
        run_tcp_connect_churn(rt, b)
    });
    if extras_enabled() {
        c.bench_function(&format!("{prefix}/tcp_echo_rtt_p99"), |b| {
            run_tcp_echo_rtt_p99(rt, b)
        });
        c.bench_function(&format!("{prefix}/tcp_accept_rate"), |b| {
            run_accept_rate(rt, b)
        });
        c.bench_function(&format!("{prefix}/tcp_register_dereg"), |b| {
            run_register_dereg(rt, b)
        });
    }
}

fn bench_traditional(c: &mut Criterion) {
    if ct_enabled() {
        let rt = rt_traditional_ct();
        bench_backend(c, "traditional_ct", &rt);
    } else {
        let rt = rt_traditional();
        bench_backend(c, "traditional", &rt);
    }
}

#[cfg(all(tokio_unstable, feature = "bench-uring-reactor", target_os = "linux"))]
fn bench_uring(c: &mut Criterion) {
    if ct_enabled() {
        let rt = rt_uring_ct();
        bench_backend(c, "uring_ct", &rt);
    } else {
        let rt = rt_uring();
        bench_backend(c, "uring", &rt);
    }
}

#[cfg(not(all(tokio_unstable, feature = "bench-uring-reactor", target_os = "linux")))]
fn bench_uring(_c: &mut Criterion) {}

criterion_group!(net, bench_traditional, bench_uring);
criterion_main!(net);
