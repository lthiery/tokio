//! current_thread / `LocalRuntime` under the uring reactor.
//!
//! `enable_uring_reactor()` on `new_current_thread()` builds the
//! forced-global single-ring shape: one shared relaxed ring (no
//! `SINGLE_ISSUER`/`DEFER_TASKRUN`), driven by whichever thread holds
//! the scheduler core (`GlobalRing` with one worker slot). The design
//! doc is `DESIGN-uring-local-runtime.md` (in the main tree's
//! `.claude/`).
//!
//! The claim this suite leans on hardest: the scheduler core — and with
//! it the ring-driving duty — migrates across `block_on` threads, and
//! the relaxed ring plus worker-indexed (never thread-identified) park
//! state make that migration invariant. `core_migrates_*` and
//! `shutdown_on_other_thread` are the regression tests for it.
//!
//! Wedge-class failures here are HANGS, not asserts, so the timer/wake
//! tests arm a watchdog that aborts loudly.

#![cfg(all(
    tokio_unstable,
    feature = "io-uring-reactor",
    feature = "rt-multi-thread",
    feature = "net",
    target_os = "linux",
))]
#![warn(rust_2018_idioms)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime;

fn build_rt() -> runtime::Runtime {
    runtime::Builder::new_current_thread()
        .enable_uring_reactor()
        .build()
        .expect("current_thread uring runtime builds")
}

fn build_local() -> runtime::LocalRuntime {
    runtime::Builder::new_current_thread()
        .enable_uring_reactor()
        .build_local(Default::default())
        .expect("LocalRuntime uring runtime builds")
}

/// Aborts the process if the guard is still armed after `timeout`; the
/// failure modes under test (lost wakes, deaf ring after a core
/// migration) present as hangs a plain assert can never catch.
struct HangWatchdog {
    done: Arc<AtomicBool>,
}

impl HangWatchdog {
    fn arm(name: &'static str, timeout: Duration) -> Self {
        let done = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&done);
        std::thread::spawn(move || {
            let deadline = Instant::now() + timeout;
            while Instant::now() < deadline {
                if flag.load(Ordering::Acquire) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            if !flag.load(Ordering::Acquire) {
                eprintln!("{name}: runtime wedged, aborting");
                std::process::abort();
            }
        });
        Self { done }
    }
}

impl Drop for HangWatchdog {
    fn drop(&mut self) {
        self.done.store(true, Ordering::Release);
    }
}

#[test]
fn spawn_and_join() {
    let rt = build_rt();
    let n = rt.block_on(async { tokio::spawn(async { 42 }).await.unwrap() });
    assert_eq!(n, 42);
}

/// End-to-end fd registration on the shared ring: connect/accept/read/
/// write all inside one `block_on`, driven entirely by the core-holder
/// park path (`GlobalRing::park_worker` slot 0).
#[test]
fn tcp_round_trip() {
    let _watchdog = HangWatchdog::arm("tcp_round_trip", Duration::from_secs(30));
    let rt = build_rt();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            sock.read_exact(&mut buf).await.unwrap();
            sock.write_all(&buf).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
        server.await.unwrap();
    });
}

/// Hybrid timer flow on the core-holder park path: the ring park must
/// fold in the legacy wheel's deadline (no other thread exists to fire
/// timers for us).
#[test]
fn sleep_fires() {
    let _watchdog = HangWatchdog::arm("sleep_fires", Duration::from_secs(30));
    let rt = build_rt();
    let started = Instant::now();
    rt.block_on(async {
        tokio::time::sleep(Duration::from_millis(50)).await;
    });
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(40),
        "returned early: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "took too long: {elapsed:?}"
    );
}

/// A remote `Handle::spawn` while the core holder is blocked in
/// `submit_and_wait` must route through `GlobalRing::unpark(0)` (the
/// eventfd), not the never-polled legacy mio waker. Pre-wiring this
/// hung: the wake vanished and `block_on` slept forever.
#[test]
fn remote_spawn_wakes_parked_runtime() {
    let _watchdog = HangWatchdog::arm("remote_spawn_wakes_parked_runtime", Duration::from_secs(30));
    let rt = build_rt();
    let handle = rt.handle().clone();
    let (tx, rx) = tokio::sync::oneshot::channel::<u32>();

    let spawner = std::thread::spawn(move || {
        // Give block_on time to park on the ring, deadline-free.
        std::thread::sleep(Duration::from_millis(200));
        handle.spawn(async move {
            tx.send(7).unwrap();
        })
    });

    // No timers anywhere: the ONLY way this returns is the remote
    // spawn's unpark punching through to the ring's eventfd. A lost
    // wake is an indefinite park → watchdog abort.
    let n = rt.block_on(async { rx.await.unwrap() });
    assert_eq!(n, 7);
    spawner.join().unwrap();
}

/// The core-migration claim, part 1: registrations armed on the shared
/// ring by thread A remain live and deliver readiness when thread B
/// steals the core and drives the ring. Also carries an in-flight timer
/// (inserted under A, fired under B) across the same handoff.
#[test]
fn core_migrates_across_block_on_threads() {
    let _watchdog = HangWatchdog::arm(
        "core_migrates_across_block_on_threads",
        Duration::from_secs(60),
    );
    let rt = build_rt();

    // Thread MAIN: create live registrations + an in-flight timer.
    let (mut client, mut server, sleeper) = rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, (server, _)) =
            tokio::join!(async { TcpStream::connect(addr).await.unwrap() }, async {
                listener.accept().await.unwrap()
            });
        // Timer armed now, awaited on the next thread.
        let sleeper = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            13u32
        });
        (client, server, sleeper)
    });

    // Thread B: steal the core, drive the SAME registrations + reap the
    // timer armed on MAIN.
    std::thread::scope(|s| {
        s.spawn(|| {
            rt.block_on(async {
                client.write_all(b"one").await.unwrap();
                let mut buf = [0u8; 3];
                server.read_exact(&mut buf).await.unwrap();
                assert_eq!(&buf, b"one");
                assert_eq!(sleeper.await.unwrap(), 13);
            });
        })
        .join()
        .unwrap();
    });

    // Back on MAIN: third steal, same registrations, reverse direction.
    rt.block_on(async {
        server.write_all(b"two").await.unwrap();
        let mut buf = [0u8; 3];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"two");
    });
}

/// The core-migration claim, part 2: a second `block_on` thread that
/// LOSES the core race parks on the scheduler `Notify` (never the ring)
/// and still completes via the remote-wake path.
#[test]
fn concurrent_block_on_two_threads() {
    let _watchdog = HangWatchdog::arm("concurrent_block_on_two_threads", Duration::from_secs(60));
    let rt = Arc::new(build_rt());

    let (tx, rx) = tokio::sync::oneshot::channel::<u32>();
    let rt2 = Arc::clone(&rt);
    let loser = std::thread::spawn(move || rt2.block_on(async { rx.await.unwrap() }));

    // This thread does IO + a timer while the other block_on waits.
    rt.block_on(async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (mut c, (mut s, _)) =
            tokio::join!(async { TcpStream::connect(addr).await.unwrap() }, async {
                listener.accept().await.unwrap()
            });
        c.write_all(b"x").await.unwrap();
        let mut b = [0u8; 1];
        s.read_exact(&mut b).await.unwrap();
        tx.send(9).unwrap();
    });

    assert_eq!(loser.join().unwrap(), 9);
}

/// Shutdown on a different thread than the last ring driver: `shutdown2`
/// runs wherever the runtime is dropped; with a live registration still
/// armed on the shared ring. Legal (relaxed ring), must not hang or
/// leak the drop.
#[test]
fn shutdown_on_other_thread() {
    let _watchdog = HangWatchdog::arm("shutdown_on_other_thread", Duration::from_secs(30));
    let rt = build_rt();
    let streams = rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (c, (s, _)) = tokio::join!(async { TcpStream::connect(addr).await.unwrap() }, async {
            listener.accept().await.unwrap()
        });
        (listener, c, s)
    });
    std::thread::spawn(move || {
        // Registrations (and their armed POLL_ADD_MULTIs) die with the
        // runtime on THIS thread.
        drop(streams);
        drop(rt);
    })
    .join()
    .unwrap();
}

/// Register/dereg churn through the single pending-op queue — the path
/// the historical ArmTable-exhaustion wedge lived on. Volume is sized
/// for a smoke test, not a bench.
#[test]
fn register_dereg_churn_smoke() {
    let _watchdog = HangWatchdog::arm("register_dereg_churn_smoke", Duration::from_secs(60));
    let rt = build_rt();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..200 {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut b = [0u8; 1];
                sock.read_exact(&mut b).await.unwrap();
            }
        });
        for _ in 0..200 {
            let mut c = TcpStream::connect(addr).await.unwrap();
            c.write_all(b"z").await.unwrap();
            // c drops here: Deregister queued behind its Register.
        }
        server.await.unwrap();
    });
}

/// `LocalRuntime` + `spawn_local` with a genuinely `!Send` task doing
/// net I/O — the strategic point of this arm.
#[test]
fn local_runtime_spawn_local_not_send_io() {
    let _watchdog = HangWatchdog::arm(
        "local_runtime_spawn_local_not_send_io",
        Duration::from_secs(30),
    );
    let rt = build_local();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Rc makes the future !Send; spawn_local (not spawn) must accept it.
        let marker = std::rc::Rc::new(5u8);
        let server = tokio::task::spawn_local(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1];
            sock.read_exact(&mut buf).await.unwrap();
            buf[0] + *marker
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&[1u8]).await.unwrap();
        assert_eq!(server.await.unwrap(), 6);
    });
}

/// Timer-insert kick regression (the F1 class from
/// `rt_uring_late_timer.rs`, current_thread edition). On ct the core
/// holder recomputes the wheel minimum at every park entry, so the only
/// stale-deadline window is a timer inserted by a thread that is NOT
/// the core holder while the holder is blocked in the ring: a second
/// `block_on` caller that lost the core race polls its future — and
/// arms its `Sleep` — on the `Notify` fallback path. That insert must
/// kick the holder's ring (`unpark_for_insert` → `uring_handle()` →
/// `timer_kick`); the legacy mio unpark would land on a driver nobody
/// polls and BOTH threads would sleep forever (→ watchdog abort).
#[test]
fn late_timer_from_non_core_block_on_fires() {
    let _watchdog = HangWatchdog::arm(
        "late_timer_from_non_core_block_on_fires",
        Duration::from_secs(30),
    );
    let rt = Arc::new(build_rt());
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let rt2 = Arc::clone(&rt);
    let non_core = std::thread::spawn(move || {
        // Lose the core race: main thread's block_on has been parked on
        // the ring (deadline-free) for 200ms by the time we arrive.
        std::thread::sleep(Duration::from_millis(200));
        rt2.block_on(async move {
            // Polled on the Notify path — this insert happens off the
            // core-holder thread.
            tokio::time::sleep(Duration::from_millis(50)).await;
            tx.send(()).unwrap();
        });
    });

    // Core holder: parks indefinitely; only the non-core thread's timer
    // firing (driven by OUR ring park honoring the kicked deadline) can
    // resolve the oneshot.
    rt.block_on(async { rx.await.unwrap() });
    non_core.join().unwrap();
}

/// Exercises the readiness (poll) path heavily on the current_thread
/// runtime: repeated connect/accept/read/write round-trips must drive
/// the io_uring reactor's `POLL_ADD_MULTI` registrations without a
/// crash or hang.
#[test]
fn poll_path_only_smoke() {
    let rt = build_rt();
    rt.block_on(async {
        for _ in 0..10 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (mut c, (mut s, _)) =
                tokio::join!(async { TcpStream::connect(addr).await.unwrap() }, async {
                    listener.accept().await.unwrap()
                });
            c.write_all(b"hello").await.unwrap();
            let mut buf = [0u8; 5];
            s.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hello");
        }
    });
}
