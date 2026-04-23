//! TcpStream / TcpListener round-trip tests under the experimental
//! per-worker `io_uring` reactor.
//!
//! These exercise the end-to-end fd registration path:
//!
//!   `TcpStream::connect` → `PollEvented::new` →
//!   `Registration::new_with_interest_and_handle` (uring branch) →
//!   `UringHandle::add_source` → `PendingOp::Register` →
//!   worker park drains queue → `reactor.register` submits
//!   `POLL_ADD_MULTI` → CQE wakes task via `ScheduledIo`.
//!
//! Only built on Linux with `tokio_unstable` + `io-uring-reactor` +
//! `rt-multi-thread` + `net`.

#![cfg(all(
    tokio_unstable,
    feature = "io-uring-reactor",
    feature = "rt-multi-thread",
    feature = "net",
    target_os = "linux",
))]
#![warn(rust_2018_idioms)]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime;

fn build_rt(workers: usize) -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_uring_reactor()
        .build()
        .expect("uring reactor runtime builds")
}

#[test]
fn tcp_single_worker_round_trip() {
    let rt = build_rt(1);
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 11];
            sock.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hello uring");
            sock.write_all(b"pong").await.unwrap();
            sock.shutdown().await.unwrap();
        });

        let client = tokio::spawn(async move {
            let mut sock = TcpStream::connect(addr).await.unwrap();
            sock.write_all(b"hello uring").await.unwrap();
            let mut buf = [0u8; 4];
            sock.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"pong");
        });

        server.await.unwrap();
        client.await.unwrap();
    });
}

#[test]
fn tcp_multi_worker_round_trip() {
    // With 4 workers the connect and accept ends typically land on
    // different workers, exercising the cross-worker pending-ops queue
    // plus round-robin fd→worker assignment.
    let rt = build_rt(4);
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            sock.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping!");
            sock.write_all(b"pong!").await.unwrap();
        });

        let client = tokio::spawn(async move {
            let mut sock = TcpStream::connect(addr).await.unwrap();
            sock.write_all(b"ping!").await.unwrap();
            let mut buf = [0u8; 5];
            sock.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"pong!");
        });

        server.await.unwrap();
        client.await.unwrap();
    });
}

#[test]
fn tcp_many_concurrent_connections() {
    // Exercise the pending-ops queue under fan-out: each connect creates a
    // fresh fd which is registered via `PendingOp::Register`. All four
    // workers will be registering fds and processing CQEs in parallel.
    const N: usize = 32;
    let rt = build_rt(4);
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let mut accepts = Vec::with_capacity(N);
            for _ in 0..N {
                let (mut sock, _) = listener.accept().await.unwrap();
                accepts.push(tokio::spawn(async move {
                    let mut buf = [0u8; 4];
                    sock.read_exact(&mut buf).await.unwrap();
                    sock.write_all(&buf).await.unwrap();
                    sock.shutdown().await.unwrap();
                }));
            }
            for h in accepts {
                h.await.unwrap();
            }
        });

        let mut clients = Vec::with_capacity(N);
        for i in 0..N {
            clients.push(tokio::spawn(async move {
                let mut sock = TcpStream::connect(addr).await.unwrap();
                let payload = (i as u32).to_be_bytes();
                sock.write_all(&payload).await.unwrap();
                let mut buf = [0u8; 4];
                sock.read_exact(&mut buf).await.unwrap();
                assert_eq!(buf, payload);
            }));
        }
        for h in clients {
            h.await.unwrap();
        }
        server.await.unwrap();
    });
}

#[test]
fn tcp_read_blocks_then_wakes() {
    // The reader parks waiting for data; the writer (possibly on a
    // different worker) produces it after a delay. Verifies POLL_ADD_MULTI
    // CQEs actually wake the parked reader.
    let rt = build_rt(2);
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            sock.write_all(b"delayed").await.unwrap();
        });

        let mut sock = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 7];
        let start = std::time::Instant::now();
        sock.read_exact(&mut buf).await.unwrap();
        let elapsed = start.elapsed();
        assert_eq!(&buf, b"delayed");
        assert!(elapsed >= Duration::from_millis(20), "read returned too early: {elapsed:?}");
        server.await.unwrap();
    });
}
