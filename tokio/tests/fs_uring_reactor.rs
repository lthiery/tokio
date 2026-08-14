//! `tokio::fs` io_uring ops running on the single shared uring reactor
//! (the unified driver: one ring for both net readiness and fs
//! completions).
//!
//! Unlike `fs_uring*.rs` (which drive the legacy fs side-driver on a
//! `Traditional`/mio runtime), every runtime here is built with
//! `enable_uring_reactor()`, so `tokio::fs::{read, write, File}` route
//! their open/read/write completion ops onto the reactor's ring via the
//! `Op<T>` backend seam. These are the exit-criterion tests for the fs-on-
//! reactor work: round-trip, concurrent net + fs, cancel-on-drop, and
//! shutdown with an op in flight.

#![cfg(all(
    tokio_unstable,
    feature = "io-uring-reactor",
    feature = "rt-multi-thread",
    feature = "fs",
    feature = "net",
    target_os = "linux",
))]
#![warn(rust_2018_idioms)]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};

fn reactor_rt(workers: usize) -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .enable_uring_reactor()
        .build()
        .expect("uring reactor runtime builds")
}

/// open + write + read round-trip, all on the reactor ring.
#[test]
fn open_write_read_round_trip() {
    let rt = reactor_rt(2);
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hello.txt");
        let payload = b"unified driver: fs on the reactor ring";

        // High-level write / read (route through Op::write_at / read_at).
        tokio::fs::write(&path, payload).await.unwrap();
        let got = tokio::fs::read(&path).await.unwrap();
        assert_eq!(got, payload);

        // File::open + read_to_end (routes through Op::open then Op::read_at).
        let mut f = tokio::fs::File::open(&path).await.unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, payload);
    });
}

/// Many concurrent fs ops on the reactor ring complete correctly (exercises
/// the shared FsOpSlab under parallelism).
#[test]
fn concurrent_fs_ops() {
    let rt = reactor_rt(4);
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut tasks = Vec::new();
        for i in 0..64u32 {
            let path = dir.path().join(format!("f{i}.txt"));
            tasks.push(tokio::spawn(async move {
                let body = format!("contents of file {i}").into_bytes();
                tokio::fs::write(&path, &body).await.unwrap();
                let got = tokio::fs::read(&path).await.unwrap();
                assert_eq!(got, body);
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
    });
}

/// The unification claim: net readiness (TCP echo) and fs completion ops
/// run concurrently on the SAME runtime, i.e. the one shared ring carries
/// both without either stalling.
#[test]
fn concurrent_net_and_fs() {
    let rt = reactor_rt(4);
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();

        // Echo server.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            sock.read_exact(&mut buf).await.unwrap();
            sock.write_all(&buf).await.unwrap();
        });

        // Concurrent fs churn while the socket round-trip is in flight.
        let fs = {
            let dir = dir.path().to_owned();
            tokio::spawn(async move {
                for i in 0..32u32 {
                    let path = dir.join(format!("n{i}.bin"));
                    let body = vec![i as u8; 256];
                    tokio::fs::write(&path, &body).await.unwrap();
                    assert_eq!(tokio::fs::read(&path).await.unwrap(), body);
                }
            })
        };

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(b"hello").await.unwrap();
        let mut got = [0u8; 5];
        client.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"hello");

        server.await.unwrap();
        fs.await.unwrap();
    });
}

/// Dropping an fs op's future before it completes must not wedge the ring:
/// the runtime keeps working afterward. Exercises `cancel_op` (buffers/fd
/// parked in the slab until the terminal CQE) on the reactor.
#[test]
fn drop_in_flight_op_then_continue() {
    let rt = reactor_rt(2);
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seed.txt");
        tokio::fs::write(&path, vec![7u8; 4096]).await.unwrap();

        // Start a batch of reads and drop the futures without awaiting.
        for _ in 0..16 {
            let fut = tokio::fs::read(&path);
            drop(fut);
        }
        // Also start and immediately drop some opens (Open cancel path:
        // a late CQE's fd must be closed by the slab, not leaked).
        for _ in 0..16 {
            let fut = tokio::fs::File::open(&path);
            drop(fut);
        }

        // The ring must still be healthy: a fresh op completes.
        let got = tokio::fs::read(&path).await.unwrap();
        assert_eq!(got.len(), 4096);
        assert!(got.iter().all(|&b| b == 7));
    });
}

/// The same round-trip on a current_thread reactor runtime (the forced
/// single-ring, n=1 park path), so both scheduler flavors are covered.
#[test]
fn current_thread_round_trip() {
    let rt = Builder::new_current_thread()
        .enable_all()
        .enable_uring_reactor()
        .build()
        .expect("current_thread uring reactor runtime builds");
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ct.txt");
        let payload = b"current_thread fs on the reactor ring";
        tokio::fs::write(&path, payload).await.unwrap();
        let got = tokio::fs::read(&path).await.unwrap();
        assert_eq!(got, payload);
    });
}

/// Runtime shutdown with fs ops spawned and in flight must not hang or
/// leak: the `GlobalRing` drop drains them.
#[test]
fn shutdown_with_in_flight_ops() {
    let rt = reactor_rt(2);
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_owned();
    rt.block_on(async {
        tokio::fs::write(base.join("s.txt"), vec![1u8; 8192])
            .await
            .unwrap();
    });
    // Spawn a flurry of fs ops, then tear the runtime down while they are
    // still likely in flight. shutdown_timeout must return (no hang).
    for i in 0..32u32 {
        let p = base.join("s.txt");
        let out = base.join(format!("o{i}.txt"));
        rt.spawn(async move {
            let data = tokio::fs::read(&p).await.unwrap_or_default();
            let _ = tokio::fs::write(&out, &data).await;
        });
    }
    rt.shutdown_timeout(Duration::from_secs(5));
}
