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

/// The owned-buffer APIs (`uring_send`/`uring_recv`/`uring_recv_multi`)
/// submit through the caller's per-worker `LOCAL_REACTOR`, which the
/// global-ring mode (`TOKIO_URING_GLOBAL=1`) intentionally never
/// installs — there, they return `Unsupported` by design (see
/// `.claude/DESIGN-uring-global-phase1.md`). Tests exercising those
/// APIs skip under the knob instead of failing.
fn skip_in_global_mode() -> bool {
    let skip = std::env::var("TOKIO_URING_GLOBAL").is_ok_and(|v| v.trim() == "1");
    if skip {
        eprintln!("skipping: owned-buffer uring APIs are Unsupported under TOKIO_URING_GLOBAL=1");
    }
    skip
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

// ======================================================================
// Owned-buffer `uring_send` / `uring_recv` tests. These exercise the
// owned-Bytes ops path: the caller hands a buffer to the kernel, the
// reactor holds it in its slab for the full SQE lifetime, and returns
// it via the terminal CQE alongside the kernel's byte count. See
// `runtime/io/uring_bytes_ops.rs` for the ownership model docs.
// ======================================================================

#[test]
fn uring_send_recv_round_trip() {
    if skip_in_global_mode() {
        return;
    }
    use bytes::{Bytes, BytesMut};
    let rt = build_rt(1);
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let buf = BytesMut::zeroed(64);
            let (result, buf) = sock.uring_recv(buf).await;
            let n = result.expect("recv ok");
            assert_eq!(&buf[..n], b"ping");

            let out = Bytes::from_static(b"pong!");
            let (result, _buf) = sock.uring_send(out).await;
            let n = result.expect("send ok");
            assert_eq!(n, 5);
        });

        let client = tokio::spawn(async move {
            let sock = TcpStream::connect(addr).await.unwrap();
            let out = Bytes::from_static(b"ping");
            let (result, out) = sock.uring_send(out).await;
            let n = result.expect("client send ok");
            assert_eq!(n, 4);
            assert_eq!(&out[..], b"ping", "buffer returned intact");

            let buf = BytesMut::zeroed(64);
            let (result, buf) = sock.uring_recv(buf).await;
            let n = result.expect("client recv ok");
            assert_eq!(&buf[..n], b"pong!");
        });

        server.await.unwrap();
        client.await.unwrap();
    });
}

#[test]
fn uring_recv_short_read_returns_partial_buffer() {
    if skip_in_global_mode() {
        return;
    }
    // Capacity 64, peer sends 3 bytes and closes — the kernel's recv
    // returns 3, the buffer is 64 bytes long, and bytes [0..3] match
    // the payload.
    use bytes::{Bytes, BytesMut};
    let rt = build_rt(1);
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let (r, _) = sock.uring_send(Bytes::from_static(b"hey")).await;
            r.expect("send");
            drop(sock);
        });

        let client = tokio::spawn(async move {
            let sock = TcpStream::connect(addr).await.unwrap();
            let buf = BytesMut::zeroed(64);
            let (result, buf) = sock.uring_recv(buf).await;
            let n = result.expect("recv ok");
            assert_eq!(n, 3, "partial read kernel byte count");
            assert_eq!(&buf[..n], b"hey");
            assert_eq!(buf.len(), 64, "buffer capacity preserved on short read");
        });
        client.await.unwrap();
        server.await.unwrap();
    });
}

#[test]
fn uring_send_off_worker_returns_unsupported_with_buf() {
    // Outside a uring worker (plain std thread), uring_send must not
    // consume the buffer — it returns Unsupported and hands back the
    // exact bytes we passed in. We still need a uring worker somewhere
    // to construct a `TcpStream`, but the send itself is invoked from
    // a spawn_blocking context (no local reactor installed).
    use bytes::Bytes;
    let rt = build_rt(1);
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _server = tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let sock = TcpStream::connect(addr).await.unwrap();

        let fut = tokio::task::spawn_blocking(move || {
            let payload = Bytes::from_static(b"payload");
            // Block on the future from the blocking thread — no uring
            // reactor installed there.
            let (result, recovered) = futures::executor::block_on(sock.uring_send(payload));
            (result, recovered)
        });
        let (result, recovered) = fut.await.unwrap();
        let err = result.expect_err("off-worker uring_send should fail");
        assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
        assert_eq!(&recovered[..], b"payload", "buffer returned on off-worker error");
    });
}

#[test]
fn uring_recv_multi_round_trip_and_eof() {
    if skip_in_global_mode() {
        return;
    }
    // Exercise the multishot recv path end-to-end:
    //   1) Client arms `uring_recv_multi` on a connected socket.
    //   2) Server `uring_send`s two messages, then closes.
    //   3) Client drains the stream: two BufferLeases then None.
    use bytes::Bytes;
    let rt = build_rt(1);
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let (r, _) = sock.uring_send(Bytes::from_static(b"hello ")).await;
            r.expect("send1");
            let (r, _) = sock.uring_send(Bytes::from_static(b"world")).await;
            r.expect("send2");
            drop(sock);
        });

        let client = tokio::spawn(async move {
            let sock = TcpStream::connect(addr).await.unwrap();
            let mut stream = sock.uring_recv_multi();

            // Collect bytes until EOF. Short / coalesced reads are
            // possible (kernel may merge the two sends into one
            // delivery), so concatenate whatever we get.
            let mut collected: Vec<u8> = Vec::new();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
                let next = tokio::time::timeout(timeout, stream.next()).await
                    .expect("recv multi timed out");
                match next {
                    Some(Ok(lease)) => {
                        collected.extend_from_slice(&lease[..]);
                    }
                    Some(Err(e)) => panic!("recv multi error: {e}"),
                    None => break,
                }
            }
            assert_eq!(&collected[..], b"hello world");
        });

        server.await.unwrap();
        client.await.unwrap();
    });
}

#[test]
fn uring_send_future_drop_is_safe() {
    if skip_in_global_mode() {
        return;
    }
    // Drop the uring_send future *before* it completes. The buffer
    // must not be freed until the terminal CQE (potentially
    // `-ECANCELED` from our best-effort AsyncCancel) arrives. We
    // verify safety by (a) immediately reusing the socket for another
    // send + recv and (b) not crashing / not hanging. The best signal
    // we have for "buffer released on terminal CQE" is the second op
    // succeeding without corruption.
    use bytes::{Bytes, BytesMut};
    let rt = build_rt(1);
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let buf = BytesMut::zeroed(1024);
            let (r, buf) = sock.uring_recv(buf).await;
            let n = r.expect("server recv ok");
            // We don't care what landed — only that it didn't crash.
            assert!(n >= 1 && buf.len() == 1024);
        });

        let client = tokio::spawn(async move {
            let sock = TcpStream::connect(addr).await.unwrap();

            // Issue a send and drop the future before polling it to
            // completion. The buffer lives in the slab; our Drop impl
            // submits AsyncCancel.
            {
                let payload = Bytes::from_static(b"doomed");
                let _fut = sock.uring_send(payload);
                // _fut drops here without awaiting.
            }

            // Issue a second send and wait for it. If the first op's
            // buffer had been freed prematurely (UAF), this operation
            // would likely fault or scramble; success means the
            // ownership invariant held.
            let (r, _) = sock.uring_send(Bytes::from_static(b"x")).await;
            r.expect("second send ok");
            drop(sock);
        });

        client.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server didn't hang")
            .unwrap();
    });
}
