#![warn(rust_2018_idioms)]
// WASIp1 doesn't support bind
#![cfg(all(
    feature = "net",
    feature = "macros",
    feature = "rt",
    feature = "rt-multi-thread",
    feature = "io-util",
    not(all(target_os = "wasi", target_env = "p1")),
))]

//! A `TcpStream` is registered with the I/O driver on its first use, not when
//! it is created.

use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::{TcpListener, TcpStream};

use std::future::poll_fn;
use std::io::{Read, Write};
use std::net::TcpStream as StdStream;

/// A connected pair: the tokio side has never been used, the std side is
/// blocking.
async fn pair() -> (TcpStream, StdStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = std::thread::spawn(move || StdStream::connect(addr).unwrap());
    let (server, _) = listener.accept().await.unwrap();
    (server, client.join().unwrap())
}

#[tokio::test]
async fn unused_stream_round_trips_through_std() {
    let (server, mut client) = pair().await;

    // Never registered, so there is nothing to deregister.
    let mut std_server = server.into_std().unwrap();
    std_server.set_nonblocking(false).unwrap();

    client.write_all(b"ping").unwrap();
    let mut buf = [0u8; 4];
    std_server.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"ping");

    // And back: from_std registers on first use too.
    std_server.set_nonblocking(true).unwrap();
    let mut server = TcpStream::from_std(std_server).unwrap();
    client.write_all(b"pong").unwrap();
    server.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"pong");
}

#[tokio::test]
async fn first_use_from_each_side_of_a_split() {
    let (mut server, mut client) = pair().await;
    let (mut rd, mut wr) = server.split();

    client.write_all(b"ping").unwrap();

    // Both halves poll before either has registered the socket.
    let read = async {
        let mut buf = [0u8; 4];
        rd.read_exact(&mut buf).await.unwrap();
        buf
    };
    let write = async { wr.write_all(b"pong").await.unwrap() };
    let (buf, ()) = tokio::join!(read, write);
    assert_eq!(&buf, b"ping");

    let mut buf = [0u8; 4];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"pong");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_use_races_across_workers() {
    for _ in 0..64 {
        let (server, mut client) = pair().await;
        let (mut rd, mut wr) = server.into_split();

        client.write_all(b"ping").unwrap();

        // The owned halves race to register the socket from two workers.
        let read = tokio::spawn(async move {
            let mut buf = [0u8; 4];
            rd.read_exact(&mut buf).await.unwrap();
            buf
        });
        let write = tokio::spawn(async move { wr.write_all(b"pong").await.unwrap() });
        assert_eq!(&read.await.unwrap(), b"ping");
        write.await.unwrap();

        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"pong");
    }
}

#[tokio::test]
async fn try_io_registers() {
    let (server, mut client) = pair().await;

    // `try_read` is the stream's first use, so it must register the socket
    // for the later `readable` to ever resolve.
    let mut buf = [0u8; 4];
    match server.try_read(&mut buf) {
        Ok(0) => panic!("unexpected EOF"),
        Ok(_) => panic!("nothing was sent yet"),
        Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock),
    }

    client.write_all(b"ping").unwrap();
    server.readable().await.unwrap();
    assert_eq!(server.try_read(&mut buf).unwrap(), 4);
    assert_eq!(&buf, b"ping");
}

#[tokio::test]
async fn readiness_registers() {
    let (server, mut client) = pair().await;

    client.write_all(b"ping").unwrap();
    let ready = server.ready(Interest::READABLE).await.unwrap();
    assert!(ready.is_readable());
}

#[tokio::test]
async fn poll_ready_registers() {
    let (server, mut client) = pair().await;

    client.write_all(b"ping").unwrap();
    poll_fn(|cx| server.poll_read_ready(cx)).await.unwrap();
    poll_fn(|cx| server.poll_write_ready(cx)).await.unwrap();
}

/// Registering a descriptor the driver already has fails; with deferred
/// registration that error comes from the first use, not from `from_std`.
#[cfg(unix)]
#[tokio::test]
async fn registration_error_surfaces_from_first_use() {
    use std::os::unix::io::{AsRawFd, FromRawFd};

    let (server, mut client) = pair().await;
    let std_server = server.into_std().unwrap();
    let raw = std_server.as_raw_fd();

    let mut first = TcpStream::from_std(std_server).unwrap();
    // The same descriptor a second time: creating the stream succeeds, but
    // registering it cannot.
    let second = TcpStream::from_std(unsafe { StdStream::from_raw_fd(raw) }).unwrap();

    client.write_all(b"ping").unwrap();
    let mut buf = [0u8; 4];
    first.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");

    let err = second.readable().await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);

    // `second` never registered, so this is a plain unwrap; the descriptor
    // belongs to `first`, so it must not be closed twice.
    std::mem::forget(second.into_std().unwrap());
}
