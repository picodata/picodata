use std::convert::TryInto;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::time::Duration;

use tarantool::coio::{self, channel, CoIOListener, CoIOStream};
use tarantool::fiber;

pub fn coio_accept() {
    let tcp_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = tcp_listener.local_addr().unwrap();

    let coio_listener: CoIOListener = tcp_listener.try_into().unwrap();
    let client_fiber = fiber::start(|| {
        fiber::sleep(Duration::from_millis(10));
        TcpStream::connect(addr).unwrap();
    });
    let accept_result = coio_listener.accept();
    assert!(accept_result.is_ok());
    client_fiber.join();
}

pub fn coio_read_write() {
    let (reader_soc, writer_soc) = UnixStream::pair().unwrap();

    let reader_fiber = fiber::start(move || {
        let mut stream = CoIOStream::new(TcpStream::from(OwnedFd::from(reader_soc))).unwrap();
        let mut buf: Vec<u8> = vec![0; 4];
        stream.read_exact(&mut buf).unwrap();
        assert_eq!(buf, vec![1, 2, 3, 4]);
    });

    let writer_fiber = fiber::start(move || {
        let mut stream = CoIOStream::new(TcpStream::from(OwnedFd::from(writer_soc))).unwrap();
        stream.write_all(&[1, 2, 3, 4]).unwrap();
    });

    reader_fiber.join();
    writer_fiber.join();
}

pub fn coio_call() {
    let res = coio::coio_call(
        &mut |x| {
            assert_eq!(*x, 99);
            100
        },
        99,
    );
    assert_eq!(res, 100)
}

pub fn coio_channel() {
    let (tx, rx) = channel::<i32>(1);

    let fiber_a = fiber::start(move || {
        tx.send(99).unwrap();
    });

    let fiber_b = fiber::start(move || {
        let value = rx.recv().unwrap();
        assert_eq!(value, 99);
    });

    fiber_a.join();
    fiber_b.join();
}

pub fn channel_rx_closed() {
    let (tx, _) = channel::<i32>(1);

    let fiber = fiber::start(move || {
        assert!(tx.send(99).is_err());
    });
    fiber.join();
}

pub fn channel_tx_closed() {
    let (_, rx) = channel::<i32>(1);

    let fiber = fiber::start(move || {
        assert!(rx.recv().is_none());
    });
    fiber.join();
}
