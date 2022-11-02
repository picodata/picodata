use std::convert::TryInto;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::os::unix::net::UnixStream;
use std::time::Duration;

use tarantool::coio::{self, channel, CoIOListener, CoIOStream, Receiver, Sender};
use tarantool::fiber::{sleep, Fiber};

pub fn coio_accept() {
    let tcp_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = tcp_listener.local_addr().unwrap();

    let coio_listener: CoIOListener = tcp_listener.try_into().unwrap();
    let mut client_fiber = Fiber::new("test_fiber", &mut |_| {
        sleep(Duration::from_millis(10));
        TcpStream::connect(addr).unwrap();
        0
    });
    client_fiber.start(());
    let accept_result = coio_listener.accept();
    assert!(accept_result.is_ok());
}

pub fn coio_read_write() {
    let (reader_soc, writer_soc) = UnixStream::pair().unwrap();

    let mut reader_fiber = Fiber::new("test_fiber", &mut move |soc: Box<UnixStream>| {
        let mut stream =
            CoIOStream::new(unsafe { TcpStream::from_raw_fd(soc.as_raw_fd()) }).unwrap();
        let mut buf: Vec<u8> = vec![0; 4];
        stream.read_exact(&mut buf).unwrap();
        assert_eq!(buf, vec![1, 2, 3, 4]);
        0
    });
    reader_fiber.set_joinable(true);
    reader_fiber.start(reader_soc);

    let mut writer_fiber = Fiber::new("test_fiber", &mut move |soc: Box<UnixStream>| {
        let mut stream =
            CoIOStream::new(unsafe { TcpStream::from_raw_fd(soc.as_raw_fd()) }).unwrap();
        stream.write_all(&[1, 2, 3, 4]).unwrap();
        0
    });
    writer_fiber.set_joinable(true);
    writer_fiber.start(writer_soc);

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

    let mut fiber_a = Fiber::new("test_fiber_a", &mut |tx: Box<Sender<i32>>| {
        tx.send(99).unwrap();
        0
    });
    fiber_a.set_joinable(true);
    fiber_a.start(tx);

    let mut fiber_b = Fiber::new("test_fiber_b", &mut |rx: Box<Receiver<i32>>| {
        let value = rx.recv().unwrap();
        assert_eq!(value, 99);
        0
    });
    fiber_b.set_joinable(true);
    fiber_b.start(rx);

    fiber_a.join();
    fiber_b.join();
}

pub fn channel_rx_closed() {
    let (tx, _) = channel::<i32>(1);

    let mut fiber = Fiber::new("test_fiber", &mut |tx: Box<Sender<i32>>| {
        assert!(tx.send(99).is_err());
        0
    });
    fiber.set_joinable(true);
    fiber.start(tx);
    fiber.join();
}

pub fn channel_tx_closed() {
    let (_, rx) = channel::<i32>(1);

    let mut fiber = Fiber::new("test_fiber", &mut |rx: Box<Receiver<i32>>| {
        assert!(rx.recv().is_none());
        0
    });
    fiber.set_joinable(true);
    fiber.start(rx);
    fiber.join();
}
