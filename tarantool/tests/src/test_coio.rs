use std::convert::TryInto;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::os::unix::net::UnixStream;

use tarantool::coio::{channel, coio_call, CoIOListener, CoIOStream, Receiver, Sender};
use tarantool::fiber::{sleep, Fiber};

pub fn test_coio_accept() {
    let tcp_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = tcp_listener.local_addr().unwrap().clone();

    let coio_listener: CoIOListener = tcp_listener.try_into().unwrap();
    let mut client_fiber = Fiber::new("test_fiber", &mut |_| {
        sleep(0.01);
        TcpStream::connect(addr).unwrap();
        0
    });
    client_fiber.start(());
    let accept_result = coio_listener.accept();
    assert!(accept_result.is_ok());
}

pub fn test_coio_read_write() {
    let (reader_soc, writer_soc) = UnixStream::pair().unwrap();

    let mut reader_fiber = Fiber::new("test_fiber", &mut move |soc: Box<UnixStream>| {
        let mut stream =
            CoIOStream::new(unsafe { TcpStream::from_raw_fd(soc.as_raw_fd()) }).unwrap();
        let mut buf: Vec<u8> = vec![0; 4];
        stream.read(&mut buf).unwrap();
        assert_eq!(buf, vec![1, 2, 3, 4]);
        0
    });
    reader_fiber.set_joinable(true);
    reader_fiber.start(reader_soc);

    let mut writer_fiber = Fiber::new("test_fiber", &mut move |soc: Box<UnixStream>| {
        let mut stream =
            CoIOStream::new(unsafe { TcpStream::from_raw_fd(soc.as_raw_fd()) }).unwrap();
        stream.write_all(&vec![1, 2, 3, 4]).unwrap();
        0
    });
    writer_fiber.set_joinable(true);
    writer_fiber.start(writer_soc);

    reader_fiber.join();
    writer_fiber.join();
}

pub fn test_coio_call() {
    let res = coio_call(
        &mut |x| {
            assert_eq!(*x, 99);
            100
        },
        99,
    );
    assert_eq!(res, 100)
}

pub fn test_channel() {
    let (tx, rx) = channel::<i32>(1);

    let mut fiber_a = Fiber::new("test_fiber_a", &mut |tx: Box<Sender<i32>>| {
        tx.send(99);
        0
    });
    fiber_a.set_joinable(true);
    fiber_a.start(tx);

    let mut fiber_b = Fiber::new("test_fiber_b", &mut |rx: Box<Receiver<i32>>| {
        let value = rx.recv();
        assert_eq!(value, 99);
        0
    });
    fiber_b.set_joinable(true);
    fiber_b.start(rx);

    fiber_a.join();
    fiber_b.join();
}
