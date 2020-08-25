use std::convert::TryInto;
use std::net::{TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::io::{Read, Write};
use std::os::unix::io::{AsRawFd, FromRawFd};

use tarantool_module::{CoIOListener, CoIOStream, Fiber};
use tarantool_module::fiber::sleep;

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
        let mut stream = CoIOStream::<TcpStream>::new(
            unsafe { TcpStream::from_raw_fd(soc.as_raw_fd()) }
        ).unwrap();
        let mut buf: Vec<u8> = vec![0; 4];
        stream.read(&mut buf).unwrap();
        assert_eq!(buf, vec![1, 2, 3, 4]);
        0
    });
    reader_fiber.set_joinable(true);
    reader_fiber.start(reader_soc);

    let mut writer_fiber = Fiber::new("test_fiber", &mut move |soc: Box<UnixStream>| {
        let mut stream = CoIOStream::<TcpStream>::new(
            unsafe { TcpStream::from_raw_fd(soc.as_raw_fd()) }
        ).unwrap();
        stream.write_all(&vec![1, 2, 3, 4]).unwrap();
        0
    });
    writer_fiber.set_joinable(true);
    writer_fiber.start(writer_soc);

    reader_fiber.join();
    writer_fiber.join();
}
