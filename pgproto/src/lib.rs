use bytes::{BufMut, BytesMut};
use pgwire::messages::Message;
use std::io::{self, Read};
use tarantool::{
    coio::{CoIOListener, CoIOStream},
    log::TarantoolLogger,
};

fn read_into_buf(reader: &mut CoIOStream, buf: &mut impl BufMut) -> io::Result<usize> {
    let slice = buf.chunk_mut();

    // SAFETY: coio's Read impl won't read uninitialized bytes.
    let cnt = unsafe {
        let uninit = slice.as_uninit_slice_mut();
        reader.read(std::mem::transmute(uninit))
    }?;

    unsafe { buf.advance_mut(cnt) }
    Ok(cnt)
}

// This will be executed once the library is loaded.
#[ctor::ctor]
fn setup_logger() {
    static LOGGER: TarantoolLogger = TarantoolLogger::new();
    log::set_logger(&LOGGER).expect("failed to set logger");
    log::set_max_level(log::LevelFilter::Info);
}

#[tarantool::proc]
fn server_start() {
    log::info!("starting server...");
    let server = server_bind(("127.0.0.1", 5432)).unwrap();

    // TODO: handle each client in a new fiber.
    while let Ok(s) = server.accept() {
        handle_client(s);
    }
}

fn server_bind(addr: (&str, u16)) -> io::Result<CoIOListener> {
    let mut socket = None;
    let mut f = |_| {
        let wrapped = std::net::TcpListener::bind(addr);
        log::info!("PG socket bind result: {wrapped:?}");
        socket.replace(wrapped);
        0
    };

    if tarantool::coio::coio_call(&mut f, ()) != 0 {
        return Err(io::Error::last_os_error());
    }

    let socket = socket.expect("uninitialized socket")?;
    tarantool::coio::CoIOListener::try_from(socket)
}

fn handle_client(mut client: CoIOStream) {
    let mut buf = BytesMut::with_capacity(8192);
    let cnt = read_into_buf(&mut client, &mut buf).unwrap();
    log::info!("read {cnt} bytes");

    log::info!("raw message: {buf:x?}");
    let message = pgwire::messages::startup::Startup::decode(&mut buf)
        .unwrap()
        .unwrap();

    log::info!("parsed message: {message:?}");
}
