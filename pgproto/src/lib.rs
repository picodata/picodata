use bytes::{BufMut, BytesMut};
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
    let server = server_bind();

    // TODO: handle each client in a new fiber.
    while let Ok(s) = server.accept() {
        handle_client(s);
    }
}

fn server_bind() -> CoIOListener {
    let mut socket = None;
    let mut f = |_| {
        let raw = match std::net::TcpListener::bind(("127.0.0.1", 5432)) {
            Ok(listener) => listener,
            Err(e) => {
                log::error!("failed to bind postgres socket: {e}");
                return -1;
            }
        };

        log::info!("new postgres server: {raw:?}");
        socket.replace(raw);
        0
    };

    let res = tarantool::coio::coio_call(&mut f, ());
    assert!(res == 0);

    let socket = socket.expect("uninitialized socket");
    tarantool::coio::CoIOListener::try_from(socket).unwrap()
}

fn handle_client(mut client: CoIOStream) {
    let mut buf = BytesMut::with_capacity(8192);
    let cnt = read_into_buf(&mut client, &mut buf).unwrap();
    log::info!("read {cnt} bytes");

    log::info!("raw message: {buf:x?}");
    use pgwire::messages::Message;
    let message = pgwire::messages::startup::Startup::decode(&mut buf)
        .unwrap()
        .unwrap();

    log::info!("parsed message: {message:?}");
}
