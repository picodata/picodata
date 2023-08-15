use std::io::Read;
use tarantool::log::TarantoolLogger;

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

    let mut socket = None;
    let mut f = |_| {
        let raw = std::net::TcpListener::bind(("127.0.0.1", 5432)).unwrap();
        let _ = socket.insert(raw);
        0
    };

    let res = tarantool::coio::coio_call(&mut f, ());
    assert!(res == 0);

    log::info!("{socket:?}");
    let listener = tarantool::coio::CoIOListener::try_from(socket.unwrap()).unwrap();

    while let Ok(mut s) = listener.accept() {
        let mut buf = [0u8; 78];
        s.read_exact(&mut buf).unwrap();
        log::info!("YAY, BYTES: {buf:x?}");
        let mut bytes = bytes::BytesMut::from(buf.as_ref());

        let sockaddr = std::net::SocketAddr::from(([0; 4], 5432));
        let mut codec = pgwire::tokio::PgWireMessageServerCodec::new(
            pgwire::api::ClientInfoHolder::new(sockaddr, false),
        );

        use tokio_util::codec::Decoder;
        let message = codec.decode(&mut bytes).unwrap().unwrap();
        log::info!("YAY, MESSAGE: {message:?}");

        handle_message(message);
    }
}

fn handle_message(message: pgwire::messages::PgWireFrontendMessage) {
    use pgwire::messages::PgWireFrontendMessage::*;
    match message {
        Startup(_) => todo!(),
        PasswordMessageFamily(_) => todo!(),
        Query(_) => todo!(),
        Parse(_) => todo!(),
        Close(_) => todo!(),
        Bind(_) => todo!(),
        Describe(_) => todo!(),
        Execute(_) => todo!(),
        Flush(_) => todo!(),
        Sync(_) => todo!(),
        Terminate(_) => todo!(),
        CopyData(_) => todo!(),
        CopyFail(_) => todo!(),
        CopyDone(_) => todo!(),
    }
}
