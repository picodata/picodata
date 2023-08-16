mod server;
mod stream;

use std::io;
use stream::{messages, BeMessage, PgStream};
use tarantool::{coio::CoIOStream, fiber::UnitJoinHandle, log::TarantoolLogger};

// This will be executed once the library is loaded.
#[ctor::ctor]
fn setup_logger() {
    static LOGGER: TarantoolLogger = TarantoolLogger::new();
    log::set_logger(&LOGGER).expect("failed to set logger");
    log::set_max_level(log::LevelFilter::Info);
}

#[tarantool::proc]
fn server_start() {
    log::info!("starting postgres server...");
    let server = server::new_listener(("127.0.0.1", 5432)).unwrap();

    let mut handles = vec![];
    while let Ok(raw) = server.accept() {
        let stream = PgStream::new(raw);
        handles.push(handle_client(stream));
    }

    // TODO: this feels forced; find a better way.
    for handle in handles {
        handle.join();
    }
}

fn handle_client(client: PgStream<CoIOStream>) -> UnitJoinHandle<'static> {
    log::info!("spawning a new fiber for postgres client connection");
    tarantool::fiber::start_proc(move || {
        let res = do_handle_client(client);
        if let Err(e) = res {
            log::error!("postgres client connection error: {e}");
        }
    })
}

fn do_handle_client(mut client: PgStream<CoIOStream>) -> io::Result<()> {
    let message = client.read_message()?;
    log::info!("received a message: {message:?}");

    client
        .write_message_noflush(BeMessage::Authentication(
            messages::startup::Authentication::Ok,
        ))?
        .write_message_noflush(BeMessage::ParameterStatus(
            messages::startup::ParameterStatus::new(
                String::from("server_version"),
                String::from("15.3"),
            ),
        ))?
        .write_message(BeMessage::ReadyForQuery(
            messages::response::ReadyForQuery::new(messages::response::READY_STATUS_IDLE),
        ))?;

    // Now sleep indefinitely...
    loop {
        tarantool::fiber::sleep(std::time::Duration::from_secs(1));
    }
}
