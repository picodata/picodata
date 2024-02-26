mod client;
mod entrypoints;
mod error;
mod helpers;
mod messages;
mod server;
mod storage;
mod stream;
mod tls;

use std::path::{Path, PathBuf};

use crate::client::PgClient;
use crate::error::PgResult;
use crate::tls::{TlsAcceptor, TlsConfig};
use stream::PgStream;
use tarantool::{coio::CoIOStream, fiber::UnitJoinHandle, log::TarantoolLogger};

// This will be executed once the library is loaded.
#[ctor::ctor]
fn setup_logger() {
    static LOGGER: TarantoolLogger = TarantoolLogger::new();
    log::set_logger(&LOGGER).expect("failed to set logger");
    log::set_max_level(log::LevelFilter::Info);
}

fn tls_from_pgdata(pgdata: &Path) -> PgResult<TlsAcceptor> {
    let config = TlsConfig::from_pgdata(pgdata)?;
    let acceptor = TlsAcceptor::new(&config)?;
    Ok(acceptor)
}

// The PGDATA directory contains several subdirectories and control files.
// For more details see https://www.postgresql.org/docs/current/storage-file-layout.html.
#[tarantool::proc]
fn server_start(host: &str, port: &str, pgdata: PathBuf) {
    let port = port.parse::<u16>().expect("bad port");

    let tls_acceptor = tls_from_pgdata(&pgdata)
        .map_err(|err| {
            log::info!("tls configuration failed: {err}");
            err
        })
        .ok();

    log::info!("starting postgres server at {host}:{port}...");
    let server = server::new_listener((&host, port)).unwrap();

    let mut handles = vec![];
    while let Ok(raw) = server.accept() {
        let stream = PgStream::new(raw);
        handles.push(handle_client(stream, tls_acceptor.clone()));
    }

    // TODO: this feels forced; find a better way.
    for handle in handles {
        handle.join();
    }
}

fn handle_client(
    client: PgStream<CoIOStream>,
    tls_acceptor: Option<TlsAcceptor>,
) -> UnitJoinHandle<'static> {
    log::info!("spawning a new fiber for postgres client connection");
    tarantool::fiber::start_proc(move || {
        let res = do_handle_client(client, tls_acceptor);
        if let Err(e) = res {
            log::error!("postgres client connection error: {e}");
        }
    })
}

fn do_handle_client(
    stream: PgStream<CoIOStream>,
    tls_acceptor: Option<TlsAcceptor>,
) -> PgResult<()> {
    let mut client = PgClient::accept(stream, tls_acceptor)?;
    client.send_parameter("server_version", "15.0")?;
    client.send_parameter("server_encoding", "UTF8")?;
    client.send_parameter("client_encoding", "UTF8")?;
    client.send_parameter("date_style", "ISO YMD")?;
    client.send_parameter("integer_datetimes", "on")?;
    client.process_messages_loop()?;
    Ok(())
}
