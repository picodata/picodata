use crate::address::Address;
use crate::tlog;
use tarantool::coio::CoIOListener;
use thiserror::Error;

mod client;
mod entrypoints;
mod error;
mod messages;
mod server;
mod storage;
mod stream;
mod tls;

use self::client::PgClient;
use self::error::PgResult;
use self::tls::{TlsAcceptor, TlsConfig};
use crate::introspection::Introspection;
use std::cell::Cell;
use std::path::{Path, PathBuf};
use stream::PgStream;
use tarantool::{coio::CoIOStream, fiber::JoinHandle};

pub use error::PgError;

fn server_start(context: Context) {
    let mut handles = vec![];
    while let Ok(raw) = context.server.accept() {
        let stream = PgStream::new(raw);
        handles.push(handle_client(stream, context.tls_acceptor.clone()));
    }

    // TODO: this feels forced; find a better way.
    for handle in handles {
        handle.join();
    }
}

fn handle_client(
    client: PgStream<CoIOStream>,
    tls_acceptor: Option<TlsAcceptor>,
) -> JoinHandle<'static, ()> {
    tlog!(Info, "spawning a new fiber for postgres client connection");
    tarantool::fiber::start(move || {
        let res = do_handle_client(client, tls_acceptor);
        if let Err(e) = res {
            tlog!(Error, "postgres client connection error: {e}");
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

#[derive(PartialEq, Default, Debug, Clone, serde::Deserialize, serde::Serialize, Introspection)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: Option<Address>,

    #[introspection(config_default = false)]
    pub ssl: Option<bool>,
}

impl Config {
    pub fn enabled(&self) -> bool {
        // Pgproto is enabled if listen was specified.
        self.listen.is_some()
    }

    pub fn listen(&self) -> Address {
        self.listen
            .clone()
            .expect("must be checked before the call")
    }

    pub fn ssl(&self) -> bool {
        self.ssl.expect("set by default")
    }
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("bad port: {0}")]
    BadPort(String),
}

pub struct Context {
    server: CoIOListener,
    tls_acceptor: Option<TlsAcceptor>,
}

impl Context {
    pub fn new(config: &Config, data_dir: &Path) -> PgResult<Self> {
        assert!(config.enabled(), "must be checked before the call");

        let listen = config.listen();
        let host = listen.host.as_str();
        let port = listen
            .port
            .parse::<u16>()
            .map_err(|_| ConfigError::BadPort(listen.port.clone()))?;

        let tls_acceptor = if config.ssl() {
            let tls_config = TlsConfig::from_data_dir(data_dir)?;
            Some(TlsAcceptor::new(&tls_config)?)
        } else {
            None
        };

        let addr = (host, port);
        tlog!(Info, "starting postgres server at {:?}...", addr);
        let server = server::new_listener(addr)?;

        Ok(Self {
            server,
            tls_acceptor,
        })
    }
}

/// Start a postgres server fiber.
///
/// WARNING: It must be called only once, otherwise a panic will happen.
pub fn start(config: &Config, data_dir: PathBuf) -> PgResult<()> {
    let context = Context::new(config, &data_dir)?;
    let handler = tarantool::fiber::start(move || server_start(context));

    // There's currently no way of detaching a fiber without leaking memory,
    // so we have to store it's join handle somewhere.
    //
    // From JoinHandle's doc:
    // NOTE: if `JoinHandle` is dropped before [`JoinHandle::join`] is called on it
    // a panic will happen.
    thread_local! {
        static FIBER_JOIN_HANDLE: Cell<Option<JoinHandle<'static, ()>>> = Cell::new(None);
    }
    FIBER_JOIN_HANDLE.replace(Some(handler));

    Ok(())
}
