use self::{client::PgClient, error::PgResult, tls::TlsAcceptor};
use crate::{address::IprotoAddress, introspection::Introspection, tlog, traft::error::Error};
use std::path::{Path, PathBuf};
use stream::PgStream;
use tarantool::coio::{CoIOListener, CoIOStream};

mod backend;
mod client;
mod error;
mod messages;
mod server;
mod stream;
mod tls;
mod value;

/// Main postgres server configuration.
#[derive(PartialEq, Default, Debug, Clone, serde::Deserialize, serde::Serialize, Introspection)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: Option<IprotoAddress>,

    #[introspection(config_default = false)]
    pub ssl: Option<bool>,
}

impl Config {
    pub fn enabled(&self) -> bool {
        // Pgproto is enabled if listen was specified.
        self.listen.is_some()
    }

    pub fn listen(&self) -> IprotoAddress {
        self.listen
            .clone()
            .expect("must be checked before the call")
    }

    pub fn ssl(&self) -> bool {
        self.ssl.expect("set by default")
    }
}

fn server_start(context: Context) {
    // Help DBA diagnose storages by initializing them asap.
    backend::storage::force_init_portals_and_statements();

    while let Ok(raw) = context.server.accept() {
        let stream = PgStream::new(raw);
        if let Err(e) = handle_client(stream, context.tls_acceptor.clone()) {
            tlog!(Error, "failed to handle client {e}");
        }
    }
}

fn handle_client(
    client: PgStream<CoIOStream>,
    tls_acceptor: Option<TlsAcceptor>,
) -> tarantool::Result<()> {
    tlog!(Info, "spawning a new fiber for postgres client connection");

    tarantool::fiber::Builder::new()
        .name("pgproto::client")
        .func(move || {
            let res = do_handle_client(client, tls_acceptor);
            if let Err(e) = res {
                tlog!(Error, "postgres client connection error: {e}");
            }
        })
        .start_non_joinable()?;

    Ok(())
}

fn do_handle_client(
    stream: PgStream<CoIOStream>,
    tls_acceptor: Option<TlsAcceptor>,
) -> PgResult<()> {
    let mut client = PgClient::accept(stream, tls_acceptor)?;

    // Send important parameters to the client.
    client
        .send_parameter("server_version", "15.0")?
        .send_parameter("server_encoding", "UTF8")?
        .send_parameter("client_encoding", "UTF8")?
        .send_parameter("DateStyle", "ISO, MDY")?
        .send_parameter("integer_datetimes", "on")?
        .send_parameter("TimeZone", "UTC")?;

    client.process_messages_loop()?;

    Ok(())
}

/// Server execution context.
pub struct Context {
    server: CoIOListener,
    tls_acceptor: Option<TlsAcceptor>,
}

impl Context {
    pub fn new(config: &Config, data_dir: &Path) -> Result<Self, Error> {
        assert!(config.enabled(), "must be checked before the call");

        let listen = config.listen();
        let host = listen.host.as_str();
        let port = listen.port.parse::<u16>().map_err(|_| {
            Error::invalid_configuration(format!("bad postgres port {}", listen.port))
        })?;

        let tls_acceptor = config
            .ssl()
            .then(|| TlsAcceptor::new_from_dir(data_dir))
            .transpose()
            .map_err(Error::invalid_configuration)?;

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
pub fn start(config: &Config, data_dir: PathBuf) -> Result<(), Error> {
    let context = Context::new(config, &data_dir)?;

    tarantool::fiber::Builder::new()
        .name("pgproto")
        .func(move || server_start(context))
        .start_non_joinable()?;

    Ok(())
}
