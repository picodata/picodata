#![warn(clippy::or_fun_call)]

use self::{client::PgClient, error::PgResult, tls::TlsAcceptor};
use crate::{
    address::PgprotoAddress, introspection::Introspection, static_ref, storage::Catalog, tlog,
    traft::error::Error,
};
use prometheus::IntCounter;
use std::{
    os::fd::{AsRawFd, BorrowedFd},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        LazyLock,
    },
};
use stream::PgStream;
use tarantool::coio::{CoIOListener, CoIOStream};

pub mod backend;
mod client;
mod error;
mod messages;
mod server;
mod stream;
mod tls;
mod value;

static PGPROTO_CONNECTIONS_OPENED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(prometheus::Opts::new(
        "pico_pgproto_connections_opened_total",
        "Total number of opened connections since startup",
    ))
    .unwrap()
});

static PGPROTO_CONNECTIONS_CLOSED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::with_opts(prometheus::Opts::new(
        "pico_pgproto_connections_closed_total",
        "Total number of closed connections since startup",
    ))
    .unwrap()
});

/// Register all of pgproto's metrics.
pub fn register_metrics(registry: &prometheus::Registry) -> prometheus::Result<()> {
    registry.register(Box::new(PGPROTO_CONNECTIONS_CLOSED_TOTAL.clone()))?;
    registry.register(Box::new(PGPROTO_CONNECTIONS_OPENED_TOTAL.clone()))?;
    registry.register(Box::new(
        backend::storage::PGPROTO_PORTALS_CLOSED_TOTAL.clone(),
    ))?;
    registry.register(Box::new(
        backend::storage::PGPROTO_PORTALS_OPENED_TOTAL.clone(),
    ))?;
    registry.register(Box::new(
        backend::storage::PGPROTO_STATEMENTS_CLOSED_TOTAL.clone(),
    ))?;
    registry.register(Box::new(
        backend::storage::PGPROTO_STATEMENTS_OPENED_TOTAL.clone(),
    ))?;

    Ok(())
}

/// Initialized to enable PostgreSQL server protocol.
/// WARNING: if it is initialized, it does not directly mean
/// that PostgreSQL server protocol is currently running.
static mut CONTEXT: Option<Context> = None;

/// Main postgres server configuration.
#[derive(PartialEq, Default, Debug, Clone, serde::Deserialize, serde::Serialize, Introspection)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[introspection(config_default = PgprotoAddress::default())]
    pub listen: Option<PgprotoAddress>,

    #[introspection(config_default = self.listen())]
    pub advertise: Option<PgprotoAddress>,

    #[introspection(config_default = false)]
    pub ssl: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_file: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_file: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
}

impl Config {
    pub fn listen(&self) -> PgprotoAddress {
        self.listen
            .clone()
            .expect("must be checked before the call")
    }

    pub fn advertise(&self) -> PgprotoAddress {
        self.advertise
            .clone()
            .expect("must be checked before the call")
    }

    pub fn ssl(&self) -> bool {
        self.ssl.expect("set by default")
    }
}

fn enable_tcp_nodelay(raw: &CoIOStream) -> std::io::Result<()> {
    let fd = raw.as_raw_fd();
    // SAFETY: stream contains a valid descriptor
    let fd = unsafe { BorrowedFd::borrow_raw(fd) };
    let socket = socket2::SockRef::from(&fd);
    // FIXME: this blocks the event loop, as well as `CoioStream::new`
    socket.set_nodelay(true)?;
    Ok(())
}

fn server_start(context: &'static Context) {
    // Help DBA diagnose storages by initializing them asap.
    backend::storage::force_init_portals_and_statements();

    while let Ok(raw) = context
        .server
        .accept()
        .inspect_err(|e| tlog!(Error, "accept failed: {e:?}"))
    {
        if let Err(e) = enable_tcp_nodelay(&raw) {
            tlog!(Error, "failed to enable TCP_NODELAY on socket: {e:?}");
        }
        let stream = PgStream::new(raw);
        if let Err(e) = handle_client(stream, context.tls_acceptor.clone(), context.storage) {
            tlog!(Error, "failed to handle client {e}");
        }
    }

    tlog!(Info, "shut down postgres server");
}

fn handle_client(
    client: PgStream<CoIOStream>,
    tls_acceptor: Option<TlsAcceptor>,
    storage: &'static Catalog,
) -> tarantool::Result<()> {
    tlog!(Info, "spawning a new fiber for postgres client connection");

    tarantool::fiber::Builder::new()
        .name("pgproto::client")
        .func(move || {
            let res = do_handle_client(client, tls_acceptor, storage);
            if let Err(e) = res {
                tlog!(Error, "{e}");
            }
            tlog!(Info, "connection closed");
        })
        .start_non_joinable()?;

    Ok(())
}

fn do_handle_client(
    stream: PgStream<CoIOStream>,
    tls_acceptor: Option<TlsAcceptor>,
    storage: &Catalog,
) -> PgResult<()> {
    let mut client = PgClient::accept(stream, tls_acceptor, storage)?;

    // Having two distinct counters lets us have both the number
    // of active connections and the rate at which connections
    // are opened or closed.
    PGPROTO_CONNECTIONS_OPENED_TOTAL.inc();
    scopeguard::defer! {
        PGPROTO_CONNECTIONS_CLOSED_TOTAL.inc();
    }

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
struct Context {
    server: CoIOListener,
    tls_acceptor: Option<TlsAcceptor>,
    storage: &'static Catalog,
}

impl Context {
    fn new(config: &Config, instance_dir: &Path, storage: &'static Catalog) -> Result<Self, Error> {
        let listen = config.listen();
        let host = listen.host.as_str();
        let port = listen.port.parse::<u16>().map_err(|_| {
            Error::invalid_configuration(format!("bad postgres port {}", listen.port))
        })?;

        let tls_acceptor = config
            .ssl()
            .then(|| {
                TlsAcceptor::new_from_paths(
                    instance_dir,
                    config.cert_file.as_deref(),
                    config.key_file.as_deref(),
                    config.ca_file.as_deref(),
                )
            })
            .transpose()
            .map_err(Error::invalid_configuration)?
            .inspect(|tls| tlog!(Info, "configured {} for pgproto", tls.kind()));

        let addr = (host, port);
        tlog!(Info, "starting postgres server at {:?}...", addr);
        let server = server::new_listener(addr).map_err(|err| {
            Error::other(format!(
                "failed to start postgres server on {host}:{port}: {err}",
            ))
        })?;

        Ok(Self {
            server,
            tls_acceptor,
            storage,
        })
    }
}

/// Initialize PostgreSQL protocol server context. Sets up a global static
/// variable, instead of returning a context back to the caller.
/// **XXX: panics if called more than once!**
pub fn init_once(
    config: &Config,
    instance_dir: &Path,
    storage: &'static Catalog,
) -> Result<(), Error> {
    let context = Context::new(config, instance_dir, storage)?;

    // SAFETY: safe as long as only called from tx thread.
    unsafe {
        // This check protects us from use-after-free in client fibers.
        if static_ref!(const CONTEXT).is_some() {
            panic!("pgproto cannot be initialized more than once!");
        }
        CONTEXT = Some(context);
    }

    Ok(())
}

/// Start a PostgreSQL server fiber unless it's already running.
/// This function should only be called after [`init_once`],
/// **otherwise it will panic!**
pub fn start_once() -> Result<(), Error> {
    // Used to provide idempotency to enabling a PostgreSQL protocol.
    static IS_RUNNING: AtomicBool = AtomicBool::new(false);

    if !IS_RUNNING.load(Ordering::Relaxed) {
        // SAFETY: safe as long as only called from tx thread.
        let context = unsafe { static_ref!(const CONTEXT) }
            .as_ref()
            .expect("pgproto main context is uninitialized");

        tarantool::fiber::Builder::new()
            .name("pgproto")
            .func(|| server_start(context))
            .start_non_joinable()?;

        IS_RUNNING.store(true, Ordering::Relaxed);
    }

    Ok(())
}
