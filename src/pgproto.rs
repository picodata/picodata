#![warn(clippy::or_fun_call)]

use self::{client::PgClient, error::PgResult, tls::TlsAcceptor};
use crate::config::PgprotoConfig;
use crate::{static_ref, storage::Catalog, tlog, traft::error::Error};
use prometheus::IntCounter;
#[cfg(target_os = "linux")]
use smol_str::ToSmolStr;
use smol_str::{format_smolstr, SmolStr};
#[cfg(target_os = "linux")]
use std::os::linux::net::SocketAddrExt;
use std::{
    io,
    path::Path,
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
        backend::copy::PGPROTO_COPY_BATCHES_FLUSHED_TOTAL.clone(),
    ))?;
    registry.register(Box::new(
        backend::copy::PGPROTO_COPY_BATCH_FLUSH_DURATION.clone(),
    ))?;
    registry.register(Box::new(
        backend::copy::PGPROTO_COPY_BYTES_RECEIVED_TOTAL.clone(),
    ))?;
    registry.register(Box::new(
        backend::copy::PGPROTO_COPY_RECORD_LIMIT_ERRORS_TOTAL.clone(),
    ))?;
    registry.register(Box::new(
        backend::copy::PGPROTO_COPY_ROWS_INSERTED_TOTAL.clone(),
    ))?;
    registry.register(Box::new(
        backend::copy::PGPROTO_COPY_SESSIONS_STARTED_TOTAL.clone(),
    ))?;
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

fn get_peer_address(raw: &CoIOStream) -> io::Result<SmolStr> {
    let addr = socket2::SockRef::from(raw).peer_addr()?;

    if let Some(sa) = addr.as_socket() {
        return Ok(format_smolstr!("{sa}"));
    }

    if let Some(unix) = addr.as_unix() {
        if let Some(name) = unix.as_pathname() {
            return Ok(format_smolstr!("{}", name.display()));
        }

        #[cfg(target_os = "linux")]
        if let Some(name) = unix.as_abstract_name() {
            return Ok(name.escape_ascii().to_smolstr());
        }

        return Ok(format_smolstr!("<unix>"));
    }

    Ok(format_smolstr!("{addr:?}"))
}

fn server_start(context: &'static Context) {
    // Help DBA diagnose storages by initializing them asap.
    backend::storage::force_init_portals_and_statements();

    while let Ok(raw) = context
        .server
        .accept()
        .inspect_err(|e| tlog!(Error, "failed to accept: {e:?}"))
    {
        if let Err(e) = socket2::SockRef::from(&raw).set_nodelay(true) {
            tlog!(Error, "failed to enable TCP_NODELAY on socket: {e:?}");
        }

        let stream = PgStream::new(raw);
        let tls_acceptor = context.tls_acceptor.clone();
        if let Err(e) = handle_client(stream, tls_acceptor, context.storage) {
            tlog!(Error, "failed to handle client: {e}");
        }
    }

    tlog!(Info, "shut down postgres server");
}

fn handle_client(
    client: PgStream<CoIOStream>,
    tls_acceptor: Option<TlsAcceptor>,
    storage: &'static Catalog,
) -> tarantool::Result<()> {
    let peer_addr = get_peer_address(client.as_ref())?;

    tarantool::fiber::Builder::new()
        .name(format!("pgproto::client[{peer_addr}]"))
        .func(move || {
            tlog!(Info, "spawned a fiber for postgres client connection");
            let res = do_handle_client(client, tls_acceptor, storage);
            if let Err(e) = res {
                tlog!(Error, "connection has {e}");
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
    fn new(
        config: &PgprotoConfig,
        instance_dir: &Path,
        storage: &'static Catalog,
    ) -> Result<Self, Error> {
        let listen = config.listen();
        let host = listen.host.as_str();
        let port = listen.port.parse::<u16>().map_err(|_| {
            Error::invalid_configuration(format!("bad postgres port {}", listen.port))
        })?;

        let tls_acceptor = config
            .tls
            .enabled()
            .then(|| {
                if config.tls.password_file.is_some() {
                    tlog!(
                        Warning,
                        "Ignoring password_file option when creating a pgproto TlsAcceptor"
                    );
                }

                TlsAcceptor::new_from_paths(
                    instance_dir,
                    config.tls.cert_file.as_deref(),
                    config.tls.key_file.as_deref(),
                    config.tls.ca_file.as_deref(),
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
    config: &PgprotoConfig,
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
