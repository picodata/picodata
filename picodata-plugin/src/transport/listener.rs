//! Listener API for plugin services.
//!
//! This module provides a unified API for creating listening sockets in plugins
//! with optional TLS/mTLS support.
//!
//! # Example
//!
//! ```ignore
//! use picodata_plugin::plugin::prelude::*;
//! use picodata_plugin::transport::listener::PicoListener;
//! use std::io::{Read, Write};
//!
//! struct MyService;
//!
//! impl Service for MyService {
//!     type Config = ();
//!
//!     fn on_start(&mut self, ctx: &PicoContext, _cfg: Self::Config) -> CallbackResult<()> {
//!         let Some(listener) = PicoListener::bind(ctx)? else {
//!             // Listener is disabled in configuration
//!             return Ok(());
//!         };
//!
//!         // The listener uses cooperative I/O (CoIOListener), so accept() won't block
//!         // the Tarantool fiber scheduler. Each accept() will yield to other fibers.
//!         loop {
//!             let (mut connection, addr) = listener.accept()?;
//!             println!("New connection from {}", addr);
//!
//!             // Handle the connection (supports both plain and TLS streams)
//!             let mut buf = [0u8; 1024];
//!             let n = connection.read(&mut buf)?;
//!             connection.write_all(&buf[..n])?;
//!         }
//!     }
//! }
//! ```

use crate::plugin::interface::PicoContext;
use crate::transport::stream::{PicoStream, PicoStreamError, TlsHandshakeError};
use log::info;
use openssl::error::ErrorStack;
use openssl::ssl::{HandshakeError, SslAcceptor, SslFiletype, SslMethod, SslStream};
use openssl::x509::store::X509StoreBuilder;
use openssl::x509::{X509NameEntries, X509};
use std::fs;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use tarantool::coio::{CoIOListener, CoIOStream};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////

/// Error that can occur during TLS configuration.
#[derive(Error, Debug)]
pub enum TlsConfigError {
    /// OpenSSL internal error.
    #[error("openssl error: {0}")]
    OpenSsl(#[from] ErrorStack),

    /// Error reading certificate file.
    #[error("cert file error '{0}': {1}")]
    CertFile(PathBuf, io::Error),

    /// Error reading key file.
    #[error("key file error '{0}': {1}")]
    KeyFile(PathBuf, io::Error),

    /// Error reading CA file.
    #[error("ca file error '{0}': {1}")]
    CaFile(PathBuf, io::Error),

    /// Error reading password file.
    #[error("password file error '{0}': {1}")]
    PasswordFile(PathBuf, io::Error),
}

/// Error that can occur when creating a PicoListener.
#[derive(Error, Debug)]
pub enum PicoListenerError {
    /// Listener configuration is missing or invalid.
    #[error("listener configuration error: {0}")]
    Config(String),

    /// IO error when binding the socket.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// TLS configuration error.
    #[error("tls configuration error: {0}")]
    Tls(#[from] TlsConfigError),
}

////////////////////////////////////////////////////////////////////////////////
// TlsConfig
////////////////////////////////////////////////////////////////////////////////

/// TLS configuration for a listener.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    cert: PathBuf,
    key: PathBuf,
    ca_cert: Option<PathBuf>,
    password: Option<String>,
}

impl TlsConfig {
    /// Creates a new TLS configuration.
    ///
    /// # Arguments
    ///
    /// * `cert_file` - Path to the certificate file (PEM format).
    /// * `key_file` - Path to the private key file (PEM format).
    /// * `ca_file` - Optional path to CA certificate for mTLS.
    /// * `password_file` - Optional path to password file for encrypted keys.
    pub fn new(
        cert_file: impl AsRef<Path>,
        key_file: impl AsRef<Path>,
        ca_file: Option<impl AsRef<Path>>,
        password_file: Option<impl AsRef<Path>>,
    ) -> Result<Self, TlsConfigError> {
        let cert = fs::canonicalize(cert_file.as_ref())
            .map_err(|e| TlsConfigError::CertFile(cert_file.as_ref().to_path_buf(), e))?;

        let key = fs::canonicalize(key_file.as_ref())
            .map_err(|e| TlsConfigError::KeyFile(key_file.as_ref().to_path_buf(), e))?;

        let ca_cert = match ca_file {
            Some(path) => {
                let path = path.as_ref();
                let p = fs::canonicalize(path)
                    .map_err(|e| TlsConfigError::CaFile(path.to_path_buf(), e))?;
                Some(p)
            }
            None => None,
        };

        let password = match password_file {
            Some(path) => {
                let path = path.as_ref();
                let content = fs::read_to_string(path)
                    .map_err(|e| TlsConfigError::PasswordFile(path.to_path_buf(), e))?;
                Some(content.trim().to_string())
            }
            None => None,
        };

        Ok(Self {
            cert,
            key,
            ca_cert,
            password,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
// TlsAcceptor
////////////////////////////////////////////////////////////////////////////////

/// TLS acceptor for accepting encrypted connections.
#[derive(Clone)]
pub struct TlsAcceptor(Rc<SslAcceptor>);

impl TlsAcceptor {
    /// Creates a new TLS acceptor from the given configuration.
    pub fn new(config: &TlsConfig) -> Result<Self, TlsConfigError> {
        let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())?;

        info!("TLS: found key: {}", config.key.display());
        if let Some(ref password) = config.password {
            let key_pem = fs::read(&config.key)
                .map_err(|e| TlsConfigError::KeyFile(config.key.clone(), e))?;
            let pkey = openssl::pkey::PKey::private_key_from_pem_passphrase(
                &key_pem,
                password.as_bytes(),
            )?;
            builder.set_private_key(&pkey)?;
        } else {
            builder
                .set_private_key_file(&config.key, SslFiletype::PEM)
                .map_err(|e| TlsConfigError::KeyFile(config.key.clone(), e.into()))?;
        }

        info!("TLS: found certificate: {}", config.cert.display());
        builder
            .set_certificate_chain_file(&config.cert)
            .map_err(|e| TlsConfigError::CertFile(config.cert.clone(), e.into()))?;

        let pem =
            fs::read(&config.cert).map_err(|e| TlsConfigError::CertFile(config.cert.clone(), e))?;
        let certs = X509::stack_from_pem(&pem)?;
        if let Ok(cert_info) = certs_to_string(&certs) {
            info!("TLS: cert stack: {}", cert_info);
        }

        if let Some(ref ca) = config.ca_cert {
            info!("TLS: found CA certificate: {}", ca.display());
            let pem = fs::read(ca).map_err(|e| TlsConfigError::CaFile(ca.clone(), e))?;
            let certs = X509::stack_from_pem(&pem)?;
            if let Ok(cert_info) = certs_to_string(&certs) {
                info!("TLS: CA cert stack: {}", cert_info);
            }

            let mut store_builder = X509StoreBuilder::new()?;
            for cert in certs {
                store_builder.add_cert(cert)?;
            }
            builder.set_verify_cert_store(store_builder.build())?;

            use openssl::ssl::SslVerifyMode;
            let mode = SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT;
            builder.set_verify(mode);
        }

        Ok(Self(Rc::new(builder.build())))
    }

    /// Accepts a TLS connection on the given stream.
    pub fn accept<S>(&self, stream: S) -> Result<SslStream<S>, TlsHandshakeError>
    where
        S: Read + Write,
    {
        self.0.accept(stream).map_err(|e| match e {
            HandshakeError::SetupFailure(stack) => TlsHandshakeError::SetupFailure(stack),
            HandshakeError::Failure(e) | HandshakeError::WouldBlock(e) => {
                TlsHandshakeError::Failure(e.into_error())
            }
        })
    }

    /// Returns a description of the TLS mode.
    pub fn kind(&self) -> &'static str {
        use openssl::ssl::SslVerifyMode;
        if self.0.context().verify_mode().contains(SslVerifyMode::PEER) {
            "mTLS (mutual TLS)"
        } else {
            "TLS"
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Certificate formatting helpers
////////////////////////////////////////////////////////////////////////////////

// TODO: This function is duplicated in src/pgproto/tls.rs
fn x509_name_entries_to_string(
    entries: X509NameEntries,
) -> Result<String, openssl::error::ErrorStack> {
    let mut s = vec![];

    for entry in entries {
        s.push(format!(
            "{key} = {val:?}",
            key = entry.object(),
            val = entry.data().as_utf8()?,
        ));
    }

    Ok(s.join(", "))
}

// TODO: This function is duplicated in src/pgproto/tls.rs
// Consider creating a shared TLS utilities crate for both picodata and plugins.
fn cert_to_string(cert: &X509) -> Result<String, openssl::error::ErrorStack> {
    let issuer = x509_name_entries_to_string(cert.issuer_name().entries())?;
    let subject = x509_name_entries_to_string(cert.subject_name().entries())?;
    let serial = cert.serial_number().to_bn()?.to_hex_str()?;

    let not_before = cert.not_before().to_string();
    let not_after = cert.not_after().to_string();

    let s = format!(
        "\
        \n    Serial number: {serial}\
        \n    Issuer:  {issuer}\
        \n    Subject: {subject}\
        \n    Validity:\
        \n        Not before: {not_before}\
        \n        Not after:  {not_after}\
        "
    );

    Ok(s)
}

// TODO: This function is duplicated in src/pgproto/tls.rs
fn certs_to_string(certs: &[X509]) -> Result<String, openssl::error::ErrorStack> {
    let mut s = String::new();

    for cert in certs {
        use std::fmt::Write;
        writeln!(&mut s, "{}", cert_to_string(cert)?).unwrap();
    }

    Ok(s)
}

////////////////////////////////////////////////////////////////////////////////
// ListenerConfig (internal)
////////////////////////////////////////////////////////////////////////////////

/// Internal configuration structure for creating a PicoListener.
#[derive(Debug, Clone, Default)]
pub struct ListenerConfig {
    /// Whether the listener is enabled.
    pub enabled: bool,
    /// The address to listen on.
    pub listen: Option<String>,
    /// The address to advertise.
    pub advertise: Option<String>,
    /// TLS settings.
    pub tls_enabled: bool,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub ca_file: Option<PathBuf>,
    pub password_file: Option<PathBuf>,
}

impl From<&crate::internal::types::FfiListenerConfig> for ListenerConfig {
    fn from(ffi: &crate::internal::types::FfiListenerConfig) -> Self {
        ListenerConfig {
            enabled: ffi.is_enabled(),
            listen: ffi.listen().map(String::from),
            advertise: ffi.advertise().map(String::from),
            tls_enabled: ffi.is_tls_enabled(),
            cert_file: ffi.cert_file().map(PathBuf::from),
            key_file: ffi.key_file().map(PathBuf::from),
            ca_file: ffi.ca_file().map(PathBuf::from),
            password_file: ffi.password_file().map(PathBuf::from),
        }
    }
}

/// Get listener configuration by calling picodata via FFI.
fn get_listener_config(plugin: &str, service: &str) -> Option<ListenerConfig> {
    use crate::util::FfiSafeStr;

    let ffi_config = unsafe {
        crate::internal::ffi::pico_ffi_get_listener_config(
            FfiSafeStr::from(plugin),
            FfiSafeStr::from(service),
        )
    };

    ffi_config
        .into_option()
        .map(|cfg| ListenerConfig::from(&cfg))
}

/// Parse a listen address string into SocketAddr.
fn parse_listen_addr(addr: &str) -> Result<SocketAddr, PicoListenerError> {
    addr.parse::<SocketAddr>()
        .map_err(|e| PicoListenerError::Config(format!("invalid listen address '{}': {}", addr, e)))
}

/// Creates a CoIOListener.
fn create_coio_listener(addr: SocketAddr) -> Result<CoIOListener, PicoListenerError> {
    let tcp_listener = std::net::TcpListener::bind(addr)?;
    let coio_listener = CoIOListener::try_from(tcp_listener)?;
    Ok(coio_listener)
}

fn with_socket<F, T>(stream: &CoIOStream, f: F) -> io::Result<T>
where
    F: FnOnce(&socket2::SockRef) -> io::Result<T>,
{
    let fd = stream.as_raw_fd();
    // SAFETY: stream contains a valid descriptor
    let fd = unsafe { BorrowedFd::borrow_raw(fd) };
    let socket = socket2::SockRef::from(&fd);
    f(&socket)
}

fn get_peer_addr(stream: &CoIOStream) -> io::Result<SocketAddr> {
    with_socket(stream, |socket| {
        let addr = socket.peer_addr()?;
        addr.as_socket()
            .ok_or_else(|| io::Error::other("peer address is not a socket address"))
    })
}

////////////////////////////////////////////////////////////////////////////////
// PicoListener
////////////////////////////////////////////////////////////////////////////////

/// A listener for accepting incoming connections in a plugin service.
///
/// The listener is created from the service's `PicoContext` and uses the
/// configuration from the `plugin.<name>.service.<name>.listener` section
/// in the configuration file.
pub struct PicoListener {
    listener: CoIOListener,
    tls_acceptor: Option<TlsAcceptor>,
    advertise: String,
}

impl PicoListener {
    /// Creates a new listener for the service using the provided configuration.
    ///
    /// This is the preferred method when the configuration is available
    /// (e.g., passed through `on_start` callback).
    ///
    /// # Returns
    ///
    /// - `Ok(Some(listener))` if the listener was successfully bound
    /// - `Ok(None)` if the listener is disabled in the configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The socket cannot be bound
    /// - TLS is enabled but certificate files cannot be read
    pub fn bind_with_config(config: &ListenerConfig) -> Result<Option<Self>, PicoListenerError> {
        if !config.enabled {
            return Ok(None);
        }

        let listen_addr = config
            .listen
            .as_ref()
            .ok_or_else(|| PicoListenerError::Config("listen address not specified".into()))?;

        let socket_addr = parse_listen_addr(listen_addr)?;

        let listener = create_coio_listener(socket_addr)?;

        let tls_acceptor = if config.tls_enabled {
            let cert_file = config.cert_file.as_ref().ok_or_else(|| {
                PicoListenerError::Config("TLS enabled but cert_file not specified".into())
            })?;

            let key_file = config.key_file.as_ref().ok_or_else(|| {
                PicoListenerError::Config("TLS enabled but key_file not specified".into())
            })?;

            let tls_config = TlsConfig::new(
                cert_file,
                key_file,
                config.ca_file.as_ref(),
                config.password_file.as_ref(),
            )?;

            Some(TlsAcceptor::new(&tls_config)?)
        } else {
            None
        };

        let advertise = config
            .advertise
            .clone()
            .unwrap_or_else(|| listen_addr.clone());

        Ok(Some(Self {
            listener,
            tls_acceptor,
            advertise,
        }))
    }

    /// Creates a new listener for the service associated with the given context.
    ///
    /// This method looks up the configuration from the global registry.
    /// The configuration must have been registered by picodata before calling this method.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(listener))` if the listener was successfully bound
    /// - `Ok(None)` if the listener is disabled in the configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The listener configuration is missing or invalid
    /// - The socket cannot be bound
    /// - TLS is enabled but certificate files cannot be read
    pub fn bind(context: &PicoContext) -> Result<Option<Self>, PicoListenerError> {
        let plugin = context.plugin_name();
        let service = context.service_name();

        let Some(config) = get_listener_config(plugin, service) else {
            return Ok(None);
        };

        Self::bind_with_config(&config)
    }

    /// Accepts a new connection.
    ///
    /// This method blocks cooperatively using Tarantool's fiber scheduler until
    /// a new connection is available. If TLS is configured, the TLS handshake
    /// is performed automatically.
    ///
    /// # Returns
    ///
    /// A tuple containing the stream and the remote address.
    pub fn accept(&self) -> Result<(PicoStream, SocketAddr), PicoStreamError> {
        let stream = self.listener.accept()?;

        let addr = get_peer_addr(&stream)?;

        let pico_stream = match &self.tls_acceptor {
            Some(acceptor) => {
                let tls_stream = acceptor.accept(stream)?;
                PicoStream::tls(tls_stream)
            }
            None => PicoStream::plain(stream),
        };

        Ok((pico_stream, addr))
    }

    /// Returns the advertise address for this listener.
    ///
    /// This is the address that should be registered in `_pico_peer_address`
    /// for other nodes to connect to this service.
    pub fn advertise_address(&self) -> &str {
        &self.advertise
    }

    /// Returns true if TLS is enabled for this listener.
    pub fn is_tls(&self) -> bool {
        self.tls_acceptor.is_some()
    }

    /// Returns the TLS mode description if TLS is enabled.
    pub fn tls_kind(&self) -> Option<&'static str> {
        self.tls_acceptor.as_ref().map(|a| a.kind())
    }
}
