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

use crate::internal::types::FfiListenerConfigError;
use crate::plugin::interface::PicoContext;
use crate::transport::stream::{PicoStream, PicoStreamError, TlsHandshakeError};
use openssl::pkey::{PKey, Private};
use openssl::ssl::{HandshakeError, SslAcceptor, SslMethod, SslStream};
use openssl::x509::store::X509StoreBuilder;
use openssl::x509::X509;
use picodata_plugin_proc_macro::internal;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, BorrowedFd};
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
    OpenSsl(#[from] openssl::error::ErrorStack),

    #[error("The provided certificate chain did not contain any certificates")]
    EmptyCertificateChain,
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
// TlsAcceptor
////////////////////////////////////////////////////////////////////////////////

/// TLS acceptor for accepting encrypted connections.
#[internal]
#[derive(Clone)]
pub struct TlsAcceptor(Rc<SslAcceptor>);

impl TlsAcceptor {
    /// Creates a new TLS acceptor from the given configuration.
    pub fn new(config: &LoadedListenerTlsConfig) -> Result<Self, TlsConfigError> {
        let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())?;

        builder.set_private_key(&config.key)?;

        if let [head, tail @ ..] = config.cert_chain.as_slice() {
            builder.set_certificate(head)?;
            for cert in tail {
                builder.add_extra_chain_cert(cert.clone())?;
            }
        } else {
            return Err(TlsConfigError::EmptyCertificateChain);
        };

        if let Some(mtls_ca_chain) = &config.mtls_ca_chain {
            let mut store_builder = X509StoreBuilder::new()?;
            for cert in mtls_ca_chain {
                store_builder.add_cert(cert.clone())?;
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
// ListenerConfig (internal)
////////////////////////////////////////////////////////////////////////////////

/// Contains configuration needed to setup a [`PicoListener`].
///
/// [`PicoListener`]: PicoListener
#[internal]
#[derive(Clone, Debug)]
pub struct ListenerConfig {
    /// The address to listen on.
    pub listen: String,
    /// The address to advertise.
    pub advertise: String,
    /// Whether TLS is enabled.
    pub tls: Option<ListenerTlsConfig>,
}

/// Contains PEM-encoded TLS configuration needed to set up TLS for [`PicoListener`].
///
/// [`PicoListener`]: PicoListener
#[internal]
#[derive(Clone, Debug)]
pub struct ListenerTlsConfig {
    /// PEM-encoded certificate chain.
    pub cert_chain_pem: Vec<u8>,
    /// PEM-encoded private key to the certificate.
    pub key_pem: Vec<u8>,
    /// PEM-encoded CA certificate chain to verify connecting clients against. None if mTLS is not used.
    pub mtls_ca_chain_pem: Option<Vec<u8>>,
}

impl ListenerTlsConfig {
    /// Inflate the stored PEM-encoded representations of certificates and keys into a [`LoadedListenerTlsConfig`]
    pub fn load(&self) -> Result<LoadedListenerTlsConfig, TlsConfigError> {
        let cert_chain = X509::stack_from_pem(&self.cert_chain_pem)?;
        let key = PKey::private_key_from_pem(&self.key_pem)?;

        let mtls_ca_chain = if let Some(mtls_ca_chain_pem) = &self.mtls_ca_chain_pem {
            Some(X509::stack_from_pem(mtls_ca_chain_pem)?)
        } else {
            None
        };

        Ok(LoadedListenerTlsConfig {
            cert_chain,
            key,
            mtls_ca_chain,
        })
    }
}

impl From<ListenerConfig> for crate::internal::types::FfiListenerConfig {
    fn from(cfg: ListenerConfig) -> Self {
        crate::internal::types::FfiListenerConfig {
            listen: cfg.listen.into(),
            advertise: cfg.advertise.into(),
            tls: cfg
                .tls
                .map(|tls| crate::internal::types::FfiListenerTlsConfig {
                    cert_chain_pem: tls.cert_chain_pem.into(),
                    key_pem: tls.key_pem.into(),
                    mtls_ca_chain_pem: tls.mtls_ca_chain_pem.map(|chain| chain.into()).into(),
                })
                .into(),
        }
    }
}

impl From<crate::internal::types::FfiListenerConfig> for ListenerConfig {
    fn from(ffi: crate::internal::types::FfiListenerConfig) -> Self {
        ListenerConfig {
            listen: ffi.listen.to_string(),
            advertise: ffi.advertise.to_string(),
            tls: ffi.tls.into_option().map(|tls_ffi| ListenerTlsConfig {
                cert_chain_pem: tls_ffi.cert_chain_pem.into_vec(),
                key_pem: tls_ffi.key_pem.into_vec(),
                mtls_ca_chain_pem: tls_ffi
                    .mtls_ca_chain_pem
                    .into_option()
                    .map(|chain_ffi| chain_ffi.into_vec()),
            }),
        }
    }
}

/// Get listener configuration by calling picodata via FFI.
fn get_listener_config(
    plugin: &str,
    service: &str,
) -> Result<ListenerConfig, FfiListenerConfigError> {
    use crate::util::FfiSafeStr;

    let ffi_config = unsafe {
        crate::internal::ffi::pico_ffi_get_listener_config(
            FfiSafeStr::from(plugin),
            FfiSafeStr::from(service),
        )
    };

    ffi_config.into_result().map(ListenerConfig::from)
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
    /// - TLS is enabled but certificates could not be parsed
    fn bind_with_config(config: &ListenerConfig) -> Result<Self, PicoListenerError> {
        let socket_addr = parse_listen_addr(&config.listen)?;

        let listener = create_coio_listener(socket_addr)?;

        let mut tls_acceptor = None;
        if let Some(tls) = &config.tls {
            let loaded_tls = tls.load()?;

            tls_acceptor = Some(TlsAcceptor::new(&loaded_tls)?)
        };

        let advertise = config.advertise.clone();

        Ok(Self {
            listener,
            tls_acceptor,
            advertise,
        })
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
    /// - TLS is enabled but certificates could not be parsed
    pub fn bind(context: &PicoContext) -> Result<Option<Self>, PicoListenerError> {
        let plugin = context.plugin_name();
        let service = context.service_name();

        match get_listener_config(plugin, service) {
            Ok(config) => Ok(Some(Self::bind_with_config(&config)?)),
            Err(FfiListenerConfigError::Disabled) => Ok(None),
            Err(FfiListenerConfigError::Undefined) => Err(PicoListenerError::Config(format!(
                "configuration for plugin {plugin}.{service} is undefined"
            ))),
            Err(FfiListenerConfigError::Malformed) => Err(PicoListenerError::Config(format!(
                "configuration for plugin {plugin}.{service} is malformed. See picodata logs for more details."
            ))),
        }
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

/// Contains TLS configuration needed to set up TLS for [`PicoListener`] as loaded openssl objects.
///
/// [`PicoListener`]: PicoListener
#[internal]
pub struct LoadedListenerTlsConfig {
    /// Server certificate chain.
    pub cert_chain: Vec<X509>,
    /// Private key to the server certificate.
    pub key: PKey<Private>,
    /// CA certificate chain to verify connecting clients against. None if mTLS is not used.
    pub mtls_ca_chain: Option<Vec<X509>>,
}

impl LoadedListenerTlsConfig {
    /// Encodes the loaded TLS settings as PEM.
    pub fn serialize(&self) -> Result<ListenerTlsConfig, openssl::error::ErrorStack> {
        let mut cert_chain_pem = Vec::new();
        for cert in &self.cert_chain {
            cert_chain_pem.append(&mut cert.to_pem()?);
        }

        let key_pem = self.key.private_key_to_pem_pkcs8()?;

        let mtls_ca_chain_pem = if let Some(ca_chain) = &self.mtls_ca_chain {
            let mut result = Vec::new();
            for cert in ca_chain {
                result.append(&mut cert.to_pem()?);
            }
            Some(result)
        } else {
            None
        };

        Ok(ListenerTlsConfig {
            cert_chain_pem,
            key_pem,
            mtls_ca_chain_pem,
        })
    }
}
