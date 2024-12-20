use openssl::ssl::SslVerifyMode;
use openssl::x509::store::X509StoreBuilder;
use openssl::x509::X509;
use openssl::{
    error::ErrorStack,
    ssl::{self, HandshakeError, SslFiletype, SslMethod, SslStream},
};

use std::{fs, io, path::Path, path::PathBuf, rc::Rc};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TlsHandshakeError {
    #[error("setup failure: {0}")]
    SetupFailure(ErrorStack),

    #[error("handshake failure")]
    HandshakeFailure,
}

#[derive(Error, Debug)]
pub enum TlsConfigError {
    #[error("error stack: {0}")]
    ErrorStack(#[from] ErrorStack),

    #[error("cert file error '{0}': {1}")]
    CertFile(PathBuf, std::io::Error),

    #[error("key file error '{0}': {1}")]
    KeyFile(PathBuf, std::io::Error),

    #[error("ca file error '{0}': {1}")]
    CaFile(PathBuf, std::io::Error),
}

#[derive(Debug)]
pub struct TlsConfig {
    cert: PathBuf,
    key: PathBuf,
    // Optional CA certificate for peer certificates verification (mTLS).
    ca_cert: Option<PathBuf>,
}

impl TlsConfig {
    pub fn from_instance_dir(instance_dir: &Path) -> Result<Self, TlsConfigError> {
        // We should use the absolute paths here, because SslContextBuilder::set_certificate_chain_file
        // fails for relative paths with an unclear error, represented as an empty error stack.
        let cert = instance_dir.join("server.crt");
        let cert = fs::canonicalize(&cert).map_err(|e| TlsConfigError::CertFile(cert, e))?;

        let key = instance_dir.join("server.key");
        let key = fs::canonicalize(&key).map_err(|e| TlsConfigError::KeyFile(key, e))?;

        let ca_cert = instance_dir.join("ca.crt");
        let ca_cert = match fs::canonicalize(&ca_cert) {
            Ok(path) => Some(path),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => Err(TlsConfigError::CaFile(ca_cert, e))?,
        };

        // TODO: Make sure that the file permissions are set to 0640 or 0600.
        // See https://www.postgresql.org/docs/current/ssl-tcp.html#SSL-SETUP for details.
        Ok(Self { key, cert, ca_cert })
    }
}

pub type TlsStream<S> = SslStream<S>;

#[derive(Clone)]
pub struct TlsAcceptor(Rc<ssl::SslAcceptor>);

impl TlsAcceptor {
    pub fn new(config: &TlsConfig) -> Result<Self, TlsConfigError> {
        let mut builder = ssl::SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())?;
        builder.set_certificate_chain_file(&config.cert)?;
        builder.set_private_key_file(&config.key, SslFiletype::PEM)?;

        if let Some(path) = &config.ca_cert {
            let pem = fs::read(path).map_err(|e| TlsConfigError::CaFile(path.clone(), e))?;
            let mut store_builder = X509StoreBuilder::new()?;
            store_builder.add_cert(X509::from_pem(&pem)?)?;
            builder.set_verify_cert_store(store_builder.build())?;

            let mut verify_mode = SslVerifyMode::PEER;
            verify_mode.insert(SslVerifyMode::FAIL_IF_NO_PEER_CERT);
            builder.set_verify(verify_mode);
        }

        Ok(Self(builder.build().into()))
    }

    pub fn new_from_dir(instance_dir: &Path) -> Result<Self, TlsConfigError> {
        let tls_config = TlsConfig::from_instance_dir(instance_dir)?;
        Self::new(&tls_config)
    }

    pub fn accept<S>(&self, stream: S) -> Result<TlsStream<S>, TlsHandshakeError>
    where
        S: io::Read + io::Write,
    {
        self.0.accept(stream).map_err(|e| match e {
            HandshakeError::SetupFailure(stack) => TlsHandshakeError::SetupFailure(stack),
            _ => TlsHandshakeError::HandshakeFailure,
        })
    }

    pub fn kind(&self) -> &'static str {
        if self.0.context().verify_mode().contains(SslVerifyMode::PEER) {
            "mTLS"
        } else {
            "TLS"
        }
    }
}
