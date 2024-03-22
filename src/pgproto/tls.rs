use super::error::PgResult;
use openssl::{
    error::ErrorStack,
    ssl::{self, HandshakeError, SslFiletype, SslMethod, SslStream},
};
use std::{fs, io, path::Path, path::PathBuf, rc::Rc};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TlsError {
    #[error("error stack: {0}")]
    ErrorStack(#[from] ErrorStack),

    #[error("setup failure: {0}")]
    SetupFailure(ErrorStack),

    #[error("handshake failure")]
    HandshakeFailure,

    // A helper error that indicates that the error happened with a cert file.
    #[error("cert file error '{0}': {1}")]
    CertFile(PathBuf, std::io::Error),

    // A helper error that indicates that the error happened with a key file.
    #[error("key file error '{0}': {1}")]
    KeyFile(PathBuf, std::io::Error),
}

impl<S> From<HandshakeError<S>> for TlsError {
    fn from(value: HandshakeError<S>) -> Self {
        match value {
            HandshakeError::SetupFailure(stack) => TlsError::SetupFailure(stack),
            _ => TlsError::HandshakeFailure,
        }
    }
}

#[derive(Debug)]
pub struct TlsConfig {
    cert: PathBuf,
    key: PathBuf,
}

impl TlsConfig {
    pub fn from_data_dir(data_dir: &Path) -> PgResult<Self> {
        // We should use the absolute paths here, because SslContextBuilder::set_certificate_chain_file
        // fails for relative paths with an unclear error, represented as an empty error stack.
        let cert = data_dir.join("server.crt");
        let cert = fs::canonicalize(&cert).map_err(|e| TlsError::CertFile(cert, e))?;

        let key = data_dir.join("server.key");
        let key = fs::canonicalize(&key).map_err(|e| TlsError::KeyFile(key, e))?;

        // TODO: Make sure that the file permissions are set to 0640 or 0600.
        // See https://www.postgresql.org/docs/current/ssl-tcp.html#SSL-SETUP for details.
        Ok(Self { key, cert })
    }
}

pub type TlsStream<S> = SslStream<S>;

#[derive(Clone)]
pub struct TlsAcceptor(Rc<ssl::SslAcceptor>);

impl TlsAcceptor {
    pub fn new(config: &TlsConfig) -> Result<Self, TlsError> {
        let mut builder = ssl::SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())?;
        builder.set_certificate_chain_file(&config.cert)?;
        builder.set_private_key_file(&config.key, SslFiletype::PEM)?;
        Ok(Self(builder.build().into()))
    }

    pub fn accept<S>(&self, stream: S) -> Result<TlsStream<S>, TlsError>
    where
        S: io::Read + io::Write,
    {
        self.0.accept(stream).map_err(Into::into)
    }
}
