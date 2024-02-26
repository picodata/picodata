use crate::error::PgResult;
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
    pub fn from_pgdata(pgdata: &Path) -> PgResult<Self> {
        // We should use the absolute pathes here, because SslContextBuilder::set_certificate_chain_file
        // fails for relative pathes with an unclear error, represented as an empty error stack.
        let cert = fs::canonicalize(pgdata.join("server.crt"))?;
        let key = fs::canonicalize(pgdata.join("server.key"))?;
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
