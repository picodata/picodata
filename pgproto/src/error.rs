use pgwire::error::{ErrorInfo, PgWireError};
use std::io;
use thiserror::Error;

pub type PgResult<T> = Result<T, PgError>;

/// See https://www.postgresql.org/docs/current/errcodes-appendix.html.
#[derive(Error, Debug)]
pub enum PgError {
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("protocol violation: {0}")]
    ProtocolViolation(String),
    #[error("feature is not supported: {0}")]
    FeatureNotSupported(String),
    #[error("authentication failed for user '{0}'")]
    InvalidPassword(String),
    #[error("pgwire error: {0}")]
    PgWireError(#[from] PgWireError),
}

impl PgError {
    fn code(&self) -> &str {
        use PgError::*;
        match self {
            InternalError(_) => "XX000",
            ProtocolViolation(_) => "08P01",
            FeatureNotSupported(_) => "0A000",
            InvalidPassword(_) => "28P01",
            // TODO: make the code depending on the error kind
            PgWireError(_) => "XX000",
        }
    }
}

/// Build error info from PgError.
pub fn error_info(error: &PgError) -> ErrorInfo {
    ErrorInfo::new(
        "ERROR".to_string(),
        error.code().to_string(),
        error.to_string(),
    )
}

impl From<PgError> for std::io::Error {
    fn from(error: PgError) -> Self {
        io::Error::new(io::ErrorKind::Other, error.to_string())
    }
}
