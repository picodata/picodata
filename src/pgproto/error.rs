use std::io;
use thiserror::Error;

// Use case: server could not encode a value into client's format.
// To the client it's as meaningful & informative as any other "internal error".
#[derive(Error, Debug)]
#[error("{0}")]
pub struct EncodingError(Box<dyn std::error::Error + Send + Sync>);

impl EncodingError {
    #[inline(always)]
    pub fn new(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(e.into())
    }
}

// Use case: server could not decode a value received from client.
// To the client it's as meaningful & informative as any other "internal error".
#[derive(Error, Debug)]
#[error("{0}")]
pub struct DecodingError(Box<dyn std::error::Error + Send + Sync>);

impl DecodingError {
    #[inline(always)]
    pub fn new(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(e.into())
    }
}

pub type PgResult<T> = Result<T, PgError>;

/// Pgproto's main error type.
/// See <https://www.postgresql.org/docs/current/errcodes-appendix.html>.
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

    // Server could not encode a value into client's format.
    // We don't care about any details as long as it's logged.
    #[error("encoding error: {0}")]
    EncodingError(#[from] EncodingError),

    // Server could not decode a value received from client.
    // We don't care about any details as long as it's logged.
    #[error("decoding error: {0}")]
    DecodingError(#[from] DecodingError),

    // Common error for postges protocol helpers.
    #[error("pgwire error: {0}")]
    PgWireError(#[from] pgwire::error::PgWireError),

    // This is picodata's main app error which incapsulates
    // everything else, including sbroad and tarantool errors.
    #[error("picodata error: {0}")]
    PicodataError(#[from] crate::traft::error::Error),

    // Generic IO error (TLS/SSL errors also go here).
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    // TODO: exterminate this error.
    #[error("{0}")]
    Other(Box<dyn std::error::Error>),
}

impl From<sbroad::errors::SbroadError> for PgError {
    #[inline(always)]
    fn from(e: sbroad::errors::SbroadError) -> Self {
        crate::traft::error::Error::from(e).into()
    }
}

impl From<tarantool::error::Error> for PgError {
    #[inline(always)]
    fn from(e: tarantool::error::Error) -> Self {
        crate::traft::error::Error::from(e).into()
    }
}

impl tarantool::error::IntoBoxError for PgError {
    fn into_box_error(self) -> tarantool::error::BoxError {
        self.to_string().into_box_error()
    }
}

impl PgError {
    /// Convert the error into a corresponding postgres error code.
    fn code(&self) -> &str {
        use PgError::*;
        match self {
            InternalError(_) => "XX000",
            ProtocolViolation(_) => "08P01",
            FeatureNotSupported(_) => "0A000",
            InvalidPassword(_) => "28P01",
            IoError(_) => "58030",
            // TODO: make the code depending on the error kind
            _otherwise => "XX000",
        }
    }

    /// Build [`pgwire`]'s error info from [`PgError`].
    pub fn info(&self) -> pgwire::error::ErrorInfo {
        pgwire::error::ErrorInfo::new(
            "ERROR".to_string(),
            self.code().to_string(),
            self.to_string(),
        )
    }
}
