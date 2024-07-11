use std::io;
use thiserror::Error;

/// See <https://www.postgresql.org/docs/current/errcodes-appendix.html>.
#[derive(Debug, Clone, Copy)]
pub enum PgErrorCode {
    DuplicateCursor,
    DuplicatePreparedStatement,
    FeatureNotSupported,
    InternalError,
    InvalidPassword,
    IoError,
    ProtocolViolation,
}

impl PgErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            PgErrorCode::DuplicateCursor => "42P03",
            PgErrorCode::DuplicatePreparedStatement => "42P05",
            PgErrorCode::FeatureNotSupported => "0A000",
            PgErrorCode::InternalError => "XX000",
            PgErrorCode::InvalidPassword => "28P01",
            PgErrorCode::IoError => "58030",
            PgErrorCode::ProtocolViolation => "08P01",
        }
    }
}

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

    #[error("{1}")]
    WithExplicitCode(PgErrorCode, String),

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
    fn code(&self) -> PgErrorCode {
        use PgError::*;
        match self {
            InternalError(_) => PgErrorCode::InternalError,
            ProtocolViolation(_) => PgErrorCode::ProtocolViolation,
            FeatureNotSupported(_) => PgErrorCode::FeatureNotSupported,
            InvalidPassword(_) => PgErrorCode::InvalidPassword,
            IoError(_) => PgErrorCode::InvalidPassword,
            WithExplicitCode(code, _) => *code,
            // TODO: make the code depending on the error kind
            _otherwise => PgErrorCode::InternalError,
        }
    }

    /// Build [`pgwire`]'s error info from [`PgError`].
    pub fn info(&self) -> pgwire::error::ErrorInfo {
        pgwire::error::ErrorInfo::new(
            "ERROR".to_string(),
            self.code().as_str().to_string(),
            self.to_string(),
        )
    }
}
