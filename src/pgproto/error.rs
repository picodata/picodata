use std::io;
use tarantool::error::IntoBoxError;
use thiserror::Error;

/// See <https://www.postgresql.org/docs/current/errcodes-appendix.html>.
#[derive(Debug, Clone, Copy)]
pub enum PgErrorCode {
    DuplicateCursor,
    DuplicatePreparedStatement,
    FeatureNotSupported,
    InternalError,
    InvalidAuthorizationSpecification,
    InvalidBinaryRepresentation,
    InvalidPassword,
    InvalidTextRepresentation,
    IoError,
    ProtocolViolation,
}

impl PgErrorCode {
    #[inline(always)]
    pub fn as_str(&self) -> &'static str {
        match self {
            PgErrorCode::DuplicateCursor => "42P03",
            PgErrorCode::DuplicatePreparedStatement => "42P05",
            PgErrorCode::FeatureNotSupported => "0A000",
            PgErrorCode::InternalError => "XX000",
            PgErrorCode::InvalidAuthorizationSpecification => "28000",
            PgErrorCode::InvalidBinaryRepresentation => "22P03",
            PgErrorCode::InvalidPassword => "28P01",
            PgErrorCode::InvalidTextRepresentation => "22P02",
            PgErrorCode::IoError => "58030",
            PgErrorCode::ProtocolViolation => "08P01",
        }
    }
}

pub type DynError = dyn std::error::Error + Send + Sync;

// Use case: server could not encode a value into client's format.
// To the client it's as meaningful & informative as any other "internal error".
#[derive(Error, Debug)]
#[error("{0}")]
pub struct EncodingError(Box<DynError>);
impl IntoBoxError for EncodingError {}

impl EncodingError {
    #[inline(always)]
    pub fn new(e: impl Into<Box<DynError>>) -> Self {
        Self(e.into())
    }
}

// Use case: server could not decode a value received from client.
// To the client it's as meaningful & informative as any other "internal error".
#[derive(Error, Debug)]
#[error("{0}")]
pub struct DecodingError(Box<DynError>);
impl IntoBoxError for DecodingError {}

impl DecodingError {
    #[inline(always)]
    pub fn new(e: impl Into<Box<DynError>>) -> Self {
        Self(e.into())
    }
}

/// A well-formed error which includes postgres protocol error code.
#[derive(Error, Debug)]
#[error("{1}")]
pub struct PedanticError(PgErrorCode, Box<DynError>);
impl IntoBoxError for PedanticError {}

impl PedanticError {
    pub fn new(code: PgErrorCode, e: impl Into<Box<DynError>>) -> Self {
        Self(code, e.into())
    }
}

pub type PgResult<T> = Result<T, PgError>;

/// Pgproto's main error type.
/// Error message formats are important, preferably Postgres-compatible.
#[derive(Error, Debug)]
pub enum PgError {
    #[error("internal error: {0}")]
    InternalError(String),

    #[error("protocol violation: {0}")]
    ProtocolViolation(String),

    #[error("feature is not supported: {0}")]
    FeatureNotSupported(String),

    #[error("this server requires the client to use ssl")]
    SslRequired,

    // TODO: rename to AuthError
    #[error("authentication failed for user '{0}'")]
    InvalidPassword(String),

    // TODO: merge with InvalidPassword (AuthError)
    // Error message format is compatible with Postgres.
    #[error("authentication failed for user '{0}': LDAP: {1}")]
    LdapAuthError(String, String),

    #[error(transparent)]
    WithExplicitCode(#[from] PedanticError),

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

    // TODO: replace with WithExplicitCode
    // Generic IO error (TLS/SSL errors also go here).
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    // TODO: exterminate this error.
    #[error(transparent)]
    Other(Box<DynError>),
}

impl PgError {
    /// NOTE: new uses of this helper or [`PgError::Other`] are highly discouraged.
    pub fn other<E: Into<Box<DynError>>>(e: E) -> Self {
        Self::Other(e.into())
    }
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

impl From<tarantool::error::BoxError> for PgError {
    #[inline(always)]
    fn from(e: tarantool::error::BoxError) -> Self {
        crate::traft::error::Error::from(e).into()
    }
}

impl IntoBoxError for PgError {}

impl PgError {
    /// Convert the error into a corresponding postgres error code.
    fn code(&self) -> PgErrorCode {
        use PgError::*;
        match self {
            InternalError(_) => PgErrorCode::InternalError,
            ProtocolViolation(_) => PgErrorCode::ProtocolViolation,
            FeatureNotSupported(_) => PgErrorCode::FeatureNotSupported,
            SslRequired => PgErrorCode::InvalidAuthorizationSpecification,
            InvalidPassword(_) => PgErrorCode::InvalidPassword,
            IoError(_) => PgErrorCode::IoError,
            WithExplicitCode(PedanticError(code, _)) => *code,
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
