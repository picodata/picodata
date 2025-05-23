use smol_str::{format_smolstr, SmolStr};
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
#[error("encoding error: {0}")]
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
#[error("decoding error: {1}")]
pub struct DecodingError(PgErrorCode, Box<DynError>);
impl IntoBoxError for DecodingError {}

impl DecodingError {
    pub fn unknown_oid(oid: u32) -> Self {
        Self(
            PgErrorCode::FeatureNotSupported,
            format!("unknown type oid {oid}").into(),
        )
    }

    pub fn unsupported_type(ty: impl std::fmt::Display) -> Self {
        Self(
            PgErrorCode::FeatureNotSupported,
            format!("unsupported type {ty}").into(),
        )
    }

    pub fn bad_utf8(e: impl Into<Box<DynError>>) -> Self {
        Self(PgErrorCode::InvalidTextRepresentation, e.into())
    }

    pub fn bad_lit_of_type(lit: &str, ty: impl std::fmt::Display) -> Self {
        Self(
            PgErrorCode::InvalidTextRepresentation,
            format!("'{lit}' is not a valid {ty}").into(),
        )
    }

    pub fn bad_bin_of_type(ty: impl std::fmt::Display) -> Self {
        Self(
            PgErrorCode::InvalidBinaryRepresentation,
            format!("binary data is not a valid {ty}").into(),
        )
    }
}

/// A well-formed error which includes postgres protocol error code.
#[derive(Error, Debug)]
#[error("{1}")]
pub struct PedanticError(PgErrorCode, Box<DynError>);
impl IntoBoxError for PedanticError {}

impl PedanticError {
    #[inline(always)]
    pub fn new(code: PgErrorCode, e: impl Into<Box<DynError>>) -> Self {
        Self(code, e.into())
    }
}

#[derive(Error, Debug)]
#[error("authentication failed for user '{user}'{}",
    match extra {
        None => format_smolstr!(""),
        Some(extra) => format_smolstr!(": {extra}"),
    }
)]
pub struct AuthError {
    pub user: SmolStr,
    pub extra: Option<SmolStr>,
}

impl AuthError {
    #[inline(always)]
    pub fn for_username(user: impl Into<SmolStr>) -> Self {
        Self {
            user: user.into(),
            extra: None,
        }
    }
}

pub type PgResult<T> = Result<T, PgError>;

/// Pgproto's main error type.
/// Error message formats are important, preferably Postgres-compatible.
#[derive(Error, Debug)]
pub enum PgError {
    #[error("this server requires the client to use ssl")]
    SslRequired,

    #[error("protocol violation: {0}")]
    ProtocolViolation(SmolStr),

    #[error("feature is not supported: {0}")]
    FeatureNotSupported(SmolStr),

    // Error message format is compatible with Postgres.
    #[error(transparent)]
    AuthError(#[from] AuthError),

    // This is picodata's main app error which incapsulates
    // everything else, including sbroad and tarantool errors.
    #[error(transparent)]
    PicodataError(Box<crate::traft::error::Error>),

    #[error(transparent)]
    WithExplicitCode(#[from] PedanticError),

    // Server could not encode a value into client's format.
    // We don't care about any details as long as it's logged.
    #[error(transparent)]
    EncodingError(#[from] EncodingError),

    // Server could not decode a value received from client.
    // We don't care about any details as long as it's logged.
    #[error(transparent)]
    DecodingError(#[from] DecodingError),

    // Generic IO error (TLS/SSL errors also go here).
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    // TODO: exterminate this error.
    #[error(transparent)]
    Other(Box<DynError>),
}

impl PgError {
    /// Enrich [`PgError`] with parameter-related message prefix.
    pub fn cannot_bind_param(self, index: usize) -> Self {
        let code = self.code();
        let message = format!("failed to bind parameter ${index}: {self}");
        PedanticError::new(code, message).into()
    }
}

impl PgError {
    /// NOTE: new uses of this helper or [`PgError::Other`] are highly discouraged.
    pub fn other<E: Into<Box<DynError>>>(e: E) -> Self {
        Self::Other(e.into())
    }
}

// We could use `#[from] crate::traft::error::Error`, but the type's too big.
impl From<crate::traft::error::Error> for PgError {
    #[inline(always)]
    fn from(e: crate::traft::error::Error) -> Self {
        Self::PicodataError(e.into())
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
        match self {
            Self::SslRequired => PgErrorCode::InvalidAuthorizationSpecification,
            Self::ProtocolViolation(_) => PgErrorCode::ProtocolViolation,
            Self::FeatureNotSupported(_) => PgErrorCode::FeatureNotSupported,
            Self::AuthError(_) => PgErrorCode::InvalidPassword,
            Self::WithExplicitCode(PedanticError(code, _)) => *code,
            Self::DecodingError(DecodingError(code, _)) => *code,
            Self::IoError(_) => PgErrorCode::IoError,

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

    pub fn is_sbroad_parsing_error(&self) -> bool {
        use crate::traft::error::Error;
        use sbroad::errors::SbroadError;

        if let Self::PicodataError(e) = self {
            if let Error::Sbroad(SbroadError::ParsingError(..)) = **e {
                return true;
            }
        }

        false
    }
}
