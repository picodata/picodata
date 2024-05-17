use pgwire::error::{ErrorInfo, PgWireError};
use std::{error, io};
use tarantool::error::{BoxError, IntoBoxError};
use thiserror::Error;

pub type PgResult<T> = Result<T, PgError>;

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

    // Server could not encode value into client's format.
    #[error("encoding error: {0}")]
    EncodingError(Box<dyn error::Error>),

    // Server could not decode value recieved from client.
    #[error("decoding error: {0}")]
    DecodingError(#[from] DecodingError),

    // Common error for postges protocol helpers.
    #[error("pgwire error: {0}")]
    PgWireError(#[from] PgWireError),

    // This is picodata's main app error which incapsulates
    // everything else, including sbroad and tarantool errors.
    #[error("picodata error: {0}")]
    PicodataError(#[from] crate::traft::error::Error),

    // Generic IO error (TLS/SSL errors also go here).
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    #[error("{0}")]
    Other(Box<dyn error::Error>),
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

#[derive(Error, Debug)]
pub enum DecodingError {
    #[error("failed to decode int: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("failed to decode float: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),

    #[error("from utf8 error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    #[error("failed to decode bool: {0}")]
    ParseBoolError(#[from] std::str::ParseBoolError),

    #[error("decoding error: {0}")]
    Other(Box<dyn error::Error>),
}

/// Build error info from PgError.
impl PgError {
    pub fn info(&self) -> ErrorInfo {
        ErrorInfo::new(
            "ERROR".to_string(),
            self.code().to_string(),
            self.to_string(),
        )
    }
}

impl PgError {
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
}

impl IntoBoxError for PgError {
    fn into_box_error(self) -> BoxError {
        self.to_string().into_box_error()
    }
}
