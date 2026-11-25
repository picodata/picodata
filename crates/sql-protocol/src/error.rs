use crate::error::ProtocolError::DecodeError;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum ProtocolError {
    #[error("failed to decode MessagePack data: {0}")]
    DecodeError(String),
}

impl From<rmp::decode::ValueReadError> for ProtocolError {
    fn from(value: rmp::decode::ValueReadError) -> Self {
        DecodeError(value.to_string())
    }
}

impl From<rmp::decode::NumValueReadError> for ProtocolError {
    fn from(value: rmp::decode::NumValueReadError) -> Self {
        DecodeError(value.to_string())
    }
}
