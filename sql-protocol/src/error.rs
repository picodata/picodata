use crate::error::ProtocolError::DecodeError;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum ProtocolError {
    #[error("failed to decode MessagePack data: {0}")]
    DecodeError(String),

    // Protocol-specific errors
    #[error("vtable ({vtable_id}) version mismatch: expected {expected}, got {actual}")]
    VersionMismatch {
        vtable_id: u32,
        expected: u64,
        actual: u64,
    },

    #[error("vtable {vtable_id} is not found")]
    VersionNotFound { vtable_id: u32 },

    #[error("failed to insert tuples into vtable '{vtable}': {reason}")]
    InsertFailed { vtable: String, reason: String },

    #[error("failed to execute operation: {reason}")]
    ExecutionFailed { reason: String },
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
