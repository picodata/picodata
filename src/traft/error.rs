use raft::StorageError;
use rmp_serde::decode::Error as RmpDecodeError;
use rmp_serde::encode::Error as RmpEncodeError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoercionError {
    #[error("unknown entry type ({0})")]
    UnknownEntryType(i32),
    #[error("unknown message type ({0})")]
    UnknownMessageType(i32),
    #[error("invalid msgpack: {0}")]
    MsgpackDecodeError(#[from] RmpDecodeError),
    #[error("failed encoding msgpack: {0}")]
    MsgpackEncodeError(#[from] RmpEncodeError),
    #[error("invalid base64 string")]
    InvalidBase64(#[from] base64::DecodeError),
}

impl From<CoercionError> for StorageError {
    fn from(err: CoercionError) -> StorageError {
        StorageError::Other(Box::new(err))
    }
}

#[derive(Debug, Error)]
pub enum PoolSendError {
    #[error("unknown recipient")]
    UnknownRecipient,
    #[error("message coercion")]
    MessageCoercionError(#[from] CoercionError),
}
