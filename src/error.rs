use raft::StorageError;
use rmp_serde::decode::Error as RmpDecodeError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoercionError {
    #[error("unknown entry type \"{0}\"")]
    UnknownEntryType(String),
    #[error("unknown message type \"{0}\"")]
    UnknownMessageType(String),
    #[error("invalid msgpack")]
    InvalidMsgpack(#[from] RmpDecodeError),
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
    #[error("worker is busy")]
    WorkerBusy,
}
