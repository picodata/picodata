use crate::traft::RaftId;
use raft::StorageError;
use rmp_serde::decode::Error as RmpDecodeError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoercionError {
    #[error("unknown entry type ({0})")]
    UnknownEntryType(i32),
    #[error("invalid msgpack: {0}")]
    MsgpackDecodeError(#[from] RmpDecodeError),
}

impl From<CoercionError> for StorageError {
    fn from(err: CoercionError) -> StorageError {
        StorageError::Other(Box::new(err))
    }
}

#[derive(Debug, Error)]
pub enum PoolSendError {
    #[error("unknown recipient ({0})")]
    UnknownRecipient(RaftId),
}
