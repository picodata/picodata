use crate::traft::InstanceId;
use crate::traft::RaftId;
use ::tarantool::tlua::LuaError;
use raft::StorageError;
use rmp_serde::decode::Error as RmpDecodeError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("uninitialized yet")]
    Uninitialized,
    #[error("events system is uninitialized yet")]
    EventsUninitialized,
    #[error("timeout")]
    Timeout,
    #[error("{0}")]
    Raft(#[from] raft::Error),
    #[error("downcast error")]
    DowncastError,
    /// cluster_id of the joining peer mismatches the cluster_id of the cluster
    #[error("cannot join the instance to the cluster: cluster_id mismatch: cluster_id of the instance = {instance_cluster_id:?}, cluster_id of the cluster = {cluster_cluster_id:?}")]
    ClusterIdMismatch {
        instance_cluster_id: String,
        cluster_cluster_id: String,
    },
    /// Peer was requested to configure replication with different replicaset.
    #[error("cannot replicate with different replicaset: expected {instance_rsid:?}, requested {requested_rsid:?}")]
    ReplicasetIdMismatch {
        instance_rsid: String,
        requested_rsid: String,
    },
    #[error("error during execution of lua code: {0}")]
    Lua(#[from] LuaError),
    #[error("{0}")]
    Tarantool(#[from] ::tarantool::error::Error),
    #[error("peer with id {0} not found")]
    NoPeerWithRaftId(RaftId),
    #[error("peer with id {0:?} not found")]
    NoPeerWithInstanceId(InstanceId),
    #[error("other error: {0}")]
    Other(Box<dyn std::error::Error>),
}

impl Error {
    pub fn other<E>(error: E) -> Self
    where
        E: Into<Box<dyn std::error::Error>>,
    {
        Self::Other(error.into())
    }
}

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
