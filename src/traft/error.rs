use crate::instance::InstanceId;
use crate::traft::{RaftId, RaftTerm};
use ::tarantool::fiber::r#async::timeout;
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
    #[error("downcast error: expected {expected:?}, actual: {actual:?}")]
    DowncastError {
        expected: &'static str,
        actual: &'static str,
    },
    /// cluster_id of the joining instance mismatches the cluster_id of the cluster
    #[error("cannot join the instance to the cluster: cluster_id mismatch: cluster_id of the instance = {instance_cluster_id:?}, cluster_id of the cluster = {cluster_cluster_id:?}")]
    ClusterIdMismatch {
        instance_cluster_id: String,
        cluster_cluster_id: String,
    },
    /// Instance was requested to configure replication with different replicaset.
    #[error("cannot replicate with different replicaset: expected {instance_rsid:?}, requested {requested_rsid:?}")]
    ReplicasetIdMismatch {
        instance_rsid: String,
        requested_rsid: String,
    },
    #[error("operation request from different term {requested}, current term is {current}")]
    TermMismatch {
        requested: RaftTerm,
        current: RaftTerm,
    },
    #[error("not a leader")]
    NotALeader,
    #[error("error during execution of lua code: {0}")]
    Lua(#[from] LuaError),
    #[error("{0}")]
    Tarantool(#[from] ::tarantool::error::Error),
    #[error("instance with id {0} not found")]
    NoInstanceWithRaftId(RaftId),
    #[error("instance with id \"{0}\" not found")]
    NoInstanceWithInstanceId(InstanceId),
    #[error("address of peer with id {0} not found")]
    AddressUnknownForRaftId(RaftId),
    #[error("address of peer with id \"{0}\" not found")]
    AddressUnknownForInstanceId(InstanceId),
    #[error("address of peer is incorrectly formatted: {0}")]
    AddressParseFailure(String),
    #[error("rpc answer is empty")]
    EmptyRpcAnswer,
    #[error("leader is unknown yet")]
    LeaderUnknown,
    #[error("governor has stopped")]
    GovernorStopped,

    #[error("compare-and-swap request failed: {0}")]
    Cas(#[from] crate::traft::rpc::cas::Error),

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

impl<E> From<timeout::Error<E>> for Error
where
    Error: From<E>,
{
    fn from(err: timeout::Error<E>) -> Self {
        match err {
            timeout::Error::Expired => Self::Timeout,
            timeout::Error::Failed(err) => err.into(),
        }
    }
}

impl From<::tarantool::network::Error> for Error {
    fn from(err: ::tarantool::network::Error) -> Self {
        Self::Tarantool(err.into())
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
