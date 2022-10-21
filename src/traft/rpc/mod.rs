use crate::traft::error::Error;
use crate::traft::node::Status;
use crate::traft::{RaftId, RaftTerm};

use ::tarantool::tuple::{DecodeOwned, Encode};

use std::fmt::Debug;

use serde::de::DeserializeOwned;

pub mod replication;
pub mod sharding;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LeaderWithTerm {
    pub leader_id: RaftId,
    pub term: RaftTerm,
}

impl LeaderWithTerm {
    /// Check if requested `self.leader_id` and `self.term`
    /// match the current ones from `status`.
    pub fn check(&self, status: &Status) -> Result<(), Error> {
        let status_leader_id = status.leader_id.ok_or(Error::LeaderUnknown)?;
        if self.leader_id != status_leader_id {
            return Err(Error::LeaderIdMismatch {
                requested: self.leader_id,
                current: status_leader_id,
            });
        }
        if self.term != status.term {
            return Err(Error::TermMismatch {
                requested: self.term,
                current: status.term,
            });
        }
        Ok(())
    }
}

/// Types implementing this trait represent an RPC's (remote procedure call)
/// arguments. This trait contains information about the request.
pub trait Request: Encode + DecodeOwned {
    /// Remote procedure name.
    const PROC_NAME: &'static str;

    /// Describes data returned from a successful RPC request.
    type Response: Encode + DeserializeOwned + Debug + 'static;
}

impl Request for super::JoinRequest {
    const PROC_NAME: &'static str = crate::stringify_cfunc!(super::node::raft_join);
    type Response = super::JoinResponse;
}

impl Request for super::UpdatePeerRequest {
    const PROC_NAME: &'static str = crate::stringify_cfunc!(super::failover::raft_update_peer);
    type Response = super::UpdatePeerResponse;
}

impl Request for super::ExpelRequest {
    const PROC_NAME: &'static str = crate::stringify_cfunc!(super::node::raft_expel);
    type Response = super::ExpelResponse;
}

impl Request for super::SyncRaftRequest {
    const PROC_NAME: &'static str = crate::stringify_cfunc!(super::node::raft_sync_raft);
    type Response = super::SyncRaftResponse;
}
