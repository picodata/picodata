use ::tarantool::tuple::{DecodeOwned, Encode};

use std::fmt::Debug;

use serde::de::DeserializeOwned;

pub mod replication;
pub mod sharding;
pub mod sync;

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
