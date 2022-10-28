use ::tarantool::tuple::{DecodeOwned, Encode};

use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::Result;

use std::fmt::{Debug, Display};
use std::net::ToSocketAddrs;
use std::time::Duration;

use serde::de::DeserializeOwned;

pub mod expel;
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

#[inline(always)]
pub fn net_box_call<R>(
    address: impl ToSocketAddrs + Display,
    request: &R,
    timeout: Duration,
) -> ::tarantool::Result<R::Response>
where
    R: Request,
{
    crate::tarantool::net_box_call(&address, R::PROC_NAME, request, timeout)
}

#[inline]
pub fn net_box_call_to_leader<R>(request: &R, timeout: Duration) -> Result<R::Response>
where
    R: Request,
{
    let node = node::global()?;
    let leader_id = node.status().leader_id.ok_or(Error::LeaderUnknown)?;
    let leader = node.storage.peers.get(&leader_id)?;
    let resp = net_box_call(&leader.peer_address, request, timeout)?;
    Ok(resp)
}
