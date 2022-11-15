use ::tarantool::tuple::{DecodeOwned, Encode};

use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::Result;

use std::fmt::{Debug, Display};
use std::net::ToSocketAddrs;
use std::time::Duration;

use serde::de::DeserializeOwned;

pub mod expel;
pub mod migration;
pub mod replication;
pub mod sharding;
pub mod sync;
pub mod update_peer;

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

#[macro_export]
macro_rules! define_rpc_request {
    (
        $(#[$proc_meta:meta])*
        fn $proc:ident($_r:ident: $_request:ty) -> $result:ty {
            $($proc_body:tt)*
        }

        $(#[$req_meta:meta])*
        pub $req_record:tt $request:ident
        $({ $($req_named_fields:tt)* })?
        $(( $($req_unnamed_fields:tt)* );)?

        $(#[$res_meta:meta])*
        pub $res_record:tt  $response:ident
        $({ $($res_named_fields:tt)* })?
        $(( $($res_unnamed_fields:tt)* );)?
    ) => {
        $(#[$proc_meta])*
        #[::tarantool::proc(packed_args)]
        fn $proc($_r: $_request) -> $result {
            $($proc_body)*
        }

        impl ::tarantool::tuple::Encode for $request {}
        #[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
        $(#[$req_meta])*
        pub $req_record $request
        $({ $($req_named_fields)* })?
        $(( $($req_unnamed_fields)* );)?

        impl ::tarantool::tuple::Encode for $response {}
        #[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
        $(#[$res_meta])*
        pub $res_record $response
        $({ $($res_named_fields)* })?
        $(( $($res_unnamed_fields)* );)?

        impl $crate::traft::rpc::Request for $request {
            const PROC_NAME: &'static str = $crate::stringify_cfunc!($proc);
            type Response = $response;
        }
    }
}
