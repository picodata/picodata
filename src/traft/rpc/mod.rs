use ::tarantool::network::AsClient as _;
use ::tarantool::network::Client;
use ::tarantool::tuple::{DecodeOwned, Encode};

use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::Result;

use std::fmt::Debug;
use std::io;

use serde::de::DeserializeOwned;

pub mod expel;
pub mod join;
pub mod migration;
pub mod replication;
pub mod sharding;
pub mod sync;
pub mod update_instance;

/// Types implementing this trait represent an RPC's (remote procedure call)
/// arguments. This trait contains information about the request.
pub trait Request: Encode + DecodeOwned {
    /// Remote procedure name.
    const PROC_NAME: &'static str;

    /// Describes data returned from a successful RPC request.
    type Response: Encode + DeserializeOwned + Debug + 'static;
}

pub async fn network_call<R>(address: &str, request: &R) -> ::tarantool::Result<R::Response>
where
    R: Request,
{
    // TODO: move address parsing into client
    let (address, port) = address.rsplit_once(':').ok_or_else(|| {
        ::tarantool::error::Error::IO(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid address: {}", address),
        ))
    })?;
    let port: u16 = port.parse().map_err(|err| {
        ::tarantool::error::Error::IO(io::Error::new(io::ErrorKind::InvalidInput, err))
    })?;
    let client = Client::connect(address, port).await?;
    let tuple = client
        .call(R::PROC_NAME, request)
        .await?
        .expect("unexpected result Ok(None)");
    tuple.decode().map(|((res,),)| res)
}

pub async fn network_call_to_leader<R>(request: &R) -> Result<R::Response>
where
    R: Request,
{
    let node = node::global()?;
    let leader_id = node.status().leader_id.ok_or(Error::LeaderUnknown)?;
    let leader_address = node.storage.peer_addresses.try_get(leader_id)?;
    let resp = network_call(&leader_address, request).await?;
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
            $crate::tarantool::fiber_name(stringify!($proc));
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
