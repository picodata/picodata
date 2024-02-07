//! Remote procedure calls

use ::tarantool::network::AsClient as _;
use ::tarantool::network::Client;
use ::tarantool::network::Config;
use ::tarantool::tuple::{DecodeOwned, Encode};

use crate::pico_service::pico_service_password;
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::Result;

use std::fmt::Debug;
use std::io;

use serde::de::DeserializeOwned;

pub mod ddl_apply;
pub mod expel;
pub mod join;
pub mod replication;
pub mod sharding;
pub mod snapshot;
pub mod update_instance;

/// Types implementing this trait represent an RPC's (remote procedure call)
/// arguments. This trait contains information about the request.
pub trait RequestArgs: Encode + DecodeOwned {
    /// Remote procedure name.
    const PROC_NAME: &'static str;

    /// Describes data returned from a successful RPC request.
    type Response: serde::Serialize + DeserializeOwned + Debug + 'static;
}

#[inline(always)]
pub fn decode_iproto_return_value<T>(tuple: tarantool::tuple::Tuple) -> tarantool::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let (res,) = tuple.decode()?;
    Ok(res)
}

/// Invoke remote procedure call on an instance specified by `address`.
pub async fn network_call<R>(address: &str, request: &R) -> ::tarantool::Result<R::Response>
where
    R: RequestArgs,
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

    let mut config = Config::default();
    config.creds = Some((
        PICO_SERVICE_USER_NAME.into(),
        pico_service_password().into(),
    ));
    let client = Client::connect_with_config(address, port, config).await?;

    let tuple = client.call(R::PROC_NAME, request).await?;
    decode_iproto_return_value(tuple)
}

/// Invoke remote procedure call on a Raft leader.
pub async fn network_call_to_leader<R>(request: &R) -> Result<R::Response>
where
    R: RequestArgs,
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

        impl $crate::rpc::RequestArgs for $request {
            const PROC_NAME: &'static str = $crate::stringify_cfunc!($proc);
            type Response = $response;
        }
    }
}
