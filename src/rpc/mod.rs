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

use std::collections::HashSet;
use std::fmt::Debug;
use std::io;

use serde::de::DeserializeOwned;

pub mod ddl_apply;
pub mod disable_service;
pub mod enable_all_plugins;
pub mod enable_plugin;
pub mod enable_service;
pub mod expel;
pub mod join;
pub mod load_plugin_dry_run;
pub mod replication;
pub mod sharding;
pub mod snapshot;
pub mod update_instance;

static mut STATIC_PROCS: Option<HashSet<String>> = None;

#[inline(always)]
pub fn init_static_proc_set() {
    let mut map = HashSet::new();
    for proc in ::tarantool::proc::all_procs().iter() {
        map.insert(format!(".{}", proc.name()));
    }

    // Safety: safe as long as only called from tx thread
    unsafe {
        assert!(STATIC_PROCS.is_none());
        STATIC_PROCS = Some(map);
    }
}

#[inline(always)]
pub fn to_static_proc_name(name: &str) -> Option<&'static str> {
    // Safety: safe as long as only called from tx thread
    let name_ref = unsafe {
        STATIC_PROCS
            .as_ref()
            .expect("should be initialized at startup")
            .get(name)?
    };

    Some(name_ref)
}

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
    T: tarantool::tuple::DecodeOwned,
{
    let tuple_len = tuple.len();
    if tuple_len != 1 {
        return Err(tarantool::error::Error::other(format!("failed to decode rpc response: expected a msgpack array of length 1, found array of length {tuple_len}")));
    }
    let res = tuple
        .field(0)?
        .expect("already checked there's enough fields");
    Ok(res)
}

/// Create a one-time iproto connection and send a remote procedure call `request`
/// to the instance specified by `address`.
#[inline(always)]
pub async fn network_call<R>(address: &str, request: &R) -> ::tarantool::Result<R::Response>
where
    R: RequestArgs,
{
    network_call_raw(address, R::PROC_NAME, request).await
}

/// Create a one-time iproto connection and send a request to execute stored
/// procedure `proc` with provided `args` on the instance specified by `address`.
pub async fn network_call_raw<A, R>(
    address: &str,
    proc: &'static str,
    args: &A,
) -> ::tarantool::Result<R>
where
    A: tarantool::tuple::ToTupleBuffer,
    R: tarantool::tuple::DecodeOwned + 'static,
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

    let tuple = client.call(proc, args).await?;
    decode_iproto_return_value(tuple)
}

/// Create a one-time iproto connection and send a remote procedure call `request`
/// to the current raft leader.
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
            const PROC_NAME: &'static str = $crate::proc_name!($proc);
            type Response = $response;
        }
    }
}
