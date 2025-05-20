//! Remote procedure calls

use ::tarantool::network::AsClient as _;
use ::tarantool::network::Client;
use ::tarantool::tuple::{DecodeOwned, Encode};

use crate::has_states;
use crate::instance::Instance;
use crate::instance::InstanceName;
use crate::replicaset::Replicaset;
use crate::replicaset::ReplicasetName;
use crate::static_ref;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::{node, ConnectionType, Result};
use crate::util::relay_connection_config;

use std::collections::HashMap;
use std::fmt::Debug;
use std::io;

use serde::de::DeserializeOwned;
use std::collections::HashSet;

pub mod before_online;
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

/// Returns vec of pairs [(instance_name, tier_name), ...]
pub fn replicasets_masters<'a>(
    replicasets: &HashMap<&ReplicasetName, &'a Replicaset>,
    instances: &'a [Instance],
) -> Vec<(&'a InstanceName, &'a String)> {
    let mut masters = Vec::with_capacity(replicasets.len());
    // TODO: invert this loop to improve performance
    // `for instances { replicasets.get() }` instead of `for replicasets { instances.find() }`
    for r in replicasets.values() {
        #[rustfmt::skip]
        let Some(master) = instances.iter().find(|i| i.name == r.current_master_name) else {
            tlog!(
                Warning,
                "couldn't find instance with name {}, which is chosen as master of replicaset {}",
                r.current_master_name,
                r.name,
            );
            // Send them a request anyway just to be safe
            masters.push((&r.current_master_name, &r.tier));
            continue;
        };
        if has_states!(master, Expelled -> *) {
            continue;
        }
        masters.push((&master.name, &master.tier));
    }

    masters
}

#[inline(always)]
pub fn init_static_proc_set() {
    let mut map = HashSet::new();
    for proc in ::tarantool::proc::all_procs().iter() {
        map.insert(format!(".{}", proc.name()));
    }

    // SAFETY: only called from main thread + never mutated after initialization
    unsafe {
        assert!(static_ref!(const STATIC_PROCS).is_none());
        STATIC_PROCS = Some(map);
    }
}

#[inline(always)]
pub fn to_static_proc_name(name: &str) -> Option<&'static str> {
    // SAFETY: only called from main thread + never mutated after initialization
    let name_ref = unsafe {
        static_ref!(const STATIC_PROCS)
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
///
/// The value of `proc_name` must be the same as `R::PROC_NAME`. We require
/// this argument to be passed explicitly just so that it's easier to
/// understand the code and see from what places which stored procedures are
/// being called.
#[inline(always)]
pub async fn network_call<R>(
    address: &str,
    proc_name: &'static str,
    request: &R,
) -> ::tarantool::Result<R::Response>
where
    R: RequestArgs,
{
    debug_assert_eq!(proc_name, R::PROC_NAME);
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

    let config = relay_connection_config();
    let client = Client::connect_with_config(address, port, config).await?;

    let tuple = client.call(proc, args).await?;
    decode_iproto_return_value(tuple)
}

/// Create a one-time iproto connection and send a remote procedure call `request`
/// to the current raft leader.
pub async fn network_call_to_leader<R>(proc_name: &'static str, request: &R) -> Result<R::Response>
where
    R: RequestArgs,
{
    let node = node::global()?;
    let leader_id = node.status().leader_id.ok_or(Error::LeaderUnknown)?;
    let leader_address = node
        .storage
        .peer_addresses
        .try_get(leader_id, &ConnectionType::Iproto)?;
    let resp = network_call(&leader_address, proc_name, request).await?;
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

        service_label: $service_label:expr
    ) => {
        $(#[$proc_meta])*
        #[::tarantool::proc(packed_args)]
        fn $proc($_r: $_request) -> $result {
            let start = tarantool::time::Instant::now_fiber();

            let service_label = $service_label;
            let result: ::std::result::Result<_, $crate::traft::error::Error> = { $($proc_body)* };

            let duration = tarantool::time::Instant::now_fiber().duration_since(start).as_secs_f64();
            $crate::picodata_metrics::rpc_request_duration_seconds().observe(duration);

            match &result {
                Ok(_) => {
                    $crate::picodata_metrics::rpc_request_total()
                        .with_label_values(&[service_label])
                        .inc();
                },
                Err(_) => {
                    $crate::picodata_metrics::rpc_request_errors_total()
                        .with_label_values(&[service_label])
                        .inc();
                },
            }

            result
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
