use crate::cas::{compare_and_swap, Bound, Range, Request};
use crate::info::{InstanceInfo, RaftInfo, VersionInfo};
use crate::instance::StateVariant;
use crate::plugin::{rpc, PluginIdentifier};
use crate::traft::node;
use crate::traft::op::{Dml, Op};
use crate::util::effective_user_id;
use crate::{cas, sql, traft};
use abi_stable::pmr::{RErr, RNone, ROk, ROption, RResult, RSome};
use abi_stable::std_types::{RDuration, RVec, Tuple2};
use abi_stable::{sabi_extern_fn, RTuple};
use picoplugin::internal::types;
use picoplugin::internal::types::{DmlInner, OpInner};
use picoplugin::sql::types::{SqlValue, SqlValueInner};
use picoplugin::transport::rpc::client::FfiSafeRpcRequestArguments;
use picoplugin::transport::rpc::server::FfiRpcHandler;
use picoplugin::util::FfiSafeBytes;
use sbroad::ir::value::double::Double;
use sbroad::ir::value::{LuaValue, Tuple, Value};
use std::time::Duration;
use std::{mem, slice};
use tarantool::datetime::Datetime;
use tarantool::error::IntoBoxError;
use tarantool::ffi::tarantool::BoxTuple;
use tarantool::tuple::{RawByteBuf, TupleBuffer};
use tarantool::uuid::Uuid;

#[no_mangle]
extern "C" fn pico_ffi_version() -> RTuple!(*const u8, usize) {
    let version = VersionInfo::current().picodata_version;
    Tuple2(version.as_ptr(), version.len())
}

#[no_mangle]
extern "C" fn pico_ffi_rpc_version() -> RTuple!(*const u8, usize) {
    let version = VersionInfo::current().rpc_api_version;
    Tuple2(version.as_ptr(), version.len())
}

impl From<StateVariant> for types::StateVariant {
    fn from(variant: StateVariant) -> Self {
        match variant {
            StateVariant::Offline => types::StateVariant::Offline,
            StateVariant::Replicated => types::StateVariant::Replicated,
            StateVariant::Online => types::StateVariant::Online,
            StateVariant::Expelled => types::StateVariant::Expelled,
        }
    }
}

#[no_mangle]
#[sabi_extern_fn]
extern "C" fn pico_ffi_instance_info() -> RResult<types::InstanceInfo, ()> {
    let node = node::global().expect("node must be already initialized");
    let info = match InstanceInfo::try_get(node, None) {
        Ok(info) => info,
        Err(e) => return error_into_tt_error(e),
    };
    ROk(types::InstanceInfo::new(
        info.raft_id,
        info.advertise_address,
        info.instance_id.0,
        info.instance_uuid,
        info.replicaset_id.0,
        info.replicaset_uuid,
        info.cluster_id,
        types::State::new(
            info.current_state.variant.into(),
            info.current_state.incarnation,
        ),
        types::State::new(
            info.target_state.variant.into(),
            info.target_state.incarnation,
        ),
        info.tier,
    ))
}

impl From<node::RaftState> for types::RaftState {
    #[inline(always)]
    fn from(state: node::RaftState) -> Self {
        match state {
            node::RaftState::Follower => Self::Follower,
            node::RaftState::Candidate => Self::Candidate,
            node::RaftState::Leader => Self::Leader,
            node::RaftState::PreCandidate => Self::PreCandidate,
        }
    }
}

#[no_mangle]
#[sabi_extern_fn]
extern "C" fn pico_ffi_raft_info() -> types::RaftInfo {
    let node = node::global().expect("node must be already initialized");
    let info = RaftInfo::get(node);
    types::RaftInfo::new(
        info.id,
        info.term,
        info.applied,
        info.leader_id,
        info.state.into(),
    )
}

impl From<types::Dml> for Dml {
    fn from(value: types::Dml) -> Self {
        match value.0 {
            DmlInner::Insert {
                table,
                tuple,
                initiator,
            } => Dml::Insert {
                table,
                tuple: unsafe { TupleBuffer::from_vec_unchecked(tuple.to_vec()) },
                initiator,
            },
            DmlInner::Replace {
                table,
                tuple,
                initiator,
            } => Dml::Replace {
                table,
                tuple: unsafe { TupleBuffer::from_vec_unchecked(tuple.to_vec()) },
                initiator,
            },
            DmlInner::Update {
                table,
                key,
                ops,
                initiator,
            } => {
                let ops = ops
                    .into_iter()
                    .map(|op| unsafe { TupleBuffer::from_vec_unchecked(op.to_vec()) })
                    .collect();

                Dml::Update {
                    table,
                    key: unsafe { TupleBuffer::from_vec_unchecked(key.to_vec()) },
                    ops,
                    initiator,
                }
            }
            DmlInner::Delete {
                table,
                key,
                initiator,
            } => Dml::Delete {
                table,
                key: unsafe { TupleBuffer::from_vec_unchecked(key.to_vec()) },
                initiator,
            },
        }
    }
}

impl From<types::Op> for Op {
    fn from(value: types::Op) -> Self {
        match value.0 {
            OpInner::Nop => Op::Nop,
            OpInner::Dml(safe_dml) => Op::Dml(safe_dml.into()),
            OpInner::BatchDml(batch) => Op::BatchDml {
                ops: batch.into_iter().map(Dml::from).collect(),
            },
        }
    }
}

impl From<types::Bound> for Bound {
    fn from(value: types::Bound) -> Self {
        match value.kind {
            types::BoundKind::Included => {
                let raw = value.key.expect("should be Some").to_vec();
                Bound::included(&RawByteBuf(raw))
            }
            types::BoundKind::Excluded => {
                let raw = value.key.expect("should be Some").to_vec();
                Bound::included(&RawByteBuf(raw))
            }
            types::BoundKind::Unbounded => Bound::unbounded(),
        }
    }
}

impl From<types::Predicate> for cas::Predicate {
    fn from(value: types::Predicate) -> Self {
        cas::Predicate {
            index: value.index,
            term: value.term,
            ranges: value
                .ranges
                .into_iter()
                .map(|safe_range| Range {
                    table: safe_range.table,
                    key_min: safe_range.key_min.into(),
                    key_max: safe_range.key_max.into(),
                })
                .collect(),
        }
    }
}

fn error_into_tt_error<T>(source: impl IntoBoxError) -> RResult<T, ()> {
    source.set_last_error();
    RErr(())
}

#[no_mangle]
#[sabi_extern_fn]
extern "C" fn pico_ffi_cas(
    op: types::Op,
    predicate: types::Predicate,
    timeout: RDuration,
) -> RResult<ROption<RTuple!(u64, u64)>, ()> {
    let op = Op::from(op);
    let pred = cas::Predicate::from(predicate);
    let timeout = Duration::from(timeout);
    let user_id = effective_user_id();
    let request = match Request::new(op, pred, user_id) {
        Ok(req) => req,
        Err(e) => {
            return error_into_tt_error(e);
        }
    };

    match compare_and_swap(&request, timeout) {
        Ok((index, term)) => ROk(RSome(Tuple2(index, term))),
        Err(traft::error::Error::Timeout) => ROk(RNone),
        Err(e) => error_into_tt_error(e),
    }
}

#[no_mangle]
#[sabi_extern_fn]
extern "C" fn pico_ffi_wait_index(index: u64, timeout: RDuration) -> RResult<ROption<u64>, ()> {
    let node = node::global().expect("node is initialized before plugins");
    match node.wait_index(index, timeout.into()) {
        Ok(idx) => ROk(RSome(idx)),
        Err(traft::error::Error::Timeout) => ROk(RNone),
        Err(e) => error_into_tt_error(e),
    }
}

/// Wrapper over `sbroad::ir::value::LuaValue`, using for transformation between
/// a ffi and `sbroad` type.
struct SBroadLuaValue(LuaValue);

/// Wrapper over `sbroad::ir::value::Value`, using for transformation between
/// a ffi and `sbroad` type.
struct SBroadValue(Value);

impl From<SqlValue> for SBroadValue {
    fn from(value: SqlValue) -> Self {
        match value.into_inner() {
            SqlValueInner::Boolean(b) => SBroadValue(Value::Boolean(b)),
            SqlValueInner::Decimal(raw_dec) => {
                let dec = unsafe {
                    tarantool::decimal::Decimal::from_raw(tarantool::ffi::decimal::decNumber::from(
                        raw_dec,
                    ))
                };
                SBroadValue(Value::Decimal(dec))
            }
            SqlValueInner::Double(f) => SBroadValue(Value::Double(Double { value: f })),
            SqlValueInner::Datetime(dt_raw) => SBroadValue(Value::Datetime(
                Datetime::from_ffi_dt(dt_raw.into()).unwrap(),
            )),
            SqlValueInner::Integer(i) => SBroadValue(Value::Integer(i)),
            SqlValueInner::Null => SBroadValue(Value::Null),
            SqlValueInner::String(s) => SBroadValue(Value::String(s.to_string())),
            SqlValueInner::Unsigned(u) => SBroadValue(Value::Unsigned(u)),
            SqlValueInner::Array(arr) => SBroadValue(Value::Tuple(Tuple::from(
                arr.into_iter()
                    .map(|v| SBroadValue::from(v).0)
                    .collect::<Vec<_>>(),
            ))),
            SqlValueInner::Uuid(raw_uuid) => SBroadValue(Value::Uuid(Uuid::from_bytes(raw_uuid))),
        }
    }
}

impl From<SqlValue> for SBroadLuaValue {
    fn from(value: SqlValue) -> Self {
        match value.into_inner() {
            SqlValueInner::Boolean(b) => SBroadLuaValue(LuaValue::Boolean(b)),
            SqlValueInner::Decimal(raw_dec) => {
                let dec = unsafe {
                    tarantool::decimal::Decimal::from_raw(tarantool::ffi::decimal::decNumber::from(
                        raw_dec,
                    ))
                };
                SBroadLuaValue(LuaValue::Decimal(dec))
            }
            SqlValueInner::Double(f) => SBroadLuaValue(LuaValue::Double(f)),
            SqlValueInner::Datetime(dt_raw) => SBroadLuaValue(LuaValue::Datetime(
                Datetime::from_ffi_dt(dt_raw.into()).unwrap(),
            )),
            SqlValueInner::Integer(i) => SBroadLuaValue(LuaValue::Integer(i)),
            SqlValueInner::Null => SBroadLuaValue(LuaValue::Null(())),
            SqlValueInner::String(s) => SBroadLuaValue(LuaValue::String(s.to_string())),
            SqlValueInner::Unsigned(u) => SBroadLuaValue(LuaValue::Unsigned(u)),
            SqlValueInner::Array(arr) => SBroadLuaValue(LuaValue::Tuple(Tuple::from(
                arr.into_iter()
                    .map(|v| SBroadValue::from(v).0)
                    .collect::<Vec<_>>(),
            ))),
            SqlValueInner::Uuid(raw_uuid) => {
                SBroadLuaValue(LuaValue::Uuid(Uuid::from_bytes(raw_uuid)))
            }
        }
    }
}

#[no_mangle]
#[sabi_extern_fn]
extern "C" fn pico_ffi_sql_query(
    query: *const u8,
    query_len: usize,
    params: RVec<SqlValue>,
) -> RResult<*mut BoxTuple, ()> {
    // SAFETY: caller (picoplugin) should provide a pointer to valid utf8 string
    let query = unsafe { std::str::from_utf8_unchecked(slice::from_raw_parts(query, query_len)) };
    let params = params
        .into_iter()
        .map(|v| SBroadLuaValue::from(v).0)
        .collect();

    let dispatch_result = sql::sql_dispatch(query, params, None, None);
    match dispatch_result {
        Ok(t) => {
            let ptr = t.as_ptr();
            mem::forget(t);
            ROk(ptr)
        }
        Err(e) => error_into_tt_error(e),
    }
}

#[no_mangle]
extern "C" fn pico_ffi_register_rpc_handler(handler: FfiRpcHandler) -> i32 {
    if let Err(e) = rpc::server::register_rpc_handler(handler) {
        e.set_last();
        return -1;
    }

    return 0;
}

/// Send an RPC request with given `arguments` and block the current fiber until
/// the response is received. `output` will point to data allocated on the
/// region allocator, so the caller is responsible for calling [`box_region_truncate`].
///
/// [`box_region_truncate`]: tarantool::ffi::tarantool::box_region_truncate
#[no_mangle]
extern "C" fn pico_ffi_rpc_request(
    arguments: &FfiSafeRpcRequestArguments,
    timeout: f64,
    output: *mut FfiSafeBytes,
) -> i32 {
    let (plugin, service, version, target, path, input);
    // SAFETY: pointers must be valid for the lifetime of this function
    unsafe {
        plugin = arguments.plugin.as_str();
        service = arguments.service.as_str();
        version = arguments.version.as_str();
        target = &arguments.target;
        path = arguments.path.as_str();
        input = arguments.input.as_bytes();
    };

    let identity = &PluginIdentifier::new(plugin.to_string(), version.to_string());
    match rpc::client::send_rpc_request(identity, service, target, path, input, timeout) {
        Ok(out) => {
            // SAFETY: pointers must be valid for the lifetime of this function
            unsafe { std::ptr::write(output, out.into()) }

            return 0;
        }
        Err(e) => {
            e.into_box_error().set_last();
            return -1;
        }
    }
}
