use crate::info::{InstanceInfo, RaftInfo, VersionInfo};
use crate::instance::StateVariant;
use crate::plugin::{rpc, PluginIdentifier};
use crate::sql::port::{dispatch_dump_mp, PicoPortOwned};
use crate::traft::node;
use crate::traft::op::{Dml, Op};
use crate::util::effective_user_id;
use crate::{cas, sql, traft};
use ::sql::ir::operator::ConflictStrategy;
use ::sql::ir::value::double::Double;
use ::sql::ir::value::{Tuple, Value};
use abi_stable::pmr::{RErr, RNone, ROk, ROption, RResult, RSome};
use abi_stable::std_types::{RDuration, RString, RVec, Tuple2};
use abi_stable::{sabi_extern_fn, RTuple};
use picodata_plugin::background::FfiBackgroundJobCancellationToken;
use picodata_plugin::background::JobCancellationResult;
use picodata_plugin::internal::types;
use picodata_plugin::internal::types::{DmlInner, OpInner};
use picodata_plugin::metrics::FfiMetricsHandler;
use picodata_plugin::plugin::interface::ServiceId;
use picodata_plugin::sql::types::{SqlValue, SqlValueInner};
use picodata_plugin::transport::rpc::client::FfiSafeRpcRequestArguments;
use picodata_plugin::transport::rpc::server::FfiRpcHandler;
use picodata_plugin::util::FfiSafeBytes;
use picodata_plugin::util::FfiSafeStr;
use std::time::Duration;
use std::{mem, slice};
use tarantool::datetime::Datetime;
use tarantool::error::IntoBoxError;
use tarantool::error::TarantoolErrorCode;
use tarantool::ffi::tarantool::BoxTuple;
use tarantool::fiber;
use tarantool::tuple::{TupleBuffer, TupleBuilder};
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

#[no_mangle]
extern "C" fn pico_ffi_cluster_uuid() -> RResult<RString, ()> {
    let node = node::global().expect("node must be already initialized");
    let cluster_uuid = node
        .raft_storage
        .cluster_uuid()
        .expect("storage should never fail");
    ROk(RString::from(cluster_uuid))
}

impl From<StateVariant> for types::StateVariant {
    fn from(variant: StateVariant) -> Self {
        match variant {
            StateVariant::Offline => types::StateVariant::Offline,
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
        info.iproto_advertise,
        info.name.0,
        info.uuid,
        info.replicaset_name.0,
        info.replicaset_uuid,
        info.cluster_name,
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
                conflict_strategy: ConflictStrategy::DoFail,
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
                metainfo: None,
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

fn convert_predicate(predicate: types::Predicate) -> Result<cas::Predicate, traft::error::Error> {
    let index = predicate.index;
    let term = predicate.term;
    let mut ranges = Vec::with_capacity(predicate.ranges.len());
    for ffi_range in predicate.ranges {
        let mut key_min = None;
        let ffi_key_min: Vec<_> = ffi_range.key_min.key.into();
        if !ffi_key_min.is_empty() {
            key_min = Some(TupleBuffer::try_from_vec(ffi_key_min)?);
        }
        let key_min_included = ffi_range.key_min.is_included;

        let mut key_max = None;
        let ffi_key_max: Vec<_> = ffi_range.key_max.key.into();
        if !ffi_key_max.is_empty() {
            key_max = Some(TupleBuffer::try_from_vec(ffi_key_max)?);
        }
        let key_max_included = ffi_range.key_max.is_included;

        let range = cas::Range::from_parts(
            ffi_range.table,
            key_min,
            key_min_included,
            key_max,
            key_max_included,
        );
        ranges.push(range);
    }
    Ok(cas::Predicate {
        index,
        term,
        ranges,
    })
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
    let deadline = fiber::clock().saturating_add(timeout.into());
    let op = Op::from(op);
    let predicate = crate::unwrap_ok_or!(convert_predicate(predicate),
        Err(e) => return error_into_tt_error(e)
    );
    let user_id = effective_user_id();
    let res = (|| -> Result<_, _> {
        let request = cas::Request::new(op, predicate, user_id)?;
        cas::compare_and_swap(&request, true, false, deadline)
    })();
    match res {
        Ok(cas::CasResult::Ok((index, term, _))) => ROk(RSome(Tuple2(index, term))),
        Ok(cas::CasResult::RetriableError(e)) => error_into_tt_error(e),
        // FIXME: this is wrong, just return an error instead
        Err(e) if e.error_code() == TarantoolErrorCode::Timeout as u32 => ROk(RNone),
        Err(e) => error_into_tt_error(e),
    }
}

#[no_mangle]
#[sabi_extern_fn]
extern "C" fn pico_ffi_wait_index(index: u64, timeout: RDuration) -> RResult<ROption<u64>, ()> {
    let node = node::global().expect("node is initialized before plugins");
    match node.wait_index(index, timeout.into()) {
        Ok(idx) => ROk(RSome(idx)),
        // FIXME: this is wrong, just return an error instead
        Err(e) if e.error_code() == TarantoolErrorCode::Timeout as u32 => ROk(RNone),
        Err(e) => error_into_tt_error(e),
    }
}

/// Wrapper over `sql::ir::value::Value`, using for transformation between
/// a ffi and `sbroad` type.
struct SBroadValue(Value);

impl From<SqlValue> for SBroadValue {
    #[allow(deprecated)]
    fn from(value: SqlValue) -> Self {
        match value.into_inner() {
            SqlValueInner::Boolean(b) => SBroadValue(Value::Boolean(b)),
            SqlValueInner::Decimal(raw_dec) => {
                let dec = unsafe {
                    tarantool::decimal::Decimal::from_raw(tarantool::ffi::decimal::decNumber::from(
                        raw_dec,
                    ))
                };
                SBroadValue(Value::from(dec))
            }
            SqlValueInner::Double(f) => SBroadValue(Value::Double(Double { value: f })),
            SqlValueInner::Datetime(dt_raw) => SBroadValue(Value::Datetime(
                Datetime::from_ffi_dt(dt_raw.into()).unwrap(),
            )),
            SqlValueInner::Integer(i) => SBroadValue(Value::Integer(i)),
            SqlValueInner::Null => SBroadValue(Value::Null),
            SqlValueInner::String(s) => SBroadValue(Value::String(s.to_string())),
            SqlValueInner::Unsigned(u) => SBroadValue(Value::Integer(u as i64)),
            SqlValueInner::Array(arr) => SBroadValue(Value::Tuple(Tuple::from(
                arr.into_iter()
                    .map(|v| SBroadValue::from(v).0)
                    .collect::<Vec<_>>(),
            ))),
            SqlValueInner::Uuid(raw_uuid) => SBroadValue(Value::Uuid(Uuid::from_bytes(raw_uuid))),
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
    // SAFETY: caller (picodata_plugin) should provide a pointer to valid utf8 string
    let query = unsafe { std::str::from_utf8_unchecked(slice::from_raw_parts(query, query_len)) };
    let params = params.into_iter().map(|v| SBroadValue::from(v).0).collect();

    let mut port = PicoPortOwned::new();
    if let Err(e) = sql::parse_and_dispatch(query, params, None, None, &mut port) {
        return error_into_tt_error(e);
    }
    let mut builder = TupleBuilder::rust_allocated();
    if let Err(e) = dispatch_dump_mp(&mut builder, port.port_c_mut()) {
        return error_into_tt_error(tarantool::error::Error::IO(e));
    }
    match builder.into_tuple() {
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

/// Register a custom metrics generator which will be invoked when handling an
/// http GET /metrics.
#[no_mangle]
pub extern "C" fn pico_ffi_register_metrics_handler(handler: FfiMetricsHandler) -> i32 {
    if let Err(e) = crate::plugin::metrics::register_metrics_handler(handler) {
        e.set_last();
        return -1;
    }

    0
}

/// Returns error with code [`picodata_plugin::error_code::ErrorCode::NoSuchService`]
/// if there's no service with provided id.
///
/// Otherwise writes into `result` info about how many jobs (if any) didn't
/// finish in the provided `timeout`.
#[no_mangle]
pub extern "C" fn pico_ffi_background_cancel_jobs_by_tag(
    plugin: FfiSafeStr,
    service: FfiSafeStr,
    version: FfiSafeStr,
    job_tag: FfiSafeStr,
    timeout: f64,
    result: *mut JobCancellationResult,
) -> i32 {
    // SAFETY: data outlives this function call
    let plugin = unsafe { plugin.as_str() };
    let service = unsafe { service.as_str() };
    let version = unsafe { version.as_str() };
    let service_id = ServiceId::new(plugin, service, version);

    let job_tag = unsafe { job_tag.as_str() };

    let res = crate::plugin::background::cancel_background_jobs_by_tag(
        service_id,
        job_tag.into(),
        Duration::from_secs_f64(timeout),
    );

    let res = match res {
        Ok(v) => v,
        Err(e) => {
            e.set_last();
            return -1;
        }
    };

    // SAFETY: the caller is responsible for this to be safe
    unsafe { std::ptr::write(result, res) };

    0
}

#[no_mangle]
pub extern "C" fn pico_ffi_background_register_job_cancellation_token(
    plugin: FfiSafeStr,
    service: FfiSafeStr,
    version: FfiSafeStr,
    job_tag: FfiSafeStr,
    token: FfiBackgroundJobCancellationToken,
) -> i32 {
    // SAFETY: data outlives this function call
    let plugin = unsafe { plugin.as_str() };
    let service = unsafe { service.as_str() };
    let version = unsafe { version.as_str() };
    let service_id = ServiceId::new(plugin, service, version);

    let job_tag = unsafe { job_tag.as_str() };

    let res = crate::plugin::background::register_background_job_cancellation_token(
        service_id,
        job_tag.into(),
        token,
    );

    if let Err(e) = res {
        e.set_last();
        return -1;
    }

    0
}

/// Set a user-specified timeout value when shutting down all background jobs
/// in case of service shutdown.
#[no_mangle]
pub extern "C" fn pico_ffi_background_set_jobs_shutdown_timeout(
    plugin: FfiSafeStr,
    service: FfiSafeStr,
    version: FfiSafeStr,
    timeout: f64,
) -> i32 {
    // SAFETY: data outlives this function call
    let plugin = unsafe { plugin.as_str() };
    let service = unsafe { service.as_str() };
    let version = unsafe { version.as_str() };
    let service_id = ServiceId::new(plugin, service, version);
    crate::plugin::background::set_jobs_shutdown_timeout(
        service_id,
        Duration::from_secs_f64(timeout),
    );

    0
}

/// See [`crate::auth::authenticate_with_password`] for more information.
#[no_mangle]
#[sabi_extern_fn]
pub extern "C" fn pico_ffi_authenticate(name: FfiSafeStr, password: FfiSafeBytes) -> i32 {
    // SAFETY: data outlives this function call
    let name = unsafe { name.as_str() };
    let password = unsafe { password.as_bytes() };

    match crate::auth::authenticate_with_password(name, password) {
        Ok(_) => 0,
        Err(e) => {
            e.into_box_error().set_last();
            -1
        }
    }
}
