use crate::cas::{compare_and_swap, Bound, Range, Request};
use crate::info::{InstanceInfo, RaftInfo, VersionInfo};
use crate::instance::GradeVariant;
use crate::traft::node;
use crate::traft::op::{Dml, Op};
use crate::util::effective_user_id;
use crate::{cas, traft};
use abi_stable::pmr::{RErr, RNone, ROk, ROption, RResult, RSome};
use abi_stable::std_types::{RDuration, Tuple2};
use abi_stable::{sabi_extern_fn, RTuple};
use picoplugin::internal::types;
use picoplugin::internal::types::DmlInner;
use picoplugin::internal::types::OpInner;
use std::time::Duration;
use tarantool::error::IntoBoxError;
use tarantool::tuple::{RawByteBuf, TupleBuffer};

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

impl From<GradeVariant> for types::GradeVariant {
    fn from(variant: GradeVariant) -> Self {
        match variant {
            GradeVariant::Offline => types::GradeVariant::Offline,
            GradeVariant::Replicated => types::GradeVariant::Replicated,
            GradeVariant::Online => types::GradeVariant::Online,
            GradeVariant::Expelled => types::GradeVariant::Expelled,
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
        types::Grade::new(
            info.current_grade.variant.into(),
            info.current_grade.incarnation,
        ),
        types::Grade::new(
            info.target_grade.variant.into(),
            info.target_grade.incarnation,
        ),
        info.tier,
    ))
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
        info.state.to_string(),
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
