use super::execute::{
    dql_execute_locally_from_plan, filtered_delete_from_plan, full_delete_execute,
    materialize_with_dql, materialized_update_from_plan, shared_update_from_plan,
    tuple_insert_from_plan,
};
use super::port::PicoPortOwned;
use super::storage::{FullDeleteInfo, StorageRuntime};
use crate::catalog_cache::CatalogCacheRef;
use crate::config::{DEFAULT_SQL_PREEMPTION, DYNAMIC_CONFIG};
use crate::metrics::{observe_sql_local_query_duration, record_sql_local_query_total};
use crate::schema::ADMIN_ID;
use crate::sql::lua::{
    bucket_into_rs, escape_bytes, lua_custom_plan_dispatch, lua_decode_rs_ibufs,
    lua_single_plan_dispatch, reference_add, reference_del, reference_use, sched_ref_end,
    sql_lua_state, IbufTable,
};
use crate::sql::router::catalog_cache;
use crate::traft::node;
use crate::util::MpValueAsJson;
use crate::vshard;
use ahash::AHashMap;
use rmp::decode::{read_array_len, read_bool, read_int};
use rmp::encode::write_uint;
use smallvec::SmallVec;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use sql::errors::{Action, Entity, SbroadError};
use sql::executor::engine::helpers::vshard::{prepare_rs_to_ir_map, prepare_single_rs_ir_plan};
use sql::executor::engine::helpers::{
    init_delete_tuple_builder, init_insert_tuple_builder, init_local_update_tuple_builder,
    init_sharded_update_tuple_builder, vtable_columns,
};
use sql::executor::engine::protocol::{
    build_dql_protocol_for_top, DeleteCoreData, DqlProtocol, ExecutionCacheMissData,
    FilteredDeleteData, FullDeleteData, InsertCoreData, LocalInsertData, LocalUpdateData,
    SharedUpdateData, TupleInsertData, UpdateCoreData,
};
use sql::executor::engine::{BlockExecData, QueryCache, Vshard};
use sql::executor::ir::{ExecutionPlan, QueryType};
use sql::executor::protocol::SchemaInfo;
use sql::executor::result::{metadata_type_name, MetadataColumn};
use sql::executor::vtable::VirtualTable;
use sql::executor::Port;
use sql::ir::bucket::{BucketSet, Buckets};
use sql::ir::helpers::RepeatableState;
use sql::ir::node::relational::Relational;
use sql::ir::node::{Delete, Insert, Motion, NodeId, Update};
use sql::ir::operator::UpdateStrategy;
use sql::ir::options::{Options, ReadPreference};
use sql::ir::transformation::redistribution::MotionPolicy;
use sql::ir::Plan;
use sql::utils::ByteCounter;
use sql_protocol::block::write_block_packet;
use sql_protocol::decode::{execute_read_response, SqlExecute, TupleIter};
use sql_protocol::dml::delete::{write_delete_filtered_packet, write_delete_full_packet};
use sql_protocol::dml::insert::{write_insert_materialized_packet, write_insert_packet};
use sql_protocol::dml::update::{write_update_packet, write_update_shared_key_packet};
use sql_protocol::dql::write_dql_packet;
use sql_protocol::dql_encoder::{DQLDataSource, DQLOptions};
use sql_protocol::encode::write_metadata;
use std::borrow::Cow;
use std::cell::{Cell, LazyCell};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Cursor, Error as IoError, Result as IoResult};
use std::rc::{Rc, Weak};
use std::time::Duration;
use tarantool::fiber::Mutex;
use tarantool::session::with_su;
use tarantool::time::Instant;
use tarantool::tuple::{Tuple, TupleBuilder};

pub type SqlResult<T> = Result<T, SbroadError>;

/// CacheMissResponse lazy initialize ExecutionCacheMissData by calling the closure with DqlProtocol.
type CacheMissResponse = LazyCell<
    Result<ExecutionCacheMissData, SbroadError>,
    Box<dyn FnOnce() -> Result<ExecutionCacheMissData, SbroadError>>,
>;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct MetaKey(SmolStr, u64);

impl MetaKey {
    fn new(request_id: &str, plan_id: u64) -> MetaKey {
        MetaKey(request_id.into(), plan_id)
    }
}

type MetadataHashMap = HashMap<MetaKey, Weak<CacheMissResponse>>;
const QUERY_METADATA_CAPACITY: usize = 100;

thread_local! {
    static QUERY_METADATA: Rc<Mutex<MetadataHashMap>> = Rc::new(Mutex::new(HashMap::with_capacity(QUERY_METADATA_CAPACITY)));
}

pub struct CacheGuard {
    storage: Rc<Mutex<MetadataHashMap>>,
    key: MetaKey,
    handle: Rc<CacheMissResponse>,
}

impl Drop for CacheGuard {
    fn drop(&mut self) {
        let mut cache = self.storage.lock();
        if Rc::strong_count(&self.handle) == 1 {
            cache.remove(&self.key);
        }
    }
}

/// Storage for data to handle cache misses for DQL queries.
struct QueryMetaStorage {
    query_meta: Rc<Mutex<MetadataHashMap>>,
}

impl QueryMetaStorage {
    fn new() -> Self {
        Self {
            query_meta: QUERY_METADATA.with(|cache| cache.clone()),
        }
    }
    fn get(&self, request_id: &str, plan_id: u64) -> Result<Rc<CacheMissResponse>, SbroadError> {
        let key = MetaKey::new(request_id, plan_id);
        let mut metadata = self.query_meta.lock();
        let Some(value) = metadata.get(&key) else {
            return Err(SbroadError::NotFound(
                Entity::Query,
                format_smolstr!("for request_id {} with plan_id {}", request_id, plan_id),
            ));
        };
        let Some(rc_value) = value.upgrade() else {
            // Stale entry — clean up and report not found
            metadata.remove(&key);
            return Err(SbroadError::NotFound(
                Entity::Query,
                format_smolstr!("for request_id {} with plan_id {}", request_id, plan_id),
            ));
        };

        LazyCell::force(&rc_value);

        return Ok(rc_value.clone());
    }

    fn put(&self, plan: DqlProtocol) -> Result<CacheGuard, SbroadError> {
        let key = MetaKey::new(plan.get_request_id(), plan.get_plan_id());
        let mut metadata = self.query_meta.lock();
        let handle = match metadata.entry(key.clone()) {
            Entry::Vacant(e) => {
                let rc = Rc::new(CacheMissResponse::new(Box::new(move || {
                    ExecutionCacheMissData::try_from(&plan)
                })));
                e.insert(Rc::downgrade(&rc));
                rc
            }
            Entry::Occupied(mut e) => match e.get().upgrade() {
                Some(rc) => rc,
                None => {
                    let rc = Rc::new(CacheMissResponse::new(Box::new(move || {
                        ExecutionCacheMissData::try_from(&plan)
                    })));
                    e.insert(Rc::downgrade(&rc));
                    rc
                }
            },
        };

        Ok(CacheGuard {
            storage: self.query_meta.clone(),
            key,
            handle,
        })
    }
}

fn encode_tuple_with_reservation(
    byte_len: impl FnOnce(&mut ByteCounter) -> SqlResult<()>,
    write: impl FnOnce(&mut TupleBuilder) -> SqlResult<()>,
) -> SqlResult<Tuple> {
    let mut bc = ByteCounter::default();
    byte_len(&mut bc)?;

    let mut tb = TupleBuilder::rust_allocated();
    tb.reserve(bc.bytes());
    write(&mut tb)?;

    tb.into_tuple()
        .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))
}

fn local_sender_id() -> SqlResult<u64> {
    node::global()
        .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
        .map(|node| node.raft_id)
}

fn encode_dql_tuple(data_source: &impl DQLDataSource) -> SqlResult<Tuple> {
    encode_tuple_with_reservation(
        |bc| {
            write_dql_packet(bc, data_source)
                .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
        },
        |tb| {
            write_dql_packet(tb, data_source)
                .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
        },
    )
}

fn put_query_meta(plan: Option<DqlProtocol>) -> SqlResult<Option<CacheGuard>> {
    let Some(plan) = plan else {
        return Ok(None);
    };
    let query_meta_storage = QueryMetaStorage::new();
    Ok(Some(query_meta_storage.put(plan)?))
}

enum DmlRequest {
    DeleteFiltered {
        core: DeleteCoreData,
        types: Vec<sql::ir::relation::Column>,
        pattern: sql::executor::engine::helpers::TupleBuilderPattern,
        plan: DqlProtocol,
    },
    DeleteFull {
        core: DeleteCoreData,
        plan_id: u64,
        options: DQLOptions,
    },
    Insert {
        core: InsertCoreData,
        vtable: Rc<VirtualTable>,
        pattern: sql::executor::engine::helpers::TupleBuilderPattern,
    },
    InsertLocal {
        core: InsertCoreData,
        types: Vec<sql::ir::relation::Column>,
        pattern: sql::executor::engine::helpers::TupleBuilderPattern,
        plan: DqlProtocol,
    },
    UpdateShared {
        core: UpdateCoreData,
        delete_tuple_len: usize,
        vtable: Rc<VirtualTable>,
        pattern: sql::executor::engine::helpers::TupleBuilderPattern,
    },
    UpdateLocal {
        core: UpdateCoreData,
        types: Vec<sql::ir::relation::Column>,
        pattern: sql::executor::engine::helpers::TupleBuilderPattern,
        plan: DqlProtocol,
    },
}

impl DmlRequest {
    fn into_message(self) -> SqlResult<(Tuple, Option<DqlProtocol>)> {
        match self {
            Self::DeleteFiltered {
                core,
                types,
                pattern,
                plan,
            } => {
                let data = FilteredDeleteData::new(core, types, pattern, &plan);
                let tuple = encode_tuple_with_reservation(
                    |bc| {
                        write_delete_filtered_packet(bc, &data)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
                    },
                    |tb| {
                        write_delete_filtered_packet(tb, &data)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
                    },
                )?;
                Ok((tuple, Some(plan)))
            }
            Self::DeleteFull {
                core,
                plan_id,
                options,
            } => {
                let data = FullDeleteData::new(core, plan_id, options);
                let tuple = encode_tuple_with_reservation(
                    |bc| {
                        write_delete_full_packet(bc, &data)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
                    },
                    |tb| {
                        write_delete_full_packet(tb, &data)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
                    },
                )?;
                Ok((tuple, None))
            }
            Self::Insert {
                core,
                vtable,
                pattern,
            } => {
                let data = TupleInsertData::new(core, vtable, pattern);
                let tuple = encode_tuple_with_reservation(
                    |bc| {
                        write_insert_packet(bc, &data)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
                    },
                    |tb| {
                        write_insert_packet(tb, &data)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
                    },
                )?;
                Ok((tuple, None))
            }
            Self::InsertLocal {
                core,
                types,
                pattern,
                plan,
            } => {
                let data = LocalInsertData::new(core, types, pattern, &plan);
                let tuple = encode_tuple_with_reservation(
                    |bc| {
                        write_insert_materialized_packet(bc, &data)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
                    },
                    |tb| {
                        write_insert_materialized_packet(tb, &data)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
                    },
                )?;
                Ok((tuple, Some(plan)))
            }
            Self::UpdateShared {
                core,
                delete_tuple_len,
                vtable,
                pattern,
            } => {
                let data = SharedUpdateData::new(core, delete_tuple_len, vtable, pattern);
                let tuple = encode_tuple_with_reservation(
                    |bc| {
                        write_update_shared_key_packet(bc, &data)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
                    },
                    |tb| {
                        write_update_shared_key_packet(tb, &data)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
                    },
                )?;
                Ok((tuple, None))
            }
            Self::UpdateLocal {
                core,
                types,
                pattern,
                plan,
            } => {
                let data = LocalUpdateData::new(core, types, pattern, &plan);
                let tuple = encode_tuple_with_reservation(
                    |bc| {
                        write_update_packet(bc, &data)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
                    },
                    |tb| {
                        write_update_packet(tb, &data)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
                    },
                )?;
                Ok((tuple, Some(plan)))
            }
        }
    }

    fn execute_locally(self, runtime: &StorageRuntime) -> SqlResult<u64> {
        match self {
            Self::DeleteFiltered {
                core,
                types,
                pattern,
                plan,
            } => filtered_delete_from_plan(
                runtime,
                core.space_id,
                core.space_version,
                &types,
                &pattern,
                &plan,
            ),
            Self::DeleteFull { .. } => Err(SbroadError::DispatchError(
                "local delete full fast path is unsupported".into(),
            )),
            Self::Insert {
                core,
                vtable,
                pattern,
            } => tuple_insert_from_plan(
                runtime,
                core.space_id,
                core.space_version,
                core.conflict_policy,
                &pattern,
                vtable.as_ref(),
            ),
            Self::InsertLocal {
                core,
                types,
                pattern,
                plan,
            } => {
                let column_types = types
                    .iter()
                    .map(|column| column.r#type.into())
                    .collect::<Vec<_>>();
                let vtable = materialize_with_dql(runtime, &column_types, &pattern, |pico_port| {
                    dql_execute_locally_from_plan(runtime, &plan, pico_port)
                })?;
                tuple_insert_from_plan(
                    runtime,
                    core.space_id,
                    core.space_version,
                    core.conflict_policy,
                    &pattern,
                    &vtable,
                )
            }
            Self::UpdateShared {
                core,
                delete_tuple_len,
                vtable,
                pattern,
            } => shared_update_from_plan(
                runtime,
                core.space_id,
                core.space_version,
                delete_tuple_len,
                &pattern,
                vtable.as_ref(),
            ),
            Self::UpdateLocal {
                core,
                types,
                pattern,
                plan,
            } => materialized_update_from_plan(
                runtime,
                core.space_id,
                core.space_version,
                &types,
                &pattern,
                &plan,
            ),
        }
    }
}

pub(crate) fn single_plan_dispatch<'p>(
    port: &mut impl Port<'p>,
    ex_plan: Rc<ExecutionPlan>,
    top_id: NodeId,
    buckets: &Buckets,
    timeout: Duration,
    tier: Option<&str>,
) -> SqlResult<()> {
    let deadline = Instant::now_fiber().saturating_add(timeout);
    let replicasets = replicasets_from_buckets(buckets, tier, deadline)?;
    let timeout = deadline.duration_since(Instant::now_fiber());
    let query_type = query_type_for_top(&ex_plan, top_id)?;
    match &query_type {
        QueryType::DQL => {
            let max_rows = ex_plan.get_sql_motion_row_max();
            let read_preference = effective_read_preference(&ex_plan)?;
            if should_single_rs_dispatch_locally(buckets, tier, &replicasets, read_preference)? {
                execute_dql_locally(port, ex_plan, top_id, buckets, timeout, read_preference)?;
                return Ok(());
            }

            port_write_metadata_for_top(port, &ex_plan, top_id)?;
            // For read_preference = 'replica' | 'any', we follow an optimistic scenario.
            // Plan will be routed to RO replica, so references from vshard will not help.
            let is_on_leader =
                ex_plan.get_ir_plan().effective_options.read_preference == ReadPreference::Leader;
            let do_two_step = replicasets.len() != 1 && is_on_leader;
            single_plan_dispatch_dql(
                port,
                ex_plan,
                top_id,
                &replicasets,
                max_rows,
                timeout,
                tier,
                read_preference,
                do_two_step,
            )?
        }
        QueryType::DML => {
            if should_single_rs_dispatch_locally(buckets, tier, &replicasets, "leader")? {
                execute_dml_locally(port, ex_plan, top_id, timeout)?;
                return Ok(());
            }

            single_plan_dispatch_dml(port, ex_plan, top_id, &replicasets, timeout, tier)?
        }
    };
    Ok(())
}

pub(crate) fn custom_plan_dispatch<'p>(
    port: &mut impl Port<'p>,
    runtime: &impl Vshard,
    ex_plan: Rc<ExecutionPlan>,
    top_id: NodeId,
    buckets: &Buckets,
    timeout: Duration,
    tier: Option<&str>,
) -> SqlResult<()> {
    let deadline = Instant::now_fiber().saturating_add(timeout);
    let rs_buckets = buckets_by_replicasets(buckets, runtime.bucket_count(), tier, timeout)?;
    let timeout = deadline.duration_since(Instant::now_fiber());
    if rs_buckets.is_empty() {
        return Err(SbroadError::DispatchError(
            "No replicasets found for the given buckets".into(),
        ));
    }
    let query_type = query_type_for_top(&ex_plan, top_id)?;
    match &query_type {
        QueryType::DQL => {
            let max_rows = ex_plan.get_sql_motion_row_max();
            let read_preference = effective_read_preference(&ex_plan)?;
            if should_single_rs_buckets_dispatch_locally(
                buckets,
                tier,
                &rs_buckets,
                read_preference,
            )? {
                let local_plan = extract_single_rs_plan(rs_buckets, ex_plan, top_id)?;
                execute_dql_locally(port, local_plan, top_id, buckets, timeout, read_preference)?;
                return Ok(());
            }

            // All custom plans must return the same metadata,
            // so we can use the original plan to write it to the port.
            port_write_metadata_for_top(port, &ex_plan, top_id)?;
            // For read_preference = 'replica' | 'any', we follow an optimistic scenario.
            // Plan will be routed to RO replica, so references from vshard will not help.
            let is_on_leader =
                ex_plan.get_ir_plan().effective_options.read_preference == ReadPreference::Leader;
            let do_two_step = rs_buckets.len() != 1 && is_on_leader;
            custom_plan_dispatch_dql(
                port,
                ex_plan,
                top_id,
                rs_buckets,
                max_rows,
                timeout,
                tier,
                read_preference,
                do_two_step,
            )?;
        }
        QueryType::DML => {
            if should_single_rs_buckets_dispatch_locally(buckets, tier, &rs_buckets, "leader")? {
                let local_plan = extract_single_rs_plan(rs_buckets, ex_plan, top_id)?;
                execute_dml_locally(port, local_plan, top_id, timeout)?;
                return Ok(());
            }

            custom_plan_dispatch_dml(port, ex_plan, top_id, rs_buckets, timeout, tier)?;
        }
    };
    Ok(())
}

pub(crate) fn block_dispatch<'p>(
    port: &mut impl Port<'p>,
    metadata: Vec<MetadataColumn>,
    write_metadata: bool,
    block: BlockExecData,
    buckets: &Buckets,
    request_id: &str,
    timeout: Duration,
    tier: Option<&str>,
) -> Result<(), SbroadError> {
    let deadline = Instant::now_fiber().saturating_add(timeout);
    match buckets {
        Buckets::All => Err(SbroadError::other(
            "cannot execute transaction on all buckets",
        )),
        Buckets::Any => execute_block_locally_for_dispatch(port, &metadata, write_metadata, block),
        Buckets::Filtered(_) => {
            let replicasets = replicasets_from_buckets(buckets, tier, deadline)?;
            let timeout = deadline.duration_since(Instant::now_fiber());

            if should_single_rs_dispatch_locally(buckets, tier, &replicasets, "leader")? {
                return with_local_bucket_ref(timeout, "leader", || {
                    execute_block_locally_for_dispatch(port, &metadata, write_metadata, block)
                });
            }
            if write_metadata {
                port_write_block_metadata(port, &metadata)?;
            }
            let columns = metadata.len();
            let data = rmp_serde::encode::to_vec(&block).map_err(|e| {
                SbroadError::DispatchError(format_smolstr!(
                    "failed to encode block mesasge payload: {e}"
                ))
            })?;

            let mut tb = TupleBuilder::rust_allocated();
            write_block_packet(&mut tb, request_id, &data)
                .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
            let tuple = tb
                .into_tuple()
                .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;

            let lua = sql_lua_state();
            let lua_table = lua_single_plan_dispatch(
                &lua,
                &tuple,
                &replicasets,
                timeout,
                tier,
                "leader",
                false,
            )
            .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;

            // Block cannot have any motions.
            let motion_max_rows = 0;

            dql_execution_result_process(
                port,
                lua_table,
                replicasets.len(),
                columns as _,
                motion_max_rows,
            )?;

            Ok(())
        }
    }
}

fn execute_block_locally_for_dispatch<'p>(
    port: &mut impl Port<'p>,
    metadata: &[MetadataColumn],
    write_metadata: bool,
    block: BlockExecData,
) -> SqlResult<()> {
    let runtime = StorageRuntime::new();
    if write_metadata {
        port_write_block_metadata(port, metadata)?;
    }
    runtime.execute_block(block, port)
}

fn port_write_block_metadata<'p>(
    port: &mut impl Port<'p>,
    metadata: &[MetadataColumn],
) -> SqlResult<()> {
    write_metadata(
        port,
        metadata
            .iter()
            .map(|c| (c.name.as_str(), c.r#type.as_str())),
        metadata.len() as _,
    )
    .map_err(|e| {
        SbroadError::FailedTo(
            Action::Serialize,
            Some(Entity::Metadata),
            format_smolstr!("{e:?}"),
        )
    })?;
    Ok(())
}

pub(crate) fn port_write_metadata_for_top<'p>(
    port: &mut impl Port<'p>,
    ex_plan: &ExecutionPlan,
    top_id: NodeId,
) -> SqlResult<()> {
    let ir = ex_plan.get_ir_plan();
    if ir.get_relation_node(top_id)?.is_dml() || ir.is_raw_explain() {
        return Err(SbroadError::FailedTo(
            Action::Get,
            Some(Entity::Query),
            "Failed to get metadata from execution plan".into(),
        ));
    }
    if !ex_plan.must_write_result_metadata() {
        return Ok(());
    }
    // `write_metadata` walks the columns twice and the column types are
    // derived lazily, so keep them in a buffer. The buffer is on the stack
    // for up to eight columns, the names borrow from the plan and the type
    // names are inline, so a usual query writes its metadata without an
    // allocation.
    let mut metadata: SmallVec<[(Cow<'_, str>, SmolStr); 8]> = SmallVec::new();
    for column in ir.columns_of(top_id)? {
        let column = column?;
        metadata.push((column.name, metadata_type_name(column.r#type)));
    }
    write_metadata(
        port,
        metadata
            .iter()
            .map(|(name, ty)| (name.as_ref(), ty.as_str())),
        metadata.len() as u32,
    )
    .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;
    Ok(())
}

/// Get all unlogged tables and check if any of them are present in the plan tables.
fn plan_has_unlogged_tables(plan: &Plan) -> SqlResult<bool> {
    let catalog = catalog_cache()?;
    let res = (plan.relations.tables.keys())
        .filter_map(|name| catalog.table_by_name(name))
        .any(|table| table.is_unlogged());
    Ok(res)
}

/// If `sql_preemption` is enabled, `read_preference` should be equal to `leader`.
///
/// If the plan has unlogged tables `read_preference` should be equal to `leader`.
fn effective_read_preference(ex_plan: &ExecutionPlan) -> SqlResult<&'static str> {
    let plan = ex_plan.get_ir_plan();
    let read_preference = plan.effective_options.read_preference;

    if read_preference == ReadPreference::Leader {
        return Ok(read_preference.as_str());
    }

    if plan_has_unlogged_tables(plan)? {
        return Err(SbroadError::Invalid(
            Entity::Option,
            Some("read_preference must be set to 'leader' when querying unlogged tables".into()),
        ));
    };

    let sql_preemption = DYNAMIC_CONFIG
        .sql_preemption
        .try_current_value()
        .unwrap_or(DEFAULT_SQL_PREEMPTION);

    if sql_preemption {
        return Err(SbroadError::Invalid(
            Entity::Option,
            Some("read_preference must be set to 'leader' when sql_preemption is enabled".into()),
        ));
    }

    Ok(read_preference.as_str())
}

fn should_dispatch_locally(
    tier: Option<&str>,
    replicaset_uuid: &str,
    read_preference: &str,
) -> SqlResult<bool> {
    let node = node::global().map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
    if tier.is_some_and(|tier_name| tier_name != node.catalog_cache.my_tier_name()) {
        return Ok(false);
    }

    if replicaset_uuid != node.catalog_cache.my_replicaset_uuid() {
        return Ok(false);
    }

    let is_replica = node.is_readonly();
    let topology_ref = node.catalog_cache.get();
    Ok(local_dispatch_matches_read_preference(
        &topology_ref,
        is_replica,
        replicaset_uuid,
        read_preference,
    ))
}

#[inline]
fn should_all_buckets_dispatch_locally(
    tier: Option<&str>,
    read_preference: &str,
) -> SqlResult<bool> {
    let node = node::global().map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
    let target_tier = tier.unwrap_or_else(|| node.catalog_cache.my_tier_name());
    if target_tier != node.catalog_cache.my_tier_name() {
        return Ok(false);
    }

    // NOTE: catalog_cache may be slightly stale relative to the actual vshard
    // bucket distribution. During rebalancing (e.g. cluster just scaled from 1
    // to 2 RS), the cache can still report 1 replicaset while some buckets have
    // already moved. In that case this function incorrectly returns true and the
    // query is executed locally with incomplete data. This is an accepted
    // trade-off: the window is small and bounded by the catalog cache refresh
    // interval, and the same inconsistency would be caught on the next request.
    let is_replica = node.is_readonly();
    let topology_ref = node.catalog_cache.get();
    let mut tier_replicasets = topology_ref
        .all_replicasets()
        .filter(|rs| rs.tier == target_tier);
    let Some(replicaset) = tier_replicasets.next() else {
        return Ok(false);
    };
    if tier_replicasets.next().is_some() {
        return Ok(false);
    }

    Ok(local_dispatch_matches_read_preference(
        &topology_ref,
        is_replica,
        &replicaset.uuid,
        read_preference,
    ))
}

#[inline]
fn should_single_rs_dispatch_locally(
    buckets: &Buckets,
    tier: Option<&str>,
    replicasets: &[String],
    read_preference: &str,
) -> SqlResult<bool> {
    match buckets {
        Buckets::Filtered(_) => {
            if replicasets.len() != 1 {
                return Ok(false);
            }
            let Some(replicaset_uuid) = replicasets.first() else {
                return Ok(false);
            };
            should_dispatch_locally(tier, replicaset_uuid, read_preference)
        }
        Buckets::All => should_all_buckets_dispatch_locally(tier, read_preference),
        Buckets::Any => Ok(false),
    }
}

#[inline]
fn should_single_rs_buckets_dispatch_locally(
    buckets: &Buckets,
    tier: Option<&str>,
    rs_buckets: &[(String, Vec<u64>)],
    read_preference: &str,
) -> SqlResult<bool> {
    // Buckets::All means the query covers the entire cluster. Enumerating all
    // buckets via rs_buckets would require O(max_buckets) Lua calls just to
    // discover whether there is a single RS — we use the catalog cache instead,
    // matching the behaviour of should_single_rs_dispatch_locally.
    if matches!(buckets, Buckets::All) {
        return should_all_buckets_dispatch_locally(tier, read_preference);
    }

    if matches!(buckets, Buckets::Any) || rs_buckets.len() != 1 {
        return Ok(false);
    }

    let Some((replicaset_uuid, _)) = rs_buckets.first() else {
        return Ok(false);
    };
    should_dispatch_locally(tier, replicaset_uuid, read_preference)
}

#[inline]
fn local_dispatch_matches_read_preference(
    topology_ref: &CatalogCacheRef<'_>,
    is_replica: bool,
    replicaset_uuid: &str,
    read_preference: &str,
) -> bool {
    if topology_ref.this_instance().replicaset_uuid.as_str() != replicaset_uuid {
        return false;
    }

    match read_preference {
        "leader" => !is_replica,
        "replica" => is_replica,
        // Local fast-path avoids an extra network hop even for `any`.
        "any" => true,
        _ => false,
    }
}

/// The vshard ref session of the refs a storage takes for its own queries.
///
/// Routers name their sessions with UUIDs, so neither a router's rids nor its
/// cleanup after a failed dispatch can touch these refs. One session for all
/// the queries keeps vshard from collecting a session per query.
const STORAGE_REF_SESSION: &str = "picodata-storage";

thread_local! {
    /// The rid of the next ref in [`STORAGE_REF_SESSION`].
    static STORAGE_REF_RID: Cell<i64> = const { Cell::new(1) };
}

/// A vshard storage ref: while it lives, no bucket moves to or from this
/// storage.
///
/// When no bucket move runs or waits and all the buckets are writable, the ref
/// is just a count in the vshard scheduler, taken and released without Lua.
/// Otherwise it is a ref of the vshard ref module, with its waits, quotas and
/// errors.
pub(crate) enum StorageRef {
    Counted,
    Session(i64),
}

pub(crate) enum StorageRefError {
    Add(tarantool::error::Error),
    Use(tarantool::error::Error),
}

impl StorageRef {
    /// Take a ref, waiting up to `timeout` while buckets move.
    ///
    /// `as_admin` switches the session user to admin for `lref.add`, which
    /// reads `_bucket` when the cached bucket state is reset.
    pub(crate) fn take(timeout: Duration, as_admin: bool) -> Result<Self, StorageRefError> {
        let state = vshard::sched_state();
        // SAFETY: the state is used only in the TX thread, and nothing here
        // yields between the check and the update.
        unsafe {
            // The fast path of `sched.ref_start` plus the bucket wait of
            // `lref.add`, then the same updates as the success branch of
            // `sched.ref_start`.
            if (*state).move_count == 0.0 && (*state).move_queue == 0.0 && (*state).all_rw == 1.0 {
                (*state).ref_count += 1.0;
                (*state).ref_strike += 1.0;
                (*state).move_strike = 0.0;
                return Ok(Self::Counted);
            }
        }

        let rid = STORAGE_REF_RID.with(|c| c.replace(c.get() + 1));
        let add = || reference_add(rid, STORAGE_REF_SESSION, timeout);
        let added = if as_admin {
            with_su(ADMIN_ID, add).and_then(|added| added)
        } else {
            add()
        };
        added.map_err(StorageRefError::Add)?;
        if let Err(e) = reference_use(rid, STORAGE_REF_SESSION) {
            // Otherwise the added ref blocks bucket moves until it expires.
            let _ = reference_del(rid, STORAGE_REF_SESSION);
            return Err(StorageRefError::Use(e));
        }
        Ok(Self::Session(rid))
    }
}

impl Drop for StorageRef {
    fn drop(&mut self) {
        crate::error_injection!(block "BLOCK_STORAGE_REF_RELEASE");
        let released = match *self {
            Self::Counted => {
                let state = vshard::sched_state();
                // SAFETY: the state is used only in the TX thread.
                if unsafe { (*state).move_queue } > 0.0 {
                    // A bucket move waits: let vshard wake it up.
                    sched_ref_end(1)
                } else {
                    // SAFETY: as above.
                    unsafe { (*state).ref_count -= 1.0 };
                    Ok(())
                }
            }
            Self::Session(rid) => reference_del(rid, STORAGE_REF_SESSION),
        };
        released.unwrap_or_else(|e| panic!("failed to release a storage reference: {e}"));
    }
}

fn with_local_bucket_ref<T>(
    timeout: Duration,
    read_preference: &str,
    f: impl FnOnce() -> SqlResult<T>,
) -> SqlResult<T> {
    let node = node::global().map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
    if node.is_readonly() {
        return f();
    }

    debug_assert_ne!(read_preference, "replica");

    let _storage_ref = StorageRef::take(timeout, true).map_err(|e| {
        let (op, e) = match e {
            StorageRefError::Add(e) => ("add", e),
            StorageRefError::Use(e) => ("use", e),
        };
        SbroadError::DispatchError(format_smolstr!(
            "failed to {op} local bucket reference: {e}"
        ))
    })?;
    crate::error_injection!(block "BLOCK_STORAGE_REF_TAKEN");
    f()
}

fn execute_dql_locally<'p>(
    port: &mut impl Port<'p>,
    ex_plan: Rc<ExecutionPlan>,
    top_id: NodeId,
    buckets: &Buckets,
    timeout: Duration,
    read_preference: &str,
) -> SqlResult<()> {
    let start = Instant::now_fiber();
    let res = with_local_bucket_ref(timeout, read_preference, || {
        let runtime = StorageRuntime::new();
        runtime.exec_ir_on_any_node(ex_plan, top_id, buckets, port)
    });
    let result = if res.is_ok() { "ok" } else { "err" };
    observe_sql_local_query_duration("dql", result, &Instant::now_fiber().duration_since(start));
    record_sql_local_query_total("dql", result);
    res
}

fn port_write_local_dml_response<'p>(port: &mut impl Port<'p>, changed: u64) -> SqlResult<()> {
    let mut mp = [0_u8; 9];
    let pos = {
        let mut cur = Cursor::new(&mut mp[..]);
        write_uint(&mut cur, changed).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!("Failed to encode affected row count: {e}"))
        })?;
        cur.position() as usize
    };
    port.add_mp(&mp[..pos]);
    Ok(())
}

fn execute_dml_locally<'p>(
    port: &mut impl Port<'p>,
    ex_plan: Rc<ExecutionPlan>,
    top_id: NodeId,
    timeout: Duration,
) -> SqlResult<()> {
    let start = Instant::now_fiber();
    let request = build_dml_request(ex_plan, top_id)?;

    let res = with_local_bucket_ref(timeout, "leader", move || match request {
        DmlRequest::DeleteFull {
            core,
            plan_id,
            options,
        } => execute_full_delete_locally(port, core, plan_id, options),
        request => {
            let runtime = StorageRuntime::new();
            let changed = request.execute_locally(&runtime)?;
            port_write_local_dml_response(port, changed)
        }
    });
    let result = if res.is_ok() { "ok" } else { "err" };
    observe_sql_local_query_duration("dml", result, &Instant::now_fiber().duration_since(start));
    record_sql_local_query_total("dml", result);
    res
}

fn execute_full_delete_locally<'p>(
    port: &mut impl Port<'p>,
    core: DeleteCoreData,
    plan_id: u64,
    options: DQLOptions,
) -> SqlResult<()> {
    let runtime = StorageRuntime::new();
    let (table_name, current_version) = runtime.get_table_name_and_version(core.space_id)?;
    if current_version != core.space_version {
        return Err(SbroadError::OutdatedStorageSchema());
    }

    let table_versions =
        HashMap::<u32, u64, RepeatableState>::from_iter([(core.space_id, core.space_version)]);
    let schema_info = SchemaInfo::new(
        table_versions,
        HashMap::<[u32; 2], u64, RepeatableState>::default(),
    );
    let info = FullDeleteInfo::new(
        plan_id,
        schema_info,
        Options {
            sql_motion_row_max: options.sql_motion_row_max as i64,
            sql_vdbe_opcode_max: options.sql_vdbe_opcode_max as i64,
            read_preference: Default::default(),
            ..Default::default()
        },
        table_name.as_str(),
    );

    let mut tmp_port = PicoPortOwned::new();
    full_delete_execute(&runtime, &info, &mut tmp_port)?;

    let Some(mp) = tmp_port.iter().next() else {
        return Err(SbroadError::DispatchError(
            "local full delete execution returned an empty port".into(),
        ));
    };

    let changed = decode_local_dml_row_count(mp)?;
    port_write_local_dml_response(port, changed)
}

fn decode_local_dml_row_count(mp: &[u8]) -> SqlResult<u64> {
    let mut cur = Cursor::new(mp);
    let len = read_array_len(&mut cur)
        .map_err(IoError::other)
        .map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "failed to decode local DML response array: {e}"
            ))
        })?;
    if len != 1 {
        return Err(SbroadError::DispatchError(format_smolstr!(
            "expected local DML response array of length 1, got {len}"
        )));
    }
    read_int(&mut cur).map_err(IoError::other).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!(
            "failed to decode local DML affected row count: {e}"
        ))
    })
}

fn extract_single_rs_plan(
    rs_buckets: Vec<(String, Vec<u64>)>,
    ex_plan: Rc<ExecutionPlan>,
    top_id: NodeId,
) -> SqlResult<Rc<ExecutionPlan>> {
    if rs_buckets.len() != 1 {
        return Err(SbroadError::DispatchError(format_smolstr!(
            "expected a single replicaset execution plan, got {}",
            rs_buckets.len()
        )));
    }
    let (_, bucket_ids) = rs_buckets
        .into_iter()
        .next()
        .expect("single replicaset bucket set must be present");
    let ex_plan = match Rc::try_unwrap(ex_plan) {
        Ok(ex_plan) => ex_plan,
        Err(ex_plan) => ex_plan.as_ref().clone(),
    };
    Ok(Rc::new(prepare_single_rs_ir_plan(
        ex_plan,
        &bucket_ids,
        top_id,
    )?))
}

fn query_type_for_top(ex_plan: &ExecutionPlan, top_id: NodeId) -> SqlResult<QueryType> {
    let top = ex_plan.get_ir_plan().get_relation_node(top_id)?;
    if top.is_dml() {
        Ok(QueryType::DML)
    } else {
        Ok(QueryType::DQL)
    }
}

fn single_plan_dispatch_dql<'p>(
    port: &mut impl Port<'p>,
    ex_plan: Rc<ExecutionPlan>,
    top_id: NodeId,
    replicasets: &[String],
    max_rows: u64,
    timeout: Duration,
    tier: Option<&str>,
    read_preference: &str,
    do_two_step: bool,
) -> SqlResult<()> {
    let row_len = row_len(&ex_plan, top_id)?;
    let data_source = build_dql_protocol_for_top(ex_plan, top_id, local_sender_id()?)
        .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
    let tuple = encode_dql_tuple(&data_source)?;
    let _guard = put_query_meta(Some(data_source))?;

    crate::error_injection!(block "BLOCK_SQL_DISPATCH_AFTER_PUT_META");

    let lua = sql_lua_state();
    let lua_table = lua_single_plan_dispatch(
        &lua,
        &tuple,
        replicasets,
        timeout,
        tier,
        read_preference,
        do_two_step,
    )
    .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;

    dql_execution_result_process(port, lua_table, replicasets.len(), row_len, max_rows)?;

    Ok(())
}

pub(crate) fn build_cache_miss_dql_packet(request_id: &str, plan_id: u64) -> SqlResult<Tuple> {
    let query_meta_storage = QueryMetaStorage::new();
    let data = query_meta_storage.get(request_id, plan_id)?;
    let data_source = data
        .as_ref()
        .as_ref()
        .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;

    encode_tuple_with_reservation(
        |bc| {
            sql_protocol::dql::write_dql_cache_miss_packet(bc, data_source)
                .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
        },
        |tb| {
            sql_protocol::dql::write_dql_cache_miss_packet(tb, data_source)
                .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
        },
    )
}

fn custom_plan_dispatch_dql<'p>(
    port: &mut impl Port<'p>,
    ex_plan: Rc<ExecutionPlan>,
    top_id: NodeId,
    rs_buckets: Vec<(String, Vec<u64>)>,
    max_rows: u64,
    timeout: Duration,
    tier: Option<&str>,
    read_preference: &str,
    do_two_step: bool,
) -> SqlResult<()> {
    let row_len = row_len(&ex_plan, top_id)?;
    let (rs_plan, extra_plan_id) =
        prepare_rs_to_ir_map(&rs_buckets, ex_plan.as_ref().clone(), top_id)?;
    let plans = rs_plan.len();
    let mut first_args = HashMap::with_capacity(rs_plan.len());
    let mut exec_plan = None;
    let mut extra_exec_plan = None;
    for (rs, ex_plan) in rs_plan {
        let data_source = build_dql_protocol_for_top(Rc::new(ex_plan), top_id, local_sender_id()?)?;
        let tuple = encode_dql_tuple(&data_source)?;
        first_args.insert(rs, tuple);
        if Some(data_source.get_plan_id()) != extra_plan_id {
            exec_plan = Some(data_source);
        } else {
            extra_exec_plan = Some(data_source);
        }
    }
    let Some(exec_plan) = exec_plan else {
        return Err(SbroadError::DispatchError(format_smolstr!(
            "Custom plan must have at least one replicaset"
        )));
    };
    let _guard = put_query_meta(Some(exec_plan))?;

    let _guard_extra = put_query_meta(extra_exec_plan)?;

    let lua = sql_lua_state();
    let lua_table = lua_custom_plan_dispatch(
        &lua,
        &first_args,
        timeout,
        tier,
        read_preference,
        do_two_step,
    )
    .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;

    dql_execution_result_process(port, lua_table, plans, row_len, max_rows)?;

    Ok(())
}

fn row_len(ex_plan: &ExecutionPlan, top_id: NodeId) -> SqlResult<u32> {
    let ir_plan = ex_plan.get_ir_plan();
    let columns_len = ir_plan.columns_len(top_id)?;
    let len = u32::try_from(columns_len).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!(
            "Failed to convert columns length {columns_len} to u32: {e}"
        ))
    })?;
    Ok(len)
}

fn dql_execution_result_process<'lua, 'p>(
    port: &mut impl Port<'p>,
    table: Rc<IbufTable<'lua>>,
    table_len: usize,
    row_len: u32,
    max_rows: u64,
) -> SqlResult<()> {
    let mut row_count: u64 = 0;
    let rs_ibufs = lua_decode_rs_ibufs(&table, table_len).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!(
            "Failed to decode ibufs from DQL first round: {e}"
        ))
    })?;

    // First we should check that we don't have any MISS responses.
    // Otherwise we should forget ALL the data in ibufs and re-dispatch
    // to all replicasets. We can't just re-dispatch to the missed replicasets
    // as the buckets may be rebalanced meanwhile.
    for (rs, ibuf) in rs_ibufs.iter() {
        let mp = pcall_mp_process(ibuf.data()?).map_err(|_| {
            SbroadError::DispatchError(format_smolstr!(
                "Remote call on replicaset {rs} returned an error: {}",
                pcall_error(ibuf.data().unwrap_or(&[])),
            ))
        })?;
        let res = execute_read_response(mp).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to decode first round response from replicaset {rs}: {e}, msgpack: {}",
                escape_bytes(mp),
            ))
        })?;
        match res {
            SqlExecute::Dql(_) => {}
            SqlExecute::Miss => {
                return Err(SbroadError::DispatchError(
                    "Expected DQL response, got MISS".into(),
                ))
            }
            SqlExecute::Dml(_) => {
                return Err(SbroadError::DispatchError(
                    "Expected DQL response, got DML".into(),
                ))
            }
        }
    }

    // Great! All responses are DQL, so we can proceed to writing tuples to port.
    for (rs, ibuf) in rs_ibufs.into_iter() {
        let mp = pcall_mp_process(ibuf.data()?).map_err(|_| {
            SbroadError::DispatchError(format_smolstr!(
                "Remote call on replicaset {rs} returned an error: {}",
                pcall_error(ibuf.data().unwrap_or(&[])),
            ))
        })?;
        let res = execute_read_response(mp).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to decode first round response from replicaset {rs}: {e}, msgpack: {}",
                escape_bytes(mp),
            ))
        })?;
        match res {
            SqlExecute::Dql(tuples) => {
                port_write_tuples(port, tuples, max_rows, &mut row_count, row_len, &rs)?;
            }
            _ => unreachable!("We have already checked that there are no MISS or DML responses"),
        }
    }

    Ok(())
}

#[inline(always)]
fn port_write_tuples<'tuples, 'p>(
    port: &mut impl Port<'p>,
    tuples: TupleIter<'tuples>,
    max_rows: u64,
    row_count: &mut u64,
    row_len: u32,
    rs: &str,
) -> SqlResult<()> {
    for mp in tuples {
        let mp = mp.map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to decode tuple from replicaset {rs}: {e}"
            ))
        })?;
        *row_count += 1;
        if max_rows > 0 && *row_count > max_rows {
            return Err(SbroadError::motion_row_max_exceeded(max_rows, *row_count));
        }

        port_append_mp(port, mp, row_len).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to append tuple from replicaset {rs} to port: {e}"
            ))
        })?;
    }
    Ok(())
}

fn port_append_mp<'p>(port: &mut impl Port<'p>, mp: &[u8], row_len: u32) -> IoResult<()> {
    let mut cur = Cursor::new(mp);
    let len = read_array_len(&mut cur).map_err(IoError::other)?;
    if len != row_len {
        return Err(IoError::other(format!(
            "Expected array of length {row_len}, got {len}",
        )));
    }
    port.add_mp(mp);
    Ok(())
}

pub fn replicasets_from_buckets(
    buckets: &Buckets,
    tier: Option<&str>,
    deadline: Instant,
) -> SqlResult<Vec<String>> {
    let iter = match buckets {
        Buckets::Any => {
            return Err(SbroadError::DispatchError(
                "there is no sense to translate 'any' buckets into replicasets".into(),
            ));
        }
        Buckets::All => return Ok(Vec::new()),
        Buckets::Filtered(BucketSet::Exact(list)) => list.iter(),
        Buckets::Filtered(_) => {
            return Err(SbroadError::Invalid(
                Entity::Buckets,
                Some("buckets are not discovered".into()),
            ))
        }
    };
    let lua = sql_lua_state();
    let mut replicasets: Vec<String> = Vec::new();
    for id in iter {
        let rs = bucket_into_rs(&lua, *id, tier, deadline).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to get replicaset from bucket {id}: {e}"
            ))
        })?;
        replicasets.push(rs);
    }
    replicasets.sort_unstable();
    replicasets.dedup();
    Ok(replicasets)
}

fn buckets_by_replicasets(
    buckets: &Buckets,
    max_buckets: u64,
    tier: Option<&str>,
    timeout: Duration,
) -> SqlResult<Vec<(String, Vec<u64>)>> {
    enum BucketIter<'a> {
        All(std::ops::RangeInclusive<u64>),
        Filtered(std::collections::hash_set::Iter<'a, u64>),
    }

    impl Iterator for BucketIter<'_> {
        type Item = u64;
        fn next(&mut self) -> Option<u64> {
            match self {
                BucketIter::All(r) => r.next(),
                BucketIter::Filtered(it) => it.next().copied(),
            }
        }
    }

    let iter = match buckets {
        Buckets::Any => {
            return Err(SbroadError::DispatchError(
                "there is no sense to group 'any' buckets by replicasets".into(),
            ));
        }
        Buckets::All => BucketIter::All(1..=max_buckets),
        Buckets::Filtered(BucketSet::Exact(list)) => BucketIter::Filtered(list.iter()),
        Buckets::Filtered(_) => {
            return Err(SbroadError::Invalid(
                Entity::Buckets,
                Some("buckets are not discovered".into()),
            ))
        }
    };
    let lua = sql_lua_state();
    let mut map: AHashMap<String, Vec<u64>> = AHashMap::new();
    let deadline = Instant::now_fiber().saturating_add(timeout);
    for id in iter {
        let rs: String = bucket_into_rs(&lua, id, tier, deadline).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to get replicaset from bucket {id}: {e}"
            ))
        })?;
        map.entry(rs).or_default().push(id);
    }
    Ok(map.into_iter().collect())
}

/// Creates a typed DML request that can later be either encoded for remote
/// execution or executed locally without re-parsing the protocol payload.
fn build_dml_request(ex_plan: Rc<ExecutionPlan>, top_id: NodeId) -> SqlResult<DmlRequest> {
    let plan = ex_plan.get_ir_plan();
    let relation_node = plan.get_relation_node(top_id)?;

    // If DML child is a motion with local policy, data will be processed in storage.
    // Plan should be stored for cache miss handling.
    let with_dql = if let [child] = relation_node.children().as_slice() {
        matches!(
            plan.get_relation_node(*child)?,
            Relational::Motion(Motion {
                policy: MotionPolicy::Local | MotionPolicy::LocalSegment { .. },
                ..
            })
        )
    } else {
        false
    };

    /// Helper to extract table id and version from a relation reference
    fn get_table_info(plan: &Plan, relation: &SmolStr) -> SqlResult<(u32, u64)> {
        let table = plan.relations.get(relation).ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Plan,
                Some(format_smolstr!("Relation {relation} not found in plan")),
            )
        })?;

        let table_version = plan.table_version_map.get(&table.id).ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Plan,
                Some(format_smolstr!(
                    "Version of table {relation} not found in plan"
                )),
            )
        })?;

        Ok((table.id, *table_version))
    }

    match relation_node {
        Relational::Delete(Delete {
            relation, child, ..
        }) => {
            let (table_id, table_version) = get_table_info(plan, relation)?;

            let core = DeleteCoreData {
                request_id: SmolStr::new(ex_plan.get_request_id()),
                space_id: table_id,
                space_version: table_version,
            };

            match child {
                Some(child) => {
                    debug_assert!(with_dql, "Delete filtered works only with DQL");

                    let types = vtable_columns(plan, *child)?;
                    let pattern = init_delete_tuple_builder(plan, top_id)?;
                    let plan =
                        build_dql_protocol_for_top(Rc::clone(&ex_plan), top_id, local_sender_id()?)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
                    Ok(DmlRequest::DeleteFiltered {
                        core,
                        types,
                        pattern,
                        plan,
                    })
                }
                None => {
                    debug_assert!(!with_dql, "Delete full works only without DQL");
                    let plan_id = ex_plan.get_plan_id()?;
                    let options = plan.effective_options.to_protocol_options();
                    Ok(DmlRequest::DeleteFull {
                        core,
                        plan_id,
                        options,
                    })
                }
            }
        }
        Relational::Insert(Insert {
            relation,
            conflict_strategy,
            child,
            ..
        }) => {
            let (table_id, table_version) = get_table_info(plan, relation)?;
            let core = InsertCoreData {
                request_id: SmolStr::new(ex_plan.get_request_id()),
                space_id: table_id,
                space_version: table_version,
                conflict_policy: conflict_strategy.try_into()?,
            };

            if !with_dql {
                let table = ex_plan.get_motion_vtable(*child)?;
                let pattern = init_insert_tuple_builder(plan, table.get_columns(), top_id)?;
                Ok(DmlRequest::Insert {
                    core,
                    vtable: table,
                    pattern,
                })
            } else {
                let types = vtable_columns(plan, *child)?;
                let pattern = init_insert_tuple_builder(plan, types.as_slice(), top_id)?;
                let plan =
                    build_dql_protocol_for_top(Rc::clone(&ex_plan), top_id, local_sender_id()?)
                        .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
                Ok(DmlRequest::InsertLocal {
                    core,
                    types,
                    pattern,
                    plan,
                })
            }
        }
        Relational::Update(Update {
            relation,
            strategy,
            child,
            ..
        }) => {
            let (table_id, table_version) = get_table_info(plan, relation)?;

            let core = UpdateCoreData {
                request_id: SmolStr::new(ex_plan.get_request_id()),
                space_id: table_id,
                space_version: table_version,
                update_type: strategy.into(),
            };

            match strategy {
                UpdateStrategy::ShardedUpdate => {
                    debug_assert!(!with_dql, "ShardedUpdate cannot be used with DQL");
                    let delete_tuple_len = ex_plan.get_update_delete_tuple_len(top_id)?;
                    let table = ex_plan.get_motion_vtable(*child)?;
                    let pattern =
                        init_sharded_update_tuple_builder(plan, table.get_columns(), top_id)?;
                    Ok(DmlRequest::UpdateShared {
                        core,
                        delete_tuple_len,
                        vtable: table,
                        pattern,
                    })
                }
                UpdateStrategy::LocalUpdate => {
                    debug_assert!(with_dql, "LocalUpdate cannot be used without DQL");
                    let types = vtable_columns(plan, *child)?;
                    let pattern = init_local_update_tuple_builder(plan, types.as_slice(), top_id)?;
                    let plan =
                        build_dql_protocol_for_top(Rc::clone(&ex_plan), top_id, local_sender_id()?)
                            .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
                    Ok(DmlRequest::UpdateLocal {
                        core,
                        types,
                        pattern,
                        plan,
                    })
                }
            }
        }
        _ => Err(SbroadError::DispatchError(format_smolstr!(
            "Unsupported DML operation"
        ))),
    }
}

fn single_plan_dispatch_dml<'p>(
    port: &mut impl Port<'p>,
    ex_plan: Rc<ExecutionPlan>,
    top_id: NodeId,
    replicasets: &[String],
    timeout: Duration,
    tier: Option<&str>,
) -> SqlResult<()> {
    // This option is available only for DQL.
    let read_preference = ReadPreference::default().as_str();
    let (message, new_plan) = build_dml_request(ex_plan, top_id)?.into_message()?;
    let _guard = put_query_meta(new_plan)?;

    let lua = sql_lua_state();
    let lua_table = lua_single_plan_dispatch(
        &lua,
        message,
        replicasets,
        timeout,
        tier,
        read_preference,
        false,
    )
    .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;
    // TODO: all buckets will allocate nothing, because it is empty
    dml_process(port, lua_table, replicasets.len())?;
    Ok(())
}

fn custom_plan_dispatch_dml<'p>(
    port: &mut impl Port<'p>,
    ex_plan: Rc<ExecutionPlan>,
    top_id: NodeId,
    rs_buckets: Vec<(String, Vec<u64>)>,
    timeout: Duration,
    tier: Option<&str>,
) -> SqlResult<()> {
    let read_preference = ReadPreference::default().as_str();
    let (rs_plan, _) = prepare_rs_to_ir_map(&rs_buckets, ex_plan.as_ref().clone(), top_id)?;
    let mut dql_encoder = None;
    let mut args = HashMap::with_capacity(rs_plan.len());
    for (rs, ex_plan) in rs_plan {
        let (message, new_plan) = build_dml_request(Rc::new(ex_plan), top_id)?.into_message()?;
        dql_encoder = new_plan;
        args.insert(rs, message);
    }
    let _guard = put_query_meta(dql_encoder)?;
    let len = args.len();
    let lua = sql_lua_state();
    let lua_table = lua_custom_plan_dispatch(&lua, args, timeout, tier, read_preference, false)
        .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;
    dml_process(port, lua_table, len)?;
    Ok(())
}

fn dml_process<'lua, 'p>(
    port: &mut impl Port<'p>,
    table: Rc<IbufTable<'lua>>,
    length: usize,
) -> SqlResult<()> {
    let rs_ibufs = lua_decode_rs_ibufs(&table, length).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!(
            "Failed to decode DML response from Lua: {e}"
        ))
    })?;
    let mut row_count: u64 = 0;
    for (rs, ibuf) in rs_ibufs.into_iter() {
        let mp = pcall_mp_process(ibuf.data()?).map_err(|_| {
            SbroadError::DispatchError(format_smolstr!(
                "Remote call on replicaset {rs} returned an error: {}",
                pcall_error(ibuf.data().unwrap_or(&[])),
            ))
        })?;
        let res = execute_read_response(mp).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to decode DML response from replicaset {rs}: {e}, msgpack: {}",
                escape_bytes(mp),
            ))
        })?;
        match res {
            SqlExecute::Dql(_) => {
                return Err(SbroadError::DispatchError(format_smolstr!(
                    "Expected DML response from replicaset {rs}, got DQL"
                )))
            }
            SqlExecute::Miss => {
                return Err(SbroadError::DispatchError(format_smolstr!(
                    "Expected DML response from replicaset {rs}, got MISS"
                )))
            }
            SqlExecute::Dml(changed) => {
                row_count += changed;
            }
        }
    }
    port_write_local_dml_response(port, row_count)
}

fn pcall_mp_process(mp: &[u8]) -> IoResult<&[u8]> {
    let mut cur = Cursor::new(mp);
    let len = read_array_len(&mut cur).map_err(IoError::other)?;
    if len != 2 {
        return Err(IoError::other(format!(
            "Expected array of length 2 from pcall result, got {len}",
        )));
    }
    let is_ok = read_bool(&mut cur).map_err(IoError::other)?;
    if !is_ok {
        return Err(IoError::other("Lua pcall returned an error"));
    }
    Ok(&mp[cur.position() as usize..])
}

fn pcall_error(mp: &[u8]) -> String {
    match msgpack_decode(mp) {
        Ok(s) => s,
        Err(_) => escape_bytes(mp).to_string(),
    }
}

fn msgpack_decode(bytes: &[u8]) -> Result<String, String> {
    let mut cur = Cursor::new(bytes);
    let v: rmpv::Value = rmpv::decode::read_value(&mut cur).map_err(|e| format!("{e}"))?;
    serde_json::to_string(&MpValueAsJson(&v)).map_err(|e| format!("{e}"))
}
