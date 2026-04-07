use super::execute::{
    dql_execute_locally_from_plan, filtered_delete_from_plan, full_delete_execute,
    materialize_with_dql, materialized_update_from_plan, shared_update_from_plan,
    tuple_insert_from_plan,
};
use super::port::PicoPortOwned;
use super::storage::{execute_block_locally, FullDeleteInfo, StorageRuntime};
use crate::catalog::pico_table::PicoTable;
use crate::config::{DEFAULT_SQL_PREEMPTION, DYNAMIC_CONFIG};
use crate::metrics::{observe_sql_local_query_duration, record_sql_local_query_total};
use crate::schema::ADMIN_ID;
use crate::sql::lua::{
    bucket_into_rs, dispatch_session_id, escape_bytes, lua_custom_plan_dispatch,
    lua_decode_rs_ibufs, lua_single_plan_dispatch, reference_add, reference_del, reference_use,
    IbufTable,
};
use crate::sql::storage::explain_execute_block;
use crate::topology_cache::TopologyCacheRef;
use crate::traft::node;
use ahash::{AHashMap, AHashSet};
use rmp::decode::{read_array_len, read_bool, read_int};
use rmp::encode::{write_array_len, write_uint};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use sql::errors::{Action, Entity, SbroadError};
use sql::executor::engine::helpers::vshard::prepare_rs_to_ir_map;
use sql::executor::engine::helpers::{
    init_delete_tuple_builder, init_insert_tuple_builder, init_local_update_tuple_builder,
    init_sharded_update_tuple_builder, try_get_metadata_from_plan, vtable_columns,
};
use sql::executor::engine::protocol::{
    build_dql_data_source, DeleteCoreData, ExecutionCacheMissData, ExecutionData,
    FilteredDeleteData, FullDeleteData, InsertCoreData, LocalInsertData, LocalUpdateData,
    SharedUpdateData, TupleInsertData, UpdateCoreData,
};
use sql::executor::engine::{BlockExecData, QueryCache, Vshard};
use sql::executor::ir::{ExecutionPlan, QueryType};
use sql::executor::protocol::SchemaInfo;
use sql::executor::result::MetadataColumn;
use sql::executor::vtable::VirtualTable;
use sql::executor::Port;
use sql::ir::api::children::Children;
use sql::ir::bucket::{BucketSet, Buckets};
use sql::ir::helpers::RepeatableState;
use sql::ir::node::relational::Relational;
use sql::ir::node::{Delete, Insert, Motion, Update};
use sql::ir::operator::UpdateStrategy;
use sql::ir::options::{Options, ReadPreference};
use sql::ir::transformation::redistribution::MotionPolicy;
use sql::ir::{ExplainOptions, Plan};
use sql::utils::ByteCounter;
use sql_protocol::block::write_block_packet;
use sql_protocol::decode::{execute_read_response, SqlExecute, TupleIter};
use sql_protocol::dml::delete::{write_delete_filtered_packet, write_delete_full_packet};
use sql_protocol::dml::insert::{write_insert_materialized_packet, write_insert_packet};
use sql_protocol::dml::update::{write_update_packet, write_update_shared_key_packet};
use sql_protocol::dql::write_dql_packet;
use sql_protocol::dql_encoder::{DQLDataSource, DQLOptions};
use sql_protocol::encode::write_metadata;
use std::cell::{Cell, LazyCell, OnceCell};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Cursor, Error as IoError, Result as IoResult};
use std::rc::{Rc, Weak};
use tarantool::fiber::Mutex;
use tarantool::session::with_su;
use tarantool::time::Instant;
use tarantool::tlua::LuaThread;
use tarantool::tuple::{Tuple, TupleBuilder};

pub type SqlResult<T> = Result<T, SbroadError>;

/// CacheMissResponse lazy initialize ExecutionCacheMissData by calling the closure with ExecutionData.
type CacheMissResponse = LazyCell<
    Result<ExecutionCacheMissData, SbroadError>,
    Box<dyn FnOnce() -> Result<ExecutionCacheMissData, SbroadError>>,
>;
/// Weak reference allows entries to be dropped when no longer in use by any execution.
type MetadataHashMap = HashMap<u64, Weak<CacheMissResponse>>; // plan_id -> CacheMissResponse
const QUERY_METADATA_CAPACITY: usize = 100;

thread_local! {
    static QUERY_METADATA: Rc<Mutex<MetadataHashMap>> = Rc::new(Mutex::new(HashMap::with_capacity(QUERY_METADATA_CAPACITY)));
}

pub struct CacheGuard {
    storage: Rc<Mutex<MetadataHashMap>>,
    plan_id: u64,
    handle: Rc<CacheMissResponse>,
}

impl Drop for CacheGuard {
    fn drop(&mut self) {
        let mut cache = self.storage.lock();
        if Rc::strong_count(&self.handle) == 1 {
            cache.remove(&self.plan_id);
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
        let mut metadata = self.query_meta.lock();
        let Some(value) = metadata.get(&plan_id) else {
            return Err(SbroadError::NotFound(
                Entity::Query,
                format_smolstr!("for request_id {} with plan_id {}", request_id, plan_id),
            ));
        };
        let Some(rc_value) = value.upgrade() else {
            // Stale entry — clean up and report not found
            metadata.remove(&plan_id);
            return Err(SbroadError::NotFound(
                Entity::Query,
                format_smolstr!("for request_id {} with plan_id {}", request_id, plan_id),
            ));
        };

        LazyCell::force(&rc_value);

        return Ok(rc_value.clone());
    }

    fn put(&self, plan_id: u64, plan: ExecutionData) -> Result<CacheGuard, SbroadError> {
        let mut metadata = self.query_meta.lock();
        let handle = match metadata.entry(plan_id) {
            Entry::Vacant(e) => {
                let rc = Rc::new(CacheMissResponse::new(Box::new(move || {
                    ExecutionCacheMissData::try_from(plan)
                })));
                e.insert(Rc::downgrade(&rc));
                rc
            }
            Entry::Occupied(mut e) => match e.get().upgrade() {
                Some(rc) => rc,
                None => {
                    let rc = Rc::new(CacheMissResponse::new(Box::new(move || {
                        ExecutionCacheMissData::try_from(plan)
                    })));
                    e.insert(Rc::downgrade(&rc));
                    rc
                }
            },
        };

        Ok(CacheGuard {
            storage: self.query_meta.clone(),
            plan_id,
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

fn encode_dql_tuple(data_source: &ExecutionData) -> SqlResult<Tuple> {
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

fn put_query_meta(plan: Option<ExecutionData>) -> SqlResult<Option<CacheGuard>> {
    let Some(plan) = plan else {
        return Ok(None);
    };
    let key = plan.get_plan_id();
    let query_meta_storage = QueryMetaStorage::new();
    Ok(Some(query_meta_storage.put(key, plan)?))
}

enum DmlRequest {
    DeleteFiltered {
        core: DeleteCoreData,
        types: Vec<sql::ir::relation::Column>,
        pattern: sql::executor::engine::helpers::TupleBuilderPattern,
        plan: ExecutionData,
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
        plan: ExecutionData,
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
        plan: ExecutionData,
    },
}

impl DmlRequest {
    fn into_message(self) -> SqlResult<(Tuple, Option<ExecutionData>)> {
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
    ex_plan: ExecutionPlan,
    buckets: &Buckets,
    timeout: u64,
    tier: Option<&str>,
) -> SqlResult<()> {
    let lua = tarantool::lua_state();
    let replicasets = replicasets_from_buckets(&lua, buckets, tier)?;
    let query_type = ex_plan.query_type()?;
    match &query_type {
        QueryType::DQL => {
            let max_rows = ex_plan.get_sql_motion_row_max();
            let read_preference = effective_read_preference(&ex_plan)?;
            if should_single_rs_dispatch_locally(buckets, tier, &replicasets, &read_preference)? {
                execute_dql_locally(port, ex_plan, buckets, timeout, &read_preference)?;
                return Ok(());
            }

            port_write_metadata(port, &ex_plan)?;
            // For read_preference = 'replica' | 'any', we follow an optimistic scenario.
            // Plan will be routed to RO replica, so references from vshard will not help.
            let is_on_leader =
                ex_plan.get_ir_plan().effective_options.read_preference == ReadPreference::Leader;
            let do_two_step = replicasets.len() != 1 && is_on_leader;
            single_plan_dispatch_dql(
                port,
                &lua,
                ex_plan,
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
                execute_dml_locally(port, ex_plan, timeout)?;
                return Ok(());
            }

            single_plan_dispatch_dml(port, &lua, ex_plan, &replicasets, timeout, tier)?
        }
    };
    Ok(())
}

pub(crate) fn custom_plan_dispatch<'p>(
    port: &mut impl Port<'p>,
    runtime: &impl Vshard,
    ex_plan: ExecutionPlan,
    buckets: &Buckets,
    timeout: u64,
    tier: Option<&str>,
) -> SqlResult<()> {
    let lua = tarantool::lua_state();
    let rs_buckets = buckets_by_replicasets(&lua, buckets, runtime.bucket_count(), tier)?;
    if rs_buckets.is_empty() {
        return Err(SbroadError::DispatchError(
            "No replicasets found for the given buckets".into(),
        ));
    }
    let query_type = ex_plan.query_type()?;
    match &query_type {
        QueryType::DQL => {
            let max_rows = ex_plan.get_sql_motion_row_max();
            let read_preference = effective_read_preference(&ex_plan)?;
            if should_single_rs_buckets_dispatch_locally(
                buckets,
                tier,
                &rs_buckets,
                &read_preference,
            )? {
                let local_plan = extract_single_rs_plan(rs_buckets, ex_plan)?;
                execute_dql_locally(port, local_plan, buckets, timeout, &read_preference)?;
                return Ok(());
            }

            // All custom plans must return the same metadata,
            // so we can use the original plan to write it to the port.
            port_write_metadata(port, &ex_plan)?;
            // For read_preference = 'replica' | 'any', we follow an optimistic scenario.
            // Plan will be routed to RO replica, so references from vshard will not help.
            let is_on_leader =
                ex_plan.get_ir_plan().effective_options.read_preference == ReadPreference::Leader;
            let do_two_step = rs_buckets.len() != 1 && is_on_leader;
            custom_plan_dispatch_dql(
                port,
                &lua,
                ex_plan,
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
                let local_plan = extract_single_rs_plan(rs_buckets, ex_plan)?;
                execute_dml_locally(port, local_plan, timeout)?;
                return Ok(());
            }

            custom_plan_dispatch_dml(port, &lua, ex_plan, rs_buckets, timeout, tier)?;
        }
    };
    Ok(())
}

pub(crate) fn block_dispatch<'p>(
    port: &mut impl Port<'p>,
    metadata: Vec<MetadataColumn>,
    block: BlockExecData,
    buckets: &Buckets,
    request_id: &str,
    timeout: u64,
    tier: Option<&str>,
) -> Result<(), SbroadError> {
    if !block.explain_options.is_empty() {
        if block.explain_options.contains(ExplainOptions::Logical) {
            return Err(SbroadError::NotImplemented(
                Entity::Explain,
                "for transactions".to_smolstr(),
            ));
        }

        return explain_execute_block(block, buckets.determine_exec_location(), port);
    }

    match buckets {
        Buckets::All => Err(SbroadError::other(
            "cannot execute transaction on all buckets",
        )),
        Buckets::Any => execute_block_locally_for_dispatch(port, &metadata, block),
        Buckets::Filtered(_) => {
            let lua = tarantool::lua_state();
            let replicasets = replicasets_from_buckets(&lua, buckets, tier)?;

            if should_single_rs_dispatch_locally(buckets, tier, &replicasets, "leader")? {
                return with_local_bucket_ref(timeout, "leader", || {
                    execute_block_locally_for_dispatch(port, &metadata, block)
                });
            }
            port_write_block_metadata(port, &metadata)?;
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

            let lua_table = lua_single_plan_dispatch(
                &lua,
                &tuple,
                &replicasets,
                timeout,
                tier,
                String::from("leader"),
                false,
            )
            .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;

            // Block cannot have any motions.
            let motion_max_rows = 0;

            if !metadata.is_empty() {
                dql_execution_result_process(
                    port,
                    lua_table,
                    replicasets.len(),
                    columns as _,
                    motion_max_rows,
                )?;
            } else {
                dml_process(port, lua_table, replicasets.len())?;
            }

            Ok(())
        }
    }
}

fn execute_block_locally_for_dispatch<'p>(
    port: &mut impl Port<'p>,
    metadata: &[MetadataColumn],
    block: BlockExecData,
) -> SqlResult<()> {
    let runtime = StorageRuntime::new();
    runtime.validate_block_schema(&block)?;

    if !metadata.is_empty() {
        port_write_block_metadata(port, metadata)?;
        return execute_block_locally(block, port);
    }

    let mut tmp_port = PicoPortOwned::new();
    execute_block_locally(block, &mut tmp_port)?;
    let changed = parse_execute_dml_row_count(&tmp_port)?;
    port_write_local_dml_response(port, changed)
}

fn parse_execute_dml_row_count(port: &PicoPortOwned) -> SqlResult<u64> {
    let mp = port.iter().next().ok_or_else(|| {
        SbroadError::DispatchError("local block DML port must not be empty".into())
    })?;
    let mut cursor = Cursor::new(mp);
    let array_len = read_array_len(&mut cursor).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!(
            "failed to decode local block DML response array length: {e}"
        ))
    })?;
    if array_len != 1 {
        return Err(SbroadError::DispatchError(format_smolstr!(
            "expected single-element local block DML response, got array length {array_len}"
        )));
    }
    read_int(&mut cursor).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!(
            "failed to decode local block DML row count: {e}"
        ))
    })
}

fn port_write_block_metadata<'p>(
    port: &mut impl Port<'p>,
    metadata: &[MetadataColumn],
) -> SqlResult<()> {
    if metadata.is_empty() {
        return Ok(());
    }

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

pub(crate) fn port_write_metadata<'p>(
    port: &mut impl Port<'p>,
    ex_plan: &ExecutionPlan,
) -> SqlResult<()> {
    let metadata = try_get_metadata_from_plan(ex_plan)?.ok_or_else(|| {
        SbroadError::FailedTo(
            Action::Get,
            Some(Entity::Query),
            "Failed to get metadata from execution plan".into(),
        )
    })?;
    let length = metadata.len() as u32;
    write_metadata(
        port,
        metadata
            .iter()
            .map(|c| (c.name.as_str(), c.r#type.as_str())),
        length,
    )
    .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;
    Ok(())
}

/// Get all unlogged tables and check if any of them are present in the plan tables.
fn plan_has_unlogged_tables(plan: &Plan) -> SqlResult<bool> {
    let unlogged_tables = with_admin_su("getting unlogged tables", || {
        PicoTable::new().get_unlogged_tables()
    })?;

    for table in unlogged_tables {
        if plan.relations.tables.contains_key(&table.name) {
            return Ok(true);
        }
    }

    Ok(false)
}

/// If `sql_preemption` is enabled, `read_preference` should be equal to `leader`.
///
/// If the plan has unlogged tables `read_preference` should be equal to `leader`.
fn effective_read_preference(ex_plan: &ExecutionPlan) -> SqlResult<String> {
    let plan = ex_plan.get_ir_plan();
    let read_preference = plan.effective_options.read_preference;

    if read_preference == ReadPreference::Leader {
        return Ok(read_preference.to_string());
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

    Ok(read_preference.to_string())
}

fn should_dispatch_locally(
    tier: Option<&str>,
    replicaset_uuid: &str,
    read_preference: &str,
) -> SqlResult<bool> {
    let node = node::global().map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
    if tier.is_some_and(|tier_name| tier_name != node.topology_cache.my_tier_name()) {
        return Ok(false);
    }

    if replicaset_uuid != node.topology_cache.my_replicaset_uuid() {
        return Ok(false);
    }

    let is_replica = node.is_readonly();
    let topology_ref = node.topology_cache.get();
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
    let target_tier = tier.unwrap_or_else(|| node.topology_cache.my_tier_name());
    if target_tier != node.topology_cache.my_tier_name() {
        return Ok(false);
    }

    // NOTE: topology_cache may be slightly stale relative to the actual vshard
    // bucket distribution. During rebalancing (e.g. cluster just scaled from 1
    // to 2 RS), the cache can still report 1 replicaset while some buckets have
    // already moved. In that case this function incorrectly returns true and the
    // query is executed locally with incomplete data. This is an accepted
    // trade-off: the window is small and bounded by the topology cache refresh
    // interval, and the same inconsistency would be caught on the next request.
    let is_replica = node.is_readonly();
    let topology_ref = node.topology_cache.get();
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
    // discover whether there is a single RS — we use the topology cache instead,
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
    topology_ref: &TopologyCacheRef<'_>,
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

thread_local! {
    /// Session UUID shared with the Lua dispatch path (pico.dispatch.session_id).
    /// All local fast-path requests reuse this single vshard lref session so that
    /// vshard does not accumulate a dead session per request in its session_map.
    /// Initialized lazily on first use of the local fast-path.
    static LOCAL_LREF_SESSION_ID: OnceCell<String> = const { OnceCell::new() };

    /// Ref ID counter for vshard lref. Must be unique within LOCAL_LREF_SESSION_ID.
    /// Stored per-fiber in Tarantool's TX thread — no atomics needed.
    static LOCAL_LREF_RID: Cell<i64> = const { Cell::new(1) };
}

/// Returns the vshard lref session UUID, shared with the Lua dispatch path.
/// Calls pico.dispatch.session_id() once and caches the result.
fn local_lref_session_id() -> SqlResult<SmolStr> {
    LOCAL_LREF_SESSION_ID.with(|cell| {
        if let Some(s) = cell.get() {
            return Ok(s.as_str().into());
        }
        let id = dispatch_session_id().map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
        let _ = cell.set(id);
        Ok(cell.get().unwrap().as_str().into())
    })
}

fn with_admin_su<T>(op: &str, f: impl FnOnce() -> tarantool::Result<T>) -> SqlResult<T> {
    let result = with_su(ADMIN_ID, f).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!("failed to switch user for {op}: {e}"))
    })?;
    result.map_err(|e| SbroadError::DispatchError(format_smolstr!("failed to {op}: {e}")))
}

fn with_local_bucket_ref<T>(
    timeout: u64,
    read_preference: &str,
    f: impl FnOnce() -> SqlResult<T>,
) -> SqlResult<T> {
    let node = node::global().map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
    if node.is_readonly() {
        return f();
    }

    debug_assert_ne!(read_preference, "replica");

    let sid = local_lref_session_id()?;
    let sid = sid.as_str();
    let rid = LOCAL_LREF_RID.with(|c| {
        let v = c.get();
        c.set(v + 1);
        v
    });
    with_admin_su("add local bucket reference", || {
        reference_add(rid, sid, timeout as f64)
    })?;

    if let Err(e) = with_admin_su("use local bucket reference", || reference_use(rid, sid)) {
        return Err(SbroadError::DispatchError(format_smolstr!(
            "failed to use local bucket reference: {e}"
        )));
    }

    let result = f();
    with_admin_su("remove local bucket reference", || reference_del(rid, sid))
        .unwrap_or_else(|e| panic!("failed to delete local bucket reference: {e}"));
    result
}

fn execute_dql_locally<'p>(
    port: &mut impl Port<'p>,
    ex_plan: ExecutionPlan,
    buckets: &Buckets,
    timeout: u64,
    read_preference: &str,
) -> SqlResult<()> {
    let start = Instant::now_fiber();
    let res = with_local_bucket_ref(timeout, read_preference, || {
        let runtime = StorageRuntime::new();
        runtime.exec_ir_on_any_node(ex_plan, buckets, port)
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
    ex_plan: ExecutionPlan,
    timeout: u64,
) -> SqlResult<()> {
    let start = Instant::now_fiber();
    let request = build_dml_request(ex_plan)?;

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
        return Err(SbroadError::OutdatedStorageSchema);
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
    ex_plan: ExecutionPlan,
) -> SqlResult<ExecutionPlan> {
    let (mut rs_plan, _) = prepare_rs_to_ir_map(&rs_buckets, ex_plan)?;
    let Some((_, ex_plan)) = rs_plan.drain().next() else {
        return Err(SbroadError::DispatchError(
            "expected a single replicaset execution plan".into(),
        ));
    };
    Ok(ex_plan)
}

fn single_plan_dispatch_dql<'lua, 'p>(
    port: &mut impl Port<'p>,
    lua: &'lua LuaThread,
    ex_plan: ExecutionPlan,
    replicasets: &[String],
    max_rows: u64,
    timeout: u64,
    tier: Option<&str>,
    read_preference: String,
    do_two_step: bool,
) -> SqlResult<()> {
    let row_len = row_len(&ex_plan)?;
    let data_source = build_dql_data_source(ex_plan, local_sender_id()?)
        .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))?;
    let tuple = encode_dql_tuple(&data_source)?;
    let _guard = put_query_meta(Some(data_source))?;

    let lua_table = lua_single_plan_dispatch(
        lua,
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

fn custom_plan_dispatch_dql<'lua, 'p>(
    port: &mut impl Port<'p>,
    lua: &'lua LuaThread,
    ex_plan: ExecutionPlan,
    rs_buckets: Vec<(String, Vec<u64>)>,
    max_rows: u64,
    timeout: u64,
    tier: Option<&str>,
    read_preference: String,
    do_two_step: bool,
) -> SqlResult<()> {
    let row_len = row_len(&ex_plan)?;
    let (rs_plan, extra_plan_id) = prepare_rs_to_ir_map(&rs_buckets, ex_plan)?;
    let plans = rs_plan.len();
    let mut first_args = HashMap::with_capacity(rs_plan.len());
    let mut exec_plan = None;
    let mut extra_exec_plan = None;
    for (rs, ex_plan) in rs_plan {
        let data_source = build_dql_data_source(ex_plan, local_sender_id()?)?;
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

    let lua_table = lua_custom_plan_dispatch(
        lua,
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

fn row_len(ex_plan: &ExecutionPlan) -> SqlResult<u32> {
    let ir_plan = ex_plan.get_ir_plan();
    let columns_len = ir_plan
        .get_row_list(ir_plan.get_relation_node(ir_plan.get_top()?)?.output())?
        .len();
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
            return Err(SbroadError::DispatchError(format_smolstr!(
                "Exceeded maximum number of rows ({max_rows}) in virtual table: {row_count}"
            )));
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
    if len > row_len {
        return Err(IoError::other(format!(
            "Expected array of length at most {row_len}, got {len}",
        )));
    }
    if len < row_len {
        // When msgpack has been formed from Lua dump callback in the
        // executor's port, its last NULLs are omitted. We will need
        // to append nils to the end of the array.
        let extra_nils = row_len - len;
        let mut buf = Vec::with_capacity(5 + mp.len() + extra_nils as usize);
        write_array_len(&mut buf, len + extra_nils).map_err(IoError::other)?;
        buf.extend_from_slice(&mp[cur.position() as usize..]);
        buf.resize(buf.len() + extra_nils as usize, 0xc0); // nils
        port.add_mp(buf.as_slice());
        return Ok(());
    }
    port.add_mp(mp);
    Ok(())
}

fn replicasets_from_buckets(
    lua: &LuaThread,
    buckets: &Buckets,
    tier: Option<&str>,
) -> SqlResult<Vec<String>> {
    let iter = match buckets {
        Buckets::Any => {
            return Err(SbroadError::DispatchError(
                "there is no sense to trnaslate 'any' buckets into replicasets".into(),
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
    let mut replicasets: Vec<Rc<String>> = Vec::new();
    // Make sure that only replicasets owns reference to its Rc elements.
    {
        let mut seen: AHashSet<Rc<String>> = AHashSet::new();

        for id in iter {
            let rs: String = bucket_into_rs(lua, *id, tier).map_err(|e| {
                SbroadError::DispatchError(format_smolstr!(
                    "Failed to get replicaset from bucket {id}: {e}"
                ))
            })?;

            if seen.contains(&rs) {
                continue;
            }
            let rc: Rc<String> = Rc::<String>::from(rs);
            seen.insert(rc.clone());
            replicasets.push(rc);
        }
    }

    let replicasets: Vec<String> = replicasets
        .into_iter()
        .map(|rc| Rc::try_unwrap(rc).expect("Extra Rc is alive"))
        .collect();

    Ok(replicasets)
}

fn buckets_by_replicasets(
    lua: &LuaThread,
    buckets: &Buckets,
    max_buckets: u64,
    tier: Option<&str>,
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
    let mut map: AHashMap<String, Vec<u64>> = AHashMap::new();
    for id in iter {
        let rs: String = bucket_into_rs(lua, id, tier).map_err(|e| {
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
fn build_dml_request(ex_plan: ExecutionPlan) -> SqlResult<DmlRequest> {
    let plan = ex_plan.get_ir_plan();
    let top_id = plan.get_top()?;
    let relation_node = plan.get_relation_node(top_id)?;

    // If DML child is a motion with local policy, data will be processed in storage.
    // Plan should be stored for cache miss handling.
    let with_dql = if let Children::Single(child) = relation_node.children() {
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
                request_id: ex_plan.get_request_id().to_smolstr(),
                space_id: table_id,
                space_version: table_version,
            };

            match child {
                Some(child) => {
                    debug_assert!(with_dql, "Delete filtered works only with DQL");

                    let types = vtable_columns(plan, *child)?;
                    let pattern = init_delete_tuple_builder(plan, top_id)?;
                    let plan = build_dql_data_source(ex_plan, local_sender_id()?)
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
                request_id: ex_plan.get_request_id().to_smolstr(),
                space_id: table_id,
                space_version: table_version,
                conflict_policy: conflict_strategy.into(),
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
                let plan = build_dql_data_source(ex_plan, local_sender_id()?)
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
                request_id: ex_plan.get_request_id().to_smolstr(),
                space_id: table_id,
                space_version: table_version,
                update_type: strategy.into(),
            };

            match strategy {
                UpdateStrategy::ShardedUpdate { delete_tuple_len } => {
                    debug_assert!(!with_dql, "ShardedUpdate cannot be used with DQL");
                    let delete_tuple_len =
                        delete_tuple_len.expect("ShardedUpdate must have delete_tuple_len");
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
                    let plan = build_dql_data_source(ex_plan, local_sender_id()?)
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

fn single_plan_dispatch_dml<'lua, 'p>(
    port: &mut impl Port<'p>,
    lua: &'lua LuaThread,
    ex_plan: ExecutionPlan,
    replicasets: &[String],
    timeout: u64,
    tier: Option<&str>,
) -> SqlResult<()> {
    // This option is available only for DQL.
    let read_preference = ReadPreference::default().to_string();
    let (message, new_plan) = build_dml_request(ex_plan)?.into_message()?;
    let _guard = put_query_meta(new_plan)?;

    let lua_table = lua_single_plan_dispatch(
        lua,
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

fn custom_plan_dispatch_dml<'lua, 'p>(
    port: &mut impl Port<'p>,
    lua: &'lua LuaThread,
    ex_plan: ExecutionPlan,
    rs_buckets: Vec<(String, Vec<u64>)>,
    timeout: u64,
    tier: Option<&str>,
) -> SqlResult<()> {
    let read_preference = ReadPreference::default().to_string();
    let (rs_plan, _) = prepare_rs_to_ir_map(&rs_buckets, ex_plan)?;
    let mut dql_encoder = None;
    let mut args = HashMap::with_capacity(rs_plan.len());
    for (rs, ex_plan) in rs_plan {
        let (message, new_plan) = build_dml_request(ex_plan)?.into_message()?;
        dql_encoder = new_plan;
        args.insert(rs, message);
    }
    let _guard = put_query_meta(dql_encoder)?;
    let len = args.len();
    let lua_table = lua_custom_plan_dispatch(lua, args, timeout, tier, read_preference, false)
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
    serde_json::to_string(&v).map_err(|e| format!("{e}"))
}
