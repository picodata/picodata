use crate::metrics::{
    report_storage_cache_hit, report_storage_cache_miss, STORAGE_2ND_REQUESTS_TOTAL,
};
use crate::sql::lua::{lua_decode_ibufs, lua_query_metadata};
use crate::sql::router::get_table_version_by_id;
use crate::sql::PicoPortC;
use crate::tlog;
use crate::traft::{node, RaftId};
use ahash::HashMapExt;
use rmp::decode::read_array_len;
use rmp::encode::{write_array_len, write_str, write_str_len, write_uint};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use sql::backend::sql::space::ADMIN_ID;
use sql::errors::Entity::MsgPack;
use sql::errors::{Action, Entity, SbroadError};
use sql::executor::engine::helpers::{
    build_insert_args, init_delete_tuple_builder, init_insert_tuple_builder,
    init_local_update_tuple_builder, init_sharded_update_tuple_builder, old_populate_table,
    pk_name, table_name, truncate_tables, vtable_columns, FullPlanInfo, QueryInfo,
    RequiredPlanInfo, TupleBuilderCommand, TupleBuilderPattern,
};
use sql::executor::engine::{QueryCache, StorageCache, TableVersionMap, Vshard};
use sql::executor::ir::QueryType;
use sql::executor::protocol::{OptionalData, RequiredData, SchemaInfo};
use sql::executor::result::ConsumerResult;
use sql::executor::vdbe::ExecutionInsight::{BusyStmt, Nothing, StaleStmt};
use sql::executor::vdbe::{ExecutionInsight, SqlError, SqlStmt};
use sql::executor::vtable::{VTableTuple, VirtualTable, VirtualTableMeta};
use sql::executor::{Port, PortType};
use sql::ir::node::relational::Relational;
use sql::ir::operator::ConflictStrategy;
use sql::ir::value::{EncodedValue, MsgPackValue, Value};
use sql::ir::{node::NodeId, relation::SpaceEngine};
use sql::utils::MutexLike;
use sql_protocol::decode::{ProtocolMessage, ProtocolMessageIter};
use sql_protocol::dql::{DQLCacheMissResult, DQLPacketPayloadIterator, DQLResult};
use sql_protocol::dql_encoder::ColumnType;
use sql_protocol::error::ProtocolError;
use sql_protocol::iterators::TupleIterator;
use std::io::{Cursor, Write};
use std::sync::OnceLock;
use tarantool::error::{Error, TarantoolErrorCode};
use tarantool::ffi::sql::Port as TarantoolPort;
use tarantool::index::{FieldType, IndexOptions, IndexType, Part};
use tarantool::session::with_su;
use tarantool::space::{Field, Space, SpaceCreateOptions, SpaceType};
use tarantool::transaction::transaction;

const LINE_WIDTH: usize = 80;

#[macro_export]
macro_rules! protocol_get {
    ( $s:expr, $x:path ) => {{
        let Some(val) = $s.next() else {
            return Err(SbroadError::Invalid(
                Entity::MsgPack,
                Some(format_smolstr!(
                    "Expected {}, but iterator is exceed",
                    stringify!($x)
                )),
            ));
        };

        let val = val?;

        let $x(val) = val else {
            return Err(SbroadError::Invalid(
                Entity::MsgPack,
                Some(format_smolstr!("Expected {}, got {}", stringify!($x), val)),
            ));
        };

        val
    }};
}

fn populate_table(table_name: &str, tuples: TupleIterator) -> Result<(), SbroadError> {
    with_su(ADMIN_ID, || -> Result<(), SbroadError> {
        let space = Space::find(table_name).ok_or_else(|| {
            // See https://git.picodata.io/core/picodata/-/issues/1859.
            SbroadError::Invalid(
                Entity::Space,
                Some(format_smolstr!(
                    "Temporary SQL table {table_name} not found. \
                    Probably there are unused motions in the plan"
                )),
            )
        })?;
        for tuple in tuples {
            let tuple = tuple?;
            unsafe { space.insert_unchecked(tuple) }.map_err(|e|
                // It is possible that the temporary table was recreated by admin
                // user with a different format. We should not panic in this case.
                SbroadError::FailedTo(
                Action::Insert,
                Some(Entity::Tuple),
                format_smolstr!("tuple {tuple:?}, temporary table {table_name}: {e}"),
            ))?;
        }
        Ok(())
    })??;
    Ok(())
}

/// Execute a DML query on the storage.
pub(crate) fn dml_execute<'p, R: Vshard + QueryCache>(
    runtime: &R,
    required: &mut RequiredData,
    mut optional: OptionalData,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<SmolStr, NodeId>,
{
    if required.query_type != QueryType::DML {
        return Err(SbroadError::Invalid(
            Entity::Plan,
            Some("Expected a DML plan.".to_smolstr()),
        ));
    }
    let plan = optional.exec_plan.get_ir_plan();
    let top_id = plan.get_top()?;
    let top = plan.get_relation_node(top_id)?;
    match top {
        Relational::Insert(_) => insert_execute(runtime, &mut optional, required, port),
        Relational::Delete(_) => delete_execute(runtime, &mut optional, required, port),
        Relational::Update(_) => update_execute(runtime, &mut optional, required, port),
        _ => Err(SbroadError::Invalid(
            Entity::Plan,
            Some(format_smolstr!(
                "expected DML node on the plan top, got {top:?}"
            )),
        )),
    }?;
    port.set_type(PortType::ExecuteDml);
    Ok(())
}

/// Execute the first round request for DQL query on storage
/// with an attempt to hit the cache.
pub(crate) fn dql_execute_first_round<'p, R: Vshard + QueryCache>(
    runtime: &R,
    info: &mut impl RequiredPlanInfo,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<SmolStr, NodeId>,
{
    let mut cache_guarded = runtime.cache().lock();
    if let Some((stmt, motion_ids)) = cache_guarded.get(info.id())? {
        // Transaction rollbacks are very expensive in Tarantool, so we're going to
        // avoid transactions for DQL queries. We can achieve atomicity by truncating
        // temporary tables. Isolation is guaranteed by keeping a lock on the cache.
        use ExecutionInsight::*;
        match stmt_execute(stmt, info, motion_ids, port)? {
            Nothing => report_storage_cache_hit("dql", "1st"),
            BusyStmt => report_storage_cache_miss("dql", "1st", "busy"),
            StaleStmt => report_storage_cache_miss("dql", "1st", "stale"),
        }
        port.set_type(PortType::ExecuteDql);
    } else {
        // Response with a cache miss. The router will retry the query
        // on the second round.
        report_storage_cache_miss("dql", "1st", "true");
        port.set_type(PortType::ExecuteMiss);
    }

    Ok(())
}

pub(crate) fn dql_execute_second_round<'p, R: QueryCache>(
    runtime: &R,
    info: &mut impl FullPlanInfo,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<SmolStr, NodeId>,
{
    use ExecutionInsight::*;
    let mut cache_guarded = runtime.cache().lock();
    if let Some((stmt, motion_ids)) = cache_guarded.get(info.id())? {
        // Transaction rollbacks are very expensive in Tarantool, so we're going to
        // avoid transactions for DQL queries. We can achieve atomicity by truncating
        // temporary tables. Isolation is guaranteed by keeping a lock on the cache.
        match stmt_execute(stmt, info, motion_ids, port)? {
            Nothing => report_storage_cache_hit("dql", "2nd"),
            BusyStmt => report_storage_cache_miss("dql", "2nd", "busy"),
            StaleStmt => report_storage_cache_miss("dql", "2nd", "stale"),
        }
    } else {
        match old_sql_execute::<R>(&mut cache_guarded, info, port)? {
            Nothing => report_storage_cache_miss("dql", "2nd", "true"),
            BusyStmt => report_storage_cache_miss("dql", "2nd", "busy"),
            StaleStmt => report_storage_cache_miss("dql", "2nd", "stale"),
        }
    }

    // We don't set port type here, because this code can be called
    // for dispatching (on any node) as well as for local execution.
    Ok(())
}

// must be executed under lock
fn dql_execute_impl<'p, 'b>(
    stmt: &mut SqlStmt,
    mut info: impl Iterator<Item = Result<DQLResult<'b>, ProtocolError>>,
    port: &mut impl Port<'p>,
) -> Result<ExecutionInsight, SbroadError> {
    // populate vtables
    let vtables = protocol_get!(info, DQLResult::Vtables);

    let has_metadata = port.size() == 1;
    let mut tables_names = Vec::with_capacity(vtables.len() as usize);
    let mut max_rows = 0;
    let pcall = || -> Result<ExecutionInsight, SbroadError> {
        for vtable_data in vtables {
            let (vtable, tuples) = vtable_data?;
            populate_table(vtable, tuples)?;
            tables_names.push(vtable);
        }

        let (sql_motion_row_max, sql_vdbe_opcode_max) = protocol_get!(info, DQLResult::Options);

        max_rows = sql_motion_row_max;

        let params = protocol_get!(info, DQLResult::Params);

        with_su(ADMIN_ID, || -> Result<ExecutionInsight, SqlError> {
            port.process_stmt_with_raw_params(stmt, params, sql_vdbe_opcode_max)
        })?
        .map_err(SbroadError::from)
    };

    let res = pcall();
    with_su(ADMIN_ID, || {
        for name in tables_names {
            if let Some(space) = Space::find(name) {
                space
                    .truncate()
                    .expect("failed to truncate temporary table");
            }
        }
    })
    .expect("failed to switch to admin user");
    let res = res?;

    // We should check if we exceed the maximum number of rows.
    if port.size() > 0 {
        let current_rows = port.size() - if has_metadata { 1 } else { 0 }; // exclude metadata tuple
        if max_rows > 0 && current_rows as u64 > max_rows {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "Exceeded maximum number of rows ({}) in virtual table: {}",
                max_rows,
                current_rows
            )));
        }
    }

    Ok(res)
}

/// Handle a cache miss for a DQL query.
///
/// Requests additional data from the router to prepare the query execution plan.
/// After successful preparation, the plan is added to cache and execution resumes
/// with cache hit processing.
fn dql_cache_miss_execute<'p, 'b, R: Vshard + QueryCache>(
    runtime: &R,
    request_id: &str,
    plan_id: u64,
    mut info: DQLPacketPayloadIterator<'b>,
    port: &mut impl Port<'p>,
    timeout: f64,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<u64, SmolStr>,
{
    let sender_id = protocol_get!(info, DQLResult::SenderId);

    let node = node::global().expect("should be init");
    let raft_id: RaftId = sender_id.parse::<u64>().map_err(|e| {
        SbroadError::Invalid(MsgPack, Some(format_smolstr!("Invalid sender id: {}", e)))
    })?;

    let instance = with_su(ADMIN_ID, || node.storage.instances.get(&raft_id))?
        .map_err(|e| SbroadError::DispatchError(format_smolstr!("Failed to get instance: {e}")))?;

    let lua = tarantool::lua_state();
    let table = lua_query_metadata(
        &lua,
        instance.tier.as_str(),
        instance.replicaset_uuid.as_str(),
        instance.uuid.as_str(),
        request_id,
        plan_id,
        timeout,
    )?;

    let rs_ibufs = lua_decode_ibufs(&table, 1).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!(
            "Failed to decode ibufs from DQL cache miss {e}"
        ))
    })?;

    debug_assert_eq!(rs_ibufs.len(), 1);
    let rs_ibuf = &rs_ibufs[0];
    let mut buf = rs_ibuf
        .data()
        .map_err(|e| SbroadError::DispatchError(format_smolstr!("Failed to decode ibuf {e}")))?;
    let len = read_array_len(&mut buf).map_err(|e| SbroadError::Other(e.to_smolstr()))?;
    if len != 1 {
        return Err(SbroadError::Other(format_smolstr!(
            "Expected array of length 1 from pcall result, got {len}",
        )));
    }
    let mut cache_miss_info = sql_protocol::dql::DQLCacheMissPayloadIterator::new(buf)?;

    let schema_info = protocol_get!(cache_miss_info, DQLCacheMissResult::SchemaInfo);

    let mut table_versions = TableVersionMap::with_capacity(schema_info.len() as usize);
    for schema in schema_info {
        let (table, version) = schema?;
        if version != get_table_version_by_id(table)? {
            return Err(SbroadError::OutdatedStorageSchema);
        }
        table_versions.insert(table, version);
    }

    let mut cache_guarded = runtime.cache().lock();

    if let Some((stmt, _motion_ids)) = cache_guarded.get(&plan_id)? {
        match dql_execute_impl(stmt, info, port)? {
            Nothing => report_storage_cache_hit("dql", "2nd"),
            BusyStmt => report_storage_cache_miss("dql", "2nd", "busy"),
            StaleStmt => report_storage_cache_miss("dql", "2nd", "stale"),
        }

        Ok(())
    } else {
        match sql_execute::<R>(
            &mut cache_guarded,
            plan_id,
            &SchemaInfo::new(table_versions),
            info,
            cache_miss_info,
            port,
        )? {
            Nothing => report_storage_cache_miss("dql", "2nd", "true"),
            BusyStmt => report_storage_cache_miss("dql", "2nd", "busy"),
            StaleStmt => report_storage_cache_miss("dql", "2nd", "stale"),
        }

        Ok(())
    }
}

fn sql_execute<'a, 'p, R: QueryCache>(
    cache_guarded: &mut <<R as QueryCache>::Mutex as MutexLike<R::Cache>>::Guard<'_>,
    plan_id: u64,
    schema_info: &SchemaInfo,
    info: impl Iterator<Item = Result<DQLResult<'a>, ProtocolError>>,
    mut cache_miss_info: impl Iterator<Item = Result<DQLCacheMissResult<'a>, ProtocolError>>,
    port: &mut impl Port<'p>,
) -> Result<ExecutionInsight, SbroadError>
where
    R::Cache: StorageCache<u64, SmolStr>,
{
    let metadata = protocol_get!(cache_miss_info, DQLCacheMissResult::VtablesMetadata);

    let mut tables = Vec::with_capacity(metadata.len() as usize);
    for data in metadata {
        let (name, meta) = data?;
        let pk_name = format!("PK_{}", name.strip_prefix("TMP_").unwrap_or(name));
        table_create(name, pk_name.as_str(), meta)?;
        tables.push(name.to_smolstr());
    }

    let sql = protocol_get!(cache_miss_info, DQLCacheMissResult::Sql);

    let stmt = SqlStmt::compile(sql).inspect_err(|e| {
        tlog!(
            Warning,
            "Failed to compile statement for the query '{sql}': {e}"
        )
    })?;
    cache_guarded.put(plan_id, stmt, schema_info, tables)?;

    let Some((stmt, _)) = cache_guarded.get(&plan_id)? else {
        unreachable!("was just added, should be in the cache")
    };

    dql_execute_impl(stmt, info, port)
}

/// Execute a DQL query on storage.
///
/// The query is checked for presence in the cache. If not found, a cache miss
/// request is performed and the query is added to the cache for future use.
pub(crate) fn dql_execute<'p, R: Vshard + QueryCache>(
    runtime: &R,
    msg: &ProtocolMessage,
    port: &mut impl Port<'p>,
    timeout: f64,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<u64, SmolStr>,
{
    let ProtocolMessageIter::Dql(mut info) = msg.get_iter()? else {
        unreachable!("should be dql iterator")
    };

    let schema_info = protocol_get!(info, DQLResult::SchemaInfo);

    for schema in schema_info {
        let (table, version) = schema?;
        if version != get_table_version_by_id(table)? {
            return Err(SbroadError::OutdatedStorageSchema);
        }
    }

    let plan_id = protocol_get!(info, DQLResult::PlanId);

    let mut cache_guarded = runtime.cache().lock();
    if let Some((stmt, _)) = cache_guarded.get(&plan_id)? {
        // skip it
        let _ = protocol_get!(info, DQLResult::SenderId);

        match dql_execute_impl(stmt, info, port)? {
            Nothing => report_storage_cache_hit("dql", "1st"),
            BusyStmt => report_storage_cache_miss("dql", "1st", "busy"),
            StaleStmt => report_storage_cache_miss("dql", "1st", "stale"),
        }

        port.set_type(PortType::ExecuteDql);
        Ok(())
    } else {
        drop(cache_guarded);
        report_storage_cache_miss("dql", "1st", "true");

        let res = dql_cache_miss_execute(runtime, msg.request_id, plan_id, info, port, timeout);
        port.set_type(PortType::ExecuteDql);
        let result = if res.is_err() { "err" } else { "ok" };
        STORAGE_2ND_REQUESTS_TOTAL
            .with_label_values(&["dql", result])
            .inc();
        res
    }
}

pub fn explain_execute<'p, R: QueryCache>(
    runtime: &R,
    info: &mut impl FullPlanInfo,
    formatted: bool,
    location: &str,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<SmolStr, NodeId>,
{
    let _lock: <<R as QueryCache>::Mutex as MutexLike<<R as QueryCache>::Cache>>::Guard<'_> =
        runtime.cache().lock();
    let (explain, motion_ids, vtables_meta) = info.take_query_meta()?;

    // EXPLAIN QUERY PLAN output formatting for each tuple in the port:
    // - [int, int, int , string] (selectid, order, from, detail).
    // We also wish to add to the port additional headers:
    // - query location
    // - sql text
    // As we operate with SQL tables, we should repack the headers to the
    // same format as the tuples in the port, i.e.:
    // - [-1, -1, -1, query location]
    // - [-2, -2, -2, sql]
    // It is the responsibility of the port virtual table to repack the
    // results in a user-friendly way.

    let mp_header = {
        let mut mp = Vec::with_capacity(4 + 5 + location.len());
        mp.extend_from_slice(b"\x94\xff\xff\xff");
        write_str_len(&mut mp, location.len() as u32).map_err(|e| {
            SbroadError::Invalid(
                Entity::MsgPack,
                Some(format_smolstr!(
                    "Failed to write the length of explain header: {e}"
                )),
            )
        })?;
        mp.extend_from_slice(location.as_bytes());
        mp
    };
    port.add_mp(mp_header.as_slice());

    let mp_sql = {
        let sql = &explain["EXPLAIN QUERY PLAN ".len()..];
        let mut fmt_options = sqlformat::FormatOptions::default();
        if !formatted || sql.len() < LINE_WIDTH {
            fmt_options.joins_as_top_level = true;
            fmt_options.inline = true;
        }
        let params = info
            .params()
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<String>>();
        let sql_fmt =
            sqlformat::format(sql, &sqlformat::QueryParams::Indexed(params), &fmt_options);
        let mut mp = Vec::with_capacity(4 + 5 + sql_fmt.len());
        mp.extend_from_slice(b"\x94\xfe\xfe\xfe");
        write_str(&mut mp, &sql_fmt).map_err(|e| {
            SbroadError::Invalid(
                Entity::MsgPack,
                Some(format_smolstr!("Failed to write explain SQL: {e}")),
            )
        })?;
        mp
    };
    port.add_mp(mp_sql.as_slice());

    for motion_id in &motion_ids {
        let table_name = table_name(info.id(), *motion_id);
        let pk_name = pk_name(info.id(), *motion_id);
        let meta = vtables_meta.get(motion_id).ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Plan,
                Some(format_smolstr!("missing metadata for motion {motion_id}")),
            )
        })?;
        old_table_create(&table_name, &pk_name, meta)?;
    }

    let mut stmt = SqlStmt::compile(&explain)?;
    port.process_stmt(&mut stmt, info.params(), info.sql_vdbe_opcode_max())?;
    Ok(())
}

fn plan_execute<'p, R: QueryCache>(
    runtime: &R,
    info: &mut impl FullPlanInfo,
    port: &mut impl Port<'p>,
) -> Result<ExecutionInsight, SbroadError>
where
    R::Cache: StorageCache<SmolStr, NodeId>,
{
    let mut cache_guarded: <<R as QueryCache>::Mutex as MutexLike<<R as QueryCache>::Cache>>::Guard<'_> = runtime.cache().lock();
    if let Some((stmt, motion_ids)) = cache_guarded.get(info.id())? {
        // Transaction rollbacks are very expensive in Tarantool, so we're going to
        // avoid transactions for DQL queries. We can achieve atomicity by truncating
        // temporary tables. Isolation is guaranteed by keeping a lock on the cache.
        stmt_execute(stmt, info, motion_ids, port)
    } else {
        old_sql_execute::<R>(&mut cache_guarded, info, port)
    }
}

fn table_create_impl(name: &str, pk_name: &str, fields: Vec<Field>) -> Result<(), SbroadError> {
    let cleanup = |space: Space, name: &str| {
        if let Err(e) = with_su(ADMIN_ID, || space.drop()) {
            tlog!(Error, "Failed to drop temporary table {name}: {e}")
        }
    };

    // If the space already exists, it is possible that admin has
    // populated it with data (by mistake?). Clean the space up.
    if let Some(space) = with_su(ADMIN_ID, || Space::find(name))? {
        cleanup(space, name);
    }

    let options = SpaceCreateOptions {
        format: Some(fields),
        engine: SpaceEngine::Memtx.into(),
        space_type: SpaceType::Temporary,
        if_not_exists: false,
        ..Default::default()
    };

    let pk = IndexOptions {
        r#type: Some(IndexType::Tree),
        unique: Some(true),
        parts: Some(vec![Part::new(pk_name.to_string(), FieldType::Unsigned)]),
        if_not_exists: Some(false),
        ..Default::default()
    };

    let space = with_su(ADMIN_ID, || -> Result<Space, SbroadError> {
        Space::create(name, &options).map_err(|e| {
            SbroadError::FailedTo(
                Action::Create,
                Some(Entity::Space),
                format_smolstr!("{name}: {e}"),
            )
        })
    })??;
    let create_index_res = with_su(ADMIN_ID, || space.create_index(pk_name, &pk));
    match create_index_res {
        Ok(Ok(_)) => {}
        Err(e) | Ok(Err(e)) => {
            cleanup(space, name);
            return Err(SbroadError::FailedTo(
                Action::Create,
                Some(Entity::Index),
                format_smolstr!("{pk_name} for table {name}: {e}"),
            ));
        }
    }

    Ok(())
}

/// Create a temporary table. It wraps all Space API with `with_su`
/// since user may have no permissions to read/write tables.
fn old_table_create(name: &str, pk_name: &str, meta: &VirtualTableMeta) -> Result<(), SbroadError> {
    let mut fields: Vec<Field> = meta
        .columns
        .iter()
        .map(|c| Field::from(c.clone()))
        .collect();

    fields.push(Field::unsigned(pk_name));

    table_create_impl(name, pk_name, fields)
}
/// Create a temporary table. It wraps all Space API with `with_su`
/// since user may have no permissions to read/write tables.
fn table_create(
    name: &str,
    pk_name: &str,
    columns: Vec<(&str, ColumnType)>,
) -> Result<(), SbroadError> {
    let mut fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type)| {
            match column_type {
                ColumnType::Map => Field::map(*name),
                ColumnType::Boolean => Field::boolean(*name),
                ColumnType::Datetime => Field::datetime(*name),
                ColumnType::Decimal => Field::decimal(*name),
                ColumnType::Double => Field::double(*name),
                ColumnType::Integer => Field::integer(*name),
                ColumnType::String => Field::string(*name),
                ColumnType::Uuid => Field::uuid(*name),
                ColumnType::Any => Field::any(*name),
                ColumnType::Array => Field::array(*name),
                ColumnType::Scalar => Field::scalar(*name),
            }
            .is_nullable(true)
        })
        .collect();

    fields.push(Field::unsigned(pk_name));

    table_create_impl(name, pk_name, fields)
}

// Requires the cache to be locked.
pub fn stmt_execute<'p>(
    stmt: &mut SqlStmt,
    info: &mut impl RequiredPlanInfo,
    motion_ids: &[NodeId],
    port: &mut impl Port<'p>,
) -> Result<ExecutionInsight, SbroadError> {
    let vtables = info.extract_data();
    scopeguard::defer!(
        truncate_tables(motion_ids, info.id());
    );

    let has_metadata = port.size() == 1;
    for motion_id in motion_ids {
        old_populate_table(motion_id, info.id(), &vtables)?;
    }

    let res = port.process_stmt(stmt, info.params(), info.sql_vdbe_opcode_max())?;

    // We should check if we exceed the maximum number of rows.
    if port.size() > 0 {
        let max_rows = info.sql_motion_row_max();
        let current_rows = port.size() - if has_metadata { 1 } else { 0 }; // exclude metadata tuple
        if max_rows > 0 && current_rows as u64 > max_rows {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "Exceeded maximum number of rows ({}) in virtual table: {}",
                max_rows,
                current_rows
            )));
        }
    }

    Ok(res)
}

// Requires the cache to be locked.
pub fn old_sql_execute<'p, R: QueryCache>(
    cache_guarded: &mut <<R as QueryCache>::Mutex as MutexLike<R::Cache>>::Guard<'_>,
    info: &mut impl FullPlanInfo,
    port: &mut impl Port<'p>,
) -> Result<ExecutionInsight, SbroadError>
where
    R::Cache: StorageCache<SmolStr, NodeId>,
{
    let (local_sql, motion_ids, vtables_meta) = info.take_query_meta()?;
    for motion_id in &motion_ids {
        let table_name = table_name(info.id(), *motion_id);
        let pk_name = pk_name(info.id(), *motion_id);
        let meta = vtables_meta.get(motion_id).ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Plan,
                Some(format_smolstr!("missing metadata for motion {motion_id}")),
            )
        })?;
        old_table_create(&table_name, &pk_name, meta)?;
    }

    let stmt = SqlStmt::compile(&local_sql).inspect_err(|e| {
        tlog!(
            Warning,
            "Failed to compile statement for the query '{local_sql}': {e}"
        )
    })?;
    cache_guarded.put(
        info.id().clone(),
        stmt,
        info.schema_info(),
        motion_ids.clone(),
    )?;

    let (stmt, motion_ids) = cache_guarded.get(info.id())?.unwrap();
    stmt_execute(stmt, info, motion_ids, port)
}

/// Helper function to materialize a virtual table on storage.
fn virtual_table_materialize<R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
    child_id: NodeId,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<SmolStr, NodeId>,
{
    let mut info = QueryInfo::new(optional, required);
    let mut port = TarantoolPort::new_port_c();
    let mut pico_port = PicoPortC::from(unsafe { port.as_mut_port_c() });
    plan_execute::<R>(runtime, &mut info, &mut pico_port)?;
    let ir_plan = optional.exec_plan.get_ir_plan();
    let columns = vtable_columns(ir_plan, child_id)?;

    let mut vtable = VirtualTable::with_columns(columns);
    for tuple in pico_port.iter() {
        vtable.write_all(tuple).map_err(|e| {
            SbroadError::Invalid(
                Entity::VirtualTable,
                Some(format_smolstr!(
                    "failed to write a tuple to the virtual table: {e}"
                )),
            )
        })?;
    }

    optional
        .exec_plan
        .set_motion_vtable(&child_id, vtable, runtime)?;
    Ok(())
}

fn update_execute<'p, R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<SmolStr, NodeId>,
{
    let plan = optional.exec_plan.get_ir_plan();
    let update_id = plan.get_top()?;
    let update_child_id = plan.dml_child_id(update_id)?;
    let space_name = plan.dml_node_table(update_id)?.name().clone();
    let mut result = ConsumerResult::default();
    let is_sharded = plan.is_sharded_update(update_id)?;
    let build_vtable_locally = !optional
        .exec_plan
        .contains_vtable_for_motion(update_child_id);
    if build_vtable_locally {
        // This is relevant only for local Update.
        if is_sharded {
            return Err(SbroadError::Invalid(
                Entity::Update,
                Some("sharded Update's vtable must be already materialized".into()),
            ));
        }
        virtual_table_materialize(runtime, optional, required, update_child_id)?;
    }
    let vtable = optional.exec_plan.get_motion_vtable(update_child_id)?;
    let space = Space::find(&space_name).ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Space,
            Some(format_smolstr!("space {space_name} not found")),
        )
    })?;
    transaction(|| -> Result<(), SbroadError> {
        let plan = optional.exec_plan.get_ir_plan();
        if is_sharded {
            let delete_tuple_len = plan.get_update_delete_tuple_len(update_id)?;
            let builder = init_sharded_update_tuple_builder(plan, &vtable, update_id)?;
            sharded_update_execute(&mut result, &vtable, &space, &builder, delete_tuple_len)?;
        } else {
            let builder = init_local_update_tuple_builder(plan, &vtable, update_id)?;
            local_update_execute(&mut result, &builder, &vtable, &space)?;
        }
        Ok(())
    })?;
    port_write_execute_dml(port, result.row_count);

    Ok(())
}

/// A working horse for `execute_update_on_storage` in case we're dealing with
/// sharded update.
fn sharded_update_execute(
    result: &mut ConsumerResult,
    vtable: &VirtualTable,
    space: &Space,
    builder: &TupleBuilderPattern,
    delete_tuple_len: usize,
) -> Result<(), SbroadError> {
    for tuple in vtable.get_tuples() {
        if tuple.len() == delete_tuple_len {
            let pk: Vec<EncodedValue> = tuple
                .iter()
                .map(|val| EncodedValue::Ref(MsgPackValue::from(val)))
                .collect();
            if let Err(Error::Tarantool(tnt_err)) = space.delete(&pk) {
                return Err(SbroadError::FailedTo(
                    Action::Delete,
                    Some(Entity::Tuple),
                    format_smolstr!("{tnt_err:?}"),
                ));
            }
        }
    }
    for (bucket_id, positions) in vtable.get_bucket_index() {
        for pos in positions {
            let vt_tuple = vtable.get_tuples().get(*pos).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::VirtualTable,
                    Some(format_smolstr!("invalid tuple position in index: {pos}")),
                )
            })?;

            if vt_tuple.len() != delete_tuple_len {
                let mut insert_tuple: Vec<EncodedValue> = Vec::with_capacity(builder.len());
                for command in builder {
                    match command {
                        TupleBuilderCommand::TakePosition(tuple_pos) => {
                            let value = vt_tuple.get(*tuple_pos).ok_or_else(|| {
                                SbroadError::Invalid(
                                    Entity::Tuple,
                                    Some(format_smolstr!(
                                        "column at position {pos} not found in virtual table"
                                    )),
                                )
                            })?;
                            insert_tuple.push(EncodedValue::Ref(value.into()));
                        }
                        TupleBuilderCommand::TakeAndCastPosition(tuple_pos, table_type) => {
                            let value = vt_tuple.get(*tuple_pos).ok_or_else(|| {
                                SbroadError::Invalid(
                                    Entity::Tuple,
                                    Some(format_smolstr!(
                                        "column at position {pos} not found in virtual table"
                                    )),
                                )
                            })?;
                            insert_tuple.push(value.cast_and_encode(table_type)?);
                        }
                        TupleBuilderCommand::CalculateBucketId(_) => {
                            let bucket_id = i64::try_from(*bucket_id).map_err(|_| {
                                SbroadError::Invalid(
                                    Entity::Value,
                                    Some(format_smolstr!(
                                        "value for column 'bucket_id' is too large to fit in integer type range"
                                    )),
                                )
                            })?;

                            insert_tuple.push(EncodedValue::Owned(Value::Integer(bucket_id)));
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::TupleBuilderCommand,
                                Some(format_smolstr!("got command {command:?} for update insert")),
                            ));
                        }
                    }
                }
                // We can have multiple rows with the same primary key,
                // so replace is used.
                if let Err(e) = space.replace(&insert_tuple) {
                    return Err(SbroadError::FailedTo(
                        Action::Insert,
                        Some(Entity::Tuple),
                        format_smolstr!("{e:?}"),
                    ));
                }
                result.row_count += 1;
            }
        }
    }

    Ok(())
}

struct UpdateArgs<'vtable_tuple> {
    key_tuple: Vec<EncodedValue<'vtable_tuple>>,
    ops: Vec<[EncodedValue<'vtable_tuple>; 3]>,
}

fn eq_op() -> &'static Value {
    // Once lock is used because of concurrent access in tests.
    static EQ: OnceLock<Value> = OnceLock::new();

    EQ.get_or_init(|| Value::String("=".into()))
}

fn update_args<'t>(
    vt_tuple: &'t VTableTuple,
    builder: &TupleBuilderPattern,
) -> Result<UpdateArgs<'t>, SbroadError> {
    let mut ops = Vec::with_capacity(builder.len());
    let mut key_tuple = Vec::with_capacity(builder.len());
    for command in builder {
        match command {
            TupleBuilderCommand::UpdateColToPos(table_col, pos) => {
                let value = vt_tuple.get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                let op = [
                    EncodedValue::Ref(MsgPackValue::from(eq_op())),
                    // Use `as i64` quite safe here.
                    EncodedValue::Owned(Value::Integer(*table_col as i64)),
                    EncodedValue::Ref(MsgPackValue::from(value)),
                ];
                ops.push(op);
            }
            TupleBuilderCommand::TakePosition(pos) => {
                let value = vt_tuple.get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                key_tuple.push(EncodedValue::Ref(MsgPackValue::from(value)));
            }
            TupleBuilderCommand::TakeAndCastPosition(pos, table_type) => {
                let value = vt_tuple.get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                key_tuple.push(value.cast_and_encode(table_type)?);
            }
            TupleBuilderCommand::UpdateColToCastedPos(table_col, pos, table_type) => {
                let value = vt_tuple.get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "column at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                let op = [
                    EncodedValue::Ref(MsgPackValue::from(eq_op())),
                    // Use `as i64` quite safe here.
                    EncodedValue::Owned(Value::Integer(*table_col as i64)),
                    value.cast_and_encode(table_type)?,
                ];
                ops.push(op);
            }
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::TupleBuilderCommand,
                    Some(format_smolstr!("got command {command:?} for update")),
                ));
            }
        }
    }

    Ok(UpdateArgs { key_tuple, ops })
}

/// A working horse for `execute_update_on_storage` in case we're dealing with
/// nonsharded update.
fn local_update_execute(
    result: &mut ConsumerResult,
    builder: &TupleBuilderPattern,
    vtable: &VirtualTable,
    space: &Space,
) -> Result<(), SbroadError> {
    for vt_tuple in vtable.get_tuples() {
        let args = update_args(vt_tuple, builder)?;
        let update_res = space.update(&args.key_tuple, &args.ops);
        update_res.map_err(|e| {
            SbroadError::FailedTo(Action::Update, Some(Entity::Space), format_smolstr!("{e}"))
        })?;
        result.row_count += 1;
    }
    Ok(())
}

fn delete_args<'t>(
    vt_tuple: &'t VTableTuple,
    builder: &'t TupleBuilderPattern,
) -> Result<Vec<EncodedValue<'t>>, SbroadError> {
    let mut delete_tuple = Vec::with_capacity(builder.len());
    for cmd in builder {
        if let TupleBuilderCommand::TakePosition(pos) = cmd {
            let value = vt_tuple.get(*pos).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Tuple,
                    Some(format_smolstr!(
                        "column at position {pos} not found in the delete virtual table"
                    )),
                )
            })?;
            delete_tuple.push(EncodedValue::Ref(value.into()));
        } else {
            return Err(SbroadError::Invalid(
                Entity::Tuple,
                Some(format_smolstr!(
                    "unexpected tuple builder cmd for delete primary key: {cmd:?}"
                )),
            ));
        }
    }
    Ok(delete_tuple)
}

fn delete_execute<'p, R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<SmolStr, NodeId>,
{
    let plan = optional.exec_plan.get_ir_plan();
    let delete_id = plan.get_top()?;
    let delete_childen = plan.children(delete_id);

    if delete_childen.is_empty() {
        // We have a deal with a DELETE without WHERE filter
        // and want to execute local SQL instead of space api.

        let mut info = QueryInfo::new(optional, required);
        plan_execute::<R>(runtime, &mut info, port)?;
        return Ok(());
    }

    let delete_child_id = delete_childen[0];
    let builder = init_delete_tuple_builder(plan, delete_id)?;
    let space_name = plan.dml_node_table(delete_id)?.name().clone();
    let mut result = ConsumerResult::default();
    let build_vtable_locally = !optional
        .exec_plan
        .contains_vtable_for_motion(delete_child_id);
    if build_vtable_locally {
        virtual_table_materialize(runtime, optional, required, delete_child_id)?;
    }
    let vtable = optional.exec_plan.get_motion_vtable(delete_child_id)?;
    let space = Space::find(&space_name).ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Space,
            Some(format_smolstr!("space {space_name} not found")),
        )
    })?;
    transaction(|| -> Result<(), SbroadError> {
        for vt_tuple in vtable.get_tuples() {
            let delete_tuple = delete_args(vt_tuple, &builder)?;
            if let Err(Error::Tarantool(tnt_err)) = space.delete(&delete_tuple) {
                return Err(SbroadError::FailedTo(
                    Action::Delete,
                    Some(Entity::Tuple),
                    format_smolstr!("{tnt_err:?}"),
                ));
            }
            result.row_count += 1;
        }
        Ok(())
    })?;
    port_write_execute_dml(port, result.row_count);

    Ok(())
}

fn insert_execute<'p, R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<SmolStr, NodeId>,
{
    // We always generate a virtual table under the `INSERT` node
    // of the execution plan and prefer to execute it via space API
    // instead of SQL (for performance reasons).
    let plan = optional.exec_plan.get_ir_plan();
    let insert_id = plan.get_top()?;
    let insert_child_id = plan.dml_child_id(insert_id)?;
    let space_name = plan.dml_node_table(insert_id)?.name().clone();
    let mut result = ConsumerResult::default();

    // There are two ways to execute an `INSERT` query:
    // 1. Execute SQL subtree under the `INSERT` node (`INSERT .. SELECT ..`)
    //    and then repack and insert results into the space.
    // 2. A virtual table was dispatched under the `INSERT` node.
    //    Simply insert its tuples into the space.
    // The same for `UPDATE`.

    // Check is we need to execute an SQL subtree (case 1).
    let build_vtable_locally = !optional
        .exec_plan
        .contains_vtable_for_motion(insert_child_id);
    // XXX: Keep this check in sync with `encode_vtables`.
    if build_vtable_locally {
        virtual_table_materialize(runtime, optional, required, insert_child_id)?;
    }

    // Check if the virtual table have been dispatched (case 2) or built locally (case 1).
    let vtable = optional.exec_plan.get_motion_vtable(insert_child_id)?;
    let space = Space::find(&space_name).ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Space,
            Some(format_smolstr!("space {space_name} not found")),
        )
    })?;
    let plan = optional.exec_plan.get_ir_plan();
    let builder = init_insert_tuple_builder(plan, vtable.as_ref(), insert_id)?;
    let conflict_strategy = optional
        .exec_plan
        .get_ir_plan()
        .insert_conflict_strategy(insert_id)?;
    transaction(|| -> Result<(), SbroadError> {
        for (bucket_id, positions) in vtable.get_bucket_index() {
            for pos in positions {
                let vt_tuple = vtable.get_tuples().get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::VirtualTable,
                        Some(format_smolstr!(
                            "tuple at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                let insert_tuple = build_insert_args(vt_tuple, &builder, Some(bucket_id))?;
                let insert_result = space.insert(&insert_tuple);
                if let Err(Error::Tarantool(tnt_err)) = &insert_result {
                    if tnt_err.error_code() == TarantoolErrorCode::TupleFound as u32 {
                        match conflict_strategy {
                            ConflictStrategy::DoNothing => {
                                tlog!(
                                    Debug,
                                    "failed to insert tuple: {insert_tuple:?}. Skipping according to conflict strategy",
                                );
                            }
                            ConflictStrategy::DoReplace => {
                                tlog!(
                                    Debug,
                                    "failed to insert tuple: {insert_tuple:?}. Trying to replace according to conflict strategy"
                                );
                                space.replace(&insert_tuple).map_err(|e| {
                                    SbroadError::FailedTo(
                                        Action::ReplaceOnConflict,
                                        Some(Entity::Space),
                                        format_smolstr!("{e}"),
                                    )
                                })?;
                                result.row_count += 1;
                            }
                            ConflictStrategy::DoFail => {
                                return Err(SbroadError::FailedTo(
                                    Action::Insert,
                                    Some(Entity::Space),
                                    format_smolstr!("{tnt_err}"),
                                ));
                            }
                        }
                        // if either DoReplace or DoNothing was done,
                        // jump to next tuple iteration. Otherwise
                        // the error is not DuplicateKey, and we
                        // should throw it back to user.
                        continue;
                    };
                }
                insert_result.map_err(|e| {
                    SbroadError::FailedTo(
                        Action::Insert,
                        Some(Entity::Space),
                        format_smolstr!("{e}"),
                    )
                })?;
                result.row_count += 1;
            }
        }
        Ok(())
    })?;
    port_write_execute_dml(port, result.row_count);

    Ok(())
}

fn port_write_execute_dml<'p>(port: &mut impl Port<'p>, changed: u64) {
    let mut mp = [0_u8; 5 + 9];
    let pos = {
        let mut wr = Cursor::new(&mut mp[..]);
        let _ = write_array_len(&mut wr, 1)
            .expect("Failed to write a single element array in DML response");
        let _ = write_uint(&mut wr, changed).expect("Failed to write changed rows in DML response");
        wr.position() as usize
    };
    port.add_mp(&mp[..pos]);
}
