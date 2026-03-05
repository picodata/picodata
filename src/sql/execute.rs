use crate::metrics::{
    report_storage_cache_hit, report_storage_cache_miss, STORAGE_2ND_REQUESTS_TOTAL,
};
use crate::preemption::scheduler_options;
use crate::sql::lock::{lock_temp_table, TempTableLockRef};
use crate::sql::lua::{lua_decode_ibufs, lua_query_metadata};
use crate::sql::port::PicoPortOwned;
use crate::sql::router::{get_index_version_by_pk, get_table_version_by_id, VersionMap};
use crate::sql::storage::{
    ExpandedLocalExecutionInfo, ExpandedPlanInfo, FullDeleteInfo, LocalExecutionInfo, PlanInfo,
};
use crate::sql::PicoPortC;
use crate::tlog;
use crate::traft::node;
use ahash::HashMapExt;
use comfy_table::{Cell, ContentArrangement, Row, Table};
use rmp::decode::read_array_len;
use rmp::encode::{write_array_len, write_uint};
use smol_str::{format_smolstr, ToSmolStr};
use sql::backend::sql::ADMIN_ID;
use sql::errors::{Action, Entity, SbroadError};
use sql::executor::engine::helpers::{
    build_insert_args, write_shared_update_args, TupleBuilderCommand, TupleBuilderPattern,
};
use sql::executor::engine::protocol::{ExecutionCacheMissData, ExecutionData};
use sql::executor::engine::{QueryCache, StorageCache, Vshard};
use sql::executor::preemption::Scheduler;
use sql::executor::protocol::SchemaInfo;
use sql::executor::result::ConsumerResult;
use sql::executor::vdbe::ExecutionInsight::{BusyStmt, Nothing, StaleStmt};
use sql::executor::vdbe::{ExecutionInsight, SqlError, SqlStmt};
use sql::executor::vtable::{
    vtable_indexed_column_name, VTableTuple, VirtualTable, VirtualTableTupleEncoder,
};
use sql::executor::{Port, PortType};
use sql::ir::helpers::RepeatableState;
use sql::ir::options::Options;
use sql::ir::relation::SpaceEngine;
use sql::ir::relation::{Column, ColumnRole};
use sql::ir::transformation::redistribution::{MotionKey, Target};
use sql::ir::value::{EncodedValue, MsgPackValue, Value};
use sql::ir::ExplainType;
use sql::utils::MutexLike;
use sql_protocol::decode::{ProtocolMessage, ProtocolMessageIter};
use sql_protocol::dml::delete::{
    DeleteFilteredIterator, DeleteFilteredResult, DeleteFullIterator, DeleteFullResult,
};
use sql_protocol::dml::insert::{
    ConflictPolicy, InsertIterator, InsertMaterializedIterator, InsertMaterializedResult,
    InsertResult,
};
use sql_protocol::dml::update::{
    UpdateIterator, UpdateResult, UpdateSharedKeyIterator, UpdateSharedKeyResult,
};
use sql_protocol::dql::{DQLCacheMissResult, DQLPacketPayloadIterator, DQLResult};
use sql_protocol::dql_encoder::{ColumnType, DQLOptions};
use sql_protocol::dql_encoder::{DQLDataSource, MsgpackEncode};
use sql_protocol::error::ProtocolError;
use sql_protocol::iterators::{MsgpackMapIterator, TupleIterator};
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::OnceLock;
use tarantool::error::{Error, TarantoolErrorCode};
use tarantool::ffi::sql::Port as TarantoolPort;
use tarantool::index::{FieldType, IndexOptions, IndexType, Part};
use tarantool::msgpack::{self, Encode};
use tarantool::session::with_su;
use tarantool::space::{Field, Space, SpaceCreateOptions, SpaceType};
use tarantool::transaction::transaction;
use tarantool::tuple::RawBytes;

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

pub trait LendingTupleIterator {
    fn next_tuple(&mut self) -> Option<Result<&RawBytes, SbroadError>>;
}

impl LendingTupleIterator for TupleIterator<'_> {
    fn next_tuple(&mut self) -> Option<Result<&RawBytes, SbroadError>> {
        let res = self.next()?;
        Some(res.map(RawBytes::new).map_err(SbroadError::ProtocolError))
    }
}

#[derive(Clone)]
pub struct LazyVirtualTableEncoder<'e, T>
where
    T: Iterator<Item = VirtualTableTupleEncoder<'e>>,
{
    buffer: Vec<u8>,
    iter: T,
}

impl<'e, T> LazyVirtualTableEncoder<'e, T>
where
    T: Iterator<Item = VirtualTableTupleEncoder<'e>>,
{
    pub fn new(iter: T) -> Self {
        LazyVirtualTableEncoder {
            buffer: Vec::new(),
            iter,
        }
    }
}

impl<'e, T> LendingTupleIterator for LazyVirtualTableEncoder<'e, T>
where
    T: Iterator<Item = VirtualTableTupleEncoder<'e>>,
{
    fn next_tuple(&mut self) -> Option<Result<&RawBytes, SbroadError>> {
        let val = self.iter.next()?;
        self.buffer.clear();
        Some(
            val.encode_into(&mut self.buffer)
                .map_err(|e| SbroadError::DispatchError(e.to_smolstr()))
                .map(|_| RawBytes::new(self.buffer.as_slice())),
        )
    }
}

fn populate_table(
    table_name: &str,
    mut tuples: impl LendingTupleIterator,
) -> Result<(), SbroadError> {
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
        let scheduler_opts = scheduler_options();
        let mut ys = Scheduler::new(&scheduler_opts);
        while let Some(tuple) = tuples.next_tuple() {
            ys.maybe_yield(&scheduler_opts)
                .map_err(|e| SbroadError::Other(e.to_smolstr()))?;
            let tuple = tuple?;
            space.insert(tuple).map_err(|e|
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
    msg: &ProtocolMessage,
    port: &mut impl Port<'p>,
    timeout: f64,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let iter = msg.get_iter()?;

    match iter {
        ProtocolMessageIter::DmlInsert(iter) => tuple_insert_from_proto(runtime, iter, port)?,
        ProtocolMessageIter::DmlUpdate(iter) => shared_update_from_proto(runtime, iter, port)?,
        ProtocolMessageIter::DmlDelete(iter) => full_delete_from_proto(runtime, iter, port)?,
        ProtocolMessageIter::LocalDmlInsert(iter) => {
            materialized_insert_from_proto(runtime, msg.request_id, iter, port, timeout)?
        }
        ProtocolMessageIter::LocalDmlUpdate(iter) => {
            materialized_update_from_proto(runtime, msg.request_id, iter, port, timeout)?
        }
        ProtocolMessageIter::LocalDmlDelete(iter) => {
            filtered_delete_from_proto(runtime, msg.request_id, iter, port, timeout)?
        }
        _ => {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some("Expected a DML plan.".to_smolstr()),
            ))
        }
    }
    port.set_type(PortType::ExecuteDml);
    Ok(())
}

pub struct ProtocolExecutionInfo<'b> {
    vtables: MsgpackMapIterator<'b, &'b str, TupleIterator<'b>>,
    options: DQLOptions,
    params: &'b [u8],
}

impl PlanInfo for ProtocolExecutionInfo<'_> {
    fn vtables(
        &self,
    ) -> impl ExactSizeIterator<Item = Result<(&str, impl LendingTupleIterator), ProtocolError>>
    {
        self.vtables.clone()
    }

    fn params(&self) -> &[u8] {
        self.params
    }

    fn sql_vdbe_opcode_max(&self) -> u64 {
        self.options.sql_vdbe_opcode_max
    }

    fn sql_motion_row_max(&self) -> u64 {
        self.options.sql_motion_row_max
    }
}

struct ProtocolCacheMissExecution<'b> {
    schema_info: SchemaInfo,
    plan_id: u64,
    vtable_metadata: MsgpackMapIterator<'b, &'b str, Vec<(&'b str, ColumnType)>>,
    sql: &'b str,
}

impl ExpandedPlanInfo for ProtocolCacheMissExecution<'_> {
    fn schema_info(&self) -> &SchemaInfo {
        &self.schema_info
    }

    fn plan_id(&self) -> u64 {
        self.plan_id
    }

    fn vtable_metadata(
        &self,
    ) -> impl ExactSizeIterator<Item = Result<(&str, Vec<(&str, ColumnType)>), ProtocolError>> {
        self.vtable_metadata.clone()
    }

    fn sql(&self) -> &str {
        self.sql
    }
}

// must be executed under lock
pub fn stmt_execute<'p, 'b>(
    stmt: &mut SqlStmt,
    info: &impl PlanInfo,
    port: &mut impl Port<'p>,
) -> Result<ExecutionInsight, SbroadError> {
    let has_metadata = port.size() == 1;
    let vtables = info.vtables();
    let sql_motion_row_max = info.sql_motion_row_max();
    let mut tables_names = Vec::with_capacity(vtables.len());
    let pcall = || -> Result<ExecutionInsight, SbroadError> {
        for vtable_data in vtables {
            let (vtable, tuples) = vtable_data?;
            populate_table(vtable, tuples)?;
            tables_names.push(vtable);
        }

        with_su(ADMIN_ID, || -> Result<ExecutionInsight, SqlError> {
            port.process_stmt_with_raw_params(stmt, info.params(), info.sql_vdbe_opcode_max())
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
        if sql_motion_row_max > 0 && current_rows as u64 > sql_motion_row_max {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "Exceeded maximum number of rows ({}) in virtual table: {}",
                sql_motion_row_max,
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
    raft_id: u64,
    info: &impl PlanInfo,
    port: &mut impl Port<'p>,
    timeout: f64,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let node = node::global().expect("should be init");
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

    let table_schema_info = protocol_get!(cache_miss_info, DQLCacheMissResult::TableSchemaInfo);
    let index_schema_info = protocol_get!(cache_miss_info, DQLCacheMissResult::IndexSchemaInfo);

    let mut table_versions =
        VersionMap::with_capacity_and_hasher(table_schema_info.len() as usize, RepeatableState);
    let mut index_versions =
        HashMap::with_capacity_and_hasher(index_schema_info.len() as usize, RepeatableState);
    for schema in table_schema_info {
        let (table, version) = schema?;
        if version != get_table_version_by_id(table)? {
            return Err(SbroadError::OutdatedStorageSchema);
        }
        table_versions.insert(table, version);
    }

    for index_schema in index_schema_info {
        let (pk, version) = index_schema?;
        if version != get_index_version_by_pk(pk[0], pk[1])? {
            return Err(SbroadError::OutdatedStorageSchema);
        }
        index_versions.insert(pk, version);
    }

    let cached = {
        let mut cache_guarded = runtime.cache().lock();
        cache_guarded.get(&plan_id)?
    };

    if let Some(cached) = cached {
        let _table_lease = lock_temp_table(&cached.table_lock)?;
        let mut stmt_guard = cached.stmt.lock();
        match stmt_execute(&mut stmt_guard, info, port)? {
            Nothing => report_storage_cache_hit("dql", "2nd"),
            BusyStmt => report_storage_cache_miss("dql", "2nd", "busy"),
            StaleStmt => report_storage_cache_miss("dql", "2nd", "stale"),
        }

        Ok(())
    } else {
        let metadata = protocol_get!(cache_miss_info, DQLCacheMissResult::VtablesMetadata);
        let sql = protocol_get!(cache_miss_info, DQLCacheMissResult::Sql);

        let miss_info = ProtocolCacheMissExecution {
            schema_info: SchemaInfo::new(table_versions, index_versions),
            plan_id,
            vtable_metadata: metadata,
            sql,
        };

        match sql_execute::<R>(runtime, info, &miss_info, port)? {
            Nothing => report_storage_cache_miss("dql", "2nd", "true"),
            BusyStmt => report_storage_cache_miss("dql", "2nd", "busy"),
            StaleStmt => report_storage_cache_miss("dql", "2nd", "stale"),
        }

        Ok(())
    }
}

pub fn sql_execute<'a, 'p, R: QueryCache>(
    runtime: &R,
    info: &impl PlanInfo,
    miss_info: &impl ExpandedPlanInfo,
    port: &mut impl Port<'p>,
) -> Result<ExecutionInsight, SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let plan_lock = {
        let mut cache_guarded = runtime.cache().lock();
        cache_guarded.get_or_create_lock(miss_info.plan_id())
    };
    let _table_lease = lock_temp_table(&plan_lock)?;

    // Re-check cache after acquiring the plan lock to avoid duplicate
    // temporary table creation on concurrent cache misses for the same plan.
    let cached = {
        let mut cache_guarded = runtime.cache().lock();
        cache_guarded.get(&miss_info.plan_id())?
    };
    if let Some(cached) = cached {
        let mut stmt_guard = cached.stmt.lock();
        return stmt_execute(&mut stmt_guard, info, port);
    }

    let metadata = miss_info.vtable_metadata();
    let mut tables = Vec::with_capacity(metadata.len());
    for data in metadata {
        let (name, meta) = data?;
        let pk_name = generate_pk_for_tmp_table(name);
        table_create(name, pk_name.as_str(), &meta)?;
        tables.push(name.to_smolstr());
    }

    let sql = miss_info.sql();
    let stmt = SqlStmt::compile(sql).inspect_err(|e| {
        tlog!(
            Warning,
            "Failed to compile statement for the query '{sql}': {e}"
        )
    })?;
    let cached = {
        let mut cache_guarded = runtime.cache().lock();
        cache_guarded.put(miss_info.plan_id(), stmt, miss_info.schema_info(), tables)?;
        let Some(cached) = cache_guarded.get(&miss_info.plan_id())? else {
            unreachable!("was just added, should be in the cache")
        };
        cached
    };
    // Process any evictions that were deferred while the cache mutex was held.
    crate::sql::storage::process_deferred_evictions();
    let mut stmt_guard = cached.stmt.lock();
    stmt_execute(&mut stmt_guard, info, port)
}

/// Execute a DQL query on storage.
///
/// The query is checked for presence in the cache. If not found, a cache miss
/// request is performed and the query is added to the cache for future use.
fn execute_dql_with_cache<'p, R, I, P, FMiss>(
    runtime: &R,
    plan_id: u64,
    info: &I,
    port: &mut P,
    rpc_type: &str,
    on_miss: FMiss,
) -> Result<(), SbroadError>
where
    R: QueryCache,
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
    I: PlanInfo,
    P: Port<'p>,
    FMiss: FnOnce(&R, &I, &mut P) -> Result<(), SbroadError>,
{
    let cached = {
        let mut cache_guarded = runtime.cache().lock();
        cache_guarded.get(&plan_id)?
    };
    if let Some(cached) = cached {
        let _table_lease = lock_temp_table(&cached.table_lock)?;
        let mut stmt_guard = cached.stmt.lock();
        match stmt_execute(&mut stmt_guard, info, port)? {
            Nothing => report_storage_cache_hit("dql", rpc_type),
            BusyStmt => report_storage_cache_miss("dql", rpc_type, "busy"),
            StaleStmt => report_storage_cache_miss("dql", rpc_type, "stale"),
        }
        Ok(())
    } else {
        on_miss(runtime, info, port)
    }
}

pub(crate) fn dql_execute<'p, 'b, R: Vshard + QueryCache>(
    runtime: &R,
    request_id: &str,
    mut info: DQLPacketPayloadIterator<'b>,
    port: &mut impl Port<'p>,
    timeout: f64,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let table_schema_info = protocol_get!(info, DQLResult::TableSchemaInfo);
    let index_schema_info = protocol_get!(info, DQLResult::IndexSchemaInfo);

    for schema in table_schema_info {
        let (table, version) = schema?;
        if version != get_table_version_by_id(table)? {
            return Err(SbroadError::OutdatedStorageSchema);
        }
    }

    for index_schema in index_schema_info {
        let (pk, version) = index_schema?;
        if version != get_index_version_by_pk(pk[0], pk[1])? {
            return Err(SbroadError::OutdatedStorageSchema);
        }
    }

    let plan_id = protocol_get!(info, DQLResult::PlanId);
    let sender_id = protocol_get!(info, DQLResult::SenderId);

    let info = ProtocolExecutionInfo {
        vtables: protocol_get!(info, DQLResult::Vtables),
        options: protocol_get!(info, DQLResult::Options),
        params: protocol_get!(info, DQLResult::Params),
    };

    let res = execute_dql_with_cache(
        runtime,
        plan_id,
        &info,
        port,
        "1st",
        |runtime, info, port| {
            report_storage_cache_miss("dql", "1st", "true");
            let res = dql_cache_miss_execute(
                runtime, request_id, plan_id, sender_id, info, port, timeout,
            );
            let result = if res.is_err() { "err" } else { "ok" };
            STORAGE_2ND_REQUESTS_TOTAL
                .with_label_values(&["dql", result])
                .inc();
            res
        },
    );
    port.set_type(PortType::ExecuteDql);
    res
}

pub(crate) fn dql_execute_locally_from_plan<'p, R: QueryCache>(
    runtime: &R,
    dql: &ExecutionData,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    validate_dql_source_schema(dql)?;

    let options = dql.options();
    let info = LocalExecutionInfo::new(
        dql.vtables(),
        options.sql_motion_row_max,
        options.sql_vdbe_opcode_max,
        dql.encoded_params(),
    );

    let res = execute_dql_with_cache(
        runtime,
        dql.plan_id(),
        &info,
        port,
        "local",
        |runtime, info, port| {
            let miss = ExecutionCacheMissData::try_from(dql)?;
            let schema_info = SchemaInfo::new(
                miss.schema_info.table_version_map,
                miss.schema_info.index_version_map,
            );
            let miss_info = ExpandedLocalExecutionInfo::new(
                schema_info,
                dql.plan_id(),
                dql.vtables(),
                miss.sql,
            );

            match sql_execute::<R>(runtime, info, &miss_info, port)? {
                Nothing => report_storage_cache_miss("dql", "local", "true"),
                BusyStmt => report_storage_cache_miss("dql", "local", "busy"),
                StaleStmt => report_storage_cache_miss("dql", "local", "stale"),
            }
            Ok(())
        },
    );
    port.set_type(PortType::ExecuteDql);
    res
}

fn validate_dql_source_schema(dql: &impl DQLDataSource) -> Result<(), SbroadError> {
    for (table, version) in dql.get_table_schema_info() {
        if version != get_table_version_by_id(table)? {
            return Err(SbroadError::OutdatedStorageSchema);
        }
    }

    for (pk, version) in dql.get_index_schema_info() {
        if version != get_index_version_by_pk(pk[0], pk[1])? {
            return Err(SbroadError::OutdatedStorageSchema);
        }
    }

    Ok(())
}

fn create_row_from_tuple(mp: &[u8]) -> Row {
    let mut cur = Cursor::new(mp);
    let (select_id, order, from, detail) =
        rmp_serde::from_read::<_, (i64, i64, i64, String)>(&mut cur)
            .expect("explain format violation");

    let cells = [
        Cell::new(select_id),
        Cell::new(order),
        Cell::new(from),
        Cell::new(detail),
    ];

    Row::from(cells)
}

fn repack_vdbe_explain<'p>(port: &mut impl Port<'p>) -> String {
    let mut table = Table::default();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(["selectid", "order", "from", "detail"]);

    for tuple in port.iter() {
        let row = create_row_from_tuple(tuple);
        table.add_row(row);
    }

    format!("{table}\n")
}

fn repack_vdbe_error(err: String) -> String {
    let mut table = Table::default();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    let row = Row::from([Cell::new(err)]);
    table.add_row(row);

    format!("{table}\n")
}

fn format_sql(explain: &str, params: &[Value], explain_type: ExplainType) -> String {
    let sql = explain.strip_prefix("EXPLAIN QUERY PLAN ").unwrap_or("");
    let mut fmt_options = sqlformat::FormatOptions::default();
    if !matches!(explain_type, ExplainType::ExplainQueryPlanFmt) || sql.len() < LINE_WIDTH {
        fmt_options.joins_as_top_level = true;
        fmt_options.inline = true;
    }
    let params = params
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<String>>();

    sqlformat::format(sql, &sqlformat::QueryParams::Indexed(params), &fmt_options)
}

pub fn explain_execute_guarded<'p>(
    explain: &str,
    params: &[Value],
    sql_vdbe_opcode_max: u64,
    explain_type: ExplainType,
    query: &str,
    location: &str,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError> {
    let header = format!("{query} ({location}):");
    let mp_header = rmp_serde::to_vec(&[header])?;
    port.add_mp(&mp_header);

    let sql = format_sql(explain, params, explain_type);
    let mp_sql = rmp_serde::to_vec(&[sql])?;
    port.add_mp(&mp_sql);

    match SqlStmt::compile(explain) {
        Ok(mut stmt) => {
            let mut tmp_port = PicoPortOwned::new();
            tmp_port.process_stmt(&mut stmt, params, sql_vdbe_opcode_max)?;

            let vdbe_explain = repack_vdbe_explain(&mut tmp_port);
            let vdbe_explain_serialized = rmp_serde::to_vec(&[vdbe_explain])?;

            port.add_mp(&vdbe_explain_serialized);
        }
        Err(err) => {
            let err_msg = repack_vdbe_error(err.to_string());
            let err_serialized = rmp_serde::to_vec(&[err_msg])?;

            port.add_mp(&err_serialized);
        }
    }

    Ok(())
}

fn generate_pk_for_tmp_table(table_name: &str) -> String {
    format!(
        "PK_{}",
        table_name.strip_prefix("TMP_").unwrap_or(table_name)
    )
}

pub fn explain_execute<'p, R: QueryCache>(
    runtime: &R,
    miss_info: impl ExpandedPlanInfo,
    params: &[Value],
    sql_vdbe_opcode_max: u64,
    explain_type: ExplainType,
    location: &str,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
{
    let _lock = runtime.cache().lock();
    for motion_id in miss_info.vtable_metadata() {
        let (table_name, columns) = motion_id?;
        let pk_name = generate_pk_for_tmp_table(table_name);
        table_create(table_name, &pk_name, &columns)?;
    }

    explain_execute_guarded(
        miss_info.sql(),
        params,
        sql_vdbe_opcode_max,
        explain_type,
        "Query",
        location,
        port,
    )
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
fn table_create(
    name: &str,
    pk_name: &str,
    columns: &Vec<(&str, ColumnType)>,
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

/// Helper function to materialize a virtual table on storage.
fn build_virtual_table_from_port<'a, R: Vshard>(
    runtime: &R,
    column_types: &[ColumnType],
    builder: &TupleBuilderPattern,
    tuples: impl Iterator<Item = &'a [u8]>,
) -> Result<VirtualTable, SbroadError> {
    let mut vcolumns = Vec::with_capacity(column_types.len());
    for (i, column_type) in column_types.iter().copied().enumerate() {
        vcolumns.push(Column {
            name: vtable_indexed_column_name(i),
            r#type: column_type.into(),
            role: ColumnRole::User,
            is_nullable: false,
        });
    }
    let mut vtable = VirtualTable::with_columns(vcolumns);
    let scheduler_opts = scheduler_options();
    let mut ys = Scheduler::new(&scheduler_opts);
    for tuple in tuples {
        ys.maybe_yield(&scheduler_opts)
            .map_err(|e| SbroadError::Other(e.to_smolstr()))?;
        vtable.write_all(tuple).map_err(|e| {
            SbroadError::Invalid(
                Entity::VirtualTable,
                Some(format_smolstr!(
                    "failed to write a tuple to the virtual table: {e}"
                )),
            )
        })?;
    }

    for elem in builder.iter() {
        let TupleBuilderCommand::CalculateBucketId(motion_key) = elem else {
            continue;
        };

        if motion_key.targets.is_empty() {
            continue;
        }

        vtable.reshard(motion_key, runtime)?;
        break;
    }

    Ok(vtable)
}

pub(crate) fn materialize_with_dql<R, F>(
    runtime: &R,
    column_types: &[ColumnType],
    builder: &TupleBuilderPattern,
    fill: F,
) -> Result<VirtualTable, SbroadError>
where
    R: Vshard,
    F: for<'p> FnOnce(&mut PicoPortC<'p>) -> Result<(), SbroadError>,
{
    let mut port = TarantoolPort::new_port_c();
    let mut pico_port = PicoPortC::from(unsafe { port.as_mut_port_c() });
    fill(&mut pico_port)?;
    build_virtual_table_from_port(runtime, column_types, builder, pico_port.iter())
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

fn ensure_target_space<R: QueryCache>(
    runtime: &R,
    table_id: u32,
    version: u64,
) -> Result<Space, SbroadError> {
    if runtime.get_table_version_by_id(table_id)? != version {
        return Err(SbroadError::OutdatedStorageSchema);
    }

    // SAFETY: `table_id` already exists. Checked by `get_table_version_by_id`.
    Ok(unsafe { Space::from_id_unchecked(table_id) })
}

fn maybe_reshard_vtable<R: Vshard>(
    runtime: &R,
    vtable: &mut VirtualTable,
    builder: &TupleBuilderPattern,
) -> Result<(), SbroadError> {
    if !vtable.get_bucket_index().is_empty() {
        return Ok(());
    }

    for elem in builder.iter() {
        let TupleBuilderCommand::CalculateBucketId(motion_key) = elem else {
            continue;
        };

        if motion_key.targets.is_empty() {
            continue;
        }

        vtable.reshard(motion_key, runtime)?;
        break;
    }

    Ok(())
}

fn apply_insert_with_conflict(
    insert_result: Result<(), Error>,
    conflict_strategy: ConflictPolicy,
    insert_tuple: &impl std::fmt::Debug,
    replace_on_conflict: impl FnOnce() -> Result<(), SbroadError>,
) -> Result<bool, SbroadError> {
    match insert_result {
        Ok(()) => Ok(true),
        Err(Error::Tarantool(tnt_err))
            if tnt_err.error_code() == TarantoolErrorCode::TupleFound as u32 =>
        {
            match conflict_strategy {
                ConflictPolicy::DoNothing => {
                    tlog!(
                        Debug,
                        "failed to insert tuple: {insert_tuple:?}. Skipping according to conflict strategy",
                    );
                    Ok(false)
                }
                ConflictPolicy::DoReplace => {
                    tlog!(
                        Debug,
                        "failed to insert tuple: {insert_tuple:?}. Trying to replace according to conflict strategy"
                    );
                    replace_on_conflict()?;
                    Ok(true)
                }
                ConflictPolicy::DoFail => Err(SbroadError::FailedTo(
                    Action::Insert,
                    Some(Entity::Space),
                    format_smolstr!("{tnt_err}"),
                )),
            }
        }
        Err(e) => Err(SbroadError::FailedTo(
            Action::Insert,
            Some(Entity::Space),
            format_smolstr!("{e}"),
        )),
    }
}

fn find_insert_motion_key(builder: &TupleBuilderPattern) -> Option<&MotionKey> {
    builder.iter().find_map(|command| match command {
        TupleBuilderCommand::CalculateBucketId(motion_key) if !motion_key.targets.is_empty() => {
            Some(motion_key)
        }
        _ => None,
    })
}

fn determine_insert_bucket_id<R: Vshard>(
    runtime: &R,
    vt_tuple: &VTableTuple,
    motion_key: &MotionKey,
) -> Result<u64, SbroadError> {
    let mut shard_key_tuple = Vec::with_capacity(motion_key.targets.len());
    for target in &motion_key.targets {
        match target {
            Target::Reference(col_idx) => {
                let value = vt_tuple.get(*col_idx).ok_or_else(|| {
                    SbroadError::NotFound(
                        Entity::DistributionKey,
                        format_smolstr!(
                            "failed to find a distribution key column {col_idx} in the tuple {vt_tuple:?}."
                        ),
                    )
                })?;
                shard_key_tuple.push(value);
            }
            Target::Value(value) => shard_key_tuple.push(value),
        }
    }

    runtime.determine_bucket_id(&shard_key_tuple)
}

fn build_insert_bucket_index<R: Vshard>(
    runtime: &R,
    builder: &TupleBuilderPattern,
    vtable: &VirtualTable,
) -> Result<Option<HashMap<u64, Vec<usize>, RepeatableState>>, SbroadError> {
    if !vtable.get_bucket_index().is_empty() {
        return Ok(None);
    }

    let Some(motion_key) = find_insert_motion_key(builder) else {
        return Ok(None);
    };

    let mut bucket_index: HashMap<u64, Vec<usize>, RepeatableState> =
        HashMap::with_hasher(RepeatableState);
    for (pos, vt_tuple) in vtable.get_tuples().iter().enumerate() {
        let bucket_id = determine_insert_bucket_id(runtime, vt_tuple, motion_key)?;
        bucket_index.entry(bucket_id).or_default().push(pos);
    }
    Ok(Some(bucket_index))
}

fn tuple_insert_impl<R: Vshard>(
    runtime: &R,
    space: &Space,
    conflict_strategy: ConflictPolicy,
    builder: &TupleBuilderPattern,
    vtable: &VirtualTable,
) -> Result<u64, SbroadError> {
    let mut result = ConsumerResult::default();
    let computed_bucket_index = build_insert_bucket_index(runtime, builder, vtable)?;
    let bucket_index = computed_bucket_index
        .as_ref()
        .unwrap_or_else(|| vtable.get_bucket_index());

    transaction(|| -> Result<(), SbroadError> {
        if bucket_index.is_empty() {
            for vt_tuple in vtable.get_tuples() {
                let insert_tuple = build_insert_args(vt_tuple, builder, None)?;
                let insert_result = space.insert(&insert_tuple).map(|_| ());
                if apply_insert_with_conflict(
                    insert_result,
                    conflict_strategy,
                    &insert_tuple,
                    || {
                        space.replace(&insert_tuple).map(|_| ()).map_err(|e| {
                            SbroadError::FailedTo(
                                Action::ReplaceOnConflict,
                                Some(Entity::Space),
                                format_smolstr!("{e}"),
                            )
                        })
                    },
                )? {
                    result.row_count += 1;
                }
            }
            return Ok(());
        }

        for (bucket_id, positions) in bucket_index {
            for pos in positions {
                let vt_tuple = vtable.get_tuples().get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::VirtualTable,
                        Some(format_smolstr!(
                            "tuple at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                let insert_tuple = build_insert_args(vt_tuple, builder, Some(bucket_id))?;
                let insert_result = space.insert(&insert_tuple).map(|_| ());
                if apply_insert_with_conflict(
                    insert_result,
                    conflict_strategy,
                    &insert_tuple,
                    || {
                        space.replace(&insert_tuple).map(|_| ()).map_err(|e| {
                            SbroadError::FailedTo(
                                Action::ReplaceOnConflict,
                                Some(Entity::Space),
                                format_smolstr!("{e}"),
                            )
                        })
                    },
                )? {
                    result.row_count += 1;
                }
            }
        }

        Ok(())
    })?;

    Ok(result.row_count)
}

pub(crate) fn tuple_insert_from_plan<R: Vshard + QueryCache>(
    runtime: &R,
    table_id: u32,
    version: u64,
    conflict_strategy: ConflictPolicy,
    builder: &TupleBuilderPattern,
    vtable: &VirtualTable,
) -> Result<u64, SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let space = ensure_target_space(runtime, table_id, version)?;
    tuple_insert_impl(runtime, &space, conflict_strategy, builder, vtable)
}

fn local_update_impl(
    space: &Space,
    builder: &TupleBuilderPattern,
    vtable: &VirtualTable,
) -> Result<u64, SbroadError> {
    let mut result = ConsumerResult::default();

    transaction(|| -> Result<(), SbroadError> {
        for vt_tuple in vtable.get_tuples() {
            let args = update_args(vt_tuple, builder)?;
            let update_res = space.update(&args.key_tuple, &args.ops);
            update_res.map_err(|e| {
                SbroadError::FailedTo(Action::Update, Some(Entity::Space), format_smolstr!("{e}"))
            })?;
            result.row_count += 1;
        }

        Ok(())
    })?;

    Ok(result.row_count)
}

pub(crate) fn materialized_update_from_plan<R: Vshard + QueryCache>(
    runtime: &R,
    table_id: u32,
    version: u64,
    columns: &[Column],
    builder: &TupleBuilderPattern,
    dql: &ExecutionData,
) -> Result<u64, SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let space = ensure_target_space(runtime, table_id, version)?;
    let column_types = columns
        .iter()
        .map(|column| column.r#type.into())
        .collect::<Vec<_>>();
    let vtable = materialize_with_dql(runtime, &column_types, builder, |pico_port| {
        dql_execute_locally_from_plan(runtime, dql, pico_port)
    })?;
    local_update_impl(&space, builder, &vtable)
}

pub(crate) fn shared_update_from_plan<R: Vshard + QueryCache>(
    runtime: &R,
    table_id: u32,
    version: u64,
    delete_tuple_len: usize,
    builder: &TupleBuilderPattern,
    vtable: &VirtualTable,
) -> Result<u64, SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let space = ensure_target_space(runtime, table_id, version)?;
    let mut vtable = vtable.clone();
    maybe_reshard_vtable(runtime, &mut vtable, builder)?;
    let mut result = ConsumerResult::default();

    transaction(|| -> Result<(), SbroadError> {
        let mut tuple_data = Vec::new();

        for tuple in vtable
            .get_tuples()
            .iter()
            .filter(|tuple| tuple.len() == delete_tuple_len)
        {
            encode_delete_tuple(&mut tuple_data, tuple)?;
            let delete_tuple = RawBytes::new(tuple_data.as_slice());
            space.delete(delete_tuple).map_err(|e| {
                SbroadError::FailedTo(
                    Action::Delete,
                    Some(Entity::Tuple),
                    format_smolstr!("{e:?}"),
                )
            })?;
        }

        if vtable.get_bucket_index().is_empty() {
            for tuple in vtable
                .get_tuples()
                .iter()
                .filter(|tuple| tuple.len() != delete_tuple_len)
            {
                let replace_tuple = build_insert_args(tuple, builder, None)?;
                space.replace(&replace_tuple).map_err(|e| {
                    SbroadError::FailedTo(
                        Action::Insert,
                        Some(Entity::Tuple),
                        format_smolstr!("{e:?}"),
                    )
                })?;
                result.row_count += 1;
            }
            return Ok(());
        }

        for (bucket_id, positions) in vtable.get_bucket_index() {
            for pos in positions {
                let tuple = vtable.get_tuples().get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::VirtualTable,
                        Some(format_smolstr!(
                            "tuple at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                if tuple.len() == delete_tuple_len {
                    continue;
                }
                encode_shared_update_tuple(&mut tuple_data, tuple, builder, *bucket_id)?;
                let replace_tuple = RawBytes::new(tuple_data.as_slice());
                space.replace(replace_tuple).map_err(|e| {
                    SbroadError::FailedTo(
                        Action::Insert,
                        Some(Entity::Tuple),
                        format_smolstr!("{e:?}"),
                    )
                })?;
                result.row_count += 1;
            }
        }

        Ok(())
    })?;

    Ok(result.row_count)
}

fn encode_delete_tuple(data: &mut Vec<u8>, tuple: &VTableTuple) -> Result<(), SbroadError> {
    data.clear();
    write_array_len(data, tuple.len() as u32).map_err(|e| {
        SbroadError::Invalid(
            Entity::Tuple,
            Some(format_smolstr!(
                "failed to encode sharded update delete tuple header: {e}"
            )),
        )
    })?;
    for elem in tuple {
        elem.encode(data, &msgpack::Context::DEFAULT).map_err(|e| {
            SbroadError::Invalid(
                Entity::Tuple,
                Some(format_smolstr!(
                    "failed to encode sharded update delete tuple: {e}"
                )),
            )
        })?;
    }
    Ok(())
}

fn encode_shared_update_tuple(
    data: &mut Vec<u8>,
    tuple: &VTableTuple,
    builder: &TupleBuilderPattern,
    bucket_id: u64,
) -> Result<(), SbroadError> {
    data.clear();
    write_array_len(data, builder.len() as u32).map_err(|e| {
        SbroadError::Invalid(
            Entity::Tuple,
            Some(format_smolstr!(
                "failed to encode sharded update tuple header: {e}"
            )),
        )
    })?;
    write_shared_update_args(tuple, builder, bucket_id, data)?;
    Ok(())
}

fn filtered_delete_impl(
    space: &Space,
    builder: &TupleBuilderPattern,
    vtable: &VirtualTable,
) -> Result<u64, SbroadError> {
    let mut result = ConsumerResult::default();

    transaction(|| -> Result<(), SbroadError> {
        for vt_tuple in vtable.get_tuples() {
            let delete_tuple = delete_args(vt_tuple, builder)?;
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

    Ok(result.row_count)
}

pub(crate) fn filtered_delete_from_plan<R: Vshard + QueryCache>(
    runtime: &R,
    table_id: u32,
    version: u64,
    columns: &[Column],
    builder: &TupleBuilderPattern,
    dql: &ExecutionData,
) -> Result<u64, SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let space = ensure_target_space(runtime, table_id, version)?;
    let column_types = columns
        .iter()
        .map(|column| column.r#type.into())
        .collect::<Vec<_>>();
    let vtable = materialize_with_dql(runtime, &column_types, builder, |pico_port| {
        dql_execute_locally_from_plan(runtime, dql, pico_port)
    })?;
    filtered_delete_impl(&space, builder, &vtable)
}

fn tuple_insert_from_proto<'p, 'ip, R: Vshard + QueryCache>(
    runtime: &R,
    mut iter: InsertIterator<'ip>,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let mut result = ConsumerResult::default();

    let space_id = protocol_get!(iter, InsertResult::TableId);
    let version = protocol_get!(iter, InsertResult::TableVersion);

    if runtime.get_table_version_by_id(space_id)? != version {
        return Err(SbroadError::OutdatedStorageSchema);
    }

    // SAFETY: space_id already exists. Checked by get_table_version_by_id
    let space = unsafe { Space::from_id_unchecked(space_id) };

    let conflict_strategy = protocol_get!(iter, InsertResult::ConflictPolicy);
    let tuples = protocol_get!(iter, InsertResult::Tuples);
    transaction(|| -> Result<(), SbroadError> {
        for tuple in tuples {
            let tuple_data = tuple?;
            let insert_tuple = RawBytes::new(tuple_data);
            // TODO: should we care of default and so on
            let insert_result = space.insert(insert_tuple).map(|_| ());
            if apply_insert_with_conflict(insert_result, conflict_strategy, &insert_tuple, || {
                space
                    .replace(RawBytes::new(tuple_data))
                    .map(|_| ())
                    .map_err(|e| {
                        SbroadError::FailedTo(
                            Action::ReplaceOnConflict,
                            Some(Entity::Space),
                            format_smolstr!("{e}"),
                        )
                    })
            })? {
                result.row_count += 1;
            }
        }
        Ok(())
    })?;
    port_write_execute_dml(port, result.row_count);

    Ok(())
}

fn shared_update_from_proto<'p, 'ip, R: Vshard + QueryCache>(
    runtime: &R,
    mut iter: UpdateSharedKeyIterator<'ip>,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let mut result = ConsumerResult::default();

    let table_id = protocol_get!(iter, UpdateSharedKeyResult::TableId);
    let version = protocol_get!(iter, UpdateSharedKeyResult::TableVersion);

    if runtime.get_table_version_by_id(table_id)? != version {
        return Err(SbroadError::OutdatedStorageSchema);
    }

    // SAFETY: space_id already exists. Checked by get_table_version_by_id
    let space = unsafe { Space::from_id_unchecked(table_id) };

    let _ = protocol_get!(iter, UpdateSharedKeyResult::UpdateType);

    transaction(|| -> Result<(), SbroadError> {
        let del_tuples = protocol_get!(iter, UpdateSharedKeyResult::DelTuples);
        for tuple in del_tuples {
            let tuple = RawBytes::new(tuple?);
            space.delete(tuple).map_err(|e| {
                SbroadError::FailedTo(
                    Action::Delete,
                    Some(Entity::Tuple),
                    format_smolstr!("{e:?}"),
                )
            })?;
        }

        let tuples = protocol_get!(iter, UpdateSharedKeyResult::Tuples);

        for tuple in tuples {
            let tuple = RawBytes::new(tuple?);
            space.replace(tuple).map_err(|e| {
                SbroadError::FailedTo(
                    Action::Insert,
                    Some(Entity::Tuple),
                    format_smolstr!("{e:?}"),
                )
            })?;
            result.row_count += 1;
        }

        Ok(())
    })?;
    port_write_execute_dml(port, result.row_count);

    Ok(())
}

fn materialized_insert_from_proto<'p, 'ip, R: Vshard + QueryCache>(
    runtime: &R,
    request_id: &str,
    mut iter: InsertMaterializedIterator<'ip>,
    port: &mut impl Port<'p>,
    timeout: f64,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let table_id = protocol_get!(iter, InsertMaterializedResult::TableId);
    let version = protocol_get!(iter, InsertMaterializedResult::TableVersion);
    let space = ensure_target_space(runtime, table_id, version)?;
    let conflict_strategy = protocol_get!(iter, InsertMaterializedResult::ConflictPolicy);
    let columns = protocol_get!(iter, InsertMaterializedResult::Columns);
    let raw_builder = protocol_get!(iter, InsertMaterializedResult::Builder);
    let builder: TupleBuilderPattern = msgpack::decode(raw_builder).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!("failed to decode tuple builder: {e}"))
    })?;

    let dql = protocol_get!(iter, InsertMaterializedResult::DqlInfo);
    let column_types = columns.collect::<Result<Vec<_>, _>>()?;
    let vtable = materialize_with_dql(runtime, &column_types, &builder, |pico_port| {
        dql_execute(runtime, request_id, dql, pico_port, timeout)
    })?;
    let row_count = tuple_insert_impl(runtime, &space, conflict_strategy, &builder, &vtable)?;
    port_write_execute_dml(port, row_count);

    Ok(())
}

fn materialized_update_from_proto<'p, 'ip, R: Vshard + QueryCache>(
    runtime: &R,
    request_id: &str,
    mut iter: UpdateIterator<'ip>,
    port: &mut impl Port<'p>,
    timeout: f64,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let table_id = protocol_get!(iter, UpdateResult::TableId);
    let version = protocol_get!(iter, UpdateResult::TableVersion);
    let space = ensure_target_space(runtime, table_id, version)?;
    let columns = protocol_get!(iter, UpdateResult::Columns);
    let raw_builder = protocol_get!(iter, UpdateResult::Builder);
    let builder: TupleBuilderPattern = msgpack::decode(raw_builder).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!("failed to decode tuple builder: {e}"))
    })?;

    let dql = protocol_get!(iter, UpdateResult::DqlInfo);
    let column_types = columns.collect::<Result<Vec<_>, _>>()?;
    let vtable = materialize_with_dql(runtime, &column_types, &builder, |pico_port| {
        dql_execute(runtime, request_id, dql, pico_port, timeout)
    })?;
    let row_count = local_update_impl(&space, &builder, &vtable)?;
    port_write_execute_dml(port, row_count);

    Ok(())
}

fn filtered_delete_from_proto<'p, 'ip, R: Vshard + QueryCache>(
    runtime: &R,
    request_id: &str,
    mut iter: DeleteFilteredIterator<'ip>,
    port: &mut impl Port<'p>,
    timeout: f64,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let table_id = protocol_get!(iter, DeleteFilteredResult::TableId);
    let version = protocol_get!(iter, DeleteFilteredResult::TableVersion);
    let space = ensure_target_space(runtime, table_id, version)?;
    let columns = protocol_get!(iter, DeleteFilteredResult::Columns);
    let raw_builder = protocol_get!(iter, DeleteFilteredResult::Builder);
    let builder: TupleBuilderPattern = msgpack::decode(raw_builder).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!("failed to decode tuple builder: {e}"))
    })?;

    let dql = protocol_get!(iter, DeleteFilteredResult::DqlInfo);
    let column_types = columns.collect::<Result<Vec<_>, _>>()?;
    let vtable = materialize_with_dql(runtime, &column_types, &builder, |pico_port| {
        dql_execute(runtime, request_id, dql, pico_port, timeout)
    })?;
    let row_count = filtered_delete_impl(&space, &builder, &vtable)?;
    port_write_execute_dml(port, row_count);

    Ok(())
}

fn full_delete_from_proto<'p, 'ip, R: Vshard + QueryCache>(
    runtime: &R,
    mut iter: DeleteFullIterator<'ip>,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let table_id = protocol_get!(iter, DeleteFullResult::TableId);
    let version = protocol_get!(iter, DeleteFullResult::TableVersion);

    let (table_name, current_version) = runtime.get_table_name_and_version(table_id)?;

    if current_version != version {
        return Err(SbroadError::OutdatedStorageSchema);
    }

    let plan_id = protocol_get!(iter, DeleteFullResult::PlanId);
    let options = protocol_get!(iter, DeleteFullResult::Options);

    let versions = HashMap::from_iter([(table_id, version)]);
    let schema_info = SchemaInfo::new(versions, HashMap::<_, _, RepeatableState>::new());

    // We have a deal with a DELETE without WHERE filter
    // and want to execute local SQL instead of space api.
    let info = FullDeleteInfo::new(
        plan_id,
        schema_info,
        Options {
            sql_motion_row_max: options.sql_motion_row_max as i64,
            sql_vdbe_opcode_max: options.sql_vdbe_opcode_max as i64,
            ..Default::default()
        },
        table_name.as_str(),
    );

    full_delete_execute(runtime, &info, port)
}

pub fn full_delete_execute<'p, 'ip, R: Vshard + QueryCache>(
    runtime: &R,
    info: &FullDeleteInfo,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache<LockRef = TempTableLockRef>,
{
    let cached = {
        let mut cache_guarded = runtime.cache().lock();
        cache_guarded.get(&info.plan_id())?
    };
    if let Some(cached) = cached {
        let _table_lease = lock_temp_table(&cached.table_lock)?;
        let mut stmt_guard = cached.stmt.lock();
        stmt_execute(&mut stmt_guard, info, port)?;
    } else {
        sql_execute::<R>(runtime, info, info, port)?;
    }

    Ok(())
}

pub fn port_write_execute_dml<'p>(port: &mut impl Port<'p>, changed: u64) {
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
