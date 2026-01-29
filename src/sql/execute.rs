use crate::metrics::{
    report_storage_cache_hit, report_storage_cache_miss, STORAGE_2ND_REQUESTS_TOTAL,
};
use crate::preemption::scheduler_options;
use crate::sql::lua::{lua_decode_ibufs, lua_query_metadata};
use crate::sql::router::{get_index_version_by_pk, get_table_version_by_id, VersionMap};
use crate::sql::storage::{ExpandedPlanInfo, FullDeleteInfo, PlanInfo};
use crate::sql::PicoPortC;
use crate::tlog;
use crate::traft::node;
use ahash::HashMapExt;
use rmp::decode::read_array_len;
use rmp::encode::{write_array_len, write_str, write_str_len, write_uint};
use smol_str::{format_smolstr, ToSmolStr};
use sql::backend::sql::space::ADMIN_ID;
use sql::errors::{Action, Entity, SbroadError};
use sql::executor::engine::helpers::{build_insert_args, TupleBuilderCommand, TupleBuilderPattern};
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
use sql::ir::value::{EncodedValue, MsgPackValue, Value};
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
use sql_protocol::dql_encoder::MsgpackEncode;
use sql_protocol::dql_encoder::{ColumnType, DQLOptions};
use sql_protocol::error::ProtocolError;
use sql_protocol::iterators::{MsgpackArrayIterator, MsgpackMapIterator, TupleIterator};
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::OnceLock;
use tarantool::error::{Error, TarantoolErrorCode};
use tarantool::ffi::sql::Port as TarantoolPort;
use tarantool::index::{FieldType, IndexOptions, IndexType, Part};
use tarantool::msgpack;
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
        let mut ys = Scheduler::default();
        while let Some(tuple) = tuples.next_tuple() {
            ys.maybe_yield(&scheduler_options())
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
    R::Cache: StorageCache,
{
    let iter = msg.get_iter()?;

    match iter {
        ProtocolMessageIter::DmlInsert(iter) => insert_execute(runtime, iter, port)?,
        ProtocolMessageIter::DmlUpdate(iter) => update_execute(runtime, iter, port)?,
        ProtocolMessageIter::DmlDelete(iter) => delete_execute(runtime, iter, port)?,
        ProtocolMessageIter::LocalDmlInsert(iter) => {
            local_insert_execute(runtime, msg.request_id, iter, port, timeout)?
        }
        ProtocolMessageIter::LocalDmlUpdate(iter) => {
            local_update_execute(runtime, msg.request_id, iter, port, timeout)?
        }
        ProtocolMessageIter::LocalDmlDelete(iter) => {
            local_delete_execute(runtime, msg.request_id, iter, port, timeout)?
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
    R::Cache: StorageCache,
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

    let mut cache_guarded = runtime.cache().lock();

    if let Some((stmt, _motion_ids)) = cache_guarded.get(&plan_id)? {
        match stmt_execute(stmt, info, port)? {
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

        match sql_execute::<R>(&mut cache_guarded, info, &miss_info, port)? {
            Nothing => report_storage_cache_miss("dql", "2nd", "true"),
            BusyStmt => report_storage_cache_miss("dql", "2nd", "busy"),
            StaleStmt => report_storage_cache_miss("dql", "2nd", "stale"),
        }

        Ok(())
    }
}

pub fn sql_execute<'a, 'p, R: QueryCache>(
    cache_guarded: &mut <<R as QueryCache>::Mutex as MutexLike<R::Cache>>::Guard<'_>,
    info: &impl PlanInfo,
    miss_info: &impl ExpandedPlanInfo,
    port: &mut impl Port<'p>,
) -> Result<ExecutionInsight, SbroadError>
where
    R::Cache: StorageCache,
{
    let metadata = miss_info.vtable_metadata();
    let mut tables = Vec::with_capacity(metadata.len());
    for data in metadata {
        let (name, meta) = data?;
        let pk_name = format!("PK_{}", name.strip_prefix("TMP_").unwrap_or(name));
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
    cache_guarded.put(miss_info.plan_id(), stmt, miss_info.schema_info(), tables)?;

    let Some((stmt, _)) = cache_guarded.get(&miss_info.plan_id())? else {
        unreachable!("was just added, should be in the cache")
    };

    stmt_execute(stmt, info, port)
}

/// Execute a DQL query on storage.
///
/// The query is checked for presence in the cache. If not found, a cache miss
/// request is performed and the query is added to the cache for future use.
pub(crate) fn dql_execute<'p, 'b, R: Vshard + QueryCache>(
    runtime: &R,
    request_id: &str,
    mut info: DQLPacketPayloadIterator<'b>,
    port: &mut impl Port<'p>,
    timeout: f64,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
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

    let mut cache_guarded = runtime.cache().lock();
    if let Some((stmt, _)) = cache_guarded.get(&plan_id)? {
        match stmt_execute(stmt, &info, port)? {
            Nothing => report_storage_cache_hit("dql", "1st"),
            BusyStmt => report_storage_cache_miss("dql", "1st", "busy"),
            StaleStmt => report_storage_cache_miss("dql", "1st", "stale"),
        }

        port.set_type(PortType::ExecuteDql);
        Ok(())
    } else {
        drop(cache_guarded);
        report_storage_cache_miss("dql", "1st", "true");

        let res = dql_cache_miss_execute(
            runtime, request_id, plan_id, sender_id, &info, port, timeout,
        );
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
    miss_info: impl ExpandedPlanInfo,
    params: &[Value],
    sql_vdbe_opcode_max: u64,
    formatted: bool,
    location: &str,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
{
    let _lock: <<R as QueryCache>::Mutex as MutexLike<<R as QueryCache>::Cache>>::Guard<'_> =
        runtime.cache().lock();
    let explain = miss_info.sql();

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
        let params = params
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

    for motion_id in miss_info.vtable_metadata() {
        let (table_name, columns) = motion_id?;
        let pk_name = format!(
            "PK_{}",
            table_name.strip_prefix("TMP_").unwrap_or(table_name)
        );
        table_create(table_name, &pk_name, &columns)?;
    }

    let mut stmt = SqlStmt::compile(explain)?;
    port.process_stmt(&mut stmt, params, sql_vdbe_opcode_max)?;
    Ok(())
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
fn virtual_table_materialize<'a, R: Vshard + QueryCache>(
    runtime: &R,
    request_id: &str,
    columns: MsgpackArrayIterator<'a, ColumnType>,
    builder: &TupleBuilderPattern,
    info: DQLPacketPayloadIterator<'a>,
    timeout: f64,
) -> Result<VirtualTable, SbroadError>
where
    R::Cache: StorageCache,
{
    let mut port = TarantoolPort::new_port_c();
    let mut pico_port = PicoPortC::from(unsafe { port.as_mut_port_c() });
    dql_execute(runtime, request_id, info, &mut pico_port, timeout)?;

    let mut vcolumns = Vec::with_capacity(columns.len());
    for (i, column) in columns.enumerate() {
        let column_type = column?;
        vcolumns.push(Column {
            name: vtable_indexed_column_name(i),
            r#type: column_type.into(),
            role: ColumnRole::User,
            is_nullable: false,
        });
    }
    let mut vtable = VirtualTable::with_columns(vcolumns);
    let mut ys = Scheduler::default();
    for tuple in pico_port.iter() {
        ys.maybe_yield(&scheduler_options())
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

fn insert_execute<'p, 'ip, R: Vshard + QueryCache>(
    runtime: &R,
    mut iter: InsertIterator<'ip>,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
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
            let insert_tuple = RawBytes::new(tuple?);
            // TODO: should we care of default and so on
            let insert_result = space.insert(insert_tuple);
            if let Err(Error::Tarantool(tnt_err)) = &insert_result {
                if tnt_err.error_code() == TarantoolErrorCode::TupleFound as u32 {
                    match conflict_strategy {
                        ConflictPolicy::DoNothing => {
                            tlog!(
                                    Debug,
                                    "failed to insert tuple: {insert_tuple:?}. Skipping according to conflict strategy",
                                );
                        }
                        ConflictPolicy::DoReplace => {
                            tlog!(
                                    Debug,
                                    "failed to insert tuple: {insert_tuple:?}. Trying to replace according to conflict strategy"
                                );
                            space.replace(insert_tuple).map_err(|e| {
                                SbroadError::FailedTo(
                                    Action::ReplaceOnConflict,
                                    Some(Entity::Space),
                                    format_smolstr!("{e}"),
                                )
                            })?;
                            result.row_count += 1;
                        }
                        ConflictPolicy::DoFail => {
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
                SbroadError::FailedTo(Action::Insert, Some(Entity::Space), format_smolstr!("{e}"))
            })?;
            result.row_count += 1;
        }
        Ok(())
    })?;
    port_write_execute_dml(port, result.row_count);

    Ok(())
}

fn update_execute<'p, 'ip, R: Vshard + QueryCache>(
    runtime: &R,
    mut iter: UpdateSharedKeyIterator<'ip>,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
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

fn local_insert_execute<'p, 'ip, R: Vshard + QueryCache>(
    runtime: &R,
    request_id: &str,
    mut iter: InsertMaterializedIterator<'ip>,
    port: &mut impl Port<'p>,
    timeout: f64,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
{
    let mut result = ConsumerResult::default();

    let table_id = protocol_get!(iter, InsertMaterializedResult::TableId);
    let version = protocol_get!(iter, InsertMaterializedResult::TableVersion);

    if runtime.get_table_version_by_id(table_id)? != version {
        return Err(SbroadError::OutdatedStorageSchema);
    }

    // SAFETY: space_id already exists. Checked by get_table_version_by_id
    let space = unsafe { Space::from_id_unchecked(table_id) };

    let conflict_strategy = protocol_get!(iter, InsertMaterializedResult::ConflictPolicy);
    let columns = protocol_get!(iter, InsertMaterializedResult::Columns);
    let raw_builder = protocol_get!(iter, InsertMaterializedResult::Builder);
    let builder: TupleBuilderPattern = msgpack::decode(raw_builder).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!("failed to decode tuple builder: {e}"))
    })?;

    let dql = protocol_get!(iter, InsertMaterializedResult::DqlInfo);

    let vtable = virtual_table_materialize(runtime, request_id, columns, &builder, dql, timeout)?;

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
                            ConflictPolicy::DoNothing => {
                                tlog!(
                                    Debug,
                                    "failed to insert tuple: {insert_tuple:?}. Skipping according to conflict strategy",
                                );
                            }
                            ConflictPolicy::DoReplace => {
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
                            ConflictPolicy::DoFail => {
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

fn local_update_execute<'p, 'ip, R: Vshard + QueryCache>(
    runtime: &R,
    request_id: &str,
    mut iter: UpdateIterator<'ip>,
    port: &mut impl Port<'p>,
    timeout: f64,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
{
    let mut result = ConsumerResult::default();

    let table_id = protocol_get!(iter, UpdateResult::TableId);
    let version = protocol_get!(iter, UpdateResult::TableVersion);

    if runtime.get_table_version_by_id(table_id)? != version {
        return Err(SbroadError::OutdatedStorageSchema);
    }

    // SAFETY: space_id already exists. Checked by get_table_version_by_id
    let space = unsafe { Space::from_id_unchecked(table_id) };

    let columns = protocol_get!(iter, UpdateResult::Columns);
    let raw_builder = protocol_get!(iter, UpdateResult::Builder);
    let builder: TupleBuilderPattern = msgpack::decode(raw_builder).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!("failed to decode tuple builder: {e}"))
    })?;

    let dql = protocol_get!(iter, UpdateResult::DqlInfo);

    let vtable = virtual_table_materialize(runtime, request_id, columns, &builder, dql, timeout)?;

    transaction(|| -> Result<(), SbroadError> {
        for vt_tuple in vtable.get_tuples() {
            let args = update_args(vt_tuple, &builder)?;
            let update_res = space.update(&args.key_tuple, &args.ops);
            update_res.map_err(|e| {
                SbroadError::FailedTo(Action::Update, Some(Entity::Space), format_smolstr!("{e}"))
            })?;
            result.row_count += 1;
        }

        Ok(())
    })?;
    port_write_execute_dml(port, result.row_count);

    Ok(())
}

fn local_delete_execute<'p, 'ip, R: Vshard + QueryCache>(
    runtime: &R,
    request_id: &str,
    mut iter: DeleteFilteredIterator<'ip>,
    port: &mut impl Port<'p>,
    timeout: f64,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
{
    let mut result = ConsumerResult::default();

    let table_id = protocol_get!(iter, DeleteFilteredResult::TableId);
    let version = protocol_get!(iter, DeleteFilteredResult::TableVersion);

    if runtime.get_table_version_by_id(table_id)? != version {
        return Err(SbroadError::OutdatedStorageSchema);
    }

    // SAFETY: space_id already exists. Checked by get_table_version_by_id
    let space = unsafe { Space::from_id_unchecked(table_id) };

    let columns = protocol_get!(iter, DeleteFilteredResult::Columns);
    let raw_builder = protocol_get!(iter, DeleteFilteredResult::Builder);
    let builder: TupleBuilderPattern = msgpack::decode(raw_builder).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!("failed to decode tuple builder: {e}"))
    })?;

    let dql = protocol_get!(iter, DeleteFilteredResult::DqlInfo);

    let vtable = virtual_table_materialize(runtime, request_id, columns, &builder, dql, timeout)?;

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

fn delete_execute<'p, 'ip, R: Vshard + QueryCache>(
    runtime: &R,
    mut iter: DeleteFullIterator<'ip>,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
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
            read_preference: Default::default(),
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
    R::Cache: StorageCache,
{
    let mut cache_guarded: <<R as QueryCache>::Mutex as MutexLike<<R as QueryCache>::Cache>>::Guard<'_> = runtime.cache().lock();
    if let Some((stmt, _motion_ids)) = cache_guarded.get(&info.plan_id())? {
        stmt_execute(stmt, info, port)?;
    } else {
        sql_execute::<R>(&mut cache_guarded, info, info, port)?;
    }

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
