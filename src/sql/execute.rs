use crate::sql::port::{EXECUTE_DML_VTAB, EXECUTE_DQL_VTAB, EXECUTE_MISS_VTAB};
use crate::tlog;
use sbroad::backend::sql::space::ADMIN_ID;
use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::engine::helpers::{
    build_insert_args, init_delete_tuple_builder, init_insert_tuple_builder,
    init_local_update_tuple_builder, init_sharded_update_tuple_builder, vtable_columns, QueryInfo,
    TupleBuilderCommand, TupleBuilderPattern,
};
use sbroad::executor::engine::helpers::{
    pk_name, populate_table, storage::prepare, table_name, truncate_tables, FullPlanInfo,
    RequiredPlanInfo,
};
use sbroad::executor::engine::{QueryCache, StorageCache, Vshard};
use sbroad::executor::ir::QueryType;
use sbroad::executor::protocol::{EncodedOptionalData, OptionalData, RequiredData};
use sbroad::executor::result::ConsumerResult;
use sbroad::executor::vtable::{VTableTuple, VirtualTable, VirtualTableMeta};
use sbroad::ir::node::relational::Relational;
use sbroad::ir::operator::ConflictStrategy;
use sbroad::ir::value::{EncodedValue, MsgPackValue, Value};
use sbroad::ir::{node::NodeId, relation::SpaceEngine};
use sbroad::utils::MutexLike;
use smol_str::{format_smolstr, ToSmolStr};
use std::io::Write;
use std::sync::OnceLock;
use tarantool::error::{Error, TarantoolErrorCode};
use tarantool::ffi::sql::{Port, PortC, PortVTable};
use tarantool::index::{FieldType, IndexOptions, IndexType, Part};
use tarantool::space::{Field, Space, SpaceCreateOptions, SpaceType};
use tarantool::sql::sql_execute_into_port;
use tarantool::transaction::transaction;
use tarantool::tuple::Tuple;
use tarantool::{session::with_su, sql::Statement};

/// Execute a DML query on the storage.
pub fn dml_execute<R: Vshard + QueryCache>(
    runtime: &R,
    required: &mut RequiredData,
    raw_optional: Vec<u8>,
    port: &mut PortC,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
{
    if required.query_type != QueryType::DML {
        return Err(SbroadError::Invalid(
            Entity::Plan,
            Some("Expected a DML plan.".to_smolstr()),
        ));
    }
    let mut optional = OptionalData::try_from(EncodedOptionalData::from(raw_optional))?;
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
    port.vtab = &EXECUTE_DML_VTAB as *const PortVTable;
    Ok(())
}

/// Execute the first round request for DQL query on storage
/// with an attempt to hit the cache.
pub fn dql_execute_first_round<R: Vshard + QueryCache>(
    runtime: &R,
    info: &mut impl RequiredPlanInfo,
    port: &mut PortC,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
{
    let mut cache_guarded = runtime.cache().lock();
    if let Some((stmt, motion_ids)) = cache_guarded.get(info.id())? {
        // Transaction rollbacks are very expensive in Tarantool, so we're going to
        // avoid transactions for DQL queries. We can achieve atomicity by truncating
        // temporary tables. Isolation is guaranteed by keeping a lock on the cache.
        stmt_execute(stmt, info, motion_ids, port)?;
        port.vtab = &EXECUTE_DQL_VTAB as *const PortVTable;
    } else {
        // Response with a cache miss. The router will retry the query
        // on the second round.
        port.vtab = &EXECUTE_MISS_VTAB as *const PortVTable;
    }

    Ok(())
}

pub fn dql_execute_second_round<R: QueryCache>(
    runtime: &R,
    info: &mut impl FullPlanInfo,
    port: &mut PortC,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
{
    let mut cache_guarded: <<R as QueryCache>::Mutex as MutexLike<<R as QueryCache>::Cache>>::Guard<'_> = runtime.cache().lock();
    if let Some((stmt, motion_ids)) = cache_guarded.get(info.id())? {
        // Transaction rollbacks are very expensive in Tarantool, so we're going to
        // avoid transactions for DQL queries. We can achieve atomicity by truncating
        // temporary tables. Isolation is guaranteed by keeping a lock on the cache.
        stmt_execute(stmt, info, motion_ids, port)?;
    } else {
        sql_execute::<R>(&mut cache_guarded, info, port)?;
    }

    port.vtab = &EXECUTE_DQL_VTAB as *const PortVTable;
    Ok(())
}

/// Create a temporary table. It wraps all Space API with `with_su`
/// since user may have no permissions to read/write tables.
pub fn table_create(name: &str, pk_name: &str, meta: &VirtualTableMeta) -> Result<(), SbroadError> {
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

    let mut fields: Vec<Field> = meta
        .columns
        .iter()
        .map(|c| Field::from(c.clone()))
        .collect();

    fields.push(Field::unsigned(pk_name));
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

// Requires the cache to be locked.
fn stmt_execute(
    stmt: &Statement,
    info: &mut impl RequiredPlanInfo,
    motion_ids: &[NodeId],
    port: &mut PortC,
) -> Result<(), SbroadError> {
    let mut vtables = info.extract_data();
    let mut pcall = || -> Result<(), SbroadError> {
        for motion_id in motion_ids {
            populate_table(motion_id, info.id(), &mut vtables)?;
        }
        let encoded_params = encoded_params(info.params());
        with_su(ADMIN_ID, || {
            stmt.execute_into_port(&encoded_params, info.sql_vdbe_opcode_max(), port)
        })?
        .map_err(|err| SbroadError::Invalid(Entity::MsgPack, Some(format_smolstr!("{err}"))))?;
        Ok(())
    };

    let res = pcall();
    truncate_tables(motion_ids, info.id());
    res?;
    Ok(())
}

// Requires the cache to be locked.
fn sql_execute<R: QueryCache>(
    cache_guarded: &mut <<R as QueryCache>::Mutex as MutexLike<R::Cache>>::Guard<'_>,
    info: &mut impl FullPlanInfo,
    port: &mut PortC,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
{
    let (local_sql, motion_ids, vtables_meta) = info.get_query_meta()?;
    let mut vtables = info.extract_data();
    for motion_id in &motion_ids {
        let table_name = table_name(info.id(), *motion_id);
        let pk_name = pk_name(info.id(), *motion_id);
        let meta = vtables_meta.get(motion_id).ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Plan,
                Some(format_smolstr!("missing metadata for motion {motion_id}")),
            )
        })?;
        table_create(&table_name, &pk_name, meta)?;
    }

    match prepare(local_sql.clone()) {
        Ok(stmt) => {
            cache_guarded.put(
                info.id().clone(),
                stmt,
                info.schema_info(),
                motion_ids.clone(),
            )?;

            let (stmt, motion_ids) = cache_guarded.get(info.id())?.unwrap();
            stmt_execute(stmt, info, motion_ids, port)?;
        }
        Err(e) => tlog!(Warning, "failed to compile stmt: {e:?}"),
    }
    // Possibly the statement is correct, but doesn't fit into Tarantool's prepared
    // statements cache (`sql_cache_size`). So we try to execute it bypassing the cache.

    // We need the cache to be locked though we are not going to use it. If we don't lock it,
    // the prepared statement made from our pattern can be inserted into the cache by some
    // other fiber because we have removed some big statements with LRU and tarantool cache
    // has enough space to store this statement. And it can cause races in the temporary
    // tables.

    let mut pcall = || -> Result<(), SbroadError> {
        let motion_ids = vtables.keys().copied().collect::<Vec<NodeId>>();
        for motion_id in motion_ids.iter() {
            populate_table(motion_id, info.id(), &mut vtables)?;
        }
        let encoded_params = encoded_params(info.params());
        with_su(ADMIN_ID, || {
            sql_execute_into_port(
                &local_sql,
                &encoded_params,
                info.sql_vdbe_opcode_max(),
                port,
            )
        })?
        .map_err(|err| SbroadError::Invalid(Entity::MsgPack, Some(format_smolstr!("{err}"))))?;
        Ok(())
    };
    let res = pcall();
    truncate_tables(&motion_ids, info.id());
    res?;
    Ok(())
}

#[inline(always)]
fn encoded_params(params: &[Value]) -> Vec<EncodedValue> {
    params.iter().map(EncodedValue::from).collect()
}

/// Helper function to materialize a virtual table on storage.
fn virtual_table_materialize<R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
    child_id: NodeId,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
{
    let mut info = QueryInfo::new(optional, required);
    let mut locked_cache = runtime.cache().lock();
    let mut port = Port::new_port_c();
    let port_c = unsafe { port.as_mut_port_c() };
    sql_execute::<R>(&mut locked_cache, &mut info, port_c)?;
    let ir_plan = optional.exec_plan.get_ir_plan();
    let columns = vtable_columns(ir_plan, child_id)?;

    let mut vtable = VirtualTable::with_columns(columns);
    for tuple in port_c.iter() {
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

fn update_execute<R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
    port: &mut PortC,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
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

    let tuple = Tuple::new(&[result.row_count])
        .map_err(|e| SbroadError::Invalid(Entity::Tuple, Some(format_smolstr!("{e:?}"))))?;

    port.add_tuple(&tuple);

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

fn delete_execute<R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
    port: &mut PortC,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
{
    let plan = optional.exec_plan.get_ir_plan();
    let delete_id = plan.get_top()?;
    let delete_childen = plan.children(delete_id);

    if delete_childen.is_empty() {
        // We have a deal with a DELETE without WHERE filter
        // and want to execute local SQL instead of space api.

        let mut info = QueryInfo::new(optional, required);
        let mut locked_cache = runtime.cache().lock();
        return sql_execute::<R>(&mut locked_cache, &mut info, port);
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

    let tuple = Tuple::new(&[result.row_count])
        .map_err(|e| SbroadError::Invalid(Entity::Tuple, Some(format_smolstr!("{e:?}"))))?;

    port.add_tuple(&tuple);

    Ok(())
}

fn insert_execute<R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
    port: &mut PortC,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
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

    let tuple = Tuple::new(&[result.row_count])
        .map_err(|e| SbroadError::Invalid(Entity::Tuple, Some(format_smolstr!("{e:?}"))))?;

    port.add_tuple(&tuple);

    Ok(())
}
