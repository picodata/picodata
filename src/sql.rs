//! Clusterwide SQL query execution.

use crate::access_control::access_check_plugin_system;
use crate::access_control::{validate_password, UserMetadataKind};
use crate::audit;
use crate::backoff::SimpleBackoffManager;
use crate::cas::Predicate;
use crate::catalog::governor_queue;
use crate::column_name;
use crate::config::{AlterSystemParameters, DYNAMIC_CONFIG};
use crate::metrics::{self, STORAGE_1ST_REQUESTS_TOTAL};
use crate::plugin::{InheritOpts, PluginIdentifier, TopologyUpdateOpKind};
use crate::preemption::with_sql_execution_guard;
use crate::schema::{
    wait_for_ddl_commit, CreateIndexParams, CreateProcParams, CreateTableParams, DdlError,
    DistributionParam, Field, IndexOption, PrivilegeDef, PrivilegeType, RenameRoutineParams,
    RoutineDef, RoutineLanguage, RoutineParamDef, RoutineParams, RoutineSecurity, SchemaObjectType,
    ShardingFn, UserDef, ADMIN_ID,
};
use crate::sql::router::RouterRuntime;
use crate::sql::storage::{FullDeleteInfo, StorageRuntime};
use crate::storage::Catalog;
use crate::storage::{get_backup_dir_name, space_by_name, DbConfig, SystemTable, ToEntryIter};
use crate::sync::wait_for_index_globally;
use crate::traft::error::{self, Error};
use crate::traft::node::Node as TraftNode;
use crate::traft::op::{Acl as OpAcl, Ddl as OpDdl, Dml, DmlKind, Op, RenameMappingBuilder};
use crate::traft::{self, node};
use crate::util::{duration_from_secs_f64_clamped, effective_user_id};
use crate::version::Version;
use crate::{cas, has_states, plugin, tlog};

use ::tarantool::access_control::{
    box_access_check_ddl, box_access_check_space, PrivType, SchemaObjectType as TntSchemaObjectType,
};
use ::tarantool::auth::{AuthData, AuthDef};
use ::tarantool::decimal::Decimal;
use ::tarantool::error::IntoBoxError;
use ::tarantool::error::{BoxError, Error as TarantoolError, TarantoolErrorCode};
use ::tarantool::schema::function::func_next_reserved_id;
use ::tarantool::session::{with_su, UserId};
use ::tarantool::space::{FieldType, Space, SpaceId, SystemSpace, UpdateOps};
use ::tarantool::time::Instant;
use ::tarantool::tuple::{Decode, FunctionArgs, FunctionCtx, ToTupleBuffer, Tuple};
use ::tarantool::{msgpack, session, set_error};
use chrono::Utc;
use picodata_plugin::error_code::ErrorCode;
use rmp::decode::{read_array_len, read_bin_len, read_str_len};
use rmp::encode::{write_bin, write_str, write_uint};
use smallvec::SmallVec;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use sql::errors::{Entity, SbroadError};
use sql::executor::engine::helpers::{
    build_delete_args, build_insert_args, build_update_args, init_delete_tuple_builder,
    init_insert_tuple_builder, init_local_update_tuple_builder,
};
use sql::executor::engine::Router;
use sql::executor::protocol::SchemaInfo;
use sql::executor::result::ConsumerResult;
use sql::executor::ExecutingQuery;
use sql::executor::{Port, PortType};
use sql::ir::acl::{AlterOption, AuditPolicyOption, GrantRevokeType, Privilege as SqlPrivilege};
use sql::ir::ddl::{AlterSystemType, ParamDef};
use sql::ir::node::acl::AclOwned;
use sql::ir::node::block::Block;
use sql::ir::node::ddl::{Ddl, DdlOwned};
use sql::ir::node::expression::ExprOwned;
use sql::ir::node::plugin::{
    AppendServiceToTier, ChangeConfig, CreatePlugin, DisablePlugin, DropPlugin, EnablePlugin,
    MigrateTo, Plugin, RemoveServiceFromTier, SettingsPair,
};
use sql::ir::node::relational::Relational;
use sql::ir::node::{
    AlterColumn, AlterSystem, AlterTableOp, AlterUser, ArenaType, AuditPolicy, Constant,
    CreateIndex, CreateProc, CreateRole, CreateTable, CreateUser, Delete, DropIndex, DropProc,
    DropRole, DropTable, DropUser, GrantPrivilege, Insert, Node as IrNode, Node136, Node64, Node96,
    NodeOwned, Procedure, RenameIndex, RenameRoutine, RevokePrivilege, ScanRelation, SetParam,
    Update,
};
use sql::ir::node::{NodeId, TruncateTable};
use sql::ir::operator::ConflictStrategy;
use sql::ir::types::UnrestrictedType;
use sql::ir::value::Value;
use sql::ir::Plan as IrPlan;
use sql_protocol::decode::{
    execute_args_split, query_meta_args_split, ProtocolMessage, ProtocolMessageType, QueryMetaArgs,
};
use sql_protocol::encode::write_metadata;
use std::cmp::max;
use std::collections::BTreeMap;
use std::io::{Cursor, Error as IoError, Result as IoResult};
use std::ops::{ControlFlow, ControlFlow::Break, ControlFlow::Continue};
use std::rc::Rc;
use std::str::from_utf8_unchecked;
use std::time::Duration;

pub mod dispatch;
pub mod execute;
pub mod lua;
pub mod port;
pub mod router;
pub mod storage;

use self::lua::{escape_bytes, reference_add, reference_del, reference_use};
use self::port::PicoPortC;
use self::router::DEFAULT_QUERY_TIMEOUT;
use crate::sql::dispatch::build_cache_miss_dql_packet;
use serde::Serialize;
use sql::BoundStatement;

pub const DEFAULT_BUCKET_COUNT: u64 = 3000;

enum Privileges {
    Read,
    Write,
    ReadWrite,
}

// Collect data access nodes from the plan, i.e. nodes referring to tables.
fn find_data_access_nodes(plan: &IrPlan) -> impl Iterator<Item = NodeId> + use<'_> {
    fn node_id(offset: usize, arena_type: ArenaType) -> NodeId {
        NodeId {
            offset: offset.try_into().unwrap(),
            arena_type,
        }
    }

    let delete = plan
        .get_nodes()
        .iter64()
        .enumerate()
        .filter_map(|(i, n)| match n {
            Node64::Delete(_) => Some(node_id(i, ArenaType::Arena64)),
            _ => None,
        });

    let scan_and_insert = plan
        .get_nodes()
        .iter96()
        .enumerate()
        .filter_map(|(i, n)| match n {
            Node96::ScanRelation(_) => Some(node_id(i, ArenaType::Arena96)),
            Node96::Insert(_) => Some(node_id(i, ArenaType::Arena96)),
            _ => None,
        });

    let update = plan
        .get_nodes()
        .iter136()
        .enumerate()
        .filter_map(|(i, n)| match n {
            Node136::Update(_) => Some(node_id(i, ArenaType::Arena136)),
            _ => None,
        });

    delete.chain(scan_and_insert).chain(update)
}

fn check_table_privileges(plan: &IrPlan) -> traft::Result<()> {
    let nodes = find_data_access_nodes(plan);

    // We don't want to switch the user back and forth for each node, so we
    // collect all space ids and privileges and then check them all at once.
    let mut space_privs = SmallVec::<[(SpaceId, Privileges); 16]>::new();

    // Switch to admin to get space ids. At the moment we don't use space cache in tarantool
    // module and can't get space metadata without _space table read permissions.
    with_su(ADMIN_ID, || -> traft::Result<()> {
        for node_id in nodes {
            let rel_node = plan.get_relation_node(node_id)?;
            let (relation, privileges) = match rel_node {
                Relational::ScanRelation(ScanRelation { relation, .. }) => {
                    (relation, Privileges::Read)
                }
                Relational::Insert(Insert { relation, .. }) => (relation, Privileges::Write),
                Relational::Delete(Delete { relation, .. })
                | Relational::Update(Update { relation, .. }) => {
                    // We check write and read privileges for deletes and updates.
                    //
                    // Write: Picodata doesn't support delete and update privileges,
                    // so we grant write access instead.
                    //
                    // Read: SQL standard says that update and delete statements
                    // should check for read access when they contain a where
                    // clause (to protect from information leaks). But we don't
                    // expect that updates and deletes would be used without a
                    // where clause (long operations are not good for Picodata).
                    // So, let's make it simple and avoid special cases.
                    (relation, Privileges::ReadWrite)
                }
                // This should never happen as we have filtered out all other plan nodes.
                _ => unreachable!("internal bug on the table privilege check"),
            };
            let space = space_by_name(relation)?;
            space_privs.push((space.id(), privileges))
        }
        Ok(())
    })??;
    for (space_id, priviledges) in space_privs {
        match priviledges {
            Privileges::Read => {
                box_access_check_space(space_id, PrivType::Read)?;
            }
            Privileges::Write => {
                box_access_check_space(space_id, PrivType::Write)?;
            }
            Privileges::ReadWrite => {
                box_access_check_space(space_id, PrivType::Read)?;
                box_access_check_space(space_id, PrivType::Write)?;
            }
        }
    }
    Ok(())
}

fn routine_by_name(name: &str) -> traft::Result<RoutineDef> {
    // Switch to admin to get procedure definition.
    with_su(ADMIN_ID, || {
        let storage = &node::global()?.storage;
        let routine = storage.routines.by_name(name)?.ok_or_else(|| {
            Error::Sbroad(SbroadError::Invalid(
                Entity::Routine,
                Some(format_smolstr!("routine {name} not found")),
            ))
        })?;
        Ok(routine)
    })?
}

fn check_routine_privileges(plan: &IrPlan) -> traft::Result<()> {
    // At the moment we don't support nested procedure calls, so we can safely
    // assume that the top node is the only procedure in the plan.
    let top_id = plan.get_top()?;
    let Ok(Block::Procedure(Procedure { name, .. })) = plan.get_block_node(top_id) else {
        // There are no procedures in the plan tree: nothing to check.
        return Ok(());
    };

    let routine = routine_by_name(name)?;
    box_access_check_ddl(
        name,
        routine.id,
        routine.owner,
        TntSchemaObjectType::Function,
        PrivType::Execute,
    )?;
    Ok(())
}

fn port_write_dml_response<'p>(port: &mut impl Port<'p>, changed: u64) {
    let mut mp = [0_u8; 9];
    let pos = {
        let mut wr = Cursor::new(&mut mp[..]);
        let _ = write_uint(&mut wr, changed).expect("Failed to write DML response");
        wr.position() as usize
    };
    port.add_mp(&mp[..pos]);
}

/// Same as [`dispatch_bound_statement`], but does not collect any metrics
fn dispatch_bound_statement_impl<'p>(
    runtime: &RouterRuntime,
    statement: BoundStatement,
    override_deadline: Option<Instant>,
    governor_op_id: Option<u64>,
    port: &mut impl Port<'p>,
) -> traft::Result<()> {
    let mut query = ExecutingQuery::from_bound_statement(runtime, statement);
    if query.is_empty() {
        port.set_type(PortType::DispatchDml);
        port_write_dml_response(port, 0);
        return Ok(());
    }

    if query.get_exec_plan().get_ir_plan().is_raw_explain() {
        port.set_type(PortType::DispatchQueryPlan);
    } else if query.is_explain() {
        port.set_type(PortType::DispatchExplain);
    } else if query.get_exec_plan().get_ir_plan().is_dql()? || query.is_backup()? {
        port.set_type(PortType::DispatchDql);
    } else {
        port.set_type(PortType::DispatchDml);
    }

    if query.is_deallocate()? {
        port_write_dml_response(port, 0);
        return Ok(());
    }

    if query.is_tcl()? {
        let ir_plan = query.get_exec_plan().get_ir_plan();
        let top_id = ir_plan.get_top()?;
        let tcl = ir_plan.get_tcl_node(top_id)?;
        tlog!(
            Warning,
            "Transactions are currently unsupported. Empty query response provided for {}.",
            tcl.as_str()
        );
        port_write_dml_response(port, 0);
        return Ok(());
    }

    if query.is_backup()? {
        let ir_plan = query.get_exec_plan().get_ir_plan();
        let top_id = ir_plan.get_top()?;
        let ir_plan_mut = query.get_mut_exec_plan().get_mut_ir_plan();

        let ir_node = ir_plan_mut.replace_with_stub(top_id);
        let node = node::global()?;
        let tuple =
            reenterable_schema_change_request(node, ir_node, override_deadline, governor_op_id)?;
        let mp = tuple.data();
        let mut cur = Cursor::new(mp);
        let len = read_array_len(&mut cur).map_err(Error::other)?;
        if len != 2 {
            return Err(Error::other("Backup returned invalid msgpack response."));
        }

        // Extract metadata msgpack from the binary.
        let meta_len = read_bin_len(&mut cur).map_err(Error::other)? as usize;
        let pos = cur.position() as usize;
        let meta_mp = &mp[pos..pos + meta_len];
        port.write_all(meta_mp).map_err(Error::other)?;
        cur.set_position((pos + meta_len) as u64);

        // Repack directory string into a single element array msgpack.
        let dir_len = read_str_len(&mut cur).map_err(Error::other)? as usize;
        let mut row_mp: Vec<u8> = b"\x91".to_vec();
        let pos = cur.position() as usize;
        let dir_bytes = &mp[pos..pos + dir_len];
        let s = unsafe { from_utf8_unchecked(dir_bytes) };
        write_str(&mut row_mp, s).map_err(Error::other)?;
        port.write_all(row_mp.as_slice()).map_err(Error::other)?;
        Ok(())
    } else if query.is_ddl()? || query.is_acl()? {
        let ir_plan = query.get_exec_plan().get_ir_plan();
        let top_id = ir_plan.get_top()?;

        if let IrNode::Ddl(_) = ir_plan.get_node(top_id)? {
            let ddl_node = ir_plan.get_ddl_node(top_id)?;
            if let Ddl::CreateSchema = ddl_node {
                tlog!(
                    Warning,
                    "DDL for schemas is currently unsupported. Empty query response provided for CREATE SCHEMA."
                );
                port_write_dml_response(port, 0);
                return Ok(());
            }
            if let Ddl::DropSchema = ddl_node {
                tlog!(
                    Warning,
                    "DDL for schemas is currently unsupported. Empty query response provided for DROP SCHEMA."
                );
                port_write_dml_response(port, 0);
                return Ok(());
            }
        }

        let ir_plan_mut = query.get_mut_exec_plan().get_mut_ir_plan();

        let ir_node = ir_plan_mut.replace_with_stub(top_id);
        let node = node::global()?;
        let tuple =
            reenterable_schema_change_request(node, ir_node, override_deadline, governor_op_id)?;
        let row_count = rows_changed(tuple).map_err(Error::other)?;
        port_write_dml_response(port, row_count);
        Ok(())
    } else if query.is_plugin()? {
        let ir_plan = query.get_exec_plan().get_ir_plan();
        let top_id = ir_plan.get_top()?;
        let plugin = ir_plan.get_plugin_node(top_id)?;
        let timeout_from_decimal = |decimal: Decimal| -> traft::Result<_> {
            let secs = decimal
                .to_smolstr()
                .parse::<f64>()
                .map_err(|e| Error::Other(e.into()))?;
            let mut timeout = duration_from_secs_f64_clamped(secs);
            if let Some(override_deadline) = override_deadline {
                let override_deadline = override_deadline.duration_since(Instant::now_fiber());
                timeout = timeout.min(override_deadline);
            }
            Ok(timeout)
        };

        // NOTE: this is different from how access checks are done for DDL, because:
        // 1) We do not plan on allowing non-superusers to do plugin operations in the near future.
        // 2) Preparing a plugin operation involves accessing a number of
        //    internal tables and potentially even running plugin code for
        //    validation purposes, so we exit early in case of denied access.
        // See also <https://git.picodata.io/picodata/picodata/picodata/-/issues/965>
        let as_user = effective_user_id();
        with_su(ADMIN_ID, || access_check_plugin_system(as_user))??;

        match plugin {
            Plugin::Create(CreatePlugin {
                name,
                version,
                if_not_exists,
                timeout,
            }) => plugin::create_plugin(
                PluginIdentifier::new(name.clone(), version.clone()),
                timeout_from_decimal(*timeout)?,
                *if_not_exists,
                InheritOpts {
                    // config and topology inheritance always enabled currently
                    config: true,
                    topology: true,
                },
            )?,
            Plugin::Enable(EnablePlugin {
                name,
                version,
                timeout,
            }) => plugin::enable_plugin(
                &PluginIdentifier::new(name.clone(), version.clone()),
                // TODO this option should be un-hardcoded and moved into picodata configuration
                Duration::from_secs(10),
                timeout_from_decimal(*timeout)?,
            )?,
            Plugin::Disable(DisablePlugin {
                name,
                version,
                timeout,
            }) => plugin::disable_plugin(
                &PluginIdentifier::new(name.clone(), version.clone()),
                timeout_from_decimal(*timeout)?,
            )?,
            Plugin::Drop(DropPlugin {
                name,
                version,
                if_exists,
                with_data,
                timeout,
            }) => plugin::drop_plugin(
                &PluginIdentifier::new(name.clone(), version.clone()),
                *with_data,
                *if_exists,
                timeout_from_decimal(*timeout)?,
            )?,
            Plugin::MigrateTo(MigrateTo {
                name,
                version,
                opts,
            }) => plugin::migration_up(
                &PluginIdentifier::new(name.clone(), version.clone()),
                timeout_from_decimal(opts.timeout)?,
                timeout_from_decimal(opts.rollback_timeout)?,
            )?,
            Plugin::AppendServiceToTier(AppendServiceToTier {
                service_name,
                plugin_name,
                version,
                tier,
                timeout,
            }) => plugin::update_service_tiers(
                &PluginIdentifier::new(plugin_name.clone(), version.clone()),
                service_name,
                tier,
                TopologyUpdateOpKind::Add,
                timeout_from_decimal(*timeout)?,
            )?,
            Plugin::RemoveServiceFromTier(RemoveServiceFromTier {
                service_name,
                plugin_name,
                version,
                tier,
                timeout,
            }) => plugin::update_service_tiers(
                &PluginIdentifier::new(plugin_name.clone(), version.clone()),
                service_name,
                tier,
                TopologyUpdateOpKind::Remove,
                timeout_from_decimal(*timeout)?,
            )?,
            Plugin::ChangeConfig(ChangeConfig {
                plugin_name,
                version,
                key_value_grouped: key_value,
                timeout,
            }) => {
                let config = key_value
                    .iter()
                    .map(|settings| {
                        (
                            settings.name.as_str(),
                            settings
                                .pairs
                                .iter()
                                .map(|SettingsPair { key, value }| (key.as_str(), value.as_str()))
                                .collect(),
                        )
                    })
                    .collect::<Vec<_>>();

                plugin::change_config_atom(
                    &PluginIdentifier::new(plugin_name.clone(), version.clone()),
                    &config,
                    timeout_from_decimal(*timeout)?,
                )?
            }
        };

        port_write_dml_response(port, 1);
        Ok(())
    } else if query.is_block()? {
        check_routine_privileges(query.get_exec_plan().get_ir_plan())?;
        let ir_plan = query.get_mut_exec_plan().get_mut_ir_plan();
        let top_id = ir_plan.get_top()?;
        let code_block = ir_plan.get_block_node(top_id)?;
        match code_block {
            Block::Procedure(Procedure { name, values }) => {
                let values = values.clone();
                let options = ir_plan.effective_options.clone();

                let routine = routine_by_name(name)?;

                // Check that the amount of passed values is correct.
                if routine.params.len() != values.len() {
                    return Err(Error::Sbroad(SbroadError::Invalid(
                        Entity::Routine,
                        Some(format_smolstr!(
                            "expected {} parameter(s), got {}",
                            routine.params.len(),
                            values.len(),
                        )),
                    )));
                }
                // XXX: at the moment we don't support multiple SQL statements in a block.
                // So, we can safely assume that the procedure body contains only one statement
                // and call it directly.
                let pattern = routine.body;
                let mut params: Vec<Value> = Vec::with_capacity(values.len());
                for (pos, value_id) in values.into_iter().enumerate() {
                    let ir_plan_mut = query.get_mut_exec_plan().get_mut_ir_plan();
                    let constant_node = ir_plan_mut.replace_with_stub(value_id);
                    let value = match constant_node {
                        NodeOwned::Expression(ExprOwned::Constant(Constant { value, .. })) => value,
                        _ => {
                            return Err(Error::Sbroad(SbroadError::Invalid(
                                Entity::Expression,
                                Some(format_smolstr!("expected constant, got {constant_node:?}")),
                            )))
                        }
                    };
                    // We have already checked the amount of passed values, so we can
                    // safely assume that the parameter exists at the given position.
                    let param_def = &routine.params[pos];
                    let param_type = UnrestrictedType::try_from(param_def.r#type)?;
                    // Check that the value has a correct type.
                    if let Some(ty) = value.get_type().get() {
                        if !ty.is_castable_to(&param_type) {
                            return Err(Error::Sbroad(SbroadError::Invalid(
                                Entity::Routine,
                                Some(format_smolstr!(
                                    "expected {} for parameter on position {pos}, got {}",
                                    param_def.r#type,
                                    ty,
                                )),
                            )));
                        }
                    }
                    params.push(value);
                }

                let bound_statement =
                    BoundStatement::parse_and_bind(runtime, &pattern, params, options)?;
                if bound_statement.params_for_audit().is_some() {
                    audit::policy::log_dml_for_user(&pattern, bound_statement.params_for_audit());
                }

                dispatch_bound_statement_impl(
                    runtime,
                    bound_statement,
                    override_deadline,
                    None,
                    port,
                )
            }
        }
    } else {
        let plan = query.get_exec_plan().get_ir_plan();
        check_table_privileges(plan)?;

        if query.is_explain() {
            port.set_type(PortType::DispatchExplain);
            let mut mp: Vec<u8> = Vec::new();
            for line in query.as_explain()?.lines() {
                write_str(&mut mp, line).map_err(Error::other)?;
                port.add_mp(&mp);
                mp.clear();
            }
            return Ok(());
        }

        // check if table is operable
        with_su(ADMIN_ID, || {
            let top_id = plan.get_top()?;
            if plan.get_relation_node(top_id)?.is_dml() {
                let storage = &node::global()?.storage;
                let table = plan.dml_node_table(top_id)?;
                let table_name = table.name.clone();

                let table_id = storage
                    .pico_table
                    .by_name(&table_name)?
                    .ok_or(traft::error::DoesNotExist::Table(table_name))?
                    .id;

                cas::check_table_operable(storage, table_id)?;
            }

            Ok::<(), Error>(())
        })??;

        if plan.is_dml_on_global_table()? && !plan.is_raw_explain() {
            let ConsumerResult { row_count } =
                do_dml_on_global_tbl(query, override_deadline, governor_op_id)?;
            port_write_dml_response(port, row_count);
            return Ok(());
        }

        query.dispatch(port).map_err(Error::Sbroad)?;
        Ok(())
    }
}

/// Execute the cluster SQL query.
///
/// `override_deadline` if provided is used to override the timeout provided in
/// the `OPTION (TIMEOUT = ?)` part of the SQL query. Note that overriding only
/// happens downwards, that is it can only be decreased but not increased.
///
/// This is needed for example for `ALTER PLUGIN MIGRATE` queries, so that the
/// whole query doesn't take longer (give or take) than the timeout specified by
/// the user.
pub fn dispatch_bound_statement<'p>(
    runtime: &RouterRuntime,
    statement: BoundStatement,
    override_deadline: Option<Instant>,
    governor_op_id: Option<u64>,
    port: &mut impl Port<'p>,
) -> traft::Result<()> {
    let start = Instant::now_fiber();

    let result =
        dispatch_bound_statement_impl(runtime, statement, override_deadline, governor_op_id, port);

    let tier = &runtime
        .get_current_tier_name()
        .map_err(|e| Error::Other(e.into()))?
        .unwrap_or_else(|| SmolStr::new("unknown_tier"));

    let node = node::global()?;

    let raft_id = node.raft_id;
    let inst = node.storage.instances.get(&raft_id)?;
    let replicaset_name = &inst.replicaset_name;

    if result.is_err() {
        metrics::record_sql_query_errors_total(tier, replicaset_name);
    }
    let duration = Instant::now_fiber().duration_since(start).as_millis();
    metrics::observe_sql_query_duration(tier, replicaset_name, duration as f64);
    metrics::record_sql_query_total(tier, replicaset_name);

    result
}

fn err_for_tnt_console(e: traft::error::Error) -> traft::error::Error {
    match e {
        Error::Sbroad(SbroadError::ParsingError(_, message)) if message.contains('\n') => {
            // Tweak the error message so that tarantool's yaml handler
            // prints it in human-readable form
            //
            // `+ 20` for message prefix
            // `+ 1` for one extra '\n' at the end
            let mut buffer = String::with_capacity(message.len() + 21);
            buffer.push_str("rule parsing error: ");
            for line in message.lines() {
                // There must not be any spaces at the end of lines,
                // otherwise the string will be formatted incorrectly
                buffer.push_str(line.trim_end());
                buffer.push('\n');
            }
            // There must be at least one empty line so that tarantool
            // formats the string correctly (it's a special hack they use
            // for the help feature in the lua console).
            buffer.push('\n');
            BoxError::new(TarantoolErrorCode::SqlUnrecognizedSyntax, buffer).into()
        }
        e => e,
    }
}

struct DispatchArgs {
    pattern: String,
    params: Vec<Value>,
}

impl<'de> Decode<'de> for DispatchArgs {
    fn decode(data: &'de [u8]) -> tarantool::Result<Self> {
        // XXX: function arguments are stored in the fiber()->gc region and can't
        // survive a fiber yield (port_c_get_msgpack). We must materialize all the
        // data referencing arguments' bytes into the rust memory before calling any
        // code that can possibly yield to avoid heap-after-free.
        #[cfg(debug_assertions)]
        let _guard = crate::util::NoYieldsGuard::new();

        let (pattern, params): (String, Vec<Value>) = msgpack::decode(data)?;
        Ok(DispatchArgs { pattern, params })
    }
}

/// # Safety
/// Dispatches an SQL query to the cluster.
/// Part of public RPC API.
#[no_mangle]
pub unsafe extern "C" fn proc_sql_dispatch(
    mut ctx: FunctionCtx,
    args: FunctionArgs,
) -> ::std::os::raw::c_int {
    let bind_args = match args.decode::<DispatchArgs>() {
        Ok(v) => v,
        Err(e) => return report("", Error::from(e)),
    };

    // All arguments should be copied into the rust memory to
    // survive possible fiber switch.
    let result = with_sql_execution_guard(|| {
        let mut port = PicoPortC::from(ctx.mut_port_c());
        parse_and_dispatch(&bind_args.pattern, bind_args.params, None, None, &mut port)
    });

    match result {
        Ok(_) => return 0,
        Err(e) => {
            let fmt_err = err_for_tnt_console(e);
            fmt_err.set_last_error();
            return -1;
        }
    }
}

pub fn parse_and_dispatch<'p>(
    query_text: &str,
    params: Vec<Value>,
    override_deadline: Option<Instant>,
    governor_op_id: Option<u64>,
    port: &mut impl Port<'p>,
) -> traft::Result<()> {
    let router = RouterRuntime::new();

    let Some(sql_options) = DYNAMIC_CONFIG.current_sql_options() else {
        return Err(Error::Uninitialized);
    };
    let bound_statement = BoundStatement::parse_and_bind(&router, query_text, params, sql_options)?;
    if bound_statement.params_for_audit().is_some() {
        audit::policy::log_dml_for_user(query_text, bound_statement.params_for_audit());
    }

    dispatch_bound_statement(
        &router,
        bound_statement,
        override_deadline,
        governor_op_id,
        port,
    )?;
    Ok(())
}

impl TryFrom<&SqlPrivilege> for PrivilegeType {
    type Error = SbroadError;

    fn try_from(item: &SqlPrivilege) -> Result<Self, Self::Error> {
        match item {
            SqlPrivilege::Read => Ok(PrivilegeType::Read),
            SqlPrivilege::Write => Ok(PrivilegeType::Write),
            SqlPrivilege::Execute => Ok(PrivilegeType::Execute),
            SqlPrivilege::Create => Ok(PrivilegeType::Create),
            SqlPrivilege::Alter => Ok(PrivilegeType::Alter),
            SqlPrivilege::Drop => Ok(PrivilegeType::Drop),

            // Picodata does not allow to grant or revoke session or usage
            // Instead this should be done through alter user with login/nologin
            SqlPrivilege::Session => Err(SbroadError::Unsupported(
                Entity::Privilege,
                Some("session".into()),
            )),
            SqlPrivilege::Usage => Err(SbroadError::Unsupported(
                Entity::Privilege,
                Some("usage".into()),
            )),
        }
    }
}

impl TraftNode {
    /// Helper method to retrieve next id for newly created user/role.
    fn get_next_grantee_id(&self) -> traft::Result<UserId> {
        let storage = &self.storage;
        let max_user_id = storage.users.max_user_id()?;
        if let Some(max_user_id) = max_user_id {
            return Ok(max_user_id + 1);
        }

        let max_tarantool_user_id: UserId = Space::from(SystemSpace::User)
            .index("primary")
            .expect("_user should have a primary index")
            .max(&())?
            .expect("_user must contain at least one row")
            .get(0)
            .expect("_user rows must contain id column");
        Ok(max_tarantool_user_id + 1)
    }

    /// Get table id by its name.
    /// Returns:
    /// * `Some(table_id)`` in case such table exists
    /// * `None` in case such table doesn't exist
    fn get_table_id(&self, table_name: &String) -> Option<u32> {
        let table = Space::from(SystemSpace::Space)
            .index("name")
            .expect("_space should have a name index")
            .get(&(table_name,))
            .expect("name index selection from _space should succeed");
        if let Some(table) = table {
            let table_id = table.get(0).expect("_space rows must contain id column");
            Some(table_id)
        } else {
            None
        }
    }

    /// Get user or role id by its name.
    /// Returns:
    /// * `Some(user_or_role_id)`` in case such user or role exists
    /// * `None` in case such user or role doesn't exist
    fn get_user_or_role_id(&self, user_or_role_name: &String) -> Option<UserId> {
        let user_or_role = Space::from(SystemSpace::VUser)
            .index("name")
            .expect("_vuser should have a name index")
            .get(&(user_or_role_name,))
            .expect("name index selection from _vuser should succeed");
        if let Some(user_or_role) = user_or_role {
            let user_or_role_id = user_or_role
                .get(0)
                .expect("_vuser rows must contain id column");
            Some(user_or_role_id)
        } else {
            None
        }
    }

    /// Get (object_type, privilege_type, object_id) data from `GrantRevokeType`.
    fn object_resolve(
        &self,
        grant_revoke_type: &GrantRevokeType,
    ) -> traft::Result<(SchemaObjectType, PrivilegeType, i64)> {
        match grant_revoke_type {
            GrantRevokeType::User { privilege } => {
                Ok((SchemaObjectType::User, privilege.try_into()?, -1))
            }
            GrantRevokeType::SpecificUser {
                privilege,
                user_name,
            } => {
                if let Some(user_id) = self.get_user_or_role_id(&user_name.to_string()) {
                    Ok((
                        SchemaObjectType::User,
                        privilege.try_into()?,
                        user_id as i64,
                    ))
                } else {
                    Err(Error::Sbroad(SbroadError::Invalid(
                        Entity::Acl,
                        Some(format_smolstr!("There is no user with name {user_name}")),
                    )))
                }
            }
            GrantRevokeType::Role { privilege } => {
                Ok((SchemaObjectType::Role, privilege.try_into()?, -1))
            }
            GrantRevokeType::SpecificRole {
                privilege,
                role_name,
            } => {
                if let Some(role_id) = self.get_user_or_role_id(&role_name.to_string()) {
                    Ok((
                        SchemaObjectType::Role,
                        privilege.try_into()?,
                        role_id as i64,
                    ))
                } else {
                    Err(Error::Sbroad(SbroadError::Invalid(
                        Entity::Acl,
                        Some(format_smolstr!("There is no role with name {role_name}")),
                    )))
                }
            }
            GrantRevokeType::Table { privilege } => {
                Ok((SchemaObjectType::Table, privilege.try_into()?, -1))
            }
            GrantRevokeType::SpecificTable {
                privilege,
                table_name,
            } => {
                if let Some(table_id) = self.get_table_id(&table_name.to_string()) {
                    Ok((
                        SchemaObjectType::Table,
                        privilege.try_into()?,
                        table_id as i64,
                    ))
                } else {
                    Err(Error::Sbroad(SbroadError::Invalid(
                        Entity::Acl,
                        Some(format_smolstr!("There is no table with name {table_name}")),
                    )))
                }
            }
            GrantRevokeType::Procedure { privilege } => {
                Ok((SchemaObjectType::Routine, privilege.try_into()?, -1))
            }
            GrantRevokeType::SpecificProcedure {
                privilege,
                proc_name,
                proc_params,
            } => {
                if let Some(routine) = self.storage.routines.by_name(proc_name)? {
                    if let Some(params) = proc_params.as_ref() {
                        ensure_parameters_match(&routine, params)?;
                    }
                    Ok((
                        SchemaObjectType::Routine,
                        privilege.try_into()?,
                        routine.id as i64,
                    ))
                } else {
                    Err(Error::Sbroad(SbroadError::Invalid(
                        Entity::Acl,
                        Some(format_smolstr!("There is no routine with name {proc_name}")),
                    )))
                }
            }
            GrantRevokeType::RolePass { role_name } => {
                if let Some(role_id) = self.get_user_or_role_id(&role_name.to_string()) {
                    Ok((
                        SchemaObjectType::Role,
                        PrivilegeType::Execute,
                        role_id as i64,
                    ))
                } else {
                    Err(Error::Sbroad(SbroadError::Invalid(
                        Entity::Acl,
                        Some(format_smolstr!("There is no role with name {role_name}")),
                    )))
                }
            }
        }
    }
}

/// Get grantee (user or role) UserId by its name.
fn get_grantee_id(storage: &Catalog, grantee_name: &str) -> traft::Result<UserId> {
    if let Some(grantee_user_def) = storage.users.by_name(grantee_name)? {
        Ok(grantee_user_def.id)
    } else {
        // No existing user or role found.
        Err(Error::Sbroad(SbroadError::Invalid(
            Entity::Acl,
            Some(format_smolstr!(
                "Nor user, neither role with name {grantee_name} exists"
            )),
        )))
    }
}

/// Find whether given privilege was already granted.
fn check_privilege_already_granted(
    node: &TraftNode,
    grantee_id: UserId,
    object_type: &str,
    object_id: i64,
    privilege: &str,
) -> traft::Result<bool> {
    let storage = &node.storage;
    Ok(storage
        .privileges
        .get(grantee_id, object_type, object_id, privilege)?
        .is_some())
}

fn ensure_parameters_match(routine: &RoutineDef, params: &[ParamDef]) -> traft::Result<()> {
    if routine.params.len() == params.len() {
        let parameters_matched = routine
            .params
            .iter()
            .zip(params)
            .all(|(param_def, param)| param_def.r#type == FieldType::from(&param.data_type));

        if parameters_matched {
            return Ok(());
        }
    };

    let actual_signature = format!(
        "{}({})",
        routine.name,
        routine
            .params
            .iter()
            .map(|def| def.r#type.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    Err(Error::Other(
        format!("routine exists but with a different signature: {actual_signature}").into(),
    ))
}

fn check_name_emptyness(name: &str) -> traft::Result<()> {
    if name.is_empty() {
        return Err(Error::Other("expected non empty name".into()));
    }

    Ok(())
}

fn alter_user_ir_node_to_op_or_result(
    name: &SmolStr,
    alter_option: &AlterOption,
    current_user: UserId,
    schema_version: u64,
    node: &TraftNode,
    storage: &Catalog,
) -> traft::Result<ControlFlow<ConsumerResult, Op>> {
    let user_def = storage.users.by_name(name)?;
    let user_def = match user_def {
        // Unable to alter role
        Some(user_def) if user_def.is_role() => {
            return Err(Error::Other(
                format!("Role {name} exists. Unable to alter role.").into(),
            ));
        }
        None => return Err(error::DoesNotExist::User(name.clone()).into()),
        Some(user_def) => user_def,
    };

    match alter_option {
        AlterOption::Password {
            password,
            auth_method,
        } => {
            validate_password(password, auth_method, &node.alter_system_parameters)?;
            let data = AuthData::new(auth_method, name, password);
            let auth = AuthDef::new(*auth_method, data.into_string());

            if user_def
                .auth
                .expect("user always should have non empty auth")
                == auth
            {
                // Password is already the one given, no op needed.
                return Ok(Break(ConsumerResult { row_count: 0 }));
            }

            Ok(Continue(Op::Acl(OpAcl::ChangeAuth {
                user_id: user_def.id,
                auth: auth.clone(),
                initiator: current_user,
                schema_version,
            })))
        }
        AlterOption::Login => {
            // Note: We do not check if login privilege has already been granted, since a user may
            // have it on one node but not on another due to node local "out of authentication
            // attempts" automatic ban. This grant must be applied globally to ensure the user
            // has login access across all nodes.
            let priv_def = PrivilegeDef::login(
                get_grantee_id(storage, name.as_str())?,
                current_user,
                schema_version,
            );

            Ok(Continue(Op::Acl(OpAcl::GrantPrivilege { priv_def })))
        }
        AlterOption::NoLogin => {
            let priv_def = PrivilegeDef::login(
                get_grantee_id(storage, name.as_str())?,
                current_user,
                schema_version,
            );

            Ok(Continue(Op::Acl(OpAcl::RevokePrivilege {
                priv_def,
                initiator: current_user,
            })))
        }
        AlterOption::Rename { new_name } => {
            check_name_emptyness(new_name)?;

            if &user_def.name == new_name {
                return Err(error::DoesNotExist::User(user_def.name.to_smolstr()).into());
            }
            let user = storage.users.by_name(new_name)?;
            match user {
                Some(_) => return Err(error::AlreadyExists::User(new_name.clone()).into()),
                None => Ok(Continue(Op::Acl(OpAcl::RenameUser {
                    user_id: user_def.id,
                    name: new_name.clone(),
                    initiator: current_user,
                    schema_version,
                }))),
            }
        }
    }
}

fn acl_ir_node_to_op_or_result(
    acl: &AclOwned,
    current_user: UserId,
    schema_version: u64,
    node: &TraftNode,
    storage: &Catalog,
) -> traft::Result<ControlFlow<ConsumerResult, Op>> {
    match acl {
        AclOwned::DropRole(DropRole {
            name, if_exists, ..
        }) => {
            let Some(role_def) = storage.users.by_name(name)? else {
                if *if_exists {
                    return Ok(ControlFlow::Break(ConsumerResult { row_count: 0 }));
                } else {
                    return Err(error::DoesNotExist::Role(name.clone()).into());
                }
            };
            if !role_def.is_role() {
                return Err(Error::Sbroad(SbroadError::Invalid(
                    Entity::Acl,
                    Some(format_smolstr!("User {name} exists. Unable to drop user.")),
                )));
            }

            Ok(Continue(Op::Acl(OpAcl::DropRole {
                role_id: role_def.id,
                initiator: current_user,
                schema_version,
            })))
        }
        AclOwned::DropUser(DropUser {
            name, if_exists, ..
        }) => {
            let Some(user_def) = storage.users.by_name(name)? else {
                if *if_exists {
                    return Ok(ControlFlow::Break(ConsumerResult { row_count: 0 }));
                } else {
                    return Err(error::DoesNotExist::User(name.clone()).into());
                }
            };
            if user_def.is_role() {
                return Err(Error::Sbroad(SbroadError::Invalid(
                    Entity::Acl,
                    Some(format_smolstr!("Role {name} exists. Unable to drop role.")),
                )));
            }

            Ok(Continue(Op::Acl(OpAcl::DropUser {
                user_id: user_def.id,
                initiator: current_user,
                schema_version,
            })))
        }
        AclOwned::CreateRole(CreateRole {
            name,
            if_not_exists,
            ..
        }) => {
            check_name_emptyness(name)?;
            storage.users.check_user_limit()?;

            let sys_user = Space::from(SystemSpace::User)
                .index("name")
                .expect("_user should have an index by name")
                .get(&(name,))?;
            if let Some(user) = sys_user {
                let entry_type: &str = user.get(3).unwrap();
                if entry_type == "user" {
                    return Err(error::AlreadyExists::User(name.clone()).into());
                }
                if *if_not_exists {
                    return Ok(ControlFlow::Break(ConsumerResult { row_count: 0 }));
                }
                return Err(error::AlreadyExists::Role(name.clone()).into());
            }
            let id = node.get_next_grantee_id()?;
            let role_def = UserDef {
                id,
                name: name.clone(),
                // This field will be updated later.
                schema_version,
                owner: current_user,
                auth: None,
                ty: UserMetadataKind::Role,
            };
            Ok(Continue(Op::Acl(OpAcl::CreateRole { role_def })))
        }
        AclOwned::CreateUser(CreateUser {
            name,
            password,
            auth_method,
            if_not_exists,
            ..
        }) => {
            check_name_emptyness(name)?;
            storage.users.check_user_limit()?;

            validate_password(password, auth_method, &node.alter_system_parameters)?;
            let data = AuthData::new(auth_method, name, password);
            let auth = AuthDef::new(*auth_method, data.into_string());

            let user_def = storage.users.by_name(name)?;
            if let Some(user_def) = user_def {
                if user_def.is_role() {
                    return Err(error::AlreadyExists::Role(name.clone()).into());
                }
                if *if_not_exists {
                    return Ok(ControlFlow::Break(ConsumerResult { row_count: 0 }));
                }
                return Err(error::AlreadyExists::User(name.clone()).into());
            }

            let id = node.get_next_grantee_id()?;
            let user_def = UserDef {
                id,
                name: name.clone(),
                schema_version,
                auth: Some(auth.clone()),
                owner: current_user,
                ty: UserMetadataKind::User,
            };
            Ok(Continue(Op::Acl(OpAcl::CreateUser { user_def })))
        }
        AclOwned::AlterUser(AlterUser {
            name, alter_option, ..
        }) => alter_user_ir_node_to_op_or_result(
            name,
            alter_option,
            current_user,
            schema_version,
            node,
            storage,
        ),
        AclOwned::GrantPrivilege(GrantPrivilege {
            grant_type,
            grantee_name,
            ..
        }) => {
            let grantor_id = current_user;
            let grantee_id = get_grantee_id(storage, grantee_name)?;
            let (object_type, privilege, object_id) = node.object_resolve(grant_type)?;

            if check_privilege_already_granted(
                node,
                grantee_id,
                &object_type,
                object_id,
                &privilege,
            )? {
                // Privilege is already granted, no op needed.
                return Ok(Break(ConsumerResult { row_count: 0 }));
            }
            Ok(Continue(Op::Acl(OpAcl::GrantPrivilege {
                priv_def: PrivilegeDef::new(
                    grantor_id,
                    grantee_id,
                    privilege,
                    object_type,
                    object_id,
                    schema_version,
                )
                .map_err(Error::other)?,
            })))
        }
        AclOwned::RevokePrivilege(RevokePrivilege {
            revoke_type,
            grantee_name,
            ..
        }) => {
            let grantor_id = current_user;
            let grantee_id = get_grantee_id(storage, grantee_name)?;
            let (object_type, privilege, object_id) = node.object_resolve(revoke_type)?;

            if !check_privilege_already_granted(
                node,
                grantee_id,
                &object_type,
                object_id,
                &privilege,
            )? {
                // Privilege is not granted yet, no op needed.
                return Ok(Break(ConsumerResult { row_count: 0 }));
            }

            Ok(Continue(Op::Acl(OpAcl::RevokePrivilege {
                priv_def: PrivilegeDef::new(
                    grantor_id,
                    grantee_id,
                    privilege,
                    object_type,
                    object_id,
                    schema_version,
                )
                .map_err(Error::other)?,
                initiator: current_user,
            })))
        }
        AclOwned::AuditPolicy(AuditPolicy {
            policy_name,
            audit_option,
            ..
        }) => {
            let Some(policy_id) = audit::policy::get_audit_policy_id_by_name(policy_name) else {
                return Err(error::DoesNotExist::AuditPolicy(policy_name.clone()).into());
            };
            let (user_name, enable) = match audit_option {
                AuditPolicyOption::On { user_name } => (user_name, true),
                AuditPolicyOption::Off { user_name } => (user_name, false),
            };
            let Some(user_def) = storage.users.by_name(user_name)? else {
                return Err(error::DoesNotExist::User(user_name.clone()).into());
            };
            if user_def.is_role() {
                return Err(Error::Other(
                    "the statement works for users only, not for roles".into(),
                ));
            }
            let user_audit_policy = storage.users_audit_policies.get(user_def.id, policy_id)?;
            if user_audit_policy.is_some() == enable {
                // No op needed.
                return Ok(Break(ConsumerResult { row_count: 0 }));
            }
            Ok(Continue(Op::Acl(OpAcl::AuditPolicy {
                user_id: user_def.id,
                policy_id,
                enable,
                initiator: current_user,
                schema_version,
            })))
        }
    }
}

#[rustfmt::skip]
fn alter_system_ir_node_to_op_or_result(
    storage: &Catalog,
    ty: &AlterSystemType,
    tier_name: Option<&str>,
    current_user: UserId,
) -> traft::Result<ControlFlow<ConsumerResult, Op>> {
    fn make_dmls<T>(
        param_value: &T,
        param_name: &str,
        tier_name: Option<&str>,
        storage: &Catalog,
        initiator: UserId,
    ) -> traft::Result<Vec<Dml>>
    where
        T: Serialize,
    {
        let table = crate::storage::DbConfig::TABLE_ID;
        let mut dmls = Vec::new();
        if AlterSystemParameters::has_scope_tier(param_name)? {
            if let Some(tier_name) = tier_name {
                let Some(tier) = storage.tiers.iter()?.find(|tier| tier.name == *tier_name) else {
                    return Err(Error::other(format!(
                        "specified tier '{tier_name}' doesn't exist"
                    )));
                };

                dmls.push(Dml::replace(
                    table,
                    &(param_name, &tier.name, &param_value),
                    initiator,
                )?);
            } else {
                for tier in storage.tiers.iter()? {
                    dmls.push(Dml::replace(
                        table,
                        &(param_name, &tier.name, &param_value),
                        initiator,
                    )?);
                }
            }
        } else {
            if let Some(tier_name) = tier_name {
                return Err(Error::other(format!(
                    "parameter with global scope can't be configured for tier '{tier_name}'"
                )));
            };

            dmls.push(Dml::replace(
                table,
                &(param_name, DbConfig::GLOBAL_SCOPE, &param_value),
                initiator,
            )?);
        }

        Ok(dmls)
    }

    match ty {
        AlterSystemType::AlterSystemSet {
            param_name,
            param_value,
        } => {
            let casted_value: sql::ir::value::EncodedValue<'_> = crate::config::validate_alter_system_parameter_value(param_name, param_value)?;

            let dmls = make_dmls(&casted_value, param_name, tier_name, storage, current_user)?;

            Ok(Continue(Op::BatchDml{ ops: dmls }))
        }
        AlterSystemType::AlterSystemReset { param_name } => {
            match param_name {
                // reset one
                Some(param_name) => {
                    let Some(default_value) = crate::config::get_default_value_of_alter_system_parameter(param_name) else {
                        return Err(Error::other(format!("unknown parameter: '{param_name}'")));
                    };

                    let dmls = make_dmls(&default_value, param_name, tier_name, storage, current_user)?;

                    Ok(Continue(Op::BatchDml { ops: dmls }))
                }
                // reset all
                None => {
                    let tiers = storage.tiers.iter()?.map(|tier| tier.name).collect::<Vec<_>>();
                    let tiers = tiers.iter().map(|s| &**s).collect::<Vec<_>>();
                    let dmls = crate::config::get_defaults_for_all_alter_system_parameters(&tiers)?;
                    Ok(Continue(Op::BatchDml { ops: dmls }))
                }
            }
        }
    }
}

fn get_new_backup_timestamp(storage: &Catalog) -> i64 {
    // TODO: See https://git.picodata.io/core/picodata/-/issues/2186.
    let current_datetime = Utc::now();
    let current_timestamp = current_datetime.timestamp();

    let last_backup_timestamp = storage
        .properties
        .last_backup_timestamp()
        .expect("read of _pico_property should succeed");

    if let Some(last_backup_timestamp) = last_backup_timestamp {
        max(current_timestamp, last_backup_timestamp + 1)
    } else {
        current_timestamp
    }
}

fn ddl_ir_node_to_op_or_result(
    ddl: &DdlOwned,
    current_user: UserId,
    schema_version: u64,
    node: &TraftNode,
    storage: &Catalog,
    governor_op_id: Option<u64>,
) -> traft::Result<ControlFlow<ConsumerResult, Op>> {
    match ddl {
        DdlOwned::AlterSystem(AlterSystem { ty, tier_name, .. }) => {
            alter_system_ir_node_to_op_or_result(storage, ty, tier_name.as_deref(), current_user)
        }
        DdlOwned::CreateTable(CreateTable {
            name,
            format,
            primary_key,
            sharding_key,
            engine_type,
            tier,
            if_not_exists,
            ..
        }) => {
            let format = format
                .iter()
                .map(|f| Field {
                    name: f.name.clone(),
                    r#type: FieldType::from(&f.data_type),
                    is_nullable: f.is_nullable,
                })
                .collect();
            let distribution = if sharding_key.is_some() {
                DistributionParam::Sharded
            } else {
                DistributionParam::Global
            };

            let primary_key = primary_key.clone();
            let sharding_key = sharding_key.clone();

            let mut params = CreateTableParams {
                id: None,
                name: name.clone(),
                format,
                primary_key,
                distribution,
                by_field: None,
                sharding_key,
                sharding_fn: Some(ShardingFn::Murmur3),
                engine: Some(*engine_type),
                timeout: None,
                owner: current_user,
                tier: tier.clone(),
            };
            params.validate()?;

            if params.space_exists()? {
                if *if_not_exists {
                    return Ok(Break(ConsumerResult { row_count: 0 }));
                } else {
                    return Err(error::AlreadyExists::Table(params.name.to_smolstr()).into());
                }
            }

            params.check_tier_exists(storage)?;

            params.choose_id_if_not_specified(name, governor_op_id)?;
            params.check_primary_key(storage)?;
            params.test_create_space(storage)?;
            let ddl = params.into_ddl()?;
            Ok(Continue(Op::DdlPrepare {
                schema_version,
                ddl,
                governor_op_id,
            }))
        }
        DdlOwned::DropTable(DropTable {
            name, if_exists, ..
        }) => {
            let Some(table_def) = storage.pico_table.by_name(name)? else {
                if *if_exists {
                    return Ok(Break(ConsumerResult { row_count: 0 }));
                } else {
                    return Err(error::DoesNotExist::Table(name.clone()).into());
                }
            };
            let ddl = OpDdl::DropTable {
                id: table_def.id,
                initiator: current_user,
            };
            Ok(Continue(Op::DdlPrepare {
                schema_version,
                ddl,
                governor_op_id,
            }))
        }
        DdlOwned::TruncateTable(TruncateTable { name, .. }) => {
            let Some(table_def) = storage.pico_table.by_name(name)? else {
                return Err(error::DoesNotExist::Table(name.clone()).into());
            };
            let ddl = OpDdl::TruncateTable {
                id: table_def.id,
                initiator: current_user,
            };
            Ok(Continue(Op::DdlPrepare {
                schema_version,
                ddl,
                governor_op_id,
            }))
        }
        DdlOwned::Backup(_) => {
            let timestamp = get_new_backup_timestamp(storage);
            let ddl = OpDdl::Backup { timestamp };
            Ok(Continue(Op::DdlPrepare {
                schema_version,
                ddl,
                governor_op_id,
            }))
        }
        DdlOwned::CreateProc(CreateProc {
            name,
            params,
            body,
            language,
            if_not_exists,
            ..
        }) => {
            let params: RoutineParams = params
                .iter()
                .map(|p| {
                    let field_type = FieldType::from(&p.data_type);
                    RoutineParamDef::default().with_type(field_type)
                })
                .collect();
            let language = RoutineLanguage::from(language.clone());
            let security = RoutineSecurity::default();

            let params = CreateProcParams {
                name: name.clone(),
                params,
                language,
                body: body.clone(),
                security,
                owner: current_user,
            };

            if params.func_exists() || storage.routines.by_name(&params.name)?.is_some() {
                if *if_not_exists {
                    return Ok(Break(ConsumerResult { row_count: 0 }));
                }
                return Err(error::AlreadyExists::Procedure(params.name.to_smolstr()).into());
            }
            let id = func_next_reserved_id()?;
            let ddl = OpDdl::CreateProcedure {
                id,
                name: params.name.clone(),
                params: params.params.clone(),
                language: params.language.clone(),
                body: params.body.clone(),
                security: params.security.clone(),
                owner: params.owner,
            };
            Ok(Continue(Op::DdlPrepare {
                schema_version,
                ddl,
                governor_op_id,
            }))
        }
        DdlOwned::DropProc(DropProc {
            name,
            params,
            if_exists,
            ..
        }) => {
            let Some(routine) = &storage.routines.by_name(name)? else {
                if *if_exists {
                    return Ok(Break(ConsumerResult { row_count: 0 }));
                } else {
                    return Err(error::DoesNotExist::Procedure(name.clone()).into());
                }
            };

            // drop by name if no parameters are specified
            if let Some(params) = params {
                ensure_parameters_match(routine, params)?;
            }

            let ddl = OpDdl::DropProcedure {
                id: routine.id,
                initiator: current_user,
            };
            Ok(Continue(Op::DdlPrepare {
                schema_version,
                ddl,
                governor_op_id,
            }))
        }
        DdlOwned::RenameRoutine(RenameRoutine {
            old_name,
            new_name,
            params,
            ..
        }) => {
            let params = RenameRoutineParams {
                new_name: new_name.clone(),
                old_name: old_name.clone(),
                params: params.clone(),
            };

            if !params.func_exists() {
                return Err(error::DoesNotExist::Procedure(params.old_name.to_smolstr()).into());
            }

            if params.new_name_occupied() {
                return Err(Error::Other(
                    format!("Name '{}' is already taken", params.new_name).into(),
                ));
            }

            let routine_def = node
                .storage
                .routines
                .by_name(&params.old_name)?
                .expect("if routine ddl is correct, routine must exist");

            if let Some(params) = params.params.as_ref() {
                ensure_parameters_match(&routine_def, params)?;
            }

            let ddl = OpDdl::RenameProcedure {
                routine_id: routine_def.id,
                new_name: params.new_name.clone(),
                old_name: params.old_name.clone(),
                initiator_id: current_user,
                owner_id: routine_def.owner,
                schema_version,
            };

            Ok(Continue(Op::DdlPrepare {
                schema_version,
                ddl,
                governor_op_id,
            }))
        }
        DdlOwned::CreateIndex(CreateIndex {
            name,
            table_name,
            columns,
            unique,
            index_type,
            bloom_fpr,
            page_size,
            range_size,
            run_count_per_level,
            run_size_ratio,
            dimension,
            distance,
            hint,
            if_not_exists,
            ..
        }) => {
            let mut opts: Vec<IndexOption> = Vec::with_capacity(9);
            opts.push(IndexOption::Unique(*unique));
            if let Some(bloom_fpr) = bloom_fpr {
                opts.push(IndexOption::BloomFalsePositiveRate(*bloom_fpr));
            }
            if let Some(page_size) = page_size {
                opts.push(IndexOption::PageSize(*page_size));
            }
            if let Some(range_size) = range_size {
                opts.push(IndexOption::RangeSize(*range_size));
            }
            if let Some(run_count_per_level) = run_count_per_level {
                opts.push(IndexOption::RunCountPerLevel(*run_count_per_level));
            }
            if let Some(run_size_ratio) = run_size_ratio {
                opts.push(IndexOption::RunSizeRatio(*run_size_ratio));
            }
            if let Some(dimension) = dimension {
                opts.push(IndexOption::Dimension(*dimension));
            }
            if let Some(distance) = distance {
                opts.push(IndexOption::Distance(*distance));
            }
            if let Some(hint) = hint {
                opts.push(IndexOption::Hint(*hint));
            }
            opts.shrink_to_fit();

            let columns = columns.clone();

            let params = CreateIndexParams {
                name: name.clone(),
                space_name: table_name.clone(),
                columns,
                ty: *index_type,
                opts,
                initiator: current_user,
            };
            params.validate(storage)?;

            if params.index_exists() || storage.indexes.by_name(&params.name)?.is_some() {
                if *if_not_exists {
                    return Ok(Break(ConsumerResult { row_count: 0 }));
                } else {
                    return Err(error::AlreadyExists::Index(params.name.to_smolstr()).into());
                }
            }

            let ddl = params.into_ddl(storage)?;
            Ok(Continue(Op::DdlPrepare {
                schema_version,
                ddl,
                governor_op_id,
            }))
        }
        DdlOwned::DropIndex(DropIndex {
            name, if_exists, ..
        }) => {
            let Some(index) = storage.indexes.by_name(name)? else {
                if *if_exists {
                    return Ok(Break(ConsumerResult { row_count: 0 }));
                } else {
                    return Err(error::DoesNotExist::Index(name.clone()).into());
                }
            };
            let ddl = OpDdl::DropIndex {
                space_id: index.table_id,
                index_id: index.id,
                initiator: current_user,
            };
            Ok(Continue(Op::DdlPrepare {
                schema_version,
                ddl,
                governor_op_id,
            }))
        }
        DdlOwned::RenameIndex(RenameIndex {
            old_name,
            new_name,
            if_exists,
            ..
        }) => {
            let Some(index) = storage.indexes.by_name(old_name.as_str())? else {
                if *if_exists {
                    return Ok(Break(ConsumerResult { row_count: 0 }));
                } else {
                    return Err(error::DoesNotExist::Index(old_name.to_owned()).into());
                }
            };
            if storage.indexes.by_name(new_name.as_str())?.is_some() {
                return Err(error::AlreadyExists::Index(new_name.to_owned()).into());
            }
            let Some(table) = storage.pico_table.get(index.table_id)? else {
                return Err(
                    error::DoesNotExist::Table(format_smolstr!("id: {}", index.table_id)).into(),
                );
            };
            Ok(Continue(Op::DdlPrepare {
                schema_version,
                ddl: OpDdl::RenameIndex {
                    space_id: index.table_id,
                    index_id: index.id,
                    old_name: old_name.clone(),
                    new_name: new_name.clone(),
                    initiator_id: current_user,
                    owner_id: table.owner,
                    schema_version,
                },
                governor_op_id,
            }))
        }
        DdlOwned::AlterTable(alter_table) => {
            let Some(table) = &storage.pico_table.by_name(&alter_table.name)? else {
                return Err(error::DoesNotExist::Table(alter_table.name.clone()).into());
            };

            match &alter_table.op {
                AlterTableOp::AlterColumn(columns) => {
                    let current_table_format = table.format.clone();
                    let mut new_table_format = current_table_format.clone(); // inevitable clone
                    let mut column_renames = RenameMappingBuilder::new();
                    let mut column_name_set = BTreeMap::new();
                    for (i, f) in (0..).zip(&new_table_format) {
                        column_name_set.insert(SmolStr::new(&f.name), i);
                    }
                    let mut num_skipped_ops = 0;

                    for op in columns.iter() {
                        match op {
                            &AlterColumn::Add {
                                ref column,
                                if_not_exists,
                            } => {
                                // do not add this column with the same name
                                if column_name_set
                                    .insert(column.name.clone(), new_table_format.len())
                                    .is_some()
                                {
                                    if if_not_exists {
                                        num_skipped_ops += 1;
                                        continue;
                                    }
                                    return Err(
                                        error::AlreadyExists::Column(column.name.clone()).into()
                                    );
                                }

                                // append this new column
                                let field = tarantool::space::Field {
                                    name: column.name.to_string(),
                                    field_type: FieldType::from(&column.data_type),
                                    is_nullable: column.is_nullable,
                                };
                                new_table_format.push(field);
                            }
                            AlterColumn::Rename { from, to } => {
                                let Some(index) = column_name_set.remove(from.as_str()) else {
                                    return Err(error::DoesNotExist::Column(from.clone()).into());
                                };
                                if column_name_set.insert(to.clone(), index).is_some() {
                                    return Err(error::AlreadyExists::Column(to.clone()).into());
                                }
                                column_renames.add_rename(from.clone(), to.clone());
                                new_table_format[index].name = to.to_string();
                            }
                        }
                    }

                    if num_skipped_ops == columns.len() {
                        return Ok(Break(ConsumerResult { row_count: 0 }));
                    }

                    Ok(Continue(Op::DdlPrepare {
                        schema_version,
                        ddl: OpDdl::ChangeFormat {
                            table_id: table.id,
                            old_format: current_table_format,
                            new_format: new_table_format,
                            column_renames: column_renames.build(),
                            initiator_id: current_user,
                            schema_version,
                        },
                        governor_op_id,
                    }))
                }
                AlterTableOp::RenameTable { new_table_name } => {
                    if storage.pico_table.by_name(new_table_name)?.is_some() {
                        return Err(error::AlreadyExists::Table(new_table_name.clone()).into());
                    };

                    Ok(Continue(Op::DdlPrepare {
                        schema_version,
                        ddl: OpDdl::RenameTable {
                            table_id: table.id,
                            old_name: table.name.clone(),
                            new_name: new_table_name.clone(),
                            initiator_id: current_user,
                            owner_id: table.owner,
                            schema_version,
                        },
                        governor_op_id,
                    }))
                }
            }
        }
        DdlOwned::SetParam(SetParam { param_value, .. }) => {
            tlog!(
                Warning,
                "Parameters setting is currently disabled. Skipping update for {}.",
                param_value.param_name()
            );
            Ok(Break(ConsumerResult { row_count: 0 }))
        }
        DdlOwned::SetTransaction { .. } => {
            tlog!(
                Warning,
                "Transaction setting is currently disabled. Skipping."
            );
            Ok(Break(ConsumerResult { row_count: 0 }))
        }
        DdlOwned::CreateSchema | DdlOwned::DropSchema => {
            return Err(Error::Other(
                "unreachable CreateSchema/DropSchema".to_string().into(),
            ));
        }
    }
}

/// Check if operation is applied to the storage.
/// There three possible outcomes:
/// - `Ok(true)` - operation was applied successfully.
/// - `Ok(false)` - operation was not applied, but it is still pending.
/// - `Err(_)` - operation was not applied due to some error.
fn check_ddl_applied(
    storage: &Catalog,
    ddl: &traft::op::Ddl,
    schema_version: u64,
) -> traft::Result<bool> {
    let error = |reason: &str| {
        Err(BoxError::new(
            ErrorCode::RaftLogCompacted,
            "Log compaction happened during DDL execution. ".to_string() + reason,
        )
        .into())
    };

    let pending_schema_version = storage.properties.pending_schema_version()?;
    match ddl {
        OpDdl::CreateTable { name, .. } => {
            let Some(table_def) = storage.pico_table.by_name(name)? else {
                tlog!(Warning, "Table `{name}` has already been dropped.");
                return error(
                    "Table does not exist: either operation was aborted \
                    or table was dropped afterwards.",
                );
            };

            if table_def.schema_version != schema_version {
                tlog!(
                    Warning,
                    "Table `{name}` has changed, schema version: {}.",
                    table_def.schema_version
                );
                return error(
                    "Can't find out the result of the operation, \
                    but table was changed afterwards.",
                );
            }

            if !table_def.operable {
                debug_assert_eq!(pending_schema_version, Some(schema_version),);
                return Ok(false);
            }

            Ok(true)
        }
        OpDdl::DropTable { id, .. } => {
            let Some(table_def) = storage.pico_table.get(*id)? else {
                return Ok(true);
            };

            if table_def.schema_version > schema_version {
                tlog!(
                    Warning,
                    "Table with id `{id}` has changed, schema version: {}",
                    table_def.schema_version,
                );
                return error(
                    "Can't find out the result of the operation, \
                    but table was recreated afterwards.",
                );
            }

            if table_def.operable {
                tlog!(Warning, "Table with id `{id}`: deletion was aborted.");
                return error("Operation was aborted.");
            }

            debug_assert_eq!(pending_schema_version, Some(schema_version),);
            Ok(false)
        }
        OpDdl::RenameTable {
            old_name,
            new_name,
            schema_version,
            table_id,
            ..
        } => {
            let Some(table_def) = storage.pico_table.get(*table_id)? else {
                tlog!(
                    Warning,
                    "Table with id `{table_id}` was dropped while renaming \
                    `{old_name}` to `{new_name}`"
                );
                return error(
                    "Can't find out the result of the operation, \
                    but table was dropped afterwards.",
                );
            };

            if table_def.schema_version > *schema_version {
                tlog!(
                    Warning,
                    "Table `{old_name}` has changed, schema version: {} vs {}",
                    table_def.schema_version,
                    schema_version,
                );
                return error(
                    "Can't find out the result of the operation, \
                    but table was changed afterwards.",
                );
            }

            if !table_def.operable {
                debug_assert_eq!(pending_schema_version, Some(*schema_version),);
                return Ok(false);
            }

            debug_assert_eq!(&table_def.name, new_name);
            Ok(true)
        }
        OpDdl::CreateIndex {
            space_id,
            index_id,
            name,
            ..
        } => {
            let Some(index_def) = storage.indexes.get(*space_id, *index_id)? else {
                tlog!(
                    Warning,
                    "Index `{name}` ({index_id} for space {space_id}) has already been dropped"
                );
                return error("Index does not exist: either operation was aborted or index was dropped afterwards.");
            };

            if index_def.schema_version != schema_version {
                tlog!(
                    Warning,
                    "Index `{name}` has changed, schema version: {}",
                    index_def.schema_version
                );
                return error(
                    "Can't find out the result of the operation, \
                    but index was changed afterwards.",
                );
            }

            if !index_def.operable {
                debug_assert_eq!(pending_schema_version, Some(schema_version),);
                return Ok(false);
            }

            debug_assert_eq!(&index_def.name, name);
            Ok(true)
        }
        OpDdl::DropIndex {
            space_id, index_id, ..
        } => {
            let Some(index_def) = storage.indexes.get(*space_id, *index_id)? else {
                tlog!(
                    Warning,
                    "{} {} {}",
                    "Index with id `{index_id}` on space with space_id `{space_id}`",
                    "was already dropped, probably not by current DropTable with",
                    "schema_version={schema_version}"
                );
                return Ok(true);
            };

            if index_def.schema_version > schema_version {
                tlog!(
                    Warning,
                    "Index with id `{index_id}` on space with space_id `{space_id}` \
                    has changed, schema version: {}",
                    index_def.schema_version
                );
                return error(
                    "Can't find out the result of the operation, \
                    but index was changed afterwards.",
                );
            }

            if index_def.operable {
                tlog!(
                    Warning,
                    "Index with id `{index_id}` on space with id `{space_id}`: \
                    deletion was aborted."
                );
                return error("Operation was aborted");
            }

            debug_assert_eq!(pending_schema_version, Some(schema_version));
            Ok(false)
        }
        OpDdl::RenameIndex {
            space_id, index_id, ..
        } => {
            let Some(index_def) = storage.indexes.get(*space_id, *index_id)? else {
                tlog!(
                    Warning,
                    "Index ({index_id} for space {space_id}) has already been dropped"
                );
                return Ok(true);
            };

            if index_def.schema_version != schema_version {
                tlog!(
                    Warning,
                    "Index ({index_id} for space {space_id}) has changed, \
                    schema version: {} vs {}",
                    index_def.schema_version,
                    schema_version,
                );
                return error(
                    "Can't find out the result of the operation, \
                    but index was changed afterwards.",
                );
            }

            if !index_def.operable {
                debug_assert_eq!(pending_schema_version, Some(schema_version));
                return Ok(false);
            }

            Ok(true)
        }
        OpDdl::CreateProcedure { id, name, .. } => {
            let Some(routine_def) = storage.routines.by_id(*id)? else {
                tlog!(
                    Warning,
                    "Routine `{name}` with id {id} has already been dropped"
                );
                return error(
                    "Routine does not exist: either operation was aborted \
                    or routine was dropped afterwards.",
                );
            };

            if routine_def.schema_version != schema_version {
                tlog!(
                    Warning,
                    "Routine `{name}` has changed, schema version: {}",
                    routine_def.schema_version
                );
                return error(
                    "Can't find out the result of the operation, \
                    but routine was changed afterwards.",
                );
            }

            if !routine_def.operable {
                debug_assert_eq!(pending_schema_version, Some(schema_version));
                return Ok(false);
            }

            Ok(true)
        }
        OpDdl::DropProcedure { id, .. } => {
            let Some(routine_def) = storage.routines.by_id(*id)? else {
                return Ok(true);
            };

            if routine_def.schema_version > schema_version {
                tlog!(
                    Warning,
                    "Routine with id `{id}` has changed, schema version: {}",
                    routine_def.schema_version
                );
                return error(
                    "Can't find out the result of the operation, \
                    but routine was changed afterwards.",
                );
            }

            if routine_def.operable {
                tlog!(Warning, "Routine with id `{id}`: deletion was aborted.");
                return error("Operation was aborted.");
            }

            debug_assert_eq!(pending_schema_version, Some(schema_version));
            Ok(false)
        }
        OpDdl::RenameProcedure {
            routine_id,
            old_name,
            new_name,
            ..
        } => {
            // New name should exists
            let Some(routine_def) = storage.routines.by_id(*routine_id)? else {
                tlog!(
                    Warning,
                    "Routine {routine_id} was deleted while renaming \
                    from `{old_name}` to `{new_name}`"
                );
                return error(
                    "Can't find out the result of the operation, \
                    but routine was deleted afterwards.",
                );
            };

            if routine_def.schema_version > schema_version {
                tlog!(
                    Warning,
                    "Routine with id {routine_id} has changed, schema version: {}",
                    routine_def.schema_version
                );
                return error(
                    "Can't find out the result of the operation, \
                    but routine was changed afterwards.",
                );
            }

            if !routine_def.operable {
                debug_assert_eq!(pending_schema_version, Some(schema_version));
                return Ok(false);
            }

            debug_assert_eq!(&routine_def.name, new_name);
            Ok(true)
        }
        OpDdl::ChangeFormat {
            table_id,
            new_format,
            ..
        } => {
            let Some(table_def) = storage.pico_table.get(*table_id)? else {
                tlog!(
                    Warning,
                    "Table with id `{table_id}` has already been dropped."
                );
                return error("Table {table_id} has been dropped while changing format.");
            };

            if table_def.schema_version > schema_version {
                tlog!(
                    Warning,
                    "Table `{}` has changed, schema version: {}",
                    table_def.name,
                    table_def.schema_version,
                );
                return error(
                    "Can't find out the result of the operation, \
                    but table was changed afterwards.",
                );
            }

            if !table_def.operable {
                debug_assert_eq!(pending_schema_version, Some(schema_version));
                return Ok(false);
            }

            debug_assert_eq!(&table_def.format, new_format);
            Ok(true)
        }
        OpDdl::TruncateTable { id, .. } => {
            if storage.pico_table.get(*id)?.is_none() {
                tlog!(Warning, "Table with id `{id}` has already been dropped.");
                return error("Table has been dropped while truncating.");
            };

            Ok(true)
        }
        OpDdl::Backup { timestamp } => {
            let pending_schema_change = storage.properties.pending_schema_change()?;
            if let Some(traft::op::Ddl::Backup {
                timestamp: pending_timestamp,
            }) = pending_schema_change
            {
                if pending_timestamp == *timestamp {
                    return Ok(false);
                }
            }

            let Some(last_backup_timestamp) = storage.properties.last_backup_timestamp()? else {
                return error("abort for unknown reason: log compacted");
            };

            if last_backup_timestamp < *timestamp {
                return error("abort for unknown reason: log compacted");
            }

            Ok(true)
        }
    }
}

/// Validates whether a DDL operation is allowed.
///
/// A cluster is considered heterogeneous if any instance differs from another by major or minor version.
/// DDL execution in such cluster is prohibited, the only exception is the `DROP` operations.
///
/// Returns an error if the operation is not permitted under the current cluster conditions.
fn ensure_ddl_allowed_in_cluster(node: &TraftNode, ir_node: &NodeOwned) -> traft::Result<()> {
    match ir_node {
        NodeOwned::Ddl(ref ddl) if !ddl.is_drop_operation() => {
            let topology_ref = node.topology_cache.get();
            let mut not_expelled_instances = topology_ref
                .all_instances()
                .filter(|instance| !has_states!(instance, Expelled -> *));

            let first_instance = not_expelled_instances
                .next()
                .expect("cluster should consist of at least one instance");

            let first_version = Version::try_from(first_instance.picodata_version.as_str())
                .expect("got from system table, should be already verified");

            for instance in not_expelled_instances {
                let version = Version::try_from(instance.picodata_version.as_str())
                    .expect("got from system table, should be already verified");

                if first_version.cmp_up_to_minor(&version).is_ne() {
                    let first_instance_name = first_instance.name.to_string();
                    let first_instance_version = first_instance.picodata_version.to_string();
                    let second_instance_name = instance.name.to_string();
                    let second_instance_version = instance.picodata_version.to_string();

                    let err = DdlError::ProhibitedInHeterogeneousCluster {
                        first_instance_name,
                        first_instance_version,
                        second_instance_name,
                        second_instance_version,
                    };

                    tlog!(Warning, "{}", err);

                    return Err(err.into());
                }
            }
        }
        _ => (),
    }

    Ok(())
}

pub(crate) fn reenterable_schema_change_request(
    node: &TraftNode,
    ir_node: NodeOwned,
    override_deadline: Option<Instant>,
    governor_op_id: Option<u64>,
) -> traft::Result<Tuple> {
    let storage = &node.storage;

    ensure_ddl_allowed_in_cluster(node, &ir_node)?;

    // Save current user as later user is switched to admin
    let current_user = effective_user_id();

    // This timeout comes from `OPTION (TIMEOUT = ?)` part of the SQL query
    let timeout = match &ir_node {
        NodeOwned::Ddl(ddl) => ddl.timeout()?,
        NodeOwned::Acl(acl) => acl.timeout()?,
        n => {
            unreachable!("this function should only be called for ddl or acl nodes, not {n:?}")
        }
    };
    let timeout = duration_from_secs_f64_clamped(timeout);
    let mut deadline = Instant::now_fiber().saturating_add(timeout);
    // This timeout comes from the arugments to this function.
    // For example this could be a timeout passed to `ALTER PLUGIN MIGRATE TO`.
    if let Some(override_deadline) = override_deadline {
        deadline = override_deadline.min(deadline);
    }

    let _su = session::su(ADMIN_ID).expect("cant fail because admin should always have session");

    'retry: loop {
        if Instant::now_fiber() > deadline {
            return Err(Error::timeout());
        }

        // read_index is important as a protection from stale reads.
        // Behavior we'd like to avoid:
        // 1. The instance is stale
        // 2. The user tries to create an index, but the preceeding
        //    table creation didn't replicate to this instance yet
        // 3. Without read_index the error message would be misleading
        //    saying that the table doesn't exist but in fact an
        //    instance is just lagging behind and cant serve the request
        let index = node.read_index(deadline.duration_since(Instant::now_fiber()))?;

        if storage.properties.pending_schema_change()?.is_some() {
            let target_index = index + 1;
            tlog!(
                Info,
                "Waiting for {target_index} pending_schema_change target index"
            );
            node.wait_index(target_index, deadline.duration_since(Instant::now_fiber()))?;
            continue 'retry;
        }

        let schema_version = storage.properties.next_schema_version()?;

        let mut wait_applied_globally = false;
        let op_or_result = match &ir_node {
            NodeOwned::Acl(acl) => {
                acl_ir_node_to_op_or_result(acl, current_user, schema_version, node, storage)?
            }
            NodeOwned::Ddl(ddl) => {
                wait_applied_globally = ddl.wait_applied_globally();
                ddl_ir_node_to_op_or_result(
                    ddl,
                    current_user,
                    schema_version,
                    node,
                    storage,
                    governor_op_id,
                )?
            }
            n => unreachable!("function must be called only for ddl or acl nodes, not {n:?}"),
        };

        let op = match op_or_result {
            Break(consumer_result) => {
                let tuple: Tuple = Tuple::new(&(consumer_result,))?;
                return Ok(tuple);
            }
            Continue(op) => op,
        };

        // TODO: Should look at https://git.picodata.io/picodata/picodata/picodata/-/issues/866.
        let predicate = cas::Predicate::new(index, cas::schema_change_ranges());
        let req = crate::cas::Request::new(op.clone(), predicate, current_user)?;
        let res = cas::compare_and_swap_and_wait(&req, deadline)?;
        let index = match res {
            cas::CasResult::Ok((index, _, _)) => index,
            cas::CasResult::RetriableError(_) => continue,
        };

        if let Op::DdlPrepare { ref ddl, .. } = op {
            if governor_op_id.is_some() {
                // It means we are running a governor operation
                // (`sql_dispatch` was called from the governor)
                // Returns early and continues to process DDL
                // in the next governor's step
                let res = ConsumerResult { row_count: 1 };
                let tuple: Tuple = Tuple::new(&(res,))?;
                return Ok(tuple);
            }
            // this is not a governor operation, so do all stuff here...
            let commit_index = loop {
                let res = wait_for_ddl_commit(index, deadline.duration_since(Instant::now_fiber()));

                let compacted_index = match res {
                    Ok(index) => index,
                    Err(err) => {
                        if err.error_code() != ErrorCode::RaftLogCompacted as u32 {
                            return Err(err);
                        }

                        // If we will find our DDL result in metadata with corresponding version, then our DdlCommit was compacted,
                        // otherwise it may be aborted, not yet applied, recreated(schema_version will be higher than ours).
                        match check_ddl_applied(storage, ddl, schema_version) {
                            // !!! commit index used only to wait for it applied globally,
                            // so we can use compacted_index.
                            Ok(true) => node.raft_storage.compacted_index()?,
                            Ok(false) => {
                                let applied_index = node.get_index();
                                let target_index = applied_index + 1;
                                tlog!(
                                    Info,
                                    "Waiting for {target_index} unapplied ddl target index"
                                );
                                node.wait_index(
                                    target_index,
                                    deadline.duration_since(Instant::now_fiber()),
                                )?;
                                continue;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                };

                break compacted_index;
            };

            if wait_applied_globally {
                wait_for_index_globally(
                    &node.topology_cache,
                    Rc::clone(&node.pool),
                    commit_index,
                    deadline,
                )
                .map_err(|_| {
                    Error::Other(
                        "ddl operation committed, but failed to receive \
                         acknowledgements from all instances"
                            .into(),
                    )
                })?;
            }
        }

        let tuple = match op {
            Op::DdlPrepare {
                ddl: OpDdl::Backup { timestamp },
                ..
            } => {
                // Append metadata and a row as a binary and string msgpack
                // entries to reduce decoding when repack to the port:
                // [binary metadata, string backup_dir_name]
                let mut mp: Vec<u8> = b"\x92".to_vec();

                // Pack metadata as binary.
                let mut meta: Vec<u8> = Vec::new();
                write_metadata(&mut meta, [("backup_dir_name", "string")].into_iter(), 1)
                    .map_err(Error::other)?;
                write_bin(&mut mp, meta.as_slice()).map_err(Error::other)?;
                meta.clear();

                // Pack backup_dir_name as string.
                let backup_dir_name = get_backup_dir_name(timestamp);
                write_str(&mut mp, &backup_dir_name).map_err(Error::other)?;

                Tuple::try_from_slice(mp.as_slice())?
            }
            _ => {
                let res = ConsumerResult { row_count: 1 };
                Tuple::new(&(res,))?
            }
        };
        return Ok(tuple);
    }
}

fn rows_changed(tuple: Tuple) -> IoResult<u64> {
    let [ConsumerResult { row_count }] = tuple.decode().map_err(|e| {
        IoError::other(format!(
            "Failed to decode consumer result from reentrable schema change request: {e}"
        ))
    })?;
    Ok(row_count)
}

#[inline(always)]
fn report(msg: &str, e: Error) -> i32 {
    set_error!(TarantoolErrorCode::ProcC, "{msg}{e}");
    TarantoolErrorCode::ProcC as i32
}

struct ExecArgs {
    timeout: f64,
    need_ref: bool,
    sid: SmallVec<[u8; 36]>,
    rid: i64,
    data: Vec<u8>,
}

impl<'de> Decode<'de> for ExecArgs {
    fn decode(data: &'de [u8]) -> tarantool::Result<Self> {
        // XXX: function arguments are stored in the fiber()->gc region and can't
        // survive a fiber yield (port_c_get_msgpack). We must materialize all the
        // data referencing arguments' bytes into the rust memory before calling any
        // code that can possibly yield (reference add or plan execution) to avoid
        // heap-after-free.
        //
        // See also:
        // - https://github.com/tarantool/tarantool/issues/4792#issuecomment-592893087
        // - https://git.picodata.io/core/picodata/-/issues/2359
        #[cfg(debug_assertions)]
        let _guard = crate::util::NoYieldsGuard::new();

        let args = execute_args_split(data).map_err(|e| {
            TarantoolError::other(format!(
                "Failed to decode '.proc_sql_execute' arguments, msgpack {}: {e}",
                escape_bytes(data),
            ))
        })?;

        // We expect session id to be a text representation of UUID.
        let mut sid = smallvec::SmallVec::<[u8; 36]>::new();
        sid.extend_from_slice(args.sid.as_bytes());
        let timeout = args.timeout;
        let rid = args.rid;
        let need_ref = args.need_ref;

        Ok(ExecArgs {
            timeout,
            need_ref,
            sid,
            rid,
            data: args.data.to_vec(),
        })
    }
}

unsafe fn proc_sql_execute_impl(args: ExecArgs, port: &mut PicoPortC) -> ::std::os::raw::c_int {
    // Safety: safe as the original args.sid is as valid UTF-8 string.
    let sid = unsafe { std::str::from_utf8_unchecked(args.sid.as_slice()) };

    let mut pcall = || -> Result<(), Error> {
        let package =
            sql_protocol::decode::ProtocolMessage::decode_from_bytes(args.data.as_slice())
                .map_err(Error::other)?;
        let runtime = StorageRuntime::new();
        runtime.execute_plan(package, port, args.timeout)?;
        Ok(())
    };

    // References are useless on replicas, so skip ref operations there.
    let is_replica = node::global()
        .map(|node| node.is_readonly())
        .unwrap_or(false);
    if !is_replica {
        if args.need_ref {
            if let Err(e) = reference_add(args.rid, sid, args.timeout) {
                return report("Failed to add a storage reference: ", e.into());
            };
        }

        if let Err(e) = reference_use(args.rid, sid) {
            return report("Failed to use a storage reference: ", e.into());
        };
    }

    let rc = pcall();
    if !is_replica {
        // We should always unref the storage reference before exit.
        reference_del(args.rid, sid).expect("Failed to remove reference from the storage");
    }

    match rc {
        Ok(()) => 0,
        Err(e) => return report("Failed to execute '.proc_sql_execute': ", e),
    }
}

/// # Safety
/// Executes a query sub-plan on the local node.
#[no_mangle]
pub unsafe extern "C" fn proc_sql_execute(
    mut ctx: FunctionCtx,
    func_args: FunctionArgs,
) -> ::std::os::raw::c_int {
    let args = match func_args.decode::<ExecArgs>() {
        Ok(args) => args,
        Err(e) => return report("", Error::from(e)),
    };

    // All arguments should be copied into the rust memory to
    // survive possible fiber switch.
    with_sql_execution_guard(|| {
        let mut port = PicoPortC::from(ctx.mut_port_c());

        let query_type =
            // TODO: can we reuse it?
            match ProtocolMessage::decode_from_bytes(args.data.as_slice()) {
                Ok(msg) => {
                    let query_type = match msg.msg_type {
                        ProtocolMessageType::Dql => "dql",
                        ProtocolMessageType::Dml(_) | ProtocolMessageType::LocalDml(_) => "dml",
                    };
                    query_type
                }
                Err(e) => return report("", Error::other(e)),
            };

        let rc = proc_sql_execute_impl(args, &mut port);

        let result = if rc == 0 { "ok" } else { "err" };

        STORAGE_1ST_REQUESTS_TOTAL
            .with_label_values(&[query_type, result])
            .inc();

        let is_replica = node::global()
            .map(|node| node.is_readonly())
            .unwrap_or(false);
        if is_replica {
            metrics::record_sql_replicas_read_total();
        }

        rc
    })
}

struct QueryMetaRequest<'q> {
    args: QueryMetaArgs<'q>,
}

impl<'de> Decode<'de> for QueryMetaRequest<'de> {
    fn decode(data: &'de [u8]) -> tarantool::Result<Self> {
        Ok(Self {
            args: query_meta_args_split(data)?,
        })
    }
}

#[tarantool::proc(packed_args)]
pub fn proc_query_metadata(req: QueryMetaRequest) -> Result<Tuple, Error> {
    // SAFETY: The code below never yields, so it's safe to use a slice here
    #[cfg(debug_assertions)]
    let _guard = crate::util::NoYieldsGuard::new();
    let args = req.args;
    let tuple = build_cache_miss_dql_packet(args.request_id, args.plan_id)?;
    Ok(tuple)
}

// Each DML request on global tables has the following plan:
// Root(Update/Delete/Insert) -> Motion with full policy -> ...
// At this point Motion subtree is not materialized.
// This is done on purpose: we need to save raft index and term
// before doing any reads. After materializing the subtree we
// convert virtual table to a batch of DML ops and apply it via
// CAS. No retries are made in case of CAS error.
fn do_dml_on_global_tbl(
    mut query: ExecutingQuery<RouterRuntime>,
    override_deadline: Option<Instant>,
    governor_op_id: Option<u64>,
) -> traft::Result<ConsumerResult> {
    let mut backoff = SimpleBackoffManager::new(
        "global DML retry",
        Duration::from_millis(100),
        Duration::from_secs(30),
    );
    // Without randomization the `test_global_dml_contention_load`
    // integration test runs ~5 times slower, because the parallel
    // workers encounter conflicts at about the same time and thefore
    // choose the same timeouts. We don't actually expect it to be this
    // bad in real-life scenarios, but adding randomization may
    // sometimes help. It should do any harm anyways
    backoff.randomize = true;
    // Golden ratio, because multiplying by 2 makes it grow too fast for this case
    backoff.multiplier_coefficient = 1.6180339887;
    loop {
        let res = do_dml_on_global_tbl_no_retry(&mut query, override_deadline, governor_op_id);
        let res = match res {
            Ok(v) => v,
            Err(e) => {
                if e.is_retriable() {
                    metrics::record_sql_global_dml_query_retries_total();

                    backoff.handle_failure();
                    let timeout = backoff.timeout();

                    tlog!(
                        Warning,
                        "global DML failed: {e}, retrying in {timeout:.02?}..."
                    );
                    tarantool::fiber::sleep(timeout);

                    continue;
                }
                return Err(e);
            }
        };

        metrics::record_sql_global_dml_query_total();

        return Ok(res);
    }
}

fn create_dml_ops(
    query: &mut ExecutingQuery<RouterRuntime>,
    on_conflict: &mut ConflictStrategy,
    current_user: u32,
) -> traft::Result<(Vec<Dml>, usize)> {
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top()?;
    let table = plan.dml_node_table(top)?;
    let table_name = &table.name;
    let childen = plan.children(top);
    let space = Space::find(table_name.as_str())
        .ok_or_else(|| Error::other(format!("failed to find table with name: {table_name}")))?;
    if childen.is_empty() {
        // If children are empty, there's delete without filter and
        // there's no need to generate tuples.
        let plan_id = plan.new_pattern_id(top)?;
        let info = FullDeleteInfo::new(
            plan_id,
            SchemaInfo::new(
                plan.table_version_map.clone(),
                plan.index_version_map.clone(),
            ),
            plan.effective_options.clone(),
            table_name.as_str(),
        );
        let op = Dml::delete(space.id(), &Vec::<u8>::new(), current_user, Some(info))?;
        return Ok((vec![op], space.len()?));
    }

    // Materialize reading subtree and extract some needed data from Plan
    let (table_id, dml_kind, vtable, on_conflict) = {
        let ir = query.get_exec_plan().get_ir_plan();
        let top = ir.get_top()?;

        let node = ir.get_relation_node(top)?;
        let dml_kind: DmlKind = match node {
            Relational::Insert(Insert {
                conflict_strategy, ..
            }) => {
                *on_conflict = *conflict_strategy;
                DmlKind::Insert
            }
            Relational::Update { .. } => DmlKind::Update,
            Relational::Delete { .. } => DmlKind::Delete,
            _ => unreachable!(),
        };

        let motion_id = ir.get_first_rel_child(top)?;
        let slices = ir.calculate_slices(motion_id)?;
        let port: Option<&mut PicoPortC> = None;
        query.materialize_subtree(slices.into(), port)?;

        let exec_plan = query.get_mut_exec_plan();
        let vtable = exec_plan
            .get_mut_vtables()
            .remove(&motion_id)
            .expect("subtree must be materialized");

        (space.id(), dml_kind, vtable, on_conflict)
    };

    // Convert virtual table to a batch of DML opcodes

    let ir = query.get_exec_plan().get_ir_plan();
    let top = ir.get_top()?;
    let builder = match dml_kind {
        DmlKind::Insert => init_insert_tuple_builder(ir, vtable.get_columns(), top)?,
        DmlKind::Update => init_local_update_tuple_builder(ir, vtable.get_columns(), top)?,
        DmlKind::Delete => init_delete_tuple_builder(ir, top)?,
        DmlKind::Replace => unreachable!("SQL does not support replace"),
    };

    let tuples = vtable.get_tuples();
    let mut ops = Vec::with_capacity(tuples.len());
    for tuple in tuples {
        let op = match dml_kind {
            // TODO: intoduce new opcode that inserts/updates/deletes
            // many tuples for one table.
            DmlKind::Insert => {
                let tuple = build_insert_args(tuple, &builder, None)?;
                Dml::insert_with_on_conflict(table_id, &tuple, current_user, *on_conflict)?
            }
            DmlKind::Delete => {
                let tuple = build_delete_args(tuple, &builder)?;
                Dml::delete(table_id, &tuple, current_user, None)?
            }
            DmlKind::Update => {
                let args = build_update_args(tuple, &builder)?;
                let ops = args
                    .ops
                    .into_iter()
                    .map(|op| {
                        op.to_tuple_buffer().map_err(|e| {
                            SbroadError::GlobalDml(format_smolstr!("can't update op: {e}"))
                        })
                    })
                    .collect::<Result<Vec<_>, SbroadError>>()?;
                Dml::update(table_id, &args.key_tuple, ops, current_user)?
            }
            DmlKind::Replace => unreachable!("SQL does not support replace"),
        };
        ops.push(op);
    }

    let len = ops.len();
    Ok((ops, len))
}

fn do_dml_on_global_tbl_no_retry(
    query: &mut ExecutingQuery<RouterRuntime>,
    override_deadline: Option<Instant>,
    governor_op_id: Option<u64>,
) -> traft::Result<ConsumerResult> {
    let current_user = effective_user_id();

    let raft_node = node::global()?;
    let raft_index = raft_node.get_index();

    let mut on_conflict = ConflictStrategy::DoFail;
    let (mut ops, mut row_count) = create_dml_ops(query, &mut on_conflict, current_user)?;
    // CAS will return error on empty batch
    if ops.is_empty() {
        return Ok(ConsumerResult { row_count: 0 });
    }

    // If we have DML in governor operation (running from governor),
    // we need to add final update operation to change status.
    if let Some(governor_op_id) = governor_op_id {
        let mut update_ops = UpdateOps::new();
        update_ops.assign(
            column_name!(governor_queue::GovernorOperationDef, status),
            governor_queue::GovernorOpStatus::Done,
        )?;
        let op = Dml::update(
            governor_queue::GovernorQueue::TABLE_ID,
            &[governor_op_id],
            update_ops,
            ADMIN_ID,
        )?;
        ops.push(op);

        row_count = ops.len();
    }

    // CAS must be done under admin, as we access system spaces
    // there.
    with_su(ADMIN_ID, || -> traft::Result<ConsumerResult> {
        let timeout = Duration::from_secs(DEFAULT_QUERY_TIMEOUT);
        let mut deadline = Instant::now_fiber().saturating_add(timeout);
        if let Some(override_deadline) = override_deadline {
            deadline = override_deadline.min(deadline);
        }

        let op = crate::traft::op::Op::BatchDml { ops };

        let predicate = Predicate::new(raft_index, []);
        let cas_req = crate::cas::Request::new(op, predicate, current_user)?;
        let res = crate::cas::compare_and_swap_and_wait(&cas_req, deadline)?;

        let (_, _, count) = res.no_retries()?;
        if on_conflict == ConflictStrategy::DoNothing {
            Ok(ConsumerResult { row_count: count })
        } else {
            Ok(ConsumerResult {
                row_count: row_count as u64,
            })
        }
    })?
}

// TODO: move this to sbroad
pub(crate) fn value_type_str(value: &Value) -> &'static str {
    match value {
        Value::Boolean { .. } => "boolean",
        Value::Decimal { .. } => "decimal",
        Value::Double { .. } => "double",
        Value::Datetime { .. } => "datetime",
        Value::Integer { .. } => "integer",
        Value::Null => "null",
        Value::String { .. } => "string",
        Value::Tuple { .. } => "tuple",
        Value::Uuid { .. } => "uuid",
    }
}
