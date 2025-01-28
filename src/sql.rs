//! Clusterwide SQL query execution.

use crate::access_control::access_check_plugin_system;
use crate::access_control::{validate_password, UserMetadataKind};
use crate::cas::Predicate;
use crate::schema::{
    wait_for_ddl_commit, CreateIndexParams, CreateProcParams, CreateTableParams, DistributionParam,
    Field, IndexOption, PrivilegeDef, PrivilegeType, RenameRoutineParams, RoutineDef,
    RoutineLanguage, RoutineParamDef, RoutineParams, RoutineSecurity, SchemaObjectType, ShardingFn,
    UserDef, ADMIN_ID,
};
use crate::sql::router::RouterRuntime;
use crate::sql::storage::StorageRuntime;
use crate::storage::space_by_name;
use crate::sync::wait_for_index_globally;
use crate::traft::error::{self, Error, Unsupported};
use crate::traft::node::Node as TraftNode;
use crate::traft::op::{Acl as OpAcl, Ddl as OpDdl, Dml, DmlKind, Op};
use crate::traft::{self, node};
use crate::util::{duration_from_secs_f64_clamped, effective_user_id};
use crate::{cas, plugin, tlog};

use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::engine::helpers::{
    build_delete_args, build_insert_args, build_update_args, decode_msgpack,
    init_delete_tuple_builder, init_insert_tuple_builder, init_local_update_tuple_builder,
    replace_metadata_in_dql_result, try_get_metadata_from_plan,
};
use sbroad::executor::protocol::{EncodedRequiredData, RequiredData};
use sbroad::executor::result::ConsumerResult;
use sbroad::executor::Query;
use sbroad::ir::acl::{AlterOption, GrantRevokeType, Privilege as SqlPrivilege};
use sbroad::ir::ddl::{AlterSystemType, ParamDef};
use sbroad::ir::node::acl::AclOwned;
use sbroad::ir::node::block::Block;
use sbroad::ir::node::ddl::{Ddl, DdlOwned};
use sbroad::ir::node::expression::ExprOwned;
use sbroad::ir::node::relational::Relational;
use sbroad::ir::node::NodeId;
use sbroad::ir::node::{
    AlterSystem, AlterUser, Constant, CreateIndex, CreateProc, CreateRole, CreateTable, CreateUser,
    Delete, DropIndex, DropProc, DropRole, DropTable, DropUser, GrantPrivilege, Insert,
    Node as IrNode, NodeOwned, Procedure, RenameRoutine, RevokePrivilege, ScanRelation, SetParam,
    Update,
};
use tarantool::decimal::Decimal;

use crate::plugin::{InheritOpts, PluginIdentifier, TopologyUpdateOpKind};
use sbroad::ir::node::plugin::{
    AppendServiceToTier, ChangeConfig, CreatePlugin, DisablePlugin, DropPlugin, EnablePlugin,
    MigrateTo, Plugin, RemoveServiceFromTier, SettingsPair,
};
use sbroad::ir::node::Node;
use sbroad::ir::operator::ConflictStrategy;
use sbroad::ir::relation::Type;
use sbroad::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};
use sbroad::ir::value::{LuaValue, Value};
use sbroad::ir::{Options, Plan as IrPlan};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use tarantool::access_control::{box_access_check_ddl, SchemaObjectType as TntSchemaObjectType};
use tarantool::schema::function::func_next_reserved_id;
use tarantool::tuple::ToTupleBuffer;

use crate::storage::Clusterwide;
use ::tarantool::access_control::{box_access_check_space, PrivType};
use ::tarantool::auth::{AuthData, AuthDef, AuthMethod};
use ::tarantool::error::BoxError;
use ::tarantool::error::TarantoolErrorCode;
use ::tarantool::proc;
use ::tarantool::session::{with_su, UserId};
use ::tarantool::space::{FieldType, Space, SpaceId, SystemSpace};
use ::tarantool::time::Instant;
use ::tarantool::tuple::{RawBytes, Tuple};
use std::ops::{ControlFlow, ControlFlow::Break, ControlFlow::Continue};
use std::rc::Rc;
use std::str::FromStr;
use std::time::Duration;
use tarantool::session;

pub mod router;
pub mod storage;

use self::router::DEFAULT_QUERY_TIMEOUT;
use serde::Serialize;

pub const DEFAULT_BUCKET_COUNT: u64 = 3000;

enum Privileges {
    Read,
    Write,
    ReadWrite,
}

fn check_table_privileges(plan: &IrPlan) -> traft::Result<()> {
    let filter = |node_id: NodeId| -> bool {
        if let Ok(IrNode::Relational(
            Relational::ScanRelation { .. }
            | Relational::Delete { .. }
            | Relational::Insert { .. }
            | Relational::Update { .. },
        )) = plan.get_node(node_id)
        {
            return true;
        }
        false
    };
    let mut plan_traversal = PostOrderWithFilter::with_capacity(
        |node| plan.subtree_iter(node, false),
        REL_CAPACITY,
        Box::new(filter),
    );
    let top_id = plan.get_top()?;
    plan_traversal.populate_nodes(top_id);
    let nodes = plan_traversal.take_nodes();

    // We don't want to switch the user back and forth for each node, so we
    // collect all space ids and privileges and then check them all at once.
    let mut space_privs: Vec<(SpaceId, Privileges)> = Vec::with_capacity(nodes.len());

    // Switch to admin to get space ids. At the moment we don't use space cache in tarantool
    // module and can't get space metadata without _space table read permissions.
    with_su(ADMIN_ID, || -> traft::Result<()> {
        for LevelNode(_, node_id) in nodes {
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

fn empty_query_response() -> traft::Result<Tuple> {
    #[derive(Serialize)]
    struct EmptyQueryResponse {
        row_count: usize,
    }

    let mut buf = vec![];
    let resp = vec![EmptyQueryResponse { row_count: 0 }];
    rmp_serde::encode::write_named(&mut buf, &resp).map_err(|e| Error::Other(e.into()))?;
    Tuple::try_from_slice(&buf).map_err(Into::into)
}

pub fn dispatch(mut query: Query<RouterRuntime>) -> traft::Result<Tuple> {
    if query.is_empty() {
        return empty_query_response();
    }

    if query.is_deallocate()? {
        return empty_query_response();
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
        return empty_query_response();
    }

    if query.is_ddl()? || query.is_acl()? {
        let ir_plan = query.get_exec_plan().get_ir_plan();
        let top_id = ir_plan.get_top()?;

        if let Node::Ddl(_) = ir_plan.get_node(top_id)? {
            let ddl_node = ir_plan.get_ddl_node(top_id)?;
            if let Ddl::CreateSchema = ddl_node {
                tlog!(
                    Warning,
                    "DDL for schemas is currently unsupported. Empty query response provided for CREATE SCHEMA."
                );
                return empty_query_response();
            }
            if let Ddl::DropSchema = ddl_node {
                tlog!(
                    Warning,
                    "DDL for schemas is currently unsupported. Empty query response provided for DROP SCHEMA."
                );
                return empty_query_response();
            }
        }

        let ir_plan_mut = query.get_mut_exec_plan().get_mut_ir_plan();

        let ir_node = ir_plan_mut.replace_with_stub(top_id);
        let node = node::global()?;
        let result = reenterable_schema_change_request(node, ir_node)?;
        let tuple = Tuple::new(&(result,))?;
        Ok(tuple)
    } else if query.is_plugin()? {
        let ir_plan = query.get_exec_plan().get_ir_plan();
        let top_id = ir_plan.get_top()?;
        let plugin = ir_plan.get_plugin_node(top_id)?;
        let timeout_from_decimal = |decimal: Decimal| -> traft::Result<_> {
            let secs = decimal
                .to_smolstr()
                .parse::<f64>()
                .map_err(|e| Error::Other(e.into()))?;
            Ok(duration_from_secs_f64_clamped(secs))
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
                PluginIdentifier::new(name.to_string(), version.to_string()),
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
                &PluginIdentifier::new(name.to_string(), version.to_string()),
                // TODO this option should be un-hardcoded and moved into picodata configuration
                Duration::from_secs(10),
                timeout_from_decimal(*timeout)?,
            )?,
            Plugin::Disable(DisablePlugin {
                name,
                version,
                timeout,
            }) => plugin::disable_plugin(
                &PluginIdentifier::new(name.to_string(), version.to_string()),
                timeout_from_decimal(*timeout)?,
            )?,
            Plugin::Drop(DropPlugin {
                name,
                version,
                if_exists,
                with_data,
                timeout,
            }) => plugin::drop_plugin(
                &PluginIdentifier::new(name.to_string(), version.to_string()),
                *with_data,
                *if_exists,
                timeout_from_decimal(*timeout)?,
            )?,
            Plugin::MigrateTo(MigrateTo {
                name,
                version,
                opts,
            }) => plugin::migration_up(
                &PluginIdentifier::new(name.to_string(), version.to_string()),
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
                &PluginIdentifier::new(plugin_name.to_string(), version.to_string()),
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
                &PluginIdentifier::new(plugin_name.to_string(), version.to_string()),
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
                    &PluginIdentifier::new(plugin_name.to_string(), version.to_string()),
                    &config,
                    timeout_from_decimal(*timeout)?,
                )?
            }
        };

        Ok(Tuple::new(&(ConsumerResult { row_count: 1 },))?)
    } else if query.is_block()? {
        check_routine_privileges(query.get_exec_plan().get_ir_plan())?;
        let ir_plan = query.get_mut_exec_plan().get_mut_ir_plan();
        let top_id = ir_plan.get_top()?;
        let code_block = ir_plan.get_block_node(top_id)?;
        match code_block {
            Block::Procedure(Procedure { name, values }) => {
                let values = values.clone();
                let options = ir_plan.raw_options.clone();
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
                    let param_type = Type::try_from(param_def.r#type)?;
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
                let runtime = RouterRuntime::new()?;
                let mut stmt_query = with_su(ADMIN_ID, || Query::new(&runtime, &pattern, params))??;
                // Take options from the original query.
                let stmt_ir_plan = stmt_query.get_mut_exec_plan().get_mut_ir_plan();
                stmt_ir_plan.raw_options = options;
                dispatch(stmt_query)
            }
        }
    } else {
        let plan = query.get_exec_plan().get_ir_plan();
        check_table_privileges(plan)?;

        let metadata = try_get_metadata_from_plan(query.get_exec_plan())?;

        if query.is_explain() {
            return Ok(*query
                .produce_explain()?
                .downcast::<Tuple>()
                .expect("explain must always return a tuple"));
        }

        // check if table is operable
        with_su(ADMIN_ID, || {
            let top_id = plan.get_top()?;
            if plan.get_relation_node(top_id)?.is_dml() {
                let storage = &node::global()?.storage;
                let table = plan.dml_node_table(top_id)?;
                let table_name = table.name.clone();

                let table_id = storage
                    .tables
                    .by_name(&table_name)?
                    .ok_or(traft::error::DoesNotExist::Table(table_name))?
                    .id;

                cas::check_table_operable(storage, table_id)?;
            }

            Ok::<(), Error>(())
        })??;

        if plan.is_dml_on_global_table()? {
            let res = do_dml_on_global_tbl(query)?;
            return Ok(Tuple::new(&(res,))?);
        }

        let tuple = match query.dispatch() {
            Ok(mut any_tuple) => {
                if let Some(tuple) = any_tuple.downcast_mut::<Tuple>() {
                    tlog!(Trace, "dispatch: Dispatch result: {tuple:?}");
                    let tuple: Tuple = std::mem::replace(tuple, Tuple::new(&())?);
                    Ok(tuple)
                } else {
                    Err(Error::from(SbroadError::FailedTo(
                        Action::Decode,
                        None,
                        format_smolstr!("tuple {any_tuple:?}"),
                    )))
                }
            }
            Err(e) => Err(Error::from(e)),
        }?;

        // replace tarantool's metadata with the metadata from the plan
        if let Some(metadata) = metadata {
            return Ok(replace_metadata_in_dql_result(&tuple, &metadata)?);
        }
        Ok(tuple)
    }
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

/// Dispatches an SQL query to the cluster.
/// Part of public RPC API.
#[proc]
pub fn proc_sql_dispatch(pattern: String, params: Vec<LuaValue>) -> traft::Result<Tuple> {
    sql_dispatch(&pattern, params).map_err(err_for_tnt_console)
}

pub fn sql_dispatch(pattern: &str, params: Vec<LuaValue>) -> traft::Result<Tuple> {
    let runtime = RouterRuntime::new()?;
    let node = node::global()?;
    // Admin privileges are need for reading tables metadata.
    let query = with_su(ADMIN_ID, || {
        let sql_vdbe_opcode_max = node.storage.db_config.sql_vdbe_opcode_max()?;
        let sql_motion_row_max = node.storage.db_config.sql_motion_row_max()?;
        let default_options = Some(Options::new(sql_motion_row_max, sql_vdbe_opcode_max));
        Query::with_options(
            &runtime,
            pattern,
            params.into_iter().map(Into::into).collect(),
            default_options,
        )
    })??;
    dispatch(query)
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
fn get_grantee_id(storage: &Clusterwide, grantee_name: &str) -> traft::Result<UserId> {
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
        format!(
            "routine exists but with a different signature: {}",
            actual_signature
        )
        .into(),
    ))
}

fn check_name_emptyness(name: &str) -> traft::Result<()> {
    if name.is_empty() {
        return Err(Error::Other("expected non empty name".into()));
    }

    Ok(())
}

fn parse_auth_method(auth_method: &str) -> traft::Result<AuthMethod> {
    AuthMethod::from_str(&auth_method.to_lowercase())
        .map_err(|_| Error::Other(format!("Unknown auth method: {auth_method}").into()))
}

fn alter_user_ir_node_to_op_or_result(
    name: &SmolStr,
    alter_option: &AlterOption,
    current_user: UserId,
    schema_version: u64,
    storage: &Clusterwide,
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
            let method = parse_auth_method(auth_method)?;
            validate_password(password, &method, storage)?;
            let data = AuthData::new(&method, name, password);
            let auth = AuthDef::new(method, data.into_string());

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
                    name: new_name.to_string(),
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
    storage: &Clusterwide,
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
                name: name.to_string(),
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

            let method = parse_auth_method(auth_method)?;
            validate_password(password, &method, storage)?;
            let data = AuthData::new(&method, name, password);
            let auth = AuthDef::new(method, data.into_string());

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
                name: name.to_string(),
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
    }
}

fn alter_system_ir_node_to_op_or_result(
    ty: &AlterSystemType,
    tier_name: Option<&str>,
    current_user: UserId,
) -> traft::Result<ControlFlow<ConsumerResult, Op>> {
    if tier_name.is_some() {
        // TODO: Should be resolved as a part of
        //       https://git.picodata.io/picodata/picodata/picodata/-/issues/867.
        return Err(Error::Unsupported(Unsupported::new(
            String::from("specifying tier name in alter system"),
            Some(String::from("use 'all tiers' instead")),
        )));
    }

    let table = crate::storage::ClusterwideTable::DbConfig;
    let initiator = current_user;
    match ty {
        AlterSystemType::AlterSystemSet {
            param_name,
            param_value,
        } => {
            let casted_value =
                crate::config::validate_alter_system_parameter_value(param_name, param_value)?;

            let dml = Dml::replace(table, &(param_name, casted_value), initiator)?;

            Ok(Continue(Op::Dml(dml)))
        }
        AlterSystemType::AlterSystemReset { param_name } => {
            match param_name {
                Some(param_name) => {
                    // reset one
                    let Some(default_value) =
                        crate::config::get_default_value_of_alter_system_parameter(param_name)
                    else {
                        return Err(Error::other(format!("unknown parameter: '{param_name}'")));
                    };
                    let dml = Dml::replace(table, &(param_name, default_value), initiator)?;

                    Ok(Continue(Op::Dml(dml)))
                }
                None => {
                    // reset all
                    let defaults = crate::config::get_defaults_for_all_alter_system_parameters();
                    let mut dmls = Vec::with_capacity(defaults.len());
                    for (param_name, default_value) in defaults {
                        let dml = Dml::replace(table, &(param_name, default_value), initiator)?;
                        dmls.push(dml);
                    }
                    Ok(Continue(Op::BatchDml { ops: dmls }))
                }
            }
        }
    }
}

fn ddl_ir_node_to_op_or_result(
    ddl: &DdlOwned,
    current_user: UserId,
    schema_version: u64,
    node: &TraftNode,
    storage: &Clusterwide,
) -> traft::Result<ControlFlow<ConsumerResult, Op>> {
    match ddl {
        DdlOwned::AlterSystem(AlterSystem { ty, tier_name, .. }) => {
            alter_system_ir_node_to_op_or_result(
                ty,
                tier_name.as_ref().map(|s| s.as_str()),
                current_user,
            )
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
                    name: f.name.to_string(),
                    r#type: FieldType::from(&f.data_type),
                    is_nullable: f.is_nullable,
                })
                .collect();
            let distribution = if sharding_key.is_some() {
                DistributionParam::Sharded
            } else {
                DistributionParam::Global
            };

            let primary_key = primary_key.iter().cloned().map(String::from).collect();
            let sharding_key = sharding_key
                .as_ref()
                .map(|sh_key| sh_key.iter().cloned().map(String::from).collect());

            let mut params = CreateTableParams {
                id: None,
                name: name.to_string(),
                format,
                primary_key,
                distribution,
                by_field: None,
                sharding_key,
                sharding_fn: Some(ShardingFn::Murmur3),
                engine: Some(*engine_type),
                timeout: None,
                owner: current_user,
                tier: tier.as_ref().map(SmolStr::to_string),
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

            params.choose_id_if_not_specified()?;
            params.test_create_space(storage)?;
            let ddl = params.into_ddl()?;
            Ok(Continue(Op::DdlPrepare {
                schema_version,
                ddl,
            }))
        }
        DdlOwned::DropTable(DropTable {
            name, if_exists, ..
        }) => {
            let Some(space_def) = storage.tables.by_name(name)? else {
                if *if_exists {
                    return Ok(Break(ConsumerResult { row_count: 0 }));
                } else {
                    return Err(error::DoesNotExist::Table(name.clone()).into());
                }
            };
            let ddl = OpDdl::DropTable {
                id: space_def.id,
                initiator: current_user,
            };
            Ok(Continue(Op::DdlPrepare {
                schema_version,
                ddl,
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
                name: name.to_string(),
                params,
                language,
                body: body.to_string(),
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
            }))
        }
        DdlOwned::RenameRoutine(RenameRoutine {
            old_name,
            new_name,
            params,
            ..
        }) => {
            let params = RenameRoutineParams {
                new_name: new_name.to_string(),
                old_name: old_name.to_string(),
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
                ddl,
                schema_version,
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

            let columns = columns.iter().cloned().map(String::from).collect();

            let params = CreateIndexParams {
                name: name.to_string(),
                space_name: table_name.to_string(),
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
            }))
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

pub(crate) fn reenterable_schema_change_request(
    node: &TraftNode,
    ir_node: NodeOwned,
) -> traft::Result<ConsumerResult> {
    let storage = &node.storage;
    // Save current user as later user is switched to admin
    let current_user = effective_user_id();

    let timeout = match &ir_node {
        NodeOwned::Ddl(ddl) => ddl.timeout()?,
        NodeOwned::Acl(acl) => acl.timeout()?,
        n => {
            unreachable!("this function should only be called for ddl or acl nodes, not {n:?}")
        }
    };
    let timeout = duration_from_secs_f64_clamped(timeout);
    let deadline = Instant::now_fiber().saturating_add(timeout);

    let _su = session::su(ADMIN_ID).expect("cant fail because admin should always have session");

    'retry: loop {
        if Instant::now_fiber() > deadline {
            return Err(Error::Timeout);
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
            node.wait_index(index + 1, deadline.duration_since(Instant::now_fiber()))?;
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
                ddl_ir_node_to_op_or_result(ddl, current_user, schema_version, node, storage)?
            }
            n => unreachable!("function must be called only for ddl or acl nodes, not {n:?}"),
        };

        let op = match op_or_result {
            Break(consumer_result) => return Ok(consumer_result),
            Continue(op) => op,
        };
        let is_ddl_prepare = matches!(op, Op::DdlPrepare { .. });

        // TODO: Should look at https://git.picodata.io/picodata/picodata/picodata/-/issues/866.
        let predicate = cas::Predicate::new(index, cas::schema_change_ranges());
        let req = crate::cas::Request::new(op, predicate, current_user)?;
        let res = cas::compare_and_swap_and_wait(&req, deadline)?;
        let index = match res {
            cas::CasResult::Ok((index, _)) => index,
            cas::CasResult::RetriableError(_) => continue,
        };

        if is_ddl_prepare {
            let commit_index =
                wait_for_ddl_commit(index, deadline.duration_since(Instant::now_fiber()))?;
            if wait_applied_globally {
                wait_for_index_globally(
                    &node.storage,
                    Rc::clone(&node.pool),
                    commit_index,
                    deadline,
                ).map_err(|_| Error::Other("ddl operation committed, but failed to receive acknowledgements from all replicasets".into()))?;
            }
        }

        return Ok(ConsumerResult { row_count: 1 });
    }
}

/// Executes a query sub-plan on the local node.
#[proc(packed_args)]
pub fn proc_sql_execute(raw: &RawBytes) -> traft::Result<Tuple> {
    let (raw_required, optional_bytes, cache_info) = decode_msgpack(raw)?;
    let mut required = RequiredData::try_from(EncodedRequiredData::from(raw_required))?;
    let runtime = StorageRuntime::new()?;
    match runtime.execute_plan(&mut required, optional_bytes, cache_info) {
        Ok(mut any_tuple) => {
            if let Some(tuple) = any_tuple.downcast_mut::<Tuple>() {
                tlog!(Trace, "proc_sql_execute: Execution result: {tuple:?}");
                let tuple: Tuple = std::mem::replace(tuple, Tuple::new(&())?);
                Ok(tuple)
            } else {
                Err(Error::from(SbroadError::FailedTo(
                    Action::Decode,
                    None,
                    format_smolstr!("tuple {any_tuple:?}"),
                )))
            }
        }
        Err(e) => Err(Error::from(e)),
    }
}

// Each DML request on global tables has the following plan:
// Root(Update/Delete/Insert) -> Motion with full policy -> ...
// At this point Motion subtree is not materialized.
// This is done on purpose: we need to save raft index and term
// before doing any reads. After materializing the subtree we
// convert virtual table to a batch of DML ops and apply it via
// CAS. No retries are made in case of CAS error.
fn do_dml_on_global_tbl(mut query: Query<RouterRuntime>) -> traft::Result<ConsumerResult> {
    let current_user = effective_user_id();

    let raft_node = node::global()?;
    let raft_index = raft_node.get_index();

    // Materialize reading subtree and extract some needed data from Plan
    let (table_id, dml_kind, vtable) = {
        let ir = query.get_exec_plan().get_ir_plan();
        let top = ir.get_top()?;
        let table = ir.dml_node_table(top)?;
        let table_name = &table.name;
        let table_id = Space::find(table_name.as_str())
            .ok_or(Error::other(format!(
                "failed to find table with name: {table_name}"
            )))?
            .id();

        let node = ir.get_relation_node(top)?;
        if matches!(
            node,
            Relational::Insert(Insert {
                conflict_strategy: ConflictStrategy::DoReplace | ConflictStrategy::DoNothing,
                ..
            })
        ) {
            Err(Error::other("insert on conflict is not supported yet"))?;
        }
        let dml_kind: DmlKind = match node {
            Relational::Insert { .. } => DmlKind::Insert,
            Relational::Update { .. } => DmlKind::Update,
            Relational::Delete { .. } => DmlKind::Delete,
            _ => unreachable!(),
        };

        let motion_id = ir.get_relational_child(top, 0)?;
        let slices = ir.calculate_slices(motion_id)?;
        query.materialize_subtree(slices.into())?;

        let exec_plan = query.get_mut_exec_plan();
        let vtables_map = exec_plan
            .vtables
            .as_mut()
            .expect("subtree must be materialized");
        let vtable = vtables_map
            .mut_map()
            .remove(&motion_id)
            .expect("subtree must be materialized");

        (table_id, dml_kind, vtable)
    };

    // CAS will return error on empty batch
    if vtable.get_tuples().is_empty() {
        return Ok(ConsumerResult { row_count: 0 });
    }

    // Convert virtual table to a batch of DML opcodes

    let ir = query.get_exec_plan().get_ir_plan();
    let top = ir.get_top()?;
    let builder = match dml_kind {
        DmlKind::Insert => init_insert_tuple_builder(ir, &vtable, top)?,
        DmlKind::Update => init_local_update_tuple_builder(ir, &vtable, top)?,
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
                Dml::insert(table_id, &tuple, current_user)?
            }
            DmlKind::Delete => {
                let tuple = build_delete_args(tuple, &builder)?;
                Dml::delete(table_id, &tuple, current_user)?
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

    // CAS must be done under admin, as we access system spaces
    // there.
    with_su(ADMIN_ID, || -> traft::Result<ConsumerResult> {
        let timeout = Duration::from_secs(DEFAULT_QUERY_TIMEOUT);
        let deadline = Instant::now_fiber().saturating_add(timeout);

        let ops_count = ops.len();
        let op = crate::traft::op::Op::BatchDml { ops };

        let predicate = Predicate::new(raft_index, []);
        let cas_req = crate::cas::Request::new(op, predicate, current_user)?;
        let res = crate::cas::compare_and_swap_and_wait(&cas_req, deadline)?;
        res.no_retries()?;

        Ok(ConsumerResult {
            row_count: ops_count as u64,
        })
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
        Value::Null { .. } => "null",
        Value::String { .. } => "string",
        Value::Unsigned { .. } => "unsigned",
        Value::Tuple { .. } => "tuple",
        Value::Uuid { .. } => "uuid",
    }
}
