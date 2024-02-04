//! Clusterwide SQL query execution.

use crate::schema::{
    wait_for_ddl_commit, CreateProcParams, CreateTableParams, DistributionParam, Field,
    PrivilegeDef, PrivilegeType, RoleDef, RoutineDef, RoutineLanguage, RoutineParamDef,
    RoutineParams, RoutineSecurity, SchemaObjectType, ShardingFn, UserDef, ADMIN_ID,
};
use crate::sql::pgproto::{
    with_portals, BoxedPortal, Describe, Descriptor, UserDescriptors, PG_PORTALS,
};
use crate::sql::router::RouterRuntime;
use crate::sql::storage::StorageRuntime;
use crate::storage::space_by_name;
use crate::traft::error::Error;
use crate::traft::node::Node as TraftNode;
use crate::traft::op::{Acl as OpAcl, Ddl as OpDdl, Op};
use crate::traft::{self, node};
use crate::util::{duration_from_secs_f64_clamped, effective_user_id};
use crate::{cas, unwrap_ok_or};

use opentelemetry::sdk::trace::Tracer;
use opentelemetry::{baggage::BaggageExt, Context, KeyValue};
use sbroad::backend::sql::ir::{EncodedPatternWithParams, PatternWithParams};
use sbroad::debug;
use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::engine::helpers::{decode_msgpack, normalize_name_for_space_api};
use sbroad::executor::engine::{QueryCache, Router, TableVersionMap};
use sbroad::executor::ir::ExecutionPlan;
use sbroad::executor::lru::Cache;
use sbroad::executor::protocol::{EncodedRequiredData, RequiredData};
use sbroad::executor::result::ConsumerResult;
use sbroad::executor::Query;
use sbroad::frontend::Ast;
use sbroad::ir::acl::{Acl, AlterOption, GrantRevokeType, Privilege as SqlPrivilege};
use sbroad::ir::ddl::{Ddl, ParamDef};
use sbroad::ir::operator::Relational;
use sbroad::ir::tree::traversal::{PostOrderWithFilter, REL_CAPACITY};
use sbroad::ir::value::{LuaValue, Value};
use sbroad::ir::{Node as IrNode, Plan as IrPlan};
use sbroad::otm::{query_id, query_span, OTM_CHAR_LIMIT};
use serde::Deserialize;
use tarantool::schema::function::func_next_reserved_id;

use crate::storage::Clusterwide;
use ::tarantool::access_control::{box_access_check_space, PrivType};
use ::tarantool::auth::{AuthData, AuthDef, AuthMethod};
use ::tarantool::proc;
use ::tarantool::session::{with_su, UserId};
use ::tarantool::space::{FieldType, Space, SpaceId, SystemSpace};
use ::tarantool::time::Instant;
use ::tarantool::tuple::{RawBytes, Tuple};
use std::collections::HashMap;
use std::str::FromStr;
use tarantool::session;

pub mod otm;
pub mod pgproto;
pub mod router;
pub mod storage;
use otm::TracerKind;

pub const DEFAULT_BUCKET_COUNT: u64 = 3000;
const SPECTIAL_CHARACTERS: [char; 6] = ['&', '|', '?', '!', '$', '@'];

enum Privileges {
    Read,
    Write,
    ReadWrite,
}

fn check_table_privileges(plan: &IrPlan) -> traft::Result<()> {
    let filter = |node_id: usize| -> bool {
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
    let top_id = plan.get_top().map_err(Error::from)?;
    plan_traversal.populate_nodes(top_id);
    let nodes = plan_traversal.take_nodes();

    // We don't want to switch the user back and forth for each node, so we
    // collect all space ids and privileges and then check them all at once.
    let mut space_privs: Vec<(SpaceId, Privileges)> = Vec::with_capacity(nodes.len());

    // Switch to admin to get space ids. At the moment we don't use space cache in tarantool
    // module and can't get space metadata without _space table read permissions.
    with_su(ADMIN_ID, || -> traft::Result<()> {
        for (_, node_id) in nodes {
            let rel_node = plan.get_relation_node(node_id).map_err(Error::from)?;
            let (relation, privileges) = match rel_node {
                Relational::ScanRelation { relation, .. } => (relation, Privileges::Read),
                Relational::Insert { relation, .. } => (relation, Privileges::Write),
                Relational::Delete { relation, .. } | Relational::Update { relation, .. } => {
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
            let space_name = normalize_name_for_space_api(relation);
            let space = space_by_name(&space_name).map_err(Error::from)?;
            space_privs.push((space.id(), privileges));
        }
        Ok(())
    })??;
    for (space_id, priviledges) in space_privs {
        match priviledges {
            Privileges::Read => {
                box_access_check_space(space_id, PrivType::Read).map_err(Error::from)?;
            }
            Privileges::Write => {
                box_access_check_space(space_id, PrivType::Write).map_err(Error::from)?;
            }
            Privileges::ReadWrite => {
                box_access_check_space(space_id, PrivType::Read).map_err(Error::from)?;
                box_access_check_space(space_id, PrivType::Write).map_err(Error::from)?;
            }
        }
    }
    Ok(())
}

fn dispatch(mut query: Query<RouterRuntime>) -> traft::Result<Tuple> {
    if query.is_ddl().map_err(Error::from)? || query.is_acl().map_err(Error::from)? {
        let ir_plan = query.get_exec_plan().get_ir_plan();
        let top_id = ir_plan.get_top().map_err(Error::from)?;
        let ir_plan_mut = query.get_mut_exec_plan().get_mut_ir_plan();

        // XXX: add Node::take_node method to simplify the following 2 lines
        let ir_node = ir_plan_mut.get_mut_node(top_id).map_err(Error::from)?;
        let ir_node = std::mem::replace(ir_node, IrNode::Parameter);
        let node = node::global()?;
        let result = reenterable_schema_change_request(node, ir_node)?;
        Tuple::new(&(result,)).map_err(Error::from)
    } else {
        let plan = query.get_exec_plan().get_ir_plan();
        check_table_privileges(plan)?;
        match query.dispatch() {
            Ok(mut any_tuple) => {
                if let Some(tuple) = any_tuple.downcast_mut::<Tuple>() {
                    debug!(
                        Option::from("dispatch"),
                        &format!("Dispatch result: {tuple:?}"),
                    );
                    let tuple: Tuple = std::mem::replace(tuple, Tuple::new(&())?);
                    Ok(tuple)
                } else {
                    Err(Error::from(SbroadError::FailedTo(
                        Action::Decode,
                        None,
                        format!("tuple {any_tuple:?}"),
                    )))
                }
            }
            Err(e) => Err(Error::from(e)),
        }
    }
}

#[inline]
pub fn with_tracer(ctx: Context, tracer_kind: TracerKind) -> Context {
    ctx.with_baggage(vec![KeyValue::new(TRACER_KEY, tracer_kind.to_string())])
}

/// Dispatches a query to the cluster.
#[proc(packed_args)]
pub fn dispatch_query(encoded_params: EncodedPatternWithParams) -> traft::Result<Tuple> {
    let mut params = PatternWithParams::try_from(encoded_params).map_err(Error::from)?;
    let id = params.clone_id();
    let mut ctx = params.extract_context();
    let mut tracer_kind = TracerKind::default();

    if let Some(tracer_kind_str) = params.tracer.as_ref() {
        tracer_kind =
            TracerKind::from_str(tracer_kind_str).map_err(|e| Error::Other(Box::new(e)))?;
        ctx = with_tracer(ctx, tracer_kind);
    }

    let dispatch = || {
        let runtime = RouterRuntime::new().map_err(Error::from)?;
        let build_query =
            || Query::new(&runtime, &params.pattern, params.params).map_err(Error::from);

        let query = with_su(ADMIN_ID, || -> traft::Result<Query<RouterRuntime>> {
            build_query()
        })??;
        dispatch(query)
    };

    query_span::<Result<Tuple, Error>, _>(
        "\"api.router.dispatch\"",
        &id,
        tracer_kind.get_tracer(),
        &ctx,
        &params.pattern,
        dispatch,
    )
}

struct BindArgs {
    descriptor: Descriptor,
    params: Vec<Value>,
    traceable: bool,
}

impl BindArgs {
    fn take(self) -> (Descriptor, Vec<Value>, bool) {
        (self.descriptor, self.params, self.traceable)
    }
}

impl<'de> Deserialize<'de> for BindArgs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct EncodedBindArgs(Descriptor, Option<Vec<LuaValue>>, Option<bool>);

        let EncodedBindArgs(descriptor, params, traceable) =
            EncodedBindArgs::deserialize(deserializer)?;

        let params = params
            .unwrap_or_default()
            .into_iter()
            .map(Value::from)
            .collect::<Vec<Value>>();

        Ok(Self {
            descriptor,
            params,
            traceable: traceable.unwrap_or(false),
        })
    }
}

// helper function to get `TracerRef`
fn get_tracer_param(traceable: bool) -> &'static Tracer {
    let kind = TracerKind::from_traceable(traceable);
    kind.get_tracer()
}

#[proc(packed_args)]
pub fn proc_pg_bind(args: BindArgs) -> traft::Result<()> {
    let (key, params, traceable) = args.take();
    with_portals(key, |portal| {
        let mut plan = std::mem::take(portal.plan_mut());
        let ctx = with_tracer(Context::new(), TracerKind::from_traceable(traceable));
        query_span::<traft::Result<()>, _>(
            "\"api.router.bind\"",
            portal.id(),
            get_tracer_param(traceable),
            &ctx,
            portal.sql(),
            || {
                if !plan.is_ddl()? && !plan.is_acl()? {
                    plan.bind_params(params)?;
                    plan.apply_options()?;
                    plan.optimize()?;
                }
                Ok(())
            },
        )?;
        *portal.plan_mut() = plan;
        Ok(())
    })
}

#[proc]
pub fn proc_pg_portals() -> UserDescriptors {
    UserDescriptors::new()
}

#[proc]
pub fn proc_pg_close(key: Descriptor) -> traft::Result<()> {
    let portal: BoxedPortal = PG_PORTALS.with(|storage| storage.borrow_mut().remove(key))?;
    drop(portal);
    Ok(())
}

#[proc]
pub fn proc_pg_describe(key: Descriptor, traceable: bool) -> traft::Result<Describe> {
    with_portals(key, |portal| {
        let ctx = with_tracer(Context::new(), TracerKind::from_traceable(traceable));
        let description = query_span::<traft::Result<Describe>, _>(
            "\"api.router.describe\"",
            portal.id(),
            get_tracer_param(traceable),
            &ctx,
            portal.sql(),
            || Describe::new(portal.plan()).map_err(Error::from),
        )?;
        Ok(description)
    })
}

#[proc]
pub fn proc_pg_execute(key: Descriptor, traceable: bool) -> traft::Result<Tuple> {
    with_portals(key, |portal| {
        let ctx = with_tracer(Context::new(), TracerKind::from_traceable(traceable));
        let res = query_span::<traft::Result<Tuple>, _>(
            "\"api.router.execute\"",
            portal.id(),
            get_tracer_param(traceable),
            &ctx,
            portal.sql(),
            || {
                let runtime = RouterRuntime::new().map_err(Error::from)?;
                let query = Query::from_parts(
                    portal.plan().is_explain(),
                    // XXX: the router runtime cache contains only unbinded IR plans to
                    // speed up SQL parsing and metadata resolution. We need to clone the
                    // plan here as its IR would be mutate during query execution (bind,
                    // optimization, dispatch steps). Otherwise we'll polute the parsing
                    // cache entry.
                    ExecutionPlan::from(portal.plan().clone()),
                    &runtime,
                    HashMap::new(),
                );
                dispatch(query)
            },
        )?;
        Ok(res)
    })
}

#[proc]
pub fn proc_pg_parse(query: String, traceable: bool) -> traft::Result<Descriptor> {
    let id = query_id(&query);
    // Keep the query patterns for opentelemetry spans short enough.
    let sql = query
        .char_indices()
        .filter_map(|(i, c)| if i <= OTM_CHAR_LIMIT { Some(c) } else { None })
        .collect::<String>();
    let ctx = with_tracer(Context::new(), TracerKind::from_traceable(traceable));
    query_span::<traft::Result<Descriptor>, _>(
        "\"api.router.parse\"",
        &id.clone(),
        get_tracer_param(traceable),
        &ctx,
        &sql.clone(),
        || {
            let runtime = RouterRuntime::new().map_err(Error::from)?;
            let mut cache = runtime
                .cache()
                .try_borrow_mut()
                .map_err(|e| Error::Other(format!("runtime query cache: {e:?}").into()))?;
            if let Some(plan) = cache.get(&query)? {
                let portal = BoxedPortal::new(id, sql.clone(), plan.clone());
                let descriptor = portal.descriptor();
                PG_PORTALS.with(|cache| cache.borrow_mut().put(descriptor, portal))?;
                return Ok(descriptor);
            }
            let ast = <RouterRuntime as Router>::ParseTree::new(&query).map_err(Error::from)?;
            let metadata = &*runtime.metadata().map_err(Error::from)?;
            let plan = with_su(ADMIN_ID, || -> traft::Result<IrPlan> {
                let mut plan = ast.resolve_metadata(metadata).map_err(Error::from)?;
                if runtime.provides_versions() {
                    let mut table_version_map =
                        TableVersionMap::with_capacity(plan.relations.tables.len());
                    for table in plan.relations.tables.keys() {
                        let normalized = normalize_name_for_space_api(table);
                        let version = runtime
                            .get_table_version(normalized.as_str())
                            .map_err(Error::from)?;
                        table_version_map.insert(normalized, version);
                    }
                    plan.version_map = table_version_map;
                }
                Ok(plan)
            })??;
            if !plan.is_ddl()? && !plan.is_acl()? {
                cache.put(query, plan.clone())?;
            }
            let portal = BoxedPortal::new(id, sql, plan);
            let descriptor = portal.descriptor();
            PG_PORTALS.with(|storage| storage.borrow_mut().put(descriptor, portal))?;
            Ok(descriptor)
        },
    )
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
        let max_role_id = storage.roles.max_role_id()?;
        let mut new_id: UserId = 0;
        if let Some(max_user_id) = max_user_id {
            new_id = max_user_id + 1
        }
        if let Some(max_role_id) = max_role_id {
            if new_id <= max_role_id {
                new_id = max_role_id + 1
            }
        }
        if new_id != 0 {
            return Ok(new_id);
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
                if let Some(user_id) = self.get_user_or_role_id(user_name) {
                    Ok((
                        SchemaObjectType::User,
                        privilege.try_into()?,
                        user_id as i64,
                    ))
                } else {
                    Err(Error::Sbroad(SbroadError::Invalid(
                        Entity::Acl,
                        Some(format!("There is no user with name {user_name}")),
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
                if let Some(role_id) = self.get_user_or_role_id(role_name) {
                    Ok((
                        SchemaObjectType::Role,
                        privilege.try_into()?,
                        role_id as i64,
                    ))
                } else {
                    Err(Error::Sbroad(SbroadError::Invalid(
                        Entity::Acl,
                        Some(format!("There is no role with name {role_name}")),
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
                if let Some(table_id) = self.get_table_id(table_name) {
                    Ok((
                        SchemaObjectType::Table,
                        privilege.try_into()?,
                        table_id as i64,
                    ))
                } else {
                    Err(Error::Sbroad(SbroadError::Invalid(
                        Entity::Acl,
                        Some(format!("There is no table with name {table_name}")),
                    )))
                }
            }
            GrantRevokeType::RolePass { role_name } => {
                if let Some(role_id) = self.get_user_or_role_id(role_name) {
                    Ok((
                        SchemaObjectType::Role,
                        PrivilegeType::Execute,
                        role_id as i64,
                    ))
                } else {
                    Err(Error::Sbroad(SbroadError::Invalid(
                        Entity::Acl,
                        Some(format!("There is no role with name {role_name}")),
                    )))
                }
            }
        }
    }
}

fn validate_password(
    password: &str,
    auth_method: &AuthMethod,
    node: &TraftNode,
) -> traft::Result<()> {
    if let AuthMethod::Ldap = auth_method {
        // LDAP doesn't need password for authentication
        return Ok(());
    }

    let storage = &node.storage;

    // This check is called from user facing API.
    // A user is not expected to have access to _pico_property
    let password_min_length =
        session::with_su(ADMIN_ID, || storage.properties.password_min_length())??;
    if password.len() < password_min_length {
        return Err(Error::Other(
            format!(
                "password is too short: expected at least {}, got {}",
                password_min_length,
                password.len()
            )
            .into(),
        ));
    }

    let password_enforce_uppercase =
        session::with_su(ADMIN_ID, || storage.properties.password_enforce_uppercase())??;
    if password_enforce_uppercase && !password.chars().any(|ch| ch.is_uppercase()) {
        return Err(Error::Other(
            "invalid password: password should contains at least one uppercase letter".into(),
        ));
    }

    let password_enforce_lowercase =
        session::with_su(ADMIN_ID, || storage.properties.password_enforce_lowercase())??;
    if password_enforce_lowercase && !password.chars().any(|ch| ch.is_lowercase()) {
        return Err(Error::Other(
            "invalid password: password should contains at least one lowercase letter".into(),
        ));
    }

    let password_enforce_digits =
        session::with_su(ADMIN_ID, || storage.properties.password_enforce_digits())??;
    if password_enforce_digits && !password.chars().any(|ch| ch.is_ascii_digit()) {
        return Err(Error::Other(
            "invalid password: password should contains at least one digit".into(),
        ));
    }

    let password_enforce_specialchars = session::with_su(ADMIN_ID, || {
        storage.properties.password_enforce_specialchars()
    })??;
    if password_enforce_specialchars
        && !password.chars().any(|ch| SPECTIAL_CHARACTERS.contains(&ch))
    {
        return Err(Error::Other(
            format!(
                "invalid password: password should contains at least one special character - {:?}",
                SPECTIAL_CHARACTERS
            )
            .into(),
        ));
    }

    Ok(())
}

/// Get grantee (user or role) UserId by its name.
fn get_grantee_id(storage: &Clusterwide, grantee_name: &String) -> traft::Result<UserId> {
    if let Some(grantee_user_def) = storage.users.by_name(grantee_name)? {
        Ok(grantee_user_def.id)
    } else if let Some(grantee_role_def) = storage.roles.by_name(grantee_name)? {
        Ok(grantee_role_def.id)
    } else {
        // No existing user or role found.
        Err(Error::Sbroad(SbroadError::Invalid(
            Entity::Acl,
            Some(format!(
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

fn reenterable_schema_change_request(
    node: &TraftNode,
    ir_node: IrNode,
) -> traft::Result<ConsumerResult> {
    let storage = &node.storage;
    // Save current user as later user is switched to admin
    let current_user = effective_user_id();

    let timeout = match &ir_node {
        IrNode::Ddl(ddl) => ddl.timeout()?,
        IrNode::Acl(acl) => acl.timeout()?,
        n => {
            unreachable!("this function should only be called for ddl or acl nodes, not {n:?}")
        }
    };
    let timeout = duration_from_secs_f64_clamped(timeout);
    let deadline = Instant::now().saturating_add(timeout);

    // Check parameters
    let params = match ir_node {
        IrNode::Ddl(Ddl::CreateProc {
            name,
            params: args,
            body,
            language,
            ..
        }) => {
            let args: RoutineParams = args
                .into_iter()
                .map(|p| {
                    let field_type = FieldType::from(&p.data_type);
                    RoutineParamDef::default().with_type(field_type)
                })
                .collect();
            let language = RoutineLanguage::from(language);
            let security = RoutineSecurity::default();

            let params = CreateProcParams {
                name,
                params: args,
                language,
                body,
                security,
                owner: current_user,
            };
            params.validate(storage)?;
            Params::CreateProcedure(params)
        }

        IrNode::Ddl(Ddl::CreateTable {
            name,
            format,
            primary_key,
            sharding_key,
            engine_type,
            ..
        }) => {
            let format = format
                .into_iter()
                .map(|f| Field {
                    name: f.name,
                    r#type: FieldType::from(&f.data_type),
                    is_nullable: f.is_nullable,
                })
                .collect();
            let distribution = if sharding_key.is_some() {
                DistributionParam::Sharded
            } else {
                DistributionParam::Global
            };
            let params = CreateTableParams {
                id: None,
                name,
                format,
                primary_key,
                distribution,
                by_field: None,
                sharding_key,
                sharding_fn: Some(ShardingFn::Murmur3),
                engine: Some(engine_type),
                timeout: None,
                owner: current_user,
            };
            params.validate()?;
            Params::CreateTable(params)
        }
        IrNode::Ddl(Ddl::DropProc { name, params, .. }) => {
            // Nothing to check
            Params::DropProcedure(name, params)
        }
        IrNode::Ddl(Ddl::DropTable { name, .. }) => {
            // Nothing to check
            Params::DropTable(name)
        }
        IrNode::Acl(Acl::DropUser { name, .. }) => {
            // Nothing to check
            Params::DropUser(name)
        }
        IrNode::Acl(Acl::CreateRole { name, .. }) => {
            // Nothing to check
            Params::CreateRole(name)
        }
        IrNode::Acl(Acl::DropRole { name, .. }) => {
            // Nothing to check
            Params::DropRole(name)
        }
        IrNode::Acl(Acl::CreateUser {
            name,
            password,
            auth_method,
            ..
        }) => {
            let method = AuthMethod::from_str(&auth_method)
                .map_err(|_| Error::Other(format!("Unknown auth method: {auth_method}").into()))?;
            validate_password(&password, &method, node)?;
            let data = AuthData::new(&method, &name, &password);
            let auth = AuthDef::new(method, data.into_string());
            Params::CreateUser(name, auth)
        }
        IrNode::Acl(Acl::AlterUser {
            name, alter_option, ..
        }) => {
            let alter_option_param = match alter_option {
                AlterOption::Password {
                    password,
                    auth_method,
                } => {
                    let method = AuthMethod::from_str(&auth_method).map_err(|_| {
                        Error::Other(format!("Unknown auth method: {auth_method}").into())
                    })?;
                    validate_password(&password, &method, node)?;
                    let data = AuthData::new(&method, &name, &password);
                    let auth = AuthDef::new(method, data.into_string());
                    AlterOptionParam::ChangePassword(auth)
                }
                AlterOption::Login => AlterOptionParam::Login,
                AlterOption::NoLogin => AlterOptionParam::NoLogin,
            };
            Params::AlterUser(name, alter_option_param)
        }
        IrNode::Acl(Acl::GrantPrivilege {
            grant_type,
            grantee_name,
            ..
        }) => {
            // Nothing to check
            Params::GrantPrivilege(grant_type, grantee_name)
        }
        IrNode::Acl(Acl::RevokePrivilege {
            revoke_type,
            grantee_name,
            ..
        }) => {
            // Nothing to check
            Params::RevokePrivilege(revoke_type, grantee_name)
        }
        n => {
            unreachable!("this function should only be called for ddl or acl nodes, not {n:?}")
        }
    };

    let _su = session::su(ADMIN_ID).expect("cant fail because admin should always have session");

    'retry: loop {
        if Instant::now() > deadline {
            return Err(Error::Timeout);
        }

        let index = node.read_index(deadline.duration_since(Instant::now()))?;

        if storage.properties.pending_schema_change()?.is_some() {
            node.wait_index(index + 1, deadline.duration_since(Instant::now()))?;
            continue 'retry;
        }

        let schema_version = storage.properties.next_schema_version()?;

        // Check for conflicts and make the op
        let op = match &params {
            Params::CreateProcedure(params) => {
                if params.func_exists() {
                    // Function already exists, no op needed.
                    return Ok(ConsumerResult { row_count: 0 });
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
                Op::DdlPrepare {
                    schema_version,
                    ddl,
                }
            }
            Params::DropProcedure(name, params) => {
                let Some(routine) = &storage.routines.by_name(name)? else {
                    // Procedure doesn't exist yet, no op needed
                    return Ok(ConsumerResult { row_count: 0 });
                };

                // drop by name if no parameters are specified
                if !params.is_empty() {
                    ensure_parameters_match(routine, params)?;
                }

                let ddl = OpDdl::DropProcedure {
                    id: routine.id,
                    initiator: current_user,
                };
                Op::DdlPrepare {
                    schema_version,
                    ddl,
                }
            }
            Params::CreateTable(params) => {
                if params.space_exists()? {
                    // Space already exists, no op needed
                    return Ok(ConsumerResult { row_count: 0 });
                }
                // XXX: this is stupid, we pass raft op by value everywhere even
                // though it's always just dropped right after serialization.
                // This forces us to clone it quite often. The root problem is
                // that we nest structs a lot and having references to structs
                // in other structs (which is what we should be doing) is very
                // painfull in rust.
                let mut params = params.clone();
                params.choose_id_if_not_specified()?;
                params.test_create_space(storage)?;
                let ddl = params.into_ddl()?;
                Op::DdlPrepare {
                    schema_version,
                    ddl,
                }
            }
            Params::DropTable(name) => {
                let Some(space_def) = storage.tables.by_name(name)? else {
                    // Space doesn't exist yet, no op needed
                    return Ok(ConsumerResult { row_count: 0 });
                };
                let ddl = OpDdl::DropTable {
                    id: space_def.id,
                    initiator: current_user,
                };
                Op::DdlPrepare {
                    schema_version,
                    ddl,
                }
            }
            Params::CreateUser(name, auth) => {
                if storage.roles.by_name(name)?.is_some() {
                    return Err(Error::Other(format!("Role {name} already exists").into()));
                }
                if let Some(user_def) = storage.users.by_name(name)? {
                    if user_def.auth != *auth {
                        return Err(Error::Other(
                            format!("User {name} already exists with different auth method").into(),
                        ));
                    }
                    // User already exists, no op needed
                    return Ok(ConsumerResult { row_count: 0 });
                }
                let id = node.get_next_grantee_id()?;
                let user_def = UserDef {
                    id,
                    name: name.clone(),
                    schema_version,
                    auth: auth.clone(),
                    owner: current_user,
                };
                Op::Acl(OpAcl::CreateUser { user_def })
            }
            Params::AlterUser(name, alter_option_param) => {
                if storage.roles.by_name(name)?.is_some() {
                    return Err(Error::Other(
                        format!("Role {name} exists. Unable to alter role.").into(),
                    ));
                }
                let Some(user_def) = storage.users.by_name(name)? else {
                    // User doesn't exists, no op needed.
                    return Ok(ConsumerResult { row_count: 0 });
                };

                // For ALTER Login/NoLogin.
                let grantor_id = session::euid()?;
                let grantee_id = get_grantee_id(storage, name)?;
                let object_type = SchemaObjectType::Universe;
                let object_id = 0;
                let privilege = PrivilegeType::Login;
                let priv_def = PrivilegeDef::new(
                    privilege,
                    object_type,
                    object_id,
                    grantee_id,
                    grantor_id,
                    schema_version,
                )
                .map_err(Error::other)?;

                match alter_option_param {
                    AlterOptionParam::ChangePassword(auth) => {
                        if &user_def.auth == auth {
                            // Password is already the one given, no op needed.
                            return Ok(ConsumerResult { row_count: 0 });
                        }
                        Op::Acl(OpAcl::ChangeAuth {
                            user_id: user_def.id,
                            auth: auth.clone(),
                            initiator: current_user,
                            schema_version,
                        })
                    }

                    AlterOptionParam::Login => {
                        // It will be checked at a later stage whether login is already granted
                        Op::Acl(OpAcl::GrantPrivilege { priv_def })
                    }
                    AlterOptionParam::NoLogin => {
                        // It will be checked at a later stage whether login was not granted
                        Op::Acl(OpAcl::RevokePrivilege {
                            priv_def,
                            initiator: current_user,
                        })
                    }
                }
            }
            Params::DropUser(name) => {
                let Some(user_def) = storage.users.by_name(name)? else {
                    // User doesn't exist yet, no op needed
                    return Ok(ConsumerResult { row_count: 0 });
                };
                Op::Acl(OpAcl::DropUser {
                    user_id: user_def.id,
                    initiator: current_user,
                    schema_version,
                })
            }
            Params::CreateRole(name) => {
                let sys_user = Space::from(SystemSpace::User)
                    .index("name")
                    .expect("_user should have an index by name")
                    .get(&(name,))?;
                if let Some(user) = sys_user {
                    let entry_type: &str = user.get(3).unwrap();
                    if entry_type == "user" {
                        return Err(Error::Sbroad(SbroadError::Invalid(
                            Entity::Acl,
                            Some(format!("Unable to create role {name}. User with the same name already exists")),
                        )));
                    } else {
                        return Ok(ConsumerResult { row_count: 0 });
                    }
                }
                let id = node.get_next_grantee_id()?;
                let role_def = RoleDef {
                    id,
                    name: name.clone(),
                    // This field will be updated later.
                    schema_version,
                    owner: current_user,
                };
                Op::Acl(OpAcl::CreateRole { role_def })
            }
            Params::DropRole(name) => {
                let Some(role_def) = storage.roles.by_name(name)? else {
                    // Role doesn't exist yet, no op needed
                    return Ok(ConsumerResult { row_count: 0 });
                };
                Op::Acl(OpAcl::DropRole {
                    role_id: role_def.id,
                    initiator: current_user,
                    schema_version,
                })
            }
            Params::GrantPrivilege(grant_type, grantee_name) => {
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
                    return Ok(ConsumerResult { row_count: 0 });
                }
                Op::Acl(OpAcl::GrantPrivilege {
                    priv_def: PrivilegeDef::new(
                        privilege,
                        object_type,
                        object_id,
                        grantee_id,
                        grantor_id,
                        schema_version,
                    )
                    .map_err(Error::other)?,
                })
            }
            Params::RevokePrivilege(revoke_type, grantee_name) => {
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
                    return Ok(ConsumerResult { row_count: 0 });
                }

                Op::Acl(OpAcl::RevokePrivilege {
                    priv_def: PrivilegeDef::new(
                        privilege,
                        object_type,
                        object_id,
                        grantee_id,
                        grantor_id,
                        schema_version,
                    )
                    .map_err(Error::other)?,
                    initiator: current_user,
                })
            }
        };
        let is_ddl_prepare = matches!(op, Op::DdlPrepare { .. });

        let term = raft::Storage::term(&node.raft_storage, index)?;
        let predicate = cas::Predicate {
            index,
            term,
            ranges: cas::schema_change_ranges().into(),
        };
        // Note: as_user doesnt really serve any purpose for DDL checks
        // It'll change when access control checks will be introduced for DDL
        let res = cas::compare_and_swap(
            op,
            predicate,
            current_user,
            deadline.duration_since(Instant::now()),
        );
        let (index, term) = unwrap_ok_or!(res,
            Err(e) => {
                if e.is_retriable() {
                    continue 'retry;
                } else {
                    return Err(e);
                }
            }
        );

        node.wait_index(index, deadline.duration_since(Instant::now()))?;
        if is_ddl_prepare {
            wait_for_ddl_commit(index, deadline.duration_since(Instant::now()))?;
        }

        if term != raft::Storage::term(&node.raft_storage, index)? {
            // Leader has changed and the entry got rolled back, retry.
            continue 'retry;
        }

        return Ok(ConsumerResult { row_count: 1 });
    }

    enum AlterOptionParam {
        ChangePassword(AuthDef),
        Login,
        NoLogin,
    }

    // THOUGHT: should `owner_id` be part of `CreateUser`, `CreateRole` params?
    enum Params {
        CreateTable(CreateTableParams),
        DropTable(String),
        CreateUser(String, AuthDef),
        AlterUser(String, AlterOptionParam),
        DropUser(String),
        CreateRole(String),
        DropRole(String),
        GrantPrivilege(GrantRevokeType, String),
        RevokePrivilege(GrantRevokeType, String),
        CreateProcedure(CreateProcParams),
        DropProcedure(String, Vec<ParamDef>),
    }
}

const TRACER_KEY: &str = "Tracer";

/// Executes a query sub-plan on the local node.
#[proc(packed_args)]
pub fn execute(raw: &RawBytes) -> traft::Result<Tuple> {
    let (raw_required, mut raw_optional) = decode_msgpack(raw)?;

    let mut required = RequiredData::try_from(EncodedRequiredData::from(raw_required))?;

    let tracing_meta = std::mem::take(&mut required.tracing_meta);
    let mut exec = || {
        let runtime = StorageRuntime::new().map_err(Error::from)?;
        match runtime.execute_plan(&mut required, &mut raw_optional) {
            Ok(mut any_tuple) => {
                if let Some(tuple) = any_tuple.downcast_mut::<Tuple>() {
                    debug!(
                        Option::from("execute"),
                        &format!("Execution result: {tuple:?}"),
                    );
                    let tuple: Tuple = std::mem::replace(tuple, Tuple::new(&())?);
                    Ok(tuple)
                } else {
                    Err(Error::from(SbroadError::FailedTo(
                        Action::Decode,
                        None,
                        format!("tuple {any_tuple:?}"),
                    )))
                }
            }
            Err(e) => Err(Error::from(e)),
        }
    };

    if let Some(mut meta) = tracing_meta {
        let ctx: Context = (&mut meta.context).into();
        let tracer_kind = ctx.baggage().get(TRACER_KEY);
        let kind = if let Some(value) = tracer_kind {
            TracerKind::from_str(&value.as_str()).map_err(|_| {
                Error::from(SbroadError::Invalid(
                    Entity::RequiredData,
                    Some(format!("unknown tracer: {}", value.as_str())),
                ))
            })?
        } else {
            return Err(Error::from(SbroadError::Invalid(
                Entity::RequiredData,
                Some("no tracer in context".into()),
            )));
        };

        let tracer = kind.get_tracer();
        query_span::<Result<Tuple, Error>, _>(
            "\"api.storage.execute\"",
            &meta.trace_id,
            tracer,
            &ctx,
            "",
            exec,
        )
    } else {
        exec()
    }
}
