//! Clusterwide SQL query execution.

use crate::schema::{
    wait_for_ddl_commit, CreateSpaceParams, DistributionParam, Field, RoleDef, ShardingFn, UserDef,
    UserId,
};
use crate::sql::router::RouterRuntime;
use crate::sql::storage::StorageRuntime;
use crate::traft::error::Error;
use crate::traft::node::Node as TraftNode;
use crate::traft::op::{Acl as OpAcl, Ddl as OpDdl, Op};
use crate::traft::{self, node};
use crate::util::duration_from_secs_f64_clamped;
use crate::{cas, unwrap_ok_or};

use sbroad::backend::sql::ir::{EncodedPatternWithParams, PatternWithParams};
use sbroad::debug;
use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::engine::helpers::decode_msgpack;
use sbroad::executor::protocol::{EncodedRequiredData, RequiredData};
use sbroad::executor::result::ConsumerResult;
use sbroad::executor::Query;
use sbroad::ir::acl::Acl;
use sbroad::ir::ddl::Ddl;
use sbroad::ir::Node as IrNode;
use sbroad::otm::query_span;

use ::tarantool::auth::{AuthData, AuthDef, AuthMethod};
use ::tarantool::proc;
use ::tarantool::space::{FieldType, Space, SystemSpace};
use ::tarantool::time::Instant;
use ::tarantool::tuple::{RawBytes, Tuple};
use std::str::FromStr;

pub mod router;
pub mod storage;

pub const DEFAULT_BUCKET_COUNT: u64 = 3000;

/// Dispatches a query to the cluster.
#[proc(packed_args)]
pub fn dispatch_query(encoded_params: EncodedPatternWithParams) -> traft::Result<Tuple> {
    let mut params = PatternWithParams::try_from(encoded_params).map_err(Error::from)?;
    let id = params.clone_id();
    let ctx = params.extract_context();
    let tracer = params.tracer;

    query_span::<Result<Tuple, Error>, _>(
        "\"api.router\"",
        &id,
        &tracer,
        &ctx,
        &params.pattern,
        || {
            let runtime = RouterRuntime::new().map_err(Error::from)?;
            let mut query =
                Query::new(&runtime, &params.pattern, params.params).map_err(Error::from)?;
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
        },
    )
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
}

fn check_password_min_length(
    password: &str,
    auth_method: &AuthMethod,
    node: &TraftNode,
) -> traft::Result<()> {
    if let AuthMethod::Ldap = auth_method {
        // LDAP doesn't need password for authentication
        return Ok(());
    }

    let storage = &node.storage;
    let password_min_length = storage.properties.password_min_length()?;
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
    Ok(())
}

fn reenterable_schema_change_request(
    node: &TraftNode,
    ir_node: IrNode,
) -> traft::Result<ConsumerResult> {
    let storage = &node.storage;

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
            let params = CreateSpaceParams {
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
            };
            params.validate()?;
            Params::CreateSpace(params)
        }
        IrNode::Ddl(Ddl::DropTable { name, .. }) => {
            // Nothing to check
            Params::DropSpace(name)
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
            check_password_min_length(&password, &method, node)?;
            let data = AuthData::new(&method, &name, &password);
            let auth = AuthDef::new(method, data.into_string());
            Params::CreateUser(name, auth)
        }
        IrNode::Acl(Acl::AlterUser {
            name,
            password,
            auth_method,
            ..
        }) => {
            let method = AuthMethod::from_str(&auth_method)
                .map_err(|_| Error::Other(format!("Unknown auth method: {auth_method}").into()))?;
            check_password_min_length(&password, &method, node)?;
            let data = AuthData::new(&method, &name, &password);
            let auth = AuthDef::new(method, data.into_string());
            Params::AlterUser(name, auth)
        }
        n => {
            unreachable!("this function should only be called for ddl or acl nodes, not {n:?}")
        }
    };

    'retry: loop {
        if Instant::now() > deadline {
            return Err(Error::Timeout);
        }

        let index = node.read_index(deadline.duration_since(Instant::now()))?;

        if storage.properties.pending_schema_change()?.is_some() {
            node.wait_index(index + 1, deadline.duration_since(Instant::now()))?;
            continue 'retry;
        }

        // Check for conflicts and make the op
        let mut op = match &params {
            Params::CreateSpace(params) => {
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
                params.test_create_space()?;
                let ddl = params.into_ddl()?;
                Op::DdlPrepare {
                    // This will be set right after the match statement.
                    schema_version: 0,
                    ddl,
                }
            }
            Params::DropSpace(name) => {
                let Some(space_def) = storage.spaces.by_name(name)? else {
                    // Space doesn't exist yet, no op needed
                    return Ok(ConsumerResult { row_count: 0 });
                };
                let ddl = OpDdl::DropSpace { id: space_def.id };
                Op::DdlPrepare {
                    // This will be set right after the match statement.
                    schema_version: 0,
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
                    // This will be set right after the match statement.
                    schema_version: 0,
                    auth: auth.clone(),
                };
                Op::Acl(OpAcl::CreateUser { user_def })
            }
            Params::AlterUser(name, auth) => {
                if storage.roles.by_name(name)?.is_some() {
                    return Err(Error::Other(
                        format!("Role {name} exists. Unable to alter role.").into(),
                    ));
                }
                let Some(user_def) = storage.users.by_name(name)? else {
                    // User doesn't exists, no op needed.
                    return Ok(ConsumerResult { row_count: 0 });
                };
                Op::Acl(OpAcl::ChangeAuth {
                    user_id: user_def.id,
                    auth: auth.clone(),
                    // This will be set right after the match statement.
                    schema_version: 0,
                })
            }
            Params::DropUser(name) => {
                let Some(user_def) = storage.users.by_name(name)? else {
                    // User doesn't exist yet, no op needed
                    return Ok(ConsumerResult { row_count: 0 });
                };
                Op::Acl(OpAcl::DropUser {
                    user_id: user_def.id,
                    // This will be set right after the match statement.
                    schema_version: 0,
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
                            Some(format!("Unable to create role {name}. User with the same name already exists"))
                        )));
                    } else {
                        return Ok(ConsumerResult { row_count: 0 });
                    }
                }
                let id = node.get_next_grantee_id()?;
                let role_def = RoleDef {
                    id,
                    name: name.clone(),
                    // This will be set right after the match statement.
                    schema_version: 0,
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
                    // This will be set right after the match statement.
                    schema_version: 0,
                })
            }
        };
        op.set_schema_version(storage.properties.next_schema_version()?);
        let is_ddl_prepare = matches!(op, Op::DdlPrepare { .. });

        let term = raft::Storage::term(&node.raft_storage, index)?;
        let predicate = cas::Predicate {
            index,
            term,
            ranges: cas::schema_change_ranges().into(),
        };
        let res = cas::compare_and_swap(op, predicate, deadline.duration_since(Instant::now()));
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

    enum Params {
        CreateSpace(CreateSpaceParams),
        DropSpace(String),
        CreateUser(String, AuthDef),
        AlterUser(String, AuthDef),
        DropUser(String),
        CreateRole(String),
        DropRole(String),
    }
}

/// Executes a query sub-plan on the local node.
#[proc(packed_args)]
pub fn execute(raw: &RawBytes) -> traft::Result<Tuple> {
    let (raw_required, mut raw_optional) = decode_msgpack(raw)?;

    let mut required = RequiredData::try_from(EncodedRequiredData::from(raw_required))?;

    let id: String = required.id().into();
    let ctx = required.extract_context();
    let tracer = required.tracer();

    query_span::<Result<Tuple, Error>, _>("\"api.storage\"", &id, &tracer, &ctx, "", || {
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
    })
}
