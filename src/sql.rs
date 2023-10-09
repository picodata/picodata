//! Clusterwide SQL query execution.

use crate::schema::{wait_for_ddl_commit, CreateSpaceParams, DistributionParam, Field, ShardingFn};
use crate::sql::router::RouterRuntime;
use crate::sql::storage::StorageRuntime;
use crate::traft::error::Error;
use crate::traft::op::{Acl as OpAcl, Ddl as OpDdl, Op};
use crate::traft::{self, node};
use crate::util::duration_from_secs_f64_clamped;
use crate::{cas, unwrap_ok_or};

use sbroad::backend::sql::ir::{EncodedPatternWithParams, PatternWithParams};
use sbroad::debug;
use sbroad::errors::{Action, SbroadError};
use sbroad::executor::engine::helpers::decode_msgpack;
use sbroad::executor::protocol::{EncodedRequiredData, RequiredData};
use sbroad::executor::result::ConsumerResult;
use sbroad::executor::Query;
use sbroad::ir::acl::Acl;
use sbroad::ir::ddl::Ddl;
use sbroad::otm::query_span;

use ::tarantool::proc;
use ::tarantool::space::FieldType;
use ::tarantool::time::Instant;
use ::tarantool::tuple::{RawBytes, Tuple};

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
            if query.is_ddl().map_err(Error::from)? {
                let ir_plan = query.get_exec_plan().get_ir_plan();
                let top_id = ir_plan.get_top().map_err(Error::from)?;
                let ir_plan_mut = query.get_mut_exec_plan().get_mut_ir_plan();
                let ddl = ir_plan_mut.take_ddl_node(top_id).map_err(Error::from)?;
                let timeout = duration_from_secs_f64_clamped(ddl.timeout()?);
                let deadline = Instant::now().saturating_add(timeout);
                let node = node::global()?;
                let result = reenterable_ddl_request(node, ddl, deadline)?;
                Tuple::new(&(result,)).map_err(Error::from)
            } else if query.is_acl().map_err(Error::from)? {
                let ir_plan = query.get_exec_plan().get_ir_plan();
                let top_id = ir_plan.get_top().map_err(Error::from)?;
                let ir_plan_mut = query.get_mut_exec_plan().get_mut_ir_plan();
                let acl = ir_plan_mut.take_acl_node(top_id).map_err(Error::from)?;
                let timeout = duration_from_secs_f64_clamped(acl.timeout()?);
                let deadline = Instant::now().saturating_add(timeout);
                let node = node::global()?;
                let result = reenterable_acl_request(node, acl, deadline)?;
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

fn reenterable_acl_request(
    node: &node::Node,
    acl: Acl,
    deadline: Instant,
) -> traft::Result<ConsumerResult> {
    let storage = &node.storage;

    // Check parameters
    let params = match acl {
        Acl::DropUser { name, .. } => {
            // Nothing to check
            Params::DropUser(name)
        }
        Acl::DropRole { .. } | Acl::CreateRole { .. } | Acl::CreateUser { .. } => {
            return Err(Error::Other("ACL is not implemented yet".into()))
        }
    };

    'retry: loop {
        let index = node.read_index(deadline.duration_since(Instant::now()))?;

        if storage.properties.pending_schema_change()?.is_some() {
            node.wait_index(index + 1, deadline.duration_since(Instant::now()))?;
            continue 'retry;
        }

        // Check for conflicts and make the op.
        let acl = match &params {
            Params::DropUser(name) => {
                let Some(user_def) = storage.users.by_name(name)? else {
                    // User doesn't exist yet, no op needed
                    return Ok(ConsumerResult { row_count: 0 });
                };
                let schema_version = storage.properties.next_schema_version()?;
                OpAcl::DropUser {
                    user_id: user_def.id,
                    schema_version,
                }
            }
        };

        let op = Op::Acl(acl);
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

        if term != raft::Storage::term(&node.raft_storage, index)? {
            // Leader has changed and the entry got rolled back, retry.
            continue 'retry;
        }

        return Ok(ConsumerResult { row_count: 1 });
    }

    enum Params {
        DropUser(String),
    }
}

fn reenterable_ddl_request(
    node: &node::Node,
    ddl: Ddl,
    deadline: Instant,
) -> traft::Result<ConsumerResult> {
    let storage = &node.storage;

    // Check parameters
    let params = match ddl {
        Ddl::CreateTable {
            name,
            format,
            primary_key,
            sharding_key,
            engine_type,
            ..
        } => {
            let format = format
                .into_iter()
                .map(|f| Field {
                    name: f.name,
                    r#type: FieldType::from(&f.data_type),
                    is_nullable: f.is_nullable,
                })
                .collect();
            let params = CreateSpaceParams {
                id: None,
                name,
                format,
                primary_key,
                distribution: DistributionParam::Sharded,
                by_field: None,
                sharding_key,
                sharding_fn: Some(ShardingFn::Murmur3),
                engine: Some(engine_type),
                timeout: None,
            };
            params.validate()?;
            Params::CreateSpace(params)
        }
        Ddl::DropTable { name, .. } => {
            // Nothing to check
            Params::DropSpace(name)
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
        let ddl = match &params {
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
                params.into_ddl()?
            }
            Params::DropSpace(name) => {
                let Some(space_def) = storage.spaces.by_name(name)? else {
                    // Space doesn't exist yet, no op needed
                    return Ok(ConsumerResult { row_count: 0 });
                };
                OpDdl::DropSpace { id: space_def.id }
            }
        };

        let schema_version = storage.properties.next_schema_version()?;
        let op = Op::DdlPrepare {
            schema_version,
            ddl,
        };
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
        wait_for_ddl_commit(index, deadline.duration_since(Instant::now()))?;

        if term != raft::Storage::term(&node.raft_storage, index)? {
            // Leader has changed and the entry got rolled back, retry.
            continue 'retry;
        }

        return Ok(ConsumerResult { row_count: 1 });
    }

    enum Params {
        CreateSpace(CreateSpaceParams),
        DropSpace(String),
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
