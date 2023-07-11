//! Clusterwide SQL query execution.

use crate::schema::{self, CreateSpaceParams, DistributionParam, Field, ShardingFn};
use crate::sql::runtime::router::RouterRuntime;
use crate::sql::runtime::storage::StorageRuntime;
use crate::traft::{self, node, op::Op};

use sbroad::backend::sql::ir::{EncodedPatternWithParams, PatternWithParams};
use sbroad::debug;
use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::engine::helpers::decode_msgpack;
use sbroad::executor::protocol::{EncodedRequiredData, RequiredData};
use sbroad::executor::result::ConsumerResult;
use sbroad::executor::Query;
use sbroad::ir::ddl::Ddl;
use sbroad::otm::query_span;

use std::time::Duration;

use ::tarantool::decimal::Decimal;
use ::tarantool::proc;
use ::tarantool::space::FieldType;
use ::tarantool::tuple::{RawBytes, Tuple};

pub mod runtime;

/// Dispatches a query to the cluster.
#[proc(packed_args)]
pub fn dispatch_query(encoded_params: EncodedPatternWithParams) -> traft::Result<Tuple> {
    let mut params = PatternWithParams::from(encoded_params);
    let id = params.clone_id();
    let ctx = params.extract_context();
    let tracer = params.get_tracer();

    let result = query_span(
        "\"api.router\"",
        &id,
        &tracer,
        &ctx,
        &params.pattern,
        || {
            let runtime = RouterRuntime::new()?;
            let mut query = Query::new(&runtime, &params.pattern, params.params)?;
            if query.is_ddl()? {
                let ir_plan = query.get_exec_plan().get_ir_plan();
                let top_id = ir_plan.get_top()?;
                let ir_plan_mut = query.get_mut_exec_plan().get_mut_ir_plan();
                let ddl = ir_plan_mut.get_mut_ddl_node(top_id)?;
                match ddl {
                    Ddl::CreateShardedTable {
                        ref mut name,
                        ref mut format,
                        ref mut primary_key,
                        ref mut sharding_key,
                        ref mut timeout,
                    } => {
                        let format = format
                            .iter_mut()
                            .map(|f| Field {
                                name: std::mem::take(&mut f.name),
                                r#type: FieldType::from(&f.data_type),
                                is_nullable: f.is_nullable,
                            })
                            .collect();
                        let duration: f64 = std::mem::replace(timeout, Decimal::zero())
                            .to_string()
                            .parse()
                            .map_err(|e| {
                                SbroadError::Invalid(
                                    Entity::SpaceMetadata,
                                    Some(format!("timeout parsing error {e:?}")),
                                )
                            })?;
                        let params = CreateSpaceParams {
                            id: None,
                            name: std::mem::take(name),
                            format,
                            primary_key: std::mem::take(primary_key),
                            distribution: DistributionParam::Sharded,
                            by_field: None,
                            sharding_key: Some(std::mem::take(sharding_key)),
                            sharding_fn: Some(ShardingFn::Murmur3),
                            timeout: duration,
                        };
                        let timeout = Duration::from_secs_f64(params.timeout);
                        let storage = &node::global()
                            .map_err(|e| {
                                SbroadError::Invalid(
                                    Entity::Runtime,
                                    Some(format!("raft node error {e:?}")),
                                )
                            })?
                            .storage;
                        let mut params = params.validate(storage).map_err(|e| {
                            SbroadError::Invalid(
                                Entity::SpaceMetadata,
                                Some(format!("space parameters validation error {e:?}")),
                            )
                        })?;
                        params.test_create_space(storage).map_err(|e| {
                            SbroadError::Invalid(
                                Entity::SpaceMetadata,
                                Some(format!("space parameters test error {e:?}")),
                            )
                        })?;
                        let ddl = params.into_ddl(storage).map_err(|e| {
                            SbroadError::FailedTo(
                                Action::Create,
                                Some(Entity::SpaceMetadata),
                                format!("{e:?}"),
                            )
                        })?;
                        let schema_version =
                            storage.properties.next_schema_version().map_err(|e| {
                                SbroadError::FailedTo(
                                    Action::Get,
                                    Some(Entity::Schema),
                                    format!("{e:?}"),
                                )
                            })?;
                        let op = Op::DdlPrepare {
                            schema_version,
                            ddl,
                        };
                        let index = schema::prepare_schema_change(op, timeout).map_err(|e| {
                            SbroadError::FailedTo(
                                Action::Prepare,
                                Some(Entity::Schema),
                                format!("{e:?}"),
                            )
                        })?;
                        schema::wait_for_ddl_commit(index, timeout).map_err(|e| {
                            SbroadError::FailedTo(
                                Action::Create,
                                Some(Entity::Space),
                                format!("{e:?}"),
                            )
                        })?;
                        let result = ConsumerResult { row_count: 1 };
                        Tuple::new(&(result,)).map_err(|e| {
                            SbroadError::FailedTo(
                                Action::Decode,
                                Some(Entity::Tuple),
                                format!("{:?}", e),
                            )
                        })
                    }
                }
            } else {
                match query.dispatch() {
                    Ok(mut any_tuple) => {
                        if let Some(tuple) = any_tuple.downcast_mut::<Tuple>() {
                            debug!(
                                Option::from("dispatch"),
                                &format!("Dispatch result: {tuple:?}"),
                            );
                            let empty_tuple = Tuple::new(&()).map_err(|e| {
                                SbroadError::FailedTo(
                                    Action::Decode,
                                    None,
                                    format!("tuple {:?}", e),
                                )
                            })?;
                            let tuple: Tuple = std::mem::replace(tuple, empty_tuple);
                            Ok(tuple)
                        } else {
                            Err(SbroadError::FailedTo(
                                Action::Decode,
                                None,
                                format!("tuple {any_tuple:?}"),
                            ))
                        }
                    }
                    Err(e) => Err(e),
                }
            }
        },
    );
    Ok(result?)
}

/// Executes a query sub-plan on the local node.
#[proc(packed_args)]
pub fn execute(raw: &RawBytes) -> traft::Result<Tuple> {
    let (raw_required, mut raw_optional) = decode_msgpack(raw)?;

    let mut required = RequiredData::try_from(EncodedRequiredData::from(raw_required))?;

    let id: String = required.id().into();
    let ctx = required.extract_context();
    let tracer = required.tracer();

    let result = query_span("\"api.storage\"", &id, &tracer, &ctx, "", || {
        let runtime = StorageRuntime::new()?;
        match runtime.execute_plan(&mut required, &mut raw_optional) {
            Ok(mut any_tuple) => {
                if let Some(tuple) = any_tuple.downcast_mut::<Tuple>() {
                    debug!(
                        Option::from("execute"),
                        &format!("Execution result: {tuple:?}"),
                    );
                    let empty_tuple = Tuple::new(&()).map_err(|e| {
                        SbroadError::FailedTo(Action::Decode, None, format!("tuple {:?}", e))
                    })?;
                    let tuple: Tuple = std::mem::replace(tuple, empty_tuple);
                    Ok(tuple)
                } else {
                    Err(SbroadError::FailedTo(
                        Action::Decode,
                        None,
                        format!("tuple {any_tuple:?}"),
                    ))
                }
            }
            Err(e) => Err(e),
        }
    });
    Ok(result?)
}
