//! Clusterwide SQL query execution.

use crate::schema::{self, CreateSpaceParams, DistributionParam, Field, ShardingFn, SpaceDef};
use crate::sql::router::RouterRuntime;
use crate::sql::storage::StorageRuntime;
use crate::traft::error::Error;
use crate::traft::op::{Ddl as OpDdl, Op};
use crate::traft::{self, node};

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

use ::tarantool::proc;
use ::tarantool::space::FieldType;
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
                let timeout: f64 = ddl.timeout().map_err(Error::from)?;
                let storage = &node::global()?.storage;
                let ddl_op = match ddl {
                    Ddl::CreateShardedTable {
                        name,
                        mut format,
                        primary_key,
                        sharding_key,
                        ..
                    } => {
                        let format = format
                            .iter_mut()
                            .map(|f| Field {
                                name: std::mem::take(&mut f.name),
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
                            sharding_key: Some(sharding_key),
                            sharding_fn: Some(ShardingFn::Murmur3),
                            timeout,
                        };
                        let storage = &node::global()?.storage;
                        let mut params = params.validate(storage)?;
                        params.test_create_space(storage)?;
                        params.into_ddl(storage)?
                    }
                    Ddl::DropTable { ref name, .. } => {
                        let space_def: SpaceDef =
                            storage.spaces.by_name(name)?.ok_or_else(|| {
                                Error::from(SbroadError::FailedTo(
                                    Action::Find,
                                    Some(Entity::Table),
                                    format!("{name} doesn't exist in pico_space"),
                                ))
                            })?;
                        OpDdl::DropSpace { id: space_def.id }
                    }
                };
                let duration = Duration::from_secs_f64(timeout);
                let schema_version = storage.properties.next_schema_version()?;
                let op = Op::DdlPrepare {
                    schema_version,
                    ddl: ddl_op,
                };
                let index = schema::prepare_schema_change(op, duration)?;
                schema::wait_for_ddl_commit(index, duration)?;
                let result = ConsumerResult { row_count: 1 };
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
