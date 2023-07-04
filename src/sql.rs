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
            match query.dispatch() {
                Ok(mut any_tuple) => {
                    if let Some(tuple) = any_tuple.downcast_mut::<Tuple>() {
                        debug!(
                            Option::from("dispatch"),
                            &format!("Dispatch result: {tuple:?}"),
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
