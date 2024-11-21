use anyhow::{anyhow, bail, Context};
use sbroad::executor::engine::helpers::{
    decode_msgpack, replace_metadata_in_dql_result, try_get_metadata_from_plan,
};

use tarantool::tuple::{RawBytes, Tuple};

use crate::api::helper::load_config;
use crate::api::{COORDINATOR_ENGINE, SEGMENT_ENGINE};
use crate::utils::{wrap_proc_result, RawProcResult, RetResult};
use sbroad::backend::sql::ir::PatternWithParams;
use sbroad::executor::protocol::{EncodedRequiredData, RequiredData};
use sbroad::executor::Query;

use sbroad::debug;

/// Dispatch parameterized SQL query from coordinator to the segments.
#[tarantool::proc(packed_args)]
fn dispatch_query(args: &RawBytes) -> RetResult<RawProcResult, String> {
    RetResult(wrap_proc_result(
        Some("dispatch_query"),
        dispatch_query_inner(args),
    ))
}

fn dispatch_query_inner(args: &RawBytes) -> anyhow::Result<RawProcResult> {
    let tuple = Tuple::try_from_slice(args)?;
    let lua_params = PatternWithParams::try_from(&tuple).context("build params")?;

    load_config(&COORDINATOR_ENGINE)?;

    COORDINATOR_ENGINE.with(|engine| {
        let runtime = engine.lock();
        let mut query =
            Query::new(&*runtime, &lua_params.pattern, lua_params.params).context("build query")?;
        if let Ok(true) = query.is_ddl() {
            bail!("DDL queries are not supported");
        }
        if let Ok(true) = query.is_acl() {
            bail!("ACL queries are not supported");
        }
        if let Ok(true) = query.is_block() {
            bail!("blocks of commands are not supported");
        }
        if let Ok(true) = query.is_plugin() {
            bail!("plugin of commands are not supported");
        }

        let metadata = try_get_metadata_from_plan(query.get_exec_plan())?;
        let dispatch_result = query.dispatch()?;
        if let Ok(mut tuple) = dispatch_result.downcast::<Tuple>() {
            debug!(
                Option::from("query dispatch"),
                &format!("Returning tuple: {tuple:?}")
            );

            // replace tarantool's metadata with the metadata from the plan
            if let Some(metadata) = metadata {
                tuple = Box::new(replace_metadata_in_dql_result(&tuple, &metadata)?);
            }
            Ok(RawProcResult::Tuple(*tuple))
        } else {
            bail!("unsupported result type")
        }
    })
}

#[tarantool::proc(packed_args)]
fn proc_sql_execute(args: &RawBytes) -> RetResult<RawProcResult, String> {
    RetResult(wrap_proc_result(Some("execute_query"), execute_inner(args)))
}

fn execute_inner(args: &RawBytes) -> anyhow::Result<RawProcResult> {
    let (raw_required, optional_bytes, cache_info) = decode_msgpack(&args.0)?;

    load_config(&SEGMENT_ENGINE)?;

    let mut required = RequiredData::try_from(EncodedRequiredData::from(raw_required))
        .context("decode required data")?;

    SEGMENT_ENGINE.with(|engine| {
        let runtime = engine.lock();
        let result = runtime
            .execute_plan(&mut required, optional_bytes, cache_info)
            .context("execute plan")?;
        result
            .downcast::<Tuple>()
            .map_or_else(
                |maybe_mp| {
                    maybe_mp
                        .downcast::<Vec<u8>>()
                        .map(|mp| RawProcResult::Msgpack(*mp))
                },
                |tuple| Ok(RawProcResult::Tuple(*tuple)),
            )
            .map_err(|_| anyhow!("unsupported result type"))
    })
}
