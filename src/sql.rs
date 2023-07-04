//! Cluster-wide SQL query execution.
use crate::traft;
use ::tarantool::proc;
use ::tarantool::tuple::{RawByteBuf, RawBytes};
use sbroad::backend::sql::ir::{EncodedPatternWithParams, PatternWithParams};
use sbroad_picodata::api::{dispatch_sql, execute_sql};

/// Dispatches a query to the cluster.
#[proc(packed_args)]
pub fn dispatch_query(encoded_params: EncodedPatternWithParams) -> traft::Result<RawByteBuf> {
    let params = PatternWithParams::from(encoded_params);
    let bytes = dispatch_sql(params)?;
    Ok(RawByteBuf::from(bytes))
}

/// Executes a query sub-plan on the local node.
#[proc(packed_args)]
pub fn execute(raw: &RawBytes) -> traft::Result<RawByteBuf> {
    let bytes = execute_sql(raw)?;
    Ok(RawByteBuf::from(bytes))
}
