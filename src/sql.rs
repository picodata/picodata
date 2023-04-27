use crate::traft;
use ::tarantool::proc;
use ::tarantool::tuple::{RawByteBuf, RawBytes};
use sbroad::backend::sql::ir::{EncodedPatternWithParams, PatternWithParams};
use sbroad::ir::value::Value;
use sbroad_picodata::api::{determine_bucket_id, dispatch_sql, execute_sql};

#[proc(packed_args)]
pub fn dispatch_query(encoded_params: EncodedPatternWithParams) -> traft::Result<RawByteBuf> {
    let params = PatternWithParams::from(encoded_params);
    let bytes = dispatch_sql(params)?;
    Ok(RawByteBuf::from(bytes))
}

#[proc(packed_args)]
pub fn execute(raw: &RawBytes) -> traft::Result<RawByteBuf> {
    let bytes = execute_sql(raw)?;
    Ok(RawByteBuf::from(bytes))
}

#[proc]
pub fn calculate_bucket_id(rec: String) -> traft::Result<u64> {
    let bucket_str = Value::from(rec);
    let bucket_id = determine_bucket_id(&[&bucket_str])?;
    Ok(bucket_id)
}
