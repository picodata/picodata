use std::collections::HashMap;

use sbroad::errors::{Entity, SbroadError};
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr};
use tarantool::msgpack;
use tarantool::tuple::{Decode, RawBytes, Tuple};

use crate::api::helper::load_config;
use crate::api::COORDINATOR_ENGINE;
use crate::utils::{wrap_proc_result, ProcResult};

use sbroad::executor::engine::Router;
use sbroad::ir::value::Value;

#[derive(Debug, Default, PartialEq, Eq)]
/// Tuple with space name and `key:value` map of values
pub struct ArgsMap {
    /// A key:value `HashMap` with key SmolStr and custom type Value
    pub rec: HashMap<SmolStr, Value>,
    /// Space name as `SmolStr`
    pub space: SmolStr,
}

/// Custom decode of the input function arguments
impl<'de> Decode<'de> for ArgsMap {
    fn decode(data: &'de [u8]) -> tarantool::Result<Self> {
        let (mut rec, space): (HashMap<String, Value>, String) = msgpack::decode(data)?;

        let rec: HashMap<SmolStr, Value> = rec
            .drain()
            .map(|(key, value)| (SmolStr::from(key), value))
            .collect();
        let space = SmolStr::from(space);

        Ok(ArgsMap { rec, space })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Tuple with space name and vec of values
pub struct ArgsTuple {
    /// Vec of custom type Value
    pub rec: Vec<Value>,
    /// Space name as `SmolStr`
    pub space: SmolStr,
}

/// Custom decode of the input function arguments
impl<'de> Decode<'de> for ArgsTuple {
    fn decode(data: &'de [u8]) -> tarantool::Result<Self> {
        let (rec, space): (Vec<Value>, String) = msgpack::decode(data)?;

        let space = SmolStr::from(space);

        Ok(ArgsTuple { rec, space })
    }
}

#[derive(Serialize, Deserialize)]
/// Lua function params
pub struct ArgsString {
    /// The input string for calculating bucket
    pub rec: SmolStr,
}

enum Args {
    String(ArgsString),
    Tuple(ArgsTuple),
    Map(ArgsMap),
}

impl TryFrom<&Tuple> for Args {
    type Error = SbroadError;

    fn try_from(tuple: &Tuple) -> Result<Self, Self::Error> {
        if let Ok(args) = tuple.decode::<ArgsString>() {
            return Ok(Self::String(args));
        }
        if let Ok(args) = tuple.decode::<ArgsTuple>() {
            return Ok(Self::Tuple(args));
        }
        if let Ok(args) = tuple.decode::<ArgsMap>() {
            return Ok(Self::Map(args));
        }

        Err(SbroadError::ParsingError(
            Entity::Args,
            format_smolstr!(
                "expected string, tuple with a space name, or map with a space name as an argument, \
                got args {:?}",
                &tuple
            ),
        ))
    }
}

#[tarantool::proc(packed_args)]
fn calculate_bucket_id(args: &RawBytes) -> ProcResult<u64> {
    wrap_proc_result(
        "calculate_bucket_id".into(),
        calculate_bucket_id_inner(args),
    )
}

fn calculate_bucket_id_inner(args: &RawBytes) -> anyhow::Result<u64> {
    let tuple = Tuple::try_from_slice(args)?;
    let args = Args::try_from(&tuple)?;
    load_config(&COORDINATOR_ENGINE)?;

    COORDINATOR_ENGINE.with(|engine| {
        let runtime = engine.lock();
        Ok(match args {
            Args::String(params) => {
                let bucket_str = Value::from(params.rec);
                runtime.determine_bucket_id(&[&bucket_str])
            }
            Args::Tuple(params) => runtime
                .extract_sharding_key_from_tuple(params.space, &params.rec)
                .map(|tuple| runtime.determine_bucket_id(&tuple))?,
            Args::Map(params) => runtime
                .extract_sharding_key_from_map(params.space, &params.rec)
                .map(|tuple| runtime.determine_bucket_id(&tuple))?,
        })
    })
}
