use rmp::encode::write_array_len;
use smol_str::{format_smolstr, SmolStr};
use std::any::Any;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::OnceLock;
use tarantool::session::with_su;
use tarantool::space::Space;
use tarantool::sql::{prepare_and_execute_raw, Statement};
use tarantool::tuple::{Tuple, TupleBuffer};

use crate::backend::sql::space::ADMIN_ID;
use crate::error;
use crate::errors::SbroadError;
use crate::executor::engine::helpers::proxy::sql_cache_proxy;
use crate::executor::engine::helpers::table_name;
use crate::executor::lru::DEFAULT_CAPACITY;
use crate::ir::node::NodeId;
use crate::ir::value::{EncodedValue, Value};
use crate::utils::ByteCounter;

// Byte keys used to decode MessagePack result came
// from local Tarantool execution.
const IPROTO_DATA: u8 = 0x30;
const IPROTO_ROW_COUNT: u8 = 0x42;
const IPROTO_META: u8 = 0x32;

/// Format in which we want to transform Local tarantool execution result into.
///
/// Fields:
/// * Dql -> { 'metadata', 'raws' }
/// * Dml -> { `row_count` }
///
/// Format:
/// * Raw -> vector of bytes
/// * Vshard -> Tuple(row count, cache miss flag, producer result)
#[derive(Debug)]
pub enum StorageReturnFormat {
    // Return result as a vector of bytes, no need
    // to allocate Tuple. Used for global tables
    // reads (exec_ir_on_any) or DML queries, that
    // materialize reading subtree locally.
    DqlRaw,
    /// The same as for `DqlRaw`.
    DmlRaw,
    // Return result as a Tuple of three elements:
    // [rows count, cache miss flag, producer result]
    // Used by all dql queries who go through vshard
    // (and so iproto), where we always must return
    // Tuple
    DqlVshard,
}

/// Storage runtime configuration.
#[allow(clippy::module_name_repetitions)]
pub struct StorageMetadata {
    /// Prepared statements cache capacity (on the storage).
    pub storage_capacity: usize,
    /// Prepared statements cache size in bytes (on the storage).
    /// If a new statement is bigger doesn't fit into the cache,
    /// it would not be cached but executed directly.
    pub storage_size_bytes: usize,
}

impl Default for StorageMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageMetadata {
    #[must_use]
    pub fn new() -> Self {
        StorageMetadata {
            storage_capacity: DEFAULT_CAPACITY,
            storage_size_bytes: 1024 * DEFAULT_CAPACITY,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.storage_capacity == 0 && self.storage_size_bytes == 0
    }
}

pub fn prepare(pattern: String) -> Result<Statement, SbroadError> {
    let proxy = sql_cache_proxy();
    let stmt = proxy.prepare(pattern).map_err(|e| {
        error!(Option::from("prepare"), &format!("{e:?}"));
        SbroadError::from(e)
    })?;
    Ok(stmt)
}

pub fn unprepare(
    plan_id: &SmolStr,
    entry: &mut (Statement, Vec<NodeId>),
) -> Result<(), SbroadError> {
    let (stmt, table_ids) = std::mem::take(entry);

    // Remove the statement from the instance cache.
    let proxy = sql_cache_proxy();
    proxy.unprepare(stmt).map_err(|e| {
        error!(Option::from("unprepare"), &format!("{e:?}"));
        SbroadError::from(e)
    })?;

    // Remove temporary tables from the instance.
    for node_id in table_ids {
        let table = table_name(plan_id, node_id);
        Space::find(table.as_str()).map(|space| with_su(ADMIN_ID, || space.drop()));
    }

    Ok(())
}

type EncodedRowCount = rmpv::Value;
type EncodedMetadata = rmpv::Value;
type EncodedData = rmpv::Value;

fn parse_dml_result(stream: &mut (impl Read + Sized)) -> EncodedRowCount {
    let map_len = rmp::decode::read_map_len(stream).expect("failed to read DML result");
    assert_eq!(map_len, 1, "invalid format for DML result");
    let key = rmp::decode::read_pfix(stream).expect("failed to read IPROTO key");
    let row_count = match key {
        IPROTO_ROW_COUNT => {
            let decode_res =
                rmpv::decode::read_value(stream).expect("failed to read DML row_count");
            let rmpv::Value::Map(map_vec) = decode_res else {
                panic!("Expected map for decode_res, got: {decode_res:?}");
            };
            let (_, value) = map_vec.first().expect("Decoded map is empty");
            value.clone()
        }
        _ => {
            unreachable!("unexpected IPROTO key: {}", key);
        }
    };
    row_count
}

fn parse_dql_result(stream: &mut (impl Read + Sized)) -> (EncodedMetadata, EncodedData) {
    let map_len = rmp::decode::read_map_len(stream).expect("failed to read DQL result");
    assert_eq!(map_len, 2, "invalid format for DQL result");
    let mut meta = None;
    let mut data = None;
    for _ in 0..map_len {
        let key = rmp::decode::read_pfix(stream).expect("failed to read IPROTO key");
        match key {
            IPROTO_META => {
                meta = Some(rmpv::decode::read_value(stream).expect("failed to read DQL metadata"));
            }
            IPROTO_DATA => {
                data = Some(rmpv::decode::read_value(stream).expect("failed to read DQL data"));
            }
            _ => {
                unreachable!("unexpected IPROTO key: {}", key);
            }
        }
    }
    (
        meta.expect("missing DQL metadata"),
        data.expect("missing DQL data"),
    )
}

fn repack_meta(meta: EncodedMetadata) -> (rmpv::Value, rmpv::Value) {
    let mut maps = rmpv::ext::from_value::<Vec<HashMap<u32, rmpv::Value>>>(meta)
        .expect("failed to decode DQL metadata");
    let mut repacked = Vec::with_capacity(2);
    for mut map in maps.drain(..) {
        let col_name = map.remove(&0).expect("missing name in DQL metadata");
        let col_type = map.remove(&1).expect("missing type in DQL metadata");
        // repacked.push(rmpv::Value::Map(vec![(col_name, col_type)]));
        repacked.push(rmpv::Value::Map(vec![
            (rmpv::Value::String("name".into()), col_name),
            (rmpv::Value::String("type".into()), col_type),
        ]));
    }
    ("metadata".into(), rmpv::Value::Array(repacked))
}

fn repack_data(
    data: EncodedData,
    max_rows: u64,
) -> Result<(rmpv::Value, rmpv::Value), SbroadError> {
    let rows = rmpv::ext::from_value::<Vec<rmpv::Value>>(data).expect("failed to decode DQL data");
    if rows.len() as u64 > max_rows {
        return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
            "Exceeded maximum number of rows ({max_rows}) in virtual table: {}",
            rows.len()
        )));
    }
    Ok(("rows".into(), rmpv::Value::Array(rows)))
}

/// Transform single MessagePack Value(row_count)
/// into pair of (Value("row_count"), Value(row_count)).
fn repack_row_count(row_count: EncodedRowCount) -> (rmpv::Value, rmpv::Value) {
    ("row_count".into(), row_count)
}

pub(crate) fn dql_cache_miss_result() -> Tuple {
    const RESULT_SIZE: usize = 20;

    fn create_result<W>(buf_ref: &mut W)
    where
        W: Write,
    {
        let empty_result = {
            let new_meta: (rmpv::Value, rmpv::Value) =
                ("metadata".into(), rmpv::Value::Array(vec![]));
            let new_data: (rmpv::Value, rmpv::Value) = ("rows".into(), rmpv::Value::Array(vec![]));
            rmpv::Value::Map(vec![new_meta, new_data])
        };

        write_array_len(buf_ref, 3).expect("failed to write array length");
        // write number of rows in result
        rmpv::encode::write_value(buf_ref, &rmpv::Value::from(0))
            .expect("failed to write rows count");
        // write cache miss flag
        rmpv::encode::write_value(buf_ref, &rmpv::Value::from(true))
            .expect("failed to write cache miss flag");
        rmpv::encode::write_value(buf_ref, &empty_result).expect("failed to write result");
    }

    fn check_result_size() -> bool {
        let mut buf = Vec::with_capacity(100);
        create_result(&mut buf);
        buf.len() == RESULT_SIZE
    }

    debug_assert!(check_result_size());

    static RESULT_BYTES: OnceLock<[u8; RESULT_SIZE]> = OnceLock::new();
    let bytes = RESULT_BYTES.get_or_init(|| {
        let mut res = [0; RESULT_SIZE];
        create_result(&mut res.as_mut_slice());
        res
    });

    Tuple::try_from_slice(bytes.as_slice()).expect("failed to create cache miss tuple")
}

/// Transform result of local Tarantool execution into result structure we'll later
/// show to user. local Tarantool result cames to us in a view of a MessagePack.
/// We want to transofrm it into ConsumerResult (DML) or ProducerResult (DQL).
fn result_to_tuple(
    stream: &mut (impl Read + Sized),
    max_rows: u64,
    format: &StorageReturnFormat,
) -> Result<Box<dyn Any>, SbroadError> {
    let (repacked, rows_len) = match format {
        StorageReturnFormat::DmlRaw => {
            let row_count = parse_dml_result(stream);
            let new_row_count = repack_row_count(row_count);
            (rmpv::Value::Map(vec![new_row_count]), None)
        }
        StorageReturnFormat::DqlRaw | StorageReturnFormat::DqlVshard => {
            let (meta, data) = parse_dql_result(stream);
            let rows_len: u64 = if let rmpv::Value::Array(rows) = &data {
                rows.len() as u64
            } else {
                panic!("Unexpected rows format!");
            };
            let new_meta = repack_meta(meta);
            let new_data = repack_data(data, max_rows)?;

            (rmpv::Value::Map(vec![new_meta, new_data]), Some(rows_len))
        }
    };

    // To pre-allocate the buffer, we need to know the size of the encoded value.
    let mut byte_counter = ByteCounter::default();
    rmpv::encode::write_value(&mut byte_counter, &repacked).expect("failed to write result");
    let mut buf = Vec::with_capacity(byte_counter.bytes());

    let res: Box<dyn Any> = match format {
        StorageReturnFormat::DmlRaw | StorageReturnFormat::DqlRaw => {
            write_array_len(&mut buf, 1).expect("failed to write array length");
            rmpv::encode::write_value(&mut buf, &repacked).expect("failed to write result");
            Box::new(buf)
        }
        StorageReturnFormat::DqlVshard => {
            write_array_len(&mut buf, 3).expect("failed to write array length");
            rmpv::encode::write_value(&mut buf, &rmpv::Value::from(rows_len.unwrap()))
                .expect("failed to write rows count");
            rmpv::encode::write_value(&mut buf, &rmpv::Value::from(false))
                .expect("failed to write cache miss flag");
            let inner = rmpv::Value::Array(vec![repacked]);
            rmpv::encode::write_value(&mut buf, &inner).expect("failed to write result");
            let tup_buf =
                TupleBuffer::try_from_vec(buf).expect("failed to convert to tuple buffer");
            Box::new(Tuple::from(&tup_buf))
        }
    };
    Ok(res)
}

fn encoded_params(params: &[Value]) -> Vec<EncodedValue> {
    params.iter().map(EncodedValue::from).collect()
}

pub fn execute_prepared(
    stmt: &Statement,
    params: &[Value],
    vdbe_max_steps: u64,
    max_rows: u64,
    format: &StorageReturnFormat,
) -> Result<Box<dyn Any>, SbroadError> {
    let encoded_params = encoded_params(params);
    let mut stream = with_su(ADMIN_ID, || {
        stmt.execute_raw(&encoded_params, vdbe_max_steps)
    })??;
    result_to_tuple(&mut stream, max_rows, format)
}

pub fn execute_unprepared(
    query: &str,
    params: &[Value],
    vdbe_max_steps: u64,
    max_rows: u64,
    format: &StorageReturnFormat,
) -> Result<Box<dyn Any>, SbroadError> {
    let encoded_params = encoded_params(params);
    let mut stream = with_su(ADMIN_ID, || {
        prepare_and_execute_raw(query, &encoded_params, vdbe_max_steps)
    })??;
    result_to_tuple(&mut stream, max_rows, format)
}
