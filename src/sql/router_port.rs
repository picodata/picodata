use ::tarantool::tlua::ffi::lua_State;
use std::io::{Cursor, Write};
use std::ptr::NonNull;

use ::tarantool::error::{TarantoolError, TarantoolErrorCode};
use ::tarantool::ffi::sql::{obuf_append, Obuf, Port, PortC, PortVTable};

use rmp::decode::read_u64;
use rmp::encode::{write_array_len, write_map_len, write_str, write_u64};

/// Result alias using tarantool error type.
pub type Result<T> = std::result::Result<T, ::tarantool::error::Error>;

struct ObufWriter(*mut Obuf);

impl Write for ObufWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        unsafe {
            obuf_append(self.0, buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// We don't want to materialize result set in memory at once.
/// So, we serialize DQL result given an iterator over msgpack packets.
/// The first packet should be metadata.
/// The remaining packets are tuples.
fn serialize_dql_iter<'bytes, I>(iter: &mut I, rows_cnt: usize, w: &mut impl Write) -> Result<()>
where
    I: Iterator<Item = &'bytes [u8]>,
{
    // Take the first msgpack that contains metadata
    let meta_mp = match iter.next() {
        Some(mp) => mp,
        None => {
            return Err(TarantoolError::new(
                TarantoolErrorCode::ProcC,
                "Failed to fetch metadata from DQL port",
            )
            .into())
        }
    };

    // Validate that the first packet is indeed an array.
    // We check only the first byte (MessagePack marker) here – it must
    // encode an array.
    use rmp::Marker;
    match Marker::from_u8(meta_mp[0]) {
        Marker::FixArray(_) | Marker::Array16 | Marker::Array32 => {}
        _ => {
            return Err(TarantoolError::new(
                TarantoolErrorCode::IllegalParams,
                "Invalid metadata detected in DQL port",
            )
            .into())
        }
    }

    // The metadata bytes are forwarded as-is.
    let val_bytes = meta_mp;

    // Write a map with two key-value pairs ("metadata", metadata) and ("rows", array of tuples).
    // Map header with 2 entries
    write_map_len(w, 2)?;

    // First pair: "metadata" -> metadata
    write_str(w, "metadata")?;
    w.write_all(val_bytes)?;

    // Second pair: "rows" -> array of tuples
    write_str(w, "rows")?;
    write_array_len(w, rows_cnt as u32)?;

    for mp in iter {
        w.write_all(mp)?;
    }

    Ok(())
}

/// Serialize EXPLAIN result given an iterator over msgpack packets.
/// Expect exactly one packet that is a string.
fn serialize_explain_iter<'bytes, I>(iter: &mut I, w: &mut impl Write) -> Result<()>
where
    I: Iterator<Item = &'bytes [u8]>,
{
    // Take the first msgpack
    let first_mp = match iter.next() {
        Some(mp) => mp,
        None => {
            return Err(TarantoolError::new(
                TarantoolErrorCode::ProcC,
                "Failed to fetch msgpack from EXPLAIN port",
            )
            .into())
        }
    };

    // There should be no more packets.
    if iter.next().is_some() {
        return Err(TarantoolError::new(
            TarantoolErrorCode::IllegalParams,
            "EXPLAIN port shouldn't contain more than one msgpack",
        )
        .into());
    }

    // Just forward the packet as-is.
    w.write_all(first_mp)?;
    Ok(())
}

/// Encode {"row_count": u64} into `w` as msgpack.
fn write_row_count(row_count: u64, w: &mut impl Write) -> Result<()> {
    write_map_len(w, 1)?;
    write_str(w, "row_count")?;
    write_u64(w, row_count)?;
    Ok(())
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn router_dump_dql(port: *mut Port, out: *mut Obuf) {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    let mut w = ObufWriter(out);

    // A DQL port must always contain at least one tuple with metadata.
    // If it is absent, this state is considered unreachable.
    let size = port_c.size();
    if size == 0 {
        unreachable!("router_dump_dql: DQL port contains no msgpacks (metadata is missing)");
    }

    // The first msgpack in DQL result is metadata, the remaining ones are tuples.
    // We use port_c.size() - 1 to correctly handle number of tuples.
    let rows_cnt = (size - 1) as usize;
    let mut iter = port_c.iter();
    serialize_dql_iter(&mut iter, rows_cnt, &mut w).expect("router_dump_dql failed");
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn router_dump_explain(port: *mut Port, out: *mut Obuf) {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    let mut w = ObufWriter(out);
    let mut iter = port_c.iter();
    serialize_explain_iter(&mut iter, &mut w).expect("router_dump_explain failed");
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn dump_dml(port: *mut Port, out: *mut Obuf) {
    let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
    let mut w = ObufWriter(out);

    let mut iter = port_c.iter();
    let first_mp = iter.next();
    if iter.next().is_some() {
        unreachable!(
            "dump_dml: DML port shouldn't contain more than a single msgpack with amount of modified rows"
        );
    }

    let row_count = if let Some(mp) = first_mp {
        read_u64(&mut Cursor::new(mp)).expect("Failed to decode row count")
    } else {
        unreachable!("dump_dml: DML port contains incorrect msgpack (row count is missing)");
    };

    write_row_count(row_count, &mut w).expect("dump_dml failed");
}

#[no_mangle]
unsafe extern "C" fn dump_lua(_port: *mut Port, _l: *mut lua_State, _is_flat: bool) {
    unimplemented!();
}

pub static ROUTER_DQL_VTAB: PortVTable = PortVTable::new(router_dump_dql, dump_lua);

pub static ROUTER_EXPLAIN_VTAB: PortVTable = PortVTable::new(router_dump_explain, dump_lua);

pub static DML_VTAB: PortVTable = PortVTable::new(dump_dml, dump_lua);

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use rmp::decode::read_u64;
    use std::io::Cursor;
    use std::io::Write;

    // Helper: serialize DML result given a slice of msgpack packets.
    fn serialize_dml_slices(mps: &[&[u8]], w: &mut impl Write) -> Result<()> {
        let row_count = if let Some(first) = mps.first() {
            read_u64(&mut Cursor::new(*first)).map_err(|_| {
                Into::<::tarantool::error::Error>::into(TarantoolError::new(
                    TarantoolErrorCode::ProcC,
                    "Failed to decode row count".to_string(),
                ))
            })?
        } else {
            0
        };

        write_row_count(row_count, w)
    }

    #[test]
    fn dml_basic() {
        // msgpack encoding of unsigned 64-bit integer 3
        const ROWCNT_MP: &[u8] = b"\xcf\x00\x00\x00\x00\x00\x00\x00\x03";

        let mps = vec![ROWCNT_MP];

        let mut vec_out = Vec::new();
        serialize_dml_slices(&mps, &mut vec_out).unwrap();

        let expected: &[u8] = b"\x81\xa9row_count\xcf\x00\x00\x00\x00\x00\x00\x00\x03";

        assert_eq!(vec_out, expected);
    }

    #[test]
    pub fn dml_edge_cases() {
        // case 1: empty (row_count = 0)
        let mut vec1 = Vec::new();
        serialize_dml_slices(&[], &mut vec1).unwrap();
        let exp1: &[u8] = b"\x81\xa9row_count\xcf\x00\x00\x00\x00\x00\x00\x00\x00";
        assert_eq!(vec1, exp1);

        // case 2: row_count = 0 explicitly
        const ZERO_MP: &[u8] = b"\xcf\x00\x00\x00\x00\x00\x00\x00\x00";
        let mps2 = vec![ZERO_MP];
        let mut vec2 = Vec::new();
        serialize_dml_slices(&mps2, &mut vec2).unwrap();
        assert_eq!(vec2, exp1);

        // case 3: large row_count (123456)
        const BIG_MP: &[u8] = b"\xcf\x00\x00\x00\x00\x00\x01\xe2\x40";
        let mps3 = vec![BIG_MP];
        let mut vec3 = Vec::new();
        serialize_dml_slices(&mps3, &mut vec3).unwrap();
        let exp3: &[u8] = b"\x81\xa9row_count\xcf\x00\x00\x00\x00\x00\x01\xe2\x40";
        assert_eq!(vec3, exp3);
    }

    #[test]
    fn dql_basic() {
        // meta_mp = [], tuple_mp = [1,2]
        const META_MP: &[u8] = b"\x90";
        const TUPLE_MP: &[u8] = b"\x92\x01\x02";

        let mps = vec![META_MP, TUPLE_MP];

        let mut vec_out = Vec::new();
        let rows_cnt = mps.len().saturating_sub(1);
        let mut iter = mps.iter().copied();
        serialize_dql_iter(&mut iter, rows_cnt, &mut vec_out).unwrap();

        // expected: {"metadata": [], "rows": [[1,2]]}
        let expected: &[u8] = b"\x82\xa8metadata\x90\xa4rows\x91\x92\x01\x02";

        assert_eq!(vec_out, expected);
    }

    #[test]
    pub fn dql_empty_input() {
        let mut empty_iter = std::iter::empty::<&[u8]>();
        let res = serialize_dql_iter(&mut empty_iter, 0, &mut Vec::new());
        assert!(res.is_err(), "expected error for empty DQL input");
    }

    #[test]
    pub fn dql_invalid_key() {
        // first mp with unexpected key "foo"
        const BAD_MP: &[u8] = b"\x81\xa3foo\xc0";

        let mps = vec![BAD_MP];
        let mut iter = mps.iter().copied();
        let res = serialize_dql_iter(&mut iter, 0, &mut Vec::new());
        assert!(res.is_err(), "expected error for unexpected first key");
    }

    #[test]
    fn dql_two_cols() {
        // meta_mp = [{"name": "col_1", "type": "unsigned"}, {"name": "col_2", "type": "unsigned"}], tuple_mp = [1,2]
        const META_MP: &[u8] = b"\x92\x82\xa4name\xa5col_1\xa4type\xa8unsigned\x82\xa4name\xa5col_2\xa4type\xa8unsigned";
        const TUPLE_MP: &[u8] = b"\x92\x01\x02";

        let mps = vec![META_MP, TUPLE_MP];

        let mut vec_out = Vec::new();
        let rows_cnt = mps.len().saturating_sub(1);
        let mut iter = mps.iter().copied();
        serialize_dql_iter(&mut iter, rows_cnt, &mut vec_out).unwrap();

        // expected: {"metadata": [{"name": "col_1", "type": "unsigned"}, {"name": "col_2", "type": "unsigned"}], "rows": [[1,2]]}
        const EXPECTED: &[u8] = b"\x82\xa8metadata\x92\
              \x82\xa4name\xa5col_1\xa4type\xa8unsigned\
              \x82\xa4name\xa5col_2\xa4type\xa8unsigned\
              \xa4rows\x91\x92\x01\x02";

        assert_eq!(vec_out, EXPECTED);
    }

    #[test]
    fn dql_single_col_multi_rows() {
        // meta_mp = [{"name":"id","type":"integer"}], rows = [ [1], [2], [32] ]

        const META_MP: &[u8] = b"\x91\x82\xa4name\xa2id\xa4type\xa7integer";
        const ROW1_MP: &[u8] = b"\x91\x01";
        const ROW2_MP: &[u8] = b"\x91\x02";
        const ROW3_MP: &[u8] = b"\x91\x20";

        let mps = vec![META_MP, ROW1_MP, ROW2_MP, ROW3_MP];

        let mut vec_out = Vec::new();
        let rows_cnt = mps.len().saturating_sub(1);
        let mut iter = mps.iter().copied();
        serialize_dql_iter(&mut iter, rows_cnt, &mut vec_out).unwrap();

        // expected: {"metadata": [...], "rows": [[1],[2],[32]]}
        const EXPECTED: &[u8] = b"\x82\xa8metadata\x91\x82\xa4name\xa2id\xa4type\xa7integer\xa4rows\x93\x91\x01\x91\x02\x91\x20";

        assert_eq!(vec_out, EXPECTED);
    }

    #[test]
    fn dql_metadata_map_rejected() {
        // First packet uses the incorrect format: {"metadata": []}. It must be rejected.
        const OLD_META_MP: &[u8] = b"\x81\xa8metadata\x90";

        let mps = vec![OLD_META_MP];
        let mut iter = mps.iter().copied();
        let res = serialize_dql_iter(&mut iter, 0, &mut Vec::new());
        assert!(
            res.is_err(),
            "expected error when metadata is wrapped into a map"
        );
    }

    #[test]
    fn dml_decode_error() {
        // not a u64
        let mut mp = Vec::new();
        write_str(&mut mp, "oops").unwrap();
        let mps = vec![mp.as_slice()];
        let res = serialize_dml_slices(&mps, &mut Vec::new());
        assert!(res.is_err(), "Failed to decode row count");
    }

    #[test]
    pub fn explain_basic() {
        // "plan"
        const EXPLAIN_MP: &[u8] = b"\xa4plan";

        let mps = vec![EXPLAIN_MP];

        let mut vec_out = Vec::new();
        let mut iter = mps.iter().copied();
        serialize_explain_iter(&mut iter, &mut vec_out).unwrap();

        assert_eq!(vec_out, EXPLAIN_MP);
    }

    #[test]
    fn explain_multiple_packets() {
        // two packets instead of one
        const EXPLAIN_MP: &[u8] = b"\xa4plan";
        let mps = vec![EXPLAIN_MP, EXPLAIN_MP];
        let mut iter = mps.iter().copied();
        let res = serialize_explain_iter(&mut iter, &mut Vec::new());
        assert!(
            res.is_err(),
            "expected error when EXPLAIN contains multiple packets"
        );
    }
}
