use std::io::{Cursor, Write};
use std::os::raw::c_char;
use std::ptr::NonNull;

use rmp::decode::{read_array_len, read_map_len, read_marker, read_str_len, read_u64, RmpRead};
use rmp::encode::{write_array_len, write_map_len, write_str, write_u64};
use rmp::Marker;

use ::tarantool::error::{TarantoolError, TarantoolErrorCode};
use ::tarantool::ffi::sql::{obuf_append, Obuf, Port, PortC, PortVTable};
use ::tarantool::tlua::ffi as lua;
use ::tarantool::tlua::ffi::lua_State;

/// Result alias using tarantool error type.
pub type Result<T> = std::result::Result<T, ::tarantool::error::Error>;

pub(crate) struct ObufWriter(pub *mut Obuf);

impl Write for ObufWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        unsafe {
            obuf_append(self.0, buf).map_err(|e| std::io::Error::other(e.to_string()))?;
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

/// Recursively read a MessagePack value from the given cursor and push it to the provided Lua
/// state using Tarantool small allocator. The implementation purposefully avoids any heap
/// allocations in Rust, relying instead on Lua to allocate memory for the produced values.
unsafe fn push_mp_value_to_lua(l: *mut lua_State, cur: &mut Cursor<&[u8]>) -> Result<()> {
    let marker = read_marker(cur)?;
    match marker {
        Marker::Null => lua::lua_pushnil(l),
        Marker::True => lua::lua_pushboolean(l, 1),
        Marker::False => lua::lua_pushboolean(l, 0),

        // Positive fixint
        Marker::FixPos(val) => lua::lua_pushinteger(l, val as isize),
        // Negative fixint
        Marker::FixNeg(val) => lua::lua_pushinteger(l, val as isize),

        // Unsigned ints
        Marker::U8 => lua::lua_pushinteger(l, cur.read_data_u8()? as isize),
        Marker::U16 => lua::lua_pushinteger(l, cur.read_data_u16()? as isize),
        Marker::U32 => lua::lua_pushnumber(l, cur.read_data_u32()? as f64),
        Marker::U64 => lua::lua_pushnumber(l, cur.read_data_u64()? as f64),

        // Signed ints
        Marker::I8 => lua::lua_pushinteger(l, cur.read_data_i8()? as isize),
        Marker::I16 => lua::lua_pushinteger(l, cur.read_data_i16()? as isize),
        Marker::I32 => lua::lua_pushinteger(l, cur.read_data_i32()? as isize),
        Marker::I64 => {
            let v = cur.read_data_i64()?;
            lua::lua_pushnumber(l, v as f64);
        }

        // Floats
        Marker::F32 => lua::lua_pushnumber(l, cur.read_data_f32()? as f64),
        Marker::F64 => lua::lua_pushnumber(l, cur.read_data_f64()?),

        // Strings
        Marker::FixStr(len) => {
            let start = cur.position() as usize;
            let bytes = &cur.get_ref()[start..start + len as usize];
            cur.set_position((start + len as usize) as u64);
            lua::lua_pushlstring(l, bytes.as_ptr() as *const c_char, bytes.len());
        }
        Marker::Str8 | Marker::Str16 | Marker::Str32 => {
            let len = match marker {
                Marker::Str8 => cur.read_data_u8()? as usize,
                Marker::Str16 => cur.read_data_u16()? as usize,
                Marker::Str32 => cur.read_data_u32()? as usize,
                _ => unreachable!(),
            };
            let start = cur.position() as usize;
            let bytes = &cur.get_ref()[start..start + len];
            cur.set_position((start + len) as u64);
            lua::lua_pushlstring(l, bytes.as_ptr() as *const c_char, bytes.len());
        }

        // Binary — treat as Lua string as well
        Marker::Bin8 | Marker::Bin16 | Marker::Bin32 => {
            let len = match marker {
                Marker::Bin8 => cur.read_data_u8()? as usize,
                Marker::Bin16 => cur.read_data_u16()? as usize,
                Marker::Bin32 => cur.read_data_u32()? as usize,
                _ => unreachable!(),
            };
            let start = cur.position() as usize;
            let bytes = &cur.get_ref()[start..start + len];
            cur.set_position((start + len) as u64);
            lua::lua_pushlstring(l, bytes.as_ptr() as *const c_char, bytes.len());
        }

        // Arrays
        Marker::FixArray(len) => {
            lua::lua_createtable(l, len as i32, 0);
            for idx in 0..len {
                push_mp_value_to_lua(l, cur)?;
                lua::lua_rawseti(l, -2, idx as i32 + 1);
            }
        }
        Marker::Array16 => {
            let len = cur.read_data_u16()? as usize;
            lua::lua_createtable(l, len as i32, 0);
            for idx in 0..len {
                push_mp_value_to_lua(l, cur)?;
                lua::lua_rawseti(l, -2, idx as i32 + 1);
            }
        }
        Marker::Array32 => {
            let len = cur.read_data_u32()? as usize;
            lua::lua_createtable(l, len as i32, 0);
            for idx in 0..len {
                push_mp_value_to_lua(l, cur)?;
                lua::lua_rawseti(l, -2, idx as i32 + 1);
            }
        }

        // Maps
        Marker::FixMap(len) => {
            lua::lua_createtable(l, 0, len as i32);
            for _ in 0..len {
                // Keys: use only string / integer
                push_mp_value_to_lua(l, cur)?; // key
                push_mp_value_to_lua(l, cur)?; // value
                lua::lua_settable(l, -3);
            }
        }
        Marker::Map16 => {
            let len = cur.read_data_u16()? as usize;
            lua::lua_createtable(l, 0, len as i32);
            for _ in 0..len {
                push_mp_value_to_lua(l, cur)?; // key
                push_mp_value_to_lua(l, cur)?; // value
                lua::lua_settable(l, -3);
            }
        }
        Marker::Map32 => {
            let len = cur.read_data_u32()? as usize;
            lua::lua_createtable(l, 0, len as i32);
            for _ in 0..len {
                push_mp_value_to_lua(l, cur)?; // key
                push_mp_value_to_lua(l, cur)?; // value
                lua::lua_settable(l, -3);
            }
        }

        // Anything else
        _ => {
            return Err(TarantoolError::new(
                TarantoolErrorCode::IllegalParams,
                format!("Unsupported MessagePack type: {marker:?}"),
            )
            .into());
        }
    }

    Ok(())
}

/// Push metadata (an array of maps) stored as MessagePack bytes onto the Lua stack.
unsafe fn push_metadata_to_lua(l: *mut lua_State, metadata_mp: &[u8]) -> Result<()> {
    let mut cur = Cursor::new(metadata_mp);
    // Expect an array.
    let len = read_array_len(&mut cur)? as usize;
    lua::lua_createtable(l, len as i32, 0);
    for idx in 0..len {
        // Each element is a map.
        let map_len = read_map_len(&mut cur)? as usize;
        lua::lua_createtable(l, 0, map_len as i32);
        for _ in 0..map_len {
            // Push key
            push_mp_value_to_lua(l, &mut cur)?;
            // Push value
            push_mp_value_to_lua(l, &mut cur)?;
            // table[key] = value
            lua::lua_settable(l, -3);
        }
        // rows[idx] = column_map
        lua::lua_rawseti(l, -2, idx as i32 + 1);
    }
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

/// # Safety
#[no_mangle]
unsafe extern "C" fn dump_lua(port: *mut Port, l: *mut lua_State, _is_flat: bool) {
    let port_c: &mut PortC = NonNull::new_unchecked(port as *mut PortC).as_mut();

    // If the port is empty, we should push nil to Lua stack to resemble
    // an empty result.
    if port_c.size() == 0 {
        lua::lua_pushnil(l);
        return;
    }

    // Inspect the first msgpack packet to determine the port type.
    let mut iter = port_c.iter();
    let first_mp = iter
        .next()
        .expect("dump_lua: router port must contain at least one msgpack");
    let marker = Marker::from_u8(first_mp[0]);

    match marker {
        Marker::FixStr(_) | Marker::Str8 | Marker::Str16 | Marker::Str32 => {
            // Decode string inline without allocations.
            let mut c = Cursor::new(first_mp);
            let len = read_str_len(&mut c).expect("dump_lua: failed to read str len") as usize;
            let start = c.position() as usize;
            let bytes = &first_mp[start..start + len];
            lua::lua_pushlstring(l, bytes.as_ptr() as *const c_char, bytes.len());
        }
        Marker::FixPos(_) | Marker::U8 | Marker::U16 | Marker::U32 | Marker::U64 => {
            // Decode affected rows.
            let row_count = read_u64(&mut Cursor::new(first_mp))
                .expect("dump_lua: failed to decode row_count from DML port");

            // result = {"row_count": u64}
            lua::lua_createtable(l, 0, 1);
            lua::lua_pushinteger(l, row_count as isize);
            lua::lua_setfield(l, -2, c"row_count".as_ptr() as *const c_char);
        }
        Marker::FixArray(_) | Marker::Array16 | Marker::Array32 => {
            // Build the resulting Lua table.
            lua::lua_createtable(l, 0, 2);

            // Push metadata field.
            lua::lua_pushstring(l, c"metadata".as_ptr() as *const c_char);
            push_metadata_to_lua(l, first_mp).expect("dump_lua: failed to push metadata");
            lua::lua_settable(l, -3);

            // Build tuples.
            let rows_cnt = port_c.size() as usize - 1;
            lua::lua_pushstring(l, c"rows".as_ptr() as *const c_char);
            lua::lua_createtable(l, rows_cnt as i32, 0);

            for (idx, mp_bytes) in iter.enumerate() {
                let mut cur = Cursor::new(mp_bytes);
                push_mp_value_to_lua(l, &mut cur).expect("dump_lua: failed to decode tuple mp");
                lua::lua_rawseti(l, -2, idx as i32 + 1);
            }

            // Set tuples into result table.
            lua::lua_settable(l, -3);
        }
        _ => {
            lua::lua_pushnil(l);
        }
    }
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

mod tarantool_tests {
    #![allow(clippy::manual_c_str_literals)]
    use super::*;

    #[tarantool::test]
    fn lua_dump_dml() {
        use rmp::encode::write_u64;
        use std::ffi::c_char;
        use tarantool::tlua::{ffi as lua, AsLua};

        // Prepare msgpack with row_count = 5
        let row_count: u64 = 5;
        let mut mp_buf = Vec::new();
        write_u64(&mut mp_buf, row_count).unwrap();

        // Create port and add msgpack
        let mut port = Port::new_port_c();
        let port_c = unsafe { port.as_mut_port_c() };
        unsafe { port_c.add_mp(&mp_buf) };

        // Create temporary Lua state
        let lua_state = tarantool::tlua::TempLua::new();

        // Call dump_lua
        unsafe {
            dump_lua(port_c.as_mut_ptr(), lua_state.as_lua(), false);
        }

        unsafe {
            // Stack: [ result_table ]
            lua::lua_getfield(
                lua_state.as_lua(),
                -1,
                b"row_count\0".as_ptr() as *const c_char,
            );
            let val = lua::lua_tointeger(lua_state.as_lua(), -1);
            assert_eq!(val as u64, row_count);

            // Pop value and table
            lua::lua_pop(lua_state.as_lua(), 2);
        }
    }

    #[tarantool::test]
    fn lua_dump_explain() {
        use tarantool::tlua::{ffi as lua, AsLua};

        // Prepare msgpack string "plan"
        const EXPLAIN_MP: &[u8] = b"\xa4plan";

        let mut port = Port::new_port_c();
        let port_c = unsafe { port.as_mut_port_c() };
        unsafe { port_c.add_mp(EXPLAIN_MP) };

        let lua_state = tarantool::tlua::TempLua::new();

        unsafe {
            dump_lua(port_c.as_mut_ptr(), lua_state.as_lua(), false);

            // Now top of stack should be string "plan".
            let mut len: usize = 0;
            let s_ptr = lua::lua_tolstring(lua_state.as_lua(), -1, &mut len as *mut usize);
            let slice = std::slice::from_raw_parts(s_ptr as *const u8, len);
            let s = std::str::from_utf8(slice).unwrap();
            assert_eq!(s, "plan");

            // Pop string.
            lua::lua_pop(lua_state.as_lua(), 1);
        }
    }

    #[tarantool::test]
    fn dump_lua_dql() {
        use std::ffi::c_char;
        use tarantool::tlua::{ffi as lua, AsLua};

        // [{"name":"id","type":"integer"}]
        const META_MP: &[u8] = b"\x91\x82\xa4name\xa2id\xa4type\xa7integer";
        // [7]
        const ROW_MP: &[u8] = b"\x91\x07";

        let mut port = Port::new_port_c();
        let port_c = unsafe { port.as_mut_port_c() };

        unsafe {
            port_c.add_mp(META_MP);
            port_c.add_mp(ROW_MP);
        }

        let lua_state = tarantool::tlua::TempLua::new();
        let l = lua_state.as_lua();

        unsafe {
            dump_lua(port_c.as_mut_ptr(), l, false);

            // Check metadata exists
            lua::lua_getfield(l, -1, b"metadata\0".as_ptr() as *const c_char);
            assert!(!lua::lua_isnil(l, -1), "metadata must exist");
            lua::lua_pop(l, 1);

            // Check rows and value
            lua::lua_getfield(l, -1, b"rows\0".as_ptr() as *const c_char);
            assert!(!lua::lua_isnil(l, -1), "rows must exist");

            // Get first row
            lua::lua_rawgeti(l, -1, 1);
            assert!(!lua::lua_isnil(l, -1), "first row must exist");

            // Get first value from row
            lua::lua_rawgeti(l, -1, 1);
            assert_eq!(lua::lua_tointeger(l, -1), 7, "row value must be 7");
            lua::lua_pop(l, 1);

            lua::lua_pop(l, 3);
        }
    }
}
