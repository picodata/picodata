use ::tarantool::error::{TarantoolError, TarantoolErrorCode};
use ::tarantool::ffi::sql::{Port, PortC};
use ::tarantool::tlua::ffi as lua;
use ::tarantool::tlua::ffi::lua_State;
use rmp::decode::{read_array_len, read_int, read_map_len, read_marker, read_str_len, RmpRead};
use rmp::Marker;
use std::io::Cursor;
use std::os::raw::c_char;
use std::ptr::NonNull;

pub(crate) unsafe extern "C" fn dispatch_dump_lua(
    port: *mut Port,
    l: *mut lua_State,
    _is_flat: bool,
) {
    let port_c: &mut PortC = NonNull::new_unchecked(port as *mut PortC).as_mut();

    // If the port is empty, we should push nil to Lua stack to resemble
    // an empty result.
    if port_c.size() == 0 {
        lua::lua_pushnil(l);
        return;
    }

    // Inspect the first msgpack packet to determine the port type.
    let mut iter = port_c.iter();
    let first_mp = iter.next().expect("Port must contain at least one msgpack");
    let marker = Marker::from_u8(first_mp[0]);

    match marker {
        Marker::FixStr(_) | Marker::Str8 | Marker::Str16 | Marker::Str32 => {
            // Decode string inline without allocations.
            let mut c = Cursor::new(first_mp);
            let len = read_str_len(&mut c).expect("Failed to read string length") as usize;
            let start = c.position() as usize;
            let bytes = &first_mp[start..start + len];
            lua::lua_pushlstring(l, bytes.as_ptr() as *const c_char, bytes.len());
        }
        Marker::FixPos(_) | Marker::U8 | Marker::U16 | Marker::U32 | Marker::U64 => {
            // Decode affected rows.
            let row_count: u64 = read_int(&mut Cursor::new(first_mp))
                .expect("Failed to decode row_count from DML port");

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
            push_metadata_to_lua(l, first_mp).expect("Failed to push metadata");
            lua::lua_settable(l, -3);

            // Build tuples.
            let rows_cnt = port_c.size() as usize - 1;
            lua::lua_pushstring(l, c"rows".as_ptr() as *const c_char);
            lua::lua_createtable(l, rows_cnt as i32, 0);

            for (idx, mp_bytes) in iter.enumerate() {
                let mut cur = Cursor::new(mp_bytes);
                push_mp_value_to_lua(l, &mut cur).expect("Failed to decode tuple mp");
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

pub(crate) unsafe extern "C" fn execute_dql_dump_lua(
    port: *mut Port,
    l: *mut lua_State,
    _is_flat: bool,
) {
    let port_c: &mut PortC = NonNull::new_unchecked(port as *mut PortC).as_mut();
    if port_c.size() == 0 {
        lua::lua_createtable(l, 0, 1);
        lua::lua_pushstring(l, c"dql".as_ptr() as *const c_char);
        lua::lua_createtable(l, 0, 1);
        lua::lua_pushnil(l);
        lua::lua_rawseti(l, -2, 1);
        lua::lua_settable(l, -3);
        return;
    }

    // Inspect the first msgpack packet to determine the amount of columns in the table.
    let mut iter = port_c.iter();
    let first_mp = iter
        .next()
        .expect("DQL port must contain at least one msgpack");
    let mut cur = Cursor::new(first_mp);
    let columns =
        read_array_len(&mut cur).expect("Failed to read the amount of columns in the tuple") as i32;

    lua::lua_createtable(l, 0, 1);
    lua::lua_pushstring(l, c"dql".as_ptr() as *const c_char);
    lua::lua_createtable(l, columns, port_c.size());

    // First row (already peeked).
    let mut cur = Cursor::new(first_mp);
    push_mp_value_to_lua(l, &mut cur).expect("Failed to push the first row to Lua");
    lua::lua_rawseti(l, -2, 1);

    // Remaining rows.
    let mut idx = 2;
    for mp_bytes in iter {
        let mut cur = Cursor::new(mp_bytes);
        push_mp_value_to_lua(l, &mut cur).expect("Failed to push a row to Lua");
        lua::lua_rawseti(l, -2, idx);
        idx += 1;
    }

    lua::lua_settable(l, -3);
}

pub(crate) unsafe extern "C" fn execute_dml_dump_lua(
    port: *mut Port,
    l: *mut lua_State,
    _is_flat: bool,
) {
    let port_c: &mut PortC = NonNull::new_unchecked(port as *mut PortC).as_mut();
    let first_mp = port_c.iter().next().unwrap_or(b"\xcc\x00");
    let mut cur = Cursor::new(first_mp);
    lua::lua_createtable(l, 0, 1);
    lua::lua_pushstring(l, c"dml".as_ptr() as *const c_char);
    push_mp_value_to_lua(l, &mut cur).expect("Failed to push DML result to Lua");
    lua::lua_settable(l, -3);
}

pub(crate) unsafe extern "C" fn execute_miss_dump_lua(
    _port: *mut Port,
    l: *mut lua_State,
    _is_flat: bool,
) {
    lua::lua_createtable(l, 0, 1);
    lua::lua_pushstring(l, c"miss".as_ptr() as *const c_char);
    lua::lua_pushnil(l);
    lua::lua_settable(l, -3);
}

type Result<T> = std::result::Result<T, ::tarantool::error::Error>;

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

#[allow(clippy::manual_c_str_literals)]
mod tarantool_tests {
    use super::*;
    use std::ffi::c_char;
    use tarantool::tlua::{ffi as lua, AsLua};

    #[tarantool::test]
    fn test_dispatch_dml_dump_lua() {
        // Prepare msgpack with row_count = 5
        let mp = b"\xcc\x05";

        // Create port and add msgpack
        let mut port = Port::new_port_c();
        let port_c = unsafe { port.as_mut_port_c() };
        unsafe { port_c.add_mp(mp) };

        // Create temporary Lua state
        let lua_state = tarantool::tlua::TempLua::new();

        // Call dump_lua
        unsafe {
            dispatch_dump_lua(port_c.as_mut_ptr(), lua_state.as_lua(), false);
        }

        unsafe {
            // Stack: [ result_table ]
            lua::lua_getfield(
                lua_state.as_lua(),
                -1,
                b"row_count\0".as_ptr() as *const c_char,
            );
            let val = lua::lua_tointeger(lua_state.as_lua(), -1);
            assert_eq!(val as u64, 5);

            // Pop value and table
            lua::lua_pop(lua_state.as_lua(), 2);
        }
    }

    #[tarantool::test]
    fn test_dispatch_explain_dump_lua() {
        // Prepare msgpack string "plan"
        const EXPLAIN_MP: &[u8] = b"\xa4plan";

        let mut port = Port::new_port_c();
        let port_c = unsafe { port.as_mut_port_c() };
        unsafe { port_c.add_mp(EXPLAIN_MP) };

        let lua_state = tarantool::tlua::TempLua::new();

        unsafe {
            dispatch_dump_lua(port_c.as_mut_ptr(), lua_state.as_lua(), false);

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
    fn test_dispatch_dql_dump_lua() {
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
            dispatch_dump_lua(port_c.as_mut_ptr(), l, false);

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

    #[tarantool::test]
    fn test_execute_dml_dump_lua() {
        let mp = b"\xcc\x05"; // [5]
        let mut port = Port::new_port_c();
        let port_c = unsafe { port.as_mut_port_c() };
        unsafe { port_c.add_mp(mp) };

        let lua_state = tarantool::tlua::TempLua::new();
        let l = lua_state.as_lua();
        unsafe {
            execute_dml_dump_lua(port_c.as_mut_ptr(), l, false);
        }

        unsafe {
            lua::lua_getfield(l, -1, b"dml\0".as_ptr() as *const c_char);
            assert!(!lua::lua_isnil(l, -1), "dml must exist");
        }
    }
}
