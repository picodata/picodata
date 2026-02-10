use ::tarantool::datetime::Datetime;
use ::tarantool::error::{Error as TntError, TarantoolError, TarantoolErrorCode};
use ::tarantool::ffi::sql::{ibuf_reinit, Ibuf, Port, PortC};
use ::tarantool::msgpack::ExtStruct;
use ::tarantool::tlua;
use ::tarantool::tlua::ffi as lua;
use ::tarantool::tlua::ffi::lua_State;
use ::tarantool::tlua::{
    CDataOnStack, LuaError, LuaFunction, LuaRead, LuaState, LuaTable, LuaThread, PushGuard,
    PushInto, StringInLua, Void,
};
use rmp::decode::{read_array_len, read_int, read_marker, RmpRead};
use rmp::Marker;
use sql_protocol::query_plan::ExplainIter;
use std::fmt::Write;
use std::io::Cursor;
use std::os::raw::c_char;
use std::ptr::NonNull;
use std::rc::Rc;

pub(crate) unsafe extern "C" fn dispatch_explain_dump_lua(
    port: *mut Port,
    l: *mut lua_State,
    _is_flat: bool,
) {
    let port_c: &mut PortC = NonNull::new_unchecked(port as *mut PortC).as_mut();

    // It should never happen that the port is empty, but let's be defensive.
    if port_c.size() == 0 {
        lua::lua_pushnil(l);
        return;
    }

    let len = port_c.size() as usize;
    lua::lua_createtable(l, len as i32, 0);

    let iter = port_c.iter();

    for (idx, mp_bytes) in iter.enumerate() {
        let mut cur = Cursor::new(mp_bytes);
        push_mp_value_to_lua(l, &mut cur).unwrap_or_else(|_| {
            panic!(
                "Failed to decode explain row, msgpack: {}",
                escape_bytes(mp_bytes)
            )
        });
        lua::lua_rawseti(l, -2, idx as i32 + 1);
    }
}

pub(crate) unsafe extern "C" fn dispatch_query_plan_dump_lua(
    port: *mut Port,
    l: *mut lua_State,
    _is_flat: bool,
) {
    let port_c: &mut PortC = NonNull::new_unchecked(port as *mut PortC).as_mut();

    // It should never happen that the port is empty, but let's be defensive.
    if port_c.size() == 0 {
        lua::lua_pushnil(l);
        return;
    }

    lua::lua_createtable(l, 0, 0);
    let mut idx: i32 = 0;

    let explain: Vec<String> = ExplainIter::new(port_c.iter()).collect();
    for line in explain.iter().flat_map(|s| s.split('\n')) {
        lua::lua_pushlstring(l, line.as_ptr() as *const c_char, line.len());
        idx += 1;
        lua::lua_rawseti(l, -2, idx);
    }
}

pub(crate) unsafe extern "C" fn dispatch_dml_dump_lua(
    port: *mut Port,
    l: *mut lua_State,
    _is_flat: bool,
) {
    let port_c: &mut PortC = NonNull::new_unchecked(port as *mut PortC).as_mut();

    // It should never happen that the port is empty, but let's be defensive.
    if port_c.size() == 0 {
        lua::lua_pushnil(l);
        return;
    }

    let mut iter = port_c.iter();
    let mp = iter.next().expect("DML port must contain a single msgpack");

    // Decode affected rows.
    let row_count: u64 = read_int(&mut Cursor::new(mp)).unwrap_or_else(|_| {
        panic!(
            "Failed to decode changed rows from DML port, msgpack: {}",
            escape_bytes(mp)
        )
    });

    // result = {"row_count": u64}
    lua::lua_createtable(l, 0, 1);
    lua::luaL_pushuint64(l, row_count);
    lua::lua_setfield(l, -2, c"row_count".as_ptr() as *const c_char);
}

pub(crate) unsafe extern "C" fn dispatch_dql_dump_lua(
    port: *mut Port,
    l: *mut lua_State,
    _is_flat: bool,
) {
    let port_c: &mut PortC = NonNull::new_unchecked(port as *mut PortC).as_mut();

    // It should never happen that the port is empty, but let's be defensive.
    if port_c.size() == 0 {
        lua::lua_pushnil(l);
        return;
    }

    let mut iter = port_c.iter();
    let first_mp = iter.next().expect("DML port must contain a single msgpack");

    // Build the resulting Lua table.
    lua::lua_createtable(l, 0, 2);

    // Push metadata field.
    lua::lua_pushstring(l, c"metadata".as_ptr() as *const c_char);
    push_metadata_to_lua(l, first_mp).unwrap_or_else(|_| {
        panic!(
            "Failed to push metadata, msgpack: {}",
            escape_bytes(first_mp)
        )
    });
    lua::lua_settable(l, -3);

    // Build tuples.
    let rows_cnt = port_c.size() as usize - 1;
    lua::lua_pushstring(l, c"rows".as_ptr() as *const c_char);
    lua::lua_createtable(l, rows_cnt as i32, 0);

    for (idx, mp_bytes) in iter.enumerate() {
        let mut cur = Cursor::new(mp_bytes);
        push_mp_value_to_lua(l, &mut cur).unwrap_or_else(|_| {
            panic!(
                "Failed to decode tuple, msgpack: {}",
                escape_bytes(mp_bytes)
            )
        });
        lua::lua_rawseti(l, -2, idx as i32 + 1);
    }

    // Set tuples into result table.
    lua::lua_settable(l, -3);
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
    let first_mp = port_c.iter().next().unwrap_or(b"\x91\xcc\x00");
    let mut cur = Cursor::new(first_mp);
    let _ = read_array_len(&mut cur).unwrap_or_else(|_| {
        panic!(
            "Failed to decode changed rows, msgpack: {}",
            escape_bytes(first_mp)
        )
    });
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
    lua::lua_pushinteger(l, 0);
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
        // Negative fixint

        // Unsigned ints
        Marker::FixPos(val) => lua::luaL_pushuint64(l, val as u64),
        Marker::U8 => lua::luaL_pushuint64(l, cur.read_data_u8()? as u64),
        Marker::U16 => lua::luaL_pushuint64(l, cur.read_data_u16()? as u64),
        Marker::U32 => lua::luaL_pushuint64(l, cur.read_data_u32()? as u64),
        Marker::U64 => lua::luaL_pushuint64(l, cur.read_data_u64()?),

        // Signed ints
        Marker::FixNeg(val) => lua::luaL_pushint64(l, val as i64),
        Marker::I8 => lua::luaL_pushint64(l, cur.read_data_i8()? as i64),
        Marker::I16 => lua::luaL_pushint64(l, cur.read_data_i16()? as i64),
        Marker::I32 => lua::luaL_pushint64(l, cur.read_data_i32()? as i64),
        Marker::I64 => lua::luaL_pushint64(l, cur.read_data_i64()?),

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
        Marker::FixArray(_) | Marker::Array16 | Marker::Array32 => {
            let len = match marker {
                Marker::FixArray(len) => len as usize,
                Marker::Array16 => cur.read_data_u16()? as usize,
                Marker::Array32 => cur.read_data_u32()? as usize,
                _ => unreachable!(),
            };
            lua::lua_createtable(l, len as i32, 0);
            for idx in 0..len {
                push_mp_value_to_lua(l, cur)?;
                lua::lua_rawseti(l, -2, idx as i32 + 1);
            }

            // Set a serializing metatable.
            lua::lua_createtable(l, 0, 0);
            lua::lua_pushstring(l, c"__serialize".as_ptr() as *const c_char);
            lua::lua_pushstring(l, c"seq".as_ptr() as *const c_char);
            lua::lua_settable(l, -3);
            lua::lua_setmetatable(l, -2);
        }

        // Maps
        Marker::FixMap(_) | Marker::Map16 | Marker::Map32 => {
            let len = match marker {
                Marker::FixMap(len) => len as usize,
                Marker::Map16 => cur.read_data_u16()? as usize,
                Marker::Map32 => cur.read_data_u32()? as usize,
                _ => unreachable!(),
            };
            lua::lua_createtable(l, 0, len as i32);
            for _ in 0..len {
                push_mp_value_to_lua(l, cur)?; // key
                push_mp_value_to_lua(l, cur)?; // value
                lua::lua_settable(l, -3);
            }

            // Set a serializing metatable.
            lua::lua_createtable(l, 0, 0);
            lua::lua_pushstring(l, c"__serialize".as_ptr() as *const c_char);
            lua::lua_pushstring(l, c"map".as_ptr() as *const c_char);
            lua::lua_settable(l, -3);
            lua::lua_setmetatable(l, -2);
        }

        Marker::FixExt1
        | Marker::FixExt2
        | Marker::FixExt4
        | Marker::FixExt8
        | Marker::FixExt16
        | Marker::Ext8
        | Marker::Ext16
        | Marker::Ext32 => {
            let start = cur.position() as usize - 1;
            let len = read_ext_len(marker, cur)?;
            let ext_type = cur.read_data_i8()?;
            let data_start = cur.position() as usize;
            let end = data_start + len;
            let bytes = &cur.get_ref()[data_start..end];
            cur.set_position(end as u64);

            match ext_type {
                1 => {
                    // MP_DECIMAL
                    extern "C" {
                        fn luaT_newdecimal(L: *mut lua_State) -> *mut std::ffi::c_void;
                        fn decimal_unpack(
                            data: *mut *const u8,
                            len: u32,
                            dec: *mut std::ffi::c_void,
                        ) -> *mut std::ffi::c_void;
                    }

                    let dec = luaT_newdecimal(l);
                    let mut p = bytes.as_ptr();

                    if decimal_unpack(&mut p, len as u32, dec).is_null() {
                        return Err(TarantoolError::new(
                            TarantoolErrorCode::ProcC,
                            "Failed to decode decimal".to_string(),
                        )
                        .into());
                    }
                }
                2 => {
                    // MP_UUID
                    extern "C" {
                        fn luaT_newuuid(L: *mut lua_State) -> *mut std::ffi::c_void;
                        fn mp_decode_uuid(
                            data: *mut *const u8,
                            uuid: *mut std::ffi::c_void,
                        ) -> *mut std::ffi::c_void;
                    }
                    let uuid = luaT_newuuid(l);
                    // mp_decode_uuid consumes the buffer pointer, so give it a local.
                    let mut ptr = cur.get_ref()[start..end].as_ptr();
                    if mp_decode_uuid(&mut ptr, uuid).is_null() {
                        return Err(TarantoolError::new(
                            TarantoolErrorCode::ProcC,
                            "Failed to decode UUID".to_string(),
                        )
                        .into());
                    }
                }

                4 => {
                    // MP_DATETIME
                    extern "C" {
                        fn luaT_pushdatetime(
                            L: *mut lua_State,
                            dt: *const ::tarantool::ffi::datetime::datetime,
                        ) -> *mut std::ffi::c_void;
                    }

                    // Decode datetime using Rust implementation
                    let ext = ExtStruct::new(::tarantool::ffi::datetime::MP_DATETIME, bytes);
                    let dt = match Datetime::try_from(ext) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(TarantoolError::new(
                                TarantoolErrorCode::ProcC,
                                format!("Failed to decode datetime: {e}"),
                            )
                            .into())
                        }
                    };

                    let raw = dt.as_ffi_dt();
                    luaT_pushdatetime(l, &raw);
                }

                6 => {
                    // MP_INTERVAL
                    extern "C" {
                        fn luaT_newinterval(L: *mut lua_State) -> *mut std::ffi::c_void;
                        fn interval_unpack(
                            data: *const u8,
                            len: u32,
                            itv: *mut std::ffi::c_void,
                        ) -> *mut std::ffi::c_void;
                    }
                    let itv = luaT_newinterval(l);
                    if interval_unpack(bytes.as_ptr(), len as u32, itv).is_null() {
                        return Err(TarantoolError::new(
                            TarantoolErrorCode::ProcC,
                            "Failed to decode interval".to_string(),
                        )
                        .into());
                    }
                }
                _ => {
                    return Err(TarantoolError::new(
                        TarantoolErrorCode::Unsupported,
                        format!("Unsupported msgpack extension type: {ext_type:?}"),
                    )
                    .into());
                }
            }
        }

        // Anything else
        _ => {
            return Err(TarantoolError::new(
                TarantoolErrorCode::IllegalParams,
                format!("Unsupported msgpack type: {marker:?}"),
            )
            .into());
        }
    }

    Ok(())
}

fn read_ext_len(marker: Marker, cur: &mut Cursor<&[u8]>) -> Result<usize> {
    Ok(match marker {
        Marker::FixExt1 => 1,
        Marker::FixExt2 => 2,
        Marker::FixExt4 => 4,
        Marker::FixExt8 => 8,
        Marker::FixExt16 => 16,
        Marker::Ext8 => cur.read_data_u8()? as usize,
        Marker::Ext16 => cur.read_data_u16()? as usize,
        Marker::Ext32 => cur.read_data_u32()? as usize,
        _ => unreachable!(),
    })
}

/// Push metadata (an array of maps) stored as MessagePack bytes onto the Lua stack.
unsafe fn push_metadata_to_lua(l: *mut lua_State, metadata_mp: &[u8]) -> Result<()> {
    let mut cur = Cursor::new(metadata_mp);
    // Expect an array.
    let len = read_array_len(&mut cur)? as usize;
    lua::lua_createtable(l, len as i32, 0);
    for idx in 0..len {
        // Expect that each element is a map with correct
        // metadata for serializing format.
        push_mp_value_to_lua(l, &mut cur)?;
        lua::lua_rawseti(l, -2, idx as i32 + 1);
    }
    Ok(())
}

pub(crate) type IbufTable<'lua> = LuaTable<PushGuard<DispatchFunction<'lua>>>;
pub(crate) type DispatchFunction<'lua> =
    LuaFunction<PushGuard<LuaTable<PushGuard<LuaTable<PushGuard<&'lua LuaThread>>>>>>;
pub(crate) type StorageFunction<'lua> = LuaFunction<
    PushGuard<LuaTable<PushGuard<LuaTable<PushGuard<LuaTable<PushGuard<&'lua LuaThread>>>>>>>,
>;

pub(crate) struct LuaInputBuffer<'lua> {
    // Input buffer pointer.
    ibuf_ptr: *mut Ibuf,
    // Reference to lua table, where ibuf was created (destruction guard).
    _guard: Rc<IbufTable<'lua>>,
}

impl LuaInputBuffer<'_> {
    pub(crate) fn data(&self) -> Result<&[u8]> {
        let ibuf: &Ibuf = unsafe {
            NonNull::new(self.ibuf_ptr)
                .ok_or_else(|| {
                    TarantoolError::new(TarantoolErrorCode::IllegalParams, "Ibuf pointer is null")
                })?
                .as_ref()
        };
        let data = unsafe {
            std::slice::from_raw_parts(ibuf.rpos, ibuf.wpos.offset_from(ibuf.rpos) as usize)
        };
        Ok(data)
    }
}

impl Drop for LuaInputBuffer<'_> {
    fn drop(&mut self) {
        unsafe {
            ibuf_reinit(self.ibuf_ptr);
        }
    }
}

pub(crate) fn lua_decode_rs_ibufs<'lua>(
    table: &Rc<IbufTable<'lua>>,
    len: usize,
) -> Result<Vec<(String, LuaInputBuffer<'lua>)>> {
    let mut ibufs = Vec::with_capacity(len);
    let iter = table.iter::<StringInLua<_>, CDataOnStack<_>>();
    for item in iter {
        let (rs, mut ibuf_raw) = item.map_err(|e| TntError::other(format!("{e}")))?;
        let ibuf = LuaInputBuffer {
            ibuf_ptr: ibuf_raw.data_mut().as_mut_ptr() as *mut Ibuf,
            _guard: Rc::clone(table),
        };
        ibufs.push(((*rs).to_string(), ibuf));
    }
    Ok(ibufs)
}

pub(crate) fn lua_decode_ibufs<'lua>(
    table: &Rc<IbufTable<'lua>>,
    len: usize,
) -> Result<Vec<LuaInputBuffer<'lua>>> {
    let mut ibufs = Vec::with_capacity(len);
    let iter = table.iter::<i32, CDataOnStack<_>>();
    for item in iter {
        let (_, mut ibuf_raw) = item.map_err(|e| TntError::other(format!("{e}")))?;
        let ibuf = LuaInputBuffer {
            ibuf_ptr: ibuf_raw.data_mut().as_mut_ptr() as *mut Ibuf,
            _guard: Rc::clone(table),
        };
        ibufs.push(ibuf);
    }
    Ok(ibufs)
}

pub(crate) fn lua_single_plan_dispatch<'lua, T>(
    lua: &'lua LuaThread,
    args: T,
    replicasets: &[String],
    timeout: u64,
    tier: Option<&str>,
    read_preference: String,
    do_two_step: bool,
) -> Result<Rc<IbufTable<'lua>>>
where
    T: PushInto<LuaState>,
    T::Err: std::fmt::Debug,
    Void: From<<T as PushInto<*mut lua_State>>::Err>,
{
    let name = "single_plan_dispatch";
    let tier = tier.map(|s| s.to_string());
    let func = dispatch_get_func(lua, name)?;

    let call_res = func.into_call_with_args::<LuaTable<_>, _>((
        args,
        replicasets,
        timeout,
        tier,
        read_preference,
        do_two_step,
    ));
    match call_res {
        Ok(v) => Ok(Rc::new(v)),
        Err(e) => Err(TarantoolError::new(
            TarantoolErrorCode::ProcLua,
            format!("{}", LuaError::from(e)),
        )
        .into()),
    }
}

pub(crate) fn lua_custom_plan_dispatch<'lua, T>(
    lua: &'lua LuaThread,
    args: T,
    timeout: u64,
    tier: Option<&str>,
    read_preference: String,
    do_two_step: bool,
) -> Result<Rc<IbufTable<'lua>>>
where
    T: PushInto<LuaState>,
    T::Err: std::fmt::Debug,
    Void: From<<T as PushInto<*mut lua_State>>::Err>,
{
    let name = "custom_plan_dispatch";
    let tier = tier.map(|s| s.to_string());
    let func = dispatch_get_func(lua, name)?;

    let call_res = func.into_call_with_args::<IbufTable, _>((
        args,
        timeout,
        tier,
        read_preference,
        do_two_step,
    ));
    match call_res {
        Ok(v) => Ok(Rc::new(v)),
        Err(e) => Err(TarantoolError::new(
            TarantoolErrorCode::ProcLua,
            format!("{}", LuaError::from(e)),
        )
        .into()),
    }
}

pub(crate) fn bucket_into_rs(
    lua: &LuaThread,
    bucket_id: u64,
    tier: Option<&str>,
) -> Result<String> {
    let name = "bucket_into_rs";
    let func = dispatch_get_func(lua, name)?;

    match func.call_with_args((bucket_id, tier)) {
        Ok(rs) => Ok(rs),
        Err(e) => Err(TarantoolError::new(
            TarantoolErrorCode::ProcLua,
            format!("{}", LuaError::from(e)),
        )
        .into()),
    }
}

pub(crate) fn reference_add(rid: i64, sid: &str, timeout: f64) -> Result<()> {
    let lua = tarantool::lua_state();
    let func = storage_get_func(&lua, "add")?;

    match func.call_with_args::<(Option<bool>, Option<VshardError>), _>((rid, sid, timeout)) {
        Ok((Some(_), _)) => Ok(()),
        Ok((None, Some(err))) => Err(err.to_tarantool_error().into()),
        Ok((None, None)) => Err(TarantoolError::new(
            TarantoolErrorCode::ProcLua,
            "Adding a storage reference: unexpected empty result from vshard".to_string(),
        )
        .into()),
        Err(e) => Err(TarantoolError::new(
            TarantoolErrorCode::ProcLua,
            format!("{}", LuaError::from(e)),
        )
        .into()),
    }
}

pub(crate) fn reference_use(rid: i64, sid: &str) -> Result<()> {
    let lua = tarantool::lua_state();
    let func = storage_get_func(&lua, "use")?;

    match func.call_with_args::<(Option<bool>, Option<VshardError>), _>((rid, sid)) {
        Ok((Some(_), _)) => Ok(()),
        Ok((None, Some(err))) => Err(err.to_tarantool_error().into()),
        Ok((None, None)) => Err(TarantoolError::new(
            TarantoolErrorCode::ProcLua,
            "Using a storage reference: unexpected empty result from vshard".to_string(),
        )
        .into()),
        Err(e) => Err(TarantoolError::new(
            TarantoolErrorCode::ProcLua,
            format!("{}", LuaError::from(e)),
        )
        .into()),
    }
}

pub(crate) fn reference_del(rid: i64, sid: &str) -> Result<()> {
    let lua = tarantool::lua_state();
    let func = storage_get_func(&lua, "del")?;

    match func.call_with_args((rid, sid)) {
        Ok(()) => Ok(()),
        Err(e) => Err(TarantoolError::new(
            TarantoolErrorCode::ProcLua,
            format!("{}", LuaError::from(e)),
        )
        .into()),
    }
}

pub(crate) fn lua_query_metadata<'lua>(
    lua: &'lua LuaThread,
    tier: &str,
    replicaset: &str,
    instance: &str,
    request_id: &str,
    plan_id: u64,
    timeout: f64,
) -> Result<Rc<IbufTable<'lua>>> {
    let func = dispatch_get_func(lua, "query_metadata")?;

    let call_res = func.into_call_with_args::<IbufTable, _>((
        tier, replicaset, instance, request_id, plan_id, timeout,
    ));
    match call_res {
        Ok(table) => Ok(Rc::new(table)),
        Err(e) => Err(TarantoolError::new(
            TarantoolErrorCode::ProcLua,
            format!("{}", LuaError::from(e)),
        )
        .into()),
    }
}

#[derive(Debug, LuaRead)]
struct VshardError {
    message: String,
    r#type: String,
    code: i64,
    name: String,
}

impl VshardError {
    fn to_tarantool_error(&self) -> TarantoolError {
        TarantoolError::new(
            TarantoolErrorCode::ProcLua,
            format!(
                "vshard error: {} (type: {}, code: {}, name: {})",
                self.message, self.r#type, self.code, self.name
            ),
        )
    }
}

fn storage_get_func<'lua>(lua: &'lua LuaThread, name: &str) -> Result<StorageFunction<'lua>> {
    let Some(pico): Option<LuaTable<_>> = lua.get("pico") else {
        return Err(TarantoolError::new(TarantoolErrorCode::NoSuchModule, "no module pico").into());
    };
    let dispatch: LuaTable<_> = match pico.into_get("dispatch") {
        Ok(table) => table,
        Err(e) => {
            return Err(TarantoolError::new(
                TarantoolErrorCode::NoSuchModule,
                format!("no module pico.dispatch: {e:?}"),
            )
            .into());
        }
    };
    let lref: LuaTable<_> = match dispatch.into_get("lref") {
        Ok(table) => table,
        Err(e) => {
            return Err(TarantoolError::new(
                TarantoolErrorCode::NoSuchModule,
                format!("no module pico.dispatch.lref: {e:?}"),
            )
            .into());
        }
    };
    let func: StorageFunction = match lref.into_get(name) {
        Ok(f) => f,
        Err(e) => {
            return Err(TarantoolError::new(
                TarantoolErrorCode::NoSuchFunction,
                format!("no function '{name}': {e:?}"),
            )
            .into());
        }
    };
    Ok(func)
}

fn dispatch_get_func<'lua>(lua: &'lua LuaThread, name: &str) -> Result<DispatchFunction<'lua>> {
    let Some(pico): Option<LuaTable<_>> = lua.get("pico") else {
        return Err(TarantoolError::new(TarantoolErrorCode::NoSuchModule, "no module pico").into());
    };
    let dispatch: LuaTable<_> = match pico.into_get("dispatch") {
        Ok(table) => table,
        Err(e) => {
            return Err(TarantoolError::new(
                TarantoolErrorCode::NoSuchModule,
                format!("no module pico.dispatch: {e:?}"),
            )
            .into());
        }
    };
    let func: DispatchFunction = match dispatch.into_get(name) {
        Ok(f) => f,
        Err(e) => {
            return Err(TarantoolError::new(
                TarantoolErrorCode::NoSuchFunction,
                format!("no function '{name}': {e:?}"),
            )
            .into());
        }
    };
    Ok(func)
}

pub(crate) fn escape_bytes(data: &[u8]) -> String {
    let mut out = String::from("b\"");
    for &b in data {
        if b.is_ascii_graphic() || b == b' ' {
            out.push(b as char);
        } else {
            write!(&mut out, "\\x{b:02x}").unwrap();
        }
    }
    out.push('"');
    out
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
            dispatch_dml_dump_lua(port_c.as_mut_ptr(), lua_state.as_lua(), false);
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
        let l = lua_state.as_lua();

        unsafe {
            dispatch_explain_dump_lua(port_c.as_mut_ptr(), l, false);

            assert!(!lua::lua_isnil(l, -1), "Explain row must exist");
            lua::lua_rawgeti(l, -1, 1);
            let s_ptr = lua::lua_tostring(l, -1);
            let s = std::ffi::CStr::from_ptr(s_ptr)
                .to_str()
                .expect("Failed to convert CStr to str");
            assert_eq!(s, "plan", "Expected 'plan' string");

            lua::lua_pop(l, 1);
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
            dispatch_dql_dump_lua(port_c.as_mut_ptr(), l, false);

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
        let mp = b"\x91\xcc\x05"; // [5]
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
