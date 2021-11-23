use std::io::Write;

use crate::{
    c_ptr,
    AsLua,
    Lua,
    LuaFunction,
    LuaState,
    Push,
    PushGuard,
    PushOne,
    Void,
};

#[allow(dead_code)]
#[derive(Debug)]
enum ValueOnTheStack {
    Absolute(i32),
    Relative(i32),
}

impl<L: AsLua> Push<L> for ValueOnTheStack {
    type Err = Void;

    fn push_to_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
        let index = match self {
            Self::Absolute(index) | Self::Relative(index) => index,
        };
        unsafe {
            ffi::lua_pushvalue(lua.as_lua(), index);
            Ok(PushGuard::new(lua, 1))
        }
    }
}

impl<L: AsLua> PushOne<L> for ValueOnTheStack {}

pub fn dump_stack_to(lua: impl AsLua, mut out: impl Write) -> std::io::Result<()> {
    let top = unsafe { ffi::lua_gettop(lua.as_lua()) };
    let lua = unsafe { Lua::from_existing_state(lua.as_lua(), false) };
    let f_type: LuaFunction<_> = lua.get("type").unwrap();
    let f_tostring: LuaFunction<_> = lua.get("tostring").unwrap();
    for i in 1..=top {
        let t: String = f_type.call_with_args(ValueOnTheStack::Absolute(i)).unwrap();
        let s: String = f_tostring.call_with_args(ValueOnTheStack::Absolute(i)).unwrap();
        writeln!(out, "{}: {}({})", i, t, s)?;
    }
    Ok(())
}

pub fn dump_stack(lua: impl AsLua) {
    dump_stack_to(lua, std::io::stderr()).unwrap()
}

pub fn dump_stack_raw_to(lua: LuaState, mut out: impl Write) -> std::io::Result<()> {
    unsafe {
        let top = ffi::lua_gettop(lua);
        ffi::lua_getglobal(lua, c_ptr!("type"));
        ffi::lua_getglobal(lua, c_ptr!("tostring"));
        for i in 1..=top {
            ffi::lua_pushvalue(lua, -2);
            ffi::lua_pushvalue(lua, i);
            ffi::lua_pcall(lua, 1, 1, 0);
            let t = std::ffi::CStr::from_ptr(ffi::lua_tostring(lua, -1)).to_owned();
            let t = t.to_string_lossy();
            ffi::lua_pop(lua, 1);

            ffi::lua_pushvalue(lua, -1);
            ffi::lua_pushvalue(lua, i);
            ffi::lua_pcall(lua, 1, 1, 0);
            let s = std::ffi::CStr::from_ptr(ffi::lua_tostring(lua, -1)).to_owned();
            let s = s.to_string_lossy();
            ffi::lua_pop(lua, 1);

            writeln!(out, "{}: {}({})", i, t, s)?;
        }
        ffi::lua_pop(lua, 2);
        Ok(())
    }
}

pub fn dump_stack_raw(lua: LuaState) {
    dump_stack_raw_to(lua, std::io::stderr()).unwrap()
}

