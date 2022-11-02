use std::io::Write;

use crate::{c_ptr, ffi, AsLua, LuaState, Push, PushGuard, PushOne, Void};

#[allow(dead_code)]
#[derive(Debug)]
enum ValueOnTheStack {
    Absolute(i32),
    Relative(i32),
}

impl<L: AsLua> Push<L> for ValueOnTheStack {
    type Err = Void;

    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
        let index = match self {
            Self::Absolute(index) | Self::Relative(index) => index,
        };
        unsafe {
            ffi::lua_pushvalue(lua.as_lua(), *index);
            Ok(PushGuard::new(lua, 1))
        }
    }
}

impl<L: AsLua> PushOne<L> for ValueOnTheStack {}

#[allow(clippy::missing_safety_doc)]
pub unsafe fn dump_stack_raw_to(lua: LuaState, mut out: impl Write) -> std::io::Result<()> {
    let top = ffi::lua_gettop(lua);
    ffi::luaopen_base(lua);
    ffi::lua_getglobal(lua, c_ptr!("type"));
    ffi::lua_getglobal(lua, c_ptr!("tostring"));
    for i in 1..=top {
        ffi::lua_pushvalue(lua, -2);
        ffi::lua_pushvalue(lua, i);
        assert_eq!(ffi::lua_pcall(lua, 1, 1, 0), 0);
        let t = std::ffi::CStr::from_ptr(ffi::lua_tostring(lua, -1)).to_owned();
        let t = t.to_string_lossy();
        ffi::lua_pop(lua, 1);

        ffi::lua_pushvalue(lua, -1);
        ffi::lua_pushvalue(lua, i);
        assert_eq!(ffi::lua_pcall(lua, 1, 1, 0), 0);
        let s = std::ffi::CStr::from_ptr(ffi::lua_tostring(lua, -1)).to_owned();
        let s = s.to_string_lossy();
        ffi::lua_pop(lua, 1);

        writeln!(out, "{}: {}({})", i, t, s)?;
    }
    ffi::lua_settop(lua, top);
    Ok(())
}

#[allow(clippy::missing_safety_doc)]
pub unsafe fn dump_stack_raw(lua: LuaState) {
    dump_stack_raw_to(lua, std::io::stderr()).unwrap()
}
