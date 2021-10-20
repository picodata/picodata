pub mod any;
pub mod functions_write;
pub mod lua_functions;
pub mod lua_tables;
pub mod misc;
pub mod rust_tables;
pub mod userdata;
pub mod values;

pub fn global<'lua>() -> tarantool::hlua::Lua<'lua> {
    unsafe {
        tarantool::hlua::Lua::from_existing_state(
            tarantool::ffi::tarantool::luaT_state(), false
        )
    }
}
