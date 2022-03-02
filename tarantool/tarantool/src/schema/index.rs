use crate::error::{Error, TarantoolError};
use crate::c_ptr;
use crate::ffi::lua;
use crate::ffi::tarantool::{luaT_state, luaT_call};
use crate::index::{Index, IndexOptions};
use tlua::{
    LuaFunction,
    LuaTable,
    LuaError::{self, ExecutionError},
};

/// Create new index for space.
///
/// - `space_id`   - ID of existing space.
/// - `index_name` - name of index to create, which should conform to the rules for object names.
/// - `opts`       - see IndexOptions struct.
///
/// For details see [space_object:create_index](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/create_index/)
pub fn create_index(space_id: u32, index_name: &str, opts: &IndexOptions) -> Result<Index, Error> {
    let lua = crate::lua_state();
    let b: LuaTable<_> = lua.get("box")
        .ok_or_else(|| ExecutionError("box == nil".into()))?;
    let b_schema: LuaTable<_> = b.get("schema")
        .ok_or_else(|| ExecutionError("box.schema == nil".into()))?;
    let b_s_index: LuaTable<_> = b_schema.get("index")
        .ok_or_else(|| ExecutionError("box.schema.index == nil".into()))?;
    let index_create: LuaFunction<_> = b_s_index.get("create")
        .ok_or_else(|| ExecutionError("box.schema.index.create == nil".into()))?;
    let new_index: LuaTable<_> = index_create.call_with_args((space_id, index_name, opts))
        .map_err(LuaError::from)?;
    let index_id: u32 = new_index.get("id")
        .ok_or_else(|| ExecutionError(
                format!("box.space[{}].index['{}'] == nil", space_id, index_name)
                    .into()
        ))?;
    Ok(Index::new(space_id, index_id))
}

/// Drop existing index.
///
/// - `space_id` - ID of existing space.
/// - `index_name` - ID of existing index.
pub fn drop_index(space_id: u32, index_id: u32) -> Result<(), Error> {
    unsafe {
        // Create new stack (just in case - in order no to mess things
        // in current stack).
        let state = luaT_state();
        let drop_state = lua::lua_newthread(state);

        // Execute the following Lua code:
        // -- space = box.space._space:get(space_id)
        // -- space_name = space.name
        // -- index = box.space._index:get({space_id, index_id})
        // -- index_name = index.name
        // -- box.space.space_name.index.index_name:drop()

        // -- space = box.space._space:get({"id": space_id})
        lua::lua_getglobal(drop_state, c_ptr!("box"));
        lua::lua_getfield(drop_state, -1, c_ptr!("space"));
        lua::lua_getfield(drop_state, -1, c_ptr!("_space"));
        lua::lua_getfield(drop_state, -1, c_ptr!("get"));
        lua::lua_pushvalue(drop_state, -2);
        lua::lua_pushinteger(drop_state, space_id as isize);
        if luaT_call(drop_state, 2, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // -- space_name = space.name
        lua::lua_getfield(drop_state, -1, c_ptr!("name"));
        let space_name = lua::lua_tostring(drop_state, -1);
        lua::lua_remove(drop_state, -1);

        // -- index = box.space._index:get({space_id, index_id})
        lua::lua_getglobal(drop_state, c_ptr!("box"));
        lua::lua_getfield(drop_state, -1, c_ptr!("space"));
        lua::lua_getfield(drop_state, -1, c_ptr!("_index"));
        lua::lua_getfield(drop_state, -1, c_ptr!("get"));
        lua::lua_pushvalue(drop_state, -2);
        lua::lua_newtable(drop_state);
        lua::lua_pushinteger(drop_state, 1);
        lua::lua_pushinteger(drop_state, space_id as isize);
        lua::lua_settable(drop_state, -3);
        lua::lua_pushinteger(drop_state, 2);
        lua::lua_pushinteger(drop_state, index_id as isize);
        lua::lua_settable(drop_state, -3);
        if luaT_call(drop_state, 2, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // -- index_name = index.name
        lua::lua_getfield(drop_state, -1, c_ptr!("name"));
        let index_name = lua::lua_tostring(drop_state, -1);
        lua::lua_remove(drop_state, -1);

        // -- box.space.space_name.index.index_name:drop()
        lua::lua_getglobal(drop_state, c_ptr!("box"));
        lua::lua_getfield(drop_state, -1, c_ptr!("space"));
        lua::lua_getfield(drop_state, -1, space_name);
        lua::lua_getfield(drop_state, -1, c_ptr!("index"));
        lua::lua_getfield(drop_state, -1, index_name);
        lua::lua_getfield(drop_state, -1, c_ptr!("drop"));
        lua::lua_pushvalue(drop_state, -2);
        if luaT_call(drop_state, 1, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // No need to clean drop_state. It will be gc'ed.
    }

    Ok(())
}
