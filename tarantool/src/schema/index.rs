use std::os::raw::c_int;

use crate::error::{Error, TarantoolError};
use crate::ffi::helper::new_c_str;
use crate::ffi::lua;
use crate::index::{
    IndexFieldType, IndexOptions, IndexSequenceOption, IndexType, RtreeIndexDistanceType,
};

/// Create new index for space.
///
/// - `space_id`   - ID of existing space.
/// - `index_name` - name of index to create, which should conform to the rules for object names.
/// - `opts`       - see IndexOptions struct.
///
/// For details see [space_object:create_index](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/create_index/)
pub fn create_index(space_id: u32, index_name: &str, opts: &IndexOptions) -> Result<(), Error> {
    unsafe {
        // Create new stack (just in case - in order no to mess things
        // in current stack).
        let state = lua::luaT_state();
        let ci_state = lua::lua_newthread(state);

        // Execute the following lua Code:
        // -- space = box.space._space:get(space_id)
        // -- space_name = space.name
        // -- box.space[space_name]:create_index(name, opts)

        // -- space = box.space._space:get(space_id)
        lua::lua_getglobal(ci_state, new_c_str("box").as_ptr());
        lua::lua_getfield(ci_state, -1, new_c_str("space").as_ptr());
        lua::lua_getfield(ci_state, -1, new_c_str("_space").as_ptr());
        lua::lua_getfield(ci_state, -1, new_c_str("get").as_ptr());
        lua::lua_pushvalue(ci_state, -2);
        lua::lua_pushinteger(ci_state, space_id as isize);
        if lua::luaT_call(ci_state, 2, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // -- space_name = space.name
        lua::lua_getfield(ci_state, -1, new_c_str("name").as_ptr());
        let space_name = lua::lua_tostring(ci_state, -1);
        lua::lua_remove(ci_state, -1);

        // -- box.space[space_name].create_index(name, opts)
        lua::lua_getglobal(ci_state, new_c_str("box").as_ptr());
        lua::lua_getfield(ci_state, -1, new_c_str("space").as_ptr());
        lua::lua_getfield(ci_state, -1, space_name);
        lua::lua_getfield(ci_state, -1, new_c_str("create_index").as_ptr());

        // Put args on the stack:

        // self
        lua::lua_pushvalue(ci_state, -2);

        // name
        lua::lua_pushstring(ci_state, new_c_str(index_name).as_ptr());

        // options
        lua::lua_newtable(ci_state);

        // opts.index_type
        if let Some(index_type) = opts.index_type {
            let index_type_str = match index_type {
                IndexType::Hash => "hash",
                IndexType::Tree => "tree",
                IndexType::Bitset => "bitset",
                IndexType::Rtree => "rtree",
            };
            lua::lua_pushstring(ci_state, new_c_str(index_type_str).as_ptr());
            lua::lua_setfield(ci_state, -2, new_c_str("type").as_ptr());
        }

        // opts.id
        if let Some(id) = opts.id {
            lua::lua_pushinteger(ci_state, id as isize);
            lua::lua_setfield(ci_state, -2, new_c_str("id").as_ptr());
        }

        // opts.unique
        if let Some(unique) = opts.unique {
            lua::lua_pushboolean(ci_state, unique as c_int);
            lua::lua_setfield(ci_state, -2, new_c_str("unique").as_ptr());
        }

        // opts.if_not_exists
        if let Some(if_not_exists) = opts.if_not_exists {
            lua::lua_pushboolean(ci_state, if_not_exists as c_int);
            lua::lua_setfield(ci_state, -2, new_c_str("if_not_exists").as_ptr());
        }

        // opts.parts
        if let Some(parts) = &opts.parts {
            lua::lua_newtable(ci_state);

            for (idx, p) in parts.iter().enumerate() {
                lua::lua_pushinteger(ci_state, (idx + 1) as isize);
                lua::lua_newtable(ci_state);

                // part.field
                lua::lua_pushinteger(ci_state, p.field_index as isize);
                lua::lua_setfield(ci_state, -2, new_c_str("field").as_ptr());

                // part.type
                let field_type = match p.field_type {
                    IndexFieldType::Unsigned => "unsigned",
                    IndexFieldType::String => "string",
                    IndexFieldType::Integer => "integer",
                    IndexFieldType::Number => "number",
                    IndexFieldType::Double => "double",
                    IndexFieldType::Decimal => "decimal",
                    IndexFieldType::Boolean => "boolean",
                    IndexFieldType::Varbinary => "varbinary",
                    IndexFieldType::Uuid => "uuid",
                    IndexFieldType::Array => "array",
                    IndexFieldType::Scalar => "scalar",
                };
                lua::lua_pushstring(ci_state, new_c_str(field_type).as_ptr());
                lua::lua_setfield(ci_state, -2, new_c_str("type").as_ptr());

                // part.collation
                if let Some(collation) = &p.collation {
                    lua::lua_pushstring(ci_state, new_c_str(collation).as_ptr());
                    lua::lua_setfield(ci_state, -2, new_c_str("collation").as_ptr());
                }

                // part.is_nullable
                if let Some(is_nullable) = &p.is_nullable {
                    lua::lua_pushboolean(ci_state, if *is_nullable { 1 } else { 0 });
                    lua::lua_setfield(ci_state, -2, new_c_str("is_nullable").as_ptr());
                }

                // part.path
                if let Some(path) = &p.path {
                    lua::lua_pushstring(ci_state, new_c_str(path).as_ptr());
                    lua::lua_setfield(ci_state, -2, new_c_str("path").as_ptr());
                }

                lua::lua_settable(ci_state, -3);
            }

            lua::lua_setfield(ci_state, -2, new_c_str("parts").as_ptr())
        }

        // opts.dimension
        if let Some(dimension) = opts.dimension {
            lua::lua_pushinteger(ci_state, dimension as isize);
            lua::lua_setfield(ci_state, -2, new_c_str("dimension").as_ptr());
        }

        // opts.distance
        if let Some(distance) = opts.distance {
            let distance_str = match distance {
                RtreeIndexDistanceType::Euclid => "euclid",
                RtreeIndexDistanceType::Manhattan => "manhattan",
            };
            lua::lua_pushstring(ci_state, new_c_str(distance_str).as_ptr());
            lua::lua_setfield(ci_state, -2, new_c_str("distance").as_ptr());
        }

        // opts.bloom_fpr
        if let Some(bloom_fpr) = opts.bloom_fpr {
            lua::lua_pushnumber(ci_state, bloom_fpr as f64);
            lua::lua_setfield(ci_state, -2, new_c_str("bloom_fpr").as_ptr());
        }

        // opts.page_size
        if let Some(page_size) = opts.page_size {
            lua::lua_pushinteger(ci_state, page_size as isize);
            lua::lua_setfield(ci_state, -2, new_c_str("page_size").as_ptr());
        }

        // opts.range_size
        if let Some(range_size) = opts.range_size {
            lua::lua_pushinteger(ci_state, range_size as isize);
            lua::lua_setfield(ci_state, -2, new_c_str("range_size").as_ptr());
        }

        // opts.run_count_per_level
        if let Some(run_count_per_level) = opts.run_count_per_level {
            lua::lua_pushinteger(ci_state, run_count_per_level as isize);
            lua::lua_setfield(ci_state, -2, new_c_str("run_count_per_level").as_ptr());
        }

        // opts.run_size_ratio
        if let Some(run_size_ratio) = opts.run_size_ratio {
            lua::lua_pushnumber(ci_state, run_size_ratio as f64);
            lua::lua_setfield(ci_state, -2, new_c_str("run_size_ratio").as_ptr());
        }

        // opts.sequence
        if let Some(sequence) = &opts.sequence {
            match sequence {
                // sequence = {id = sequence identifier , field = field number }
                IndexSequenceOption::SeqId {
                    seq_id,
                    field_index,
                } => {
                    lua::lua_newtable(ci_state);
                    lua::lua_pushinteger(ci_state, *seq_id as isize);
                    lua::lua_setfield(ci_state, -2, new_c_str("id").as_ptr());
                    if let Some(fi) = field_index {
                        lua::lua_pushinteger(ci_state, *fi as isize);
                        lua::lua_setfield(ci_state, -2, new_c_str("field").as_ptr());
                    }
                }
                // sequence = {id = sequence name , field = field number }
                IndexSequenceOption::SeqName {
                    seq_name,
                    field_index,
                } => {
                    lua::lua_newtable(ci_state);
                    lua::lua_pushstring(ci_state, new_c_str(seq_name).as_ptr());
                    lua::lua_setfield(ci_state, -2, new_c_str("id").as_ptr());
                    if let Some(fi) = field_index {
                        lua::lua_pushinteger(ci_state, *fi as isize);
                        lua::lua_setfield(ci_state, -2, new_c_str("field").as_ptr());
                    }
                }
                // sequence = true
                IndexSequenceOption::True => {
                    lua::lua_pushboolean(ci_state, true as c_int);
                }
                // sequence = {}
                IndexSequenceOption::Empty => {
                    lua::lua_newtable(ci_state);
                }
            }
            lua::lua_setfield(ci_state, -2, new_c_str("sequence").as_ptr());
        }

        // opts.func
        if let Some(func) = &opts.func {
            lua::lua_pushstring(ci_state, new_c_str(func).as_ptr());
            lua::lua_setfield(ci_state, -2, new_c_str("func").as_ptr());
        }

        // Only for Tarantool >= 2.6
        // opt.hint
        /* if let Some(hint) = opts.hint {
            ffi_lua::lua_pushboolean(ci_state, bool_as_int(hint));
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("hint").as_ptr());
        }
        */

        // Call space_object:create_index.
        if lua::luaT_call(ci_state, 3, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // No need to clean ci_state. It will be gc'ed.
    }

    Ok(())
}

/// Drop existing index.
///
/// - `space_id` - ID of existing space.
/// - `index_name` - ID of existing index.
pub fn drop_index(space_id: u32, index_id: u32) -> Result<(), Error> {
    unsafe {
        // Create new stack (just in case - in order no to mess things
        // in current stack).
        let state = lua::luaT_state();
        let drop_state = lua::lua_newthread(state);

        // Execute the following Lua code:
        // -- space = box.space._space:get(space_id)
        // -- space_name = space.name
        // -- index = box.space._index:get({space_id, index_id})
        // -- index_name = index.name
        // -- box.space.space_name.index.index_name:drop()

        // -- space = box.space._space:get({"id": space_id})
        lua::lua_getglobal(drop_state, new_c_str("box").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("space").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("_space").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("get").as_ptr());
        lua::lua_pushvalue(drop_state, -2);
        lua::lua_pushinteger(drop_state, space_id as isize);
        if lua::luaT_call(drop_state, 2, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // -- space_name = space.name
        lua::lua_getfield(drop_state, -1, new_c_str("name").as_ptr());
        let space_name = lua::lua_tostring(drop_state, -1);
        lua::lua_remove(drop_state, -1);

        // -- index = box.space._index:get({space_id, index_id})
        lua::lua_getglobal(drop_state, new_c_str("box").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("space").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("_index").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("get").as_ptr());
        lua::lua_pushvalue(drop_state, -2);
        lua::lua_newtable(drop_state);
        lua::lua_pushinteger(drop_state, 1);
        lua::lua_pushinteger(drop_state, space_id as isize);
        lua::lua_settable(drop_state, -3);
        lua::lua_pushinteger(drop_state, 2);
        lua::lua_pushinteger(drop_state, index_id as isize);
        lua::lua_settable(drop_state, -3);
        if lua::luaT_call(drop_state, 2, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // -- index_name = index.name
        lua::lua_getfield(drop_state, -1, new_c_str("name").as_ptr());
        let index_name = lua::lua_tostring(drop_state, -1);
        lua::lua_remove(drop_state, -1);

        // -- box.space.space_name.index.index_name:drop()
        lua::lua_getglobal(drop_state, new_c_str("box").as_ptr());
        lua::lua_getfield(drop_state, -1, new_c_str("space").as_ptr());
        lua::lua_getfield(drop_state, -1, space_name);
        lua::lua_getfield(drop_state, -1, new_c_str("index").as_ptr());
        lua::lua_getfield(drop_state, -1, index_name);
        lua::lua_getfield(drop_state, -1, new_c_str("drop").as_ptr());
        lua::lua_pushvalue(drop_state, -2);
        if lua::luaT_call(drop_state, 1, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // No need to clean drop_state. It will be gc'ed.
    }

    Ok(())
}
