//! Box: schema
use std::os::raw::c_int;

use crate::serde::{Serialize, Serializer};
use crate::serde_json::{Map, Value};

use crate::error::{Error, TarantoolError};
use crate::ffi::helper::new_c_str;
use crate::ffi::lua as ffi_lua;
use crate::index::IteratorType;
use crate::space::{Space, SystemSpace};
use crate::tuple::{AsTuple, Tuple};

/// Type of engine, used by space.
#[derive(Copy, Clone, Debug)]
pub enum SpaceEngineType {
    Memtx,
    Vinyl,
}

impl Serialize for SpaceEngineType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            SpaceEngineType::Memtx => serializer.serialize_str("memtx"),
            SpaceEngineType::Vinyl => serializer.serialize_str("vinyl"),
        }
    }
}

/// SpaceInternal is tuple, holdiing space metdata in system `_space` space.
/// For details see internal Space::insert_new_space function.
#[derive(Serialize, Debug)]
pub struct SpaceMetadata {
    pub id: u32,
    pub uid: u32,
    pub name: String,
    pub engine: SpaceEngineType,
    pub field_count: u32,
    pub options: Map<String, Value>,
    pub format: Vec<Value>,
}

impl AsTuple for SpaceMetadata {}

/// List of options for new or updated index.
/// (for details see [space_object:create_index - options](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/create_index/)).
pub struct IndexOptions {
    pub index_type: Option<IndexType>,
    pub id: Option<u32>,
    pub unique: Option<bool>,
    pub if_not_exists: Option<bool>,
    pub parts: Option<Vec<IndexPart>>,
    pub dimension: Option<u32>,
    pub distance: Option<RtreeIndexDistanceType>,
    pub bloom_fpr: Option<f32>,
    pub page_size: Option<u32>,
    pub range_size: Option<u32>,
    pub run_count_per_level: Option<u32>,
    pub run_size_ratio: Option<f32>,
    pub sequence: Option<IndexSequenceOption>,
    pub func: Option<String>,
    // Only for Tarantool >= 2.6
    // pub hint: Option<bool>,
}

impl Default for IndexOptions {
    fn default() -> Self {
        IndexOptions {
            index_type: Some(IndexType::Tree),
            id: None,
            unique: Some(true),
            if_not_exists: Some(false),
            parts: Some(vec![IndexPart {
                field_index: 1,
                field_type: IndexFieldType::Unsigned,
                collation: None,
                is_nullable: None,
                path: None,
            }]),
            dimension: Some(2),
            distance: Some(RtreeIndexDistanceType::Euclid),
            bloom_fpr: Some(0.05),
            page_size: Some(8 * 1024),
            range_size: None,
            run_count_per_level: Some(2),
            run_size_ratio: Some(3.5),
            sequence: None,
            func: None,
            // Only for Tarantool >= 2.6
            // hint: Some(true),
        }
    }
}

/// Type of index.
#[derive(Copy, Clone, Debug)]
pub enum IndexType {
    Hash,
    Tree,
    Bitset,
    Rtree,
}

/// Type of index part.
#[derive(Copy, Clone, Debug)]
pub enum IndexFieldType {
    Unsigned,
    String,
    Integer,
    Number,
    Double,
    Decimal,
    Boolean,
    Varbinary,
    Uuid,
    Array,
    Scalar,
}

/// Index part.
pub struct IndexPart {
    pub field_index: u32,
    pub field_type: IndexFieldType,
    pub collation: Option<String>,
    pub is_nullable: Option<bool>,
    pub path: Option<String>,
}

/// Type of distance for retree index.
#[derive(Copy, Clone, Debug)]
pub enum RtreeIndexDistanceType {
    Euclid,
    Manhattan,
}

/// Sequence option for new or updated index.
/// (for details see [specifying a sequence in create_index](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema_sequence/create_index/#box-schema-sequence-create-index)).
pub enum IndexSequenceOption {
    SeqId {
        seq_id: u32,
        field_index: Option<u32>,
    },
    SeqName {
        seq_name: String,
        field_index: Option<u32>,
    },
    True,
    Empty,
}

/// Create new index for space.
/// (for details see [space_object:create_index](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/create_index/))
///
/// - `space_id`   - ID of existing space.
/// - `index_name` - name of index to create, which should conform to the rules for object names.
/// - `opts`       - see IndexOptions struct.
pub fn create_index(space_id: u32, index_name: &str, opts: &IndexOptions) -> Result<(), Error> {
    unsafe {
        // Create new stack (just in case - in order no to mess things
        // in current stack).
        let state = ffi_lua::luaT_state();
        let ci_state = ffi_lua::lua_newthread(state);

        // Execute the following lua Code:
        // -- space = box.space._space:get(space_id)
        // -- space_name = space.name
        // -- box.space[space_name]:create_index(name, opts)

        // -- space = box.space._space:get(space_id)
        ffi_lua::lua_getglobal(ci_state, new_c_str("box").as_ptr());
        ffi_lua::lua_getfield(ci_state, -1, new_c_str("space").as_ptr());
        ffi_lua::lua_getfield(ci_state, -1, new_c_str("_space").as_ptr());
        ffi_lua::lua_getfield(ci_state, -1, new_c_str("get").as_ptr());
        ffi_lua::lua_pushvalue(ci_state, -2);
        ffi_lua::lua_pushinteger(ci_state, space_id as isize);
        if ffi_lua::luaT_call(ci_state, 2, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // -- space_name = space.name
        ffi_lua::lua_getfield(ci_state, -1, new_c_str("name").as_ptr());
        let space_name = ffi_lua::lua_tostring(ci_state, -1);
        ffi_lua::lua_remove(ci_state, -1);

        // -- box.space[space_name].create_index(name, opts)
        ffi_lua::lua_getglobal(ci_state, new_c_str("box").as_ptr());
        ffi_lua::lua_getfield(ci_state, -1, new_c_str("space").as_ptr());
        ffi_lua::lua_getfield(ci_state, -1, space_name);
        ffi_lua::lua_getfield(ci_state, -1, new_c_str("create_index").as_ptr());

        // Put args on the stack:

        // self
        ffi_lua::lua_pushvalue(ci_state, -2);

        // name
        ffi_lua::lua_pushstring(ci_state, new_c_str(index_name).as_ptr());

        // options
        ffi_lua::lua_newtable(ci_state);

        // opts.index_type
        if let Some(index_type) = opts.index_type {
            let index_type_str = match index_type {
                IndexType::Hash => "hash",
                IndexType::Tree => "tree",
                IndexType::Bitset => "bitset",
                IndexType::Rtree => "rtree",
            };
            ffi_lua::lua_pushstring(ci_state, new_c_str(index_type_str).as_ptr());
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("type").as_ptr());
        }

        // opts.id
        if let Some(id) = opts.id {
            ffi_lua::lua_pushinteger(ci_state, id as isize);
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("id").as_ptr());
        }

        // opts.unique
        if let Some(unique) = opts.unique {
            ffi_lua::lua_pushboolean(ci_state, unique as c_int);
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("unique").as_ptr());
        }

        // opts.if_not_exists
        if let Some(if_not_exists) = opts.if_not_exists {
            ffi_lua::lua_pushboolean(ci_state, if_not_exists as c_int);
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("if_not_exists").as_ptr());
        }

        // opts.parts
        if let Some(parts) = &opts.parts {
            ffi_lua::lua_newtable(ci_state);

            for (idx, p) in parts.iter().enumerate() {
                ffi_lua::lua_pushinteger(ci_state, (idx + 1) as isize);
                ffi_lua::lua_newtable(ci_state);

                // part.field
                ffi_lua::lua_pushinteger(ci_state, p.field_index as isize);
                ffi_lua::lua_setfield(ci_state, -2, new_c_str("field").as_ptr());

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
                ffi_lua::lua_pushstring(ci_state, new_c_str(field_type).as_ptr());
                ffi_lua::lua_setfield(ci_state, -2, new_c_str("type").as_ptr());

                // part.collation
                if let Some(collation) = &p.collation {
                    ffi_lua::lua_pushstring(ci_state, new_c_str(collation).as_ptr());
                    ffi_lua::lua_setfield(ci_state, -2, new_c_str("collation").as_ptr());
                }

                // part.is_nullable
                if let Some(is_nullable) = &p.is_nullable {
                    ffi_lua::lua_pushboolean(ci_state, if *is_nullable { 1 } else { 0 });
                    ffi_lua::lua_setfield(ci_state, -2, new_c_str("is_nullable").as_ptr());
                }

                // part.path
                if let Some(path) = &p.path {
                    ffi_lua::lua_pushstring(ci_state, new_c_str(path).as_ptr());
                    ffi_lua::lua_setfield(ci_state, -2, new_c_str("path").as_ptr());
                }

                ffi_lua::lua_settable(ci_state, -3);
            }

            ffi_lua::lua_setfield(ci_state, -2, new_c_str("parts").as_ptr())
        }

        // opts.dimension
        if let Some(dimension) = opts.dimension {
            ffi_lua::lua_pushinteger(ci_state, dimension as isize);
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("dimension").as_ptr());
        }

        // opts.distance
        if let Some(distance) = opts.distance {
            let distance_str = match distance {
                RtreeIndexDistanceType::Euclid => "euclid",
                RtreeIndexDistanceType::Manhattan => "manhattan",
            };
            ffi_lua::lua_pushstring(ci_state, new_c_str(distance_str).as_ptr());
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("distance").as_ptr());
        }

        // opts.bloom_fpr
        if let Some(bloom_fpr) = opts.bloom_fpr {
            ffi_lua::lua_pushnumber(ci_state, bloom_fpr as f64);
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("bloom_fpr").as_ptr());
        }

        // opts.page_size
        if let Some(page_size) = opts.page_size {
            ffi_lua::lua_pushinteger(ci_state, page_size as isize);
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("page_size").as_ptr());
        }

        // opts.range_size
        if let Some(range_size) = opts.range_size {
            ffi_lua::lua_pushinteger(ci_state, range_size as isize);
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("range_size").as_ptr());
        }

        // opts.run_count_per_level
        if let Some(run_count_per_level) = opts.run_count_per_level {
            ffi_lua::lua_pushinteger(ci_state, run_count_per_level as isize);
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("run_count_per_level").as_ptr());
        }

        // opts.run_size_ratio
        if let Some(run_size_ratio) = opts.run_size_ratio {
            ffi_lua::lua_pushnumber(ci_state, run_size_ratio as f64);
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("run_size_ratio").as_ptr());
        }

        // opts.sequence
        if let Some(sequence) = &opts.sequence {
            match sequence {
                // sequence = {id = sequence identifier , field = field number }
                IndexSequenceOption::SeqId {
                    seq_id,
                    field_index,
                } => {
                    ffi_lua::lua_newtable(ci_state);
                    ffi_lua::lua_pushinteger(ci_state, *seq_id as isize);
                    ffi_lua::lua_setfield(ci_state, -2, new_c_str("id").as_ptr());
                    if let Some(fi) = field_index {
                        ffi_lua::lua_pushinteger(ci_state, *fi as isize);
                        ffi_lua::lua_setfield(ci_state, -2, new_c_str("field").as_ptr());
                    }
                }
                // sequence = {id = sequence name , field = field number }
                IndexSequenceOption::SeqName {
                    seq_name,
                    field_index,
                } => {
                    ffi_lua::lua_newtable(ci_state);
                    ffi_lua::lua_pushstring(ci_state, new_c_str(seq_name).as_ptr());
                    ffi_lua::lua_setfield(ci_state, -2, new_c_str("id").as_ptr());
                    if let Some(fi) = field_index {
                        ffi_lua::lua_pushinteger(ci_state, *fi as isize);
                        ffi_lua::lua_setfield(ci_state, -2, new_c_str("field").as_ptr());
                    }
                }
                // sequence = true
                IndexSequenceOption::True => {
                    ffi_lua::lua_pushboolean(ci_state, true as c_int);
                }
                // sequence = {}
                IndexSequenceOption::Empty => {
                    ffi_lua::lua_newtable(ci_state);
                }
            }
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("sequence").as_ptr());
        }

        // opts.func
        if let Some(func) = &opts.func {
            ffi_lua::lua_pushstring(ci_state, new_c_str(func).as_ptr());
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("func").as_ptr());
        }

        // Only for Tarantool >= 2.6
        // opt.hint
        /* if let Some(hint) = opts.hint {
            ffi_lua::lua_pushboolean(ci_state, bool_as_int(hint));
            ffi_lua::lua_setfield(ci_state, -2, new_c_str("hint").as_ptr());
        }
        */

        // Call space_object:create_index.
        if ffi_lua::luaT_call(ci_state, 3, 1) == 1 {
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
        let state = ffi_lua::luaT_state();
        let drop_state = ffi_lua::lua_newthread(state);

        // Execute the following Lua code:
        // -- space = box.space._space:get(space_id)
        // -- space_name = space.name
        // -- index = box.space._index:get({space_id, index_id})
        // -- index_name = index.name
        // -- box.space.space_name.index.index_name:drop()

        // -- space = box.space._space:get({"id": space_id})
        ffi_lua::lua_getglobal(drop_state, new_c_str("box").as_ptr());
        ffi_lua::lua_getfield(drop_state, -1, new_c_str("space").as_ptr());
        ffi_lua::lua_getfield(drop_state, -1, new_c_str("_space").as_ptr());
        ffi_lua::lua_getfield(drop_state, -1, new_c_str("get").as_ptr());
        ffi_lua::lua_pushvalue(drop_state, -2);
        ffi_lua::lua_pushinteger(drop_state, space_id as isize);
        if ffi_lua::luaT_call(drop_state, 2, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // -- space_name = space.name
        ffi_lua::lua_getfield(drop_state, -1, new_c_str("name").as_ptr());
        let space_name = ffi_lua::lua_tostring(drop_state, -1);
        ffi_lua::lua_remove(drop_state, -1);

        // -- index = box.space._index:get({space_id, index_id})
        ffi_lua::lua_getglobal(drop_state, new_c_str("box").as_ptr());
        ffi_lua::lua_getfield(drop_state, -1, new_c_str("space").as_ptr());
        ffi_lua::lua_getfield(drop_state, -1, new_c_str("_index").as_ptr());
        ffi_lua::lua_getfield(drop_state, -1, new_c_str("get").as_ptr());
        ffi_lua::lua_pushvalue(drop_state, -2);
        ffi_lua::lua_newtable(drop_state);
        ffi_lua::lua_pushinteger(drop_state, 1);
        ffi_lua::lua_pushinteger(drop_state, space_id as isize);
        ffi_lua::lua_settable(drop_state, -3);
        ffi_lua::lua_pushinteger(drop_state, 2);
        ffi_lua::lua_pushinteger(drop_state, index_id as isize);
        ffi_lua::lua_settable(drop_state, -3);
        if ffi_lua::luaT_call(drop_state, 2, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // -- index_name = index.name
        ffi_lua::lua_getfield(drop_state, -1, new_c_str("name").as_ptr());
        let index_name = ffi_lua::lua_tostring(drop_state, -1);
        ffi_lua::lua_remove(drop_state, -1);

        // -- box.space.space_name.index.index_name:drop()
        ffi_lua::lua_getglobal(drop_state, new_c_str("box").as_ptr());
        ffi_lua::lua_getfield(drop_state, -1, new_c_str("space").as_ptr());
        ffi_lua::lua_getfield(drop_state, -1, space_name);
        ffi_lua::lua_getfield(drop_state, -1, new_c_str("index").as_ptr());
        ffi_lua::lua_getfield(drop_state, -1, index_name);
        ffi_lua::lua_getfield(drop_state, -1, new_c_str("drop").as_ptr());
        ffi_lua::lua_pushvalue(drop_state, -2);
        if ffi_lua::luaT_call(drop_state, 1, 1) == 1 {
            return Err(TarantoolError::last().into());
        }

        // No need to clean drop_state. It will be gc'ed.
    }

    Ok(())
}

/// Revoke all privileges associated with the given object.
///
/// - `obj_type` - string representation of object's type. Can be one of the following: "space", "sequence" or "function".
/// - `obj_id` - object's ID
pub fn revoke_object_priveleges(obj_type: &str, obj_id: u32) -> Result<(), Error> {
    let sys_vpriv: Space = SystemSpace::VPriv.into();
    let mut sys_priv: Space = SystemSpace::Priv.into();

    let index_obj = sys_vpriv.index("object").unwrap();
    let privs: Vec<Tuple> = index_obj
        .select(IteratorType::Eq, &(obj_type, obj_id))?
        .collect();

    for t in privs {
        let uid = t.field::<u32>(1)?.unwrap();
        sys_priv.delete(&(uid,))?;
    }

    Ok(())
}
