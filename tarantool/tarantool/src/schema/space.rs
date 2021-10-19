use std::cmp::max;

use serde::Serialize;
use serde_json::{Map, Number, Value};

use crate::error::{Error, TarantoolError, TarantoolErrorCode};
use crate::index::IteratorType;
use crate::schema;
use crate::schema::sequence as schema_seq;
use crate::session;
use crate::space::{Space, SystemSpace, SYSTEM_ID_MAX};
use crate::space::{SpaceCreateOptions, SpaceEngineType};
use crate::tuple::{AsTuple, Tuple};

/// SpaceMetadata is tuple, holding space metadata in system `_space` space.
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

/// Create a space.
/// (for details see [box.schema.space.create()](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema/space_create/)).
///
/// - `name` -  name of space, which should conform to the rules for object names.
/// - `opts` - see SpaceCreateOptions struct.
///
/// Returns a new space.
pub fn create_space(name: &str, opts: &SpaceCreateOptions) -> Result<Space, Error> {
    // Check if space already exists.
    let space = Space::find(name);
    if space.is_some() {
        return if opts.if_not_exists {
            Ok(space.unwrap())
        } else {
            set_error!(TarantoolErrorCode::SpaceExists, "{}", name);
            Err(TarantoolError::last().into())
        };
    }

    // Resolve ID of user, specified in options, or use ID of current session's user.
    let user_id = match &opts.user {
        None => session::uid()? as u32,
        Some(user) => {
            let resolved_uid = schema::resolve_user_or_role(user.as_str())?;
            match resolved_uid {
                Some(uid) => uid,
                None => {
                    set_error!(TarantoolErrorCode::NoSuchUser, "{}", user.as_str());
                    return Err(TarantoolError::last().into());
                }
            }
        }
    };

    // Resolve ID of new space or use ID, specified in options.
    let space_id = match opts.id {
        None => resolve_new_space_id()?,
        Some(id) => id,
    };

    insert_new_space(space_id, user_id, name, opts)
}

fn resolve_new_space_id() -> Result<u32, Error> {
    let sys_space: Space = SystemSpace::Space.into();
    let mut sys_schema: Space = SystemSpace::Schema.into();

    // Try to update max_id in _schema space.
    let new_max_id = sys_schema.update(&("max_id",), &vec![("+".to_string(), 1, 1)])?;

    let space_id = if new_max_id.is_some() {
        // In case of successful update max_id return its value.
        new_max_id.unwrap().field::<u32>(1)?.unwrap()
    } else {
        // Get tuple with greatest id. Increment it and use as id of new space.
        let max_tuple = sys_space.index("primary").unwrap().max(&())?.unwrap();
        let max_tuple_id = max_tuple.field::<u32>(0)?.unwrap();
        let max_id_val = max(max_tuple_id, SYSTEM_ID_MAX);
        // Insert max_id into _schema space.
        let created_max_id = sys_schema
            .insert(&("max_id".to_string(), max_id_val + 1))?
            .unwrap();
        created_max_id.field::<u32>(1)?.unwrap()
    };

    return Ok(space_id);
}

fn insert_new_space(
    id: u32,
    uid: u32,
    name: &str,
    opts: &SpaceCreateOptions,
) -> Result<Space, Error> {
    // `engine`
    let engine = match opts.engine {
        None => SpaceEngineType::Memtx,
        Some(e) => e,
    };

    // `field_count`
    let field_count = match opts.field_count {
        None => 0,
        Some(count) => count,
    };

    // `space_opts`
    let mut space_opts = Map::<String, Value>::new();
    if opts.is_local {
        space_opts.insert("group_id".to_string(), Value::Number(Number::from(1)));
    }
    if opts.is_temporary {
        space_opts.insert("temporary".to_string(), Value::Bool(true));
    }
    // Only for Tarantool version >= 2.6
    // space_opts.insert("is_sync".to_string(), Value::Bool(opts.is_sync));

    // `space_format`
    let mut space_format = Vec::<Value>::new();
    if let Some(format) = &opts.format {
        for ft in format {
            let mut field_format = Map::<String, Value>::new();
            field_format.insert("name".to_string(), Value::String(ft.name.clone()));
            field_format.insert("type".to_string(), Value::String(ft.field_type.to_string()));
            space_format.push(Value::Object(field_format));
        }
    }

    let new_space = SpaceMetadata {
        id: id,
        uid: uid,
        name: name.to_string(),
        engine: engine,
        field_count: field_count,
        options: space_opts.clone(),
        format: space_format.clone(),
    };

    let mut sys_space: Space = SystemSpace::Space.into();
    match sys_space.insert(&new_space) {
        Err(e) => Err(e),
        Ok(_) => Ok(Space::find(name).unwrap()),
    }
}

/// Drop a space.
pub fn drop_space(space_id: u32) -> Result<(), Error> {
    // Delete automatically generated sequence.
    let mut sys_space_sequence: Space = SystemSpace::SpaceSequence.into();
    let seq_tuple = sys_space_sequence.delete(&(space_id,))?;
    match seq_tuple {
        None => (),
        Some(t) => {
            let is_generated = t.field::<bool>(2)?.unwrap();
            if is_generated {
                let seq_id = t.field::<u32>(1)?.unwrap();
                schema_seq::drop_sequence(seq_id)?;
            }
        }
    }

    // Remove from _trigger.
    let mut sys_trigger: Space = SystemSpace::Trigger.into();
    let sys_space_idx = sys_trigger.index("space_id").unwrap();
    for t in sys_space_idx
        .select(IteratorType::Eq, &(space_id,))?
        .collect::<Vec<Tuple>>()
    {
        let name = t.field::<String>(0)?.unwrap();
        sys_trigger.delete(&(name,))?;
    }

    // Remove from _fk_constraint.
    let mut sys_fk_constraint: Space = SystemSpace::FkConstraint.into();
    let sys_space_idx = sys_fk_constraint.index("child_id").unwrap();
    for t in sys_space_idx
        .select(IteratorType::Eq, &(space_id,))?
        .collect::<Vec<Tuple>>()
    {
        let name = t.field::<String>(0)?.unwrap();
        sys_fk_constraint.delete(&(name, space_id))?;
    }

    // CRemove from _ck_constraint.
    let mut sys_ck_constraint: Space = SystemSpace::CkConstraint.into();
    let sys_space_idx = sys_ck_constraint.index("primary").unwrap();
    for t in sys_space_idx
        .select(IteratorType::Eq, &(space_id,))?
        .collect::<Vec<Tuple>>()
    {
        let name = t.field::<String>(2)?.unwrap();
        sys_ck_constraint.delete(&(space_id, name))?;
    }

    // Remove from _func_index.
    let mut sys_func_index: Space = SystemSpace::FuncIndex.into();
    let sys_space_idx = sys_func_index.index("primary").unwrap();
    for t in sys_space_idx
        .select(IteratorType::Eq, &(space_id,))?
        .collect::<Vec<Tuple>>()
    {
        let index_id = t.field::<u32>(1)?.unwrap();
        sys_func_index.delete(&(space_id, index_id))?;
    }

    // Remove from _index.
    let sys_vindex: Space = SystemSpace::VIndex.into();
    let mut sys_index: Space = SystemSpace::Index.into();
    let keys = sys_vindex
        .select(IteratorType::Eq, &(space_id,))?
        .collect::<Vec<Tuple>>();
    for i in 1..keys.len() + 1 {
        let t_idx = keys.len() - i;
        let t = &keys[t_idx];
        let id = t.field::<u32>(0)?.unwrap();
        let iid = t.field::<u32>(1)?.unwrap();
        sys_index.delete(&(id, iid))?;
    }

    // Revoke priveleges.
    schema::revoke_object_privileges("space", space_id)?;

    // Remove from _truncate.
    let mut sys_truncate: Space = SystemSpace::Truncate.into();
    sys_truncate.delete(&(space_id,))?;

    // Remove from _space.
    let mut sys_space: Space = SystemSpace::Space.into();
    sys_space.delete(&(space_id,))?;

    Ok(())
}
