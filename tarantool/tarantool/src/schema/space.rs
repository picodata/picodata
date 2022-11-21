use std::borrow::Cow;
use std::{cmp::max, collections::BTreeMap};

use serde::{Deserialize, Serialize};

use crate::error::{Error, TarantoolError, TarantoolErrorCode};
use crate::index::IteratorType;
use crate::schema;
use crate::schema::sequence as schema_seq;
use crate::session;
use crate::set_error;
use crate::space::{Space, SystemSpace, SYSTEM_ID_MAX};
use crate::space::{SpaceCreateOptions, SpaceEngineType};
use crate::tuple::{Encode, Tuple};
use crate::util::Value;

/// Create a space.
/// (for details see [box.schema.space.create()](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema/space_create/)).
///
/// - `name` -  name of space, which should conform to the rules for object names.
/// - `opts` - see SpaceCreateOptions struct.
///
/// Returns a new space.
pub fn create_space(name: &str, opts: &SpaceCreateOptions) -> Result<Space, Error> {
    // Check if space already exists.
    if let Some(space) = Space::find(name) {
        return if opts.if_not_exists {
            Ok(space)
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
    let id = opts.id.map(Ok).unwrap_or_else(resolve_new_space_id)?;

    let flags = opts
        .is_local
        .then(|| ("group_id".into(), Value::Num(1)))
        .into_iter()
        .chain(
            opts.is_temporary
                .then(|| ("temporary".into(), Value::Bool(true))),
        )
        .chain(opts.is_sync.then(|| ("is_sync".into(), Value::Bool(true))))
        .collect();

    let format = opts
        .format
        .iter()
        .flat_map(|f| f.iter())
        .map(|f| {
            IntoIterator::into_iter([
                ("name".into(), Value::Str(f.name.as_str().into())),
                ("type".into(), Value::Str(f.field_type.as_str().into())),
                ("is_nullable".into(), Value::Bool(f.is_nullable)),
            ])
            .collect()
        })
        .collect();

    let sys_space: Space = SystemSpace::Space.into();
    sys_space.insert(&SpaceMetadata {
        id,
        user_id,
        name: name.into(),
        engine: opts.engine,
        field_count: opts.field_count,
        flags,
        format,
    })?;

    Ok(Space::find(name).unwrap())
}

/// SpaceMetadata is tuple, holding space metadata in system `_space` space.
#[derive(Serialize, Deserialize, Debug)]
pub struct SpaceMetadata<'a> {
    pub id: u32,
    pub user_id: u32,
    pub name: Cow<'a, str>,
    pub engine: SpaceEngineType,
    pub field_count: u32,
    pub flags: BTreeMap<Cow<'a, str>, Value<'a>>,
    pub format: Vec<BTreeMap<Cow<'a, str>, Value<'a>>>,
}

impl Encode for SpaceMetadata<'_> {}

fn resolve_new_space_id() -> Result<u32, Error> {
    let sys_space: Space = SystemSpace::Space.into();
    let sys_schema: Space = SystemSpace::Schema.into();

    // Try to update max_id in _schema space.
    let new_max_id = sys_schema.update(&("max_id",), [("+", 1, 1)])?;

    let space_id = if let Some(new_max_id) = new_max_id {
        // In case of successful update max_id return its value.
        new_max_id.field::<u32>(1)?.unwrap()
    } else {
        // Get tuple with greatest id. Increment it and use as id of new space.
        let max_tuple = sys_space.index("primary").unwrap().max(&())?.unwrap();
        let max_tuple_id = max_tuple.field::<u32>(0)?.unwrap();
        let max_id_val = max(max_tuple_id, SYSTEM_ID_MAX);
        // Insert max_id into _schema space.
        let created_max_id = sys_schema.insert(&("max_id".to_string(), max_id_val + 1))?;
        created_max_id.field::<u32>(1)?.unwrap()
    };

    Ok(space_id)
}

/// Drop a space.
pub fn drop_space(space_id: u32) -> Result<(), Error> {
    // Delete automatically generated sequence.
    let sys_space_sequence: Space = SystemSpace::SpaceSequence.into();
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
    let sys_trigger: Space = SystemSpace::Trigger.into();
    let sys_space_idx = sys_trigger.index("space_id").unwrap();
    for t in sys_space_idx
        .select(IteratorType::Eq, &(space_id,))?
        .collect::<Vec<Tuple>>()
    {
        let name = t.field::<String>(0)?.unwrap();
        sys_trigger.delete(&(name,))?;
    }

    // Remove from _fk_constraint.
    let sys_fk_constraint: Space = SystemSpace::FkConstraint.into();
    let sys_space_idx = sys_fk_constraint.index("child_id").unwrap();
    for t in sys_space_idx
        .select(IteratorType::Eq, &(space_id,))?
        .collect::<Vec<Tuple>>()
    {
        let name = t.field::<String>(0)?.unwrap();
        sys_fk_constraint.delete(&(name, space_id))?;
    }

    // CRemove from _ck_constraint.
    let sys_ck_constraint: Space = SystemSpace::CkConstraint.into();
    let sys_space_idx = sys_ck_constraint.index("primary").unwrap();
    for t in sys_space_idx
        .select(IteratorType::Eq, &(space_id,))?
        .collect::<Vec<Tuple>>()
    {
        let name = t.field::<String>(2)?.unwrap();
        sys_ck_constraint.delete(&(space_id, name))?;
    }

    // Remove from _func_index.
    let sys_func_index: Space = SystemSpace::FuncIndex.into();
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
    let sys_index: Space = SystemSpace::Index.into();
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
    let sys_truncate: Space = SystemSpace::Truncate.into();
    sys_truncate.delete(&(space_id,))?;

    // Remove from _space.
    let sys_space: Space = SystemSpace::Space.into();
    sys_space.delete(&(space_id,))?;

    Ok(())
}
