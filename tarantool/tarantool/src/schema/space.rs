use crate::error::{Error, TarantoolError, TarantoolErrorCode};
use crate::index::IteratorType;
use crate::schema;
use crate::schema::sequence as schema_seq;
use crate::session;
use crate::set_error;
use crate::space;
use crate::space::{Metadata, SpaceCreateOptions};
use crate::space::{Space, SpaceId, SpaceType, SystemSpace};
use crate::transaction;
use crate::tuple::Tuple;
use crate::unwrap_or;
use crate::util::Value;
use std::collections::BTreeMap;

/// Create a space.
/// (for details see [box.schema.space.create()](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_schema/space_create/)).
///
/// - `name` -  name of space, which should conform to the rules for object names.
/// - `opts` - see SpaceCreateOptions struct.
///
/// Returns a new space.
///
/// **NOTE:** This function will initiate a transaction if there's isn't an
/// active one, and if there is the active transaction may be aborted in case
/// of an error. This shouldn't be a problem if you always consider this
/// function returning an error to be worthy of a transcation roll back,
/// which you should.
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
        None => session::uid()?,
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
    let id = if let Some(opts_id) = opts.id {
        opts_id
    } else {
        generate_space_id(opts.space_type == SpaceType::Temporary)?
    };

    let mut flags = BTreeMap::new();
    match opts.space_type {
        SpaceType::DataTemporary => {
            flags.insert("temporary".into(), true.into());
        }
        SpaceType::Temporary => {
            flags.insert("type".into(), "temporary".into());
        }
        SpaceType::DataLocal => {
            flags.insert("group_id".into(), 1.into());
        }
        SpaceType::Synchronous => {
            flags.insert("is_sync".into(), true.into());
        }
        SpaceType::Normal => {}
    }

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

    let nested_transaction = transaction::is_in_transaction();
    if !nested_transaction {
        transaction::begin()?;
    }

    let res = (|| -> Result<_, Error> {
        let sys_space = SystemSpace::Space.as_space();
        sys_space.insert(&Metadata {
            id,
            user_id,
            name: name.into(),
            engine: opts.engine,
            field_count: opts.field_count,
            flags,
            format,
        })?;

        // Update max_id for backwards compatibility.
        if opts.space_type != SpaceType::Temporary {
            let sys_schema = SystemSpace::Schema.as_space();
            sys_schema.replace(&("max_id", id))?;
        }

        Ok(())
    })();

    if let Err(e) = res {
        // If we were already in the transaction before calling this function,
        // the user can choose to ignore the result and commit the transaction
        // anyway. This most likely would be a logic error, because we would've
        // already rolled back any changes made by the caller and box_txn_commit
        // would silently return ok, but unfortunately there's nothing we can do
        // about it.
        transaction::rollback()?;
        return Err(e);
    }

    if !nested_transaction {
        transaction::commit()?;
    }

    // Safety: this is safe because inserting into _space didn't fail, so the
    // space has been created.
    let space = unsafe { Space::from_id_unchecked(id) };
    Ok(space)
}

#[deprecated = "use `tarantool::space::Metadata` instead"]
pub type SpaceMetadata<'a> = Metadata<'a>;

/// Returns `None` if fully temporary spaces aren't supported in the current
/// tarantool executable.
fn space_id_temporary_min() -> Option<SpaceId> {
    // Safety: this is safe because we only create space in tx thread.
    unsafe {
        static mut VALUE: Option<Option<SpaceId>> = None;
        if VALUE.is_none() {
            VALUE = Some(
                crate::lua_state()
                    .eval("return box.schema.SPACE_ID_TEMPORARY_MIN")
                    .ok(),
            )
        }
        VALUE.unwrap()
    }
}

/// Implementation ported from box_generate_space_id.
/// <https://github.com/tarantool/tarantool/blob/70e423e92fc00df2ffe385f31dae9ea8e1cc1732/src/box/box.cc#L5737>
fn generate_space_id(is_temporary: bool) -> Result<SpaceId, Error> {
    let sys_space = SystemSpace::Space.as_space();
    let (id_range_min, id_range_max);
    if is_temporary {
        id_range_min = unwrap_or!(space_id_temporary_min(), {
            set_error!(
                TarantoolErrorCode::Unsupported,
                "fully temporary space api is not supported in the current tarantool executable"
            );
            return Err(TarantoolError::last().into());
        });
        id_range_max = space::SPACE_ID_MAX + 1;
    } else {
        id_range_min = space::SYSTEM_ID_MAX + 1;
        id_range_max = space_id_temporary_min().unwrap_or(space::SPACE_ID_MAX + 1);
    };

    let mut iter = sys_space.select(IteratorType::LT, &[id_range_max])?;
    let tuple = iter.next().expect("there's always at least system spaces");
    let mut max_id: SpaceId = tuple
        .field(0)
        .expect("space metadata should decode fine")
        .expect("space id should always be present");

    let find_next_unused_id = |start: SpaceId| -> Result<SpaceId, Error> {
        let iter = sys_space.select(IteratorType::GE, &[start])?;
        let mut next_id = start;
        for tuple in iter {
            let id: SpaceId = tuple
                .field(0)
                .expect("space metadata should decode fine")
                .expect("space id should always be present");
            if id != next_id {
                // Found a hole in the id range.
                return Ok(next_id);
            }
            next_id += 1;
        }
        Ok(next_id)
    };

    if max_id < id_range_min {
        max_id = id_range_min;
    }

    let mut space_id = find_next_unused_id(max_id)?;
    if space_id >= id_range_max {
        space_id = find_next_unused_id(id_range_min)?;
        if space_id >= id_range_max {
            set_error!(TarantoolErrorCode::CreateSpace, "space id limit is reached");
            return Err(TarantoolError::last().into());
        }
    }

    Ok(space_id)
}

pub fn space_metadata(space_id: SpaceId) -> Result<Metadata<'static>, Error> {
    let sys_space = SystemSpace::VSpace.as_space();
    let tuple = sys_space.get(&[space_id])?.ok_or(Error::MetaNotFound)?;
    tuple.decode::<Metadata>()
}

/// Drop a space.
pub fn drop_space(space_id: SpaceId) -> Result<(), Error> {
    // Delete automatically generated sequence.
    let sys_space_sequence: Space = SystemSpace::SpaceSequence.into();
    if let Some(t) = sys_space_sequence.get(&(space_id,))? {
        sys_space_sequence.delete(&(space_id,))?;
        let is_generated = t.field::<bool>(2)?.unwrap();
        if is_generated {
            let seq_id = t.field::<u32>(1)?.unwrap();
            schema_seq::drop_sequence(seq_id)?;
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
