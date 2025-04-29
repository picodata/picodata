use crate::schema::{fields_to_format, Distribution, PrivilegeType, SchemaObjectType};
use crate::schema::{IndexDef, IndexOption};
use crate::schema::{PrivilegeDef, RoutineDef, UserDef};
use crate::schema::{ADMIN_ID, PUBLIC_ID, UNIVERSE_ID};
use crate::storage::Catalog;
use crate::storage::RoutineId;
use crate::storage::{set_local_schema_version, space_by_id_unchecked};
use crate::traft::error::Error;
use crate::traft::op::Ddl;
use crate::{column_name, traft};
use serde::Serialize;
use std::collections::HashMap;
use std::ffi::CString;
use std::str::FromStr;
use tarantool::auth::AuthDef;
use tarantool::decimal::Decimal;
use tarantool::error::{BoxError, Error as TntError, TarantoolErrorCode as TntErrorCode};
use tarantool::index::FieldType as IndexFieldType;
#[allow(unused_imports)]
use tarantool::index::Metadata as IndexMetadata;
use tarantool::index::{Index, IndexId, IndexType, IteratorType};
use tarantool::index::{IndexOptions, Part};
use tarantool::schema::index::{create_index, drop_index};
use tarantool::session::UserId;
use tarantool::space::UpdateOps;
use tarantool::space::{Space, SpaceId, SystemSpace};
use tarantool::tlua::{self, LuaError};
use tarantool::tuple::Encode;

////////////////////////////////////////////////////////////////////////////////
// ddl meta
////////////////////////////////////////////////////////////////////////////////

/// Updates the field `"operable"` for a space with id `space_id` and any
/// necessary entities (currently all existing indexes).
///
/// This function is called when applying the different ddl operations.
pub fn ddl_meta_space_update_operable(
    storage: &Catalog,
    space_id: SpaceId,
    operable: bool,
) -> traft::Result<()> {
    storage.tables.update_operable(space_id, operable)?;
    let iter = storage.indexes.by_space_id(space_id)?;
    for index in iter {
        storage
            .indexes
            .update_operable(index.table_id, index.id, operable)?;
    }
    Ok(())
}

/// Deletes the picodata internal metadata for a space with id `space_id`.
///
/// This function is called when applying the different ddl operations.
pub fn ddl_meta_drop_space(storage: &Catalog, space_id: SpaceId) -> traft::Result<()> {
    storage
        .privileges
        .delete_all_by_object(SchemaObjectType::Table, space_id as i64)?;
    let iter = storage.indexes.by_space_id(space_id)?;
    for index in iter {
        storage.indexes.delete(index.table_id, index.id)?;
    }
    storage.tables.delete(space_id)?;
    Ok(())
}

/// Deletes the picodata internal metadata for a routine with id `routine_id`.
pub fn ddl_meta_drop_routine(storage: &Catalog, routine_id: RoutineId) -> traft::Result<()> {
    storage
        .privileges
        .delete_all_by_object(SchemaObjectType::Routine, routine_id.into())?;
    storage.routines.delete(routine_id)?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// ddl
////////////////////////////////////////////////////////////////////////////////

pub fn ddl_abort_on_master(storage: &Catalog, ddl: &Ddl, version: u64) -> traft::Result<()> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_space = Space::from(SystemSpace::Space);
    let sys_index = Space::from(SystemSpace::Index);
    let sys_func = Space::from(SystemSpace::Func);

    match *ddl {
        Ddl::CreateTable { id, .. } => {
            sys_index.delete(&[id, 1])?;
            sys_index.delete(&[id, 0])?;
            sys_space.delete(&[id])?;
            set_local_schema_version(version)?;
        }

        Ddl::DropTable { .. } => {
            // Actual drop happens only on commit, so there's nothing to abort.
            crate::vshard::enable_rebalancer()?;
        }
        Ddl::RenameTable {
            table_id,
            ref old_name,
            ..
        } => {
            ddl_rename_table_on_master(table_id, old_name)?;
            set_local_schema_version(version)?;
        }
        Ddl::TruncateTable { .. } => {
            unreachable!("TRUNCATE execution should not reach `ddl_abort_on_master` call")
        }

        Ddl::CreateProcedure { id, .. } => {
            sys_func.delete(&[id])?;
            set_local_schema_version(version)?;
        }

        Ddl::DropProcedure { .. } => {
            // Actual drop happens only on commit, so there's nothing to abort.
        }

        Ddl::RenameProcedure {
            routine_id,
            ref old_name,
            ..
        } => {
            ddl_rename_function_on_master(storage, routine_id, old_name)?;
            set_local_schema_version(version)?;
        }

        Ddl::CreateIndex {
            space_id, index_id, ..
        } => {
            sys_index.delete(&[space_id, index_id])?;
            set_local_schema_version(version)?;
        }

        Ddl::DropIndex { .. } => {
            // Actual drop happens only on commit, so there's nothing to abort.
        }

        Ddl::ChangeFormat {
            table_id,
            ref old_format,
            // we don't need to care about column renames here because tarantool operates on column indices under the hood, yay
            ..
        } => {
            ddl_change_format_on_master(table_id, old_format)?;
            set_local_schema_version(version)?;
        }
    }

    Ok(())
}

pub fn ddl_create_index_on_master(
    storage: &Catalog,
    space_id: SpaceId,
    index_id: IndexId,
) -> traft::Result<()> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let pico_index_def = storage
        .indexes
        .get(space_id, index_id)?
        .ok_or_else(|| Error::other(format!("index with id {index_id} not found")))?;
    let mut opts = IndexOptions {
        parts: Some(pico_index_def.parts.into_iter().map(|i| i.into()).collect()),
        r#type: Some(pico_index_def.ty),
        id: Some(pico_index_def.id),
        ..Default::default()
    };
    let dec_to_f32 = |d: Decimal| f32::from_str(&d.to_string()).expect("decimal to f32");
    for opt in pico_index_def.opts {
        match opt {
            IndexOption::Unique(unique) => opts.unique = Some(unique),
            IndexOption::Dimension(dim) => opts.dimension = Some(dim),
            IndexOption::Distance(distance) => opts.distance = Some(distance),
            IndexOption::BloomFalsePositiveRate(rate) => opts.bloom_fpr = Some(dec_to_f32(rate)),
            IndexOption::PageSize(size) => opts.page_size = Some(size),
            IndexOption::RangeSize(size) => opts.range_size = Some(size),
            IndexOption::RunCountPerLevel(count) => opts.run_count_per_level = Some(count),
            IndexOption::RunSizeRatio(ratio) => opts.run_size_ratio = Some(dec_to_f32(ratio)),
            IndexOption::Hint(_) => {
                // FIXME: `hint` option is disabled in Tarantool module.
            }
        }
    }
    create_index(space_id, &pico_index_def.name, &opts)?;

    Ok(())
}

pub fn ddl_drop_index_on_master(space_id: SpaceId, index_id: IndexId) -> traft::Result<()> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    drop_index(space_id, index_id)?;

    Ok(())
}

// Metadata for a dummy function to be created on master in _func space.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct FunctionMetadata<'a> {
    pub id: RoutineId,
    pub owner: UserId,
    pub name: &'a str,
    pub setuid: u32,
    pub language: &'static str,
    pub body: String,
    pub routine_type: &'static str,
    pub param_list: Vec<&'static str>,
    pub returns: &'static str,
    pub aggregate: &'static str,
    pub sql_data_access: &'static str,
    pub is_deterministic: bool,
    pub is_sandboxed: bool,
    pub is_null_call: bool,
    pub exports: Vec<&'static str>,
    pub opts: HashMap<String, String>,
    pub comment: &'static str,
    pub created: String,
    pub last_altered: String,
}

impl<'a> From<&'a RoutineDef> for FunctionMetadata<'a> {
    fn from(def: &'a RoutineDef) -> Self {
        // Note: The default values are in [box.schema.func.create](https://git.picodata.io/picodata/tarantool/-/blob/2.11.2-picodata/src/box/lua/schema.lua?ref_type=heads#L3098)
        FunctionMetadata {
            id: def.id,
            owner: def.owner,
            name: &def.name,
            setuid: 0,
            language: "LUA",
            body: format!(
                "function() error(\"function {} is used internally by picodata\") end",
                def.name
            ),
            routine_type: "function",
            param_list: Vec::new(),
            returns: "any",
            aggregate: "none",
            sql_data_access: "none",
            is_deterministic: false,
            is_sandboxed: false,
            is_null_call: true,
            exports: vec!["LUA"],
            opts: HashMap::new(),
            comment: "",
            created: time::OffsetDateTime::now_utc().to_string(),
            last_altered: time::OffsetDateTime::now_utc().to_string(),
        }
    }
}

impl Encode for FunctionMetadata<'_> {}

/// Create tarantool function which throws an error if it's called.
/// It's safe to call this rust function multiple times.
pub fn ddl_create_function_on_master(storage: &Catalog, func_id: u32) -> traft::Result<()> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

    let routine_def = storage
        .routines
        .by_id(func_id)?
        .ok_or_else(|| Error::other(format!("routine with id {func_id} not found")))?;
    let func_space = Space::from(SystemSpace::Func);

    func_space.put(&FunctionMetadata::from(&routine_def))?;
    Ok(())
}

pub fn ddl_rename_table_on_master(table_id: u32, new_name: &str) -> traft::Result<()> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

    let new_name_c_string =
        CString::new(new_name).expect("table name shouldn't contain interior null byte");
    let rc = unsafe { sql_rename_table(table_id, new_name_c_string.as_ptr()) };
    if rc == -1 {
        return Err(Error::other(format!(
            "error while renaming table with id {table_id} to {new_name}",
        )));
    }

    return Ok(());

    extern "C" {
        fn sql_rename_table(
            table_id: core::ffi::c_uint,
            new_table_name: *const core::ffi::c_char,
        ) -> core::ffi::c_int;
    }
}

pub fn ddl_rename_function_on_master(
    storage: &Catalog,
    id: u32,
    new_name: &str,
) -> traft::Result<()> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

    let routine_def = storage
        .routines
        .by_id(id)?
        .ok_or_else(|| Error::other(format!("routine with id {id} not found")))?;
    let func_space = Space::from(SystemSpace::Func);
    let mut meta = FunctionMetadata::from(&routine_def);

    // function does not support alter, so we need to delete it first
    func_space.delete(&[id])?;

    // update name of the procedure
    meta.name = new_name;
    func_space.put(&FunctionMetadata::from(&routine_def))?;

    Ok(())
}

/// Drop tarantool function created by ddl_create_function_on_master and it's privileges.
/// Dropping a non-existent function is not an error.
///
// FIXME: this function returns 2 kinds of errors: retryable and non-retryable.
// Currently this is impelemnted by returning one kind of errors as Err(e) and
// the other as Ok(Some(e)). This was the simplest solution at the time this
// function was implemented, as it requires the least amount of boilerplate and
// error forwarding code. But this signature is not intuitive, so maybe there's
// room for improvement.
pub fn ddl_drop_function_on_master(func_id: u32) -> traft::Result<Option<TntError>> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_func = Space::from(SystemSpace::Func);
    let sys_priv = Space::from(SystemSpace::Priv);

    let priv_idx: Index = sys_priv
        .index("object")
        .expect("index 'object' not found in space '_priv'");
    let iter = priv_idx.select(IteratorType::Eq, &("function", func_id))?;
    let mut priv_ids = Vec::new();
    for tuple in iter {
        let grantee: u32 = tuple
            .field(1)?
            .expect("decoding metadata should never fail");
        priv_ids.push((grantee, "function", func_id));
    }

    let res = (|| -> tarantool::Result<()> {
        for pk_tuple in priv_ids.iter().rev() {
            sys_priv.delete(pk_tuple)?;
        }
        sys_func.delete(&[func_id])?;
        Ok(())
    })();
    Ok(res.err())
}

/// Create tarantool space and any required indexes. Currently it creates a
/// primary index and a `bucket_id` index if it's a sharded space.
///
/// Return values:
/// * `Ok(None)` in case of success.
/// * `Ok(Some(abort_reason))` in case of error which should result in a ddl abort.
/// * `Err(e)` in case of retryable errors.
///
// FIXME: this function returns 2 kinds of errors: retryable and non-retryable.
// Currently this is implemented by returning one kind of errors as Err(e) and
// the other as Ok(Some(e)). This was the simplest solution at the time this
// function was implemented, as it requires the least amount of boilerplate and
// error forwarding code. But this signature is not intuitive, so maybe there's
// room for improvement.
pub fn ddl_create_space_on_master(
    storage: &Catalog,
    space_id: SpaceId,
) -> traft::Result<Option<TntError>> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_space = Space::from(SystemSpace::Space);
    let sys_index = Space::from(SystemSpace::Index);

    let pico_space_def = storage
        .tables
        .get(space_id)?
        .ok_or_else(|| Error::other(format!("space with id {space_id} not found")))?;
    // TODO: set defaults
    let tt_space_def = pico_space_def.to_space_metadata()?;

    let pico_pk_def = storage.indexes.get(space_id, 0)?.ok_or_else(|| {
        Error::other(format!(
            "primary index for space {} not found",
            pico_space_def.name
        ))
    })?;
    let tt_pk_def = pico_pk_def.to_index_metadata(&pico_space_def);

    let bucket_id_def = match &pico_space_def.distribution {
        Distribution::ShardedImplicitly { .. } => {
            let index = IndexDef {
                table_id: pico_space_def.id,
                id: 1,
                name: "bucket_id".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(false)],
                parts: vec![Part::field("bucket_id")
                    .field_type(IndexFieldType::Unsigned)
                    .is_nullable(false)],
                operable: false,
                schema_version: pico_space_def.schema_version,
            };
            Some(index)
        }
        _ => None,
    };

    let res = (|| -> tarantool::Result<()> {
        if tt_pk_def.parts.is_empty() {
            return Err(BoxError::new(
                tarantool::error::TarantoolErrorCode::ModifyIndex,
                format!(
                    "can't create index '{}' in space '{}': parts list cannot be empty",
                    tt_pk_def.name, tt_space_def.name,
                ),
            )
            .into());
        }
        sys_space.insert(&tt_space_def)?;
        sys_index.insert(&tt_pk_def)?;
        if let Some(def) = bucket_id_def {
            sys_index.insert(&def.to_index_metadata(&pico_space_def))?;
        }

        Ok(())
    })();
    Ok(res.err())
}

/// Drop tarantool space and any entities which depend on it (indexes, privileges
/// and truncates).
///
/// Return values:
/// * `Ok(None)` in case of success.
/// * `Ok(Some(abort_reason))` in case of error which should result in a ddl abort.
/// * `Err(e)` in case of retryable errors.
///
// FIXME: this function returns 2 kinds of errors: retryable and non-retryable.
// Currently this is impelemnted by returning one kind of errors as Err(e) and
// the other as Ok(Some(e)). This was the simplest solution at the time this
// function was implemented, as it requires the least amount of boilerplate and
// error forwarding code. But this signature is not intuitive, so maybe there's
// room for improvement.
pub fn ddl_drop_space_on_master(space_id: SpaceId) -> traft::Result<Option<TntError>> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let sys_space = Space::from(SystemSpace::Space);
    let sys_index = Space::from(SystemSpace::Index);
    let sys_truncate = Space::from(SystemSpace::Truncate);
    let sys_priv = Space::from(SystemSpace::Priv);

    let iter = sys_index.select(IteratorType::Eq, &[space_id])?;
    let mut index_ids = Vec::with_capacity(4);
    for tuple in iter {
        let index_id: IndexId = tuple
            .field(1)?
            .expect("decoding metadata should never fail");
        // Primary key is handled explicitly.
        if index_id != 0 {
            index_ids.push(index_id);
        }
    }

    let priv_idx: Index = sys_priv
        .index("object")
        .expect("index 'object' not found in space '_priv'");
    let iter = priv_idx.select(IteratorType::Eq, &("space", space_id))?;
    let mut priv_ids = Vec::new();
    for tuple in iter {
        let grantee: u32 = tuple
            .field(1)?
            .expect("decoding metadata should never fail");
        priv_ids.push((grantee, "space", space_id));
    }

    let res = (|| -> tarantool::Result<()> {
        for pk_tuple in priv_ids.iter().rev() {
            sys_priv.delete(pk_tuple)?;
        }
        for iid in index_ids.iter().rev() {
            sys_index.delete(&(space_id, iid))?;
        }
        // Primary key must be dropped last.
        sys_index.delete(&(space_id, 0))?;
        sys_truncate.delete(&[space_id])?;
        sys_space.delete(&[space_id])?;

        Ok(())
    })();
    Ok(res.err())
}

/// Truncate tarantool space.
///
/// Return values:
/// * `Ok(None)` in case of success.
/// * `Ok(Some(abort_reason))` in case of error which should result in a ddl abort.
/// * `Err(e)` in case of retryable errors.
///
// FIXME: this function returns 2 kinds of errors: retryable and non-retryable.
// Currently this is impelemnted by returning one kind of errors as Err(e) and
// the other as Ok(Some(e)). This was the simplest solution at the time this
// function was implemented, as it requires the least amount of boilerplate and
// error forwarding code. But this signature is not intuitive, so maybe there's
// room for improvement.
pub fn ddl_truncate_space_on_master(space_id: SpaceId) -> traft::Result<Option<TntError>> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });
    let space = space_by_id_unchecked(space_id);

    let res = (|| -> tarantool::Result<()> {
        space.truncate()?;

        Ok(())
    })();
    Ok(res.err())
}

/// Change tarantool space format.
///
/// Return values:
/// * `Ok(())` in case of success.
/// * `Err(e)` in case of error which should result in a ddl abort.
pub fn ddl_change_format_on_master(
    space_id: SpaceId,
    format: &[tarantool::space::Field],
) -> Result<(), TntError> {
    debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

    let format = fields_to_format(format);
    let sys_space = Space::from(SystemSpace::Space);
    let mut ops = UpdateOps::with_capacity(1);
    ops.assign(column_name!(tarantool::space::Metadata, format), format)?;
    sys_space.update(&[space_id], ops)?;
    Ok(())
}

pub fn ddl_create_tt_proc_on_master(proc_name: &str) -> traft::Result<()> {
    let lua = ::tarantool::lua_state();
    let proc = ::tarantool::proc::all_procs()
        .iter()
        .find(|p| p.name() == proc_name)
        .ok_or_else(|| {
            Error::other(format!(
                "cannot find procedure {proc_name} in `proc::all_procs` for schema creation"
            ))
        })?;
    if sbroad::frontend::sql::NAMES_OF_FUNCTIONS_IN_SOURCES.contains(&proc_name) {
        lua.exec_with(
            "local name, is_public = ...
            local proc_name = '.' .. name
            box.schema.func.create(proc_name, {language = 'C', if_not_exists = true, exports = {'LUA', 'SQL'}, returns = 'any'})
            if is_public then
                box.schema.role.grant('public', 'execute', 'function', proc_name, {if_not_exists = true})
            end
            ",
            (proc_name, proc.is_public()),
        )
        .map_err(LuaError::from)?;
    } else {
        lua.exec_with(
            "local name, is_public = ...
            local proc_name = '.' .. name
            box.schema.func.create(proc_name, {language = 'C', if_not_exists = true})
            if is_public then
                box.schema.role.grant('public', 'execute', 'function', proc_name, {if_not_exists = true})
            end
            ",
            (proc_name, proc.is_public()),
        )
        .map_err(LuaError::from)?;
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// acl
////////////////////////////////////////////////////////////////////////////////

pub mod acl {
    use tarantool::clock;

    use crate::access_control::user_by_id;

    use super::*;

    impl PrivilegeDef {
        /// Resolve grantee's type and name and return them as strings.
        /// Panics if the storage's invariants do not uphold.
        fn grantee_type_and_name(
            &self,
            storage: &Catalog,
        ) -> tarantool::Result<(&'static str, String)> {
            let user_def = storage.users.by_id(self.grantee_id())?;
            let Some(user_def) = user_def else {
                panic!("found neither user nor role for grantee_id")
            };

            if user_def.is_role() {
                Ok(("role", user_def.name))
            } else {
                Ok(("user", user_def.name))
            }
        }
    }

    fn get_default_privileges_for_user(
        user_def: &UserDef,
        grantor_id: UserId,
    ) -> [PrivilegeDef; 3] {
        [
            // SQL: GRANT 'public' TO <user_name>
            PrivilegeDef::new(
                grantor_id,
                user_def.id,
                PrivilegeType::Execute,
                SchemaObjectType::Role,
                PUBLIC_ID as _,
                user_def.schema_version,
            )
            .expect("valid"),
            // SQL: ALTER USER <user_name> WITH LOGIN
            PrivilegeDef::new(
                ADMIN_ID,
                user_def.id,
                PrivilegeType::Login,
                SchemaObjectType::Universe,
                UNIVERSE_ID,
                user_def.schema_version,
            )
            .expect("valid"),
            // SQL: GRANT ALTER ON <user_name> TO <user_name>
            PrivilegeDef::new(
                grantor_id,
                user_def.id,
                PrivilegeType::Alter,
                SchemaObjectType::User,
                user_def.id as _,
                user_def.schema_version,
            )
            .expect("valid"),
        ]
    }

    ////////////////////////////////////////////////////////////////////////////
    // acl in global storage
    ////////////////////////////////////////////////////////////////////////////

    /// Persist a user definition with it's default privileges in the internal clusterwide storage.
    pub fn global_create_user(storage: &Catalog, user_def: &UserDef) -> tarantool::Result<()> {
        storage.users.insert(user_def)?;

        let owner_def = user_by_id(user_def.owner)?;

        let user = &user_def.name;
        crate::audit!(
            message: "created user `{user}`",
            title: "create_user",
            severity: High,
            auth_type: user_def.auth.as_ref().expect("user always should have non empty auth").method.as_str(),
            user: user,
            initiator: owner_def.name,
        );

        for user_priv in get_default_privileges_for_user(user_def, user_def.owner) {
            global_grant_privilege(storage, &user_priv)?;
        }

        Ok(())
    }

    /// Change user's auth info in the internal clusterwide storage.
    pub fn global_change_user_auth(
        storage: &Catalog,
        user_id: UserId,
        auth: &AuthDef,
        initiator: UserId,
    ) -> tarantool::Result<()> {
        storage.users.update_auth(user_id, auth)?;

        let user_def = storage.users.by_id(user_id)?.expect("failed to get user");
        let user = &user_def.name;

        let initiator_def = user_by_id(initiator)?;

        crate::audit!(
            message: "password of user `{user}` was changed",
            title: "change_password",
            severity: High,
            auth_type: auth.method.as_str(),
            user: user,
            initiator: initiator_def.name,
        );

        Ok(())
    }

    /// Change user's name in the internal clusterwide storage.
    pub fn global_rename_user(
        storage: &Catalog,
        user_id: UserId,
        new_name: &str,
        initiator: UserId,
    ) -> tarantool::Result<()> {
        let user_with_old_name = storage.users.by_id(user_id)?.expect("failed to get user");
        let old_name = &user_with_old_name.name;
        storage.users.update_name(user_id, new_name)?;

        let initiator_def = user_by_id(initiator)?;

        crate::audit!(
            message: "name of user `{old_name}` was changed to `{new_name}`",
            title: "rename_user",
            severity: High,
            old_name: old_name,
            new_name: new_name,
            initiator: initiator_def.name,
        );

        Ok(())
    }

    /// Remove a user with no dependent objects.
    pub fn global_drop_user(
        storage: &Catalog,
        user_id: UserId,
        initiator: UserId,
    ) -> traft::Result<()> {
        let user_def = storage.users.by_id(user_id)?.ok_or_else(|| {
            BoxError::new(TntErrorCode::NoSuchUser, format!("no such user #{user_id}"))
        })?;
        user_def.ensure_no_dependent_objects(storage)?;

        storage.privileges.delete_all_by_grantee_id(user_id)?;
        storage.users.delete(user_id)?;

        let initiator_def = user_by_id(initiator)?;

        let user = &user_def.name;
        crate::audit!(
            message: "dropped user `{user}`",
            title: "drop_user",
            severity: Medium,
            user: user,
            initiator: initiator_def.name,
        );

        Ok(())
    }

    /// Persist a role definition in the internal clusterwide storage.
    pub fn global_create_role(storage: &Catalog, role_def: &UserDef) -> tarantool::Result<()> {
        storage.users.insert(role_def)?;

        let initiator_def = user_by_id(role_def.owner)?;

        let role = &role_def.name;
        crate::audit!(
            message: "created role `{role}`",
            title: "create_role",
            severity: High,
            role: role,
            initiator: initiator_def.name,
        );

        Ok(())
    }

    /// Remove a role definition and any entities owned by it from the internal
    /// clusterwide storage.
    pub fn global_drop_role(
        storage: &Catalog,
        role_id: UserId,
        initiator: UserId,
    ) -> traft::Result<()> {
        let role_def = storage.users.by_id(role_id)?.expect("role should exist");
        storage.privileges.delete_all_by_grantee_id(role_id)?;
        storage.privileges.delete_all_by_granted_role(role_id)?;
        storage.users.delete(role_id)?;

        let initiator_def = user_by_id(initiator)?;

        let role = &role_def.name;
        crate::audit!(
            message: "dropped role `{role}`",
            title: "drop_role",
            severity: Medium,
            role: role,
            initiator: initiator_def.name,
        );
        Ok(())
    }

    /// Persist a privilege definition in the internal clusterwide storage.
    pub fn global_grant_privilege(
        storage: &Catalog,
        priv_def: &PrivilegeDef,
    ) -> tarantool::Result<()> {
        storage.privileges.insert(priv_def, true)?;

        let privilege = &priv_def.privilege();
        let object = priv_def
            .resolve_object_name(storage)
            .expect("target object should exist");
        let object_type = &priv_def.object_type();
        let (grantee_type, grantee) = priv_def.grantee_type_and_name(storage)?;
        let initiator_def = user_by_id(priv_def.grantor_id())?;

        // Reset login attempts counter for a user on session grant
        if *privilege == PrivilegeType::Login {
            // Borrowing will not panic as there are no yields while it's borrowed
            storage.login_attempts.borrow_mut().remove(&grantee);
        }

        // Emit audit log
        match (privilege.as_str(), object_type.as_str()) {
            ("execute", "role") => {
                let object = object.expect("should be set");
                crate::audit!(
                    message: "granted role `{object}` to {grantee_type} `{grantee}`",
                    title: "grant_role",
                    severity: High,
                    role: &object,
                    grantee: &grantee,
                    grantee_type: grantee_type,
                    initiator: initiator_def.name,
                );
            }
            _ => {
                let object = object.as_deref().unwrap_or("*");
                crate::audit!(
                    message: "granted privilege {privilege} \
                        on {object_type} `{object}` \
                        to {grantee_type} `{grantee}`",
                    title: "grant_privilege",
                    severity: High,
                    privilege: privilege.as_str(),
                    object: object,
                    object_type: object_type.as_str(),
                    grantee: &grantee,
                    grantee_type: grantee_type,
                    initiator: initiator_def.name,
                );
            }
        }

        Ok(())
    }

    /// Remove a privilege definition from the internal clusterwide storage.
    pub fn global_revoke_privilege(
        storage: &Catalog,
        priv_def: &PrivilegeDef,
        initiator: UserId,
    ) -> tarantool::Result<()> {
        storage.privileges.delete(
            priv_def.grantee_id(),
            &priv_def.object_type(),
            priv_def.object_id_raw(),
            &priv_def.privilege(),
        )?;

        let privilege = &priv_def.privilege();
        let object = priv_def
            .resolve_object_name(storage)
            .expect("target object should exist");
        let object_type = &priv_def.object_type();
        let (grantee_type, grantee) = priv_def.grantee_type_and_name(storage)?;
        let initiator_def = user_by_id(initiator)?;

        match (privilege.as_str(), object_type.as_str()) {
            ("execute", "role") => {
                let object = object.expect("should be set");
                crate::audit!(
                    message: "revoked role `{object}` from {grantee_type} `{grantee}`",
                    title: "revoke_role",
                    severity: High,
                    role: &object,
                    grantee: &grantee,
                    grantee_type: grantee_type,
                    initiator: initiator_def.name,
                );
            }
            _ => {
                let object = object.as_deref().unwrap_or("*");
                crate::audit!(
                    message: "revoked privilege {privilege} \
                        on {object_type} `{object}` \
                        from {grantee_type} `{grantee}`",
                    title: "revoke_privilege",
                    severity: High,
                    privilege: privilege.as_str(),
                    object: object,
                    object_type: object_type.as_str(),
                    grantee: &grantee,
                    grantee_type: grantee_type,
                    initiator: initiator_def.name,
                );
            }
        }

        Ok(())
    }

    ////////////////////////////////////////////////////////////////////////////
    // acl in local storage on replicaset leader
    ////////////////////////////////////////////////////////////////////////////

    /// Create a tarantool user.
    ///
    /// If `basic_privileges` is `true` the new user is granted the following:
    /// - Role "public"
    /// - Alter self
    /// - Session access on "universe"
    /// - Usage access on "universe"
    pub fn on_master_create_user(
        user_def: &UserDef,
        basic_privileges: bool,
    ) -> tarantool::Result<()> {
        let sys_user = Space::from(SystemSpace::User);
        let user_id = user_def.id;

        // This implementation was copied from box.schema.user.create excluding the
        // password hashing.

        let auth_def = user_def
            .auth
            .as_ref()
            .expect("user always should have non empty auth");

        // Tarantool expects auth info to be a map of form `{ method: data }`,
        // and currently the simplest way to achieve this is to use a HashMap.
        let auth_map = HashMap::from([(auth_def.method, &auth_def.data)]);
        sys_user.insert(&(
            user_id,
            user_def.owner,
            &user_def.name,
            "user",
            auth_map,
            &[(); 0],
            0,
        ))?;

        if !basic_privileges {
            return Ok(());
        }

        let lua = ::tarantool::lua_state();
        lua.exec_with("box.schema.user.grant(...)", (user_id, "public"))
            .map_err(LuaError::from)?;
        lua.exec_with(
            "box.schema.user.grant(...)",
            (user_id, "alter", "user", user_id),
        )
        .map_err(LuaError::from)?;
        lua.exec_with(
            "box.session.su('admin', box.schema.user.grant, ...)",
            (
                user_id,
                "session,usage",
                "universe",
                tlua::Nil,
                tlua::AsTable((("if_not_exists", true),)),
            ),
        )
        .map_err(LuaError::from)?;

        Ok(())
    }

    /// Update a tarantool user's authentication details.
    pub fn on_master_change_user_auth(user_id: UserId, auth: &AuthDef) -> tarantool::Result<()> {
        const USER_FIELD_AUTH: i32 = 4;
        const USER_FIELD_LAST_MODIFIED: i32 = 6;
        let sys_user = Space::from(SystemSpace::User);

        // Tarantool expects auth info to be a map of form `{ method: data }`,
        // and currently the simplest way to achieve this is to use a HashMap.
        let auth_map = HashMap::from([(auth.method, &auth.data)]);
        let mut ops = UpdateOps::with_capacity(2);
        ops.assign(USER_FIELD_AUTH, auth_map)?;
        ops.assign(USER_FIELD_LAST_MODIFIED, clock::time64())?;
        sys_user.update(&[user_id], ops)?;
        Ok(())
    }

    /// Rename a tarantool user.
    pub fn on_master_rename_user(user_id: UserId, new_name: &str) -> tarantool::Result<()> {
        const USER_FIELD_NAME: i32 = 2;
        let sys_user = Space::from(SystemSpace::User);

        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(USER_FIELD_NAME, new_name)?;
        sys_user.update(&[user_id], ops)?;
        Ok(())
    }

    /// Drop a tarantool user and any entities (spaces, etc.) owned by it.
    pub fn on_master_drop_user(user_id: UserId) -> tarantool::Result<()> {
        let lua = ::tarantool::lua_state();
        lua.exec_with("box.schema.user.drop(...)", user_id)
            .map_err(LuaError::from)?;
        Ok(())
    }

    /// Create a tarantool role.
    pub fn on_master_create_role(role_def: &UserDef) -> tarantool::Result<()> {
        let sys_user = Space::from(SystemSpace::User);

        // This implementation was copied from box.schema.role.create.

        // Tarantool expects auth info to be a map `{}`, and currently the simplest
        // way to achieve this is to use a HashMap.
        sys_user.insert(&(
            role_def.id,
            role_def.owner,
            &role_def.name,
            "role",
            HashMap::<(), ()>::new(),
            &[(); 0],
            0,
        ))?;

        Ok(())
    }

    /// Drop a tarantool role and revoke it from anybody it was assigned to.
    pub fn on_master_drop_role(role_id: UserId) -> tarantool::Result<()> {
        let lua = ::tarantool::lua_state();
        lua.exec_with("box.schema.role.drop(...)", role_id)
            .map_err(LuaError::from)?;

        Ok(())
    }

    /// Grant a tarantool user or role the privilege defined by `priv_def`.
    /// Is idempotent: will not return an error even if the privilege is already granted.
    pub fn on_master_grant_privilege(priv_def: &PrivilegeDef) -> tarantool::Result<()> {
        let lua = ::tarantool::lua_state();
        lua.exec_with(
            "local grantee_id, privilege, object_type, object_id = ...
            local grantee_def = box.space._user:get(grantee_id)
            if grantee_def.type == 'user' then
                box.schema.user.grant(grantee_id, privilege, object_type, object_id, {if_not_exists=true})
            else
                box.schema.role.grant(grantee_id, privilege, object_type, object_id, {if_not_exists=true})
            end",
            (
                priv_def.grantee_id(),
                priv_def.privilege().as_tarantool(),
                priv_def.object_type().as_tarantool(),
                priv_def.object_id(),
            ),
        )
        .map_err(LuaError::from)?;

        Ok(())
    }

    /// Revoke a privilege from a tarantool user or role.
    /// Is idempotent: will not return an error even if the privilege was not granted.
    pub fn on_master_revoke_privilege(priv_def: &PrivilegeDef) -> tarantool::Result<()> {
        let lua = ::tarantool::lua_state();
        lua.exec_with(
            "local grantee_id, privilege, object_type, object_id = ...
            local grantee_def = box.space._user:get(grantee_id)
            if not grantee_def then
                -- Grantee already dropped -> privileges already revoked
                return
            end
            if grantee_def.type == 'user' then
                box.schema.user.revoke(grantee_id, privilege, object_type, object_id, {if_exists=true})
            else
                box.schema.role.revoke(grantee_id, privilege, object_type, object_id, {if_exists=true})
            end",
            (
                priv_def.grantee_id(),
                priv_def.privilege().as_tarantool(),
                priv_def.object_type().as_tarantool(),
                priv_def.object_id(),
            ),
        )
        .map_err(LuaError::from)?;

        Ok(())
    }
}
