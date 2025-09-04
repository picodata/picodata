//! Access control
//!
//! This module implements access control checks according to picodata access control model.
//!
//! Currently objects we control access to include tables, users and roles. The model closely follows one from
//! vanilla tarantool with some deviations. We build on top of tarantool infrastructure, i e in vanilla tarantool
//! permissions are represented with bit masks for each pair of object and user (for more context see `user_def.h`).
//! So we do not reimplement it in picodata, instead we have two functions exported from vanilla tarantool named
//! `box_access_check_space` and `box_access_check_ddl`. The first one as the name suggests implements checks for
//! DML operations on spaces, the second one allows to check remaining permissions with exception of some edge cases
//! that are handled separately in `access_check_grant_revoke` below. `access_check_grant_revoke` closely follows
//! `priv_def_check` from `alter.cc`. In vanilla ddl permission checks are performed via triggers attached to system spaces.
//! for example `on_replace_dd_space` or `on_replace_dd_priv`. Checks implemented in this module are 1:1 mapping of those.
//!
//! Since picodata is a distributed database we perform clusterwide operations in a coordinated fashion using compare and swap
//! building block. So every clusterwide operation has to go through raft leader that takes care of safely distributing this
//! operation across all instances. Because of that we have a distinction, privileges that are related to operations that change
//! state of the cluster (e g via creating users, new tables, writing to global tables) are checked before going forward with CaS
//! operation. On the other size permissions that do not change state of the cluster (reading and writing to sharded tables)
//! are handled by our sql processing logic in `sql.rs`.
//!
//! Executing CaS operation involves coordination between nodes of the cluster, since cluster nodes authenticate with each other
//! without involving end user credentials we need to pass the user who initiated the request along with the request to raft
//! leader so it can perform the access check. In order for this to work properly node that initiates the request needs to access
//! certain system spaces (see [`crate::cas::compare_and_swap`] for details). Connected users do not have such a permission from vanilla
//! tarantool perspective so we bypass vanilla checks by using `box.session.su(ADMIN)`. Then on the raft leader box.session.su
//! is used again to switch to user provided in the request. This is needed because tarantool functions we use for access checks
//! make them based on effective user.
use std::collections::{HashMap, HashSet};

use tarantool::auth::AuthMethod;
use tarantool::error::TarantoolErrorCode::{self, AccessDenied};
use tarantool::{
    access_control::{
        box_access_check_ddl, box_access_check_space, PrivType,
        SchemaObjectType as TntSchemaObjectType,
    },
    error::BoxError,
    session::{self, UserId},
    space::{Space, SystemSpace},
    tuple::Encode,
};

use crate::storage::SPACE_ID_INTERNAL_MAX;
use crate::traft::error::Error;
use crate::traft::op::Dml;
use crate::{
    schema::{
        PrivilegeDef, PrivilegeType, SchemaObjectType as PicoSchemaObjectType, ADMIN_ID,
        PICO_SERVICE_ID, PICO_SERVICE_USER_NAME,
    },
    storage::{make_routine_not_found, space_by_id, Catalog, ToEntryIter},
    traft::{
        self,
        op::{self, Op},
    },
};

const SPECIAL_CHARACTERS: [char; 6] = ['&', '|', '?', '!', '$', '@'];

tarantool::define_str_enum! {
    pub enum UserMetadataKind {
        User = "user",
        Role = "role",
    }
}

/// User metadata. Represents a tuple of a system `_user` space.
/// TODO move to tarantool-module along with some space specific methods from storage.rs
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UserMetadata {
    pub id: UserId,
    pub owner_id: UserId,
    pub name: String,
    #[serde(rename = "type")]
    pub ty: UserMetadataKind,
    pub auth: HashMap<String, String>,
    // Note: no way to see the real type of this thing.
    // It is an undocumented enterprise edition only field.
    // For msgpack encoding to match needs to be a sequence
    pub auth_history: [(); 0],
    pub last_modified: usize,
}

impl Encode for UserMetadata {}

fn make_no_such_user(name: &str) -> tarantool::error::Error {
    tarantool::set_error!(
        tarantool::error::TarantoolErrorCode::NoSuchUser,
        "no user named {}",
        name
    );
    tarantool::error::TarantoolError::last().into()
}

/// Get user by id. If there is no such user the last tarantool error is set to no such user
/// and returned.
pub fn user_by_id(id: UserId) -> tarantool::Result<UserMetadata> {
    user_by_id_if_exists(id)?.ok_or_else(|| {
        BoxError::new(
            TarantoolErrorCode::NoSuchUser,
            format!("no such user #{id}"),
        )
        .into()
    })
}

/// Get user by id. If there is no such user None is returned.
pub fn user_by_id_if_exists(id: UserId) -> tarantool::Result<Option<UserMetadata>> {
    let Some(sys_user) = Space::from(SystemSpace::User).get(&(id,))? else {
        return Ok(None);
    };
    sys_user.decode().map(Some)
}

pub fn validate_password(
    password: &str,
    auth_method: &AuthMethod, // reference is taken for easier API interaction
    storage: &Catalog,
) -> traft::Result<()> {
    if let AuthMethod::Ldap = auth_method {
        // LDAP doesn't need password for authentication
        return Ok(());
    }

    // This check is called from user facing API.
    // A user is not expected to have access to _pico_property
    let password_min_length =
        session::with_su(ADMIN_ID, || storage.db_config.auth_password_length_min())??;
    if password.len() < password_min_length {
        return Err(Error::Other(
            format!(
                "password is too short: expected at least {}, got {}",
                password_min_length,
                password.len()
            )
            .into(),
        ));
    }

    let password_enforce_uppercase = session::with_su(ADMIN_ID, || {
        storage.db_config.auth_password_enforce_uppercase()
    })??;
    if password_enforce_uppercase && !password.chars().any(|ch| ch.is_uppercase()) {
        return Err(Error::Other(
            "invalid password: password should contain at least one uppercase letter".into(),
        ));
    }

    let password_enforce_lowercase = session::with_su(ADMIN_ID, || {
        storage.db_config.auth_password_enforce_lowercase()
    })??;
    if password_enforce_lowercase && !password.chars().any(|ch| ch.is_lowercase()) {
        return Err(Error::Other(
            "invalid password: password should contain at least one lowercase letter".into(),
        ));
    }

    let password_enforce_digits = session::with_su(ADMIN_ID, || {
        storage.db_config.auth_password_enforce_digits()
    })??;
    if password_enforce_digits && !password.chars().any(|ch| ch.is_ascii_digit()) {
        return Err(Error::Other(
            "invalid password: password should contain at least one digit".into(),
        ));
    }

    let password_enforce_specialchars = session::with_su(ADMIN_ID, || {
        storage.db_config.auth_password_enforce_specialchars()
    })??;
    if password_enforce_specialchars && !password.chars().any(|ch| SPECIAL_CHARACTERS.contains(&ch))
    {
        return Err(Error::Other(
            format!(
                "invalid password: password should contain at least one special character - {SPECIAL_CHARACTERS:?}",
            )
            .into(),
        ));
    }

    Ok(())
}

fn forbid_drop_if_system_space(storage: &Catalog, space_id: u32) -> tarantool::Result<()> {
    if space_id > SPACE_ID_INTERNAL_MAX {
        return Ok(());
    }

    let table_name = storage
        .pico_table
        .get(space_id)?
        .map_or_else(|| format!("id={space_id}"), |table| table.name);

    return Err(BoxError::new(
        TarantoolErrorCode::AccessDenied,
        format!("Drop access to table '{table_name}' is denied for all users"),
    )
    .into());
}

/// There are no cases when box_access_check_ddl is called several times
/// in a row so it is ok that we need to switch once to user who initiated the request
/// This wrapper is needed because usually before checking permissions we need to
/// retrieve some metadata about the object being tested for access and this requires
/// access to system spaces which original user is not required to have.
///
/// # Panicking
///
/// Note that not all combinations of parameters are valid.
/// For in depth description of cases when this function may panic see
/// [box_access_check_ddl(box_access_check_ddl) in tarantool module.
fn box_access_check_ddl_as_user(
    object_name: &str,
    object_id: u32,
    owner_id: u32,
    object_type: TntSchemaObjectType,
    access: PrivType,
    as_user: UserId,
) -> tarantool::Result<()> {
    let _su = session::su(as_user)?;
    box_access_check_ddl(object_name, object_id, owner_id, object_type, access)
}

fn access_check_dml(storage: &Catalog, dml: &Dml, as_user: UserId) -> tarantool::Result<()> {
    let space_id = dml.space();
    if space_id <= SPACE_ID_INTERNAL_MAX && !is_superuser(as_user) {
        let table_name = storage
            .pico_table
            .get(space_id)?
            .map_or_else(|| format!("id={space_id}"), |table| table.name);

        return Err(tarantool::error::BoxError::new(
            TarantoolErrorCode::AccessDenied,
            format!("Write access to table '{table_name}' is denied for all users"),
        )
        .into());
    }

    let _su = session::su(as_user)?;
    box_access_check_space(space_id, PrivType::Write)
}

/// This function performs access control checks that are identical to ones performed in
/// vanilla tarantool in on_replace_dd_space, on_replace_dd_index and on_replace_dd_func respectively
fn access_check_ddl(storage: &Catalog, ddl: &op::Ddl, as_user: UserId) -> tarantool::Result<()> {
    match ddl {
        op::Ddl::CreateTable {
            id, name, owner, ..
        } => {
            assert_eq!(
                *owner, as_user,
                "when creating objects creator is the owner"
            );

            box_access_check_ddl_as_user(
                name,
                *id,
                *owner,
                TntSchemaObjectType::Space,
                PrivType::Create,
                as_user,
            )
        }
        op::Ddl::DropTable { id, .. } => {
            forbid_drop_if_system_space(storage, *id)?;

            let space = space_by_id(*id)?;
            let meta = space.meta()?;

            box_access_check_ddl_as_user(
                &space.meta()?.name,
                *id,
                meta.user_id,
                TntSchemaObjectType::Space,
                PrivType::Drop,
                as_user,
            )
        }
        op::Ddl::TruncateTable { id, .. } => {
            let space = space_by_id(*id)?;
            let meta = space.meta()?;

            box_access_check_ddl_as_user(
                &space.meta()?.name,
                *id,
                meta.user_id,
                TntSchemaObjectType::Space,
                PrivType::Write,
                as_user,
            )
        }
        op::Ddl::ChangeFormat { table_id, .. } => {
            let space = space_by_id(*table_id)?;
            let meta = space.meta()?;

            box_access_check_ddl_as_user(
                &space.meta()?.name,
                *table_id,
                meta.user_id,
                TntSchemaObjectType::Space,
                PrivType::Write,
                as_user,
            )
        }
        op::Ddl::CreateIndex { space_id, .. } => {
            let space = space_by_id(*space_id)?;
            let meta = space.meta()?;

            box_access_check_ddl_as_user(
                &meta.name,
                *space_id,
                meta.user_id,
                TntSchemaObjectType::Space,
                PrivType::Create,
                as_user,
            )
        }
        op::Ddl::DropIndex { space_id, .. } => {
            forbid_drop_if_system_space(storage, *space_id)?;

            let space = space_by_id(*space_id)?;
            let meta = space.meta()?;

            box_access_check_ddl_as_user(
                &meta.name,
                *space_id,
                meta.user_id,
                TntSchemaObjectType::Space,
                PrivType::Drop,
                as_user,
            )
        }
        op::Ddl::CreateProcedure {
            id, name, owner, ..
        } => box_access_check_ddl_as_user(
            name,
            *id,
            *owner,
            TntSchemaObjectType::Function,
            PrivType::Create,
            as_user,
        ),
        op::Ddl::DropProcedure { id, .. } => {
            let node = traft::node::global().expect("node must be already initialized");
            let Some(routine) = node.storage.routines.by_id(*id)? else {
                tarantool::set_error!(
                    tarantool::error::TarantoolErrorCode::NoSuchProc,
                    "no such procedure #{}",
                    id
                );
                return Err(tarantool::error::TarantoolError::last().into());
            };

            box_access_check_ddl_as_user(
                &routine.name,
                *id,
                routine.owner,
                TntSchemaObjectType::Function,
                PrivType::Drop,
                as_user,
            )
        }
        op::Ddl::RenameProcedure {
            routine_id,
            old_name,
            owner_id,
            ..
        } => box_access_check_ddl_as_user(
            old_name,
            *routine_id,
            *owner_id,
            TntSchemaObjectType::Function,
            PrivType::Alter,
            as_user,
        ),
        op::Ddl::RenameTable {
            table_id,
            old_name,
            owner_id,
            ..
        } => box_access_check_ddl_as_user(
            old_name,
            *table_id,
            *owner_id,
            TntSchemaObjectType::Space,
            PrivType::Alter,
            as_user,
        ),
    }
}

/// This is port of vanilla tarantool function box/user.cc::role_check.
fn detect_role_grant_cycles(
    granted_role: &UserMetadata,
    priv_def: &PrivilegeDef,
    storage: &Catalog,
) -> tarantool::Result<()> {
    let grantee_id = priv_def.grantee_id();
    let grantee_name = {
        let grantee = user_by_id(grantee_id)?;
        grantee.name
    };

    let role_id_to_grantees_id = {
        let mut hm = HashMap::new();
        for privilege in storage.privileges.iter()? {
            if privilege.object_type() == PicoSchemaObjectType::Role
                && privilege.privilege() == PrivilegeType::Execute
            {
                match privilege.object_id() {
                    Some(object_id) => {
                        hm.entry(object_id as i64)
                            .or_insert(Vec::new())
                            .push(privilege.grantee_id() as i64);
                    }
                    None => continue,
                }
            }
        }

        hm
    };

    let mut visited = HashSet::new();
    visited.insert(grantee_id as i64);

    let mut current_layer = visited.clone();
    // It's BFS with start from grantee_id
    while !current_layer.is_empty() {
        let mut next_layer = HashSet::new();

        for role_id in current_layer.iter() {
            let parents = role_id_to_grantees_id.get(role_id);
            if let Some(parents) = parents {
                next_layer.extend(parents.iter());
            }
        }

        visited.extend(next_layer.iter());
        current_layer = next_layer;
    }

    if visited.contains(&(granted_role.id as i64)) {
        let err = BoxError::new(
            tarantool::error::TarantoolErrorCode::RoleLoop,
            format!(
                "Granting role {} to role {} would create a loop",
                granted_role.name, grantee_name,
            ),
        );
        return Err(err.into());
    }

    Ok(())
}

fn access_check_grant_revoke(
    storage: &Catalog,
    priv_def: &PrivilegeDef,
    grantor_id: UserId,
    access: PrivType,
    access_name: &str,
) -> tarantool::Result<()> {
    // Note: this is vanilla tarantool user, not picodata one.
    let grantor = user_by_id(grantor_id)?;
    let user_name = &grantor.name;
    let object_type = priv_def.object_type();

    let object_id = match priv_def.object_id() {
        None => {
            // This is a wildcard grant on entire entity type i e grant on all spaces.
            // In vanilla tarantool each object type has counterpart with entity suffix
            // i e BOX_SC_ENTITY_SPACE. Since these variants are not stored we do
            // not materialize them as valid SchemaObjectType variants and streamline this check.
            if grantor_id != ADMIN_ID {
                #[rustfmt::skip]
                return Err(BoxError::new(AccessDenied, format!("{access_name} to {object_type} is denied for user '{user_name}'")).into());
            }
            return Ok(());
        }
        Some(object_id) => object_id,
    };

    // Nobody should be able to revoke privileges from pico_service
    // As it would break the cluster permanently.
    if access == PrivType::Revoke && priv_def.grantee_id() == PICO_SERVICE_ID {
        tarantool::set_error!(
            tarantool::error::TarantoolErrorCode::AccessDenied,
            "Revoke '{}' from '{}' is denied for all users",
            priv_def.privilege(),
            PICO_SERVICE_USER_NAME,
        );
        return Err(tarantool::error::TarantoolError::last().into());
    }

    match object_type {
        PicoSchemaObjectType::Universe => {
            if priv_def.privilege() != PrivilegeType::Login {
                // This assumption is used in the following checks
                crate::warn_or_panic!(
                    "Only Login privilege can be granted on Universe, got {}",
                    priv_def.privilege()
                );
                #[rustfmt::skip]
                return Err(BoxError::new(AccessDenied, format!("{access_name} to {object_type} is denied for user '{user_name}'")).into());
            }

            // Following checks are assuming that it's a grant or revoke of Login privilege

            let target_sys_user = user_by_id(priv_def.grantee_id())?;
            if target_sys_user.ty != UserMetadataKind::User {
                return Err(make_no_such_user(&target_sys_user.name));
            }

            // Only owner or admin can grant login on user
            if target_sys_user.owner_id != grantor_id && grantor_id != ADMIN_ID {
                #[rustfmt::skip]
                return Err(BoxError::new(AccessDenied, format!("{access_name} Login from '{}' is denied for '{user_name}'", target_sys_user.name)).into());
            }
        }
        PicoSchemaObjectType::Table => {
            if priv_def.privilege() == PrivilegeType::Drop {
                forbid_drop_if_system_space(storage, object_id)?;
            }

            let space = space_by_id(object_id)?;
            let meta = space.meta()?;

            assert_eq!(object_id, meta.id, "user metadata id mismatch");

            // Only owner or admin can grant on space
            if meta.user_id != grantor_id && grantor_id != ADMIN_ID {
                let table_name = meta.name;
                #[rustfmt::skip]
                return Err(BoxError::new(AccessDenied, format!("{access_name} to {object_type} '{table_name}' is denied for user '{user_name}'")).into());
            }

            return box_access_check_ddl_as_user(
                &meta.name,
                meta.id,
                grantor_id,
                TntSchemaObjectType::Space,
                access,
                grantor_id,
            );
        }
        PicoSchemaObjectType::Role => {
            let granted_role = user_by_id(object_id)?;

            assert_eq!(object_id, granted_role.id, "user metadata id mismatch");

            // Forbid revoking role "public" since it takes away permission to execute SQL queries
            if access == PrivType::Revoke && granted_role.name == "public" {
                return Err(BoxError::new(
                    AccessDenied,
                    format!("Revoking role '{}' is denied", granted_role.name),
                )
                .into());
            }

            // Only the creator of the role or admin can grant or revoke it.
            // Everyone can grant 'PUBLIC' role.
            // Note that having a role means having execute privilege on it.
            if granted_role.owner_id != grantor_id
                && grantor_id != ADMIN_ID
                && !(granted_role.name == "public"
                    && priv_def.privilege() == PrivilegeType::Execute)
            {
                let role_name = &granted_role.name;
                #[rustfmt::skip]
                return Err(BoxError::new(AccessDenied, format!("{access_name} to {object_type} '{role_name}' is denied for user '{user_name}'")).into());
            }

            detect_role_grant_cycles(&granted_role, priv_def, storage)?;

            return box_access_check_ddl_as_user(
                &granted_role.name,
                granted_role.id,
                grantor_id,
                TntSchemaObjectType::Role,
                access,
                grantor_id,
            );
        }
        PicoSchemaObjectType::User => {
            let target_sys_user = user_by_id(object_id)?;
            if target_sys_user.ty != UserMetadataKind::User {
                return Err(make_no_such_user(&target_sys_user.name));
            }

            // Only owner or admin can grant on user
            if target_sys_user.owner_id != grantor_id && grantor_id != ADMIN_ID {
                let target_user_name = &target_sys_user.name;
                #[rustfmt::skip]
                return Err(BoxError::new(AccessDenied, format!("{access_name} to {object_type} '{target_user_name}' is denied for user '{user_name}'")).into());
            }

            return box_access_check_ddl_as_user(
                &target_sys_user.name,
                target_sys_user.id,
                grantor_id,
                TntSchemaObjectType::User,
                access,
                grantor_id,
            );
        }

        PicoSchemaObjectType::Routine => {
            let routine = storage
                .routines
                .by_id(object_id)?
                .ok_or_else(|| make_routine_not_found(object_id))?;

            // Only owner or admin can grant on routine.
            if routine.owner != grantor_id && grantor_id != ADMIN_ID {
                let routine_name = &routine.name;
                #[rustfmt::skip]
                return Err(BoxError::new(AccessDenied, format!("{access_name} to {object_type} '{routine_name}' is denied for user '{user_name}'")).into());
            }

            return box_access_check_ddl_as_user(
                &routine.name,
                routine.id,
                grantor_id,
                TntSchemaObjectType::Function,
                access,
                grantor_id,
            );
        }
    }

    Ok(())
}

fn access_check_acl(storage: &Catalog, acl: &op::Acl, as_user: UserId) -> tarantool::Result<()> {
    match acl {
        op::Acl::CreateUser { user_def } => {
            assert_eq!(
                user_def.owner, as_user,
                "when creating objects creator is the owner"
            );
            box_access_check_ddl_as_user(
                &user_def.name,
                user_def.id,
                user_def.owner,
                TntSchemaObjectType::User,
                PrivType::Create,
                as_user,
            )
        }
        op::Acl::ChangeAuth { user_id, .. } | op::Acl::RenameUser { user_id, .. } => {
            let sys_user = user_by_id(*user_id)?;

            assert_eq!(sys_user.id, *user_id, "user metadata id mismatch");

            box_access_check_ddl_as_user(
                &sys_user.name,
                sys_user.id,
                sys_user.owner_id,
                TntSchemaObjectType::User,
                PrivType::Alter,
                as_user,
            )
        }
        op::Acl::DropUser { user_id, .. } => {
            let Some(sys_user) = user_by_id_if_exists(*user_id)? else {
                // The user has already been dropped.
                // This may have occurred during the execution of proc_cas_local,
                // but it would result in a schema version change, causing the proposed operation
                // to be rejected during the predicate check and retried later. Upon retry, the
                // initiator will detect that the user has been dropped and handle it accordingly.
                return Ok(());
            };

            assert_eq!(sys_user.id, *user_id, "user metadata id mismatch");

            box_access_check_ddl_as_user(
                &sys_user.name,
                sys_user.id,
                sys_user.owner_id,
                TntSchemaObjectType::User,
                PrivType::Drop,
                as_user,
            )
        }
        op::Acl::CreateRole { role_def } => {
            assert_eq!(
                role_def.owner, as_user,
                "when creating objects creator is the owner"
            );
            box_access_check_ddl_as_user(
                &role_def.name,
                role_def.id,
                role_def.owner,
                TntSchemaObjectType::Role,
                PrivType::Create,
                as_user,
            )
        }
        op::Acl::DropRole { role_id, .. } => {
            // In vanilla roles and users are stored in the same space
            // so we can reuse the definition
            let Some(sys_user) = user_by_id_if_exists(*role_id)? else {
                // The role has already been dropped.
                // This may have occurred during the execution of proc_cas_local,
                // but it would result in a schema version change, causing the proposed operation
                // to be rejected during the predicate check and retried later. Upon retry, the
                // initiator will detect that the role has been dropped and handle it accordingly.
                return Ok(());
            };

            assert_eq!(sys_user.id, *role_id, "user metadata id mismatch");

            box_access_check_ddl_as_user(
                &sys_user.name,
                sys_user.id,
                sys_user.owner_id,
                TntSchemaObjectType::Role,
                PrivType::Drop,
                as_user,
            )
        }
        op::Acl::GrantPrivilege { priv_def } => {
            access_check_grant_revoke(storage, priv_def, as_user, PrivType::Grant, "Grant")
        }
        op::Acl::RevokePrivilege { priv_def, .. } => {
            access_check_grant_revoke(storage, priv_def, as_user, PrivType::Revoke, "Revoke")
        }
    }
}

pub(super) fn access_check_op(
    storage: &Catalog,
    op: &Op,
    as_user: UserId,
) -> tarantool::Result<()> {
    match op {
        Op::Nop => Ok(()),
        Op::Dml(dml) => {
            access_check_dml(storage, dml, as_user)?;
            Ok(())
        }
        Op::BatchDml { ops } => {
            for op in ops {
                access_check_dml(storage, op, as_user)?;
            }
            Ok(())
        }
        Op::DdlPrepare { ddl, .. } => access_check_ddl(storage, ddl, as_user),
        Op::DdlCommit | Op::DdlAbort { .. } => {
            if as_user != ADMIN_ID {
                let sys_user = user_by_id(as_user)?;
                #[rustfmt::skip]
                return Err(BoxError::new(AccessDenied, format!("DDL access is denied for user '{}'", sys_user.name)).into());
            }
            Ok(())
        }
        Op::Plugin { .. } => {
            access_check_plugin_system(as_user)?;
            Ok(())
        }
        Op::Acl(acl) => access_check_acl(storage, acl, as_user),
    }
}

/// This function is also called from [`crate::sql::dispatch_bound_statement`] for client-side
/// permission checking. See comments next to it for details.
pub(crate) fn access_check_plugin_system(as_user: UserId) -> tarantool::Result<()> {
    if !is_superuser(as_user) {
        let sys_user = user_by_id(as_user)?;
        #[rustfmt::skip]
        return Err(BoxError::new(AccessDenied, format!("Plugin system access is denied for user '{}'", sys_user.name)).into());
    }
    Ok(())
}

// TODO: use this function everywhere we check `id != ADMIN_ID`
#[inline(always)]
pub fn is_superuser(user_id: UserId) -> bool {
    // TODO: some users may have a "super" role which theoretically should make them
    // equivalent in power to ADMIN, but currently we don't do this for some reason...
    user_id == ADMIN_ID || user_id == PICO_SERVICE_ID
}

mod tests {
    use std::collections::HashMap;

    use super::{access_check_acl, access_check_ddl, user_by_id};
    use crate::storage::schema::acl::global_create_role;
    use crate::storage::schema::acl::global_grant_privilege;
    use crate::storage::schema::acl::on_master_create_role;
    use crate::storage::schema::acl::on_master_create_user;
    use crate::storage::schema::acl::on_master_grant_privilege;
    use crate::storage::schema::acl::on_master_revoke_privilege;
    use crate::{
        access_control::{access_check_op, UserMetadataKind},
        schema::{
            Distribution, PrivilegeDef, PrivilegeType, SchemaObjectType, UserDef, ADMIN_ID,
            UNIVERSE_ID,
        },
        storage::Catalog,
        traft::op::{Acl, Ddl, Dml, Op},
    };
    use sbroad::ir::operator::ConflictStrategy;
    use tarantool::{
        auth::{AuthData, AuthDef, AuthMethod},
        session::{self, UserId},
        space::{Space, SpaceCreateOptions, SpaceEngineType},
        tuple::{Tuple, TupleBuffer},
    };

    static mut NEXT_USER_ID: u32 = 42;

    fn next_user_id() -> u32 {
        // SAFETY: tests are always run on tx thread sequentially
        unsafe {
            NEXT_USER_ID += 1;
            NEXT_USER_ID
        }
    }

    #[tarantool::test]
    fn decode_user_metadata() {
        let sys_user = user_by_id(ADMIN_ID).unwrap();
        assert_eq!(sys_user.id, ADMIN_ID);
        assert_eq!(sys_user.owner_id, ADMIN_ID);
        assert_eq!(&sys_user.name, "admin");
        assert_eq!(sys_user.ty, UserMetadataKind::User);
        assert_eq!(sys_user.auth, HashMap::new());
        assert_eq!(sys_user.auth_history, []);
        assert_eq!(sys_user.last_modified, 0);
    }

    fn dummy_auth_def() -> AuthDef {
        AuthDef::new(
            AuthMethod::Md5,
            AuthData::new(&AuthMethod::Md5, "", "").into_string(),
        )
    }

    fn dummy_user_def(id: UserId, name: String, owner: Option<UserId>) -> UserDef {
        UserDef {
            id,
            name,
            schema_version: 0,
            auth: Some(dummy_auth_def()),
            owner: owner.unwrap_or_else(|| session::uid().unwrap()),
            ty: UserMetadataKind::User,
        }
    }

    #[track_caller]
    fn make_user(name: &str, owner: Option<UserId>) -> u32 {
        let id = next_user_id();
        let user_def = dummy_user_def(id, name.to_owned(), owner);
        on_master_create_user(&user_def, true).unwrap();
        id
    }

    #[track_caller]
    fn grant(
        storage: &Catalog,
        privilege: PrivilegeType,
        object_type: SchemaObjectType,
        object_id: i64,
        grantee_id: UserId,
        grantor_id: Option<UserId>,
    ) {
        let grantor_id = grantor_id.unwrap_or_else(|| session::uid().unwrap());
        let priv_def =
            PrivilegeDef::new(grantor_id, grantee_id, privilege, object_type, object_id, 0)
                .expect("must be valid");

        access_check_op(
            storage,
            &Op::Acl(Acl::GrantPrivilege {
                priv_def: priv_def.clone(),
            }),
            priv_def.grantor_id(),
        )
        .unwrap();

        on_master_grant_privilege(&priv_def).unwrap();
    }

    #[track_caller]
    fn revoke(
        storage: &Catalog,
        grantee_id: UserId,
        privilege: PrivilegeType,
        object_type: SchemaObjectType,
        object_id: i64,
    ) {
        let priv_def = PrivilegeDef::new(
            session::uid().unwrap(),
            grantee_id,
            privilege,
            object_type,
            object_id,
            0,
        )
        .expect("must be valid");

        access_check_op(
            storage,
            &Op::Acl(Acl::RevokePrivilege {
                priv_def: priv_def.clone(),
                initiator: session::uid().unwrap(),
            }),
            priv_def.grantor_id(),
        )
        .unwrap();
        on_master_revoke_privilege(&priv_def).unwrap()
    }

    #[tarantool::test]
    fn validate_access_check_ddl() {
        let user_name = "box_access_check_space_test_user";

        let user_id = make_user(user_name, None);
        let storage = Catalog::for_tests();

        // space
        let space_name = "test_box_access_check_ddl";
        let opts = SpaceCreateOptions {
            id: Some(1025),
            ..Default::default()
        };
        let space = Space::create(space_name, &opts).unwrap();

        // create space
        {
            let space_to_be_created = Ddl::CreateTable {
                id: 42,
                name: String::from("space_to_be_created"),
                format: vec![],
                primary_key: vec![],
                distribution: Distribution::Global,
                engine: SpaceEngineType::Blackhole,
                owner: user_id,
            };

            let e = access_check_ddl(&storage, &space_to_be_created, user_id).unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("box error: AccessDenied: Create access to space 'space_to_be_created' is denied for user '{user_name}'"),
            );

            grant(
                &storage,
                PrivilegeType::Create,
                SchemaObjectType::Table,
                -1,
                user_id,
                None,
            );

            access_check_ddl(&storage, &space_to_be_created, user_id).unwrap();
        }

        // truncate can be executed only with WRITE privilege
        {
            let e = access_check_ddl(
                &storage,
                &Ddl::TruncateTable {
                    id: space.id(),
                    initiator: user_id,
                },
                user_id,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("box error: AccessDenied: Write access to space '{space_name}' is denied for user '{user_name}'"),
            );

            grant(
                &storage,
                PrivilegeType::Write,
                SchemaObjectType::Table,
                space.id() as i64,
                user_id,
                None,
            );

            access_check_ddl(
                &storage,
                &Ddl::TruncateTable {
                    id: space.id(),
                    initiator: user_id,
                },
                user_id,
            )
            .unwrap();
        }

        revoke(
            &storage,
            user_id,
            PrivilegeType::Write,
            SchemaObjectType::Table,
            space.id() as i64,
        );

        // drop can be granted with wildcard, check on particular entity works
        {
            let e = access_check_ddl(
                &storage,
                &Ddl::DropTable {
                    id: space.id(),
                    initiator: user_id,
                },
                user_id,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("box error: AccessDenied: Drop access to space '{space_name}' is denied for user '{user_name}'"),
            );

            grant(
                &storage,
                PrivilegeType::Drop,
                SchemaObjectType::Table,
                -1,
                user_id,
                None,
            );

            access_check_ddl(
                &storage,
                &Ddl::DropTable {
                    id: space.id(),
                    initiator: user_id,
                },
                user_id,
            )
            .unwrap();
        }

        revoke(
            &storage,
            user_id,
            PrivilegeType::Drop,
            SchemaObjectType::Table,
            -1,
        );

        // drop on particular entity works
        {
            let e = access_check_ddl(
                &storage,
                &Ddl::DropTable {
                    id: space.id(),
                    initiator: user_id,
                },
                user_id,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("box error: AccessDenied: Drop access to space '{space_name}' is denied for user '{user_name}'"),
            );

            grant(
                &storage,
                PrivilegeType::Drop,
                SchemaObjectType::Table,
                space.id() as i64,
                user_id,
                None,
            );

            access_check_ddl(
                &storage,
                &Ddl::DropTable {
                    id: space.id(),
                    initiator: user_id,
                },
                user_id,
            )
            .unwrap();
        }

        // owner has privileges on the object
        // owner can grant permissions on the object to other users
        {
            let grantee_user_name = format!("{user_name}_grantee");
            let grantee_user_id = make_user(&grantee_user_name, None);

            let space_name_grant = format!("{space_name}_grant");
            let space_opts = SpaceCreateOptions {
                user: Some(user_name.into()),
                ..Default::default()
            };
            let space_grant = Space::create(&space_name_grant, &space_opts).unwrap();

            let drop_op = |initiator| Op::DdlPrepare {
                schema_version: 0,
                ddl: Ddl::DropTable {
                    id: space_grant.id(),
                    initiator,
                },
                governor_op_id: None,
            };
            let write_op = |initiator| {
                Op::Dml(Dml::Insert {
                    table: space_grant.id(),
                    tuple: TupleBuffer::from(Tuple::new(&(1,)).unwrap()),
                    initiator,
                    conflict_strategy: ConflictStrategy::DoFail,
                })
            };

            // owner himself has permission on an object
            for op in [drop_op(user_id), write_op(user_id)] {
                access_check_op(&storage, &op, user_id).unwrap();
            }

            // owner can grant permissions to another user
            for (privilege, privilege_name, op) in [
                (PrivilegeType::Drop, "Drop", drop_op(grantee_user_id)),
                (PrivilegeType::Write, "Write", write_op(grantee_user_id)),
            ] {
                // run access check for another user, it fails without grant
                let e = access_check_op(&storage, &op, grantee_user_id).unwrap_err();

                assert_eq!(
                    e.to_string(),
                    format!("box error: AccessDenied: {privilege_name} access to space '{space_name_grant}' is denied for user '{grantee_user_name}'"),
                );

                // grant permission on behalf of the user owning the space
                grant(
                    &storage,
                    privilege,
                    SchemaObjectType::Table,
                    space_grant.id() as _,
                    grantee_user_id,
                    Some(user_id),
                );

                // access check should succeed
                access_check_op(&storage, &op, grantee_user_id).unwrap();
            }
        }
    }

    #[tarantool::test]
    fn validate_access_check_acl_user() {
        // create works with passed id
        let actor_user_name = "box_access_check_ddl_test_user_actor";
        let user_under_test_name = "box_access_check_ddl_test_user";

        let storage = Catalog::for_tests();

        let actor_user_id = make_user(actor_user_name, None);
        let user_under_test_id = make_user(user_under_test_name, None);

        // create works with passed id
        {
            let e = access_check_acl(
                &storage,
                &Acl::CreateUser {
                    user_def: dummy_user_def(
                        123,
                        String::from("user_to_be_created"),
                        Some(actor_user_id),
                    ),
                },
                actor_user_id,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("box error: AccessDenied: Create access to user 'user_to_be_created' is denied for user '{actor_user_name}'"),
            );

            grant(
                &storage,
                PrivilegeType::Create,
                SchemaObjectType::User,
                -1,
                actor_user_id,
                None,
            );

            access_check_acl(
                &storage,
                &Acl::CreateUser {
                    user_def: dummy_user_def(
                        123,
                        String::from("user_to_be_created"),
                        Some(actor_user_id),
                    ),
                },
                actor_user_id,
            )
            .unwrap();
        }

        // drop can be granted with wildcard, check on particular entity works
        {
            let e = access_check_acl(
                &storage,
                &Acl::DropUser {
                    user_id: user_under_test_id,
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                actor_user_id,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("box error: AccessDenied: Drop access to user '{user_under_test_name}' is denied for user '{actor_user_name}'"),
            );

            grant(
                &storage,
                PrivilegeType::Drop,
                SchemaObjectType::User,
                -1,
                actor_user_id,
                None,
            );

            access_check_acl(
                &storage,
                &Acl::DropUser {
                    user_id: user_under_test_id,
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                actor_user_id,
            )
            .unwrap();
        }

        // alter can be granted with wildcard, check on particular entity works
        {
            let e = access_check_acl(
                &storage,
                &Acl::ChangeAuth {
                    user_id: user_under_test_id,
                    auth: dummy_auth_def(),
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                actor_user_id,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("box error: AccessDenied: Alter access to user '{user_under_test_name}' is denied for user '{actor_user_name}'"),
            );

            grant(
                &storage,
                PrivilegeType::Alter,
                SchemaObjectType::User,
                -1,
                actor_user_id,
                None,
            );

            access_check_acl(
                &storage,
                &Acl::ChangeAuth {
                    user_id: user_under_test_id,
                    auth: dummy_auth_def(),
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                actor_user_id,
            )
            .unwrap();
        }

        revoke(
            &storage,
            actor_user_id,
            PrivilegeType::Drop,
            SchemaObjectType::User,
            -1,
        );
        revoke(
            &storage,
            actor_user_id,
            PrivilegeType::Alter,
            SchemaObjectType::User,
            -1,
        );

        // drop on particular entity works
        {
            let e = access_check_acl(
                &storage,
                &Acl::DropUser {
                    user_id: user_under_test_id,
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                actor_user_id,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("box error: AccessDenied: Drop access to user '{user_under_test_name}' is denied for user '{actor_user_name}'"),
            );

            grant(
                &storage,
                PrivilegeType::Drop,
                SchemaObjectType::User,
                user_under_test_id as i64,
                actor_user_id,
                None,
            );

            access_check_acl(
                &storage,
                &Acl::DropUser {
                    user_id: user_under_test_id,
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                actor_user_id,
            )
            .unwrap();
        }

        // alter on particular entity works
        {
            let e = access_check_acl(
                &storage,
                &Acl::ChangeAuth {
                    user_id: user_under_test_id,
                    auth: dummy_auth_def(),
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                actor_user_id,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("box error: AccessDenied: Alter access to user '{user_under_test_name}' is denied for user '{actor_user_name}'"),
            );

            grant(
                &storage,
                PrivilegeType::Alter,
                SchemaObjectType::User,
                user_under_test_id as i64,
                actor_user_id,
                None,
            );

            access_check_acl(
                &storage,
                &Acl::ChangeAuth {
                    user_id: user_under_test_id,
                    auth: dummy_auth_def(),
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                actor_user_id,
            )
            .unwrap();
        }

        // it is allowed to drop nonexistent users and roles if IF EXISTS option is passed
        {
            let drop_user_access = access_check_acl(
                &storage,
                &Acl::DropUser {
                    user_id: 42,
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                actor_user_id,
            );
            assert!(drop_user_access.is_ok());

            let drop_role_access = access_check_acl(
                &storage,
                &Acl::DropRole {
                    role_id: 42,
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                actor_user_id,
            );
            assert!(drop_role_access.is_ok());
        }

        // rename user
        {
            revoke(
                &storage,
                actor_user_id,
                PrivilegeType::Alter,
                SchemaObjectType::User,
                user_under_test_id.into(),
            );

            // actor_user doesn't have alter to user_under_test
            let e = access_check_acl(
                &storage,
                &Acl::RenameUser {
                    user_id: user_under_test_id,
                    name: "new_name".into(),
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                actor_user_id,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("box error: AccessDenied: Alter access to user '{user_under_test_name}' is denied for user '{actor_user_name}'"),
            );

            grant(
                &storage,
                PrivilegeType::Alter,
                SchemaObjectType::User,
                user_under_test_id as i64,
                actor_user_id,
                None,
            );

            // after grant actor_user have alter to user_under_test
            access_check_acl(
                &storage,
                &Acl::RenameUser {
                    user_id: user_under_test_id,
                    name: "new_name".into(),
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                actor_user_id,
            )
            .unwrap();

            // by default user_under_test has alter on himself
            access_check_acl(
                &storage,
                &Acl::RenameUser {
                    user_id: user_under_test_id,
                    name: "new_name".into(),
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                user_under_test_id,
            )
            .unwrap();

            revoke(
                &storage,
                user_under_test_id,
                PrivilegeType::Alter,
                SchemaObjectType::User,
                user_under_test_id.into(),
            );

            access_check_acl(
                &storage,
                &Acl::RenameUser {
                    user_id: user_under_test_id,
                    name: "new_name".into(),
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                user_under_test_id,
            )
            .unwrap_err();

            // admin always can rename user
            access_check_acl(
                &storage,
                &Acl::RenameUser {
                    user_id: user_under_test_id,
                    name: "new_name".into(),
                    initiator: actor_user_id,
                    schema_version: 0,
                },
                ADMIN_ID,
            )
            .unwrap();
        }

        // owner has privileges on the object
        // owner can grant permissions on the object to other users
        {
            let grantee_user_name = format!("{actor_user_name}_grantee");
            let grantee_user_id = make_user(&grantee_user_name, None);

            let user_name_grant = format!("{actor_user_name}_grant");
            let user_id_grant = make_user(&user_name_grant, Some(actor_user_id));

            let drop_op = |initiator| {
                Op::Acl(Acl::DropUser {
                    user_id: user_id_grant,
                    initiator,
                    schema_version: 0,
                })
            };

            let alter_op = |initiator| {
                Op::Acl(Acl::ChangeAuth {
                    user_id: user_id_grant,
                    auth: dummy_auth_def(),
                    initiator,
                    schema_version: 0,
                })
            };

            // owner himself has permission on an object
            for op in [drop_op(actor_user_id), alter_op(actor_user_id)] {
                access_check_op(&storage, &op, actor_user_id).unwrap();
            }

            // owner can grant it to another user
            for (privilege, privilege_name, op) in [
                (PrivilegeType::Drop, "Drop", drop_op(grantee_user_id)),
                (PrivilegeType::Alter, "Alter", alter_op(grantee_user_id)),
            ] {
                // run access check for another user, it fails without grant
                let e = access_check_op(&storage, &op, grantee_user_id).unwrap_err();

                assert_eq!(
                    e.to_string(),
                    format!("box error: AccessDenied: {privilege_name} access to user '{user_name_grant}' is denied for user '{grantee_user_name}'"),
                );

                // grant permission on behalf of the user owning the user
                grant(
                    &storage,
                    privilege,
                    SchemaObjectType::User,
                    user_id_grant as _,
                    grantee_user_id,
                    Some(actor_user_id),
                );

                // access check should succeed
                access_check_op(&storage, &op, grantee_user_id).unwrap();
            }
        }
    }

    #[tarantool::test]
    fn validate_access_check_acl_role() {
        let user_name = "box_access_check_ddl_test_role";

        let user_id = make_user(user_name, None);
        let storage = Catalog::for_tests();

        let role_name = "box_access_check_ddl_test_role_some_role";
        let role_def = UserDef {
            id: next_user_id(),
            name: String::from(role_name),
            schema_version: 0,
            owner: ADMIN_ID,
            auth: None,
            ty: UserMetadataKind::Role,
        };
        on_master_create_role(&role_def).expect("create role shouldnt fail");

        // create works with passed id
        {
            let role_to_be_created = UserDef {
                id: 123,
                name: String::from("role_to_be_created"),
                schema_version: 0,
                owner: user_id,
                auth: None,
                ty: UserMetadataKind::Role,
            };

            let e = access_check_acl(
                &storage,
                &Acl::CreateRole {
                    role_def: role_to_be_created.clone(),
                },
                user_id,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("box error: AccessDenied: Create access to role 'role_to_be_created' is denied for user '{user_name}'"),
            );

            grant(
                &storage,
                PrivilegeType::Create,
                SchemaObjectType::Role,
                -1,
                user_id,
                None,
            );

            access_check_acl(
                &storage,
                &Acl::CreateRole {
                    role_def: role_to_be_created,
                },
                user_id,
            )
            .unwrap();
        }

        // drop can be granted with wildcard, check on particular entity works
        {
            let e = access_check_acl(
                &storage,
                &Acl::DropRole {
                    role_id: role_def.id,
                    initiator: user_id,
                    schema_version: 0,
                },
                user_id,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("box error: AccessDenied: Drop access to role '{role_name}' is denied for user '{user_name}'"),
            );

            grant(
                &storage,
                PrivilegeType::Drop,
                SchemaObjectType::Role,
                -1,
                user_id,
                None,
            );

            access_check_acl(
                &storage,
                &Acl::DropRole {
                    role_id: role_def.id,
                    initiator: user_id,
                    schema_version: 0,
                },
                user_id,
            )
            .unwrap();
        }

        revoke(
            &storage,
            user_id,
            PrivilegeType::Drop,
            SchemaObjectType::Role,
            -1,
        );

        // drop on particular entity works
        {
            let e = access_check_acl(
                &storage,
                &Acl::DropRole {
                    role_id: role_def.id,
                    initiator: user_id,
                    schema_version: 0,
                },
                user_id,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!("box error: AccessDenied: Drop access to role '{role_name}' is denied for user '{user_name}'"),
            );

            grant(
                &storage,
                PrivilegeType::Drop,
                SchemaObjectType::Role,
                role_def.id as i64,
                user_id,
                None,
            );

            access_check_acl(
                &storage,
                &Acl::DropRole {
                    role_id: role_def.id,
                    initiator: user_id,
                    schema_version: 0,
                },
                user_id,
            )
            .unwrap();
        }

        // owner has privileges on the object
        // owner can grant permissions on the object to other users
        {
            let grantee_user_name = format!("{user_name}_grantee");
            let grantee_user_id = make_user(&grantee_user_name, None);

            let role_name_grant = format!("{role_name}_grant");
            let role_id_grant = next_user_id();
            let role_def = UserDef {
                id: role_id_grant,
                name: role_name_grant.clone(),
                schema_version: 0,
                owner: user_id,
                auth: None,
                ty: UserMetadataKind::Role,
            };
            on_master_create_role(&role_def).expect("create role shouldn't fail");

            let op = |initiator| {
                Op::Acl(Acl::DropRole {
                    role_id: role_id_grant,
                    initiator,
                    schema_version: 0,
                })
            };

            // owner himself has permission on an object
            access_check_op(&storage, &op(user_id), user_id).unwrap();

            // run access check for another user, it fails without grant
            let e = access_check_op(&storage, &op(grantee_user_id), grantee_user_id).unwrap_err();

            assert_eq!(
                    e.to_string(),
                    format!("box error: AccessDenied: Drop access to role '{role_name_grant}' is denied for user '{grantee_user_name}'"),
                );

            // grant permission on behalf of the user owning the role
            grant(
                &storage,
                PrivilegeType::Drop,
                SchemaObjectType::Role,
                role_id_grant as _,
                grantee_user_id,
                Some(user_id),
            );

            // access check should succeed
            access_check_op(&storage, &op(grantee_user_id), grantee_user_id).unwrap();
        }
    }

    #[tarantool::test]
    fn prohibit_circular_role_grant() {
        let storage = Catalog::for_tests();

        let create_role = |name| {
            let id = next_user_id();
            let role_def = UserDef {
                id,
                name: String::from(name),
                schema_version: 0,
                owner: ADMIN_ID,
                auth: None,
                ty: UserMetadataKind::Role,
            };

            on_master_create_role(&role_def).expect("create role shouldn't fail");
            global_create_role(&storage, &role_def).expect("create role shouldn't fail");
            id
        };

        let parent_id = create_role("Parent");
        let child_id = create_role("Child");

        // circular grant: child to parent, parent to child should throw error
        {
            grant(
                &storage,
                PrivilegeType::Execute,
                SchemaObjectType::Role,
                child_id as i64,
                parent_id,
                None,
            );

            // grant child to parent
            let privilege = PrivilegeDef::new(
                ADMIN_ID,
                parent_id,
                PrivilegeType::Execute,
                SchemaObjectType::Role,
                child_id as i64,
                0,
            )
            .unwrap();

            global_grant_privilege(&storage, &privilege).unwrap();

            // grant parent to child
            let privilege = PrivilegeDef::new(
                ADMIN_ID,
                child_id,
                PrivilegeType::Execute,
                SchemaObjectType::Role,
                parent_id as i64,
                0,
            )
            .unwrap();

            let e = access_check_acl(
                &storage,
                &Acl::GrantPrivilege {
                    priv_def: privilege,
                },
                ADMIN_ID,
            )
            .unwrap_err();

            assert_eq!(
                e.to_string(),
                format!(
                    "box error: RoleLoop: Granting role Parent to role Child would create a loop"
                ),
            );
        }
    }

    #[tarantool::test]
    fn alter_user_login() {
        let storage = Catalog::for_tests();

        let owner_name = "test_owner";
        let user_name = "test_user";

        let owner_id = make_user(owner_name, None);
        let user_id = make_user(user_name, Some(owner_id));

        // User that is not an owner or admin cannot grant/revoke login on user
        let e = access_check_acl(
            &storage,
            &Acl::GrantPrivilege {
                priv_def: PrivilegeDef::new(
                    user_id,
                    user_id,
                    PrivilegeType::Login,
                    SchemaObjectType::Universe,
                    UNIVERSE_ID,
                    0,
                )
                .unwrap(),
            },
            user_id,
        )
        .unwrap_err();
        assert_eq!(
            e.to_string(),
            format!(
                "box error: AccessDenied: Grant Login from '{user_name}' is denied for 'test_user'"
            )
        );

        let e = access_check_acl(
            &storage,
            &Acl::RevokePrivilege {
                priv_def: PrivilegeDef::new(
                    user_id,
                    user_id,
                    PrivilegeType::Login,
                    SchemaObjectType::Universe,
                    UNIVERSE_ID,
                    0,
                )
                .unwrap(),
                initiator: user_id,
            },
            user_id,
        )
        .unwrap_err();
        assert_eq!(
            e.to_string(),
            format!(
                "box error: AccessDenied: Revoke Login from '{user_name}' is denied for 'test_user'"
            )
        );

        // Owner can grant/revoke login on user
        access_check_acl(
            &storage,
            &Acl::GrantPrivilege {
                priv_def: PrivilegeDef::new(
                    owner_id,
                    user_id,
                    PrivilegeType::Login,
                    SchemaObjectType::Universe,
                    UNIVERSE_ID,
                    0,
                )
                .unwrap(),
            },
            owner_id,
        )
        .unwrap();
        access_check_acl(
            &storage,
            &Acl::RevokePrivilege {
                priv_def: PrivilegeDef::new(
                    owner_id,
                    user_id,
                    PrivilegeType::Login,
                    SchemaObjectType::Universe,
                    UNIVERSE_ID,
                    0,
                )
                .unwrap(),
                initiator: owner_id,
            },
            owner_id,
        )
        .unwrap();

        // Admin can grant/revoke login on user
        access_check_acl(
            &storage,
            &Acl::GrantPrivilege {
                priv_def: PrivilegeDef::new(
                    ADMIN_ID,
                    user_id,
                    PrivilegeType::Login,
                    SchemaObjectType::Universe,
                    UNIVERSE_ID,
                    0,
                )
                .unwrap(),
            },
            ADMIN_ID,
        )
        .unwrap();
        access_check_acl(
            &storage,
            &Acl::RevokePrivilege {
                priv_def: PrivilegeDef::new(
                    ADMIN_ID,
                    user_id,
                    PrivilegeType::Login,
                    SchemaObjectType::Universe,
                    UNIVERSE_ID,
                    0,
                )
                .unwrap(),
                initiator: ADMIN_ID,
            },
            ADMIN_ID,
        )
        .unwrap();
    }
}
