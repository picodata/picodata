#![cfg(feature = "picodata")]

use std::ffi::CString;

use crate::error;
use crate::ffi::tarantool as ffi;
use crate::space::SpaceId;

/// This is a direct translation of `box_privilege_type` enum from `user_def.h`
#[repr(u16)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PrivType {
    /// SELECT
    Read = 1,
    /// INSERT, UPDATE, UPSERT, DELETE, REPLACE
    Write = 2,
    /// CALL
    Execute = 4,
    /// SESSION
    Session = 8,
    /// USAGE
    Usage = 16,
    /// CREATE
    Create = 32,
    /// DROP
    Drop = 64,
    /// ALTER
    Alter = 128,
    /// REFERENCE - required by ANSI - not implemented
    Reference = 256,
    /// TRIGGER - required by ANSI - not implemented
    Trigger = 512,
    /// INSERT - required by ANSI - not implemented
    Insert = 1024,
    /// UPDATE - required by ANSI - not implemented
    Update = 2048,
    /// DELETE - required by ANSI - not implemented
    Delete = 4096,
    /// This is never granted, but used internally.
    Grant = 8192,
    /// Never granted, but used internally.
    Revoke = 16384,
    All = u16::MAX,
}

/// This function is a wrapper around similarly named one in tarantool.
/// It allows to run access check for the current user against
/// specified space and access type. Most relevant access types are read and write.
pub fn box_access_check_space(space_id: SpaceId, user_access: PrivType) -> crate::Result<()> {
    let ret = unsafe { ffi::box_access_check_space(space_id, user_access as u16) };
    if ret == -1 {
        Err(error::Error::Tarantool(error::TarantoolError::last()))
    } else {
        Ok(())
    }
}

/// This is a direct translation of `box_schema_object_type` enum from `schema_def.h`
#[repr(u32)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub enum SchemaObjectType {
    #[default]
    Unknown = 0,
    Universe = 1,
    Space = 2,
    Function = 3,
    User = 4,
    Role = 5,
    Sequence = 6,
    Collation = 7,
    ObjectTypeMax = 8,

    EntitySpace = 9,
    EntityFunction = 10,
    EntityUser = 11,
    EntityRole = 12,
    EntitySequence = 13,
    EntityCollation = 14,
    EntityTypeMax = 15,
}

impl SchemaObjectType {
    fn is_entity(&self) -> bool {
        *self as u32 > SchemaObjectType::ObjectTypeMax as u32
    }
}

/// This function allows to perform various permission checks externally.
/// Note that there are no checks performed for harmless combinations
/// it doesnt make sense, i e execute space. This shouldnt lead to any
/// critical issues like UB but is just pointless from the application perspective.
///
/// # Panicking
///
/// Note that not all combinations of parameters are valid.
///
/// For example Entity* object types can only be used with [`PrivType::Grant`]
/// or [`PrivType::Revoke`].
/// Otherwise because of how this is structured inside tarantool such a call
/// leads to undefined behavior.
///
/// Another such example is that when using Grant or Revoke owner id must be set
/// to current user because in this context the owner is the user who grants
/// the permission (grantor). This works because for Grant or Revoke
/// box_access_check_ddl is not enough. For proper permission check you need
/// to additionally perform checks contained in priv_def_check C function.
///
/// So given these limitations box_access_check_ddl guards against
/// invalid combinations that lead to UB by panicking instead.
pub fn box_access_check_ddl(
    object_name: &str,
    object_id: u32,
    owner_id: u32,
    object_type: SchemaObjectType,
    access: PrivType,
) -> crate::Result<()> {
    assert!(
        !object_type.is_entity() || matches!(access, PrivType::Grant | PrivType::Revoke),
        "Entity scoped permissons can be checked only with Grant or Revoke"
    );

    if matches!(access, PrivType::Grant | PrivType::Revoke) {
        assert_eq!(
            owner_id,
            crate::session::uid().expect("there must be current user"),
            "This is incorrect use of the API. For grant and revoke owner_id must be current user (grantor)."
        )
    }

    let name = CString::new(object_name).expect("object name may not contain interior null bytes");
    let ret = unsafe {
        ffi::box_access_check_ddl(
            name.as_ptr(),
            object_id,
            owner_id,
            object_type as u32,
            access as u16,
        )
    };
    if ret == -1 {
        Err(error::Error::Tarantool(error::TarantoolError::last()))
    } else {
        Ok(())
    }
}
