#![cfg(feature = "picodata")]

use crate::error;
use crate::ffi::tarantool as ffi;
use crate::space::SpaceId;

/// This is a direct translation of `box_privilege_type` enum from `user_def.h`
#[derive(Clone, Copy)]
#[repr(u16)]
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
    PrivAll = u16::MAX,
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
