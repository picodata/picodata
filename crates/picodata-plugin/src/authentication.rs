//! Module for authentication related entities.

use crate::internal::ffi::pico_ffi_authenticate;

use tarantool::error::BoxError;

/// # Description
///
/// Tries to authenticate a user with specified password.
/// Authentication method is determined via accessing `_pico_user`
/// system table using `admin` session.
///
/// # FFI
///
/// Uses [`pico_ffi_authenticate`].
///
/// # Errors
///
/// - User was not found in the list of available users.
/// - Authentication method was not initialized for the user.
/// - Username length is greater than `u32`.
/// - Password is not correct for the specified user.
///
/// # Panics
///
/// - Global Raft node is not initialized.
/// - Authentication data is not set for the specified user.
/// - Session of `admin` user is closed.
/// - User `admin` is not found.
/// - User `admin` does not have enough permissions.
/// - Internal error on accessing underlying Tarantool space of `_pico_user` system table.
pub fn authenticate(username: &str, password: impl AsRef<[u8]>) -> Result<(), BoxError> {
    match unsafe { pico_ffi_authenticate(username.into(), password.as_ref().into()) } {
        0 => Ok(()),
        _ => {
            let error = BoxError::last();
            Err(error)
        }
    }
}
