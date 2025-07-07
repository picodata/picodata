//! Picodata internal API.
pub mod cas;
pub(crate) mod ffi;
pub mod types;

use crate::internal::ffi::{
    pico_ffi_authenticate, pico_ffi_cluster_uuid, pico_ffi_instance_info, pico_ffi_raft_info,
    pico_ffi_rpc_version, pico_ffi_version,
};
use crate::internal::types::InstanceInfo;
use abi_stable::derive_macro_reexports::RResult;
use std::{env, fs, io, process};
use tarantool::error::BoxError;

/// Return picodata version.
pub fn picodata_version() -> &'static str {
    let ptr_and_len = unsafe { pico_ffi_version() };
    // SAFETY: ptr points to static string
    let slice = unsafe { std::slice::from_raw_parts(ptr_and_len.0, ptr_and_len.1) };
    std::str::from_utf8(slice).expect("should be valid utf8")
}

/// Return picodata RPC API version.
pub fn rpc_version() -> &'static str {
    let ptr_and_len = unsafe { pico_ffi_rpc_version() };
    // SAFETY: ptr points to static string
    let slice = unsafe { std::slice::from_raw_parts(ptr_and_len.0, ptr_and_len.1) };
    std::str::from_utf8(slice).expect("should be valid utf8")
}

/// Return UUID of the cluster the current instance belongs to.
pub fn cluster_uuid() -> Result<String, BoxError> {
    match unsafe { pico_ffi_cluster_uuid() } {
        RResult::ROk(rstring) => Ok(rstring.into()),
        RResult::RErr(_) => {
            let error = BoxError::last();
            Err(error)
        }
    }
}

/// Return information about current picodata instance.
pub fn instance_info() -> Result<InstanceInfo, BoxError> {
    match unsafe { pico_ffi_instance_info() } {
        RResult::ROk(info) => Ok(info),
        RResult::RErr(_) => {
            let error = BoxError::last();
            Err(error)
        }
    }
}

/// Return information about RAFT protocol state.
pub fn raft_info() -> types::RaftInfo {
    unsafe { pico_ffi_raft_info() }
}

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

/// Dump the backtrace to a file to make debugging easier.
/// This is also used in integration tests.
fn dump_backtrace(msg: &str) -> Result<(), io::Error> {
    let should_dump = env::var("PICODATA_INTERNAL_BACKTRACE_DUMP")
        .map(|v| !v.is_empty())
        .unwrap_or(false);

    if !should_dump {
        return Ok(());
    }

    let name = format!("picodata-{}.backtrace", process::id());
    let path = env::current_dir()?.join(&name);

    fs::write(&name, msg)
        .map(|_| tarantool::say_info!("dumped panic backtrace to `{}`", path.display()))
        .inspect_err(|e| tarantool::say_info!("{}", e))?;

    Ok(())
}

#[inline]
pub fn set_panic_hook() {
    // NOTE: this function is called ASAP when starting up the process.
    // Even if `say` isn't properly initialized yet, we
    // still should be able to print a simplified line to stderr.
    std::panic::set_hook(Box::new(|info| {
        let version = crate::internal::picodata_version();

        // Capture a backtrace regardless of RUST_BACKTRACE and such.
        let backtrace = std::backtrace::Backtrace::force_capture();
        let message = format!(
            "Picodata {version}\n\n{info}\n\nbacktrace:\n{backtrace}\naborting due to panic"
        );

        // Dump backtrace to logs and file if needed
        tarantool::say_crit!("\n\n{message}");
        dump_backtrace(&message)
            .unwrap_or_else(|e| tarantool::say_info!("Failed to dump panic backtrace: {}", e));

        std::process::abort();
    }));
}

#[derive(thiserror::Error, Debug)]
pub enum InternalError {
    #[error("timeout")]
    Timeout,
    #[error("internal error: {0}")]
    Any(BoxError),
}
