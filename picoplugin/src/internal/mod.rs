//! Picodata internal API.
pub mod cas;
pub(crate) mod ffi;
pub mod types;

use crate::internal::ffi::{
    pico_ffi_instance_info, pico_ffi_raft_info, pico_ffi_rpc_version, pico_ffi_version,
};
use crate::internal::types::InstanceInfo;
use abi_stable::derive_macro_reexports::RResult;
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

#[derive(thiserror::Error, Debug)]
pub enum InternalError {
    #[error("timeout")]
    Timeout,
    #[error("internal error: {0}")]
    Any(BoxError),
}
