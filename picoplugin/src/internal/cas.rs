use crate::internal::ffi::{pico_ffi_cas, pico_ffi_wait_index};
use crate::internal::types::{Op, Predicate};
use crate::internal::InternalError;
use abi_stable::derive_macro_reexports::RResult;
use abi_stable::std_types::{ROption, RSome};
use std::time::Duration;
use tarantool::error::BoxError;

/// Performs a clusterwide compare and swap operation.
///
/// E.g. it checks the `predicate` on leader, and if no conflicting entries were found
/// appends the `op` to the raft log and returns its index and term.
pub fn compare_and_swap(
    op: Op,
    predicate: Predicate,
    timeout: Duration,
) -> Result<(u64, u64), InternalError> {
    let res = unsafe { pico_ffi_cas(op, predicate, timeout.into()) };
    match res {
        RResult::ROk(RSome(tuple)) => Ok(tuple.into()),
        RResult::ROk(ROption::RNone) => Err(InternalError::Timeout),
        RResult::RErr(_) => {
            let error = BoxError::last();
            Err(InternalError::Any(error))
        }
    }
}

/// Waits for raft entry with specified index to be applied to the storage locally.
///
/// Returns current applied raft index. It can be equal to or
/// greater than the target one. If timeout expires beforehand, the
/// function returns `Err(Timeout)`.
///
/// **This function yields**
pub fn wait_index(index: u64, timeout: Duration) -> Result<u64, InternalError> {
    let res = unsafe { pico_ffi_wait_index(index, timeout.into()) };
    match res {
        RResult::ROk(RSome(idx)) => Ok(idx),
        RResult::ROk(ROption::RNone) => Err(InternalError::Timeout),
        RResult::RErr(_) => {
            let error = BoxError::last();
            Err(InternalError::Any(error))
        }
    }
}
