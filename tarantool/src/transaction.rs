use crate::error::TransactionError;

/// Begin a transaction in the current fiber.
///
/// A transaction is attached to caller fiber, therefore one fiber can have
/// only one active transaction.
///
/// - `f` - function will be invoked within transaction
///
/// Returns result of function `f` execution. Depending on the function result:
/// - will **commit** - if function completes successfully
/// - will **rollback** - if function completes with any error
pub fn start_transaction<T, E, F>(f: F) -> Result<T, E>
where
    F: FnOnce() -> Result<T, E>,
    E: From<TransactionError>,
{
    if unsafe { ffi::box_txn_begin() } < 0 {
        return Err(TransactionError::AlreadyStarted.into());
    }

    let result = f();
    match &result {
        Ok(_) => {
            if unsafe { ffi::box_txn_commit() } < 0 {
                return Err(TransactionError::FailedToCommit.into());
            }
        }
        Err(_) => {
            if unsafe { ffi::box_txn_rollback() } < 0 {
                return Err(TransactionError::FailedToRollback.into());
            }
        }
    }
    result
}

pub mod ffi {
    use std::ffi::c_void;
    use std::os::raw::c_int;

    extern "C" {
        pub fn box_txn() -> bool;
        pub fn box_txn_begin() -> c_int;
        pub fn box_txn_commit() -> c_int;
        pub fn box_txn_rollback() -> c_int;
        pub fn box_txn_alloc(size: usize) -> *mut c_void;
    }
}
