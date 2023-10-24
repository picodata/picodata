//! Transaction management
//!
//! For general information and examples, see
//! [Transaction control](https://www.tarantool.io/en/doc/latest/book/box/atomic_index/#atomic-atomic-execution).
//!
//! Observe the following rules when working with transactions:
//!
//! 👉 **Rule #1**
//! The requests in a transaction must be sent to a server as a single block.
//! It is not enough to enclose them between begin and commit or rollback.
//! To ensure they are sent as a single block: put them in a function, or put them all on one line, or use a delimiter
//! so that multi-line requests are handled together.
//!
//! 👉 **Rule #2**
//! All database operations in a transaction should use the same storage engine.
//! It is not safe to access tuple sets that are defined with `{engine='vinyl'}` and also access tuple sets that are
//! defined with `{engine='memtx'}`, in the same transaction.
//!
//! 👉 **Rule #3**
//! Requests which cause changes to the data definition – create, alter, drop, truncate – are only allowed with
//! Tarantool version 2.1 or later. Data-definition requests which change an index or change a format, such as
//! `space_object:create_index()` and `space_object:format()`, are not allowed inside transactions except as the first
//! request.
//!
//! See also:
//! - [Transaction control](https://www.tarantool.io/en/doc/latest/book/box/atomic/)
//! - [Lua reference: Functions for transaction management](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_txn_management/)
//! - [C API reference: Module txn](https://www.tarantool.io/en/doc/latest/dev_guide/reference_capi/txn/)

use crate::error::TarantoolError;
use crate::ffi::tarantool as ffi;

/// Transaction-related error cases
#[derive(Debug, thiserror::Error)]
pub enum TransactionError<E> {
    #[error("transaction has already been started")]
    AlreadyStarted,

    #[error("failed to commit: {0}")]
    FailedToCommit(TarantoolError),

    #[error("failed to rollback: {0}")]
    FailedToRollback(TarantoolError),

    #[error("transaction rolled-back: {0}")]
    RolledBack(E),
}

/// Executes a transaction in the current fiber.
///
/// A transaction is attached to caller fiber, therefore one fiber can have
/// only one active transaction.
///
/// - `f` - function will be invoked within transaction
///
/// Returns result of function `f` execution. Depending on the function result:
/// - will **commit** - if function completes successfully
/// - will **rollback** - if function completes with any error
pub fn transaction<T, E, F>(f: F) -> Result<T, TransactionError<E>>
where
    F: FnOnce() -> Result<T, E>,
{
    if unsafe { ffi::box_txn_begin() } < 0 {
        return Err(TransactionError::AlreadyStarted);
    }

    let result = f();
    match &result {
        Ok(_) => {
            if unsafe { ffi::box_txn_commit() } < 0 {
                let error = TarantoolError::last();
                return Err(TransactionError::FailedToCommit(error));
            }
        }
        Err(_) => {
            if unsafe { ffi::box_txn_rollback() } < 0 {
                let error = TarantoolError::last();
                return Err(TransactionError::FailedToRollback(error));
            }
        }
    }
    result.map_err(TransactionError::RolledBack)
}

/// Returns `true` if there's an active transaction.
#[inline(always)]
pub fn is_in_transaction() -> bool {
    unsafe { ffi::box_txn() }
}

/// Begin a transaction in the current fiber.
///
/// One fiber can have at most one active transaction.
///
/// Returns an error if there's already an active transcation.
/// May return an error in other cases.
///
/// **NOTE:** it is the caller's responsibility to call [`commit`] or
/// [`rollback`]. Consider using [`transaction`] instead.
#[inline(always)]
pub fn begin() -> Result<(), TarantoolError> {
    if unsafe { ffi::box_txn_begin() } < 0 {
        return Err(TarantoolError::last());
    }
    Ok(())
}

/// Commit the active transaction.
///
/// Returns `Ok(())` if there is no active transaction.
///
/// Returns an error in case of IO failure.
/// May return an error in other cases.
#[inline(always)]
pub fn commit() -> Result<(), TarantoolError> {
    if unsafe { ffi::box_txn_commit() } < 0 {
        return Err(TarantoolError::last());
    }
    Ok(())
}

/// Rollback the active transaction.
///
/// Returns `Ok(())` if there is no active transaction.
///
/// Returns an error if called from a nested statement, e.g. when called via a trigger.
/// May return an error in other cases.
#[inline(always)]
pub fn rollback() -> Result<(), TarantoolError> {
    if unsafe { ffi::box_txn_rollback() } < 0 {
        return Err(TarantoolError::last());
    }
    Ok(())
}
