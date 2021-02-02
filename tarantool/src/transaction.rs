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

use crate::error::TransactionError;
use crate::ffi::tarantool as ffi;

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
