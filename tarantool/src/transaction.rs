use crate::c_api;
use crate::error::TransactionError;

pub fn start_transaction<T, E, F>(f: F) -> Result<T, E>
        where
            F: FnOnce() -> Result<T, E>,
            E: From<TransactionError>, {

    if unsafe { c_api::box_txn_begin() } < 0 {
        return Err(TransactionError::AlreadyStarted.into());
    }

    let result = f();
    match &result {
        Ok(_) => {
            if unsafe { c_api::box_txn_commit() } < 0 {
                return Err(TransactionError::FailedToCommit.into())
            }
        },
        Err(_) => {
            if unsafe { c_api::box_txn_rollback() } < 0 {
                return Err(TransactionError::FailedToRollback.into())
            }
        },
    }
    result
}
