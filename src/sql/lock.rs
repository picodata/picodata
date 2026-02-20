use crate::metrics;
use sql::errors::SbroadError;
use std::cell::Cell;
use std::rc::{Rc, Weak};
use tarantool::fiber::Cond;

pub struct TempTableLock {
    locked: Cell<bool>,
    cond: Cond,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockError {
    Cancelled,
}

impl From<LockError> for SbroadError {
    fn from(error: LockError) -> Self {
        match error {
            LockError::Cancelled => {
                SbroadError::Other("temporary table lock wait cancelled".into())
            }
        }
    }
}

impl TempTableLock {
    fn new() -> Self {
        Self {
            locked: Cell::new(false),
            cond: Cond::new(),
        }
    }

    fn acquire(lock: &TempTableLockRef) -> Result<TempTableLease, LockError> {
        while lock.locked.get() {
            metrics::record_sql_temp_table_lock_waits_total();
            if !lock.cond.wait() {
                return Err(LockError::Cancelled);
            }
        }
        lock.locked.set(true);
        metrics::record_sql_temp_table_leases_total();
        Ok(TempTableLease {
            lock: Rc::clone(lock),
        })
    }
}

pub type TempTableLockRef = Rc<TempTableLock>;
pub type TempTableLockWeak = Weak<TempTableLock>;

pub(crate) struct TempTableLease {
    lock: TempTableLockRef,
}

impl Drop for TempTableLease {
    fn drop(&mut self) {
        self.lock.locked.set(false);
        self.lock.cond.signal();
    }
}

pub(crate) fn new_temp_table_lock() -> TempTableLockRef {
    Rc::new(TempTableLock::new())
}

pub(crate) fn downgrade_temp_table_lock(lock: &TempTableLockRef) -> TempTableLockWeak {
    Rc::downgrade(lock)
}

pub(crate) fn lock_temp_table(lock: &TempTableLockRef) -> Result<TempTableLease, LockError> {
    TempTableLock::acquire(lock)
}
