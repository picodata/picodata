use crate::tarantool::VdbeYieldArgs;
use sql::executor::preemption::YIELD_INTERVAL_NS;
use std::cell::Cell;
use std::time::Duration;
use tarantool::fiber;
use tarantool::transaction::is_in_transaction;

thread_local!(static SQL_EXECUTION_GUARD: Cell<Option<fiber::FiberId>> = const { Cell::new(None) });

fn acquire_sql_execution_guard() {
    SQL_EXECUTION_GUARD.with(|guard| {
        let current_fid = fiber::id();
        match guard.get() {
            Some(fid) if fid == current_fid => {}
            None => guard.set(Some(current_fid)),
            _ => yield_sql_execution(),
        }
    });
}

fn release_sql_execution_guard() {
    SQL_EXECUTION_GUARD.with(|guard| guard.set(None));
}

#[inline(always)]
pub(crate) fn with_sql_execution_guard<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    acquire_sql_execution_guard();
    let result = f();
    release_sql_execution_guard();
    result
}

#[inline(always)]
pub(crate) fn yield_sql_execution() {
    // Yield the fiber execution, collect all IO events and reschedule the fiber
    // to the tail of the event loop queue.
    fiber::sleep(Duration::ZERO);
}

pub(crate) extern "C" fn vdbe_yield_handler(args: *mut VdbeYieldArgs) -> libc::c_int {
    let current = unsafe { (*args).current };
    let start_mut = unsafe { (*args).start };
    let start = unsafe { *start_mut };

    // Do nothing if the deadline has not been reached yet.
    if current.abs_diff(start) < YIELD_INTERVAL_NS {
        return 0;
    }

    // Yielding inside a transaction is forbidden. Blocking the event loop is
    // currently preferred over cancelling a long-running transaction.
    if !is_in_transaction() {
        unsafe { *start_mut = current };
        yield_sql_execution();
    }

    return 0;
}
