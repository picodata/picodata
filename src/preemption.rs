use crate::config::{
    DEFAULT_SQL_PREEMPTION, DEFAULT_SQL_PREEMPTION_INTERVAL_US, DEFAULT_SQL_PREEMPTION_OPCODE_MAX,
    DYNAMIC_CONFIG,
};
use crate::metrics;
use crate::tarantool::VdbeYieldArgs;
use sql::executor::preemption::{SchedulerMetrics, SchedulerOptions};
use std::cell::Cell;
use std::time::Duration;
use tarantool::clock::monotonic64;
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
fn yield_sql_execution() {
    // Yield the fiber execution, collect all IO events and reschedule the fiber
    // to the tail of the event loop queue.
    fiber::sleep(Duration::ZERO);
}

#[inline(always)]
pub(crate) fn scheduler_options() -> SchedulerOptions {
    SchedulerOptions {
        enabled: sql_preemption(),
        yield_interval_us: sql_preemption_interval_us(),
        yield_vdbe_opcodes: sql_preemption_opcodes(),
        yield_impl: yield_sql_execution,
        metrics: SchedulerMetrics {
            record_tx_splits_total: metrics::record_sql_tx_splits_total,
            record_yield_sleep_duration: metrics::record_sql_yield_sleep_duration,
            record_yields_total: metrics::record_sql_yields_total,
        },
    }
}

pub(crate) extern "C" fn vdbe_yield_handler(args: *mut VdbeYieldArgs) -> libc::c_int {
    if !sql_preemption() {
        return 0;
    }

    let current = unsafe { (*args).current };
    let start_mut = unsafe { (*args).start };
    let start = unsafe { *start_mut };
    let yield_interval_ns = sql_preemption_interval_us() * 1000;

    // Do nothing if the deadline has not been reached yet.
    let run_interval_ns = current.abs_diff(start);
    if run_interval_ns < yield_interval_ns {
        return 0;
    }

    // Yielding inside a transaction is forbidden. Blocking the event loop is
    // currently preferred over cancelling a long-running transaction.
    if !is_in_transaction() {
        unsafe { *start_mut = current };
        yield_sql_execution();

        // NOTE: we track SQL yields here to measure only successful yield
        // events. Recording is done AFTER the yield completes to ensure that
        // failed or aborted yields are not counted, providing accurate metrics.
        metrics::record_sql_yields_total();

        // XXX: we must use `tarantool::clock::monotonic64` here. VDBE stores `start_time`
        // as a raw `clock_monotonic64` value and later computes deltas against it. Mixing
        // this with any other time source (e.g., commonly used `tarantool::time::Instant`)
        // would break timing due to different epochs or units.
        // SEE: <https://git.picodata.io/core/tarantool/-/blob/2f8f8cadeae83561188a13cecd06d6fa05b11186/src/box/sql/vdbe.c#L398>.
        let slept_ns = monotonic64().abs_diff(current as u64);
        let duration = Duration::from_nanos(slept_ns);
        metrics::record_sql_yield_sleep_duration(&duration);
    }

    return 0;
}

#[inline(always)]
pub fn sql_preemption() -> bool {
    DYNAMIC_CONFIG
        .sql_preemption
        .try_current_value()
        .unwrap_or(DEFAULT_SQL_PREEMPTION)
}

#[inline(always)]
fn sql_preemption_interval_us() -> u64 {
    DYNAMIC_CONFIG
        .sql_preemption_interval_us
        .try_current_value()
        .unwrap_or(DEFAULT_SQL_PREEMPTION_INTERVAL_US)
}

#[inline(always)]
pub fn sql_preemption_opcodes() -> u64 {
    DYNAMIC_CONFIG
        .sql_preemption_opcode_max
        .try_current_value()
        .unwrap_or(DEFAULT_SQL_PREEMPTION_OPCODE_MAX)
}
