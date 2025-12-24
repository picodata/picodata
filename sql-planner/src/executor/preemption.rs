use std::time::{Duration, Instant};
use tarantool::error::TarantoolError;
use tarantool::transaction::{begin, commit, is_in_transaction, TransactionError};

pub const YIELD_INTERVAL_NS: u64 = 500_000;
const YIELD_ITERATION_COUNT: usize = 1024;

pub struct Scheduler {
    start_time: Instant,
    ops_left: usize,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self {
            start_time: Instant::now(),
            ops_left: YIELD_ITERATION_COUNT,
        }
    }
}

impl Scheduler {
    #[inline(always)]
    pub fn maybe_yield<F>(&mut self, yield_impl: F) -> Result<(), TransactionError<TarantoolError>>
    where
        F: Fn(),
    {
        // Check iteration counter.
        debug_assert_ne!(YIELD_ITERATION_COUNT, 0);
        self.ops_left -= 1;
        if self.ops_left != 0 {
            return Ok(());
        }
        self.ops_left = YIELD_ITERATION_COUNT;

        // Check time interval.
        if self.start_time.elapsed() < Duration::from_nanos(YIELD_INTERVAL_NS) {
            return Ok(());
        }

        // Commit current progress and yield.
        let mut tx_need_restart = false;
        if is_in_transaction() {
            commit().map_err(TransactionError::FailedToCommit)?;
            tx_need_restart = true;
        }

        yield_impl();

        // Resume after yield and restart the timer.
        self.start_time = Instant::now();

        // Open new transaction.
        if tx_need_restart {
            begin().map_err(TransactionError::RolledBack)?;
        }

        Ok(())
    }
}
