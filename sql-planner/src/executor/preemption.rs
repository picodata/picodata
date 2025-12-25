use std::cell::Cell;
use std::time::{Duration, Instant};
use tarantool::error::TarantoolError;
use tarantool::transaction::{begin, commit, is_in_transaction, TransactionError};

const YIELD_ITERATION_COUNT: usize = 1024;

thread_local!(static SCHEDULER_START_TIME: Cell<Option<Instant>> = const { Cell::new(None) });

pub struct Scheduler {
    ops_left: usize,
}

impl Default for Scheduler {
    fn default() -> Self {
        SCHEDULER_START_TIME.with(|timer| {
            if timer.get().is_none() {
                timer.set(Some(Instant::now()))
            }
        });
        Self {
            ops_left: YIELD_ITERATION_COUNT,
        }
    }
}

impl Scheduler {
    #[inline(always)]
    pub fn maybe_yield<F>(
        &mut self,
        options: &SchedulerOptions,
        yield_impl: F,
    ) -> Result<(), TransactionError<TarantoolError>>
    where
        F: Fn(),
    {
        if !options.enabled {
            return Ok(());
        }

        // Check iteration counter.
        debug_assert_ne!(YIELD_ITERATION_COUNT, 0);
        self.ops_left -= 1;
        if self.ops_left != 0 {
            return Ok(());
        }
        self.ops_left = YIELD_ITERATION_COUNT;

        // Check time interval.
        let start_time =
            SCHEDULER_START_TIME.with(|timer| timer.get().expect("timer must be initialized"));
        if start_time.elapsed() < Duration::from_micros(options.yield_interval_us) {
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
        SCHEDULER_START_TIME.with(|timer| timer.set(Some(Instant::now())));

        // Open new transaction.
        if tx_need_restart {
            begin().map_err(TransactionError::RolledBack)?;
        }

        Ok(())
    }
}

pub struct SchedulerOptions {
    pub enabled: bool,
    pub yield_interval_us: u64,
}
