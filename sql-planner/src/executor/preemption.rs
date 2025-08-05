use std::cell::Cell;
use std::time::{Duration, Instant};
use tarantool::error::TarantoolError;
use tarantool::transaction::{begin, commit, is_in_transaction, TransactionError};

thread_local!(static SCHEDULER_START_TIME: Cell<Option<Instant>> = const { Cell::new(None) });

pub struct Scheduler {
    ops_left: u64,
}

impl Scheduler {
    pub fn new(options: &SchedulerOptions) -> Self {
        SCHEDULER_START_TIME.with(|timer| {
            if timer.get().is_none() {
                timer.set(Some(Instant::now()))
            }
        });
        Self {
            ops_left: options.yield_vdbe_opcodes,
        }
    }
}

impl Scheduler {
    #[inline(always)]
    pub fn maybe_yield(
        &mut self,
        options: &SchedulerOptions,
    ) -> Result<(), TransactionError<TarantoolError>> {
        if !options.enabled {
            return Ok(());
        }

        // Check iteration counter.
        let opcodes = options.yield_vdbe_opcodes;
        debug_assert_ne!(opcodes, 0);
        self.ops_left -= 1;
        if self.ops_left != 0 {
            return Ok(());
        }
        self.ops_left = opcodes;

        // Check time interval.
        let configured_yield_interval = Duration::from_micros(options.yield_interval_us);

        let last_yield_instant =
            SCHEDULER_START_TIME.with(|timer| timer.get().expect("timer should be initialized"));
        let elapsed_since_last_yield = last_yield_instant.elapsed();

        let ready_to_yield = elapsed_since_last_yield >= configured_yield_interval;
        if !ready_to_yield {
            return Ok(());
        }

        // Commit current progress and yield.
        let mut tx_need_split = false;
        if is_in_transaction() {
            commit().map_err(TransactionError::FailedToCommit)?;
            tx_need_split = true;
            (options.metrics.record_tx_splits_total)();
        }

        (options.yield_impl)();

        // Resume after yield and restart the timer.
        (options.metrics.record_yields_total)();
        SCHEDULER_START_TIME.with(|timer| {
            let new_scheduler_instant = Instant::now();
            timer.set(Some(new_scheduler_instant));

            let next_yield_instant = last_yield_instant
                .checked_add(elapsed_since_last_yield)
                .expect("previous yield duration time should be valid");
            let yield_sleep_duration =
                new_scheduler_instant.saturating_duration_since(next_yield_instant);
            (options.metrics.record_yield_sleep_duration)(&yield_sleep_duration);
        });

        // Open new transaction.
        if tx_need_split {
            begin().map_err(TransactionError::RolledBack)?;
        }

        Ok(())
    }
}

pub struct SchedulerOptions {
    pub enabled: bool,
    pub yield_interval_us: u64,
    pub yield_vdbe_opcodes: u64,
    pub yield_impl: fn(),
    pub metrics: SchedulerMetrics,
}

pub struct SchedulerMetrics {
    pub record_tx_splits_total: fn(),
    pub record_yield_sleep_duration: fn(&Duration),
    pub record_yields_total: fn(),
}

impl SchedulerMetrics {
    pub const fn noop() -> Self {
        Self {
            record_tx_splits_total: || {},
            record_yield_sleep_duration: |_| {},
            record_yields_total: || {},
        }
    }
}
