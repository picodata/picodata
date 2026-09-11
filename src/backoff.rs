use smol_str::SmolStr;
use std::time::Duration;
use tarantool::fiber;
use tarantool::time::Instant;

/// Manages the a simple exponential backoff strategy and timeout.
#[derive(Default, Debug)]
pub struct SimpleBackoffManager {
    /// For debugging purposes
    name: SmolStr,
    last_try: Option<Instant>,

    base_timeout: Duration,
    current_timeout: Duration,
    max_timeout: Duration,

    /// If `true` then everytime the timeout is increased the next value is
    /// picked randomly in the range `(current_timeout, current_timeout * multiplier_coefficient]`
    pub randomize: bool,
    pub multiplier_coefficient: f64,
}

impl SimpleBackoffManager {
    const DEFAULT_MULTIPLIER_COEFF: f64 = 2.0;

    #[inline]
    pub fn new(name: &'static str, base_timeout: Duration, max_timeout: Duration) -> Self {
        Self::with_name(SmolStr::new_static(name), base_timeout, max_timeout)
    }

    #[inline]
    pub fn with_name(name: SmolStr, base_timeout: Duration, max_timeout: Duration) -> Self {
        Self {
            name,
            last_try: None,
            base_timeout,
            current_timeout: base_timeout,
            max_timeout,
            randomize: false,
            multiplier_coefficient: Self::DEFAULT_MULTIPLIER_COEFF,
        }
    }

    /// Returns the current timeout.
    #[inline]
    pub fn timeout(&self) -> Duration {
        self.current_timeout
    }

    /// Checks whether the action (for example governor's stage) should be
    /// executed now or if it must wait for the backoff timeout.
    #[track_caller]
    pub fn should_try(&self) -> bool {
        let Some(next_try) = self.should_wait_until() else {
            return true;
        };
        let now = fiber::clock();
        let result = now > next_try;
        if !result {
            crate::tlog!(
                Debug,
                "backoff manager: {} should wait {:0.3?}",
                self.name,
                next_try.duration_since(now)
            );
        }

        result
    }

    /// Returns the instant at which it's ok to make another attempt, or
    /// `None` is it's ok right now.
    pub fn should_wait_until(&self) -> Option<Instant> {
        let last_try = self.last_try?;

        Some(last_try.saturating_add(self.timeout()))
    }

    #[inline]
    pub fn handle_success(&mut self) {
        self.last_try = None;
        self.current_timeout = self.base_timeout;
    }

    #[inline]
    pub fn handle_failure(&mut self) {
        self.last_try = Some(fiber::clock());
        if self.current_timeout == self.max_timeout {
            return;
        }

        let old_secs = self.current_timeout.as_secs_f64();
        let mut new_secs = old_secs * self.multiplier_coefficient;

        // Randomizing the timeouts helps when a lot of concurrent processes are
        // trying to do the same thing
        if self.randomize {
            // t is in [0, 1)
            let t: f64 = rand::random();
            new_secs -= (new_secs - old_secs) * t;
        }

        let new_timeout = Duration::from_secs_f64(new_secs);
        self.current_timeout = new_timeout.min(self.max_timeout);
    }
}
