use std::time::Duration;
use tarantool::fiber;
use tarantool::time::Instant;

/// Manages the a simple exponential backoff strategy and timeout.
pub struct SimpleBackoffManager {
    /// For debugging purposes
    name: &'static str,
    last_try: Option<Instant>,
    multiplier: u32,

    base_timeout: Duration,
    max_timeout: Duration,
}

impl SimpleBackoffManager {
    const MULTIPLIER_COEFF: u32 = 2;
    const BASE_MULTIPLIER: u32 = 1;

    #[inline]
    pub fn new(name: &'static str, base_timeout: Duration, max_timeout: Duration) -> Self {
        Self {
            name,
            last_try: None,
            base_timeout,
            max_timeout,
            multiplier: Self::BASE_MULTIPLIER,
        }
    }

    /// Returns the current timeout.
    #[inline]
    pub fn timeout(&self) -> Duration {
        self.base_timeout
            .saturating_mul(self.multiplier)
            .min(self.max_timeout)
    }

    /// Checks whether the action (for example governor's stage) should be
    /// executed now or if it must wait for the backoff timeout.
    #[track_caller]
    pub fn should_try(&self) -> bool {
        let Some(last_try) = self.last_try else {
            return true;
        };
        let now = fiber::clock();
        let timeout = self.timeout();
        let result = now.duration_since(last_try) > timeout;
        if !result {
            crate::tlog!(
                Debug,
                "backoff manager: {} should wait {} ms",
                self.name,
                timeout.as_millis()
            );
        }

        result
    }

    #[inline]
    pub fn handle_success(&mut self) {
        self.last_try = None;
        self.multiplier = Self::BASE_MULTIPLIER;
    }

    #[inline]
    pub fn handle_failure(&mut self) {
        self.last_try = Some(fiber::clock());
        self.multiplier = self.multiplier.saturating_mul(Self::MULTIPLIER_COEFF);
    }
}
