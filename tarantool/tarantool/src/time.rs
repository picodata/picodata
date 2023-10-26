//! Provides a custom [`Instant`] implementation, based on tarantool fiber API.

use std::mem::MaybeUninit;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::time::Duration;

/// A measurement of a monotonically nondecreasing clock.
/// Opaque and useful only with [`Duration`].
///
/// Instants are guaranteed to be no less than any previously
/// measured instant when created, and are often useful for tasks such as measuring
/// benchmarks or timing how long an operation takes.
///
/// Note, however, that instants are **not** guaranteed to be **steady**. In other
/// words, each tick of the underlying clock might not be the same length (e.g.
/// some seconds may be longer than others). An instant may jump forwards or
/// experience time dilation (slow down or speed up), but it will never go
/// backwards.
///
/// Instants should generally be condsidered as opaque types that can only be compared to one another.
/// Though there is a method to get "the number of seconds" from an instant it is implementation dependent
/// and should be used with knowledge of how this particular `Instant` was constructed.
/// Instead, prefer using other operations, such as measuring the duration between two instants, comparing two
/// instants, adding and subtracting `Duration`.
///
/// This struct is almost identical to [`std::time::Instant`] but provides
/// some additional saturating methods. And it can also be constructed with
/// [`fiber::clock`](crate::fiber::clock), in which case it behaves in a tarantool specific way.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct Instant(pub(crate) Duration);

impl Instant {
    /// Returns an instant corresponding to "now". Uses monotonic clock.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tarantool::time::Instant;
    ///
    /// let now = Instant::now();
    /// ```
    #[must_use]
    #[inline]
    pub fn now() -> Self {
        unsafe {
            let mut timespec = MaybeUninit::<libc::timespec>::zeroed().assume_init();
            if libc::clock_gettime(libc::CLOCK_MONOTONIC, (&mut timespec) as *mut _) != 0 {
                let err = std::io::Error::last_os_error();
                panic!("failed to get time: {}", err)
            }
            Self(Duration::new(
                timespec.tv_sec as u64,
                timespec.tv_nsec as u32,
            ))
        }
    }

    /// Returns the amount of time elapsed since this instant was created.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use tarantool::time::Instant;
    /// use tarantool::fiber;
    ///
    /// let instant = Instant::now();
    /// let three_secs = Duration::from_secs(3);
    /// fiber::sleep(three_secs);
    /// assert!(instant.elapsed() >= three_secs);
    /// ```
    #[must_use]
    #[inline]
    pub fn elapsed(&self) -> Duration {
        Self::now().duration_since(*self)
    }

    /// Returns `Some(t)` where `t` is the time `self + duration` if `t` can be represented as
    /// `Instant` (which means it's inside the bounds of the underlying representation), `None`
    /// otherwise.
    #[must_use]
    #[inline]
    pub fn checked_add(&self, duration: Duration) -> Option<Instant> {
        self.0.checked_add(duration).map(Instant)
    }

    /// Returns `Some(t)` where `t` is the time `self - duration` if `t` can be represented as
    /// `Instant` (which means it's inside the bounds of the underlying representation), `None`
    /// otherwise.
    #[must_use]
    #[inline]
    pub fn checked_sub(&self, duration: Duration) -> Option<Instant> {
        self.0.checked_sub(duration).map(Instant)
    }

    /// Saturating addition. Computes `self + duration`, returning maximal possible
    /// instant (allowed by the underlying representaion) if overflow occurred.
    #[must_use]
    #[inline]
    pub fn saturating_add(&self, duration: Duration) -> Instant {
        Self(self.0.saturating_add(duration))
    }

    /// Saturating subtraction. Computes `self - duration`, returning minimal possible
    /// instant (allowed by the underlying representaion) if overflow occurred.
    #[must_use]
    #[inline]
    pub fn saturating_sub(&self, duration: Duration) -> Instant {
        Self(self.0.saturating_sub(duration))
    }

    /// Returns the amount of time elapsed from another instant to this one,
    /// or None if that instant is later than this one.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use std::thread::sleep;
    /// use tarantool::time::Instant;
    ///
    /// let now = Instant::now();
    /// sleep(Duration::new(1, 0));
    /// let new_now = Instant::now();
    /// println!("{:?}", new_now.checked_duration_since(now));
    /// println!("{:?}", now.checked_duration_since(new_now)); // None
    /// ```
    #[must_use]
    #[inline]
    pub fn checked_duration_since(&self, earlier: Instant) -> Option<Duration> {
        self.0.checked_sub(earlier.0)
    }

    /// Returns the amount of time elapsed from another instant to this one,
    /// or zero duration if that instant is later than this one.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use std::thread::sleep;
    /// use tarantool::time::Instant;
    ///
    /// let now = Instant::now();
    /// sleep(Duration::new(1, 0));
    /// let new_now = Instant::now();
    /// println!("{:?}", new_now.duration_since(now));
    /// println!("{:?}", now.duration_since(new_now)); // 0ns
    /// ```
    #[must_use]
    #[inline]
    pub fn duration_since(&self, earlier: Instant) -> Duration {
        self.0.saturating_sub(earlier.0)
    }

    /// Get the inner representation of an `Instant`.
    ///
    /// # Warning
    /// The inner representation of an instant is implementation dependent
    /// and should be used with knowledge of how this particular `Instant` was constructed.
    ///
    /// If possible prefer working with `Instant` and `Duration` directly without
    /// getting its inner representation.
    #[inline(always)]
    pub fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }

    /// Get the inner representation of an `Instant`.
    ///
    /// # Warning
    /// The inner representation of an instant is implementation dependent
    /// and should be used with knowledge of how this particular `Instant` was constructed.
    ///
    /// If possible prefer working with `Instant` and `Duration` directly without
    /// getting its inner representation.
    #[inline(always)]
    pub fn as_secs_f64(&self) -> f64 {
        self.0.as_secs_f64()
    }

    /// Get the inner representation of an `Instant`.
    ///
    /// # Warning
    /// The inner representation of an instant is implementation dependent
    /// and should be used with knowledge of how this particular `Instant` was constructed.
    ///
    /// If possible prefer working with `Instant` and `Duration` directly without
    /// getting its inner representation.
    #[inline(always)]
    pub fn as_secs_f32(&self) -> f32 {
        self.0.as_secs_f32()
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;

    /// # Panics
    ///
    /// This function may panic if the resulting point in time cannot be represented by the
    /// underlying data structure. See [`Instant::checked_add`] for a version without panic.
    fn add(self, other: Duration) -> Instant {
        self.checked_add(other)
            .expect("overflow when adding duration to instant")
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, other: Duration) {
        *self = *self + other;
    }
}

impl Sub<Duration> for Instant {
    type Output = Instant;

    fn sub(self, other: Duration) -> Instant {
        self.checked_sub(other)
            .expect("overflow when subtracting duration from instant")
    }
}

impl SubAssign<Duration> for Instant {
    fn sub_assign(&mut self, other: Duration) {
        *self = *self - other;
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    /// Returns the amount of time elapsed from another instant to this one,
    /// or zero duration if that instant is later than this one.
    fn sub(self, other: Instant) -> Duration {
        self.duration_since(other)
    }
}

#[cfg(test)]
mod tests {
    use super::Instant;
    use std::time::Duration;

    #[test]
    fn fiber_sleep() {
        let before_sleep = Instant::now();
        let sleep_for = Duration::from_millis(100);
        std::thread::sleep(sleep_for);

        assert!(Instant::now() >= before_sleep);
        assert!(before_sleep.elapsed() >= Duration::ZERO);
    }

    #[test]
    fn addition() {
        let now = Instant::now();

        assert_eq!(now.checked_add(Duration::MAX), None);
        assert_eq!(now.saturating_add(Duration::MAX), Instant(Duration::MAX));

        let plus_second = now.checked_add(Duration::from_secs(1)).unwrap();
        assert_eq!(plus_second, now.saturating_add(Duration::from_secs(1)));
        assert_eq!(plus_second, now + Duration::from_secs(1));
        assert!(plus_second > now);
    }

    #[test]
    fn subtraction() {
        let now = Instant::now();

        assert_eq!(now.checked_sub(Duration::MAX), None);
        assert_eq!(now.saturating_sub(Duration::MAX), Instant(Duration::ZERO));

        let minus_second = now.checked_sub(Duration::from_secs(1)).unwrap();
        assert_eq!(minus_second, now.saturating_sub(Duration::from_secs(1)));
        assert_eq!(minus_second, now - Duration::from_secs(1));
        assert!(minus_second < now);
    }

    #[test]
    fn duration_since() {
        let now = Instant::now();
        let plus_second = now + Duration::from_secs(1);
        let minus_second = now - Duration::from_secs(1);

        assert_eq!(
            plus_second.duration_since(minus_second),
            Duration::from_secs(2)
        );
        assert_eq!(
            plus_second.checked_duration_since(minus_second),
            Some(Duration::from_secs(2))
        );

        assert_eq!(minus_second.duration_since(plus_second), Duration::ZERO);
        assert_eq!(minus_second.checked_duration_since(plus_second), None);
    }
}
