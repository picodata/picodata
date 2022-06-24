//! The `clock` module returns time values derived from the Posix / C
//! [CLOCK_GETTIME](http://pubs.opengroup.org/onlinepubs/9699919799/functions/clock_getres.html)
//! function or equivalent.
//!
//! Most functions in the module return a number of seconds; functions with names followed by “64”
//! return a 64-bit number of nanoseconds.
//!
//! - [time()](fn.time.html) - Get the wall clock time in seconds
//! - [time64()](fn.time64.html) - Get the wall clock time in nanoseconds
//! - [monotonic()](fn.monotonic.html) - Get the monotonic time in seconds
//! - [monotonic64()](fn.monotonic64.html) - Get the monotonic time in nanoseconds
//! - [proc()](fn.proc.html) - Get the processor time in seconds
//! - [proc64()](fn.proc64.html) - Get the processor time in nanoseconds
//! - [thread()](fn.thread.html) - Get the thread time in seconds
//! - [thread64()](fn.thread64.html) - Get the thread time in nanoseconds
//!
//! See also:
//! - [Lua reference: Module clock](https://www.tarantool.io/en/doc/latest/reference/reference_lua/clock/)
//! - [C API reference: Module clock](https://www.tarantool.io/en/doc/latest/dev_guide/reference_capi/clock/)

use std::time::Duration;

pub const INFINITY: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

use crate::ffi::tarantool as ffi;

/// The wall clock time in seconds.
///
/// Derived from C function `clock_gettime(CLOCK_REALTIME)`.
/// This is the best function for knowing what the official time is, as determined by the system administrator.
///
/// Return: seconds since epoch (1970-01-01 00:00:00), adjusted.
///
/// Example:
/// ```no_run
/// // This will print an approximate number of years since 1970.
/// use tarantool::clock::time;
/// println!("{}", time() / (365. * 24. * 60. * 60.));
/// ```
///
/// See also: [fiber::time()](../fiber/fn.time.html), [fiber::time64()](../fiber/fn.time64.html)
#[inline(always)]
pub fn time() -> f64 {
    unsafe { ffi::clock_realtime() }
}

/// The wall clock time in nanoseconds since epoch.
///
/// Example:
/// ```no_run
/// // This will print an approximate number of years since 1970.
/// use tarantool::clock::time64;
/// println!("{}", time64() / (365 * 24 * 60 * 60));
/// ```
/// See: [time()](fn.time.html)
#[inline(always)]
pub fn time64() -> u64 {
    unsafe { ffi::clock_realtime64() }
}

/// The monotonic time.
///
/// Derived from C function `clock_gettime(CLOCK_MONOTONIC)`.
/// Monotonic time is similar to wall clock time but is not affected by changes to or from daylight saving time, or by
/// changes done by a user. This is the best function to use with benchmarks that need to calculate elapsed time.
///
/// Return: seconds or nanoseconds since the last time that the computer was booted.
/// Return type: `u64` or `f64`
///
/// Example:
/// ```no_run
/// // This will print nanoseconds since the start.
/// use tarantool::clock::monotonic64;
/// println!("{}", monotonic64());
/// ```
#[inline(always)]
pub fn monotonic() -> f64 {
    unsafe { ffi::clock_monotonic() }
}

/// See: [monotonic()](fn.monotonic.html)
#[inline(always)]
pub fn monotonic64() -> u64 {
    unsafe { ffi::clock_monotonic64() }
}

/// The processor time.
///
/// Derived from C function `clock_gettime(CLOCK_PROCESS_CPUTIME_ID)`.
/// This is the best function to use with benchmarks that need to calculate the amount of time for which CPU was used.
///
/// Return: seconds or nanoseconds since processor start.
/// Return type: `u64` or `f64`
///
/// Example:
/// ```no_run
/// // This will print nanoseconds in the CPU since the start.
/// use tarantool::clock::process64;
/// println!("{}", process64());
/// ```
#[inline(always)]
pub fn process() -> f64 {
    unsafe { ffi::clock_process() }
}

/// See: [process()](fn.process.html)
#[inline(always)]
pub fn process64() -> u64 {
    unsafe { ffi::clock_process64() }
}

/// The thread time.
///
/// Derived from C function `clock_gettime(CLOCK_THREAD_CPUTIME_ID)`.
/// This is the best function to use with benchmarks that need to calculate hthe amount of time for which a CPU thread was used.
///
/// Return: seconds or nanoseconds since the transaction processor thread started.
/// Return type: `u64` or `f64`
///
/// Example:
/// ```no_run
/// // This will print seconds in the thread since the start.
/// use tarantool::clock::thread64;
/// println!("{}", thread64());
/// ```
#[inline(always)]
pub fn thread() -> f64 {
    unsafe { ffi::clock_thread() }
}

/// See: [thread()](fn.thread.html)
#[inline(always)]
pub fn thread64() -> u64 {
    unsafe { ffi::clock_thread64() }
}
