//! Implements a rate limiter for logs, to prevent potentially noisy log calls from polluting the log file.
//!
//! This is based on `tarantool`'s implementation in `ratelimit.h`

use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::time::{Duration, Instant};

pub struct RateLimitParams {
    pub interval: Duration,
    pub burst: u32,
}

// 10 messages per 5 seconds
// same parameters as used by tarantool in SAY_RATELIMIT_INTERVAL and SAY_RATELIMIT_BURST
/// Log rate limit params used by [`crate::tlog_ratelimited`]
pub static LOG_RATELIMIT_PARAMS: RateLimitParams = RateLimitParams {
    interval: Duration::from_secs(5),
    burst: 10,
};

#[derive(Debug, PartialEq, Eq)]
pub enum RateLimitOutcome {
    EmitNormally,
    EmitWithSuppressionBeginNotice(Duration),
    Supress,
}

struct StateItem {
    emitted: u32,
    suppressed: u32,
    start: Instant,
}

pub struct KeyedRateLimitState {
    // problems:
    // - no convenient way to look at the LRU value and decide whether to evict it
    // - hard to avoid copying the key?
    // TODO: also store the key-values
    lru: lru::LruCache<String, StateItem>,
}

impl KeyedRateLimitState {
    #[allow(clippy::new_without_default)] // Creation of an LRU cache should be explicit
    pub fn new() -> Self {
        Self {
            lru: lru::LruCache::unbounded(),
        }
    }

    pub fn check(
        &mut self,
        params: &RateLimitParams,
        key: &str,
        now: Instant,
    ) -> (RateLimitOutcome, Vec<(String, u32)>) {
        let mut ended = Vec::new();
        while let Some((_, state)) = self.lru.peek_lru() {
            let interval_end = state.start + params.interval;
            if interval_end > now {
                break;
            }
            let (key, state) = self.lru.pop_lru().unwrap();
            // only report ended keys if they actually reached the point where they should've been suppressed
            if state.emitted >= params.burst {
                ended.push((key, state.suppressed));
            }
        }

        match self.lru.peek_mut(key) {
            // NB: emitted+1 <=> burst is the same as emitted <=> burst-1, but without overflow when burst=0
            Some(state) => match (state.emitted + 1).cmp(&params.burst) {
                Ordering::Less => {
                    state.emitted += 1;
                    (RateLimitOutcome::EmitNormally, ended)
                }
                Ordering::Equal => {
                    state.emitted += 1;

                    let interval_end = state.start + params.interval;
                    let until_interval_end = interval_end.saturating_duration_since(now);

                    (
                        RateLimitOutcome::EmitWithSuppressionBeginNotice(until_interval_end),
                        ended,
                    )
                }
                Ordering::Greater => {
                    state.suppressed += 1;
                    (RateLimitOutcome::Supress, ended)
                }
            },
            None => {
                // surely we can't be rate limited yet, nobody would use burst=0 :clueless:
                self.lru.push(
                    key.to_string(),
                    StateItem {
                        emitted: 1,
                        suppressed: 0,
                        start: now,
                    },
                );
                (RateLimitOutcome::EmitNormally, ended)
            }
        }
    }
}

#[doc(hidden)]
pub fn tlog_ratelimited_impl(
    rstatic: &slog::RecordStatic<'_>,
    message: std::fmt::Arguments<'_>,
    rate_limiter: &'static std::thread::LocalKey<RefCell<KeyedRateLimitState>>,
) {
    let logger = crate::tlog::root();
    let params = &LOG_RATELIMIT_PARAMS;

    // Skip all the rate limiting machinery if the emitted log level is below the configured
    // log level. This skips costly operations of stringifying the full message and checking
    // the LRU hash table.
    if !slog::Drain::is_enabled(logger, rstatic.level) {
        return;
    }

    // Eagerly stringify the record to use it as a rate limiting key.
    // NOTE: the current implementation does not support slog KV-values (it's a compile error),
    // so we don't take them into account.
    // Supporting them would require calling the `KV` implementation and serializing it into
    // a hashable type that would then itself implement `KV` for re-emitting.
    // See: https://git.picodata.io/core/picodata/-/work_items/3263.
    let message_str = match message.as_str() {
        Some(message_str) => Cow::Borrowed(message_str),
        None => Cow::Owned(format!("{}", message)),
    };

    let (outcome, suppression_ends) =
        // Allow integration tests to disable the rate limit.
        // Don't use `error_injection!` macro here to not spam the logs.
        if crate::error_injection::is_enabled("DISABLE_LOG_RATE_LIMIT") {
            (RateLimitOutcome::EmitNormally, Vec::new())
        } else {
            // We could use `tarantool::time::Instant::now_fiber` here, and that would be slightly less overhead.
            // But we want logging to be usable outside tarantool runtime, and `now_fiber` isn't.
            let now = Instant::now();
            rate_limiter.with(|rl| rl.borrow_mut().check(params, &message_str, now))
        };

    for (end_message, suppressed) in suppression_ends {
        logger.log(&slog::Record::new(
            rstatic,
            &format_args!(
                "The following message was previously suppressed {suppressed} times: {end_message}"
            ),
            // remember: we don't support slog KV-values in tlog_ratelimited
            slog::b!(),
        ));
    }

    match outcome {
        RateLimitOutcome::EmitNormally => {
            logger.log(&slog::Record::new(
                rstatic,
                &format_args!("{}", message_str),
                slog::b!(),
            ));
        }
        RateLimitOutcome::EmitWithSuppressionBeginNotice(duration) => {
            logger.log(&slog::Record::new(
                rstatic,
                &format_args!(
                    "The following message will be suppressed for the next {duration:.03}s: {message_str}",
                    duration = duration.as_secs_f64()
                ),
                slog::b!(),
            ));
        }
        RateLimitOutcome::Supress => {}
    }
}

/// Same as [`crate::tlog!`], but applies a rate limit to the printed logs.
///
/// At most `LOG_RATELIMIT_PARAMS.burst` (10) messages are emitted per
/// `LOG_RATELIMIT_PARAMS.interval` (5 seconds) — same parameters as used by
/// tarantool in `SAY_RATELIMIT_INTERVAL` and `SAY_RATELIMIT_BURST`.
///
/// The rate limit is applied to each unique message string generated by this call site independently.
/// For example, if you call macro like this: `tlog_ratelimited!(Info, "hello {}", subject)`, each
/// unique `subject` gets its own independent rate limit.
///
/// When the last message that is under the allowable burst amount is printed, it is prepended with
/// a warning telling that a message is being suppressed.
///
/// Additionally, once the rate limit interval is over and another log call is made, another message
/// will be printed, telling how many lines were suppressed.
///
/// # Usage guidance
///
/// This macro should be used for log statements that satisfy these two criteria:
///
/// 1. There are situations where the log statement is noisy, drowning out other useful messages.
/// 2. It's undesirable to lower the severity of the message and/or impossible to restructure code
///    so that the message is not logged as often.
///
/// # Performance considerations
///
/// Since `tlog_ratelimited` is applied to each unique message, it has to store those messages in a
/// hash table, incurring additional overhead on memory and cache. From benchmarking, the effect
/// stays *tolerable* if the number of unique log strings generated by the call site is kept under
/// 1000. With 24-byte messages, this uses about 100 KiB. Note that this causes not only memory
/// usage, but also cache pressure.
#[macro_export]
macro_rules! tlog_ratelimited {
    (@do_error_on_kv) => {
        // Ideally we would want the error span to point at offending tokens here, but it's
        // not possible in declarative macros right now :(
        // See: https://github.com/rust-lang/rust/issues/44535
        compile_error!("tlog_ratelimited doesn't support providing KV-values for now");
    };

    // The complicated macro below parses the arguments passed to `tlog_ratelimited` the same way
    // `slog::log` does to separate kv arguments and the message. Passing KV arguments currently
    // results in an error saying that it's unsupported, but we may support it in the future.
    // `2` means that `;` was already found
    (@get_message 2 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr) => {
        {
            // The $kv list should be empty here, since all branches that append to it
            // were changed to emit a compile_error! instead.
            // So it's safe to ignore it.
            format_args!($msg_fmt, $($fmt)*)
        }
    };
    (@get_message 2 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr,) => {
        $crate::tlog_ratelimited!(@get_message 2 @ { $($fmt)* }, { $($kv)* }, $msg_fmt)
    };
    (@get_message 2 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr;) => {
        $crate::tlog_ratelimited!(@get_message 2 @ { $($fmt)* }, { $($kv)* }, $msg_fmt)
    };
    (@get_message 2 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr, $($args:tt)*) => {
        $crate::tlog_ratelimited!(@do_error_on_kv);
        // tlog_ratelimited!(@get_message 2 @ { $($fmt)* }, { $($kv)* $($args)*}, $msg_fmt)
    };
    // `1` means that we are still looking for `;`
    // -- handle named arguments to format string
    (@get_message 1 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr, $k:ident = $v:expr) => {
        $crate::tlog_ratelimited!(@do_error_on_kv);
        // tlog_ratelimited!(@get_message 2 @ { $($fmt)* $k = $v }, { $($kv)* stringify!($k) => $v, }, $msg_fmt)
    };
    (@get_message 1 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr, $k:ident = $v:expr;) => {
        $crate::tlog_ratelimited!(@do_error_on_kv);
        // tlog_ratelimited!(@get_message 2 @ { $($fmt)* $k = $v }, { $($kv)* stringify!($k) => $v, }, $msg_fmt)
    };
    (@get_message 1 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr, $k:ident = $v:expr,) => {
        $crate::tlog_ratelimited!(@do_error_on_kv);
        // tlog_ratelimited!(@get_message 2 @ { $($fmt)* $k = $v }, { $($kv)* stringify!($k) => $v, }, $msg_fmt)
    };
    (@get_message 1 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr, $k:ident = $v:expr; $($args:tt)*) => {
        $crate::tlog_ratelimited!(@do_error_on_kv);
        // tlog_ratelimited!(@get_message 2 @ { $($fmt)* $k = $v }, { $($kv)* stringify!($k) => $v, }, $msg_fmt, $($args)*)
    };
    (@get_message 1 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr, $k:ident = $v:expr, $($args:tt)*) => {
        $crate::tlog_ratelimited!(@do_error_on_kv);
        // tlog_ratelimited!(@get_message 1 @ { $($fmt)* $k = $v, }, { $($kv)* stringify!($k) => $v, }, $msg_fmt, $($args)*)
    };
    // -- look for `;` termination
    (@get_message 1 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr,) => {
        $crate::tlog_ratelimited!(@get_message 2 @ { $($fmt)* }, { $($kv)* }, $msg_fmt)
    };
    (@get_message 1 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr) => {
        $crate::tlog_ratelimited!(@get_message 2 @ { $($fmt)* }, { $($kv)* }, $msg_fmt)
    };
    (@get_message 1 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr, ; $($args:tt)*) => {
        $crate::tlog_ratelimited!(@get_message 1 @ { $($fmt)* }, { $($kv)* }, $msg_fmt; $($args)*)
    };
    (@get_message 1 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr; $($args:tt)*) => {
        $crate::tlog_ratelimited!(@get_message 2 @ { $($fmt)* }, { $($kv)* }, $msg_fmt, $($args)*)
    };
    // -- must be normal argument to format string
    (@get_message 1 @ { $($fmt:tt)* }, { $($kv:tt)* }, $msg_fmt:expr, $f:tt $($args:tt)*) => {
        $crate::tlog_ratelimited!(@get_message 1 @ { $($fmt)* $f }, { $($kv)* }, $msg_fmt, $($args)*)
    };
    (@get_message $($args:tt)*) => {
        $crate::tlog_ratelimited!(@get_message 1 @ { }, { } $($args)*)
    };

    ($lvl:ident, $($args:tt)*) => {{
        // Storing the RateLimitState in a thread local makes the rate limit per-thread, but, at
        // the same time, lets us not care about synchronizing the state between the threads.
        thread_local! {
            static RATE_LIMITER: std::cell::RefCell<$crate::tlog::ratelimit::KeyedRateLimitState> =
                std::cell::RefCell::new($crate::tlog::ratelimit::KeyedRateLimitState::new());
        }

        static RSTATIC: slog::RecordStatic<'_> = slog::record_static!(slog::Level::$lvl, "");
        let message = $crate::tlog_ratelimited!(@get_message, $($args)*);

        $crate::tlog::ratelimit::tlog_ratelimited_impl(&RSTATIC, message, &RATE_LIMITER);
    }}
}

#[cfg(test)]
mod tests {
    use super::{KeyedRateLimitState, RateLimitOutcome, RateLimitParams};
    use std::assert_matches;
    use std::time::{Duration, Instant};

    const TEST_PARAMS: RateLimitParams = RateLimitParams {
        interval: Duration::from_secs(5),
        burst: 10,
    };

    #[test]
    fn happy_path() {
        let mut state = KeyedRateLimitState::new();
        let now = Instant::now();

        // Emit 9 logs. Those should not be rate limited
        for _ in 0..9 {
            let (outcome, ended) = state.check(&TEST_PARAMS, "1", now);
            assert_eq!(outcome, RateLimitOutcome::EmitNormally);
            assert_eq!(ended, Vec::new());
        }

        // The tenth log is the last that can be emitted in the current burst window.
        let (outcome, ended) = state.check(&TEST_PARAMS, "1", now);
        assert_eq!(
            outcome,
            RateLimitOutcome::EmitWithSuppressionBeginNotice(TEST_PARAMS.interval)
        );
        assert_eq!(ended, Vec::new());

        // now it will just get suppressed
        let (outcome, ended) = state.check(&TEST_PARAMS, "1", now);
        assert_eq!(outcome, RateLimitOutcome::Supress);
        assert_eq!(ended, Vec::new());

        // After 10 seconds the burst interval is over, and we get to print a warning that messages were previously suppressed
        let (outcome, ended) = state.check(&TEST_PARAMS, "1", now + Duration::from_secs(10));
        assert_eq!(outcome, RateLimitOutcome::EmitNormally);
        assert_eq!(ended, vec![("1".to_string(), 1)]);
    }

    #[test]
    fn independent_keys() {
        let mut state = KeyedRateLimitState::new();
        let now = Instant::now();

        // Emit 10 logs.
        for _ in 0..10 {
            let (outcome, ended) = state.check(&TEST_PARAMS, "1", now);
            assert_matches!(
                outcome,
                RateLimitOutcome::EmitNormally
                    | RateLimitOutcome::EmitWithSuppressionBeginNotice(_)
            );
            assert_eq!(ended, Vec::new());
        }

        // The next log with key 1 will be suppressed
        let (outcome, ended) = state.check(&TEST_PARAMS, "1", now);
        assert_eq!(outcome, RateLimitOutcome::Supress);
        assert_eq!(ended, Vec::new());

        // A log with a different key can still be emitted though
        let (outcome, ended) = state.check(&TEST_PARAMS, "2", now);
        assert_eq!(outcome, RateLimitOutcome::EmitNormally);
        assert_eq!(ended, Vec::new());

        // After 10 seconds the burst interval is over, we get to report the end of suppression of 1,
        // even when we log for key 2.
        let (outcome, ended) = state.check(&TEST_PARAMS, "2", now + Duration::from_secs(10));
        assert_eq!(outcome, RateLimitOutcome::EmitNormally);
        assert_eq!(ended, vec![("1".to_string(), 1)]);
    }

    #[test]
    fn no_stray_end_events() {
        let mut state = KeyedRateLimitState::new();
        let now = Instant::now();

        // Add an item to the LRU
        let (outcome, ended) = state.check(&TEST_PARAMS, "1", now);
        assert_eq!(outcome, RateLimitOutcome::EmitNormally);
        assert_eq!(ended, Vec::new());

        // After 10 seconds, it would be popped from LRU, but not emitted, since it didn't reach the
        // burst threshold. No "ended" message is generated
        let (outcome, ended) = state.check(&TEST_PARAMS, "1", now + Duration::from_secs(10));
        assert_eq!(outcome, RateLimitOutcome::EmitNormally);
        assert_eq!(ended, Vec::new());
    }
}
