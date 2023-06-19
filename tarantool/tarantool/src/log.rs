//! Logging utils. See ["log" crate documentation](https://docs.rs/log/) for details
//!
//! Example:
//! ```no_run
//! use log::{info, LevelFilter};
//! use tarantool::log::{TarantoolLogger, say, SayLevel};
//!
//! static LOGGER: TarantoolLogger = TarantoolLogger::new();
//! log::set_logger(&LOGGER).unwrap();
//! log::set_max_level(LevelFilter::Debug);
//! # let username = "Dave";
//! info!("Hello {}", username);
//!
//! // Or you can write to Tarantool logger directly
//! say(SayLevel::Info, "log_demo.rs", 9, None, "Hello world");
//! ```
//!
//! See also:
//! - [Lua reference: Module log](https://www.tarantool.io/en/doc/latest/reference/reference_lua/log/)
//! - [C API reference: Module say (logging)](https://www.tarantool.io/en/doc/latest/dev_guide/reference_capi/say/)
use std::ffi::CString;
use std::ptr::null;

use log::{Level, Log, Metadata, Record};
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};

use crate::ffi::tarantool as ffi;

/// [Log](https://docs.rs/log/latest/log/trait.Log.html) trait implementation. Wraps [say()](fn.say.html).
pub struct TarantoolLogger(fn(Level) -> SayLevel);

impl TarantoolLogger {
    #[inline(always)]
    pub const fn new() -> Self {
        const DEFAULT_MAPPING: fn(Level) -> SayLevel = |l: Level| l.into();
        TarantoolLogger(DEFAULT_MAPPING)
    }

    #[inline(always)]
    pub fn with_mapping(map_fn: fn(Level) -> SayLevel) -> Self {
        TarantoolLogger(map_fn)
    }

    /// Convert [`log::Level`] to [`SayLevel`] taking the mapping into account.
    #[inline(always)]
    pub fn convert_level(&self, level: Level) -> SayLevel {
        (self.0)(level)
    }
}

impl Log for TarantoolLogger {
    #[inline(always)]
    fn enabled(&self, metadata: &Metadata) -> bool {
        let level = self.convert_level(metadata.level());
        level <= SayLevel::from_i32(unsafe { ffi::LOG_LEVEL }).unwrap()
    }

    #[inline]
    fn log(&self, record: &Record) {
        say(
            self.convert_level(record.level()),
            record.file().unwrap_or_default(),
            record.line().unwrap_or(0) as i32,
            None,
            record.args().to_string().as_str(),
        )
    }

    #[inline(always)]
    fn flush(&self) {}
}

/// Tarantool-native logging levels (use it with [say()](fn.say.html))
#[repr(u32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, ToPrimitive, FromPrimitive)]
pub enum SayLevel {
    Fatal = 0,
    System = 1,
    Error = 2,
    Crit = 3,
    Warn = 4,
    Info = 5,
    Verbose = 6,
    Debug = 7,
}

impl From<Level> for SayLevel {
    fn from(level: Level) -> Self {
        match level {
            Level::Error => SayLevel::Error,
            Level::Warn => SayLevel::Warn,
            Level::Info => SayLevel::Info,
            Level::Debug => SayLevel::Debug,
            Level::Trace => SayLevel::Debug,
        }
    }
}

/// Format and print a message to the Tarantool log file.
#[inline]
pub fn say(level: SayLevel, file: &str, line: i32, error: Option<&str>, message: &str) {
    let level = level.to_i32().unwrap();
    let file = CString::new(file).unwrap();
    let error = error.map(|e| CString::new(e).unwrap());
    let error_ptr = match error {
        Some(ref error) => error.as_ptr(),
        None => null(),
    };
    let message = CString::new(message).unwrap();

    unsafe { ffi::SAY_FN.unwrap()(level, file.as_ptr(), line, error_ptr, message.as_ptr()) }
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use crate::lua_state;

    struct RestoreLogLevel {
        log_level: String,
    }
    impl Drop for RestoreLogLevel {
        fn drop(&mut self) {
            let lua = lua_state();
            lua.exec_with("return box.cfg { log_level = ... }", &self.log_level)
                .unwrap();
        }
    }

    #[crate::test(tarantool = "crate")]
    fn is_enabled() {
        let lua = lua_state();
        let log_level_saved: String = lua.eval("return box.cfg.log_level").unwrap();
        let _ = RestoreLogLevel {
            log_level: log_level_saved,
        };

        // default mapping
        lua.exec("box.cfg { log_level = 'info' }").unwrap();
        let logger = TarantoolLogger::new();
        assert!(logger.enabled(&log::Metadata::builder().level(Level::Error).build()));
        assert!(!logger.enabled(&log::Metadata::builder().level(Level::Debug).build()));
        assert!(logger.enabled(&log::Metadata::builder().level(Level::Info).build()));

        // debug > info, so enabled is never true
        lua.exec("box.cfg { log_level = 'info' }").unwrap();
        let logger = TarantoolLogger::with_mapping(|_| SayLevel::Debug);
        assert!(!logger.enabled(&log::Metadata::builder().level(Level::Error).build()));
        assert!(!logger.enabled(&log::Metadata::builder().level(Level::Debug).build()));
        assert!(!logger.enabled(&log::Metadata::builder().level(Level::Info).build()));

        // debug = debug, so enabled is always true
        lua.exec("box.cfg { log_level = 'debug' }").unwrap();
        let logger = TarantoolLogger::with_mapping(|_| SayLevel::Debug);
        assert!(logger.enabled(&log::Metadata::builder().level(Level::Error).build()));
        assert!(logger.enabled(&log::Metadata::builder().level(Level::Debug).build()));
        assert!(logger.enabled(&log::Metadata::builder().level(Level::Info).build()));
    }
}
