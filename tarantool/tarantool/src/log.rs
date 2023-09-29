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
use num_traits::FromPrimitive;

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
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, num_derive::FromPrimitive)]
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

crate::define_str_enum! {
    /// Is only used for implementing LuaRead and Push for [`SayLevel`].
    enum SayLevelStr {
        Fatal = "fatal",
        System = "system",
        Error = "error",
        Crit = "crit",
        Warn = "warn",
        Info = "info",
        Verbose = "verbose",
        Debug = "debug",
    }
}

impl<L> tlua::LuaRead<L> for SayLevel
where
    L: tlua::AsLua,
{
    #[inline]
    fn lua_read_at_position(lua: L, index: std::num::NonZeroI32) -> tlua::ReadResult<Self, L> {
        let lua_type = unsafe { tlua::ffi::lua_type(lua.as_lua(), index.into()) };

        if lua_type == tlua::ffi::LUA_TSTRING {
            let l = crate::unwrap_ok_or!(
                SayLevelStr::lua_read_at_position(&lua, index),
                Err((_, e)) => {
                    return Err((lua, e.when("reading tarantool log level")));
                }
            );
            let res = match l {
                SayLevelStr::Fatal => Self::Fatal,
                SayLevelStr::System => Self::System,
                SayLevelStr::Error => Self::Error,
                SayLevelStr::Crit => Self::Crit,
                SayLevelStr::Warn => Self::Warn,
                SayLevelStr::Info => Self::Info,
                SayLevelStr::Verbose => Self::Verbose,
                SayLevelStr::Debug => Self::Debug,
            };
            return Ok(res);
        }

        if lua_type == tlua::ffi::LUA_TNUMBER {
            let lvl = u32::lua_read_at_position(&lua, index)
                .ok()
                .expect("just made sure this is a number, so reading shouldn't ever fail");
            let res = crate::unwrap_or!(Self::from_u32(lvl), {
                return Err((
                    lua,
                    tlua::WrongType::info("reading tarantool log level")
                        .expected(format!(
                            "an integer in range {}..={}",
                            Self::Fatal as u32,
                            Self::Debug as u32
                        ))
                        .actual(format!("{lvl}")),
                ));
            });
            return Ok(res);
        }

        let err = tlua::WrongType::info("reading tarantool log level")
            .expected("string or number")
            .actual_single_lua(&lua, index);
        Err((lua, err))
    }
}

impl<L> tlua::Push<L> for SayLevel
where
    L: tlua::AsLua,
{
    type Err = tlua::Void;

    #[inline]
    fn push_to_lua(&self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        let lvl = match self {
            Self::Fatal => SayLevelStr::Fatal,
            Self::System => SayLevelStr::System,
            Self::Error => SayLevelStr::Error,
            Self::Crit => SayLevelStr::Crit,
            Self::Warn => SayLevelStr::Warn,
            Self::Info => SayLevelStr::Info,
            Self::Verbose => SayLevelStr::Verbose,
            Self::Debug => SayLevelStr::Debug,
        };
        tlua::Push::push_to_lua(&lvl, lua)
    }
}
impl<L> tlua::PushOne<L> for SayLevel where L: tlua::AsLua {}

impl<L> tlua::PushInto<L> for SayLevel
where
    L: tlua::AsLua,
{
    type Err = tlua::Void;

    #[inline(always)]
    fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        tlua::Push::push_to_lua(&self, lua)
    }
}
impl<L> tlua::PushOneInto<L> for SayLevel where L: tlua::AsLua {}

/// Format and print a message to the Tarantool log file.
#[inline]
pub fn say(level: SayLevel, file: &str, line: i32, error: Option<&str>, message: &str) {
    let file = CString::new(file).unwrap();
    let error = error.map(|e| CString::new(e).unwrap());
    let error_ptr = match error {
        Some(ref error) => error.as_ptr(),
        None => null(),
    };
    let message = CString::new(message).unwrap();

    unsafe {
        ffi::SAY_FN.unwrap()(
            level as _,
            file.as_ptr(),
            line,
            error_ptr,
            crate::c_ptr!("%s"),
            message.as_ptr(),
        )
    }
}

#[cfg(feature = "internal_test")]
#[cfg(not(test))]
mod tests {
    use super::*;
    use crate::lua_state;
    use log::{warn, LevelFilter};
    use once_cell::sync::Lazy;

    struct RestoreLogLevel {
        log_level: SayLevel,
    }
    impl Drop for RestoreLogLevel {
        fn drop(&mut self) {
            let lua = lua_state();
            lua.exec_with("box.cfg { log_level = ... }", &self.log_level)
                .unwrap();
        }
    }

    #[crate::test(tarantool = "crate")]
    fn is_enabled() {
        let lua = lua_state();
        let _restore_log_level_when_dropped = RestoreLogLevel {
            log_level: lua.eval("return box.cfg.log_level").unwrap(),
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

    // This test panics cause logger already set in log_with_user_defined_mapping test.
    #[crate::test(tarantool = "crate", should_panic)]
    fn zlog() {
        static TLOGGER: TarantoolLogger = TarantoolLogger::new();
        log::set_logger(&TLOGGER).unwrap();
        log::set_max_level(LevelFilter::Debug);
        warn!(target: "target", "message {}", 99);

        say(SayLevel::Warn, "<file>", 0, Some("<error>"), "<message>");
    }

    #[crate::test(tarantool = "crate")]
    fn log_with_user_defined_mapping() {
        static TLOGGER: Lazy<TarantoolLogger> = Lazy::new(|| {
            TarantoolLogger::with_mapping(|level: Level| match level {
                Level::Warn => SayLevel::Info,
                _ => SayLevel::Warn,
            })
        });

        log::set_logger(&*TLOGGER).unwrap();
        log::set_max_level(LevelFilter::Debug);
        warn!(target: "target", "message {}", 99);

        say(SayLevel::Warn, "<file>", 0, Some("<error>"), "<message>");
    }

    #[crate::test(tarantool = "crate")]
    fn log_level_to_from_lua() {
        let lua = crate::lua_state();

        let lvl: SayLevel = lua.eval("return 'debug'").unwrap();
        assert_eq!(lvl, SayLevel::Debug);

        let lvl: SayLevel = lua.eval("return 5").unwrap();
        assert_eq!(lvl, SayLevel::Info);

        let msg = lua.eval::<SayLevel>("return 69").unwrap_err().to_string();
        assert_eq!(
            msg,
            "failed reading tarantool log level: an integer in range 0..=7 expected, got 69
    while reading value(s) returned by Lua: tarantool::log::SayLevel expected, got number"
        );

        let msg = lua
            .eval::<SayLevel>("return 'nightmare'")
            .unwrap_err()
            .to_string();
        assert_eq!(
            msg,
            "failed reading tarantool log level: one of [\"fatal\", \"system\", \"error\", \"crit\", \"warn\", \"info\", \"verbose\", \"debug\"] expected, got string 'nightmare'
    while reading value(s) returned by Lua: tarantool::log::SayLevel expected, got string"
        );

        let msg = lua
            .eval::<SayLevel>("return { lasagna = 'delicious' }")
            .unwrap_err()
            .to_string();
        assert_eq!(
            msg,
            "failed reading tarantool log level: string or number expected, got table
    while reading value(s) returned by Lua: tarantool::log::SayLevel expected, got table"
        );

        let lvl: String = lua.eval_with("return ...", SayLevel::Fatal).unwrap();
        assert_eq!(lvl, "fatal");

        let lvl: String = lua.eval_with("return ...", &SayLevel::Crit).unwrap();
        assert_eq!(lvl, "crit");
    }

    #[crate::test(tarantool = "crate")]
    fn log_format_sequences() {
        for c in b'a'..=b'z' {
            let c = c as char;
            let s = format!("%{c}");
            say(SayLevel::Warn, "<file>", 0, Some("<error>"), &s);
        }
    }
}
