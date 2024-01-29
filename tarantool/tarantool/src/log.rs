//! Logging utils. See ["log" crate documentation](https://docs.rs/log/) for details
//!
//! Example:
//! ```no_run
//! use log::{info, LevelFilter};
//! use tarantool::log::{TarantoolLogger, SayLevel};
//!
//! static LOGGER: TarantoolLogger = TarantoolLogger::new();
//! log::set_logger(&LOGGER).unwrap();
//! log::set_max_level(LevelFilter::Debug);
//! # let username = "Dave";
//! info!("Hello {}", username);
//!
//! // Or you can write to Tarantool logger directly
//! tarantool::say_verbose!("Logging some messages...");
//! tarantool::say_info!("Hello world");
//! tarantool::say_warn!("Watch out!");
//! ```
//!
//! See also:
//! - [Lua reference: Module log](https://www.tarantool.io/en/doc/latest/reference/reference_lua/log/)
//! - [C API reference: Module say (logging)](https://www.tarantool.io/en/doc/latest/dev_guide/reference_capi/say/)
use std::ffi::CString;
use std::ptr::null;

use log::{Level, Log, Metadata, Record};

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
        level <= current_level()
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

crate::define_enum_with_introspection! {
    /// Tarantool-native logging levels (use it with [say()](fn.say.html))
    #[repr(u32)]
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
}

/// Get current level of the default tarantool logger.
///
/// See also <https://www.tarantool.io/en/doc/latest/reference/configuration/#cfg-logging-log-level>.
#[inline(always)]
pub fn current_level() -> SayLevel {
    let level = SayLevel::from_i64(unsafe { ffi::LOG_LEVEL as _ });
    debug_assert!(level.is_some());
    level.unwrap_or(SayLevel::Info)
}

/// Set current level of the default tarantool logger.
///
/// See also <https://www.tarantool.io/en/doc/latest/reference/configuration/#cfg-logging-log-level>.
#[inline(always)]
pub fn set_current_level(level: SayLevel) {
    unsafe {
        ffi::say_set_log_level(level as _);
    }
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
            let res = crate::unwrap_or!(Self::from_i64(lvl as _), {
                return Err((
                    lua,
                    tlua::WrongType::info("reading tarantool log level")
                        .expected(format!(
                            "an integer in range {}..={}",
                            Self::MIN as u32,
                            Self::MAX as u32
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

#[track_caller]
pub fn say_format_args(level: SayLevel, args: std::fmt::Arguments) {
    if current_level() < level {
        return;
    }
    let loc = std::panic::Location::caller();
    let file = CString::new(loc.file()).unwrap();
    let line = loc.line();

    let mut error_str = String::new();
    let mut error_ptr = std::ptr::null();
    if matches!(level, SayLevel::System) {
        error_str = std::io::Error::last_os_error().to_string();
        error_str.push('\0');
        // error_str must outlive error_ptr
        error_ptr = error_str.as_ptr();
    }

    let mut message = std::fmt::format(args);
    message.push('\0');

    unsafe {
        ffi::SAY_FN.expect("_say is always not NULL")(
            level as _,
            file.as_ptr(),
            line as _,
            error_ptr as _,
            crate::c_ptr!("%s"),
            message.as_ptr(),
        )
    }

    drop(error_str);
}

#[macro_export]
macro_rules! say_fatal {
    ($($f:tt)*) => {
        $crate::log::say_format_args($crate::log::SayLevel::Fatal, ::std::format_args!($($f)*))
    }
}

#[macro_export]
macro_rules! say_sys_error {
    ($($f:tt)*) => {
        $crate::log::say_format_args($crate::log::SayLevel::System, ::std::format_args!($($f)*))
    }
}

#[macro_export]
macro_rules! say_error {
    ($($f:tt)*) => {
        $crate::log::say_format_args($crate::log::SayLevel::Error, ::std::format_args!($($f)*))
    }
}

#[macro_export]
macro_rules! say_crit {
    ($($f:tt)*) => {
        $crate::log::say_format_args($crate::log::SayLevel::Crit, ::std::format_args!($($f)*))
    }
}

#[macro_export]
macro_rules! say_warn {
    ($($f:tt)*) => {
        $crate::log::say_format_args($crate::log::SayLevel::Warn, ::std::format_args!($($f)*))
    }
}

#[macro_export]
macro_rules! say_verbose {
    ($($f:tt)*) => {
        $crate::log::say_format_args($crate::log::SayLevel::Verbose, ::std::format_args!($($f)*))
    }
}

#[macro_export]
macro_rules! say_debug {
    ($($f:tt)*) => {
        $crate::log::say_format_args($crate::log::SayLevel::Debug, ::std::format_args!($($f)*))
    }
}

#[macro_export]
macro_rules! say_info {
    ($($f:tt)*) => {
        $crate::log::say_format_args($crate::log::SayLevel::Info, ::std::format_args!($($f)*))
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

    #[crate::test(tarantool = "crate")]
    fn say_macros() {
        // TODO: it would be nice if we could log these to a file and then check
        // that file's contents, but unfortunately tarantool doesn't allow changing
        // logging configuration after the first box.cfg, so there's a bunch of
        // hoops to jump through to get this working.
        // For now we just check that this code compiles and doesn't crash.
        let var = "World";
        say_debug!("Hello, {var}! {}", 69);
        say_verbose!("Hello, {var}! {}", 69);
        say_info!("Hello, {var}! {}", 69);
        say_warn!("Hello, {var}! {}", 69);
        say_crit!("Hello, {var}! {}", 69);
        say_error!("Hello, {var}! {}", 69);
        say_fatal!("Hello, {var}! {}", 69);

        #[rustfmt::skip]
        let rc = unsafe { libc::open(crate::c_ptr!("/this file doesn't exist hopefully"), libc::O_RDONLY) };
        assert_eq!(rc, -1);
        // This will print the os error saying `No such file or directory` or something similar
        say_sys_error!("Hello, {var}! {}", 69);
    }

    #[crate::test(tarantool = "crate")]
    fn set_current_level() {
        let level_before = super::current_level();
        let _guard = crate::test::util::on_scope_exit(|| super::set_current_level(level_before));

        super::set_current_level(SayLevel::Info);
        assert_eq!(super::current_level(), SayLevel::Info);

        super::set_current_level(SayLevel::Verbose);
        assert_eq!(super::current_level(), SayLevel::Verbose);

        super::set_current_level(SayLevel::Warn);
        assert_eq!(super::current_level(), SayLevel::Warn);
    }
}
