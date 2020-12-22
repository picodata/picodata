//! Logging utils. See ["log" crate documentation](https://docs.rs/log/) for details
//!
//! Example:
//! ```rust
//! use log::{info, LevelFilter};
//! use tarantool::log::{TarantoolLogger, say, SayLevel};
//!
//! log::set_logger(&TarantoolLogger {}).unwrap();
//! log::set_max_level(LevelFilter::Debug);
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
use std::mem::forget;

use failure::_core::ptr::null;
use log::{Level, Log, Metadata, Record};
use num_traits::{FromPrimitive, ToPrimitive};

/// [Log](https://docs.rs/log/latest/log/trait.Log.html) trait implementation. Wraps [say()](fn.say.html).
pub struct TarantoolLogger {}

impl Log for TarantoolLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        let level: SayLevel = metadata.level().into();
        level <= SayLevel::from_i32(unsafe { ffi::LOG_LEVEL }).unwrap()
    }

    fn log(&self, record: &Record) {
        say(
            record.level().into(),
            record.file().unwrap_or_default(),
            record.line().unwrap_or(0) as i32,
            None,
            record.args().to_string().as_str(),
        )
    }

    fn flush(&self) {}
}

/// Tarantool-native logging levels (use it with [say()](fn.say.html))
#[repr(u32)]
#[derive(Debug, Clone, PartialEq, PartialOrd, ToPrimitive, FromPrimitive)]
pub enum SayLevel {
    Fatal = 0,
    System = 1,
    Error = 2,
    Crit = 3,
    Warn = 4,
    Info = 5,
    Debug = 6,
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

/// Format and print a message to Tarantool log file.
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

    unsafe { ffi::SAY_FN.unwrap()(level, file.as_ptr(), line, error_ptr, message.as_ptr()) };

    forget(file);
    forget(message);
    if error.is_some() {
        forget(error.unwrap());
    }
}

mod ffi {
    use std::os::raw::{c_char, c_int};

    pub type SayFunc = Option<
        unsafe extern "C" fn(c_int, *const c_char, c_int, *const c_char, *const c_char, ...),
    >;

    extern "C" {
        #[link_name = "log_level"]
        pub static mut LOG_LEVEL: c_int;

        #[link_name = "_say"]
        pub static mut SAY_FN: SayFunc;
    }
}
