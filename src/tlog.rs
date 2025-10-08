use crate::config::LogFormat;
use ::tarantool::log::{say, SayLevel};
use once_cell::sync::Lazy;
use std::ffi::CString;
use tarantool::fiber;

#[inline(always)]
pub fn init_core_logger(destination: Option<&str>, level: SayLevel, format: LogFormat) {
    let mut destination_ptr: *const u8 = std::ptr::null();
    let destination_cstr;
    if let Some(destination) = destination {
        destination_cstr =
            CString::new(destination).expect("log destination shouldn't contain nul-bytes");
        destination_ptr = destination_cstr.as_ptr() as _;
    }

    // SAFETY: `destination_cstr` must outlive this function call.
    unsafe {
        say_logger_init(
            destination_ptr,
            level as _,
            // Default behavior of "nonblocking" for corresponding logger type.
            -1,
            format.as_cstr().as_ptr() as _,
        );
    }

    // TODO: move to tarantool-module
    extern "C" {
        /// Initialize the default tarantool logger (accessed via the say_* functions).
        ///
        /// Parameters:
        /// - `init_str`: Destination descriptor. Pass a `null` for default
        ///   behavior of loggin to stderr.
        ///   (see also <https://www.tarantool.io/en/doc/latest/reference/configuration/#cfg-logging-log>)
        ///
        /// - `level`: Log level
        ///
        /// - `nonblock`:
        ///     - `< 0`: Use default for the corresponding logger type
        ///     - `0`: Always block until the message is written to the destination
        ///     - `> 0`: Never block until the message is written to the destination
        ///
        /// - `format`: Supported values are: "json", "plain"
        fn say_logger_init(init_str: *const u8, level: i32, nonblock: i32, format: *const u8);
    }
}

/// For user experience reasons we do some log messages enhancements (we add
/// some color and stuff) before the core tarantool logger is initialized.
/// After it's initialized the log messages may go into a file and may even be
/// in json format, so we don't do any additional processing for them. We could
/// do all of that via the tarantool's custom loggers support, but it doesn't
/// work that good and it's much simpler to just have a global variable. Maybe
/// this will change in the future.
static mut CORE_LOGGER_IS_INITIALIZED: bool = false;
pub fn set_core_logger_is_initialized(is: bool) {
    unsafe { CORE_LOGGER_IS_INITIALIZED = is }
}

/// A helper for serializing slog's record to plain string.
/// We use this for picodata's regular log (tlog).
pub struct StrSerializer {
    str: String,
}

impl slog::Serializer for StrSerializer {
    fn emit_arguments(&mut self, key: slog::Key, val: &std::fmt::Arguments) -> slog::Result {
        use std::fmt::Write;
        write!(self.str, ", {key}: {val}")?;
        Ok(())
    }
}

pub static mut DONT_LOG_KV_FOR_NEXT_SENDING_FROM: bool = false;

impl StrSerializer {
    /// Format slog's record as plain string. Most suitable for implementing [`slog::Drain`].
    pub fn format_message(record: &slog::Record, values: &slog::OwnedKVList) -> String {
        let mut s = StrSerializer {
            str: format!("{}", record.msg()),
        };

        // XXX: this is the ugliest hack I've done so far. Basically raft-rs
        // logs the full message it's sending if log level is Debug. But we're
        // sending a pretty big snapshot via raft. This results in logging of a
        // single message to take 400 milliseconds with protobuf (and > 1sec
        // with prost). We should really fix this in raft-rs or protobuf, but
        // because we delegate dependencies management to cargo there's no easy
        // way to do this. We can only wait until upstream merges our PR...
        // TODO: make a PR to raft-rs or protobuf
        // Related: https://git.picodata.io/picodata/picodata/picodata/-/issues/375
        if unsafe { DONT_LOG_KV_FOR_NEXT_SENDING_FROM } && s.str.contains("Sending from") {
            unsafe { DONT_LOG_KV_FOR_NEXT_SENDING_FROM = false }
            s.str.push_str(" MsgSnapshot");
        } else {
            use slog::KV;
            // It's safe to use .unwrap() here since
            // StrSerializer doesn't return anything but Ok()
            record.kv().serialize(record, &mut s).unwrap();
            values.serialize(record, &mut s).unwrap();
        }

        s.str
    }
}

/// A default root suitable for logging all around the project.
pub fn root() -> &'static slog::Logger {
    static ROOT: Lazy<slog::Logger> = Lazy::new(|| slog::Logger::root(Drain, slog::o!()));
    &ROOT
}

#[macro_export]
macro_rules! tlog {
    ($lvl:ident, $($args:tt)*) => {{
        // Safety: always safe
        let logger = &$crate::tlog::root();
        slog::log!(logger, slog::Level::$lvl, "", $($args)*);
    }}
}

pub struct Drain;

impl slog::Drain for Drain {
    type Ok = ();
    type Err = slog::Never;
    fn log(
        &self,
        record: &slog::Record,
        values: &slog::OwnedKVList,
    ) -> Result<Self::Ok, Self::Err> {
        // Max level is constant = trace
        // It's hardcoded in Cargo.toml dependency features
        // In runtime it's managed by tarantool box.cfg.log_level
        let level = slog_level_to_say_level(record.level());
        if level > tarantool::log::current_level() {
            return Ok(());
        }

        let msg = StrSerializer::format_message(record, values);
        say(level, record.file(), record.line() as i32, None, &msg);

        Ok(())
    }
}

#[inline]
fn slog_level_to_say_level(level: slog::Level) -> SayLevel {
    match level {
        slog::Level::Critical => SayLevel::Crit,
        slog::Level::Error => SayLevel::Error,
        slog::Level::Warning => SayLevel::Warn,
        slog::Level::Info => SayLevel::Info,
        slog::Level::Debug => SayLevel::Verbose,
        slog::Level::Trace => SayLevel::Debug,
    }
}

mod test {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[tarantool::test]
    fn thread_safe_logging() {
        let random_numbers = [6, 9, 4, 7, 2, 1, 8, 3, 5, 6];

        tlog!(Info, "start thread-safe logging from non-tx threads");
        thread::scope(|s| {
            let mut jhs = Vec::new();
            for (i, random_number) in random_numbers.into_iter().enumerate() {
                let jh = thread::Builder::new()
                    .name(format!("test thread #{i}"))
                    .spawn_scoped(s, move || {
                        tlog!(Info, "test thread #{i} started");
                        thread::sleep(Duration::from_millis(random_number));
                        tlog!(Info, "test thread #{i} ended");
                    })
                    .unwrap();
                jhs.push(jh);
            }

            for jh in jhs {
                jh.join().unwrap();
            }
        });

        // Now we have to wait until the helper fiber processes all the logging requests
        fiber::sleep(Duration::from_millis(500));

        tlog!(Info, "done thread-safe logging from non-tx threads");
    }
}
