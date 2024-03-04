use ::tarantool::log::{say, SayLevel};
use once_cell::sync::Lazy;
use std::collections::HashMap;

static mut LOG_LEVEL: SayLevel = SayLevel::Info;

pub fn set_log_level(lvl: SayLevel) {
    // SAFETY: Here and onward, globals are used in single thread.
    unsafe {
        LOG_LEVEL = lvl;
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
pub fn set_core_logger_is_initialized() {
    unsafe { CORE_LOGGER_IS_INITIALIZED = true }
}

#[rustfmt::skip]
#[repr(u8)]
#[derive(Clone, Copy)]
pub enum Color {
    Red     = 1,
    Green   = 2,
    Blue    = 4,
    Cyan    = 6,
    Yellow  = 3,
    Magenta = 5,
    White   = 7,
    Black   = 0,
}

pub static mut HIGHLIGHT: Option<HashMap<String, Color>> = None;

#[inline]
pub fn clear_highlight() {
    unsafe {
        HIGHLIGHT = None;
    }
}

#[inline]
pub fn highlight_key(key: impl Into<String> + AsRef<str>, color: Option<Color>) {
    let map = unsafe { &mut HIGHLIGHT }.get_or_insert_with(HashMap::new);
    if let Some(color) = color {
        map.insert(key.into(), color);
    } else {
        map.remove(key.as_ref());
    }
}

/// A helper for serializing slog's record to plain string.
/// We use this for picodata's regular log (tlog).
pub struct StrSerializer {
    str: String,
}

impl slog::Serializer for StrSerializer {
    fn emit_arguments(&mut self, key: slog::Key, val: &std::fmt::Arguments) -> slog::Result {
        use std::fmt::Write;
        match unsafe { HIGHLIGHT.as_ref() }.and_then(|h| h.get(key)) {
            Some(&color) => {
                let color = color as u8;
                write!(self.str, ", \x1b[3{color}m{key}: {val}\x1b[0m").unwrap();
            }
            _ => write!(self.str, ", {key}: {val}").unwrap(),
        }
        Ok(())
    }
}

pub static mut DONT_LOG_KV_FOR_NEXT_SENDING_FROM: bool = false;

impl StrSerializer {
    /// Format slog's record as plain string. Most suitable for implementing [`slog::Drain`].
    pub fn format_message(record: &slog::Record, values: &slog::OwnedKVList) -> String {
        let msg = if unsafe { CORE_LOGGER_IS_INITIALIZED } {
            format!("{}", record.msg())
        } else {
            let level = slog_level_to_say_level(record.level());
            format!(
                "{color_code}{prefix}{msg}\x1b[0m",
                color_code = color_for_log_level(level),
                prefix = prefix_for_log_level(level),
                msg = record.msg(),
            )
        };
        let mut s = StrSerializer { str: msg };

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
        let logger = &$crate::tlog::root();
        slog::slog_log!(logger, slog::Level::$lvl, "", $($args)*);
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
        if level > unsafe { LOG_LEVEL } {
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

#[inline]
fn prefix_for_log_level(level: SayLevel) -> &'static str {
    match level {
        SayLevel::System => "SYSTEM ERROR: ",
        SayLevel::Fatal => "FATAL: ",
        SayLevel::Crit => "CRITICAL: ",
        SayLevel::Error => "ERROR: ",
        SayLevel::Warn => "WARNING: ",
        SayLevel::Info => "",
        SayLevel::Verbose => "V: ",
        SayLevel::Debug => "DBG: ",
    }
}

#[inline]
fn color_for_log_level(level: SayLevel) -> &'static str {
    match level {
        SayLevel::System | SayLevel::Fatal | SayLevel::Crit | SayLevel::Error => "\x1b[31m", // red
        SayLevel::Warn => "\x1b[33m", // yellow
        SayLevel::Info | SayLevel::Verbose | SayLevel::Debug => "",
    }
}
