use ::tarantool::log::{say, SayLevel};
use once_cell::sync::Lazy;
use std::{collections::HashMap, ptr};
use tarantool::cbus::unbounded as cbus;
use tarantool::fiber;

#[inline(always)]
pub fn set_log_level(lvl: SayLevel) {
    tarantool::log::set_current_level(lvl)
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
    // SAFETY: safe as long as only called from tx thread
    let map = unsafe { ptr::addr_of_mut!(HIGHLIGHT).as_mut() }
        .unwrap()
        .get_or_insert_with(HashMap::new);

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
        // Safety: always safe
        if unsafe { ::tarantool::ffi::tarantool::cord_is_main_dont_create() }
            // This is needed, because we call tlog!() before tarantool_main in
            // some cases, which means cord machinery is not yet initialized and
            // cord_is_main_dont_create returns false for the main thread.
            || !$crate::tlog::thread_safe_logger_is_initialized()
        {
            let logger = &$crate::tlog::root();
            slog::slog_log!(logger, slog::Level::$lvl, "", $($args)*);
        } else {
            // TODO: support the structured parameters
            let message = ::pico_proc_macro::format_but_ignore_everything_after_semicolon!($($args)*);

            if cfg!(test) {
                eprintln!("{}: {message}", stringify!($lvl));
            } else {
                $crate::tlog::log_from_non_tx_thread(slog::Level::$lvl, message);
            }
        }
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

static THREAD_SAFE_LOGGING: std::sync::Mutex<Option<cbus::Sender<ThreadSafeLogRequest>>> =
    std::sync::Mutex::new(None);

struct ThreadSafeLogRequest {
    level: slog::Level,
    thread_name: String,
    message: String,
}

pub fn log_from_non_tx_thread(level: slog::Level, message: String) {
    let Ok(sender) = THREAD_SAFE_LOGGING.lock() else {
        // Somebody panics while holding the lock, not much we can do here
        return;
    };

    let sender = sender.as_ref().expect("is set at picodata startup");

    let res = sender.send(ThreadSafeLogRequest {
        thread_name: std::thread::current().name().unwrap_or("<unknown>").into(),
        level,
        message,
    });
    if let Err(_) = res {
        // Nothing we can do if sending the message failed
    }
}

#[inline(always)]
pub fn thread_safe_logger_is_initialized() -> bool {
    if let Ok(logger) = THREAD_SAFE_LOGGING.lock() {
        logger.is_some()
    } else {
        // Somebody panicked while holding the mutex, just assume the
        // logger was initialized at some point
        true
    }
}

pub fn init_thread_safe_logger() {
    let (sender, receiver) = cbus::channel::<ThreadSafeLogRequest>(crate::cbus::ENDPOINT_NAME);

    // This fiber is responsible for transmitting log messages
    // from non-tx threads to the tx thread
    fiber::Builder::new()
        .name("thread_safe_logger")
        .func(move || {
            loop {
                match receiver.receive() {
                    Err(_) => {
                        tlog!(Error, "thread safe log sender was dropped");
                        break;
                    }
                    Ok(req) => {
                        fiber::set_name(&req.thread_name);

                        // NOTE: this looks incredibly stupid and it is, but
                        // there's no easy way to do this with the slog crate
                        // (none that I found anyway).
                        match req.level {
                            slog::Level::Critical => tlog!(Critical, "{}", req.message),
                            slog::Level::Error => tlog!(Error, "{}", req.message),
                            slog::Level::Warning => tlog!(Warning, "{}", req.message),
                            slog::Level::Info => tlog!(Info, "{}", req.message),
                            slog::Level::Debug => tlog!(Debug, "{}", req.message),
                            slog::Level::Trace => tlog!(Trace, "{}", req.message),
                        }
                    }
                }
            }
        })
        .start_non_joinable()
        .expect("starting a fiber shouldn't fail");

    *THREAD_SAFE_LOGGING
        .lock()
        .expect("nobody should've touched this yet") = Some(sender);
}

mod test {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[tarantool::test]
    fn thread_safe_logging() {
        crate::cbus::init_cbus_endpoint();
        init_thread_safe_logger();

        let random_numbers = [6, 9, 4, 7, 2, 1, 8, 3, 5, 6];

        tlog!(Info, "start logging from non-tx threads");
        thread::scope(|s| {
            let mut jhs = Vec::new();
            for i in 0..10 {
                let jh = thread::Builder::new()
                    .name(format!("test thread #{i}"))
                    .spawn_scoped(s, move || {
                        tlog!(Info, "test thread #{i} started");
                        thread::sleep(Duration::from_millis(random_numbers[i]));
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

        tlog!(Info, "done logging from non-tx threads");
    }
}
