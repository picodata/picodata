use crate::traft::{LogicalClock, RaftSpaceAccess};
use once_cell::sync::OnceCell;
use std::ffi::{CStr, CString};
use tarantool::{error::TarantoolError, log::SayLevel};

/// Tarantool's low-level APIs.
/// At some point we should probably move those to tarantool-module,
/// but right now it's much more convenient to keep them here.
mod ffi {
    use super::*;

    /// An opaque log structure for type safety.
    /// The real struct has quite a few platform-dependent fields,
    /// so any attempt to re-define it in rust is likely to be impractical.
    #[repr(C)]
    #[derive(Debug, Copy, Clone)]
    pub struct Log {
        _unused: [u8; 0],
    }

    // TODO: use a definition from Tarolog once it's ready.
    pub type LogFormatFn = unsafe extern "C" fn(
        log: *const core::ffi::c_void,
        buf: *mut core::ffi::c_char,
        len: core::ffi::c_int,
        level: core::ffi::c_int,
        module: *const core::ffi::c_char,
        filename: *const core::ffi::c_char,
        line: core::ffi::c_int,
        error: *const core::ffi::c_char,
        format: *const core::ffi::c_char,
        ap: va_list::VaList,
    ) -> core::ffi::c_int;

    extern "C" {
        pub fn vsnprintf(
            s: *mut core::ffi::c_char,
            n: usize,
            format: *const core::ffi::c_char,
            ap: va_list::VaList,
        ) -> core::ffi::c_int;

        /// Allocate a new log object.
        /// Returns a pointer to the object or `NULL` if allocation failed.
        pub fn log_new() -> *mut Log;

        /// Initialize the log object using `init_str` and `nonblock`.
        /// Returns `0` on success, `-1` on system error; caller is
        /// responsible for extracting the error from diagnostics area.
        pub fn log_create(
            log: *mut Log,
            init_str: *const core::ffi::c_char,
            nonblock: core::ffi::c_int,
        ) -> core::ffi::c_int;

        /// Deinitialize the log object.
        /// NOTE: this does not reclaim the underlying memory.
        pub fn log_destroy(log: *mut Log);

        /// Set log format callback.
        /// TODO: use a definition from Tarolog once it's ready.
        pub fn log_set_format(log: *mut Log, format_func: LogFormatFn);

        /// Emit a new log entry.
        /// This function uses `printf`-ish calling convention.
        pub fn log_say(
            log: *mut Log,
            level: SayLevel,
            filename: *const core::ffi::c_char,
            line: core::ffi::c_int,
            error: *const core::ffi::c_char,
            format: *const core::ffi::c_char,
            ...
        ) -> core::ffi::c_int;
    }
}

/// A safe wrapper for tarantool's log object.
#[derive(Debug)]
pub struct Log(*mut ffi::Log);

// SAFETY: tarantool's logger should be thread-safe.
unsafe impl Sync for Log {}
unsafe impl Send for Log {}

impl Log {
    /// Create a new log object using `box.cfg`'s log option.
    fn new(params: impl AsRef<CStr>) -> Result<Self, TarantoolError> {
        // SAFETY: this call just allocates space for the object.
        let log = unsafe { ffi::log_new() };
        assert_ne!(log, std::ptr::null_mut(), "failed to allocate log");

        let params = params.as_ref().as_ptr();
        // SAFETY: arguments' invariants have already been checked.
        let res = unsafe { ffi::log_create(log, params, 0) };
        if res != 0 {
            return Err(TarantoolError::last());
        }

        // SAFETY: this call is safe as long as the log object is
        // initialized (per above) and our format callback works well.
        unsafe { ffi::log_set_format(log, say_format_audit) };

        Ok(Self(log))
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        // SAFETY: we own this object, so now we can drop it.
        unsafe {
            ffi::log_destroy(self.0);
            libc::free(self.0 as _);
        }
    }
}

/// A helper for serializing slog's record to json.
struct AuditSerializer {
    map: serde_json::Map<String, serde_json::Value>,
    clock: LogicalClock,
}

impl slog::Serializer for AuditSerializer {
    fn emit_arguments(&mut self, key: slog::Key, val: &std::fmt::Arguments) -> slog::Result {
        // TODO: optimize excessive string allocations here and below.
        // TODO: make sure we're not trying to overwrite a value here (via assert).
        self.map.insert(key.to_string(), val.to_string().into());
        Ok(())
    }
}

impl AuditSerializer {
    fn new(clock: LogicalClock) -> Self {
        Self {
            map: serde_json::Map::new(),
            clock,
        }
    }

    fn into_string(self) -> String {
        serde_json::Value::from(self.map).to_string()
    }

    fn serialize_any(mut self, msg: impl std::fmt::Display) -> Self {
        // TODO: include the name of a subject (who performed the operation).
        let id = self.clock.to_string();
        self.map.insert("id".into(), id.into());
        let time = chrono::Local::now().format("%FT%H:%M:%S%.3f%z").to_string();
        self.map.insert("time".into(), time.into());
        let message = msg.to_string();
        self.map.insert("message".into(), message.into());

        self
    }

    fn serialize_slog(mut self, record: &slog::Record, values: &slog::OwnedKVList) -> Self {
        self = self.serialize_any(record.msg());

        use slog::KV;
        // It's safe to use .unwrap() here since
        // AuditSerializer doesn't return anything but Ok()
        record.kv().serialize(record, &mut self).unwrap();
        values.serialize(record, &mut self).unwrap();

        self
    }
}

/// Special fmt string to let [`say_format_audit`] know that
/// the caller has already applied json formatting to inputs.
const AUDIT_FMT_MAGIC: &std::ffi::CStr = unsafe { tarantool::c_str!("json") };

// We don't need certain fields (e.g. fiber name) in audit log entries,
// so we have to implement the format logic ourselves.
//
// NOTE: We can't assume this function will only be called as a
// result of some action in our rust codebase; in fact, core
// tarantool may call it based on its own considerations
// (e.g. for a SIGHUP rotation event).
//
// NOTE: Panics in this function are highly undesirable.
extern "C" fn say_format_audit(
    _log: *const core::ffi::c_void,
    buf: *mut core::ffi::c_char,
    len: core::ffi::c_int,
    _level: core::ffi::c_int,
    _module: *const core::ffi::c_char,
    _filename: *const core::ffi::c_char,
    _line: core::ffi::c_int,
    _error: *const core::ffi::c_char,
    format: *const core::ffi::c_char,
    mut ap: va_list::VaList,
) -> core::ffi::c_int {
    use std::borrow::Cow;

    // SAFETY: caller is responsible for providing valid `format`.
    let format = unsafe { std::ffi::CStr::from_ptr(format as _) };

    let message = if format == AUDIT_FMT_MAGIC {
        // SAFETY: see the Drain impl below.
        let data = unsafe {
            let ptr = ap.get::<*const u8>();
            let len = ap.get::<usize>();
            std::slice::from_raw_parts(ptr, len)
        };

        Cow::Borrowed(data)
    } else {
        let mut scratch = [0u8; 1024];
        // SAFETY: caller is responsible for all args.
        let count = unsafe {
            ffi::vsnprintf(
                scratch.as_mut_ptr().cast(),
                scratch.len(),
                format.as_ptr(),
                ap,
            )
        };

        // Should be no greater than array's size and no less than zero.
        let count = count.clamp(0, scratch.len() as i32) as usize;

        // SAFETY: I'm 95% positive it will be valid utf8...
        let str = unsafe { std::str::from_utf8_unchecked(&scratch[..count]) };
        let clock = next_unique_id().expect("failed to generate audit entry id");
        let data = AuditSerializer::new(clock).serialize_any(str).into_string();

        Cow::Owned(data.into_bytes())
    };

    // SAFETY: caller is responsible for providing valid `buf` & `len`.
    let mut buffer = unsafe {
        let ptr = buf as *mut u8;
        let len = len.max(0) as usize;
        std::slice::from_raw_parts_mut(ptr, len)
    };

    use std::io::Write;
    let mut count = buffer.write(&message).unwrap_or(0);
    count += buffer.write(b"\n").unwrap_or(0);
    count as core::ffi::c_int
}

impl slog::Drain for Log {
    type Ok = ();
    type Err = slog::Never;

    fn log(
        &self,
        record: &slog::Record,
        values: &slog::OwnedKVList,
    ) -> Result<Self::Ok, Self::Err> {
        let clock = next_unique_id().expect("failed to generate audit entry id");
        let msg = AuditSerializer::new(clock)
            .serialize_slog(record, values)
            .into_string();

        // SAFETY: All arguments' invariants have already been checked.
        // Only the last two arguments will be used by the fmt callback.
        unsafe {
            ffi::log_say(
                self.0,
                SayLevel::Info,
                std::ptr::null(),
                0,
                std::ptr::null(),
                // We use (almost) the same calling convention as `say_format_json`.
                // Core tarantool might write non-json payloads to our log during
                // e.g. log rotation (see `log_rotate`), so we have to adapt.
                AUDIT_FMT_MAGIC.as_ptr(),
                // `say_format_audit` will make use of both
                // a pointer to the message and its size.
                msg.as_ptr(),
                msg.len(),
            );
        }

        Ok(())
    }
}

// Note: we don't want to expose these implementation details.
static ROOT: OnceCell<slog::Logger> = OnceCell::new();
static mut CLOCK: OnceCell<LogicalClock> = OnceCell::new();

/// Generate next unique record id.
fn next_unique_id() -> Option<LogicalClock> {
    // SAFETY: we'll call this only from TX thread.
    let clock = unsafe { CLOCK.get_mut()? };
    clock.inc();
    Some(*clock)
}

/// A public log drain for the [`crate::audit!`] macro.
pub fn root() -> Option<&'static slog::Logger> {
    ROOT.get()
}

tarantool::define_str_enum! {
    /// Type-safe entry severity for use in [`crate::audit!`].
    /// Severity levels and their usage are defined in the RFC.
    pub enum Severity {
        Low = "low",
        Medium = "medium",
        High = "high",
    }
}

// A helper macro which rewrites audit field syntax to the one used by slog.
#[doc(hidden)]
#[macro_export]
macro_rules! audit_kv(
    // Format using Display. Example: `key: %value`.
    ($key:ident : %$value:expr, $($rest:tt)*) => {
        (slog::slog_kv!(stringify!($key) => %$value), $crate::audit_kv!($($rest)*))
    };
    // Format using Debug. Example: `key: ?value`.
    ($key:ident : ?$value:expr, $($rest:tt)*) => {
        (slog::slog_kv!(stringify!($key) => ?$value), $crate::audit_kv!($($rest)*))
    };
    // Substitute as is. Example: `key: value`.
    ($key:ident : $value:expr, $($rest:tt)*) => {
        (slog::slog_kv!(stringify!($key) => $value), $crate::audit_kv!($($rest)*))
    };
    () => { () };
);

/// This is the main API for adding new entries to the audit log.
/// The required fields are `message`, `title` and `severity`,
/// the rest is up to the caller. Auxiliary values may be prefixed
/// with `%` or `?` to format them using `Display` or `Debug`.
///
/// Example:
/// ```
/// # use picodata::audit;
/// audit!(
///     message: "hello, world!",
///     title: "greeting",
///     severity: Low,
/// );
/// ```
#[macro_export(local_inner_macros)]
macro_rules! audit(
    (
        message: $message:expr,
        title: $title:expr,
        severity: $severity:ident,
        $($aux_fields:tt)*
    ) => {
        if let Some(root) = $crate::audit::root() {
            slog::slog_log!(
                // Boilerplate required by slog.
                root, slog::Level::Info, "",
                // The message itself.
                $message;
                // Additional fields.
                audit_kv!(
                    title: $title,
                    severity: $crate::audit::Severity::$severity.as_str(),
                    $($aux_fields)*
                )
            );
        }
    };
);

/// Initialize audit log.
/// Unique id generation depends on the raft machine's state.
/// Note: `config` will be parsed by tarantool's core (see `say.c`).
pub fn init(config: &str, raft_storage: &RaftSpaceAccess) {
    // Raft-related stuff should be ready at this point.
    let raft_id = raft_storage
        .raft_id()
        .expect("failed to get raft_id for audit log")
        .expect("found zero raft_id during audit log init");
    let gen = raft_storage.gen().expect("failed to get gen for audit log");

    // Note: this'll only fail if the cell's already set (shouldn't be possible).
    // SAFETY: this is the first time we access this variable, and it's
    // always done from the main (TX) thread.
    unsafe {
        CLOCK
            .set(LogicalClock::new(raft_id, gen))
            .expect("failed to initialize global audit event id generator");
    }

    let config = CString::new(config).expect("audit log config contains nul");
    let log = Log::new(config).expect("failed to create audit log");

    // Note: this'll only fail if the cell's already set (shouldn't be possible).
    ROOT.set(slog::Logger::root(log, slog::o!()))
        .expect("failed to initialize global audit drain");

    crate::audit!(
        message: "audit log is ready",
        title: "init_audit",
        severity: Low,
    );

    // Report a local startup event & register a trigger for a local shutdown event.
    // Those will only be seen in this exact instance's audit log (hence "local").
    crate::audit!(
        message: "instance is starting",
        title: "local_startup",
        severity: Low,
    );
    tarantool::trigger::on_shutdown(|| {
        crate::audit!(
            message: "instance is shutting down",
            title: "local_shutdown",
            severity: High,
        );
    })
    .expect("failed to install audit trigger for instance shutdown");
}
