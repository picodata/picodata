use crate::audit::entry::AuditEntry;
use crate::traft::LogicalClock;
use abi_stable::std_types::RStr;
use std::ffi::{CStr, CString};
use std::sync::OnceLock;
use tarantool::{error::TarantoolError, log::SayLevel};

pub mod policy;

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

/// A safe wrapper for tarantool's log object configured to write to the audit log.
#[derive(Debug)]
struct TarantoolAuditLog(*mut ffi::Log);

// SAFETY: tarantool's logger should be thread-safe.
unsafe impl Sync for TarantoolAuditLog {}
unsafe impl Send for TarantoolAuditLog {}

impl TarantoolAuditLog {
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

    fn say_json(&self, json: &[u8]) {
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
                json.as_ptr(),
                json.len(),
            );
        }
    }
}

impl Drop for TarantoolAuditLog {
    fn drop(&mut self) {
        // SAFETY: we own this object, so now we can drop it.
        unsafe {
            ffi::log_destroy(self.0);
            libc::free(self.0 as _);
        }
    }
}

mod entry;

#[derive(Debug)]
struct AuditLogger {
    clock: LogicalClock,
    log: TarantoolAuditLog,
}

impl AuditLogger {
    pub fn new(config: &str, raft_id: u64, raft_gen: u64) -> Self {
        let clock = LogicalClock::new(raft_id, raft_gen);

        let config = CString::new(config).expect("audit log config contains nul");
        let log = TarantoolAuditLog::new(config).expect("failed to create audit log");

        Self { clock, log }
    }

    pub fn record(&self, message: &str, keys: &[RStr<'_>], values: &[RStr<'_>]) {
        let id = self.clock.inc();
        let time = chrono::Local::now();

        let entry = AuditEntry::new(id, time, message, keys, values);
        let kv_str = serde_json::to_vec(&entry).expect("failed to serialize audit log");

        self.log.say_json(&kv_str);
    }
}

/// Special fmt string to let [`say_format_audit`] know that
/// the caller has already applied json formatting to inputs.
const AUDIT_FMT_MAGIC: &CStr = c"json";

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
        // This is a fallback branch for cases where tarantool has called our logger directly.
        // Regular entries emitted by picodata or plugins should be handled by the condition arm above.

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
        let message = unsafe { std::str::from_utf8_unchecked(&scratch[..count]) };
        let id = LOGGER
            .get()
            .expect("say_format_audit called while LOGGER has not been initialized")
            .clock
            .inc();
        let time = chrono::Local::now();

        // Unfortunately we have to match by string there because this message is emitted on tarantool side
        // in say.c::log_rotate without any additional arguments we'd like to have in resulting audit log entry
        let (keys, values) = if message == "log file has been reopened" {
            static KEYS: &[RStr<'static>] = &[RStr::from_str("title"), RStr::from_str("severity")];
            static VALUES: &[RStr<'static>] = &[
                RStr::from_str("audit_rotate"),
                RStr::from_str(picodata_plugin::audit::Severity::Low.as_str()),
            ];

            (KEYS, VALUES)
        } else {
            ([].as_slice(), [].as_slice())
        };

        let entry = AuditEntry::new(id, time, message, keys, values);
        let data = serde_json::to_vec(&entry).expect("failed to serialize audit log");

        Cow::Owned(data)
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

static LOGGER: OnceLock<AuditLogger> = OnceLock::new();

/// Check if audit is enabled.
///
/// Will return `false` before [`crate::audit::init`] is called,
/// even if audit is configured on this instance.
pub fn is_enabled() -> bool {
    LOGGER.get().is_some()
}

/// Actually write a record to an audit log.
///
/// While writing to audit log using this function will work, you should use the higher-level macro
/// [`crate::audit!`] instead.
pub fn record_impl(message: &str, keys: &[RStr<'_>], values: &[RStr<'_>]) -> bool {
    match LOGGER.get() {
        Some(logger) => {
            logger.record(message, keys, values);
            true
        }
        None => false,
    }
}

/// Initialize audit log.
/// NOTE: unique id generation depends on the raft machine's
/// state, and `config` will be parsed by tarantool's core (see `say.c`).
/// WARNING: this will panic if the audit was already configured (shouldn't be possible, though).
pub fn init(config: &str, raft_id: u64, raft_gen: u64) {
    // Note: this'll only fail if the cell's already set (shouldn't be possible).
    LOGGER
        .set(AuditLogger::new(config, raft_id, raft_gen))
        .expect("failed to initialize global audit logger");

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

/// Like [`picodata_plugin::audit!`], but always uses `picodata` as subsystem and skips the FFI,
/// calling picodata functions directly.
#[macro_export]
macro_rules! audit {
    (
        message: $message:expr,
        title: $title:expr,
        severity: $severity:ident,
        $($aux_fields:tt)*
    ) => {
        picodata_plugin::audit! {
            // skip FFI
            @with_functions {
                $crate::audit::is_enabled,
                $crate::audit::record_impl
            }
            {
                subsystem: "picodata",
                message: $message,
                title: $title,
                severity: $severity,
                $($aux_fields)*
            }
        }
    };
}
