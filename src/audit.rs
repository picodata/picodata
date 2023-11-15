use crate::tlog;
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

    extern "C" {
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
        ) -> ::core::ffi::c_int;
    }
}

/// A safe wrapper for tarantool's log object.
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

impl slog::Drain for Log {
    type Ok = ();
    type Err = slog::Never;

    fn log(
        &self,
        record: &slog::Record,
        values: &slog::OwnedKVList,
    ) -> Result<Self::Ok, Self::Err> {
        let level: SayLevel = tlog::MapLevel(record.level()).into();
        let file = CString::new(record.file()).expect("null byte in filename");
        let line = record.line() as core::ffi::c_int;

        // Format the message using tlog's capabilities.
        let msg = tlog::StrSerializer::format_message(record, values);
        let msg = CString::new(msg).unwrap();

        // SAFETY: All arguments' invariants have already been checked.
        unsafe {
            ffi::log_say(
                self.0,
                level,
                file.as_ptr(),
                line,
                std::ptr::null(),
                msg.as_ptr(),
            );
        }

        Ok(())
    }
}

// Note: we don't want to expose this implementation detail.
static ROOT: OnceCell<slog::Logger> = OnceCell::new();

/// A public log drain for the [`crate::audit!`] macro.
pub fn root() -> Option<&'static slog::Logger> {
    ROOT.get()
}

#[macro_export]
macro_rules! audit(
    ($lvl:ident, $($args:tt)+) => {
        if let Some(root) = $crate::audit::root() {
            // TODO: include the record id (comprised of `count,gen,raft_id`).
            // TODO: include the name of a subject (who performed the operation).
            slog::slog_log!(root, slog::Level::$lvl, "", $($args)*);
        }
    };
);

/// Initialize audit log.
/// Note: `config` will be parsed by tarantool's core (say.c).
pub fn init(config: &str) {
    let config = CString::new(config).expect("audit log config contains nul");
    let log = Log::new(config).expect("failed to create audit log");

    // Note: this'll only fail if the cell's already set (shouldn't be possible).
    ROOT.set(slog::Logger::root(log, slog::o!()))
        .expect("failed to initialize global audit drain");

    audit!(Info, "audit log is ready"; "title" => "init_audit");

    // Report a local startup event & register a trigger for a local shutdown event.
    // Those will only be seen in this exact node's audit log (hence "local").
    audit!(Info, "node is starting"; "title" => "local_startup");
    ::tarantool::trigger::on_shutdown(|| {
        audit!(Info, "node is shutting down"; "title" => "local_shutdown");
    })
    .expect("failed to install audit trigger for node shutdown");
}
