//! A collection of tarantool stored procedures used in picodata.
//!

use ::tarantool::tuple::FunctionArgs;
use ::tarantool::tuple::FunctionCtx;
use std::os::raw::c_int;

// TODO: move this into [`tarantool::ffi::tarantool`]
/// Tarantool stored procedure signature.
pub type TarantoolProc = unsafe extern "C" fn(FunctionCtx, FunctionArgs) -> c_int;

/// All stored procedures collected by
/// [`collect_proc!`](crate::collect_proc) macro.
pub struct AllProcs;

#[doc(hidden)]
#[::linkme::distributed_slice]
pub static ALL_PROCS: [(&'static str, TarantoolProc)] = [..];

impl AllProcs {
    /// Returns an iterator over all procs names.
    pub(crate) fn names() -> impl Iterator<Item = &'static str> {
        ALL_PROCS.iter().map(|v| v.0)
    }
}

#[macro_export]
/// Collects a function to be used as a tarantool stored procedure.
///
/// The objective of this macro is to simplify creating stored
/// procedures (`box.schema.func.create` invoked during the instance
/// initialization). And, since those functions aren't invoked directly,
/// to prevent symbols removal due to the link-time optimization.
///
/// For retrieving collected items refer to [`AllProcs`].
///
macro_rules! collect_proc {
    ($proc:ident) => {
        impl $crate::proc::AllProcs {
            #[allow(dead_code)]
            #[doc(hidden)]
            fn $proc() {
                // Enclosing static variable in a function allows to avoid name clash.
                #[::linkme::distributed_slice($crate::proc::ALL_PROCS)]
                pub static PROC: (&str, $crate::proc::TarantoolProc) =
                    ($crate::stringify_cfunc!($proc), $proc);
            }
        }
    };
}
