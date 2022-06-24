use crate::set_error;
use crate::error::{TarantoolError, TarantoolErrorCode};
use crate::ffi::tarantool as ffi;

use nix::errno;

/// Set a callback to be called on Tarantool shutdown.
pub fn on_shutdown<F: FnOnce() + 'static>(cb: F) -> Result<(), TarantoolError> {
    let cb_ptr = Box::into_raw(Box::new(cb));
    if unsafe { ffi::box_on_shutdown(cb_ptr as _, Some(trampoline::<F>), None) } != 0 {
        if errno::from_i32(errno::errno()) == errno::Errno::EINVAL {
            set_error!(TarantoolErrorCode::IllegalParams, "invalid arguments to on_shutdown");
        }
        return Err(TarantoolError::last())
    }

    return Ok(());

    use libc::{c_int, c_void};
    extern "C" fn trampoline<F: FnOnce()>(data: *mut c_void) -> c_int {
        let cb = unsafe { Box::from_raw(data as *mut F) };
        cb();
        0
    }
}
