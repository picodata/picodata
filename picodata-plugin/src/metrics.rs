use crate::internal::ffi::pico_ffi_register_metrics_handler;
use crate::plugin::interface::PicoContext;
use crate::transport::rpc::server::PackedServiceIdentifier;
use crate::util::copy_to_region;
use crate::util::FfiSafeStr;
use std::mem::MaybeUninit;
use tarantool::error::BoxError;

/// Register a callback with stringified metrics representation to a global metrics collection.
/// This callback will be called at every metrics poll request (by request to a "/metrics" http endpoint).
pub fn register_metrics_handler(
    context: &PicoContext,
    callback: impl Fn() -> String,
) -> Result<(), BoxError> {
    let identifier = PackedServiceIdentifier::pack(
        "",
        context.plugin_name(),
        context.service_name(),
        context.plugin_version(),
    )?;

    let handler = FfiMetricsHandler::new(identifier, callback);
    let rc = unsafe { pico_ffi_register_metrics_handler(handler) };
    if rc != 0 {
        return Err(BoxError::last());
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// ffi wrappers
////////////////////////////////////////////////////////////////////////////////

type FfiMetricsCallback = extern "C-unwind" fn(
    handler: *const FfiMetricsHandler,
    output: *mut FfiSafeStr,
) -> std::ffi::c_int;

/// **For internal use**.
///
/// Use *TBA* instead.
#[repr(C)]
pub struct FfiMetricsHandler {
    /// The result data must either be statically allocated, or allocated using
    /// the fiber region allocator (see [`box_region_alloc`]).
    ///
    /// [`box_region_alloc`]: tarantool::ffi::tarantool::box_region_alloc
    callback: FfiMetricsCallback,
    drop: extern "C-unwind" fn(*mut FfiMetricsHandler),

    /// The pointer to the closure object.
    ///
    /// Note that the pointer must be `mut` because we will at some point drop the data pointed to by it.
    /// But when calling the closure, the `const` pointer should be used.
    closure_pointer: *mut (),

    /// This data is owned by this struct (freed on drop).
    pub identifier: PackedServiceIdentifier,
}

impl Drop for FfiMetricsHandler {
    #[inline(always)]
    fn drop(&mut self) {
        (self.drop)(self)
    }
}

impl FfiMetricsHandler {
    fn new<F>(identifier: PackedServiceIdentifier, f: F) -> Self
    where
        F: Fn() -> String,
    {
        let closure = Box::new(f);
        let closure_pointer: *mut F = Box::into_raw(closure);

        Self {
            callback: Self::trampoline::<F>,
            drop: Self::drop_handler::<F>,
            closure_pointer: closure_pointer.cast(),

            identifier,
        }
    }

    /// An ABI-safe wrapper which calls the rust closure stored in `handler`.
    ///
    /// The result of the closure is copied onto the fiber's region allocation
    /// and the pointer to that allocation is written into `output`.
    extern "C-unwind" fn trampoline<F>(
        handler: *const Self,
        output: *mut FfiSafeStr,
    ) -> std::ffi::c_int
    where
        F: Fn() -> String,
    {
        // This is safe. To verify see `register_rpc_handler` above.
        let closure_pointer: *const F = unsafe { (*handler).closure_pointer.cast::<F>() };
        let closure = unsafe { &*closure_pointer };

        let response = closure();
        // Copy the data returned by the closure onto the fiber's region allocation
        let region_slice = match copy_to_region(response.as_bytes()) {
            Ok(v) => v,
            Err(e) => {
                e.set_last();
                return -1;
            }
        };

        // Safe, because data was copied from a `String`, which is utf8
        let region_str = unsafe { FfiSafeStr::from_utf8_unchecked(region_slice) };
        // This is safe. To verify see `Self::call` bellow.
        unsafe { std::ptr::write(output, region_str) }

        0
    }

    extern "C-unwind" fn drop_handler<F>(handler: *mut Self) {
        unsafe {
            let closure_pointer: *mut F = (*handler).closure_pointer.cast::<F>();
            let closure = Box::from_raw(closure_pointer);
            drop(closure);

            if cfg!(debug_assertions) {
                // Overwrite the pointer with garbage so that we fail loudly is case of a bug
                (*handler).closure_pointer = 0xcccccccccccccccc_u64 as _;
            }

            (*handler).identifier.drop();
        }
    }

    #[inline(always)]
    pub fn identity(&self) -> usize {
        self.callback as *const FfiMetricsCallback as _
    }

    /// The result is stored on the current fiber's region allocator. The caller
    /// is responsible for calling `box_region_truncate` or equivalent.
    #[inline(always)]
    #[allow(clippy::result_unit_err)]
    pub fn call(&self) -> Result<&'static str, ()> {
        let mut output = MaybeUninit::uninit();

        let rc = (self.callback)(self, output.as_mut_ptr());
        if rc == -1 {
            // Actual error is passed through tarantool. Can't return BoxError
            // here, because tarantool-module version may be different in picodata.
            return Err(());
        }

        // Safe because the data is either 'static or region allocated.
        let result = unsafe { output.assume_init().as_str() };

        Ok(result)
    }
}
