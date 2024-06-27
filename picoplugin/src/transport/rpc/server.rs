use crate::internal::ffi;
use crate::plugin::interface::PicoContext;
use crate::transport::context::Context;
use crate::transport::context::FfiSafeContext;
use crate::util::copy_to_region;
use crate::util::FfiSafeBytes;
use crate::util::FfiSafeStr;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;

////////////////////////////////////////////////////////////////////////////////
// RouteBuilder
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct RouteBuilder<'a> {
    plugin: &'a str,
    service: &'a str,
    version: &'a str,
    path: Option<&'a str>,
}

impl<'a> RouteBuilder<'a> {
    #[inline(always)]
    pub fn from_pico_context(context: &'a PicoContext) -> Self {
        Self {
            plugin: context.plugin_name(),
            service: context.service_name(),
            version: context.plugin_version(),
            path: None,
        }
    }

    #[inline(always)]
    pub fn from_service_info(plugin: &'a str, service: &'a str, version: &'a str) -> Self {
        Self {
            plugin,
            service,
            version,
            path: None,
        }
    }

    #[inline]
    pub fn path(mut self, path: &'a str) -> Self {
        if let Some(old) = self.path.take() {
            #[rustfmt::skip]
            tarantool::say_warn!("RouteBuilder path is silently changed from {old:?} to {path:?}");
        }
        self.path = Some(path);
        self
    }

    pub fn register_raw<F>(self, f: F) -> Result<(), BoxError>
    where
        F: FnMut(&[u8], &mut Context) -> Result<&'static [u8], BoxError> + 'static,
    {
        let Some(path) = self.path else {
            #[rustfmt::skip]
            return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "path must be specified for RPC endpoint"));
        };

        let identifier = FfiRpcRouteIdentifier {
            path: path.into(),
            plugin: self.plugin.into(),
            service: self.service.into(),
            version: self.version.into(),
        };
        register_rpc_handler(&identifier, f)
    }
}

impl<'a> From<&'a PicoContext> for RouteBuilder<'a> {
    #[inline(always)]
    fn from(context: &'a PicoContext) -> Self {
        Self::from_pico_context(context)
    }
}

////////////////////////////////////////////////////////////////////////////////
// ffi wrappers
////////////////////////////////////////////////////////////////////////////////

/// **For internal use**.
///
/// Use [`RouteBuilder`] instead.
#[derive(Debug, Default, Clone, Copy)]
pub struct FfiRpcRouteIdentifier {
    pub path: FfiSafeStr,
    pub plugin: FfiSafeStr,
    pub service: FfiSafeStr,
    pub version: FfiSafeStr,
}

/// **For internal use**.
#[inline]
fn register_rpc_handler<F>(identifier: &FfiRpcRouteIdentifier, f: F) -> Result<(), BoxError>
where
    F: FnMut(&[u8], &mut Context) -> Result<&'static [u8], BoxError> + 'static,
{
    let handler = FfiRpcHandler::new(identifier, f);

    // This is safe.
    let rc = unsafe { ffi::pico_ffi_register_rpc_handler(handler) };
    if rc == -1 {
        return Err(BoxError::last());
    }

    return Ok(());
}

/// **For internal use**.
///
/// Use [`RouteBuilder`] instead.
#[repr(C)]
pub struct FfiRpcHandler {
    /// The result data must either be statically allocated, or allocated using
    /// the fiber region allocator (see [`box_region_alloc`]).
    ///
    /// [`box_region_alloc`]: tarantool::ffi::tarantool::box_region_alloc
    callback: extern "C" fn(
        handler: *const FfiRpcHandler,
        input: FfiSafeBytes,
        context: *const FfiSafeContext,
        output: *mut FfiSafeBytes,
    ) -> std::ffi::c_int,
    drop: extern "C" fn(*mut FfiRpcHandler),

    closure_pointer: *mut (),

    /// Points into [`Self::string_storage`].
    path: FfiSafeStr,
    /// Points into [`Self::string_storage`].
    plugin_name: FfiSafeStr,
    /// Points into [`Self::string_storage`].
    service_name: FfiSafeStr,
    /// Points into [`Self::string_storage`].
    plugin_version: FfiSafeStr,
    /// Points into [`Self::string_storage`].
    route_repr: FfiSafeStr,

    /// This data is owned by this struct (freed on drop).
    /// This slice stores all of the strings above, so that when it's needed to
    /// be dropped we only need to free one slice.
    string_storage: FfiSafeBytes,
}

impl Drop for FfiRpcHandler {
    #[inline(always)]
    fn drop(&mut self) {
        (self.drop)(self)
    }
}

impl FfiRpcHandler {
    fn new<F>(identifier: &FfiRpcRouteIdentifier, f: F) -> Self
    where
        F: FnMut(&[u8], &mut Context) -> Result<&'static [u8], BoxError> + 'static,
    {
        let closure = Box::new(f);
        let closure_pointer: *mut F = Box::into_raw(closure);

        //
        // Store the strings in a contiguous slice of memory and set the pointers appropriately
        //
        let total_string_len = identifier.plugin.len()
            // For an extra '.' between plugin and service names
            + 1
            + identifier.service.len()
            + identifier.path.len()
            + identifier.version.len();
        let mut string_storage = Vec::with_capacity(total_string_len);
        let start = string_storage.as_mut_ptr();

        let mut p = start;
        let mut push_and_get_slice = |s: FfiSafeStr| unsafe {
            string_storage.extend_from_slice(s.as_bytes());
            let res = FfiSafeStr::from_raw_parts(NonNull::new_unchecked(p), s.len());
            p = p.add(s.len());
            res
        };
        let plugin_name = push_and_get_slice(identifier.plugin);
        push_and_get_slice(".".into());
        let service_name = push_and_get_slice(identifier.service);
        let path = push_and_get_slice(identifier.path);
        let plugin_version = push_and_get_slice(identifier.version);
        let route_repr = unsafe {
            FfiSafeStr::from_raw_parts(
                NonNull::new_unchecked(start),
                total_string_len - plugin_version.len(),
            )
        };

        debug_assert_eq!(
            start,
            string_storage.as_mut_ptr(),
            "vec must not have been reallocated, because we store pointers into it"
        );
        let capacity = string_storage.capacity();

        // Self now ownes this data and will be freeing it in it's `drop`.
        std::mem::forget(string_storage);

        let string_storage = unsafe { std::slice::from_raw_parts(start, capacity) };

        FfiRpcHandler {
            callback: Self::trampoline::<F>,
            drop: Self::drop_handler::<F>,
            closure_pointer: closure_pointer.cast(),

            path,
            plugin_name,
            service_name,
            plugin_version,
            route_repr,
            string_storage: string_storage.into(),
        }
    }

    extern "C" fn trampoline<F>(
        handler: *const FfiRpcHandler,
        input: FfiSafeBytes,
        context: *const FfiSafeContext,
        output: *mut FfiSafeBytes,
    ) -> std::ffi::c_int
    where
        F: FnMut(&[u8], &mut Context) -> Result<&'static [u8], BoxError> + 'static,
    {
        // This is safe. To verify see `register_rpc_handler` above.
        let closure_pointer: *mut F = unsafe { (*handler).closure_pointer.cast::<F>() };
        let mut closure = unsafe { Box::from_raw(closure_pointer) };
        let input = unsafe { input.as_bytes() };
        let context = unsafe { &*context };
        let mut context = Context::new(context);

        let result = (|| {
            let data = closure(input, &mut context)?;
            copy_to_region(&data)
        })();

        match result {
            Ok(region_slice) => {
                // This is safe. To verify see `FfiRpcHandler::call` bellow.
                unsafe { std::ptr::write(output, region_slice.into()) }

                return 0;
            }
            Err(e) => {
                e.set_last();
                return -1;
            }
        }
    }

    extern "C" fn drop_handler<F>(handler: *mut FfiRpcHandler) {
        unsafe {
            let closure_pointer: *mut F = (*handler).closure_pointer.cast::<F>();
            let closure = Box::from_raw(closure_pointer);
            drop(closure);

            let (pointer, capacity) = (*handler).string_storage.into_raw_parts();
            // Note: we pretend the original Vec was filled to capacity which
            // may or may not be true, there might have been some unitialized
            // data at the end. But this doesn't matter in this case because we
            // just want to drop the data, and only the capacity matters.
            let string_storage = Vec::from_raw_parts(pointer, capacity, capacity);
            drop(string_storage);
        }
    }

    #[inline(always)]
    pub fn call(&self, input: &[u8], context: &FfiSafeContext) -> Result<&'static [u8], ()> {
        let mut output = MaybeUninit::uninit();

        let rc = (self.callback)(self, input.into(), context, output.as_mut_ptr());
        if rc == -1 {
            // Actual error is passed through tarantool. Can't return BoxError
            // here, because tarantool-module version may be different in picodata.
            return Err(());
        }

        // This is safe. To verify see `trampoline` above.
        let result = unsafe { output.assume_init().as_bytes() };

        Ok(result)
    }

    #[inline(always)]
    pub fn path(&self) -> &str {
        // SAFETY: data is alive for the lifetime of `&self`, and borrow checker does it's thing
        unsafe { self.path.as_str() }
    }

    #[inline(always)]
    pub fn plugin(&self) -> &str {
        // SAFETY: data is alive for the lifetime of `&self`, and borrow checker does it's thing
        unsafe { self.plugin_name.as_str() }
    }

    #[inline(always)]
    pub fn service(&self) -> &str {
        // SAFETY: data is alive for the lifetime of `&self`, and borrow checker does it's thing
        unsafe { self.service_name.as_str() }
    }

    #[inline(always)]
    pub fn route_repr(&self) -> &str {
        // SAFETY: data is alive for the lifetime of `&self`, and borrow checker does it's thing
        unsafe { self.route_repr.as_str() }
    }

    #[inline(always)]
    pub fn version(&self) -> &str {
        // SAFETY: data is alive for the lifetime of `&self`, and borrow checker does it's thing
        unsafe { self.plugin_version.as_str() }
    }
}
