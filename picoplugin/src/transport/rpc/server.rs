use super::Request;
use super::Response;
use crate::internal::ffi;
use crate::plugin::interface::PicoContext;
use crate::transport::context::Context;
use crate::transport::context::FfiSafeContext;
use crate::util::FfiSafeBytes;
use crate::util::FfiSafeStr;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;

////////////////////////////////////////////////////////////////////////////////
// RouteBuilder
////////////////////////////////////////////////////////////////////////////////

/// A helper struct for declaring RPC endpoints.
#[derive(Debug, Clone)]
pub struct RouteBuilder<'a> {
    plugin: &'a str,
    service: &'a str,
    version: &'a str,
    path: Option<&'a str>,
}

impl<'a> RouteBuilder<'a> {
    /// A route is required to contain information about the service from which
    /// it is being registered. Currently it's only possible to automatically
    /// extract this information from [`PicoContext`].
    #[inline(always)]
    pub fn from_pico_context(context: &'a PicoContext) -> Self {
        Self {
            plugin: context.plugin_name(),
            service: context.service_name(),
            version: context.plugin_version(),
            path: None,
        }
    }

    /// A route is required to contain information about the service from which
    /// it is being registered. Use this method to specify this info explicitly.
    /// Consider using [`RouteBuilder::from_pico_context`] instead.
    ///
    /// # Safety
    /// The caller must make sure that the info is the actual service info of
    /// the currently running service.
    #[inline(always)]
    pub unsafe fn from_service_info(plugin: &'a str, service: &'a str, version: &'a str) -> Self {
        Self {
            plugin,
            service,
            version,
            path: None,
        }
    }

    /// Specify a route path. The path must start with a `'/'` character.
    #[inline]
    pub fn path(mut self, path: &'a str) -> Self {
        if let Some(old) = self.path.take() {
            #[rustfmt::skip]
            tarantool::say_warn!("RouteBuilder path is silently changed from {old:?} to {path:?}");
        }
        self.path = Some(path);
        self
    }

    /// Register the RPC endpoint with the currently chosen parameters and the
    /// provided handler.
    ///
    /// Note that `f` must implement `Fn`. This is required by rust's semantics
    /// to allow the RPC handlers to yield. If a handler yields then another
    /// concurrent RPC request may result in the same handler being executed,
    /// so we must not hold any `&mut` references in those closures (other than
    /// ones allowed by rust semantics, see official reference on undefined
    /// behaviour [here]).
    ///
    /// [here]: <https://doc.rust-lang.org/reference/behavior-considered-undefined.html>
    #[track_caller]
    pub fn register<F>(self, f: F) -> Result<(), BoxError>
    where
        F: Fn(Request<'_>, &mut Context) -> Result<Response, BoxError> + 'static,
    {
        let Some(path) = self.path else {
            #[rustfmt::skip]
            return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "path must be specified for RPC endpoint"));
        };

        let identifier = PackedServiceIdentifier::pack(
            path.into(),
            self.plugin.into(),
            self.service.into(),
            self.version.into(),
        )?;
        let handler = FfiRpcHandler::new(identifier, f);
        if let Err(e) = register_rpc_handler(handler) {
            // Note: recreating the error to capture the caller's source location
            #[rustfmt::skip]
            return Err(BoxError::new(e.error_code(), e.message()));
        }

        Ok(())
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
#[inline]
fn register_rpc_handler(handler: FfiRpcHandler) -> Result<(), BoxError> {
    // This is safe.
    let rc = unsafe { ffi::pico_ffi_register_rpc_handler(handler) };
    if rc == -1 {
        return Err(BoxError::last());
    }

    return Ok(());
}

type RpcHandlerCallback = extern "C" fn(
    handler: *const FfiRpcHandler,
    input: FfiSafeBytes,
    context: *const FfiSafeContext,
    output: *mut FfiSafeBytes,
) -> std::ffi::c_int;

/// **For internal use**.
///
/// Use [`RouteBuilder`] instead.
#[repr(C)]
pub struct FfiRpcHandler {
    /// The result data must either be statically allocated, or allocated using
    /// the fiber region allocator (see [`box_region_alloc`]).
    ///
    /// [`box_region_alloc`]: tarantool::ffi::tarantool::box_region_alloc
    callback: RpcHandlerCallback,
    drop: extern "C" fn(*mut FfiRpcHandler),

    /// The pointer to the closure object.
    ///
    /// Note that the pointer must be `mut` because we will at some point drop the data pointed to by it.
    /// But when calling the closure, the `const` pointer should be used.
    closure_pointer: *mut (),

    /// This data is owned by this struct (freed on drop).
    pub identifier: PackedServiceIdentifier,
}

impl Drop for FfiRpcHandler {
    #[inline(always)]
    fn drop(&mut self) {
        (self.drop)(self)
    }
}

impl FfiRpcHandler {
    fn new<F>(identifier: PackedServiceIdentifier, f: F) -> Self
    where
        F: Fn(Request<'_>, &mut Context) -> Result<Response, BoxError> + 'static,
    {
        let closure = Box::new(f);
        let closure_pointer: *mut F = Box::into_raw(closure);

        FfiRpcHandler {
            callback: Self::trampoline::<F>,
            drop: Self::drop_handler::<F>,
            closure_pointer: closure_pointer.cast(),

            identifier,
        }
    }

    extern "C" fn trampoline<F>(
        handler: *const FfiRpcHandler,
        input: FfiSafeBytes,
        context: *const FfiSafeContext,
        output: *mut FfiSafeBytes,
    ) -> std::ffi::c_int
    where
        F: Fn(Request<'_>, &mut Context) -> Result<Response, BoxError> + 'static,
    {
        // This is safe. To verify see `register_rpc_handler` above.
        let closure_pointer: *const F = unsafe { (*handler).closure_pointer.cast::<F>() };
        let closure = unsafe { &*closure_pointer };
        let input = unsafe { input.as_bytes() };
        let context = unsafe { &*context };
        let mut context = Context::new(context);

        let request = Request::from_bytes(input);
        let result = (|| {
            let response = closure(request, &mut context)?;
            response.to_region_slice()
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

            if cfg!(debug_assertions) {
                // Overwrite the pointer with garbage so that we fail loudly is case of a bug
                (*handler).closure_pointer = 0xcccccccccccccccc_u64 as _;
            }

            (*handler).identifier.drop();
        }
    }

    #[inline(always)]
    pub fn identity(&self) -> usize {
        self.callback as *const RpcHandlerCallback as _
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
}

/// **For internal use**.
///
/// Use [`RouteBuilder`] instead.
///
/// This struct stores an RPC route identifier in the following packed format:
/// `{plugin}.{service}{path}{version}`. This format allows for efficient
/// extraction of the RPC route identifier for purposes of logging (note that
/// version is not displayed), and also losslessly stores info about all the
/// parts of the idenifier.
///
/// This represnetation also adds a constraint on the maximum length of the
/// plugin name, service name and path, each one of them must not be longer than
/// 65535 bytes (which is obviously engough for anybody).
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct PackedServiceIdentifier {
    pub storage: FfiSafeStr,
    pub plugin_len: u16,
    pub service_len: u16,
    pub path_len: u16,
    pub version_len: u16,
}

impl PackedServiceIdentifier {
    fn pack(path: &str, plugin: &str, service: &str, version: &str) -> Result<Self, BoxError> {
        let Ok(plugin_len) = plugin.len().try_into() else {
            #[rustfmt::skip]
            return Err(BoxError::new(TarantoolErrorCode::IllegalParams, format!("plugin name length must not exceed 65535, got {}", plugin.len())));
        };
        let Ok(service_len) = service.len().try_into() else {
            #[rustfmt::skip]
            return Err(BoxError::new(TarantoolErrorCode::IllegalParams, format!("service name length must not exceed 65535, got {}", service.len())));
        };
        let Ok(path_len) = path.len().try_into() else {
            #[rustfmt::skip]
            return Err(BoxError::new(TarantoolErrorCode::IllegalParams, format!("route path length must not exceed 65535, got {}", path.len())));
        };
        let Ok(version_len) = version.len().try_into() else {
            #[rustfmt::skip]
            return Err(BoxError::new(TarantoolErrorCode::IllegalParams, format!("version string length must not exceed 65535, got {}", version.len())));
        };

        let total_string_len = plugin_len
            // For an extra '.' between plugin and service names
            + 1
            + service_len
            + path_len
            + version_len;
        let mut string_storage = Vec::with_capacity(total_string_len as _);
        string_storage.extend_from_slice(plugin.as_bytes());
        string_storage.push(b'.');
        string_storage.extend_from_slice(service.as_bytes());
        string_storage.extend_from_slice(path.as_bytes());
        string_storage.extend_from_slice(version.as_bytes());

        let start = string_storage.as_mut_ptr();
        let capacity = string_storage.capacity();

        // Safety: vec has an allocated buffer, so the pointer cannot be null.
        // Also a concatenation of utf8 strings is always a utf8 string.
        let storage =
            unsafe { FfiSafeStr::from_raw_parts(NonNull::new_unchecked(start), capacity) };

        // Self now owns this data and will be freeing it in it's `drop`.
        std::mem::forget(string_storage);

        Ok(Self {
            storage,
            plugin_len,
            service_len,
            path_len,
            version_len,
        })
    }

    #[allow(unreachable_code)]
    pub(crate) fn drop(&mut self) {
        let (pointer, capacity) = self.storage.into_raw_parts();
        if capacity == 0 {
            #[cfg(debug_assertions)]
            unreachable!("drop should only be called once");
            return;
        }

        // Note: we pretend the original Vec was filled to capacity which
        // may or may not be true, there might have been some unitialized
        // data at the end. But this doesn't matter in this case because we
        // just want to drop the data, and only the capacity matters.
        // Safety: safe because drop only happens once and the next time the
        // pointer will be set to null.
        unsafe {
            let string_storage = Vec::from_raw_parts(pointer, capacity, capacity);
            drop(string_storage);
        }
        // Overwrite with len = 0, to guard against double free
        self.storage = FfiSafeStr::from("");
    }

    #[inline(always)]
    fn storage_slice(&self, start: u16, len: u16) -> &str {
        // SAFETY: data is alive for the lifetime of `&self`, and borrow checker does it's thing
        let storage = unsafe { self.storage.as_str() };
        let end = (start + len) as usize;
        &storage[start as usize..end]
    }

    #[inline(always)]
    pub fn plugin(&self) -> &str {
        self.storage_slice(0, self.plugin_len)
    }

    #[inline(always)]
    pub fn service(&self) -> &str {
        self.storage_slice(self.plugin_len + 1, self.service_len)
    }

    #[inline(always)]
    pub fn path(&self) -> &str {
        self.storage_slice(self.plugin_len + 1 + self.service_len, self.path_len)
    }

    #[inline(always)]
    pub fn route_repr(&self) -> &str {
        self.storage_slice(0, self.plugin_len + 1 + self.service_len + self.path_len)
    }

    #[inline(always)]
    pub fn version(&self) -> &str {
        self.storage_slice(
            self.plugin_len + 1 + self.service_len + self.path_len,
            self.version_len,
        )
    }
}

impl std::fmt::Display for PackedServiceIdentifier {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}.{}:v{}{}",
            self.plugin(),
            self.service(),
            self.version(),
            self.path()
        )
    }
}
