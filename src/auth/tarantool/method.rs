//! APIs for implementing tarantool auth methods in Rust.

use super::{TarantoolAuthMethod, TarantoolAuthenticator, SALT_LEN};
use std::ffi::{c_char, c_int};
use std::ptr;
use std::ptr::NonNull;
use tarantool::error::IntoBoxError;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum AuthenticationVerdict {
    /// The authentication request is valid and the user should be authenticated.
    Authenticated,
    /// The authentication request is invalid and the user should be rejected.
    Rejected,
}

/// A tarantool authentication method, raw variant.
pub trait RawAuthMethod {
    /// Auth method name. This is the name to use for the method's registration with tarantool.
    const NAME: &'static std::ffi::CStr;

    type Authenticator: RawAuthenticator<Method = Self>;

    /// Prepare server-side auth data for the auth method.
    /// This method is called when creating a new user to save its login information.
    ///
    /// The function should return the result as a MessagePack slice, and the slice
    /// has to be allocated in [the box region allocator](crate::tarantool::box_region).
    unsafe fn auth_data_prepare(&self, password: &[u8], user: &[u8]) -> NonNull<[u8]>;

    /// Prepare auth request for the auth method.
    /// This method is called when tarantool needs to authenticate with another instance over iproto.
    ///
    /// The salt is a random value provided by the server to prevent replay attacks.
    ///
    /// The function should return the result as a MessagePack slice, and the slice
    /// has to be allocated in [the box region allocator](crate::tarantool::box_region).
    unsafe fn auth_request_prepare(
        &self,
        password: &[u8],
        user: &[u8],
        salt: &[u8; SALT_LEN],
    ) -> NonNull<[u8]>;

    /// Check a received auth request for validity.
    /// The method should not attempt to serve the auth request yet, just to check its format.
    fn auth_request_check(&self, auth_request: &[u8]) -> tarantool::Result<()>;

    /// Create an authenticator object for a certain user from their auth data.
    ///
    /// The authenticator can process the auth data into an efficient representation
    /// and then serve multiple auth requests for the same user.
    fn authenticator_new(&self, auth_data: &[u8]) -> tarantool::Result<Self::Authenticator>;
}

pub trait RawAuthenticator {
    /// The auth method this authenticator is associated with.
    type Method: RawAuthMethod;

    /// Process an authentication request received from a user.
    ///
    /// The request is guaranteed to already be validated by [`RawAuthMethod::auth_request_check`].
    ///
    /// The authenticator should return an [`AuthenticationVerdict`] if it can determine that the
    /// auth request is valid or not. It can return an error if it can't validate the request for
    /// some reason.
    ///
    /// Beware! If this function returns an error, the error message will be sent to the client via
    /// the iproto protocol, so do not include sensitive information there.
    fn authenticate_request(
        &self,
        method: &Self::Method,
        user: &[u8],
        salt: &[u8; SALT_LEN],
        auth_request: &[u8],
    ) -> tarantool::Result<AuthenticationVerdict>;
}

/// A shim object that will convert an implementor of [`RawAuthMethod`] into `*mut TarantoolAuthMethod`.
// we use `repr(C)`, because we need to cast `*mut AuthMethodWrapper`
//  to its superclass - `*mut TarantoolAuthMethod`
#[repr(C)]
pub struct AuthMethodWrapper<T> {
    base: TarantoolAuthMethod,
    rust: T,
}

impl<T: RawAuthMethod> AuthMethodWrapper<T> {
    /// Create the shim object for tarantool to be able to use the passed
    /// rust auth method implementation.
    pub fn new_raw(rust: T) -> *mut TarantoolAuthMethod {
        let this = Self {
            base: TarantoolAuthMethod {
                name: T::NAME.as_ptr(),
                // FIXME: make it possible to set flags with the trait
                flags: 0,
                auth_method_delete: Some(auth_method_delete::<T>),
                auth_data_prepare: Some(auth_data_prepare::<T>),
                auth_request_prepare: Some(auth_request_prepare::<T>),
                auth_request_check: Some(auth_request_check::<T>),
                authenticator_new: Some(authenticator_new::<T>),
                authenticator_delete: Some(authenticator_delete::<T::Authenticator>),
                authenticate_request: Some(authenticate_request::<T::Authenticator>),
            },
            rust,
        };

        let this_boxed = Box::new(this);
        let this_boxed_ptr = Box::into_raw(this_boxed);

        // Upcast `AuthMethodWrapper` to `TarantoolAuthenticator`.
        // `AuthMethodWrapper` is `repr(C)` and `TarantoolAuthMethod`
        // is included as the first field, so this will work
        this_boxed_ptr.cast::<TarantoolAuthMethod>()
    }
}

#[repr(C)]
struct AuthenticatorWrapper<T> {
    base: TarantoolAuthenticator,
    rust: T,
}

impl<T: RawAuthenticator> AuthenticatorWrapper<T> {
    pub fn new_raw(method_ptr: *const TarantoolAuthMethod, rust: T) -> *mut TarantoolAuthenticator {
        let this = Self {
            base: TarantoolAuthenticator { method: method_ptr },
            rust,
        };

        let this_boxed = Box::new(this);
        let this_boxed_ptr = Box::into_raw(this_boxed);

        // Upcast `AuthenticatorWrapper` to `TarantoolAuthenticator`.
        // `AuthenticatorWrapper` is `repr(C)` and `TarantoolAuthenticator`
        // is included as the first field, so this will work
        this_boxed_ptr.cast::<TarantoolAuthenticator>()
    }
}

////////////////////////////////////////////////////////////////////////////////
// Helper functions to reduce FFI boilerplate
////////////////////////////////////////////////////////////////////////////////

unsafe fn receive_method_value<T: RawAuthMethod>(
    method: *mut TarantoolAuthMethod,
) -> Box<AuthMethodWrapper<T>> {
    unsafe { Box::from_raw(method.cast::<AuthMethodWrapper<T>>()) }
}

unsafe fn receive_method_ref<'a, T: RawAuthMethod>(
    method: *const TarantoolAuthMethod,
) -> &'a AuthMethodWrapper<T> {
    unsafe { &*method.cast::<AuthMethodWrapper<T>>() }
}

unsafe fn receive_authenticator_value<T: RawAuthenticator>(
    method: *mut TarantoolAuthenticator,
) -> Box<AuthenticatorWrapper<T>> {
    unsafe { Box::from_raw(method.cast::<AuthenticatorWrapper<T>>()) }
}

unsafe fn receive_authenticator_ref<'a, T: RawAuthenticator>(
    authenticator: *const TarantoolAuthenticator,
) -> &'a AuthenticatorWrapper<T> {
    unsafe { &*authenticator.cast::<AuthenticatorWrapper<T>>() }
}

unsafe fn receive_array_ref<'a, const LEN: usize>(begin: *const c_char) -> &'a [u8; LEN] {
    unsafe { &*begin.cast::<[u8; LEN]>() }
}

unsafe fn receive_begin_len_slice<'a>(begin: *const c_char, len: u32) -> &'a [u8] {
    unsafe { std::slice::from_raw_parts(begin.cast::<u8>(), len as usize) }
}

unsafe fn receive_begin_end_slice<'a>(begin: *const c_char, end: *const c_char) -> &'a [u8] {
    let begin = begin.cast::<u8>();

    let len = end.cast::<u8>().offset_from_unsigned(begin);
    std::slice::from_raw_parts(begin, len)
}

unsafe fn return_box_region_out_data(
    out_data: *mut *const c_char,
    out_data_end: *mut *const c_char,
    ret_slice: NonNull<[u8]>,
) {
    let ptr = ret_slice.cast::<u8>().as_ptr();
    let len = ret_slice.len();

    unsafe {
        *out_data = ptr.cast::<c_char>();
        *out_data_end = (*out_data).add(len);
    }
}

////////////////////////////////////////////////////////////////////////////////
// FFI VTable trampolines
////////////////////////////////////////////////////////////////////////////////

unsafe extern "C-unwind" fn auth_method_delete<T: RawAuthMethod>(method: *mut TarantoolAuthMethod) {
    unsafe {
        let _ = receive_method_value::<T>(method);
    }
}

unsafe extern "C-unwind" fn auth_data_prepare<T: RawAuthMethod>(
    method: *const TarantoolAuthMethod,
    password: *const c_char,
    password_len: u32,
    user: *const c_char,
    user_len: u32,
    auth_data: *mut *const c_char,
    auth_data_end: *mut *const c_char,
) {
    let method = unsafe { receive_method_ref::<T>(method) };

    let password = unsafe { receive_begin_len_slice(password, password_len) };
    let user = unsafe { receive_begin_len_slice(user, user_len) };

    let ret_slice = method.rust.auth_data_prepare(password, user);

    unsafe { return_box_region_out_data(auth_data, auth_data_end, ret_slice) }
}

unsafe extern "C-unwind" fn auth_request_prepare<T: RawAuthMethod>(
    method: *const TarantoolAuthMethod,
    password: *const c_char,
    password_len: u32,
    user: *const c_char,
    user_len: u32,
    salt: *const c_char,
    auth_request: *mut *const c_char,
    auth_request_end: *mut *const c_char,
) {
    let method = unsafe { receive_method_ref::<T>(method) };

    let password = unsafe { receive_begin_len_slice(password, password_len) };
    let user = unsafe { receive_begin_len_slice(user, user_len) };
    let salt = unsafe { receive_array_ref(salt) };

    let ret_slice = method.rust.auth_request_prepare(password, user, salt);

    unsafe { return_box_region_out_data(auth_request, auth_request_end, ret_slice) }
}

unsafe extern "C-unwind" fn auth_request_check<T: RawAuthMethod>(
    method: *const TarantoolAuthMethod,
    auth_request: *const c_char,
    auth_request_end: *const c_char,
) -> c_int {
    let method = unsafe { receive_method_ref::<T>(method) };

    let auth_request = unsafe { receive_begin_end_slice(auth_request, auth_request_end) };

    match method.rust.auth_request_check(auth_request) {
        Ok(()) => 0,
        Err(e) => {
            e.set_last_error();
            -1
        }
    }
}

unsafe extern "C-unwind" fn authenticator_new<T: RawAuthMethod>(
    method_ptr: *const TarantoolAuthMethod,
    auth_data: *const c_char,
    auth_data_end: *const c_char,
) -> *mut TarantoolAuthenticator {
    let method = unsafe { receive_method_ref::<T>(method_ptr) };

    let auth_data = unsafe { receive_begin_end_slice(auth_data, auth_data_end) };

    match method.rust.authenticator_new(auth_data) {
        Ok(authenticator) => AuthenticatorWrapper::new_raw(method_ptr, authenticator),
        Err(e) => {
            e.set_last_error();
            ptr::null_mut()
        }
    }
}

unsafe extern "C-unwind" fn authenticator_delete<T: RawAuthenticator>(
    auth: *mut TarantoolAuthenticator,
) {
    unsafe {
        let _ = receive_authenticator_value::<T>(auth);
    }
}

unsafe extern "C-unwind" fn authenticate_request<T: RawAuthenticator>(
    auth: *const TarantoolAuthenticator,
    user: *const c_char,
    user_len: u32,
    salt: *const c_char,
    auth_request: *const c_char,
    auth_request_end: *const c_char,
) -> bool {
    let auth = unsafe { receive_authenticator_ref::<T>(auth) };
    let method = unsafe { receive_method_ref::<T::Method>(auth.base.method) };

    let user = unsafe { receive_begin_len_slice(user, user_len) };
    let salt = unsafe { receive_array_ref(salt) };

    let auth_request = unsafe { receive_begin_end_slice(auth_request, auth_request_end) };

    match auth
        .rust
        .authenticate_request(&method.rust, user, salt, auth_request)
    {
        Ok(AuthenticationVerdict::Authenticated) => true,
        Ok(AuthenticationVerdict::Rejected) => {
            // Tarantool's `authenticate_ext` will set a genetic "authentication failed" error
            //  if `false` is returned without setting an error.
            // Authentication method implementation should not set the error for this to work.
            debug_assert!(
                tarantool::error::BoxError::maybe_last().is_ok(),
                "authenticate_request returned `Rejected`, but a tarantool error was set"
            );
            tarantool::error::clear_error();
            false
        }
        Err(e) => {
            // This will expose the error message to the remote user, so should only
            //  be used for "system" errors, not failed authentication.
            e.set_last_error();
            false
        }
    }
}
