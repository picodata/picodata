use crate::auth::{TarantoolAuthMethod, TarantoolAuthenticator};
use std::{
    ffi::CStr,
    os::raw::{c_char, c_int},
};
use tarantool::error::{BoxError, TarantoolErrorCode};

pub const SCRAM_METHOD_NAME: &'static CStr = c"scram-sha256";

fn box_region_copy_unaligned(bytes: &[u8]) -> *mut [u8] {
    let size = bytes.len();

    // SAFETY: we allocate unaligned byte slice and check the result.
    let buffer = unsafe { ::tarantool::ffi::tarantool::box_region_alloc(size).cast::<u8>() };
    if buffer.is_null() {
        // This is ugly but acceptable, because other methods use `xregion_alloc`.
        panic!("failed to allocate {size} bytes in region");
    }

    // SAFETY: buffer is guaranteed to be non-null and contain enough space.
    unsafe {
        buffer.copy_from_nonoverlapping(bytes.as_ptr(), size);
        std::ptr::slice_from_raw_parts_mut(buffer, size)
    }
}

/// Allocate a new instance of scram authenticator and downcast it to [`TarantoolAuthMethod`].
/// The resulting pointer is suitable for [`crate::auth::auth_method_register_raw`].
pub fn provide_auth_method() -> *mut TarantoolAuthMethod {
    Box::into_raw(Box::new(TarantoolAuthMethod {
        name: SCRAM_METHOD_NAME.as_ptr(),
        flags: 0,

        // These methods do all the heavy lifting.
        auth_method_delete: Some(scram_auth_method_delete),
        auth_data_prepare: Some(scram_auth_data_prepare),

        // These methods will produce dummy results.
        auth_request_prepare: Some(scram_auth_request_prepare),
        auth_request_check: Some(scram_auth_request_check),

        // Since we don't plan to support scram over iproto, we need
        // to provide a mock authenticator which will just raise regardless.
        authenticator_new: Some(scram_authenticator_new),
        authenticator_delete: Some(scram_authenticator_delete),
        authenticate_request: Some(scram_authenticate_request),
    }))
}

unsafe extern "C-unwind" fn scram_auth_method_delete(method: *mut TarantoolAuthMethod) {
    // SAFETY: we assume that `method` is boxed.
    unsafe {
        let _ = Box::from_raw(method);
    }
}

unsafe extern "C-unwind" fn scram_auth_data_prepare(
    _method: *const TarantoolAuthMethod,
    password: *const c_char,
    password_len: u32,
    _user: *const c_char,
    _user_len: u32,
    auth_data: *mut *const c_char,
    auth_data_end: *mut *const c_char,
) {
    let password = std::slice::from_raw_parts(password.cast::<u8>(), password_len as _);
    let salt = rand::random::<[u8; super::secret::SCRAM_DEFAULT_SALT_LEN]>();

    let secret = super::secret::ServerSecret::generate(
        password,
        &salt,
        super::secret::SCRAM_DEFAULT_ITERATIONS,
    );

    // Note: unfortunately, currently there's no easy way to precompute length,
    // so we have to allocate and format a string object. However, we cannot return
    // it from this method, since Tarantool expects to see a region allocation.
    let rmp = rmpv::Value::String(format!("{secret}").into());
    let rmp_bytes = rmp_serde::encode::to_vec(&rmp).expect("failed to encode scram secret");

    let data = box_region_copy_unaligned(&rmp_bytes);
    *auth_data = data.cast::<c_char>();
    *auth_data_end = (*auth_data).add(data.len());
}

unsafe extern "C-unwind" fn scram_authenticator_new(
    method: *const TarantoolAuthMethod,
    _auth_data: *const c_char,
    _auth_data_end: *const c_char,
) -> *mut TarantoolAuthenticator {
    // Note: per above, we don't have to use auth data here.
    // This authenticator will always raise error during authentication.
    Box::into_raw(Box::new(TarantoolAuthenticator { method }))
}

unsafe extern "C-unwind" fn scram_authenticator_delete(auth: *mut TarantoolAuthenticator) {
    // SAFETY: we assume that `auth` is boxed.
    unsafe {
        let _ = Box::from_raw(auth);
    }
}

unsafe extern "C-unwind" fn scram_authenticate_request(
    _auth: *const TarantoolAuthenticator,
    _user: *const c_char,
    _user_len: u32,
    _salt: *const c_char,
    _auth_request: *const c_char,
    _auth_request_end: *const c_char,
) -> bool {
    BoxError::new(
        TarantoolErrorCode::InvalidAuthRequest,
        "scram over iproto is not supported",
    )
    .set_last();

    const REQUEST_FAILED: bool = false;
    REQUEST_FAILED
}

unsafe extern "C-unwind" fn scram_auth_request_prepare(
    _method: *const TarantoolAuthMethod,
    _password: *const c_char,
    _password_len: u32,
    _user: *const c_char,
    _user_len: u32,
    _salt: *const c_char,
    auth_request: *mut *const c_char,
    auth_request_end: *mut *const c_char,
) {
    // XXX: any valid msgpack will suffice, since `scram_authenticate_request` always returns false.
    let rmp = rmpv::Value::String("".into());
    let rmp_bytes = rmp_serde::encode::to_vec(&rmp).expect("failed to encode scram request");

    let req = box_region_copy_unaligned(&rmp_bytes);
    *auth_request = req.cast::<c_char>();
    *auth_request_end = (*auth_request).add(req.len());
}

// Allow everything, `scram_authenticate_request` will take care of the rest.
unsafe extern "C-unwind" fn scram_auth_request_check(
    _method: *const TarantoolAuthMethod,
    _auth_request: *const c_char,
    _auth_request_end: *const c_char,
) -> c_int {
    const CHECK_SUCCEEDED: c_int = 0;
    CHECK_SUCCEEDED
}
