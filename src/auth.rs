use std::os::raw::{c_char, c_int, c_uint};
use tarantool::auth::AuthMethod;
use tarantool::error::{BoxError, IntoBoxError, TarantoolErrorCode};
use tarantool::network::protocol::codec::{chap_sha1_prepare, ldap_prepare, md5_prepare};

/// This size is required by Tarantool to work properly.
pub(crate) const SALT_MIN_LEN: usize = 20;

unsafe extern "C" {
    /// # Safety
    ///
    /// - Pointers must have valid and non-null values.
    /// - Salt must be at least 20 bytes.
    #[link_name = "authenticate"]
    pub(crate) unsafe fn authenticate_raw(
        user: *const u8,
        user_len: u32,
        salt: *const u8,
        auth_info: *const u8,
    ) -> std::ffi::c_int;

    /// # Safety
    ///
    /// - All of TarantoolAuthMethod's fields should be initialized.
    #[link_name = "auth_method_register"]
    pub(crate) unsafe fn auth_method_register_raw(
        #[rustfmt::skip] method: *mut TarantoolAuthMethod,
    );
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // Do not provide any specific info to prevent user probing!
    #[error("user '{0}' not found or supplied credentials are invalid")]
    UserNotFoundOrInvalidCreds(String),

    #[error("username {0} is too big")]
    UsernameIsTooBig(String),
}

impl IntoBoxError for Error {}

/// Builds an authentication information that is used by [`crate::auth::authenticate_raw`].
/// Password may be either clear text or hash, depending on the provided authentication method.
fn build_auth_info(password: &[u8], method: AuthMethod) -> Vec<u8> {
    // Here is a short algorithm overview:
    // ```
    // 1) get stringified representation of a method
    // 2) concatenate method with password as a single byte string
    // 3) write length of a concatenated byte string as msgpack string prefix
    // 4) put concatenated byte string after msgpack string prefix
    // ```
    //
    // Resulting authentcation packet, encoded in MessagePack will look like:
    // ```
    // msgpack_array_marker(always 1 byte because only 2 elements are stored)
    // + method_string_prefix(always 1 byte) + method_string_content(less than 32 bytes)
    // + password_string_prefix(either 1 or 2 bytes) + password_string_content(more or less than 32 bytes)
    // = MP_ARRAY[MP_STR(method), MP_STR(password)]
    // ```
    //
    // TODO(kbezuglyi):
    // - Hashed passwords (SHA-1) are not always valid UTF-8, but Rust's `String` enforces UTF-8, so we must manually prefix
    //   the hashed bytes with a MessagePack string header to prevent `rmp` from encoding them as MessagePack binary.
    //   Encoding as binary causes Tarantool to panic during authentication, because it only can handle MessagePack strings.
    //   NIT: authentication checker for MD5 in Tarantool (i.e. `box/auth_md5.c:auth_md5_request_check`) actually handles
    //   MessagePack binary nicely but the "proxying" function we use (i.e. `box/authentication.c:authenticate`) - does not.

    const PACKET_PARTS_AMOUNT: u32 = 2;

    let method_repr = method.as_str().as_bytes();
    let method_length = method.len();
    let password_length = password.len();

    let mut result = Vec::with_capacity(64);

    rmp::encode::write_array_len(&mut result, PACKET_PARTS_AMOUNT)
        .expect("writing to vector should not have failed");

    rmp::encode::write_str_len(&mut result, method_length as _)
        .expect("writing to vector should not have failed");
    result.extend_from_slice(method_repr);

    rmp::encode::write_str_len(&mut result, password_length as _)
        .expect("writing to vector should not have failed");
    result.extend_from_slice(password);

    result
}

/// Retrieves the authentication method for a given user
/// via looking into `_pico_user` using `admin`'s session.
/// Returns `None` if the specified user does not exist.
///
/// May panic if certain invariants are not upheld.
fn determine_auth_method(username: &str) -> Option<AuthMethod> {
    let node = crate::traft::node::global().expect("node should be initialized");

    // Running as admin helps us avoid insufficient
    // permissions error on reading from `_pico_user` system table.
    let user_def = tarantool::session::with_su(crate::schema::ADMIN_ID, || {
        node.storage.users.by_name(username)
    })
    .expect("should be able to su into admin")
    .expect("failed to access storage");

    Some(user_def?.auth?.method)
}

/// See [`authenticate`] for more info.
pub(crate) fn authenticate_inner(
    user: &str,
    password: impl AsRef<[u8]>,
    auth_salt: [u8; 20],
    auth_method: AuthMethod,
) -> Result<(), Error> {
    let auth_info = build_auth_info(password.as_ref(), auth_method);
    let auth_info_ptr = auth_info.as_ptr();

    let user_ptr = user.as_ptr();
    let user_len = user.len() as u32;

    let salt_ptr = auth_salt.as_ptr();

    // SAFETY: all arguments have been validated.
    let ret = unsafe { authenticate_raw(user_ptr, user_len, salt_ptr, auth_info_ptr) };
    if ret != 0 {
        return Err(Error::UserNotFoundOrInvalidCreds(user.to_owned()));
    }

    Ok(())
}

/// Tries to authenticate with specified username and password.
/// If no authentication salt was passed, it generates a random
/// one for the first 4 bytes, filling other 16 with zeroes.
///
/// # Errors
///
/// - User was not found in the list of available users.
/// - Authentication method was not initialized for the user.
/// - Username length is greater than `u32`.
/// - Password is not correct for the specified user.
///
/// May panic if certain invariants are not upheld.
pub(crate) fn authenticate(
    user_name: &str,
    user_pass: impl AsRef<[u8]>,
    auth_salt: Option<[u8; 4]>,
) -> Result<(), BoxError> {
    const USER_NAME_MAX_LEN: usize = u32::MAX as _;

    let Some(method) = determine_auth_method(user_name) else {
        return Err(Error::UserNotFoundOrInvalidCreds(user_name.into()).into_box_error());
    };

    let user_name_len = user_name.len();
    if user_name_len > USER_NAME_MAX_LEN {
        return Err(Error::UsernameIsTooBig(user_name.into()).into_box_error());
    }

    let salt_val = auth_salt.unwrap_or_else(rand::random);
    let mut salt_buf = [0u8; SALT_MIN_LEN];
    salt_buf[0..4].copy_from_slice(&salt_val);

    let user_pass = match method {
        AuthMethod::ChapSha1 => chap_sha1_prepare(user_pass, &salt_buf),
        AuthMethod::Md5 => md5_prepare(user_name, user_pass, salt_val),
        AuthMethod::Ldap => ldap_prepare(user_pass),
        AuthMethod::ScramSha256 => {
            return Err(BoxError::new(
                TarantoolErrorCode::UnknownAuthMethod,
                "scram-sha256 is not supported yet",
            ));
        }
    };

    authenticate_inner(user_name, user_pass, salt_buf, method).map_err(|e| e.into_box_error())
}

/// Note: this struct is used in FFI.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct TarantoolAuthenticator {
    pub method: *const TarantoolAuthMethod,
}

/// Note: this struct is used in FFI.
/// Field order matters, as it's defined in Tarantool.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct TarantoolAuthMethod {
    pub name: *const c_char,
    pub flags: c_uint,

    #[rustfmt::skip]
    pub auth_method_delete: Option<
        unsafe extern "C-unwind" fn(
            method: *mut TarantoolAuthMethod,
        ),
    >,

    pub auth_data_prepare: Option<
        unsafe extern "C-unwind" fn(
            method: *const TarantoolAuthMethod,
            password: *const c_char,
            password_len: u32,
            user: *const c_char,
            user_len: u32,
            auth_data: *mut *const c_char,
            auth_data_end: *mut *const c_char,
        ),
    >,

    pub auth_request_prepare: Option<
        unsafe extern "C-unwind" fn(
            method: *const TarantoolAuthMethod,
            password: *const c_char,
            password_len: u32,
            user: *const c_char,
            user_len: u32,
            salt: *const c_char,
            auth_request: *mut *const c_char,
            auth_request_end: *mut *const c_char,
        ),
    >,

    pub auth_request_check: Option<
        unsafe extern "C-unwind" fn(
            method: *const TarantoolAuthMethod,
            auth_request: *const c_char,
            auth_request_end: *const c_char,
        ) -> c_int,
    >,

    pub authenticator_new: Option<
        unsafe extern "C-unwind" fn(
            method: *const TarantoolAuthMethod,
            auth_data: *const c_char,
            auth_data_end: *const c_char,
        ) -> *mut TarantoolAuthenticator,
    >,

    #[rustfmt::skip]
    pub authenticator_delete: Option<
        unsafe extern "C-unwind" fn(
            auth: *mut TarantoolAuthenticator,
        ),
    >,

    pub authenticate_request: Option<
        unsafe extern "C-unwind" fn(
            auth: *const TarantoolAuthenticator,
            user: *const c_char,
            user_len: u32,
            salt: *const c_char,
            auth_request: *const c_char,
            auth_request_end: *const c_char,
        ) -> bool,
    >,
}

/// Initialize extra auth methods which are implemented in rust codebase.
pub fn register_tarantool_auth_methods() {
    let scram = crate::scram::tarantool::provide_auth_method();
    unsafe {
        crate::auth::auth_method_register_raw(scram);
    }
}
