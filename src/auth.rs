use crate::{scram, storage::Catalog};
use std::os::raw::{c_char, c_int, c_uint, c_void};
use tarantool::auth::{AuthDef, AuthMethod};
use tarantool::error::{BoxError, TarantoolErrorCode};
use tarantool::network::protocol::codec::{chap_sha1_prepare, ldap_prepare, md5_prepare};

/// This size is required by Tarantool to work properly.
pub(crate) const SALT_MIN_LEN: usize = 20;

#[repr(C)]
pub(crate) struct TarantoolUser {
    _opaque: [u8; 0],
}

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
    ) -> c_int;

    /// # Safety
    ///
    /// - Pointers must have valid and non-null values.
    #[link_name = "authenticate_ext"]
    pub(crate) unsafe fn authenticate_ext_raw(
        user: *const u8,
        user_len: u32,
        auth_check: extern "C-unwind" fn(user: *const TarantoolUser, ctx: *mut c_void) -> bool,
        ctx: *mut c_void,
    ) -> c_int;

    /// # Safety
    ///
    /// - All of TarantoolAuthMethod's fields should be initialized.
    #[link_name = "auth_method_register"]
    pub(crate) unsafe fn auth_method_register_raw(
        #[rustfmt::skip] method: *mut TarantoolAuthMethod,
    );
}

/// Try fetching user auth info ([`AuthDef`]) from system catalog.
/// **Important:** whenever possible, failure to find such info should
/// nonetheless result in a fake authentication attempt to prevent
/// user probing. This is exactly what we should do for SCRAM.
///
/// Please make sure to understand this before editing this function
/// or its call sites in pgproto or elsewhere.
pub fn try_get_auth_def(storage: &Catalog, user: &str) -> Option<AuthDef> {
    let user_def =
        tarantool::session::with_su(crate::schema::ADMIN_ID, || storage.users.by_name(user))
            .expect("failed to su into admin")
            .inspect_err(|e| {
                // This should not happen (failed to even check if user exists).
                crate::tlog!(Error, "failed to get UserDef for user {user}: {e}");
            })
            .ok()??;

    if user_def.auth.is_none() {
        // This should not happen (user without auth info).
        crate::tlog!(Error, "failed to get AuthDef for user {user}");
    }

    user_def.auth
}

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

/// A bare bones wrapper for tarantool's authentication API.
/// See [`authenticate_with_password`] for more info.
pub(crate) fn do_authenticate(
    user: &str,
    password: impl AsRef<[u8]>,
    salt: &[u8; SALT_MIN_LEN],
    method: AuthMethod,
) -> Result<(), BoxError> {
    let auth_info = build_auth_info(password.as_ref(), method);
    let auth_info_ptr = auth_info.as_ptr();

    let user_ptr = user.as_ptr();
    let user_len = user.len() as u32;

    let salt_ptr = salt.as_ptr();

    // SAFETY: all arguments have been validated.
    let ret = unsafe { authenticate_raw(user_ptr, user_len, salt_ptr, auth_info_ptr) };
    if ret != 0 {
        return Err(BoxError::last());
    }

    Ok(())
}

/// A specialized auth flow for SCRAM only.
/// Still, it uses tarantool's API to run auth triggers.
fn do_authenticate_scram(
    user: &str,
    password: impl AsRef<[u8]>,
    secret: &scram::ServerSecret,
) -> Result<(), BoxError> {
    // XXX: Use tarantool's wrapper which will perform additional
    // checks, execute triggers and update the credentials.
    crate::auth::authenticate_ext(user, || {
        if secret.is_password_invalid(&password).into() {
            return Err(BoxError::new(
                TarantoolErrorCode::PasswordMismatch,
                "User not found or supplied credentials are invalid",
            ));
        }

        Ok(())
    })??;

    Ok(())
}

/// Try to authenticate with specified username and password.
///
/// # Errors
///
/// - User was not found in the list of available users.
/// - Authentication method was not initialized for the user.
/// - Password is not correct for the specified user.
///
/// May panic if certain invariants are not upheld.
pub(crate) fn authenticate_with_password(
    user: &str,
    password: impl AsRef<[u8]>,
) -> Result<(), BoxError> {
    const fn zero_salt<const N: usize>() -> [u8; N] {
        [0; N]
    }

    let node = crate::traft::node::global()?;
    let auth_def = try_get_auth_def(&node.storage, user);

    match auth_def.map(|x| (x.method, x.data)) {
        Some((method @ AuthMethod::Md5, _)) => {
            let password = md5_prepare(user, password, &zero_salt());
            do_authenticate(user, password, &zero_salt(), method)
        }
        Some((method @ AuthMethod::Ldap, _)) => {
            let password = ldap_prepare(password);
            do_authenticate(user, password, &zero_salt(), method)
        }
        Some((method @ AuthMethod::ChapSha1, _)) => {
            let password = chap_sha1_prepare(password, &zero_salt());
            do_authenticate(user, password, &zero_salt(), method)
        }
        Some((AuthMethod::ScramSha256, secret)) => {
            let secret = scram::ServerSecret::parse(&secret).expect("invalid AuthDef in catalog");
            do_authenticate_scram(user, password, &secret)
        }
        // TODO: maybe this protection should be optional.
        // If that's the case, add `if <condition>` to this branch.
        None => {
            // Use a mock to prevent timing attacks against the branch above.
            let secret = scram::ServerSecret::mock(&zero_salt());
            do_authenticate_scram(user, password, &secret)
        }
    }
}

fn authenticate_ext_impl<F>(user: &str, mut auth_check: F) -> Result<(), BoxError>
where
    F: FnMut() -> bool,
{
    extern "C-unwind" fn trampoline<F>(_user: *const TarantoolUser, ctx: *mut c_void) -> bool
    where
        F: FnMut() -> bool,
    {
        // SAFETY: we use the same `F` throughout this impl.
        let callback = unsafe { &mut *(ctx as *mut F) };
        (callback)()
    }

    let user_ptr = user.as_ptr();
    let user_len = user.len() as u32;

    // Trampoline's generic should match callback's type.
    // The callback won't be persisted by the foreign call.
    let trampoline = trampoline::<F>;
    let callback = (&mut auth_check as *mut F) as *mut c_void;

    // SAFETY: all arguments have been checked.
    let ret = unsafe { authenticate_ext_raw(user_ptr, user_len, trampoline, callback) };
    if ret != 0 {
        return Err(BoxError::last());
    }

    Ok(())
}

/// Try to authenticate with specified username and
/// customizable authentication logic callback.
///
/// **Important:** this function returns Result-of-Result;
/// the topmost layer represents the callback error while the
/// innermost layer represents the error of [`authenticate_ext`]
/// itself.
///
/// # Errors
///
/// - User was not found in the list of available users.
/// - ... anything `auth_check` has to offer.
///
/// May panic if certain invariants are not upheld.
pub(crate) fn authenticate_ext<E, F>(
    user: &str,
    mut auth_check: F,
) -> Result<Result<(), BoxError>, E>
where
    F: FnMut() -> Result<(), E>,
{
    // XXX: Use tarantool's wrapper which will perform additional
    // checks, execute triggers and update the credentials.
    let mut main_res = Ok(());
    let extra_res = crate::auth::authenticate_ext_impl(user, || {
        // Now perform the check itself and store the result.
        main_res = auth_check();

        // XXX: propagate auth result back to `authenticate_ext_impl`
        // in order to run correct triggers for e.g. audit.
        main_res.is_ok()
    });

    // XXX: throw errors in this exact order:
    // 1. callback-specific auth errors.
    // 2. other errors from `authenticate_ext_impl`.
    main_res?;
    Ok(extra_res)
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
