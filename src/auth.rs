use tarantool::auth::AuthMethod;
use tarantool::error::IntoBoxError;
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
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("user '{0}' not found or supplied credentials are invalid")] // avoid username probing
    UserNotFoundOrInvalidCreds(String),
    #[error("username {0} is too big")]
    UsernameIsTooBig(String),
}

impl IntoBoxError for Error {}

/// Builds an authentication information that is used by [`crate::auth::authenticate_raw`].
/// Password may be either clear text or hash, depending on the provided authentication method.
pub(crate) fn build_auth_info(password: &[u8], method: AuthMethod) -> Vec<u8> {
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

    const PACKET_MARKER_SIZE: usize = 1;
    const PACKET_PARTS_AMOUNT: u32 = 2;

    const MD5_METHOD_MARKER_SIZE: usize = 1;
    const MD5_METHOD_VALUE_SIZE: usize = 3;
    const MD5_PASSWORD_MARKER_SIZE: usize = 2;
    const MD5_PASSWORD_VALUE_SIZE: usize = 32 + MD5_METHOD_VALUE_SIZE;
    const MD5_PACKET_SIZE: usize = PACKET_MARKER_SIZE
        + MD5_METHOD_MARKER_SIZE
        + MD5_METHOD_VALUE_SIZE
        + MD5_PASSWORD_MARKER_SIZE
        + MD5_PASSWORD_VALUE_SIZE; // = 42 (total, always)

    const SHA1_METHOD_MARKER_SIZE: usize = 1;
    const SHA1_METHOD_VALUE_SIZE: usize = 9;
    const SHA1_PASSWORD_MARKER_SIZE: usize = 1;
    const SHA1_PASSWORD_VALUE_SIZE: usize = 20;
    const SHA1_PACKET_SIZE: usize = PACKET_MARKER_SIZE
        + SHA1_METHOD_MARKER_SIZE
        + SHA1_METHOD_VALUE_SIZE
        + SHA1_PASSWORD_MARKER_SIZE
        + SHA1_PASSWORD_VALUE_SIZE; // = 32 (total, always)

    const LDAP_METHOD_MARKER_SIZE: usize = 1;
    const LDAP_METHOD_VALUE_SIZE: usize = 4;
    const LDAP_PASSWORD_MARKER_SIZE: usize = 1; // cleartext with zero length
    const LDAP_PASSWORD_VALUE_SIZE: usize = 0; // cleartext with zero length
    const LDAP_PACKET_SIZE: usize = PACKET_MARKER_SIZE
        + LDAP_METHOD_MARKER_SIZE
        + LDAP_METHOD_VALUE_SIZE
        + LDAP_PASSWORD_MARKER_SIZE
        + LDAP_PASSWORD_VALUE_SIZE; // = 7 (total, minimum)

    //
    // preparations
    //

    let method_repr = method.as_str().as_bytes();
    let method_length = method.len();
    let password_length = password.len();

    let packet_size = match method {
        AuthMethod::Md5 => MD5_PACKET_SIZE,
        AuthMethod::ChapSha1 => SHA1_PACKET_SIZE,
        AuthMethod::Ldap => LDAP_PACKET_SIZE,
    };
    let mut result = Vec::with_capacity(packet_size);

    //
    // encoding
    //

    rmp::encode::write_array_len(&mut result, PACKET_PARTS_AMOUNT)
        .expect("writing to vector should not have failed");

    rmp::encode::write_str_len(&mut result, method_length as _)
        .expect("writing to vector should not have failed");
    result.extend_from_slice(method_repr);

    rmp::encode::write_str_len(&mut result, password_length as _)
        .expect("writing to vector should not have failed");
    result.extend_from_slice(password);

    #[cfg(debug_assertions)]
    match method {
        AuthMethod::Md5 => assert!(result.len() >= MD5_PACKET_SIZE),
        AuthMethod::ChapSha1 => assert!(result.len() >= SHA1_PACKET_SIZE),
        AuthMethod::Ldap => assert!(result.len() >= LDAP_PACKET_SIZE),
    }

    result
}

/// # Description
///
/// Retrieves the authentication method for a given user
/// via looking into `_pico_user` using `admin`'s session.
/// Returns `None` if the specified user does not exist.
///
/// # Panics
///
/// - Global Raft node is not initialized.
/// - Authentication data is not set for the specified user.
/// - Session of `admin` user is closed.
/// - User `admin` is not found.
/// - User `admin` does not have enough permissions.
/// - Internal error on access to underlying Tarantool space of `_pico_user`.
fn determine_auth_method(username: &str) -> Option<AuthMethod> {
    let node = crate::traft::node::global().expect("node should be initialized");

    // running from admin (as su) helps us avoid insufficient
    // permissions error on reading from `_pico_user` system table
    tarantool::session::with_su(crate::schema::ADMIN_ID, || {
        match node.storage.users.by_name(username) {
            Ok(Some(user_definition)) => Some(user_definition),
            Ok(None) => None, // specified user does not exist
            Err(e) => unreachable!("{e}"),
        }
    })
    .expect("admin user should always exist, it's session should always be open and have enough permissions")
    .map(|auth_data| {
        auth_data
            .auth
            .expect("user authentication data should have been set")
            .method
    })
}

/// See [`authenticate`] for more info.
pub(crate) fn authenticate_inner(
    user_name: &str,
    user_pass: impl AsRef<[u8]>,
    auth_salt: [u8; 20],
    auth_method: AuthMethod,
) -> Result<(), Error> {
    let auth_info = build_auth_info(user_pass.as_ref(), auth_method);
    let auth_info_ptr = auth_info.as_ptr();

    let user_name_ptr = user_name.as_ptr();
    let user_name_len = user_name.len() as u32;

    let auth_salt_ptr = auth_salt.as_ptr();

    // SAFETY:
    // - pointers have valid values
    // - pointers are not null
    // - values are not null
    // - salt has valid size
    // - username has valid size
    let ret =
        unsafe { authenticate_raw(user_name_ptr, user_name_len, auth_salt_ptr, auth_info_ptr) };
    if ret == 0 {
        Ok(())
    } else {
        Err(Error::UserNotFoundOrInvalidCreds(user_name.to_owned()))
    }
}

/// # Description
///
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
/// # Panics
///
/// - Global Raft node is not initialized.
/// - Authentication data is not set for the specified user.
/// - Session of `admin` user is closed.
/// - User `admin` is not found.
/// - User `admin` does not have enough permissions.
/// - Internal error on access to underlying Tarantool space of `_pico_user`.
///
/// # Internals
///
/// DO NOT DEMANGLE IT! It uses the FFI method in it's
/// implementation with the same name, but on the Rust
/// side it is renamed after the linking.
pub(crate) fn authenticate(
    user_name: &str,
    user_pass: impl AsRef<[u8]>,
    auth_salt: Option<[u8; 4]>,
) -> Result<(), Error> {
    const USER_NAME_MAX_LEN: usize = u32::MAX as _;

    let user_not_found_or_invalid_creds =
        || Err(Error::UserNotFoundOrInvalidCreds(user_name.into()));
    let user_name_is_too_big = || Err(Error::UsernameIsTooBig(user_name.into()));

    //
    // validation
    //

    let Some(method) = determine_auth_method(user_name) else {
        user_not_found_or_invalid_creds()?
    };

    let user_name_len = user_name.len();
    if user_name_len > USER_NAME_MAX_LEN {
        return user_name_is_too_big();
    }

    //
    // implementation
    //

    let salt_val = auth_salt.unwrap_or_else(rand::random);
    let mut salt_buf = [0u8; SALT_MIN_LEN];
    salt_buf[0..4].copy_from_slice(&salt_val);

    let user_pass = match method {
        AuthMethod::ChapSha1 => chap_sha1_prepare(user_pass, &salt_buf),
        AuthMethod::Md5 => md5_prepare(user_name, user_pass, salt_val),
        AuthMethod::Ldap => ldap_prepare(user_pass),
    };
    authenticate_inner(user_name, user_pass, salt_buf, method)
}
