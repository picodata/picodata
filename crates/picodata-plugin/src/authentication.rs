//! Module for authentication related entities.

use crate::internal::ffi::{pico_ffi_authenticate, pico_ffi_validate_jwt};

use abi_stable::std_types::{RResult, RString, RVec};
use abi_stable::StableAbi;
use tarantool::error::BoxError;

/// # Description
///
/// Tries to authenticate a user with specified password.
/// Authentication method is determined via accessing `_pico_user`
/// system table using `admin` session.
///
/// # FFI
///
/// Uses [`pico_ffi_authenticate`].
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
/// - Internal error on accessing underlying Tarantool space of `_pico_user` system table.
pub fn authenticate(username: &str, password: impl AsRef<[u8]>) -> Result<(), BoxError> {
    match unsafe { pico_ffi_authenticate(username.into(), password.as_ref().into()) } {
        0 => Ok(()),
        _ => {
            let error = BoxError::last();
            Err(error)
        }
    }
}

/// Identity of a WebUI user extracted from a validated JWT.
/// See [`validate_jwt`].
#[derive(StableAbi, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct JwtAuthContext {
    username: RString,
    roles: RVec<RString>,
}

impl JwtAuthContext {
    #[doc(hidden)]
    pub fn new(username: String, roles: Vec<String>) -> Self {
        Self {
            username: username.into(),
            roles: roles.into_iter().map(RString::from).collect(),
        }
    }

    /// Name of the user the token was issued to.
    pub fn username(&self) -> &str {
        self.username.as_str()
    }

    /// Names of the privileges granted to the user at the moment
    /// the token was issued (e.g. `login`).
    pub fn roles(&self) -> impl Iterator<Item = &str> {
        self.roles.iter().map(RString::as_str)
    }

    /// Returns `true` if `role` is among [`Self::roles`].
    pub fn has_role(&self, role: &str) -> bool {
        self.roles().any(|r| r == role)
    }
}

/// # Description
///
/// Validates a JWT issued by the picodata WebUI (`POST /api/v1/session`)
/// the signature is checked against the cluster-wide `jwt_secret`,
/// the token must not be expired and must be an `auth` (not `refresh`) token.
///
/// No privileges are required: it's up to the caller to check
/// [`JwtAuthContext::roles`] against whatever the plugin needs.
///
/// `token` may be either the bare token or the whole value of
/// the `Authorization` HTTP header (`Bearer <token>`).
///
/// Returns `Ok(None)` if WebUI authentication is disabled cluster-wide
/// (`jwt_secret` is set to an empty string). It's up to the caller
/// to decide whether the request should be allowed in that case.
///
/// # Example
///
/// ```no_run
/// # use std::collections::HashMap;
/// # let headers: HashMap<String, String> = HashMap::new();
/// use picodata_plugin::authentication::validate_jwt;
///
/// let header = headers.get("authorization").map(String::as_str).unwrap_or_default();
/// match validate_jwt(header) {
///     Ok(Some(ctx)) if ctx.has_role("read") => println!("request from {}", ctx.username()),
///     Ok(Some(ctx)) => println!("forbidden for {}", ctx.username()),
///     Ok(None) => println!("WebUI auth is disabled"),
///     Err(e) => println!("unauthorized: {e}"),
/// }
/// ```
///
/// # FFI
///
/// Uses [`pico_ffi_validate_jwt`].
///
/// # Errors
///
/// - [`ErrorCode::JwtExpired`]: token has expired, the client should refresh its session.
/// - [`ErrorCode::JwtInvalid`]: token is malformed, has an invalid signature
///   or is not an `auth` token.
/// - [`ErrorCode::Other`]: internal error, e.g. picodata is not initialized yet.
///
/// [`ErrorCode::JwtExpired`]: crate::error_code::ErrorCode::JwtExpired
/// [`ErrorCode::JwtInvalid`]: crate::error_code::ErrorCode::JwtInvalid
/// [`ErrorCode::Other`]: crate::error_code::ErrorCode::Other
pub fn validate_jwt(token: &str) -> Result<Option<JwtAuthContext>, BoxError> {
    let token = token.strip_prefix("Bearer ").unwrap_or(token);
    match unsafe { pico_ffi_validate_jwt(token.into()) } {
        RResult::ROk(ctx) => Ok(ctx.into_option()),
        RResult::RErr(()) => Err(BoxError::last()),
    }
}
