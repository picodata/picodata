use crate::auth::tarantool::AuthenticationVerdict;
use crate::auth::typed_method::{AuthMethod, Authenticator};
use crate::auth::SALT_LEN;
use std::ffi::CStr;
use tarantool::error::{BoxError, TarantoolErrorCode};

/// The `cert` authentication method.
///
/// The actual certificate check never happens here: for iproto it's done by
/// tarantool itself (see `try_authenticate_via_certificate` in authentication.c),
/// while pgproto compares the certificate's user name in `auth_exchange_cert`.
/// Both take a shortcut and never call into this method if the check succeeded,
/// so this impl only exists to store `cert` in the catalog and to reject the
/// connections which didn't present a suitable client certificate.
pub struct CertAuthMethod;

// SAFETY: there are no usages of tarantool box region in any of the methods
unsafe impl AuthMethod for CertAuthMethod {
    const NAME: &'static CStr = c"cert";
    const PASSWORDLESS_DATA_PREPARE: bool = true;
    type Authenticator = CertAuthenticator;

    type AuthDataOut<'a> = &'static str;
    type AuthDataIn<'a> = &'a str;

    type AuthRequestOut<'a> = &'static str;
    type AuthRequestIn<'a> = &'a str;

    type CheckedAuthRequest<'a> = ();

    fn auth_data_prepare<'a>(&self, _password: &'a [u8], _user: &'a [u8]) -> Self::AuthDataOut<'a> {
        "" // No data needed
    }

    fn auth_request_prepare<'a>(
        &self,
        _password: &'a [u8],
        _user: &'a [u8],
        _salt: &'a [u8; SALT_LEN],
    ) -> Self::AuthRequestOut<'a> {
        "" // No data needed
    }

    fn auth_request_check<'a>(
        &self,
        _auth_request: Self::AuthRequestIn<'a>,
    ) -> tarantool::Result<Self::CheckedAuthRequest<'a>> {
        Ok(())
    }

    fn authenticator_new(
        &self,
        _auth_data: Self::AuthDataIn<'_>,
    ) -> tarantool::Result<Self::Authenticator> {
        Ok(CertAuthenticator)
    }
}

pub struct CertAuthenticator;

impl Authenticator for CertAuthenticator {
    type Method = CertAuthMethod;

    fn authenticate_request(
        &self,
        _user: &[u8],
        _salt: &[u8; SALT_LEN],
        _auth_request: <Self::Method as AuthMethod>::CheckedAuthRequest<'_>,
    ) -> tarantool::Result<AuthenticationVerdict> {
        // We only get here if mTLS check failed or was disabled (no certs provided).
        // Both iproto and pgproto authenticate the user right away once
        // the certificate matches. There's no password to fall back to, either.
        Err(BoxError::new(
            TarantoolErrorCode::PasswordMismatch,
            "client certificate is missing",
        )
        .into())
    }
}
