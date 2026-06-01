use crate::auth::msgpack_helpers::StringOrBytes;
use crate::auth::tarantool::AuthenticationVerdict;
use crate::auth::tarantool::SALT_LEN;
use crate::auth::typed_method::{AuthMethod, Authenticator};
use digest::Digest as _;
use md5::Md5;
use std::ffi::CStr;
use std::io::Write as _;
use tarantool::error::{BoxError, TarantoolErrorCode};

fn err_invalid_data(message: impl Into<String>) -> tarantool::error::Error {
    BoxError::new(TarantoolErrorCode::InvalidAuthData, message).into()
}
fn err_invalid_request(message: impl Into<String>) -> tarantool::error::Error {
    BoxError::new(TarantoolErrorCode::InvalidAuthRequest, message).into()
}

/// Length of MD5 hash, when it's represented as a hexadecimal string.
const MD5_HASH_HEX_LEN: usize = 32;

/// Length of tarantool `md5` auth data.
///
/// Tarantool `md5` auth data is an MD5 hash prefixed with literal "md5".
const MD5_PASSWD_LEN: usize = "md5".len() + MD5_HASH_HEX_LEN;

/// Length of salt used in `md5` auth method.
const MD5_SALT_LEN: usize = 4;

pub struct Md5AuthMethod;

// SAFETY: there are no usages of tarantool box region in any of the methods
unsafe impl AuthMethod for Md5AuthMethod {
    const NAME: &'static CStr = c"md5";
    type Authenticator = Md5Authenticator;

    type AuthDataOut<'a> = String;
    type AuthDataIn<'a> = &'a str;

    type AuthRequestOut<'a> = String;
    type AuthRequestIn<'a> = StringOrBytes<'a>;

    type CheckedAuthRequest<'a> = &'a [u8; MD5_PASSWD_LEN];

    fn auth_data_prepare(&self, password: &[u8], user: &[u8]) -> String {
        let mut hasher = Md5::new();
        hasher.update(password);
        hasher.update(user);

        // Note: unfortunately, currently there's no easy way to precompute length,
        // so we have to allocate and format a string object. However, we cannot return
        // it from this method, since Tarantool expects to see a region allocation.
        let shadow_pass = format!("md5{:x}", &hasher.finalize_reset());

        shadow_pass
    }

    fn auth_request_prepare(&self, password: &[u8], user: &[u8], salt: &[u8; SALT_LEN]) -> String {
        // md5 only uses first 4 bytes of the salt
        let salt = &salt[..MD5_SALT_LEN];

        // FIXME: this is duplicating the algorithm also implemented in tarantool module
        // (`tarantool::network::protocol::codec::md5_prepare`)
        let mut hasher = Md5::new();
        hasher.update(password);
        hasher.update(user);
        let shadow_pass = hasher.finalize_reset();

        write!(hasher, "{:x}", shadow_pass).unwrap();
        hasher.update(salt);
        let client_pass = format!("md5{:x}", hasher.finalize());

        client_pass
    }

    fn auth_request_check<'a>(
        &self,
        auth_request: Self::AuthRequestIn<'a>,
    ) -> tarantool::Result<Self::CheckedAuthRequest<'a>> {
        let client_pass = auth_request.0;

        // check the length of the passed request string
        client_pass
            .try_into()
            .map_err(|_| err_invalid_request("md5 auth request has invalid length"))
    }

    fn authenticator_new(&self, auth_data: &str) -> tarantool::Result<Self::Authenticator> {
        let shadow_pass = auth_data;

        let Some(shadow_pass) = shadow_pass.strip_prefix("md5") else {
            return Err(err_invalid_data(
                "md5 auth data did not have the `md5` prefix",
            ));
        };

        let Ok(shadow_pass) = <&[u8; _]>::try_from(shadow_pass.as_bytes()) else {
            return Err(err_invalid_data("md5 auth data has invalid length"));
        };

        Ok(Md5Authenticator {
            shadow_pass: *shadow_pass,
        })
    }
}

pub struct Md5Authenticator {
    /// Stores `md5(user, pass)` for the current user as hex. The `"md5"` prefix is stripped
    shadow_pass: [u8; MD5_HASH_HEX_LEN],
}

impl Authenticator for Md5Authenticator {
    type Method = Md5AuthMethod;
    fn authenticate_request(
        &self,
        _user: &[u8],
        salt: &[u8; SALT_LEN],
        auth_request: &[u8; MD5_PASSWD_LEN],
    ) -> tarantool::Result<AuthenticationVerdict> {
        // md5 only uses first 4 bytes of the salt
        let salt = &salt[..MD5_SALT_LEN];

        let mut hasher = Md5::new();
        hasher.update(self.shadow_pass);
        hasher.update(salt);
        let candidate = format!("md5{:x}", hasher.finalize());

        Ok(if auth_request == candidate.as_bytes() {
            AuthenticationVerdict::Authenticated
        } else {
            AuthenticationVerdict::Rejected
        })
    }
}

mod tests {
    use super::Md5AuthMethod;
    use crate::auth::{
        tarantool::RawAuthMethod as _, typed_method::TypedAuthMethodWrapper, SALT_LEN,
    };
    use crate::tarantool::box_region::BoxRegionCheckpoint;
    use digest::Digest as _;

    // `auth_data_prepare`'s contract requires returning values in box region,
    //  which in turn requires tarantool runtime.
    // Hence, we need to use `tarantool::test`
    #[tarantool::test]
    fn auth_data_prepare() {
        let auth_method = TypedAuthMethodWrapper(Md5AuthMethod);

        let checkpoint = BoxRegionCheckpoint::new();

        // make sure that objects allocated with the box region allocator don't escape this block
        {
            let result = unsafe { auth_method.auth_data_prepare(b"secret42", b"alice") };
            let result = unsafe { result.as_ref() };

            assert_eq!(result, b"\xd9\x23md5ec986c3d9c9e938d9f1b5bb462290ae8");

            assert_eq!(
                format!("{:x}", md5::Md5::digest("secret42alice")),
                "ec986c3d9c9e938d9f1b5bb462290ae8"
            );
        }

        unsafe { checkpoint.restore() };
    }

    // `auth_request_prepare`'s contract requires returning values in box region,
    //  which in turn requires tarantool runtime.
    // Hence, we need to use `tarantool::test`
    #[tarantool::test]
    fn auth_request_prepare() {
        let auth_method = TypedAuthMethodWrapper(Md5AuthMethod);

        let checkpoint = BoxRegionCheckpoint::new();

        // make sure that objects allocated with the box region allocator don't escape this block
        {
            let salt_md5 = [42, 43, 44, 45];
            let mut salt_padded = [0; SALT_LEN];
            salt_padded[0..4].copy_from_slice(&salt_md5);

            let result =
                unsafe { auth_method.auth_request_prepare(b"secret42", b"alice", &salt_padded) };
            let result = unsafe { result.as_ref() };

            assert_eq!(result, b"\xd9\x23md53cc56766dd758d387f924d0924a779fd");

            // check that the logic duplicated logic in tarantool-module agrees with us
            let tarantool_module_md5_prepare =
                tarantool::network::protocol::md5_prepare("alice", "secret42", &salt_md5);
            assert_eq!(
                &tarantool_module_md5_prepare,
                b"md53cc56766dd758d387f924d0924a779fd"
            );
        }

        unsafe { checkpoint.restore() };
    }

    #[test]
    fn auth_request_check_good() {
        let auth_method = TypedAuthMethodWrapper(Md5AuthMethod);

        auth_method
            .auth_request_check(b"\xd9\x23md53cc56766dd758d387f924d0924a779fd")
            .unwrap();
    }

    #[test]
    fn auth_request_check_bad() {
        let auth_method = TypedAuthMethodWrapper(Md5AuthMethod);

        let err = auth_method
            // NB: the length is one short of what it should be
            .auth_request_check(b"\xd9\x22md53cc56766dd758d387f924d0924a779f")
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "box error: InvalidAuthRequest: md5 auth request has invalid length"
        );
    }

    #[test]
    fn authenticate_request_good() {
        use crate::auth::tarantool::{AuthenticationVerdict, RawAuthenticator as _};

        let auth_method = TypedAuthMethodWrapper(Md5AuthMethod);

        let salt_md5 = [42, 43, 44, 45];
        let mut salt_padded = [0; SALT_LEN];
        salt_padded[0..4].copy_from_slice(&salt_md5);

        let auth_data = b"\xd9\x23md5ec986c3d9c9e938d9f1b5bb462290ae8";
        let auth_request = b"\xd9\x23md53cc56766dd758d387f924d0924a779fd";

        let authenticator = auth_method.authenticator_new(auth_data).unwrap();

        let verdict = authenticator
            .authenticate_request(&auth_method, b"alice", &salt_padded, auth_request)
            .unwrap();

        assert_eq!(verdict, AuthenticationVerdict::Authenticated);
    }

    #[test]
    fn authenticate_request_bad() {
        use crate::auth::tarantool::{AuthenticationVerdict, RawAuthenticator as _};

        let auth_method = TypedAuthMethodWrapper(Md5AuthMethod);

        let salt_md5 = [42, 43, 44, 45];
        let mut salt_padded = [0; SALT_LEN];
        salt_padded[0..4].copy_from_slice(&salt_md5);

        let auth_data = b"\xd9\x23md5ec986c3d9c9e938d9f1b5bb462290ae8";
        // NB: the last nibble has `d` changed to `e`, making the request not match the auth data
        let auth_request = b"\xd9\x23md53cc56766dd758d387f924d0924a779fe";

        let authenticator = auth_method.authenticator_new(auth_data).unwrap();

        let verdict = authenticator
            .authenticate_request(&auth_method, b"alice", &salt_padded, auth_request)
            .unwrap();

        assert_eq!(verdict, AuthenticationVerdict::Rejected);
    }
}
