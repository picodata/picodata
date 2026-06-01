use crate::auth::tarantool::AuthenticationVerdict;
use crate::auth::tarantool::SALT_LEN;
use crate::auth::typed_method::{AuthMethod, Authenticator};
use crate::scram::{ServerSecret, SCRAM_DEFAULT_ITERATIONS, SCRAM_DEFAULT_SALT_LEN};
use std::ffi;
use tarantool::error::{BoxError, TarantoolErrorCode};

pub struct ScramAuthMethod;

// SAFETY: there are no usages of tarantool box region in any of the methods
unsafe impl AuthMethod for ScramAuthMethod {
    const NAME: &'static ffi::CStr = c"scram-sha256";
    type Authenticator = ScramAuthenticator;

    type AuthDataOut<'a> = ServerSecret;
    type AuthDataIn<'a> = ServerSecret;

    type AuthRequestOut<'a> = ();
    type AuthRequestIn<'a> = ();

    type CheckedAuthRequest<'a> = ();

    fn auth_data_prepare<'a>(&self, password: &'a [u8], _user: &'a [u8]) -> ServerSecret {
        let salt = rand::random::<[u8; SCRAM_DEFAULT_SALT_LEN]>();

        ServerSecret::generate(password, &salt, SCRAM_DEFAULT_ITERATIONS)
    }

    #[expect(clippy::unused_unit)] // `()` should be explicit here
    fn auth_request_prepare<'a>(
        &self,
        _password: &'a [u8],
        _user: &'a [u8],
        _salt: &'a [u8; SALT_LEN],
    ) -> () {
        // Using scram auth method over iproto is not supported, so generate a dummy auth request.
        ()
    }

    fn auth_request_check<'a>(
        &self,
        auth_request: Self::AuthRequestIn<'a>,
    ) -> tarantool::Result<Self::CheckedAuthRequest<'a>> {
        // Don't fail anything at validation stage.
        // The authenticator will reject everything instead.
        Ok(auth_request)
    }

    fn authenticator_new(
        &self,
        _auth_data: ServerSecret,
    ) -> tarantool::Result<Self::Authenticator> {
        Ok(ScramAuthenticator)
    }
}

pub struct ScramAuthenticator;

impl Authenticator for ScramAuthenticator {
    type Method = ScramAuthMethod;

    fn authenticate_request(
        &self,
        _user: &[u8],
        _salt: &[u8; SALT_LEN],
        _auth_request: (),
    ) -> tarantool::Result<AuthenticationVerdict> {
        Err(BoxError::new(
            TarantoolErrorCode::InvalidAuthRequest,
            "scram over iproto is not supported",
        )
        .into())
    }
}

mod tests {
    use super::ScramAuthMethod;
    use crate::auth::{
        tarantool::RawAuthMethod as _, typed_method::TypedAuthMethodWrapper, SALT_LEN,
    };
    use crate::tarantool::box_region::BoxRegionCheckpoint;

    // `auth_data_prepare`'s contract requires returning values in box region,
    //  which in turn requires tarantool runtime.
    // Hence, we need to use `tarantool::test`
    #[tarantool::test]
    fn auth_data_prepare() {
        let auth_method = TypedAuthMethodWrapper(ScramAuthMethod);

        let checkpoint = BoxRegionCheckpoint::new();

        // make sure that objects allocated with the box region allocator don't escape this block
        {
            let result = unsafe { auth_method.auth_data_prepare(b"secret42", b"alice") };
            let result = unsafe { result.as_ref() };

            // `auth_data_prepare` generates a random nonce every time,
            //  so it's hard to unit test it precisely.
            // Check that it contains the identifier string at least.
            let target_str = b"SCRAM-SHA-256$4096:";

            result
                .windows(target_str.len())
                .any(move |sub_slice| sub_slice == target_str);
        }

        unsafe { checkpoint.restore() };
    }

    // `auth_request_prepare`'s contract requires returning values in box region,
    //  which in turn requires tarantool runtime.
    // Hence, we need to use `tarantool::test`
    #[tarantool::test]
    fn auth_request_prepare() {
        let auth_method = TypedAuthMethodWrapper(ScramAuthMethod);

        let checkpoint = BoxRegionCheckpoint::new();

        // make sure that objects allocated with the box region allocator don't escape this block
        {
            let salt_padded = [0; SALT_LEN];

            let result =
                unsafe { auth_method.auth_request_prepare(b"secret42", b"alice", &salt_padded) };
            let result = unsafe { result.as_ref() };

            // `scram-sha256` doesn't support authorization over iproto,
            //  so `auth_request_prepare` returns `null`.
            // The server will in turn reject all auth attempts.
            assert_eq!(result, b"\xc0");
        }

        unsafe { checkpoint.restore() };
    }

    #[test]
    fn auth_request_check_good() {
        let auth_method = TypedAuthMethodWrapper(ScramAuthMethod);

        auth_method.auth_request_check(b"\xc0").unwrap();
    }

    #[test]
    fn auth_request_check_bad() {
        let auth_method = TypedAuthMethodWrapper(ScramAuthMethod);

        let err = auth_method
            // NB: the auth method expects null, but we pass a string
            .auth_request_check(b"\xd9\x23md53cc56766dd758d387f924d0924a779fd")
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed to decode tuple: invalid type: string \
                \"md53cc56766dd758d387f924d0924a779fd\", expected unit"
        );
    }

    // there's no "good" version because `scram-sha256` will return an error for all auth attempts
    #[test]
    fn authenticate_request_bad() {
        use crate::auth::tarantool::RawAuthenticator as _;

        let auth_method = TypedAuthMethodWrapper(ScramAuthMethod);

        let salt_padded = [0; SALT_LEN];

        let auth_data = b"\xd9\x85SCRAM-SHA-256$4096:QBou+boJS2DTYCAlUW3+gQ==$\
        NoCyIqbHM+4k/esdZPWJ95bGjFNvl4QmC7Z6W6BdOLM=:Iz9ooTKZvV26P7Pso5js3gwOPeGIxxeMPalVppzj9sc=";
        let auth_request = b"\xc0";

        let authenticator = auth_method.authenticator_new(auth_data).unwrap();

        let err = authenticator
            .authenticate_request(&auth_method, b"alice", &salt_padded, auth_request)
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "box error: InvalidAuthRequest: scram over iproto is not supported"
        );
    }
}
