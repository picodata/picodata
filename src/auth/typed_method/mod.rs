//! Provides typed counterparts to [`RawAuthMethod`] and [`RawAuthenticator`] traits.
//!
//! Instead of accepting raw msgpack bytes, they accept deserialized messages.

use self::box_region_writer::BoxRegionWriter;
use crate::auth::tarantool::SALT_LEN;
use crate::auth::tarantool::{AuthenticationVerdict, RawAuthMethod, RawAuthenticator};
use std::ffi;
use std::ptr::NonNull;

mod box_region_writer;

/// A tarantool authentication method, typed variant.
///
/// # Safety
///
/// Do not use tarantool [box region allocator](crate::tarantool::box_region) in any of the methods.
pub unsafe trait AuthMethod {
    /// Auth method name. This is the name to use for the method's registration with tarantool.
    const NAME: &'static ffi::CStr;

    /// Set if the authentication method does not need a password in [`AuthMethod::auth_data_prepare`].
    ///
    /// Tarantool will pass an empty slice for `password` if this flag is set to true.
    ///
    /// This is an opt-in because most methods require password.
    const PASSWORDLESS_DATA_PREPARE: bool = false;

    type Authenticator: Authenticator<Method = Self>;

    /// Server-side user authentication data, OUT representation.
    type AuthDataOut<'a>: serde::Serialize;
    /// Server-side user authentication data, IN representation.
    type AuthDataIn<'a>: serde::Deserialize<'a>;

    /// An auth request sent by the user, OUT representation.
    type AuthRequestOut<'a>: serde::Serialize;
    /// An auth request sent by the user, IN representation.
    type AuthRequestIn<'a>: serde::Deserialize<'a>;

    /// An auth request that has been checked for validity (and possibly transformed).
    type CheckedAuthRequest<'a>;

    /// Prepare server-side auth data for the auth method.
    /// This method is called when creating a new user to save its login information.
    fn auth_data_prepare<'a>(&self, password: &'a [u8], user: &'a [u8]) -> Self::AuthDataOut<'a>;

    /// Prepare auth request for the auth method.
    /// This method is called when tarantool needs to authenticate with another instance over iproto.
    ///
    /// The salt is a random value provided by the server to prevent replay attacks.
    fn auth_request_prepare<'a>(
        &self,
        password: &'a [u8],
        user: &'a [u8],
        salt: &'a [u8; SALT_LEN],
    ) -> Self::AuthRequestOut<'a>;

    /// Check a received auth request for validity.
    /// The method should not attempt to serve the auth request yet, just to check its format.
    ///
    /// It can transform the auth request for easier processing by the [`Authenticator`].
    fn auth_request_check<'a>(
        &self,
        auth_request: Self::AuthRequestIn<'a>,
    ) -> tarantool::Result<Self::CheckedAuthRequest<'a>>;

    /// Create an authenticator object for a certain user from their auth data.
    ///
    /// The authenticator can process the auth data into an efficient representation
    /// and then serve multiple auth requests for the same user.
    fn authenticator_new(
        &self,
        auth_data: Self::AuthDataIn<'_>,
    ) -> tarantool::Result<Self::Authenticator>;
}

pub trait Authenticator {
    type Method: AuthMethod;

    /// Process an authentication request received from a user.
    ///
    /// The request is guaranteed to already be validated by [`AuthMethod::auth_request_check`].
    ///
    /// The authenticator should return an [`AuthenticationVerdict`] if it can determine that the
    /// auth request is valid or not. It can return an error if it can't validate the request for
    /// some reason.
    ///
    /// Beware! If this function returns an error, the error message will be sent to the client via
    /// the iproto protocol, so do not include sensitive information there.
    fn authenticate_request(
        &self,
        user: &[u8],
        salt: &[u8; SALT_LEN],
        auth_request: <Self::Method as AuthMethod>::CheckedAuthRequest<'_>,
    ) -> tarantool::Result<AuthenticationVerdict>;
}

/// Initial size of the buffer allocated from the box region allocator for return values.
const INITIAL_MP_RET_BUFFER_SIZE: usize = 32;

pub struct TypedAuthMethodWrapper<T>(pub T);

impl<M: AuthMethod> RawAuthMethod for TypedAuthMethodWrapper<M> {
    const NAME: &'static ffi::CStr = M::NAME;
    const PASSWORDLESS_DATA_PREPARE: bool = M::PASSWORDLESS_DATA_PREPARE;
    type Authenticator = TypedAuthenticatorWrapper<M>;

    unsafe fn auth_data_prepare(&self, password: &[u8], user: &[u8]) -> NonNull<[u8]> {
        let auth_data = self.0.auth_data_prepare(password, user);

        let mut writer = unsafe { BoxRegionWriter::with_capacity(INITIAL_MP_RET_BUFFER_SIZE) };

        rmp_serde::encode::write(&mut writer, &auth_data).expect("Serializing auth data failed");

        writer.into_filled_slice()
    }

    unsafe fn auth_request_prepare(
        &self,
        password: &[u8],
        user: &[u8],
        salt: &[u8; SALT_LEN],
    ) -> NonNull<[u8]> {
        let auth_request = self.0.auth_request_prepare(password, user, salt);

        let mut writer = unsafe { BoxRegionWriter::with_capacity(INITIAL_MP_RET_BUFFER_SIZE) };

        rmp_serde::encode::write(&mut writer, &auth_request)
            .expect("Serializing auth request failed");

        writer.into_filled_slice()
    }

    fn auth_request_check(&self, auth_request: &[u8]) -> tarantool::Result<()> {
        let auth_request = rmp_serde::from_slice::<M::AuthRequestIn<'_>>(auth_request)?;

        // ignore the checked result type for now
        self.0.auth_request_check(auth_request).map(|_| ())
    }

    fn authenticator_new(&self, auth_data: &[u8]) -> tarantool::Result<Self::Authenticator> {
        let auth_data = rmp_serde::from_slice::<M::AuthDataIn<'_>>(auth_data)?;

        let authenticator = self.0.authenticator_new(auth_data)?;

        Ok(TypedAuthenticatorWrapper(authenticator))
    }
}

pub struct TypedAuthenticatorWrapper<M: AuthMethod>(pub M::Authenticator);

impl<M: AuthMethod> RawAuthenticator for TypedAuthenticatorWrapper<M> {
    type Method = TypedAuthMethodWrapper<M>;

    fn authenticate_request(
        &self,
        method: &Self::Method,
        user: &[u8],
        salt: &[u8; SALT_LEN],
        auth_request: &[u8],
    ) -> tarantool::Result<AuthenticationVerdict> {
        // This should not fail.
        // We have already parsed the auth request in `auth_request_check`,
        // and tarantool will only call `authenticate_request` if that succeeds
        let auth_request = rmp_serde::from_slice::<M::AuthRequestIn<'_>>(auth_request).unwrap();

        // The implemented check should not fail either.
        let check = method.0.auth_request_check(auth_request).unwrap();

        self.0.authenticate_request(user, salt, check)
    }
}
