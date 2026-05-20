mod methods;
mod msgpack_helpers;
mod tarantool;
mod tests;
mod typed_method;

use crate::{scram, storage::Catalog};
use ::tarantool::auth::{AuthDef, AuthMethod};
use ::tarantool::error::{BoxError, TarantoolErrorCode};
use ::tarantool::network::protocol::codec::{chap_sha1_prepare, ldap_prepare, md5_prepare};

pub(crate) use self::tarantool::{authenticate_ext, do_authenticate, SALT_LEN};

/// Try fetching user auth info ([`AuthDef`]) from system catalog.
/// **Important:** whenever possible, failure to find such info should
/// nonetheless result in a fake authentication attempt to prevent
/// user probing. This is exactly what we should do for SCRAM.
///
/// Please make sure to understand this before editing this function
/// or its call sites in pgproto or elsewhere.
pub fn try_get_auth_def(storage: &Catalog, user: &str) -> Option<AuthDef> {
    let user_def =
        ::tarantool::session::with_su(crate::schema::ADMIN_ID, || storage.users.by_name(user))
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

/// A specialized auth flow for SCRAM only.
/// Still, it uses tarantool's API to run auth triggers.
fn do_authenticate_scram(
    user: &str,
    password: impl AsRef<[u8]>,
    secret: &scram::ServerSecret,
) -> Result<(), BoxError> {
    // XXX: Use tarantool's wrapper which will perform additional
    // checks, execute triggers and update the credentials.
    authenticate_ext(user, || {
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
    // `authenticate_with_password` is designed for authentication use-cases when we already have
    // user's login and password on hand. There is no point in using a random salt in this case,
    // unlike tarantool's iproto authentication that the framework was designed with in mind.
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

/// Call tarantool to register auth methods that are implemented in Rust.
fn register_picodata_auth_methods() {
    let scram = typed_method::TypedAuthMethodWrapper(methods::scram::ScramAuthMethod);
    let md5 = typed_method::TypedAuthMethodWrapper(methods::md5::Md5AuthMethod);

    tarantool::auth_method_register(scram);
    tarantool::auth_method_register(md5);
}
