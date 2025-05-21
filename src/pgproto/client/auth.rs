use crate::pgproto::error::{AuthError, PgError};
use crate::pgproto::stream::{FeMessage, PgStream};
use crate::pgproto::{error::PgResult, messages};
use crate::storage::Catalog;
use crate::tlog;
use pgwire::messages::startup::PasswordMessageFamily;
use smol_str::{format_smolstr, ToSmolStr};
use std::{io, os::raw::c_int};
use tarantool::auth::AuthMethod;
use tarantool::error::BoxError;

extern "C" {
    /// pointers must have valid and non-null values, salt must be at least 20 bytes
    #[link_name = "authenticate"]
    fn authenticate_raw(
        user: *const u8,
        user_len: u32,
        salt: *const u8,
        auth_info: *const u8,
    ) -> c_int;
}

/// Build auth_info that is used by authenticate_raw.
fn build_auth_info(client_pass: &str, auth_method: &AuthMethod) -> Vec<u8> {
    let auth_packet = [auth_method.as_str(), client_pass];

    // mp_sizeof(["md5", md5-hash]) == 42,
    // but it may vary if we get a non md5 password.
    let mut result = Vec::with_capacity(42);
    rmp_serde::encode::write(&mut result, &auth_packet).unwrap();

    result
}

/// Perform authentication, throwing [`PgError::AuthError`] if it failed.
pub fn do_authenticate(
    user: &str,
    salt: [u8; 4],
    client_pass: &str,
    auth_method: AuthMethod,
) -> Result<(), AuthError> {
    let auth_info = build_auth_info(client_pass, &auth_method);

    // Tarantool requires that the salt array contains no less than 20 bytes!
    let mut extended_salt = [0u8; 20];
    extended_salt[0..4].copy_from_slice(&salt);

    // SAFETY: pointers have valid and non-null values, salt has at least 20 bytes.
    let ret = unsafe {
        authenticate_raw(
            user.as_ptr(),
            user.len() as u32,
            extended_salt.as_ptr(),
            auth_info.as_ptr(),
        )
    };

    match ret {
        0 => Ok(()),
        _ => {
            let mut extra = None;
            if auth_method == AuthMethod::Ldap {
                let error = BoxError::maybe_last()
                    .err()
                    .map(|s| s.message().to_smolstr())
                    .unwrap_or_else(|| format_smolstr!("unknown error"));

                extra = Some(format_smolstr!("LDAP: {error}"));
            }

            Err(AuthError {
                user: user.into(),
                extra,
            })
        }
    }
}

fn extract_password(message: PasswordMessageFamily) -> String {
    message
        .into_password() // TODO: replace it with something that doesn't cause panic
        .map(|x| x.password)
        .unwrap_or_default()
}

fn auth_exchage<S>(
    username: &str,
    stream: &mut PgStream<S>,
    salt: [u8; 4],
    auth_method: AuthMethod,
) -> PgResult<String>
where
    S: io::Read + io::Write,
{
    match auth_method {
        AuthMethod::ChapSha1 => {
            tlog!(Warning, "user {username} attempted to login using chap-sha1 which is unsupported in pgproto");
            // We cannot return a more specific error message because
            // it'll allow an attacker to brute force user names.
            return Err(AuthError::for_username(username).into());
        }
        AuthMethod::Md5 => {
            stream.write_message(messages::md5_auth_request(&salt))?;
        }
        AuthMethod::Ldap => {
            stream.write_message(messages::cleartext_auth_request())?;
        }
    }

    let message = stream.read_message()?;
    let FeMessage::PasswordMessageFamily(message) = message else {
        return Err(PgError::ProtocolViolation(format_smolstr!(
            "expected Password, got {message:?}"
        )));
    };

    Ok(extract_password(message))
}

/// Perform exchange of authentication messages and authentication.
/// Authentication failure is treated as an error.
pub fn authenticate<S>(stream: &mut PgStream<S>, username: &str, storage: &Catalog) -> PgResult<()>
where
    S: io::Read + io::Write,
{
    // Do not allow attackers to detect which users exist through returned error.
    let err = || AuthError::for_username(username);
    let Some(user) = storage.users.by_name(username)? else {
        return Err(err().into());
    };

    let auth = user.auth.ok_or_else(err)?;

    // Note: salt is not used by ldap, but `authenticate_raw` still needs it.
    let salt = rand::random();
    let password = auth_exchage(username, stream, salt, auth.method)?;
    do_authenticate(username, salt, &password, auth.method)?;
    stream.write_message_noflush(messages::auth_ok())?;

    Ok(())
}
