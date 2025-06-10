use crate::auth;
use crate::pgproto::error::{AuthError, PgError};
use crate::pgproto::stream::{FeMessage, PgStream};
use crate::pgproto::{error::PgResult, messages};
use crate::storage::Catalog;
use crate::tlog;
use pgwire::messages::startup::PasswordMessageFamily;
use smol_str::{format_smolstr, ToSmolStr};
use std::io;
use tarantool::auth::AuthMethod;
use tarantool::error::BoxError;

/// Perform authentication, throwing [`PgError::AuthError`] if it failed.
pub fn do_authenticate(
    user: &str,
    salt: [u8; 4],
    client_pass: &str,
    auth_method: AuthMethod,
) -> Result<(), AuthError> {
    let mut auth_salt = [0u8; auth::SALT_MIN_LEN];
    auth_salt[0..4].copy_from_slice(&salt);

    auth::authenticate_inner(user, client_pass, auth_salt, auth_method).map_err(|_| {
        let mut extra = None;
        if auth_method == AuthMethod::Ldap {
            let error = BoxError::maybe_last()
                .err()
                .map(|s| s.message().to_smolstr())
                .unwrap_or_else(|| format_smolstr!("unknown error"));

            extra = Some(format_smolstr!("LDAP: {error}"));
        }

        AuthError {
            user: user.into(),
            extra,
        }
    })
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
