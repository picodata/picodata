use crate::pgproto::error::{AuthError, PgError};
use crate::pgproto::stream::{FeMessage, PgStream};
use crate::pgproto::{error::PgResult, messages};
use crate::sasl::{Mechanism, Step};
use crate::scram::ServerSecret;
use crate::storage::Catalog;
use crate::tlog;
use pgwire::messages::startup::PasswordMessageFamily;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::io;
use tarantool::auth::{AuthDef, AuthMethod};
use tarantool::error::BoxError;

/// Extract the exact reason from [`BoxError`] depending on the auth method.
fn explain_box_error(method: AuthMethod) -> Option<SmolStr> {
    if method == AuthMethod::Ldap {
        let error = BoxError::maybe_last()
            .err()
            .map(|s| s.message().to_smolstr())
            .unwrap_or_else(|| format_smolstr!("unknown error"));

        return Some(format_smolstr!("LDAP: {error}"));
    }

    None
}

fn read_password_message(
    stream: &mut PgStream<impl io::Read + io::Write>,
) -> PgResult<PasswordMessageFamily> {
    let message = stream.read_message()?;
    let FeMessage::PasswordMessageFamily(message) = message else {
        return Err(io::Error::other(format!("unexpected message {message:?}")).into());
    };

    Ok(message)
}

fn auth_exchange_classic(
    stream: &mut PgStream<impl io::Read + io::Write>,
    user: &str,
    auth: &AuthDef,
    salt: [u8; 4],
) -> PgResult<()> {
    stream.write_message(match auth.method {
        AuthMethod::Md5 => messages::md5_auth_request(&salt),
        AuthMethod::Ldap => messages::cleartext_auth_request(),
        AuthMethod::ChapSha1 => {
            tlog!(
                Warning,
                "user {user} attempted to login using chap-sha1 which is unsupported in pgproto"
            );

            // We cannot return a more specific error message because
            // it'll allow an attacker to brute force user names.
            return Err(AuthError::for_username(user).into());
        }
        AuthMethod::ScramSha256 => {
            unreachable!("auth_exchange cannot handle scram-sha256");
        }
    })?;

    let password = read_password_message(stream)?
        .into_password()
        .map(|x| x.password)
        .map_err(io::Error::other)?;

    let mut salt_buf = [0u8; crate::auth::SALT_MIN_LEN];
    salt_buf[0..4].copy_from_slice(&salt);

    crate::auth::do_authenticate(user, password, &salt_buf, auth.method)
        // Raise a proper auth error with an explanation as needed.
        .map_err(|_| AuthError {
            user: user.into(),
            extra: explain_box_error(auth.method),
        })?;

    stream.write_message_noflush(messages::auth_ok())?;

    Ok(())
}

fn read_sasl_initial(
    stream: &mut PgStream<impl io::Read + io::Write>,
    user: &str,
) -> PgResult<bytes::Bytes> {
    let sasl_initial = read_password_message(stream)?
        .into_sasl_initial_response()
        .map_err(io::Error::other)?;

    let method = sasl_initial.auth_method;
    if !crate::scram::METHODS_WITHOUT_PLUS.contains(&&*method) {
        return Err(PgError::AuthError(AuthError {
            user: user.into(),
            extra: Some(format_smolstr!("unsupported auth method: {method}")),
        }));
    }

    let Some(bytes) = sasl_initial.data else {
        return Err(io::Error::other("missing client-initial-message").into());
    };

    Ok(bytes)
}

fn do_auth_exchange_sasl(
    stream: &mut PgStream<impl io::Read + io::Write>,
    user: &str,
    secret: &ServerSecret,
) -> PgResult<()> {
    stream.write_message(messages::sasl_auth_request())?;
    let mut input_bytes = read_sasl_initial(stream, user)?;

    let mut method = crate::scram::Exchange::new(
        secret,
        rand::random,
        // TODO: provide server certificate hash for channel binding.
        crate::scram::TlsServerEndpoint::Undefined,
    );

    loop {
        let input = std::str::from_utf8(&input_bytes).map_err(io::Error::other)?;
        match method.exchange(input) {
            Ok(Step::Continue(moved_method, reply)) => {
                method = moved_method;
                stream.write_message(messages::sasl_continue(reply))?;
            }
            Ok(Step::Success(_, reply)) => {
                stream.write_message_noflush(messages::sasl_final(reply))?;
                stream.write_message(messages::auth_ok())?;
                return Ok(());
            }
            Ok(Step::Failure) => {
                return Err(AuthError::for_username(user).into());
            }
            Err(error) => {
                return Err(error.into());
            }
        }

        let msg = read_password_message(stream)?
            .into_sasl_response()
            .map_err(io::Error::other)?;

        input_bytes = msg.data;
    }
}

fn auth_exchange_sasl(
    stream: &mut PgStream<impl io::Read + io::Write>,
    user: &str,
    secret: &ServerSecret,
) -> PgResult<()> {
    // XXX: Use tarantool's wrapper which will perform additional
    // checks, execute triggers and update the credentials.
    let main_res = crate::auth::authenticate_ext(user, || {
        // Now do the exchange itself and return the result.
        do_auth_exchange_sasl(stream, user, secret)
    });

    // The 2nd error is from `authenticate_ext` itself.
    // We have to map it in order to not leak any details.
    main_res?.map_err(|_| AuthError::for_username(user))?;

    Ok(())
}

/// Perform exchange of authentication messages and authentication.
/// Authentication failure is treated as an error.
pub fn authenticate(
    stream: &mut PgStream<impl io::Read + io::Write>,
    user: &str,
    storage: &Catalog,
) -> PgResult<()> {
    // Do not allow attackers to detect which users exist through returned error.
    // Futhermore, do not throw any errors too early -- we can use scram's mock
    // secret to make "missing user" (almost) indistinguishable from "bad password"
    // from the standpoint of timings (at least for scram).
    let maybe_auth = crate::auth::try_get_auth_def(storage, user);
    match maybe_auth {
        Some(auth) if auth.method == AuthMethod::ScramSha256 => {
            let secret = ServerSecret::parse(&auth.data).expect("invalid AuthDef in catalog");
            auth_exchange_sasl(stream, user, &secret)?;
        }
        // TODO: maybe this protection should be optional.
        // If that's the case, add `if <condition>` to this branch.
        None => {
            // Use a mock to prevent timing attacks against the branch above.
            let secret = ServerSecret::mock(&rand::random());
            auth_exchange_sasl(stream, user, &secret)?;
        }
        Some(auth) => {
            let salt = rand::random();
            auth_exchange_classic(stream, user, &auth, salt)?;
        }
    }

    Ok(())
}
