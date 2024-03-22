use crate::pgproto::error::PgError;
use crate::pgproto::stream::{FeMessage, PgStream};
use crate::pgproto::{error::PgResult, helpers, messages};
use pgwire::messages::startup::{Password, PasswordMessageFamily};
use std::{io, mem, os::raw::c_int};

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
fn build_auth_info(client_pass: &str) -> Vec<u8> {
    let auth_packet = ["md5", client_pass];

    // mp_sizeof(["md5", md5-hash]) == 42,
    // but it may vary if we get a non md5 password.
    let mut result = Vec::with_capacity(42);
    rmp_serde::encode::write(&mut result, &auth_packet).unwrap();

    result
}

/// Perform authentication.
/// In case of success true is returned, false otherwise.
pub fn do_authenticate(user: &str, salt: &[u8; 20], client_pass: &str) -> bool {
    let auth_info = build_auth_info(client_pass);
    // SAFETY: pointers must have valid and non-null values, salt must be at least 20 bytes
    let ret = unsafe {
        authenticate_raw(
            user.as_ptr(),
            user.len() as u32,
            salt.as_ptr(),
            auth_info.as_ptr(),
        )
    };
    ret == 0
}

fn extract_password(message: PasswordMessageFamily) -> String {
    let mut password = message
        .into_password() // @todo: replace it with something that doesn't cause panic
        .unwrap_or(Password::new("".to_string()));
    mem::take(password.password_mut())
}

// method authenticate expects that the salt len is not less than 20,
// see box_process_auth from tarantool/src/box.cc.
fn auth_exchage<S>(stream: &mut PgStream<S>, salt: &[u8; 20]) -> PgResult<String>
where
    S: io::Read + io::Write,
{
    // MD5 method requires only 4 bytes for salt.
    let salt = helpers::split_at_const::<4>(salt).unwrap().0;
    stream.write_message(messages::md5_auth_request(salt))?;

    let message = stream.read_message()?;
    if let FeMessage::PasswordMessageFamily(message) = message {
        let password = extract_password(message);
        Ok(password)
    } else {
        Err(PgError::ProtocolViolation(format!(
            "expected Password, got {message:?}"
        )))
    }
}

/// Perform exchange of authentication messages and authentication.
/// Authentication failure is treated as an error.
pub fn authenticate<S>(stream: &mut PgStream<S>, salt: &[u8; 20], username: &str) -> PgResult<()>
where
    S: io::Read + io::Write,
{
    // method authenticate expects that the salt len is not less than 20,
    // see box_process_auth from tarantool/src/box.cc.
    let password = auth_exchage(stream, salt)?;
    if do_authenticate(username, salt, &password) {
        stream.write_message_noflush(messages::auth_ok())?;
        Ok(())
    } else {
        Err(PgError::InvalidPassword(username.to_owned()))
    }
}
