/*
 * Original work Copyright (c) 2025 Steven Fackler
 * Original work Copyright (c) 2025 Neon, Inc
 * Modified work Copyright (c) 2025 Picodata
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Tools for SCRAM server secret management.

use super::{base64_decode_array, key::ScramKey};
use crate::scram::pbkdf2::Pbkdf2;
use base64::{prelude::BASE64_STANDARD, Engine as _};
use hmac::{
    digest::{Digest as _, FixedOutput as _},
    Hmac, Mac as _,
};
use sha2::Sha256;
use smallvec::SmallVec;
use subtle::{Choice, ConstantTimeEq};

pub const SCRAM_DEFAULT_ITERATIONS: u32 = 4096;
pub const SCRAM_DEFAULT_SALT_LEN: usize = 16;

/// Server secret is produced from user's password,
/// and is used throughout the authentication process.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ServerSecret {
    /// Number of iterations for `PBKDF2` function.
    pub iterations: u32,
    /// Salt used to hash user's password.
    pub salt_base64: Box<str>,
    /// Hashed `ClientKey`.
    pub stored_key: ScramKey,
    /// Used by client to verify server's signature.
    pub server_key: ScramKey,
    /// Should auth fail no matter what?
    /// This is exactly the case for mocked secrets.
    pub doomed: bool,
}

impl ServerSecret {
    pub fn parse(input: &str) -> Option<Self> {
        // SCRAM-SHA-256$<iterations>:<salt>$<storedkey>:<serverkey>
        let s = input.strip_prefix("SCRAM-SHA-256$")?;
        let (params, keys) = s.split_once('$')?;

        let ((iterations, salt), (stored_key, server_key)) =
            params.split_once(':').zip(keys.split_once(':'))?;

        let secret = ServerSecret {
            iterations: iterations.parse().ok()?,
            salt_base64: salt.into(),
            stored_key: base64_decode_array(stored_key)?.into(),
            server_key: base64_decode_array(server_key)?.into(),
            doomed: false,
        };

        Some(secret)
    }

    pub fn is_password_invalid(&self, client_key: &ScramKey) -> Choice {
        // constant time to not leak partial key match
        client_key.sha256().ct_ne(&self.stored_key) | Choice::from(self.doomed as u8)
    }

    /// To avoid revealing information to an attacker, we use a
    /// mocked server secret even if the user doesn't exist.
    /// See `auth-scram.c : mock_scram_secret` for details.
    pub fn mock(nonce: [u8; 32]) -> Self {
        Self {
            // We use the default number of iterations (4096),
            // otherwise clients like usql fail with strange errors.
            iterations: SCRAM_DEFAULT_ITERATIONS,
            salt_base64: BASE64_STANDARD.encode(nonce).into_boxed_str(),
            stored_key: ScramKey::default(),
            server_key: ScramKey::default(),
            doomed: true,
        }
    }

    pub fn generate(password: &[u8], salt: &[u8], iterations: u32) -> Self {
        // Prepare the password, per [RFC 4013](https://tools.ietf.org/html/rfc4013),
        // if possible.
        //
        // Postgres treats passwords as byte strings (without embedded NUL
        // bytes), but SASL expects passwords to be valid UTF-8.
        //
        // Follow the behavior of libpq's PQencryptPasswordConn(), and
        // also the backend. If the password is not valid UTF-8, or if it
        // contains prohibited characters (such as non-ASCII whitespace),
        // just skip the SASLprep step and use the original byte
        // sequence.
        let prepared: SmallVec<[u8; 32]> = match std::str::from_utf8(password) {
            Ok(password_str) => {
                match stringprep::saslprep(password_str) {
                    Ok(p) => SmallVec::from(p.as_bytes()),
                    // contains invalid characters; skip saslprep
                    Err(_) => SmallVec::from(password),
                }
            }
            // not valid UTF-8; skip saslprep
            Err(_) => SmallVec::from(password),
        };

        let salted_password = Pbkdf2::compute_no_yield(&prepared, salt, iterations);

        let mut hmac = Hmac::<Sha256>::new_from_slice(&salted_password).unwrap();
        hmac.update(b"Client Key");
        let client_key = hmac.finalize().into_bytes();

        let mut hash = Sha256::default();
        hash.update(client_key.as_slice());
        let stored_key = hash.finalize_fixed();

        let mut hmac = Hmac::<Sha256>::new_from_slice(&salted_password).unwrap();
        hmac.update(b"Server Key");
        let server_key = hmac.finalize().into_bytes();

        Self {
            iterations,
            salt_base64: BASE64_STANDARD.encode(salt).into_boxed_str(),
            stored_key: stored_key.into(),
            server_key: server_key.into(),
            doomed: false,
        }
    }
}

impl std::fmt::Display for ServerSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SCRAM-SHA-256${}:{}${}:{}",
            SCRAM_DEFAULT_ITERATIONS,
            self.salt_base64,
            BASE64_STANDARD.encode(&self.stored_key),
            BASE64_STANDARD.encode(&self.server_key),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_scram_secret() {
        let iterations = 4096;
        let salt = "+/tQQax7twvwTj64mjBsxQ==";
        let stored_key = "D5h6KTMBlUvDJk2Y8ELfC1Sjtc6k9YHjRyuRZyBNJns=";
        let server_key = "Pi3QHbcluX//NDfVkKlFl88GGzlJ5LkyPwcdlN/QBvI=";

        let secret = format!("SCRAM-SHA-256${iterations}:{salt}${stored_key}:{server_key}");

        let parsed = ServerSecret::parse(&secret).unwrap();
        assert_eq!(secret, format!("{parsed}"));

        assert_eq!(parsed.iterations, iterations);
        assert_eq!(&*parsed.salt_base64, salt);

        assert_eq!(BASE64_STANDARD.encode(parsed.stored_key), stored_key);
        assert_eq!(BASE64_STANDARD.encode(parsed.server_key), server_key);
    }
}
