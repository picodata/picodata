/*
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

//! Salted Challenge Response Authentication Mechanism.
//!
//! RFC: <https://datatracker.ietf.org/doc/html/rfc5802>.
//!
//! Reference implementation:
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/backend/libpq/auth-scram.c>
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/interfaces/libpq/fe-auth-scram.c>

mod exchange;
mod key;
mod messages;
mod pbkdf2;
mod secret;
mod signature;
pub mod tarantool;

use base64::prelude::BASE64_STANDARD;
use base64::Engine as _;
pub use exchange::Exchange;
pub use key::ScramKey;
pub use secret::ServerSecret;

const SCRAM_SHA_256: &str = "SCRAM-SHA-256";
const SCRAM_SHA_256_PLUS: &str = "SCRAM-SHA-256-PLUS";

/// A list of methods with channel binding.
#[allow(dead_code)]
pub const METHODS: &[&str] = &[SCRAM_SHA_256_PLUS, SCRAM_SHA_256];

/// A list of methods without channel binding.
pub const METHODS_WITHOUT_PLUS: &[&str] = &[SCRAM_SHA_256];

/// Channel binding parameter
///
/// <https://www.rfc-editor.org/rfc/rfc5929#section-4>
/// Description: The hash of the TLS server's certificate as it
/// appears, octet for octet, in the server's Certificate message.  Note
/// that the Certificate message contains a certificate_list, in which
/// the first element is the server's certificate.
///
/// The hash function is to be selected as follows:
///
/// * if the certificate's signatureAlgorithm uses a single hash
///   function, and that hash function is either MD5 or SHA-1, then use SHA-256;
///
/// * if the certificate's signatureAlgorithm uses a single hash
///   function and that hash function neither MD5 nor SHA-1, then use
///   the hash function associated with the certificate's
///   signatureAlgorithm;
///
/// * if the certificate's signatureAlgorithm uses no hash functions or
///   uses multiple hash functions, then this channel binding type's
///   channel bindings are undefined at this time (updates to is channel
///   binding type may occur to address this issue if it ever arises).
#[derive(Debug, Clone, Copy)]
pub enum TlsServerEndpoint {
    /// Enable channel binding.
    #[allow(dead_code)]
    Sha256([u8; 32]),

    /// Disable channel binding.
    Undefined,
}

impl TlsServerEndpoint {
    fn supported(&self) -> bool {
        !matches!(self, Self::Undefined)
    }
}

/// Decode base64 into array without any heap allocations
fn base64_decode_array<const N: usize>(input: impl AsRef<[u8]>) -> Option<[u8; N]> {
    let mut bytes = [0u8; N];

    let size = BASE64_STANDARD.decode_slice(input, &mut bytes).ok()?;
    if size != N {
        return None;
    }

    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::{Exchange, ServerSecret, TlsServerEndpoint};
    use crate::sasl::{Mechanism, Step};

    #[test]
    fn snapshot() {
        let iterations = 4096;
        let salt = "QSXCR+Q6sek8bf92";
        let stored_key = "FO+9jBb3MUukt6jJnzjPZOWc5ow/Pu6JtPyju0aqaE8=";
        let server_key = "qxJ1SbmSAi5EcS0J5Ck/cKAm/+Ixa+Kwp63f4OHDgzo=";
        let secret = format!("SCRAM-SHA-256${iterations}:{salt}${stored_key}:{server_key}");
        let secret = ServerSecret::parse(&secret).unwrap();

        const NONCE: [u8; 18] = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];
        let mut exchange = Exchange::new(&secret, || NONCE, TlsServerEndpoint::Undefined);

        let client_first = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
        let client_final =
            "c=biws,r=rOprNGfwEbeRWgbNEkqOAQIDBAUGBwgJCgsMDQ4PEBES,p=rw1r5Kph5ThxmaUBC2GAQ6MfXbPnNkFiTIvdb/Rear0=";

        let server_first =
            "r=rOprNGfwEbeRWgbNEkqOAQIDBAUGBwgJCgsMDQ4PEBES,s=QSXCR+Q6sek8bf92,i=4096";
        let server_final = "v=qtUDIofVnIhM7tKn93EQUUt5vgMOldcDVu1HC+OH0o0=";

        exchange = match exchange.exchange(client_first).unwrap() {
            Step::Continue(exchange, message) => {
                assert_eq!(message, server_first);
                exchange
            }
            Step::Success(_, _) => panic!("expected continue, got success"),
            Step::Failure => panic!("scram auth failed"),
        };

        let key = match exchange.exchange(client_final).unwrap() {
            Step::Success(key, message) => {
                assert_eq!(message, server_final);
                key
            }
            Step::Continue(_, _) => panic!("expected success, got continue"),
            Step::Failure => panic!("scram auth failed"),
        };

        assert_eq!(
            key.as_bytes(),
            [
                74, 103, 1, 132, 12, 31, 200, 48, 28, 54, 82, 232, 207, 12, 138, 189, 40, 32, 134,
                27, 125, 170, 232, 35, 171, 167, 166, 41, 70, 228, 182, 112,
            ]
        );
    }

    #[test]
    fn snapshot_generated() {
        const NONCE: [u8; 18] = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];
        let password = "великолепный пароль";
        let secret = ServerSecret::generate(password.as_ref(), &NONCE, 128);

        let mut exchange = Exchange::new(&secret, || NONCE, TlsServerEndpoint::Undefined);

        let client_first = "n,,n=,r=0LTU/4V9YVBnmS+JfES49pSv";
        let client_final =
            "c=biws,r=0LTU/4V9YVBnmS+JfES49pSvAQIDBAUGBwgJCgsMDQ4PEBES,p=U2jUhAgLixSn2NqeQH7wX1myjSzr4rcDG/Bn1Rimgk0=";

        let server_first =
            "r=0LTU/4V9YVBnmS+JfES49pSvAQIDBAUGBwgJCgsMDQ4PEBES,s=AQIDBAUGBwgJCgsMDQ4PEBES,i=128";
        let server_final = "v=Gqo+LpAxGT3lNRYBkWOVfXwcmTgHCY2gijoqGLk1AFw=";

        exchange = match exchange.exchange(client_first).unwrap() {
            Step::Continue(exchange, message) => {
                assert_eq!(message, server_first);
                exchange
            }
            Step::Success(_, _) => panic!("expected continue, got success"),
            Step::Failure => panic!("scram auth failed"),
        };

        let key = match exchange.exchange(client_final).unwrap() {
            Step::Success(key, message) => {
                assert_eq!(message, server_final);
                key
            }
            Step::Continue(_, _) => panic!("expected success, got continue"),
            Step::Failure => panic!("scram auth failed"),
        };

        assert_eq!(
            key.as_bytes(),
            [
                249, 49, 138, 22, 155, 224, 144, 172, 41, 255, 9, 135, 55, 220, 30, 126, 24, 190,
                245, 93, 91, 205, 87, 239, 81, 108, 222, 214, 122, 137, 24, 87
            ]
        );
    }
}
