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

//! Tools for client/server signature management.

use hmac::Mac as _;

use super::key::{ScramKey, SCRAM_KEY_LEN};
use crate::scram::pbkdf2::Prf;

/// A collection of message parts needed to derive the client's signature.
#[derive(Debug)]
pub struct SignatureBuilder<'a> {
    pub client_first_message_bare: &'a str,
    pub server_first_message: &'a str,
    pub client_final_message_without_proof: &'a str,
}

impl SignatureBuilder<'_> {
    pub fn build(&self, key: &ScramKey) -> Signature {
        let mut mac = Prf::new_from_slice(key.as_ref()).expect("HMAC accepts all key sizes");
        mac.update(self.client_first_message_bare.as_bytes());
        mac.update(b",");
        mac.update(self.server_first_message.as_bytes());
        mac.update(b",");
        mac.update(self.client_final_message_without_proof.as_bytes());
        Signature {
            bytes: mac.finalize().into_bytes().into(),
        }
    }
}

/// A computed value which, when xored with `ClientProof`,
/// produces `ClientKey` that we need for authentication.
#[derive(Debug)]
#[repr(transparent)]
pub struct Signature {
    bytes: [u8; SCRAM_KEY_LEN],
}

impl Signature {
    /// Derive `ClientKey` from client's signature and proof.
    pub fn derive_client_key(&self, proof: &[u8; SCRAM_KEY_LEN]) -> ScramKey {
        // This is how the proof is calculated:
        //
        // 1. sha256(ClientKey) -> StoredKey
        // 2. hmac_sha256(StoredKey, [messages...]) -> ClientSignature
        // 3. ClientKey ^ ClientSignature -> ClientProof
        //
        // Step 3 implies that we can restore ClientKey from the proof
        // by xoring the latter with the ClientSignature. Afterwards we
        // can check that the presumed ClientKey meets our expectations.
        let mut signature = self.bytes;
        for (i, x) in proof.iter().enumerate() {
            signature[i] ^= x;
        }

        signature.into()
    }
}

impl From<[u8; SCRAM_KEY_LEN]> for Signature {
    fn from(bytes: [u8; SCRAM_KEY_LEN]) -> Self {
        Self { bytes }
    }
}

impl AsRef<[u8]> for Signature {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
