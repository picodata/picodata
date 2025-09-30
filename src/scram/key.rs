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

//! Tools for client/server/stored key management.

use super::pbkdf2::Block;
use hmac::digest::Digest;
use subtle::ConstantTimeEq;
use zeroize::Zeroize as _;

/// Faithfully taken from PostgreSQL.
pub const SCRAM_KEY_LEN: usize = 32;

/// One of the keys derived from the user's password.
/// We use the same structure for all keys, i.e.
/// `ClientKey`, `StoredKey`, and `ServerKey`.
#[derive(Clone, Default, Eq, Debug)]
#[repr(transparent)]
pub struct ScramKey {
    bytes: [u8; SCRAM_KEY_LEN],
}

impl Drop for ScramKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

impl PartialEq for ScramKey {
    fn eq(&self, other: &Self) -> bool {
        self.ct_eq(other).into()
    }
}

impl ConstantTimeEq for ScramKey {
    fn ct_eq(&self, other: &Self) -> subtle::Choice {
        self.bytes.ct_eq(&other.bytes)
    }
}

impl ScramKey {
    pub fn sha256(&self) -> Self {
        Self {
            bytes: sha2::Sha256::digest(self.as_bytes()).into(),
        }
    }

    pub fn as_bytes(&self) -> [u8; SCRAM_KEY_LEN] {
        self.bytes
    }
}

impl From<Block> for ScramKey {
    #[inline(always)]
    fn from(value: Block) -> Self {
        let bytes: [u8; SCRAM_KEY_LEN] = value.into();
        Self { bytes }
    }
}

impl From<[u8; SCRAM_KEY_LEN]> for ScramKey {
    #[inline(always)]
    fn from(bytes: [u8; SCRAM_KEY_LEN]) -> Self {
        Self { bytes }
    }
}

impl AsRef<[u8]> for ScramKey {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
