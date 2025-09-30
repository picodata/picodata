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

//! For postgres password authentication, we need to perform a PBKDF2 using
//! PRF=HMAC-SHA2-256, producing only 1 block (32 bytes) of output key.

use hmac::digest::consts::U32;
use hmac::digest::generic_array::GenericArray;
use hmac::digest::Mac as _;
use zeroize::Zeroize as _;

/// The Psuedo-random function used during PBKDF2 and the SCRAM-SHA-256 handshake.
pub type Prf = hmac::Hmac<sha2::Sha256>;
pub type Block = GenericArray<u8, U32>;

pub struct Pbkdf2 {
    hmac: Prf,
    /// U{r-1} for whatever iteration r we are currently on.
    prev: Block,
    /// the output of `fold(xor, U{1}..U{r})` for whatever iteration r we are currently on.
    hi: Block,
    /// number of iterations left
    iterations: u32,
}

impl Drop for Pbkdf2 {
    fn drop(&mut self) {
        self.prev.zeroize();
        self.hi.zeroize();
    }
}

impl Pbkdf2 {
    /// Compute PBKDF2 without yielding to other fibers.
    /// Note: this is a relatively CPU-intensive computation depending on the number of iterations.
    pub fn compute_no_yield(password: &[u8], salt: &[u8], iterations: u32) -> Block {
        let mut computation = Pbkdf2::start(password, salt, iterations);
        loop {
            if let std::task::Poll::Ready(hash) = computation.turn() {
                break hash;
            };
        }
    }

    pub fn start(password: &[u8], salt: &[u8], iterations: u32) -> Self {
        // key the HMAC and derive the first block in-place
        let mut hmac = Prf::new_from_slice(password).expect("HMAC is able to accept all key sizes");

        // U1 = PRF(Password, Salt + INT_32_BE(i))
        // i = 1 since we only need 1 block of output.
        hmac.update(salt);
        hmac.update(&1u32.to_be_bytes());
        let init_block = hmac.finalize_reset().into_bytes();

        Self {
            hmac,
            // one iteration spent above
            iterations: iterations - 1,
            hi: init_block,
            prev: init_block,
        }
    }

    /// For "fairness", we implement PBKDF2 with cooperative yielding, which is why we use this `turn`
    /// function that only executes a fixed number of iterations before continuing.
    ///
    /// Task must be rescheuled if this returns [`std::task::Poll::Pending`].
    pub fn turn(&mut self) -> std::task::Poll<Block> {
        let Self {
            hmac,
            prev,
            hi,
            iterations,
        } = self;

        // only do up to 4096 iterations per turn for fairness
        let n = (*iterations).clamp(0, 4096);
        for _ in 0..n {
            let next = single_round(hmac, prev);
            xor_assign(hi, &next);
            *prev = next;
        }

        *iterations -= n;
        if *iterations == 0 {
            std::task::Poll::Ready(*hi)
        } else {
            std::task::Poll::Pending
        }
    }
}

#[inline(always)]
pub fn xor_assign(x: &mut Block, y: &Block) {
    for (x, &y) in std::iter::zip(x, y) {
        *x ^= y;
    }
}

#[inline(always)]
fn single_round(prf: &mut Prf, ui: &Block) -> Block {
    // Ui = PRF(Password, Ui-1)
    prf.update(ui);
    prf.finalize_reset().into_bytes()
}

#[cfg(test)]
mod tests {
    use super::Pbkdf2;
    use pbkdf2::pbkdf2_hmac_array;
    use sha2::Sha256;

    #[test]
    fn test_start_turn() {
        let salt = b"sodium chloride";
        let pass = b"very nice indeed";

        let mut job = Pbkdf2::start(pass, salt, 60000);
        let hash: [u8; 32] = loop {
            let std::task::Poll::Ready(hash) = job.turn() else {
                continue;
            };
            break hash.into();
        };

        let expected = pbkdf2_hmac_array::<Sha256, 32>(pass, salt, 60000);
        assert_eq!(hash, expected);
    }

    #[test]
    fn test_compute_no_yield() {
        let salt = b"sodium chloride";
        let pass = b"very nice indeed";

        let hash: [u8; 32] = Pbkdf2::compute_no_yield(pass, salt, 60000).into();
        let expected = pbkdf2_hmac_array::<Sha256, 32>(pass, salt, 60000);
        assert_eq!(hash, expected);
    }
}
