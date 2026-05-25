//! XOR8 filter — a member of the Fuse-family (Graf & Lemire, 2020;
//! Dietzfelbinger & Walzer, 2022). Compact (~9.84 bits/element with
//! p ≈ 0.0039 false-positive rate), built once over a known set of
//! 128-bit hashes via peeling.
//!
//! Wire format (after the outer type tag, which `DynamicFilter::encode_into`
//! writes):
//!
//! ```text
//! u64 LE                              seed
//! u32 LE                              array_length
//! array_length x u8                   fingerprints
//! ```
//!
//! Decode is zero-copy via `FuseView`, which simply borrows the byte
//! slice and performs `contains` by mixing the hash three times into
//! three positions and XOR-ing the borrowed fingerprints.

use std::io::{self, Write};

use crate::error::{FilterBuildError, FilterDecodeError};

pub(crate) const TYPE_TAG: u8 = 0x02;

/// Threshold under which `SmallSet` is preferred — below this, the
/// fixed overhead of a Fuse filter (header + ~3× n bytes) is heavier
/// than the sorted-set alternative.
pub const FUSE_THRESHOLD: usize = 256;

/// XOR8 mixing factor — array size = ceil(EPSILON * n) + DELTA,
/// where EPSILON ≈ 1.23 and DELTA ≥ 32 covers the lower end where
/// 1.23 * n is too small for peeling to converge reliably.
const ALPHA_NUMER: u64 = 123;
const ALPHA_DENOM: u64 = 100;
const DELTA: u32 = 32;
const MAX_BUILD_ATTEMPTS: u32 = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseFilter {
    seed: u64,
    array_length: u32,
    fps: Box<[u8]>,
    /// Keys collected via `insert`. Construction happens at `encode_into`
    /// (the build phase needs the full set; we cannot finalize earlier
    /// because the peeling may need a seed retry).
    pending: Vec<u128>,
    finalized: bool,
}

impl FuseFilter {
    pub fn with_capacity(n: usize) -> Self {
        // Provisional sizing — the actual `fps` is allocated when we
        // know the final unique-key count at finalize() time.
        Self {
            seed: 0,
            array_length: 0,
            fps: Box::new([]),
            pending: Vec::with_capacity(n),
            finalized: false,
        }
    }

    pub fn insert(&mut self, hash: u128) {
        debug_assert!(!self.finalized, "insert after finalization");
        self.pending.push(hash);
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Bytes the encoded body will occupy (header inside the filter
    /// payload — outer msgpack `bin` framing is the protocol layer's
    /// responsibility).
    pub fn encoded_len(&self) -> usize {
        // type tag (1) + seed (8) + array_length (4) + fingerprints
        let arr = if self.finalized {
            self.array_length as usize
        } else {
            sized_array_length(self.pending.len()) as usize
        };
        1 + 8 + 4 + arr
    }

    /// Write the finalized payload. Caller must have invoked `finalize`
    /// (or `DynamicFilter::finalize`) first — debug-asserted.
    pub fn encode_into<W: Write>(&self, w: &mut W) -> io::Result<()> {
        debug_assert!(self.finalized, "encode_into before finalize");
        w.write_all(&[TYPE_TAG])?;
        w.write_all(&self.seed.to_le_bytes())?;
        w.write_all(&self.array_length.to_le_bytes())?;
        w.write_all(&self.fps)?;
        Ok(())
    }

    pub(crate) fn finalize(&mut self) -> Result<(), FilterBuildError> {
        if self.finalized {
            return Ok(());
        }
        // Dedup keys before peeling — repeated keys are functionally
        // a single insert and would otherwise break the 3-uniform
        // hypergraph assumption.
        self.pending.sort_unstable();
        self.pending.dedup();

        let n = self.pending.len();
        if n == 0 {
            // Degenerate filter — array of length 0, contains is false.
            self.seed = 0;
            self.array_length = 0;
            self.fps = Box::new([]);
            self.finalized = true;
            return Ok(());
        }

        let array_length = sized_array_length(n);
        let mut seed: u64 = 0x9E3779B97F4A7C15;
        for attempt in 0..MAX_BUILD_ATTEMPTS {
            seed = seed.wrapping_add((attempt as u64).wrapping_mul(0x9E3779B97F4A7C15));
            if let Some(fps) = try_build(seed, array_length, &self.pending) {
                self.seed = seed;
                self.array_length = array_length;
                self.fps = fps;
                self.finalized = true;
                return Ok(());
            }
        }
        Err(FilterBuildError::NotConverged(MAX_BUILD_ATTEMPTS))
    }

    pub fn contains(&self, hash: u128) -> bool {
        debug_assert!(self.finalized, "contains before finalization");
        if self.array_length == 0 {
            return false;
        }
        let (h0, h1, h2) = three_positions_modded(self.seed, hash, self.array_length);
        let f = fingerprint(self.seed, hash);
        let r = self.fps[h0 as usize] ^ self.fps[h1 as usize] ^ self.fps[h2 as usize];
        r == f
    }
}

/// Zero-copy view into an encoded FuseFilter payload (without type tag).
#[derive(Debug, Clone, Copy)]
pub struct FuseView<'a> {
    seed: u64,
    array_length: u32,
    fps: &'a [u8],
}

impl<'a> FuseView<'a> {
    pub(crate) fn decode(body: &'a [u8]) -> Result<Self, FilterDecodeError> {
        if body.len() < 12 {
            return Err(FilterDecodeError::Truncated {
                need: 12,
                got: body.len(),
            });
        }
        let mut seed_bytes = [0u8; 8];
        seed_bytes.copy_from_slice(&body[0..8]);
        let seed = u64::from_le_bytes(seed_bytes);
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&body[8..12]);
        let array_length = u32::from_le_bytes(len_bytes);
        let need = 12 + array_length as usize;
        if body.len() < need {
            return Err(FilterDecodeError::Truncated {
                need,
                got: body.len(),
            });
        }
        let fps = &body[12..need];
        Ok(Self {
            seed,
            array_length,
            fps,
        })
    }

    pub fn contains(&self, hash: u128) -> bool {
        if self.array_length == 0 {
            return false;
        }
        let (h0, h1, h2) = three_positions_modded(self.seed, hash, self.array_length);
        let f = fingerprint(self.seed, hash);
        let r = self.fps[h0 as usize] ^ self.fps[h1 as usize] ^ self.fps[h2 as usize];
        r == f
    }
}

// ---------- core math ----------

#[inline]
fn sized_array_length(n: usize) -> u32 {
    // ceil(ALPHA * n) + DELTA, rounded up to a multiple of 3 so the
    // array splits cleanly into three equal segments. Each key gets one
    // position per segment, which makes the three positions distinct by
    // construction — no perturbation is needed and the random
    // hypergraph stays peelable.
    let by_alpha = (n as u64 * ALPHA_NUMER + ALPHA_DENOM - 1) / ALPHA_DENOM;
    let raw = by_alpha as u32 + DELTA;
    let rounded = raw.div_ceil(3) * 3;
    rounded.max(3)
}

#[inline]
fn segment_length(array_length: u32) -> u32 {
    array_length / 3
}

/// SplitMix64 — a well-known 64-bit avalanche with full bit mixing.
#[inline]
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

/// Derive three independent 64-bit working hashes from (seed, key128)
/// by chaining splitmix64 three times. The first input folds both
/// halves of the 128-bit key plus the seed.
#[inline]
fn derive_xor_positions(seed: u64, hash: u128) -> (u64, u64, u64) {
    let lo = hash as u64;
    let hi = (hash >> 64) as u64;
    let r0 = splitmix64(lo ^ hi.rotate_left(32) ^ seed);
    let r1 = splitmix64(r0);
    let r2 = splitmix64(r1);
    (r0, r1, r2)
}

/// Three positions, one per disjoint third of the array. Reduces each
/// 64-bit working hash to its segment via fastrange ((u32 * len) >> 32),
/// then offsets by segment start.
#[inline]
fn three_positions_modded(seed: u64, hash: u128, array_length: u32) -> (u32, u32, u32) {
    let (a, b, c) = derive_xor_positions(seed, hash);
    let seg = segment_length(array_length) as u64;
    let p0 = ((a >> 32) * seg >> 32) as u32;
    let p1 = ((b >> 32) * seg >> 32) as u32 + seg as u32;
    let p2 = ((c >> 32) * seg >> 32) as u32 + 2 * seg as u32;
    (p0, p1, p2)
}

#[inline]
fn fingerprint(seed: u64, hash: u128) -> u8 {
    // Low byte of a 64-bit mix of (seed, hash). Forced non-zero so the
    // empty-fps default (0) cannot accidentally match.
    let lo = hash as u64;
    let hi = (hash >> 64) as u64;
    let mut f = lo
        .wrapping_mul(0xD1B54A32D192ED03)
        .wrapping_add(hi.wrapping_mul(0xCC9E2D51))
        .wrapping_add(seed.rotate_right(7));
    f ^= f >> 33;
    let byte = (f & 0xFF) as u8;
    if byte == 0 { 1 } else { byte }
}

/// One peeling attempt. Returns None if the random hypergraph is not
/// peelable with this seed — caller retries with a fresh seed.
fn try_build(seed: u64, array_length: u32, keys: &[u128]) -> Option<Box<[u8]>> {
    let n = keys.len();
    if array_length == 0 || n == 0 {
        return Some(vec![0u8; array_length as usize].into_boxed_slice());
    }

    // For each key, the three array positions it touches.
    let positions: Vec<(u32, u32, u32)> = keys
        .iter()
        .map(|&h| three_positions_modded(seed, h, array_length))
        .collect();

    // For each array position, list of edge indices (keys) that touch it,
    // and the running XOR of those edge indices (used by O(n) peeling).
    let m = array_length as usize;
    let mut deg: Vec<u32> = vec![0; m];
    let mut xor_edges: Vec<u64> = vec![0; m];
    for (i, &(a, b, c)) in positions.iter().enumerate() {
        let idx = i as u64;
        deg[a as usize] += 1;
        deg[b as usize] += 1;
        deg[c as usize] += 1;
        xor_edges[a as usize] ^= idx;
        xor_edges[b as usize] ^= idx;
        xor_edges[c as usize] ^= idx;
    }

    // Peeling queue: positions with degree exactly 1.
    let mut queue: Vec<u32> = (0..array_length).filter(|&p| deg[p as usize] == 1).collect();
    let mut peel_order: Vec<(u32, u64)> = Vec::with_capacity(n);
    // `removed[i] = true` iff edge i has been peeled.
    let mut removed: Vec<bool> = vec![false; n];

    while let Some(p) = queue.pop() {
        if deg[p as usize] != 1 {
            continue;
        }
        // The only edge incident to p is xor_edges[p].
        let e = xor_edges[p as usize] as usize;
        if removed[e] {
            // p had become 0 by the time we got here.
            continue;
        }
        removed[e] = true;
        peel_order.push((p, e as u64));
        // Remove this edge from the three positions it touches.
        let (a, b, c) = positions[e];
        for &q in &[a, b, c] {
            let qi = q as usize;
            deg[qi] -= 1;
            xor_edges[qi] ^= e as u64;
            if deg[qi] == 1 {
                queue.push(q);
            }
        }
    }

    if peel_order.len() != n {
        return None;
    }

    // Reverse pass: assign fingerprints so that for each edge e =
    // (p_e, q1, q2) we get fps[p_e] = fingerprint(key_e) ^ fps[q1] ^ fps[q2].
    let mut fps = vec![0u8; m];
    for (p, e) in peel_order.iter().rev() {
        let e = *e as usize;
        let (a, b, c) = positions[e];
        // Find the two non-p positions.
        let p = *p;
        let others: [u32; 2] = if p == a {
            [b, c]
        } else if p == b {
            [a, c]
        } else {
            [a, b]
        };
        let fp = fingerprint(seed, keys[e]);
        fps[p as usize] = fp ^ fps[others[0] as usize] ^ fps[others[1] as usize];
    }

    Some(fps.into_boxed_slice())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys_n(n: usize) -> Vec<u128> {
        // Deterministic spread that is reasonably uncorrelated with our
        // mix constants.
        (0..n as u64)
            .map(|i| {
                let lo = i.wrapping_mul(0xBF58476D1CE4E5B9).wrapping_add(0x12345);
                let hi = i.wrapping_mul(0x94D049BB133111EB) ^ 0xDEADBEEF;
                ((hi as u128) << 64) | lo as u128
            })
            .collect()
    }

    fn build_for(keys: &[u128]) -> FuseFilter {
        let mut f = FuseFilter::with_capacity(keys.len());
        for &k in keys {
            f.insert(k);
        }
        f.finalize().expect("peeling converged");
        f
    }

    #[test]
    fn empty_filter() {
        let mut f = FuseFilter::with_capacity(0);
        f.finalize().unwrap();
        assert!(!f.contains(0));
        assert!(!f.contains(42));
    }

    #[test]
    fn no_false_negatives_small() {
        let keys = keys_n(50);
        let f = build_for(&keys);
        for &k in &keys {
            assert!(f.contains(k), "false negative for key {k}");
        }
    }

    #[test]
    fn no_false_negatives_medium() {
        let keys = keys_n(5_000);
        let f = build_for(&keys);
        for &k in &keys {
            assert!(f.contains(k), "false negative for key {k}");
        }
    }

    #[test]
    fn no_false_negatives_large() {
        let keys = keys_n(100_000);
        let f = build_for(&keys);
        for &k in &keys {
            assert!(f.contains(k), "false negative for key {k}");
        }
    }

    #[test]
    fn idempotent_insert() {
        let mut f = FuseFilter::with_capacity(10);
        for _ in 0..7 {
            f.insert(0xCAFEBABE);
        }
        f.finalize().unwrap();
        assert!(f.contains(0xCAFEBABE));
    }

    #[test]
    fn fpr_under_threshold() {
        // Sanity check on false-positive rate. With 8-bit fingerprints
        // the theoretical FPR is ~1/256 ≈ 0.0039. Allow a generous 5%
        // ceiling to keep the test stable.
        let n = 4_000usize;
        let keys = keys_n(n);
        let f = build_for(&keys);
        let probes = 100_000;
        let mut fp = 0u32;
        for i in 0..probes {
            // Keys disjoint from the build set.
            let probe = ((i as u128) << 64) | (i as u128).wrapping_mul(0xFEEDFACE);
            if !keys.contains(&probe) && f.contains(probe) {
                fp += 1;
            }
        }
        let rate = fp as f64 / probes as f64;
        assert!(rate < 0.05, "false positive rate {rate} too high");
    }

    #[test]
    fn roundtrip_encode_decode() {
        let keys = keys_n(1000);
        let f = build_for(&keys);
        let mut buf = vec![];
        f.encode_into(&mut buf).unwrap();
        // Strip the leading TYPE_TAG: the decode below is for the body.
        let view = FuseView::decode(&buf[1..]).unwrap();
        for &k in &keys {
            assert!(view.contains(k), "view missed key after round-trip");
        }
    }
}
