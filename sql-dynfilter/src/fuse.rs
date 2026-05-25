//! Thin wrapper over `xorf::BinaryFuse8` — a Fuse-family filter
//! (Graf & Lemire 2020; Dietzfelbinger & Walzer 2022) that compresses
//! a known u64 key set into ~9 bits/element with FPR ≈ 0.0039.
//!
//! Wire format (after the outer type tag, which `DynamicFilter::encode_into`
//! writes):
//!
//! ```text
//! u8                                  has_filter (0 or 1)
//! [if has_filter == 1]
//!   BinaryFuse8::DESCRIPTOR_LEN x u8  descriptor (xorf-internal)
//!   u32 LE                            fingerprints_len
//!   fingerprints_len x u8             fingerprints
//! ```
//!
//! Decode is zero-copy via `FuseView`: it borrows the descriptor bytes
//! (parsed eagerly into a `BinaryFuse8Ref`) and the fingerprint slice
//! from the DQL wire buffer.

use std::io::{self, Write};

use xorf::{BinaryFuse8, BinaryFuse8Ref, DmaSerializable, Filter, FilterRef};

use crate::error::{FilterBuildError, FilterDecodeError};

pub(crate) const TYPE_TAG: u8 = 0x02;

/// Threshold under which `SmallSet` is preferred — below this the
/// fixed overhead of BinaryFuse8 (descriptor + ~9 bits/key) is heavier
/// than a sorted-set alternative.
pub const FUSE_THRESHOLD: usize = 256;

#[derive(Debug, Clone)]
pub struct FuseFilter {
    /// Keys collected via `insert`. Construction happens at `finalize`
    /// (BinaryFuse8 needs the full set up-front, and may need to retry
    /// on dedup-related rare construction failures).
    pending: Vec<u64>,
    built: Option<BinaryFuse8>,
    finalized: bool,
}

impl PartialEq for FuseFilter {
    fn eq(&self, other: &Self) -> bool {
        // Equality is only meaningful post-finalize; compare the
        // serialized bytes (descriptor + fingerprints) for stability.
        // Used only by tests / debug assertions.
        if self.finalized != other.finalized {
            return false;
        }
        match (&self.built, &other.built) {
            (None, None) => self.pending == other.pending,
            (Some(a), Some(b)) => {
                let mut da = vec![0u8; BinaryFuse8::DESCRIPTOR_LEN];
                let mut db = vec![0u8; BinaryFuse8::DESCRIPTOR_LEN];
                a.dma_copy_descriptor_to(&mut da);
                b.dma_copy_descriptor_to(&mut db);
                da == db && a.dma_fingerprints() == b.dma_fingerprints()
            }
            _ => false,
        }
    }
}

impl Eq for FuseFilter {}

impl FuseFilter {
    pub fn with_capacity(n: usize) -> Self {
        Self {
            pending: Vec::with_capacity(n),
            built: None,
            finalized: false,
        }
    }

    pub fn insert(&mut self, hash: u64) {
        debug_assert!(!self.finalized, "insert after finalization");
        self.pending.push(hash);
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Bytes the encoded payload will occupy. Valid only after
    /// `finalize`. Outer msgpack `bin` framing is the protocol layer's
    /// responsibility.
    pub fn encoded_len(&self) -> usize {
        debug_assert!(self.finalized, "encoded_len before finalize");
        match &self.built {
            None => 1 + 1, // outer tag + has_filter flag
            Some(f) => 1 + 1 + BinaryFuse8::DESCRIPTOR_LEN + 4 + f.dma_fingerprints().len(),
        }
    }

    pub fn encode_into<W: Write>(&self, w: &mut W) -> io::Result<()> {
        debug_assert!(self.finalized, "encode_into before finalize");
        w.write_all(&[TYPE_TAG])?;
        match &self.built {
            None => {
                w.write_all(&[0u8])?;
            }
            Some(f) => {
                w.write_all(&[1u8])?;
                let mut desc = vec![0u8; BinaryFuse8::DESCRIPTOR_LEN];
                f.dma_copy_descriptor_to(&mut desc);
                w.write_all(&desc)?;
                let fps = f.dma_fingerprints();
                let fp_len = u32::try_from(fps.len()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "BinaryFuse8 fingerprints exceed u32::MAX",
                    )
                })?;
                w.write_all(&fp_len.to_le_bytes())?;
                w.write_all(fps)?;
            }
        }
        Ok(())
    }

    pub(crate) fn finalize(&mut self) -> Result<(), FilterBuildError> {
        if self.finalized {
            return Ok(());
        }
        // Dedup keys — BinaryFuse8 fails on duplicate keys, and
        // repeated insertions are functionally a single insert from
        // the filter's perspective.
        self.pending.sort_unstable();
        self.pending.dedup();

        if self.pending.is_empty() {
            self.built = None;
        } else {
            let f = BinaryFuse8::try_from(&self.pending[..])
                .map_err(|e| FilterBuildError::FuseBuildFailed(e.to_string()))?;
            self.built = Some(f);
        }
        self.finalized = true;
        Ok(())
    }

    pub fn contains(&self, hash: u64) -> bool {
        debug_assert!(self.finalized, "contains before finalization");
        match &self.built {
            None => false,
            Some(f) => f.contains(&hash),
        }
    }
}

/// Zero-copy view into an encoded FuseFilter payload (without type tag).
#[derive(Debug, Clone)]
pub struct FuseView<'a> {
    inner: Option<BinaryFuse8Ref<'a>>,
}

impl<'a> FuseView<'a> {
    pub(crate) fn decode(body: &'a [u8]) -> Result<Self, FilterDecodeError> {
        if body.is_empty() {
            return Err(FilterDecodeError::Truncated { need: 1, got: 0 });
        }
        let has_filter = body[0];
        let rest = &body[1..];
        match has_filter {
            0 => Ok(Self { inner: None }),
            1 => {
                let dlen = BinaryFuse8::DESCRIPTOR_LEN;
                if rest.len() < dlen + 4 {
                    return Err(FilterDecodeError::Truncated {
                        need: dlen + 4,
                        got: rest.len(),
                    });
                }
                let descriptor = &rest[..dlen];
                let fp_len = u32::from_le_bytes([
                    rest[dlen],
                    rest[dlen + 1],
                    rest[dlen + 2],
                    rest[dlen + 3],
                ]) as usize;
                let need = dlen + 4 + fp_len;
                if rest.len() < need {
                    return Err(FilterDecodeError::Truncated {
                        need,
                        got: rest.len(),
                    });
                }
                let fingerprints = &rest[dlen + 4..dlen + 4 + fp_len];
                let r = BinaryFuse8Ref::from_dma(descriptor, fingerprints);
                Ok(Self { inner: Some(r) })
            }
            other => Err(FilterDecodeError::UnknownTag(other)),
        }
    }

    pub fn contains(&self, hash: u64) -> bool {
        match &self.inner {
            None => false,
            Some(r) => r.contains(&hash),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys_n(n: usize) -> Vec<u64> {
        // Deterministic spread, decorrelated from internal mix constants.
        (0..n as u64)
            .map(|i| {
                let mut x = i.wrapping_mul(0xBF58476D1CE4E5B9).wrapping_add(0x12345);
                x ^= x >> 30;
                x = x.wrapping_mul(0x94D049BB133111EB);
                x ^= 0xDEADBEEF;
                x
            })
            .collect()
    }

    fn build_for(keys: &[u64]) -> FuseFilter {
        let mut f = FuseFilter::with_capacity(keys.len());
        for &k in keys {
            f.insert(k);
        }
        f.finalize().expect("BinaryFuse8 construction");
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
        // 8-bit fingerprints → theoretical FPR ~1/256 ≈ 0.0039.
        // 5% ceiling for stability across builds.
        let n = 4_000usize;
        let keys = keys_n(n);
        let f = build_for(&keys);
        let probes = 100_000;
        let mut fp = 0u32;
        for i in 0..probes {
            let probe = (i as u64).wrapping_mul(0xFEEDFACE) ^ 0xA5A5_A5A5_A5A5_A5A5;
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
        // Strip leading TYPE_TAG; decode operates on the body.
        let view = FuseView::decode(&buf[1..]).unwrap();
        for &k in &keys {
            assert!(view.contains(k), "view missed key after round-trip");
        }
    }

    #[test]
    fn roundtrip_empty() {
        let mut f = FuseFilter::with_capacity(0);
        f.finalize().unwrap();
        let mut buf = vec![];
        f.encode_into(&mut buf).unwrap();
        let view = FuseView::decode(&buf[1..]).unwrap();
        assert!(!view.contains(0));
        assert!(!view.contains(u64::MAX));
    }
}
