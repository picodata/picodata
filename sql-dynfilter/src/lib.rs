//! Approximate set-membership filters for SQL dynamic filter pushdown.
//!
//! Two variants share a single wire format (outer type tag + body):
//! * `SmallSet` — sorted exact set, used when n is below `FUSE_THRESHOLD`
//! * `FuseFilter` — XOR8/Fuse-family approximate filter for larger sets
//!
//! The build side hashes its JOIN keys into u128 via `TupleHasher`,
//! inserts them into a `DynamicFilter`, and serializes the result with
//! `encode_into` directly into the DQL wire buffer. The probe side
//! decodes it zero-copy via `FilterView::decode` and tests rows with
//! `contains`.

mod error;
mod fuse;
mod hasher;
mod smallset;

pub use error::{FilterBuildError, FilterDecodeError};
pub use fuse::FUSE_THRESHOLD;
pub use hasher::{NullPolicy, TupleHasher};

use std::io::{self, Write};

use fuse::{FuseFilter, FuseView};
use smallset::{SmallSet, SmallSetView};

/// Build-side filter. Pick the variant via `DynamicFilter::new(n)`,
/// where `n` is an upper bound on the number of inserts — the constructor
/// uses it to choose between `SmallSet` (low n) and `FuseFilter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DynamicFilter {
    Fuse(FuseFilter),
    SmallSet(SmallSet),
}

impl DynamicFilter {
    /// Choose SmallSet for tiny build sides, Fuse for larger ones.
    /// The threshold reflects where Fuse's fixed overhead becomes
    /// smaller than a sorted exact set.
    pub fn new(expected: usize) -> Self {
        if expected < FUSE_THRESHOLD {
            Self::SmallSet(SmallSet::with_capacity(expected))
        } else {
            Self::Fuse(FuseFilter::with_capacity(expected))
        }
    }

    pub fn insert(&mut self, hash: u128) {
        match self {
            Self::SmallSet(s) => s.insert(hash),
            Self::Fuse(f) => f.insert(hash),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::SmallSet(s) => s.len(),
            Self::Fuse(f) => f.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::SmallSet(s) => s.is_empty(),
            Self::Fuse(f) => f.is_empty(),
        }
    }

    /// Upper bound on the serialized size — used by the executor to
    /// reserve buffer space before `encode_into`.
    pub fn encoded_len(&self) -> usize {
        match self {
            Self::SmallSet(s) => s.encoded_len(),
            Self::Fuse(f) => f.encoded_len(),
        }
    }

    /// Finalize the filter: SmallSet sorts/dedups, Fuse runs peeling.
    /// Must be called once after the last `insert` and before any
    /// `encoded_len` / `encode_into` call. Fails for Fuse only if
    /// peeling does not converge after the seed-retry budget.
    pub fn finalize(&mut self) -> Result<(), FilterBuildError> {
        match self {
            Self::SmallSet(s) => {
                s.finalize();
                Ok(())
            }
            Self::Fuse(f) => f.finalize(),
        }
    }

    /// Write the encoded filter (type tag + body) into `w`. Requires
    /// `finalize` to have been called — debug-asserted by the inner
    /// implementations.
    pub fn encode_into<W: Write>(&self, w: &mut W) -> io::Result<()> {
        match self {
            Self::SmallSet(s) => s.encode_into(w),
            Self::Fuse(f) => f.encode_into(w),
        }
    }

    /// Live-side counterpart to `FilterView::contains`. Lets the
    /// coordinator query the just-built filter without a roundtrip
    /// through `encode_into` / `FilterView::decode` — needed by the
    /// §5.4 apply phase when retain runs on the coordinator side.
    /// `finalize` must have been called.
    pub fn contains(&self, hash: u128) -> bool {
        match self {
            Self::SmallSet(s) => s.contains(hash),
            Self::Fuse(f) => f.contains(hash),
        }
    }
}

/// Zero-copy view into an encoded filter (type tag + body) borrowed
/// from the DQL wire buffer.
#[derive(Debug, Clone, Copy)]
pub enum FilterView<'a> {
    Fuse(FuseView<'a>),
    SmallSet(SmallSetView<'a>),
}

impl<'a> FilterView<'a> {
    /// Decode a filter from a byte slice that starts with the type tag.
    /// The slice must outlive the returned view.
    pub fn decode(buf: &'a [u8]) -> Result<Self, FilterDecodeError> {
        let (&tag, rest) = buf
            .split_first()
            .ok_or(FilterDecodeError::Truncated { need: 1, got: 0 })?;
        match tag {
            smallset::TYPE_TAG => Ok(Self::SmallSet(SmallSetView::decode(rest)?)),
            fuse::TYPE_TAG => Ok(Self::Fuse(FuseView::decode(rest)?)),
            other => Err(FilterDecodeError::UnknownTag(other)),
        }
    }

    pub fn contains(&self, hash: u128) -> bool {
        match self {
            Self::SmallSet(s) => s.contains(hash),
            Self::Fuse(f) => f.contains(hash),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys_n(n: usize) -> Vec<u128> {
        (0..n as u64)
            .map(|i| {
                let lo = i.wrapping_mul(0xBF58476D1CE4E5B9).wrapping_add(0x12345);
                let hi = i.wrapping_mul(0x94D049BB133111EB) ^ 0xDEADBEEF;
                ((hi as u128) << 64) | lo as u128
            })
            .collect()
    }

    #[test]
    fn small_path_under_threshold() {
        let f = DynamicFilter::new(10);
        assert!(matches!(f, DynamicFilter::SmallSet(_)));
    }

    #[test]
    fn fuse_path_at_threshold() {
        let f = DynamicFilter::new(FUSE_THRESHOLD);
        assert!(matches!(f, DynamicFilter::Fuse(_)));
    }

    #[test]
    fn roundtrip_small() {
        let mut f = DynamicFilter::new(8);
        let keys = keys_n(8);
        for &k in &keys {
            f.insert(k);
        }
        f.finalize().unwrap();
        let mut buf = vec![];
        f.encode_into(&mut buf).unwrap();
        let view = FilterView::decode(&buf).unwrap();
        for &k in &keys {
            assert!(view.contains(k));
        }
    }

    #[test]
    fn roundtrip_fuse() {
        let mut f = DynamicFilter::new(1000);
        let keys = keys_n(1000);
        for &k in &keys {
            f.insert(k);
        }
        f.finalize().unwrap();
        let mut buf = vec![];
        f.encode_into(&mut buf).unwrap();
        let view = FilterView::decode(&buf).unwrap();
        for &k in &keys {
            assert!(view.contains(k));
        }
    }

    #[test]
    fn unknown_tag_rejected() {
        let buf = [0xAB, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(
            FilterView::decode(&buf).unwrap_err(),
            FilterDecodeError::UnknownTag(0xAB)
        );
    }

    #[test]
    fn threshold_n_uses_fuse() {
        let f = DynamicFilter::new(FUSE_THRESHOLD);
        assert!(matches!(f, DynamicFilter::Fuse(_)));
    }

    #[test]
    fn just_below_threshold_uses_smallset() {
        let f = DynamicFilter::new(FUSE_THRESHOLD - 1);
        assert!(matches!(f, DynamicFilter::SmallSet(_)));
    }

    #[test]
    fn empty_buffer_rejected() {
        let buf: [u8; 0] = [];
        assert!(matches!(
            FilterView::decode(&buf).unwrap_err(),
            FilterDecodeError::Truncated { .. }
        ));
    }

    #[test]
    fn smallset_tag_only_buffer_rejected() {
        // Outer type tag present (0x01 = SmallSet), but the body is
        // missing — decoder must report a truncation error rather than
        // try to read past the buffer.
        let buf = [0x01u8];
        assert!(matches!(
            FilterView::decode(&buf).unwrap_err(),
            FilterDecodeError::Truncated { .. }
        ));
    }

    #[test]
    fn fuse_tag_only_buffer_rejected() {
        let buf = [0x02u8];
        assert!(matches!(
            FilterView::decode(&buf).unwrap_err(),
            FilterDecodeError::Truncated { .. }
        ));
    }

    #[test]
    fn smallset_header_says_more_than_payload_has() {
        // SmallSet body: u32 LE length, then len * 16 bytes. Claim 3
        // entries (48 bytes) but ship only 16 bytes — decoder must reject.
        let mut buf = vec![0x01u8]; // tag
        buf.extend_from_slice(&3u32.to_le_bytes()); // claimed length
        buf.extend_from_slice(&[0u8; 16]); // only one u128 follows
        assert!(matches!(
            FilterView::decode(&buf).unwrap_err(),
            FilterDecodeError::Truncated { .. }
        ));
    }

    #[test]
    fn fuse_header_says_more_than_payload_has() {
        // Fuse body: u64 seed + u32 array_length + array_length bytes.
        // Claim 1024 fps bytes but ship none — decoder must reject.
        let mut buf = vec![0x02u8];
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&1024u32.to_le_bytes());
        // no fps bytes
        assert!(matches!(
            FilterView::decode(&buf).unwrap_err(),
            FilterDecodeError::Truncated { .. }
        ));
    }

    #[test]
    fn view_contains_matches_builder_contains_small() {
        // Build → finalize. Live-side `contains` (the just-added
        // delegating method on `DynamicFilter`) must agree bit-for-bit
        // with the zero-copy `FilterView::contains`. Otherwise the
        // §5.4 coordinator apply and the future §5.4.3 storage apply
        // would silently diverge.
        let mut f = DynamicFilter::new(8);
        let keys = keys_n(8);
        for &k in &keys {
            f.insert(k);
        }
        f.finalize().unwrap();
        let mut buf = vec![];
        f.encode_into(&mut buf).unwrap();
        let view = FilterView::decode(&buf).unwrap();
        for &k in &keys {
            assert_eq!(f.contains(k), view.contains(k));
        }
        // Negative probes — must also agree (true negatives, since
        // SmallSet has no false positives).
        for i in 1000u128..1064u128 {
            assert!(!f.contains(i));
            assert!(!view.contains(i));
        }
    }

    #[test]
    fn view_contains_matches_builder_contains_fuse() {
        let n = 2000;
        let mut f = DynamicFilter::new(n);
        let keys = keys_n(n);
        for &k in &keys {
            f.insert(k);
        }
        f.finalize().unwrap();
        let mut buf = vec![];
        f.encode_into(&mut buf).unwrap();
        let view = FilterView::decode(&buf).unwrap();
        for &k in &keys {
            assert!(f.contains(k));
            assert!(view.contains(k));
        }
        // Probes outside the build set must agree (true or false —
        // a Fuse false positive must register identically on both
        // sides because they share seed, array_length, and fps).
        for i in 0u128..1000u128 {
            let probe = i << 96; // disjoint from keys_n outputs
            assert_eq!(
                f.contains(probe),
                view.contains(probe),
                "builder/view divergence at probe {probe}"
            );
        }
    }

    #[test]
    fn dynamic_filter_contains_on_empty_returns_false() {
        // No inserts, finalize, then contains must yield false for any
        // input — important so the coordinator-side apply phase does
        // not silently keep all rows when nothing was built.
        let mut f = DynamicFilter::new(0);
        f.finalize().unwrap();
        assert!(!f.contains(0));
        assert!(!f.contains(u128::MAX));
        let mut buf = vec![];
        f.encode_into(&mut buf).unwrap();
        let view = FilterView::decode(&buf).unwrap();
        assert!(!view.contains(0));
        assert!(!view.contains(u128::MAX));
    }

    #[test]
    fn fuse_large_build_then_decode_roundtrip() {
        // Stress the Fuse path with enough keys to exceed the threshold
        // by a wide margin and exercise the multi-seed peeling
        // convergence. Each key must survive encode → decode.
        let n = 50_000;
        let mut f = DynamicFilter::new(n);
        let keys = keys_n(n);
        for &k in &keys {
            f.insert(k);
        }
        f.finalize().unwrap();
        assert!(matches!(f, DynamicFilter::Fuse(_)));
        let mut buf = vec![];
        f.encode_into(&mut buf).unwrap();
        let view = FilterView::decode(&buf).unwrap();
        // No false negatives.
        for &k in &keys {
            assert!(view.contains(k), "view missing key {k} after roundtrip");
        }
    }

    #[test]
    fn encoded_len_matches_actual_encode_length() {
        // Pre-allocation contract: after `finalize`, `encoded_len()` must
        // equal the exact number of bytes written by `encode_into`.
        // Violating this would corrupt msgpack `bin` framing in the DQL
        // packet (the encoder writes `bin_len = encoded_len` first).
        for &n in &[0usize, 1, 8, FUSE_THRESHOLD - 1, FUSE_THRESHOLD, 4096] {
            let mut f = DynamicFilter::new(n);
            for &k in &keys_n(n) {
                f.insert(k);
            }
            f.finalize().unwrap();
            let mut buf = vec![];
            f.encode_into(&mut buf).unwrap();
            assert_eq!(buf.len(), f.encoded_len(), "encoded_len mismatch for n={n}");
        }
    }

    #[test]
    fn fresh_seed_unknown_tag_rejection_does_not_panic() {
        // Fuzz the tag byte: every value other than 0x01 / 0x02 must
        // produce `UnknownTag`, never a panic or false-accept.
        for tag in 0u8..=255 {
            if tag == 0x01 || tag == 0x02 {
                continue;
            }
            let buf = [tag, 0, 0, 0, 0];
            match FilterView::decode(&buf) {
                Err(FilterDecodeError::UnknownTag(t)) => assert_eq!(t, tag),
                Err(FilterDecodeError::Truncated { .. }) => {
                    // Acceptable for tags that happen to be valid first
                    // bytes of some other framing — but since we never
                    // assign such tags, this branch should not fire in
                    // practice. Keep tolerant though.
                }
                Ok(_) => panic!("unknown tag {tag:#x} accepted"),
                Err(e) => panic!("unexpected error for tag {tag:#x}: {e:?}"),
            }
        }
    }
}
