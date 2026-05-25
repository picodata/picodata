use std::hash::Hasher;

use twox_hash::XxHash3_64;

/// How an apply phase should hash rows whose JOIN-key columns include
/// NULL.
///
/// SQL `=` is unknown when either operand is NULL, so the row would
/// never match — `Skip` reflects that semantics and drops the row
/// before hashing. `Insert` mixes a distinct NULL tag into the hash;
/// it exists so the filter can be used over `IS NOT DISTINCT FROM`
/// (NULL-equating) equalities.
///
/// The discriminant is the on-the-wire byte; both `as u8` and
/// `TryFrom<u8>` are exposed to keep the wire encoder/decoder simple.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum NullPolicy {
    Skip = 0,
    Insert = 1,
}

impl TryFrom<u8> for NullPolicy {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(NullPolicy::Skip),
            1 => Ok(NullPolicy::Insert),
            other => Err(other),
        }
    }
}

/// Streaming hasher for a multi-column JOIN key.
///
/// Produces a single `u64` per (canonical bytes for col 1, separator,
/// canonical bytes for col 2, separator, ...) sequence. The coordinator
/// and the storage MUST feed identical bytes in identical order, or the
/// filter is silently wrong.
///
/// Layout per column:
///   add_bytes(canonical_bytes_for(value)) ; add_separator()
///
/// For NULL with `NullPolicy::Insert` use add_null() instead of
/// add_bytes; with `NullPolicy::Skip` the row is dropped before the
/// hasher is touched at all.
pub struct TupleHasher {
    state: XxHash3_64,
}

const SEPARATOR_TAG: u8 = 0xFE;
const NULL_TAG: u8 = 0xFF;

impl TupleHasher {
    pub fn new() -> Self {
        Self {
            state: XxHash3_64::new(),
        }
    }

    pub fn with_seed(seed: u64) -> Self {
        Self {
            state: XxHash3_64::with_seed(seed),
        }
    }

    pub fn add_bytes(&mut self, bytes: &[u8]) {
        self.state.write(bytes);
    }

    /// Inter-column separator. Without it `("ab", "")` and `("a", "b")`
    /// would collide.
    pub fn add_separator(&mut self) {
        self.state.write(&[SEPARATOR_TAG]);
    }

    /// Distinct from any canonical byte form of a value. Used by
    /// `NullPolicy::Insert` paths only.
    pub fn add_null(&mut self) {
        self.state.write(&[NULL_TAG]);
    }

    pub fn finish(self) -> u64 {
        self.state.finish()
    }
}

impl Default for TupleHasher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_cols(cols: &[&[u8]]) -> u64 {
        let mut h = TupleHasher::new();
        for c in cols {
            h.add_bytes(c);
            h.add_separator();
        }
        h.finish()
    }

    #[test]
    fn determinism() {
        assert_eq!(
            hash_cols(&[b"hello", b"world"]),
            hash_cols(&[b"hello", b"world"])
        );
    }

    #[test]
    fn separator_prevents_ambiguity() {
        assert_ne!(hash_cols(&[b"ab", b""]), hash_cols(&[b"a", b"b"]));
    }

    #[test]
    fn null_distinct_from_empty() {
        let mut a = TupleHasher::new();
        a.add_null();
        a.add_separator();
        let mut b = TupleHasher::new();
        b.add_bytes(b"");
        b.add_separator();
        assert_ne!(a.finish(), b.finish());
    }

    #[test]
    fn order_matters() {
        assert_ne!(hash_cols(&[b"a", b"b"]), hash_cols(&[b"b", b"a"]));
    }

    #[test]
    fn null_policy_wire_roundtrip() {
        // Wire discriminants must match `as u8`. If either side drifts,
        // a row passes the filter on one node and fails on another.
        assert_eq!(NullPolicy::Skip as u8, 0u8);
        assert_eq!(NullPolicy::Insert as u8, 1u8);
        assert_eq!(NullPolicy::try_from(0u8).unwrap(), NullPolicy::Skip);
        assert_eq!(NullPolicy::try_from(1u8).unwrap(), NullPolicy::Insert);
    }

    #[test]
    fn null_policy_rejects_unknown_byte() {
        for b in 2u8..=255 {
            assert_eq!(NullPolicy::try_from(b).unwrap_err(), b);
        }
    }
}
