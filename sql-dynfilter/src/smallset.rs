//! Exact small set: sorted `Box<[u64]>` with binary-search lookup.
//!
//! Used when `n < FUSE_THRESHOLD` — for small build sides the fixed
//! overhead of a Fuse-style filter (descriptor, fingerprints) makes
//! a sorted exact set both smaller and faster.

use std::io::{self, Write};

use crate::error::FilterDecodeError;

pub(crate) const TYPE_TAG: u8 = 0x01;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SmallSet {
    entries: Vec<u64>,
    finalized: bool,
}

impl SmallSet {
    pub fn with_capacity(n: usize) -> Self {
        Self {
            entries: Vec::with_capacity(n),
            finalized: false,
        }
    }

    pub fn insert(&mut self, hash: u64) {
        debug_assert!(!self.finalized, "insert after finalization");
        self.entries.push(hash);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn finalize(&mut self) {
        if !self.finalized {
            self.entries.sort_unstable();
            self.entries.dedup();
            self.finalized = true;
        }
    }

    /// Header: type tag (1) + u32 LE length + N * u64 LE.
    /// Header itself does not include outer msgpack `bin` framing —
    /// that is added by the protocol layer.
    pub fn encoded_len(&self) -> usize {
        1 + 4 + self.entries.len() * 8
    }

    pub fn encode_into<W: Write>(&self, w: &mut W) -> io::Result<()> {
        debug_assert!(self.finalized, "encode_into before finalize");
        w.write_all(&[TYPE_TAG])?;
        w.write_all(&(self.entries.len() as u32).to_le_bytes())?;
        for h in &self.entries {
            w.write_all(&h.to_le_bytes())?;
        }
        Ok(())
    }

    pub fn contains(&self, hash: u64) -> bool {
        debug_assert!(self.finalized, "contains before finalization");
        self.entries.binary_search(&hash).is_ok()
    }
}

/// Zero-copy view into an encoded SmallSet payload.
///
/// Borrows the byte slice from the DQL wire buffer; valid as long as
/// that buffer is alive.
#[derive(Debug, Clone, Copy)]
pub struct SmallSetView<'a> {
    raw: &'a [u8],
    len: u32,
}

impl<'a> SmallSetView<'a> {
    pub(crate) fn decode(body: &'a [u8]) -> Result<Self, FilterDecodeError> {
        if body.len() < 4 {
            return Err(FilterDecodeError::Truncated {
                need: 4,
                got: body.len(),
            });
        }
        let len = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
        let need = 4 + len as usize * 8;
        if body.len() < need {
            return Err(FilterDecodeError::Truncated {
                need,
                got: body.len(),
            });
        }
        let raw = &body[4..need];
        if raw.len() != len as usize * 8 {
            return Err(FilterDecodeError::ArrayLengthMismatch {
                header: len,
                payload: raw.len() / 8,
            });
        }
        Ok(Self { raw, len })
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn read(&self, idx: usize) -> u64 {
        let off = idx * 8;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.raw[off..off + 8]);
        u64::from_le_bytes(buf)
    }

    pub fn contains(&self, hash: u64) -> bool {
        let n = self.len as usize;
        // Binary search on packed u64 LE slice.
        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let v = self.read(mid);
            match v.cmp(&hash) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return true,
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_roundtrip() {
        let mut s = SmallSet::with_capacity(0);
        s.finalize();
        let mut buf = vec![];
        s.encode_into(&mut buf).unwrap();
        let v = SmallSetView::decode(&buf[1..]).unwrap();
        assert_eq!(v.len(), 0);
        assert!(!v.contains(0));
    }

    #[test]
    fn roundtrip_no_false_negatives() {
        let mut s = SmallSet::with_capacity(8);
        for i in 0..8u64 {
            s.insert(i * 17 + 1);
        }
        s.finalize();
        let mut buf = vec![];
        s.encode_into(&mut buf).unwrap();
        let v = SmallSetView::decode(&buf[1..]).unwrap();
        for i in 0..8u64 {
            assert!(v.contains(i * 17 + 1), "missing {}", i);
        }
        assert!(!v.contains(0));
        assert!(!v.contains(999));
    }

    #[test]
    fn idempotent_insert() {
        let mut s = SmallSet::with_capacity(4);
        s.insert(42);
        s.insert(42);
        s.insert(42);
        s.finalize();
        let mut buf = vec![];
        s.encode_into(&mut buf).unwrap();
        let v = SmallSetView::decode(&buf[1..]).unwrap();
        assert_eq!(v.len(), 1);
        assert!(v.contains(42));
    }
}
