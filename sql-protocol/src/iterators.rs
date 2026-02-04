use crate::dql_encoder::MsgpackEncode;
use crate::error::ProtocolError;
use std::io::{Cursor, Write};

pub struct MsgpackMapIterator<'a, K, V> {
    count: u32,
    current: u32,
    raw_msgpack: Cursor<&'a [u8]>,
    decode_key_f: fn(&mut Cursor<&'a [u8]>) -> Result<K, ProtocolError>,
    decode_value_f: fn(&mut Cursor<&'a [u8]>) -> Result<V, ProtocolError>,
}

impl<'a, K, V> MsgpackMapIterator<'a, K, V> {
    pub(crate) fn new(
        raw_msgpack: &'a [u8],
        count: u32,
        decode_key_f: fn(&mut Cursor<&'a [u8]>) -> Result<K, ProtocolError>,
        decode_value_f: fn(&mut Cursor<&'a [u8]>) -> Result<V, ProtocolError>,
    ) -> Self {
        Self {
            count,
            current: 0,
            raw_msgpack: Cursor::new(raw_msgpack),
            decode_key_f,
            decode_value_f,
        }
    }

    pub fn empty() -> Self {
        Self {
            count: 0,
            current: 0,
            raw_msgpack: Cursor::new(&[]),
            decode_key_f: |_| Err(ProtocolError::DecodeError("empty map".to_string())),
            decode_value_f: |_| Err(ProtocolError::DecodeError("empty map".to_string())),
        }
    }
}

impl<K, V> Iterator for MsgpackMapIterator<'_, K, V> {
    type Item = Result<(K, V), ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.count {
            return None;
        }

        let key = match (self.decode_key_f)(&mut self.raw_msgpack) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };
        let value = match (self.decode_value_f)(&mut self.raw_msgpack) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };

        self.current += 1;
        Some(Ok((key, value)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let m = self.count - self.current;
        (m as usize, Some(m as usize))
    }
}

impl<K, V> ExactSizeIterator for MsgpackMapIterator<'_, K, V> {}

impl<K, V> Clone for MsgpackMapIterator<'_, K, V> {
    fn clone(&self) -> Self {
        Self {
            count: self.count,
            current: self.current,
            raw_msgpack: self.raw_msgpack.clone(),
            decode_key_f: self.decode_key_f,
            decode_value_f: self.decode_value_f,
        }
    }
}

pub struct MsgpackArrayIterator<'a, T> {
    count: u32,
    current: u32,
    raw_msgpack: Cursor<&'a [u8]>,
    decode_f: fn(&mut Cursor<&'a [u8]>) -> Result<T, ProtocolError>,
}

impl<'a, T> MsgpackArrayIterator<'a, T> {
    pub(crate) fn new(
        raw_msgpack: &'a [u8],
        count: u32,
        decode_f: fn(&mut Cursor<&'a [u8]>) -> Result<T, ProtocolError>,
    ) -> Self {
        Self {
            count,
            current: 0,
            raw_msgpack: Cursor::new(raw_msgpack),
            decode_f,
        }
    }
}

impl<T> Iterator for MsgpackArrayIterator<'_, T> {
    type Item = Result<T, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.count {
            return None;
        }

        let value = match (self.decode_f)(&mut self.raw_msgpack) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };

        self.current += 1;
        Some(Ok(value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let m = self.count - self.current;
        (m as usize, Some(m as usize))
    }
}

impl<T> ExactSizeIterator for MsgpackArrayIterator<'_, T> {}

pub struct TupleIterator<'a> {
    data: Cursor<&'a [u8]>,
    current_row: usize,
    num_rows: usize,
}

impl<'a> TupleIterator<'a> {
    pub(crate) fn new(data: &'a [u8], num_rows: usize) -> Self {
        Self {
            data: Cursor::new(data),
            current_row: 0,
            num_rows,
        }
    }

    pub fn len(&self) -> usize {
        self.num_rows
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }
}

impl<'a> Iterator for TupleIterator<'a> {
    type Item = Result<&'a [u8], ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row >= self.num_rows {
            return None;
        }

        let len = match rmp::decode::read_bin_len(&mut self.data) {
            Ok(l) => l,
            Err(err) => {
                return Some(Err(ProtocolError::DecodeError(err.to_string())));
            }
        };

        let start = self.data.position() as usize;
        let end = start + len as usize;
        if end > self.data.get_ref().len() {
            return Some(Err(ProtocolError::DecodeError(format!(
                "data is not long enough: need ({}), actual ({})",
                end,
                self.data.get_ref().len()
            ))));
        }
        let current = &self.data.get_ref()[start..end];
        self.data.set_position(end as u64);

        self.current_row += 1;

        Some(Ok(current))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let m = self.num_rows - self.current_row;
        (m, Some(m))
    }
}

#[allow(dead_code)]
pub(crate) struct TestPureTupleEncoder<'e> {
    data: &'e Vec<u64>,
}

impl<'e> TestPureTupleEncoder<'e> {
    #[allow(dead_code)]
    pub(crate) fn new(data: &'e Vec<u64>) -> Self {
        Self { data }
    }
}

impl MsgpackEncode for TestPureTupleEncoder<'_> {
    fn encode_into(&self, w: &mut impl Write) -> std::io::Result<()> {
        rmp::encode::write_array_len(w, self.data.len() as u32)?;

        for elem in self.data {
            rmp::encode::write_uint(w, *elem)?;
        }

        Ok(())
    }
}

// Iterator for a port containing result of EXPLAIN (RAW) query.
// All rows must be Vec<String> with at least one element. The general
// structure of port is Vec<Vec<String>>.
pub struct ExplainIter {
    explain: std::vec::IntoIter<String>,
    idx: u64,
}

impl ExplainIter {
    pub fn new<'a>(port: impl Iterator<Item = &'a [u8]>) -> Self {
        let explain_rows: Vec<Vec<String>> = port
            .into_iter()
            .map(|tuple| rmp_serde::decode::from_slice(tuple).expect("expected vec of strings"))
            .collect();

        let mut explain = Vec::new();
        for line_vec in explain_rows {
            for line in line_vec[0].split('\n') {
                explain.push(line.to_string());
            }
        }

        ExplainIter {
            explain: explain.into_iter(),
            idx: 0,
        }
    }
}

impl Iterator for ExplainIter {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let line = self.explain.next()?;

        if line.starts_with("Query") {
            self.idx += 1;
            Some(format!("{}. {}", self.idx, line))
        } else {
            Some(line)
        }
    }
}
