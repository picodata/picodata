use crate::dql_encoder::MsgpackWriter;
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

    pub fn len(&self) -> u32 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
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

    pub fn len(&self) -> u32 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
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
pub(crate) struct TestTuplesWriter<'tw> {
    current: Option<&'tw Vec<u64>>,
    iter: std::slice::Iter<'tw, Vec<u64>>,
}

impl<'tw> TestTuplesWriter<'tw> {
    #[allow(dead_code)]
    pub(crate) fn new(iter: std::slice::Iter<'tw, Vec<u64>>) -> Self {
        Self {
            current: None,
            iter,
        }
    }
}

impl MsgpackWriter for TestTuplesWriter<'_> {
    fn write_current(&self, w: &mut impl Write) -> std::io::Result<()> {
        let Some(elem) = self.current else {
            return Ok(());
        };

        rmp::encode::write_array_len(w, elem.len() as u32)?;

        for elem in elem {
            rmp::encode::write_uint(w, *elem)?;
        }

        Ok(())
    }

    fn next(&mut self) -> Option<()> {
        self.current = self.iter.next();

        self.current?;

        Some(())
    }

    fn len(&self) -> usize {
        self.iter.len()
    }
}
