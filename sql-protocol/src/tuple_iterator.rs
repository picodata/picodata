use crate::error::ProtocolError;
use std::io::Cursor;

pub struct TupleIterator<'a> {
    data: Cursor<&'a [u8]>,
    i: usize,
    l: usize,
    error: Option<ProtocolError>,
}

impl<'a> TupleIterator<'a> {
    pub(crate) fn new(data: &'a [u8], l: usize) -> Self {
        Self {
            data: Cursor::new(data),
            i: 0,
            l,
            error: None,
        }
    }

    fn next_tuple(&mut self) -> Option<&'a [u8]> {
        if self.i >= self.l {
            return None;
        }

        let l = match rmp::decode::read_bin_len(&mut self.data) {
            Ok(l) => l,
            Err(err) => {
                self.error = Some(ProtocolError::DecodeError(err.to_string()));
                return None;
            }
        };

        let pos = self.data.position() as usize;
        let new_pos = pos + l as usize;
        if new_pos > self.data.get_ref().len() {
            self.error = Some(ProtocolError::DecodeError(format!(
                "data is not long enough: need ({}), actual ({})",
                new_pos,
                self.data.get_ref().len()
            )));
            return None;
        }
        let current = &self.data.get_ref()[pos..new_pos];
        self.data.set_position(new_pos as u64);

        self.i += 1;

        Some(current)
    }

    pub fn rest_bytes(self) -> Result<&'a [u8], ProtocolError> {
        if let Some(err) = self.error {
            return Err(err);
        }
        let pos = self.data.position() as usize;
        Ok(&self.data.get_ref()[pos..])
    }
}

impl<'a> Iterator for TupleIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.next_tuple()
    }
}
