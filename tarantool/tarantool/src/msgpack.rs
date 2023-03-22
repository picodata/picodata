use std::io::Cursor;
use std::io::{Read, Seek, SeekFrom};

use byteorder::{BigEndian, ReadBytesExt};

use super::tuple::{Decode, RawBytes, ToTupleBuffer};
use crate::Result;

pub fn skip_value(cur: &mut (impl Read + Seek)) -> Result<()> {
    use rmp::Marker;

    match rmp::decode::read_marker(cur)? {
        Marker::FixPos(_) | Marker::FixNeg(_) | Marker::Null | Marker::True | Marker::False => {}
        Marker::U8 | Marker::I8 => {
            cur.seek(SeekFrom::Current(1))?;
        }
        Marker::U16 | Marker::I16 => {
            cur.seek(SeekFrom::Current(2))?;
        }
        Marker::U32 | Marker::I32 | Marker::F32 => {
            cur.seek(SeekFrom::Current(4))?;
        }
        Marker::U64 | Marker::I64 | Marker::F64 => {
            cur.seek(SeekFrom::Current(8))?;
        }
        Marker::FixStr(len) => {
            cur.seek(SeekFrom::Current(len as i64))?;
        }
        Marker::Str8 | Marker::Bin8 => {
            let len = cur.read_u8()?;
            cur.seek(SeekFrom::Current(len as i64))?;
        }
        Marker::Str16 | Marker::Bin16 => {
            let len = cur.read_u16::<BigEndian>()?;
            cur.seek(SeekFrom::Current(len as i64))?;
        }
        Marker::Str32 | Marker::Bin32 => {
            let len = cur.read_u32::<BigEndian>()?;
            cur.seek(SeekFrom::Current(len as i64))?;
        }
        Marker::FixArray(len) => {
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::Array16 => {
            let len = cur.read_u16::<BigEndian>()?;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::Array32 => {
            let len = cur.read_u32::<BigEndian>()?;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::FixMap(len) => {
            let len = len * 2;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::Map16 => {
            let len = cur.read_u16::<BigEndian>()? * 2;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::Map32 => {
            let len = cur.read_u32::<BigEndian>()? * 2;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::FixExt1 => {
            cur.seek(SeekFrom::Current(2))?;
        }
        Marker::FixExt2 => {
            cur.seek(SeekFrom::Current(3))?;
        }
        Marker::FixExt4 => {
            cur.seek(SeekFrom::Current(5))?;
        }
        Marker::FixExt8 => {
            cur.seek(SeekFrom::Current(9))?;
        }
        Marker::FixExt16 => {
            cur.seek(SeekFrom::Current(17))?;
        }
        Marker::Ext8 => {
            let len = cur.read_u8()?;
            cur.seek(SeekFrom::Current(len as i64 + 1))?;
        }
        Marker::Ext16 => {
            let len = cur.read_u16::<BigEndian>()?;
            cur.seek(SeekFrom::Current(len as i64 + 1))?;
        }
        Marker::Ext32 => {
            let len = cur.read_u32::<BigEndian>()?;
            cur.seek(SeekFrom::Current(len as i64 + 1))?;
        }
        Marker::Reserved => {
            return Err(rmp::decode::ValueReadError::TypeMismatch(Marker::Reserved).into())
        }
    }
    Ok(())
}

/// Write to `w` a msgpack array with values from `arr`.
pub fn write_array<T>(w: &mut impl std::io::Write, arr: &[T]) -> Result<()>
where
    T: ToTupleBuffer,
{
    rmp::encode::write_array_len(w, arr.len() as _)?;
    for elem in arr {
        elem.write_tuple_data(w)?;
    }
    Ok(())
}

/// Initiate a msgpack array of `len`
pub fn write_array_len(
    w: &mut impl std::io::Write,
    len: u32,
) -> std::result::Result<(), rmp::encode::ValueWriteError> {
    rmp::encode::write_array_len(w, len)?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// ArrayWriter
////////////////////////////////////////////////////////////////////////////////

/// A helper struct for serializing msgpack arrays from arbitrary serializable
/// types.
///
/// Call [`ArrayWriter::finish`] to finilize the msgpack array and get the
/// underlying `writer` struct.
///
/// # Example
/// ```
/// use tarantool::msgpack::ArrayWriter;
/// let mut array_writer = ArrayWriter::from_vec(vec![]);
/// array_writer.push(&1).unwrap();
/// array_writer.push(&("foo", "bar")).unwrap();
/// array_writer.push(&true).unwrap();
/// let cursor = array_writer.finish().unwrap();
/// let data = cursor.into_inner();
/// assert_eq!(data, b"\xdd\x00\x00\x00\x03\x01\x92\xa3foo\xa3bar\xc3");
/// ```
#[derive(Debug)]
pub struct ArrayWriter<W> {
    /// The underlying writer, into which the data is written.
    writer: W,
    /// Stream position of `writer` when `self` was created.
    start: u64,
    /// Current length of the msgpack array.
    ///
    /// NOTE: Msgpack max array size is 2³² - 1.
    len: u32,
}

impl ArrayWriter<Cursor<Vec<u8>>> {
    /// Create an `ArrayWriter` using a `Vec<u8>` as the underlying buffer.
    #[track_caller]
    #[inline(always)]
    pub fn from_vec(buf: Vec<u8>) -> Self {
        Self::new(Cursor::new(buf)).expect("allocation error")
    }
}

impl<W> ArrayWriter<W>
where
    W: std::io::Write + std::io::Seek,
{
    const MAX_ARRAY_HEADER_SIZE: i64 = 5;

    #[inline(always)]
    pub fn new(mut writer: W) -> Result<Self> {
        // Leave space for array size
        let start = writer.stream_position()?;
        writer.seek(SeekFrom::Current(Self::MAX_ARRAY_HEADER_SIZE))?;
        Ok(Self {
            start,
            writer,
            len: 0,
        })
    }

    /// Stream position of `writer` when `self` was created.
    #[inline(always)]
    pub fn start(&self) -> u64 {
        self.start
    }

    /// Current length of the msgpack array.
    ///
    /// NOTE: Msgpack max array size is 2³² - 1.
    #[inline(always)]
    pub fn len(&self) -> u32 {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn into_inner(self) -> W {
        self.writer
    }

    /// Push a type that can be serialized as a msgpack value.
    #[inline(always)]
    pub fn push<T>(&mut self, v: &T) -> Result<()>
    where
        T: ::serde::Serialize + ?Sized,
    {
        rmp_serde::encode::write(&mut self.writer, &v)?;
        self.len += 1;
        Ok(())
    }

    /// Push a type representable as a tarantool tuple.
    #[inline(always)]
    pub fn push_tuple<T>(&mut self, v: &T) -> Result<()>
    where
        T: ToTupleBuffer + ?Sized,
    {
        v.write_tuple_data(&mut self.writer)?;
        self.len += 1;
        Ok(())
    }

    /// Push arbitrary bytes as a msgpack array element.
    ///
    /// NOTE: The user must make sure to push a valid msgpack value.
    #[inline(always)]
    pub fn push_raw(&mut self, v: &[u8]) -> Result<()> {
        self.writer.write_all(v)?;
        self.len += 1;
        Ok(())
    }

    /// Finilize the msgpack array and return the underlying writer.
    pub fn finish(mut self) -> Result<W> {
        use rmp::encode::RmpWrite;

        self.writer.seek(SeekFrom::Start(self.start))?;
        self.writer.write_u8(rmp::Marker::Array32.to_u8())?;
        self.writer
            .write_data_u32(self.len)
            .map_err(rmp::encode::ValueWriteError::from)?;
        Ok(self.writer)
    }
}

////////////////////////////////////////////////////////////////////////////////
// ArrayIter
////////////////////////////////////////////////////////////////////////////////

/// A helper struct for iterating over msgpack values.
///
/// # Example
/// ```
/// use tarantool::msgpack::ValueIter;
/// let mut value_iter = ValueIter::from_array(b"\x93*\xc0\xa3yes").unwrap();
/// // You can decode the next value
/// assert_eq!(value_iter.decode_next::<i64>().map(Result::ok).flatten(), Some(42));
/// // Or just get the raw slice of bytes
/// assert_eq!(value_iter.next(), Some(&b"\xc0"[..]));
/// assert_eq!(value_iter.decode_next::<String>().map(Result::ok).flatten(), Some("yes".to_owned()));
///
/// // Returns None when there's no more values
/// assert_eq!(value_iter.decode_next::<String>().map(Result::ok), None);
/// assert_eq!(value_iter.next(), None);
/// ```
#[derive(Debug)]
pub struct ValueIter<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl<'a> ValueIter<'a> {
    /// Return an iterator over elements of msgpack `array`, or error in case
    /// `array` doesn't start with a valid msgpack array marker.
    pub fn from_array(array: &'a [u8]) -> std::result::Result<Self, rmp::decode::ValueReadError> {
        let mut cursor = Cursor::new(array);
        // Don't care about length, will just exhaust all the values in the slice
        rmp::decode::read_array_len(&mut cursor)?;
        Ok(Self { cursor })
    }

    /// Return an iterator over msgpack values packed one after another in `data`.
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            cursor: Cursor::new(data),
        }
    }

    /// Return an iterator over msgpack values packed one after another in `data`.
    pub fn decode_next<T>(&mut self) -> Option<Result<T>>
    where
        T: Decode<'a>,
    {
        if self.cursor.position() as usize >= self.cursor.get_ref().len() {
            return None;
        }
        let start = self.cursor.position() as usize;
        if let Err(e) = skip_value(&mut self.cursor) {
            return Some(Err(e));
        }
        let end = self.cursor.position() as usize;
        debug_assert_ne!(start, end, "skip_value should've returned Err in this case");

        let data = &self.cursor.get_ref()[start..end];
        Some(T::decode(data))
    }

    pub fn into_inner(self) -> Cursor<&'a [u8]> {
        self.cursor
    }
}

impl<'a> Iterator for ValueIter<'a> {
    type Item = &'a [u8];

    #[inline(always)]
    fn next(&mut self) -> Option<&'a [u8]> {
        self.decode_next::<&RawBytes>()?.ok().map(|b| &**b)
    }
}

////////////////////////////////////////////////////////////////////////////////
// test
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn array_writer() {
        let mut aw = ArrayWriter::from_vec(Vec::new());
        aw.push_tuple(&(420, "foo")).unwrap();
        aw.push(&"bar").unwrap();
        aw.push_raw(b"\xa3baz").unwrap();
        let data = aw.finish().unwrap().into_inner();
        eprintln!("{:x?}", &data);
        let res: ((u32, String), String, String) = rmp_serde::from_slice(&data).unwrap();
        assert_eq!(
            res,
            ((420, "foo".to_owned()), "bar".to_owned(), "baz".to_owned())
        );
    }

    #[test]
    fn value_iter() {
        let mut iter = ValueIter::new(b"");
        assert_eq!(iter.next(), None);

        let mut iter = ValueIter::new(b"*");
        assert_eq!(iter.next(), Some(&b"*"[..]));
        assert_eq!(iter.next(), None);

        let err = ValueIter::from_array(b"").unwrap_err();
        assert_eq!(err.to_string(), "failed to read MessagePack marker");

        let mut iter = ValueIter::from_array(b"\x99").unwrap();
        assert_eq!(iter.next(), None);

        let mut iter = ValueIter::from_array(b"\x99*").unwrap();
        assert_eq!(iter.next(), Some(&b"*"[..]));
        assert_eq!(iter.next(), None);

        let data = b"\x93*\x93\xc0\xc2\xc3\xa3sup";

        let mut iter = ValueIter::from_array(data).unwrap();
        let v: u32 = iter.decode_next().unwrap().unwrap();
        assert_eq!(v, 42);
        let v: Vec<Option<bool>> = iter.decode_next().unwrap().unwrap();
        assert_eq!(v, [None, Some(false), Some(true)]);
        let v: String = iter.decode_next().unwrap().unwrap();
        assert_eq!(v, "sup");

        let mut iter = ValueIter::from_array(data).unwrap();
        let v = iter.next().unwrap();
        assert_eq!(v, b"*");
        let v = iter.next().unwrap();
        assert_eq!(v, b"\x93\xc0\xc2\xc3");
        let v = iter.next().unwrap();
        assert_eq!(v, b"\xa3sup");

        let mut iter = ValueIter::new(data);
        let v: (u32, Vec<Option<bool>>, String) =
            rmp_serde::from_slice(iter.next().unwrap()).unwrap();
        assert_eq!(v, (42, vec![None, Some(false), Some(true)], "sup".into()));
    }
}
