use super::tuple::ToTupleBuffer;
use crate::unwrap_ok_or;
use crate::Result;
use std::io::{Cursor, Read, Seek, SeekFrom};

pub mod encode;
pub use encode::*;
pub use rmp;

/// Msgpack encoding of `null`.
pub const MARKER_NULL: u8 = 0xc0;

macro_rules! read_be {
    ($r:expr, $ty:ty) => {{
        let mut buf = [0_u8; std::mem::size_of::<$ty>()];
        match std::io::Read::read_exact($r, &mut buf) {
            Ok(()) => Ok(<$ty>::from_be_bytes(buf)),
            Err(e) => Err(e),
        }
    }};
}

macro_rules! slice_read_be_to {
    ($r:expr, $ty:ty, $into:expr) => {{
        let mut buf = [0_u8; std::mem::size_of::<$ty>()];
        match std::io::Read::read_exact($r, &mut buf) {
            Ok(()) => {
                $into.extend_from_slice(&buf);
                Ok(<$ty>::from_be_bytes(buf))
            }
            Err(e) => Err(e),
        }
    }};
}

// TODO: we only ever Seek forward which is equivalent to reading into a buffer
// and discarding the results. We should refactor this and make it accept a
// concrete `&mut [u8]`, which will make it much nicer to use and will improve
// both the build time and the debug perfromance.
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
            cur.seek(SeekFrom::Current(len as _))?;
        }
        Marker::Str8 | Marker::Bin8 => {
            let len = read_be!(cur, u8)?;
            cur.seek(SeekFrom::Current(len as _))?;
        }
        Marker::Str16 | Marker::Bin16 => {
            let len = read_be!(cur, u16)?;
            cur.seek(SeekFrom::Current(len as _))?;
        }
        Marker::Str32 | Marker::Bin32 => {
            let len = read_be!(cur, u32)?;
            cur.seek(SeekFrom::Current(len as _))?;
        }
        Marker::FixArray(len) => {
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::Array16 => {
            let len = read_be!(cur, u16)?;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::Array32 => {
            let len = read_be!(cur, u32)?;
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
            // Multiply by 2, because we skip key, value pairs.
            let len = read_be!(cur, u16)? * 2;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::Map32 => {
            // Multiply by 2, because we skip key, value pairs.
            let len = read_be!(cur, u32)? * 2;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::FixExt1 => {
            // Add 1, because we skip a 1-byte long type designator.
            cur.seek(SeekFrom::Current(1 + 1))?;
        }
        Marker::FixExt2 => {
            // Add 1, because we skip a 1-byte long type designator.
            cur.seek(SeekFrom::Current(2 + 1))?;
        }
        Marker::FixExt4 => {
            // Add 1, because we skip a 1-byte long type designator.
            cur.seek(SeekFrom::Current(4 + 1))?;
        }
        Marker::FixExt8 => {
            // Add 1, because we skip a 1-byte long type designator.
            cur.seek(SeekFrom::Current(8 + 1))?;
        }
        Marker::FixExt16 => {
            // Add 1, because we skip a 1-byte long type designator.
            cur.seek(SeekFrom::Current(16 + 1))?;
        }
        Marker::Ext8 => {
            let len = read_be!(cur, u8)?;
            // Add 1, because we skip a 1-byte long type designator.
            cur.seek(SeekFrom::Current(len as i64 + 1))?;
        }
        Marker::Ext16 => {
            let len = read_be!(cur, u16)?;
            // Add 1, because we skip a 1-byte long type designator.
            cur.seek(SeekFrom::Current(len as i64 + 1))?;
        }
        Marker::Ext32 => {
            let len = read_be!(cur, u32)?;
            // Add 1, because we skip a 1-byte long type designator.
            cur.seek(SeekFrom::Current(len as i64 + 1))?;
        }
        Marker::Reserved => {
            return Err(rmp::decode::ValueReadError::TypeMismatch(Marker::Reserved).into())
        }
    }

    Ok(())
}

/// Reads appropriate amount of bytes according to marker from raw bytes
/// of MessagePack, returning those bytes back in a `Vec<u8>` format.
pub fn preserve_read(from: &mut &[u8]) -> Result<Vec<u8>> {
    use rmp::Marker;

    let mut into = Vec::new();
    match Marker::from_u8(from[0]) {
        Marker::FixPos(_) | Marker::FixNeg(_) | Marker::Null | Marker::True | Marker::False => {
            into.push(from[0]);
            *from = &from[1..];
        }
        Marker::U8 | Marker::I8 => {
            into.extend_from_slice(&from[..=1]);
            *from = &from[1..];
        }
        Marker::U16 | Marker::I16 => {
            into.extend_from_slice(&from[..=2]);
            *from = &from[2..];
        }
        Marker::U32 | Marker::I32 | Marker::F32 => {
            into.extend_from_slice(&from[..=4]);
            *from = &from[4..];
        }
        Marker::U64 | Marker::I64 | Marker::F64 => {
            into.extend_from_slice(&from[..=8]);
            *from = &from[8..];
        }
        Marker::FixStr(len) => {
            into.extend_from_slice(&from[..=len as usize]);
            *from = &from[(len + 1) as usize..];
        }
        Marker::Str8 | Marker::Bin8 => {
            let len = slice_read_be_to!(from, u8, into)?;
            into.extend_from_slice(&from[..=len as usize]);
            *from = &from[(len + 1) as usize..];
        }
        Marker::Str16 | Marker::Bin16 => {
            let len = slice_read_be_to!(from, u16, into)?;
            into.extend_from_slice(&from[..=len as usize]);
            *from = &from[(len + 1) as usize..];
        }
        Marker::Str32 | Marker::Bin32 => {
            let len = slice_read_be_to!(from, u32, into)?;
            into.extend_from_slice(&from[..=len as usize]);
            *from = &from[(len + 1) as usize..];
        }
        Marker::FixArray(len) => {
            into.push(from[0]);
            *from = &from[1..];
            for _ in 0..len {
                into.extend_from_slice(&preserve_read(from)?);
            }
        }
        Marker::Array16 => {
            let len = slice_read_be_to!(from, u16, into)?;
            for _ in 0..len {
                into.extend_from_slice(&preserve_read(from)?);
            }
        }
        Marker::Array32 => {
            let len = slice_read_be_to!(from, u32, into)?;
            for _ in 0..len {
                into.extend_from_slice(&preserve_read(from)?);
            }
        }
        Marker::FixMap(len) => {
            let len = len * 2;
            into.push(from[0]);
            *from = &from[1..];
            for _ in 0..len {
                into.extend_from_slice(&preserve_read(from)?);
            }
        }
        Marker::Map16 => {
            // Multiply by 2, because we skip key, value pairs.
            let len = slice_read_be_to!(from, u16, into)? * 2;
            for _ in 0..len {
                into.extend_from_slice(&preserve_read(from)?);
            }
        }
        Marker::Map32 => {
            // Multiply by 2, because we skip key, value pairs.
            let len = slice_read_be_to!(from, u32, into)? * 2;
            for _ in 0..len {
                into.extend_from_slice(&preserve_read(from)?);
            }
        }
        Marker::FixExt1 => {
            // Add 1, because we skip a 1-byte long type designator.
            into.extend_from_slice(&from[..=(1 + 1)]);
            *from = &from[(1 + 1)..];
        }
        Marker::FixExt2 => {
            // Add 1, because we skip a 1-byte long type designator.
            into.extend_from_slice(&from[..=(2 + 1)]);
            *from = &from[(2 + 1)..];
        }
        Marker::FixExt4 => {
            // Add 1, because we skip a 1-byte long type designator.
            into.extend_from_slice(&from[..=(4 + 1)]);
            *from = &from[(4 + 1)..];
        }
        Marker::FixExt8 => {
            // Add 1, because we skip a 1-byte long type designator.
            into.extend_from_slice(&from[..=(8 + 1)]);
            *from = &from[(8 + 1)..];
        }
        Marker::FixExt16 => {
            // Add 1, because we skip a 1-byte long type designator.
            into.extend_from_slice(&from[..=(16 + 1)]);
            *from = &from[(16 + 1)..];
        }
        Marker::Ext8 => {
            // Add 1, because we skip a 1-byte long type designator.
            let len = slice_read_be_to!(from, u8, into)? as usize + 1;
            into.extend_from_slice(&from[..=len]);
            *from = &from[len..];
        }
        Marker::Ext16 => {
            // Add 1, because we skip a 1-byte long type designator.
            let len = slice_read_be_to!(from, u16, into)? as usize + 1;
            into.extend_from_slice(&from[..=len]);
            *from = &from[len..];
        }
        Marker::Ext32 => {
            // Add 1, because we skip a 1-byte long type designator.
            let len = slice_read_be_to!(from, u32, into)? as usize + 1;
            into.extend_from_slice(&from[..=len]);
            *from = &from[len..];
        }
        Marker::Reserved => {
            return Err(rmp::decode::ValueReadError::TypeMismatch(Marker::Reserved).into())
        }
    }

    Ok(into)
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

/// Initiate a msgpack array of `len`.
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
/// ```no_run
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
/// ```no_run
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
    len: Option<u32>,
    cursor: Cursor<&'a [u8]>,
}

impl<'a> ValueIter<'a> {
    /// Return an iterator over elements of msgpack `array`, or error in case
    /// `array` doesn't start with a valid msgpack array marker.
    #[inline(always)]
    pub fn from_array(array: &'a [u8]) -> std::result::Result<Self, rmp::decode::ValueReadError> {
        let mut cursor = Cursor::new(array);
        let len = rmp::decode::read_array_len(&mut cursor)?;
        Ok(Self {
            len: Some(len),
            cursor,
        })
    }

    /// Return an iterator over msgpack values packed one after another in `data`.
    #[inline(always)]
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            len: None,
            cursor: Cursor::new(data),
        }
    }

    /// Return an iterator over msgpack values packed one after another in `data`.
    #[inline(always)]
    pub fn decode_next<T>(&mut self) -> Option<Result<T>>
    where
        T: crate::tuple::Decode<'a>,
    {
        let data = self.next_raw()?;
        match data {
            Ok(data) => Some(T::decode(data)),
            Err(e) => Some(Err(e)),
        }
    }

    #[inline]
    pub fn next_raw(&mut self) -> Option<Result<&'a [u8]>> {
        if self.cursor.position() as usize >= self.cursor.get_ref().len() {
            return None;
        }
        let start = self.cursor.position() as usize;
        if let Err(e) = skip_value(&mut self.cursor) {
            return Some(Err(e));
        }
        let end = self.cursor.position() as usize;
        debug_assert_ne!(start, end, "skip_value should've returned Err in this case");

        Some(Ok(&self.cursor.get_ref()[start..end]))
    }

    #[inline(always)]
    pub fn into_inner(self) -> Cursor<&'a [u8]> {
        self.cursor
    }

    /// Return the length of the underlying msgpack array if it's known, e.g.
    /// if `self` was created using [`Self::from_array`].
    #[inline(always)]
    pub fn len(&self) -> Option<u32> {
        self.len
    }
}

impl<'a> Iterator for ValueIter<'a> {
    type Item = &'a [u8];

    #[inline(always)]
    fn next(&mut self) -> Option<&'a [u8]> {
        self.next_raw()?.ok()
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if let Some(len) = self.len {
            (len as _, Some(len as _))
        } else {
            (0, None)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ViaMsgpack
////////////////////////////////////////////////////////////////////////////////

/// A wrapper type for passing values between rust and lua by converting them
/// into msgpack first.
///
/// This type may be useful in certain situations including:
/// 1. For types from external crates for which you can't
///    #[derive(tlua::Push, tlua::LuaRead)]
/// 2. For cases when you want the lua representation of the data to be
///    equivalent to the msgpack representation.
///
/// This type is obiously not suitable for performance critical applications
/// because it involves extra encoding & decoding of all the values to & from
/// msgpack.
///
/// This type implements [`tlua::LuaRead`] & [`tlua::Push`] & [`tlua::PushInto`].
///
/// When reading a lua value it will first call `require('msgpack').encode(...)`,
/// then it will read the resulting strign from lua, and then finally it will
/// use [`Decode::decode`] to decode the msgpack into the rust value.
///
/// When pushing a rust value onto the lua stack it will first call
/// [`rmp_serde::to_vec_named`], then it will push the resulting string into lua
/// and then finally it will use `require('msgpack').decode(...)` to decode
/// msgpack into the lua value.
///
/// # Examples
/// ```no_run
/// use std::net::*;
/// use tarantool::msgpack::ViaMsgpack;
///
/// let lua = tarantool::lua_state();
///
/// let v = ViaMsgpack(SocketAddr::from(([93, 184, 216, 34], 80)));
/// lua.exec_with("assert(table.equals(..., { V4 = {{93, 184, 216, 34}, 80} }))", v).unwrap();
///
/// let d: ViaMsgpack<std::time::Duration> = lua.eval("return { 420, 69 }").unwrap();
/// assert_eq!(d.0.as_secs(), 420);
/// assert_eq!(d.0.subsec_nanos(), 69);
/// ```
#[derive(
    Debug,
    Default,
    Clone,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct ViaMsgpack<T>(pub T);

impl<L, T> tlua::Push<L> for ViaMsgpack<T>
where
    L: tlua::AsLua,
    T: serde::Serialize,
{
    type Err = crate::error::Error;

    fn push_to_lua(&self, lua: L) -> tlua::PushResult<L, Self> {
        let res = rmp_serde::to_vec_named(&self.0);
        let mp = unwrap_ok_or!(res,
            Err(e) => {
                return Err((e.into(), lua));
            }
        );

        let res = tlua::LuaFunction::load(lua.as_lua(), "return require('msgpack').decode(...)");
        let decode = unwrap_ok_or!(res,
            Err(e) => {
                return Err((e.into(), lua));
            }
        );

        let res = decode.into_call_with_args(&tlua::AnyLuaString(mp));
        let mp: tlua::Object<_> = unwrap_ok_or!(res,
            Err(e) => {
                return Err((tlua::LuaError::from(e).into(), lua))
            }
        );

        // Safety: this is safe as long as the assertions hold
        unsafe {
            // Stack:
            // ..: ...
            // -3: msgpack.decode -- we dont want this
            // -2: result         -- we want this
            // -1: position       -- we dont want this

            let res_guard = mp.into_guard();
            // It says 2 even though there's 3 values guarded by res_guard,
            // because there's a nested guard inside, and this stupid library
            // doesn't have a way of getting the total size because of
            // parametric polymorphism...
            debug_assert_eq!(res_guard.size(), 2);

            // Now we have to shuffle the stack into this configuration:
            // ..: ...
            // -3: result
            // -2: msgpack.decode
            // -1: position
            //
            // NOTE: We have to do this because this stupid library
            // will not let us disable the nested push guard without popping
            // the values guarded by the outter push guard...
            tlua::ffi::lua_insert(lua.as_lua(), -2); // Swap result <-> position
            tlua::ffi::lua_insert(lua.as_lua(), -3); // Put result above msgpack.decode

            // This call to PushGuard::into_inner will pop 2 topmost values off
            // the stack, basically this is the spot which makes us do stupid
            // things above
            let res_guard = res_guard.into_inner().into_inner();

            // This push guard was originally guarding the msgpack.decode
            // function, but it doesn't know anything about what it's guarding,
            // it only knows the number of values it must pop off the stack.
            //
            // Anyway we have to `forget` it because of the stupid parametric
            // polymorphism, because it's type doesn't match the return type of
            // this function.
            let size = res_guard.forget();
            debug_assert_eq!(size, 1);

            Ok(tlua::PushGuard::new(lua, 1))
        }
    }
}
impl<L, T> tlua::PushOne<L> for ViaMsgpack<T>
where
    L: tlua::AsLua,
    T: serde::Serialize,
{
}

impl<L, T> tlua::PushInto<L> for ViaMsgpack<T>
where
    L: tlua::AsLua,
    T: serde::Serialize,
{
    type Err = crate::error::Error;

    #[inline(always)]
    fn push_into_lua(self, lua: L) -> tlua::PushIntoResult<L, Self> {
        tlua::Push::push_to_lua(&self, lua)
    }
}
impl<L, T> tlua::PushOneInto<L> for ViaMsgpack<T>
where
    L: tlua::AsLua,
    T: serde::Serialize,
{
}

impl<L, T> tlua::LuaRead<L> for ViaMsgpack<T>
where
    L: tlua::AsLua,
    T: for<'de> serde::Deserialize<'de>,
{
    fn lua_read_at_position(lua: L, index: std::num::NonZeroI32) -> tlua::ReadResult<Self, L> {
        use crate::tuple::Decode;

        use tlua::AsLua;
        let res = (&lua).read_at_nz(index);
        let object: tlua::Object<_> = unwrap_ok_or!(res,
            Err((_, e)) => {
                return Err((lua, e));
            }
        );

        const ERROR_INFO: &str = "reading lua value via msgpack";
        let res = tlua::LuaFunction::load(lua.as_lua(), "return require('msgpack').encode(...)");
        let encode = unwrap_ok_or!(res,
            Err(e) => {
                return Err((
                    lua,
                    tlua::WrongType::info(ERROR_INFO)
                        .expected("require('msgpack').encode(...)")
                        .actual(e.to_string())
                ))
            }
        );

        let res = encode.into_call_with_args(object);
        let mp: tlua::AnyLuaString = unwrap_ok_or!(res,
            Err(e) => {
                return Err((
                    lua,
                    tlua::WrongType::info(ERROR_INFO)
                        .expected("successful conversion to msgpack")
                        .actual(e.to_string())
                ))
            }
        );

        let res = T::decode(mp.as_bytes());
        match res {
            Err(crate::error::Error::Decode { error, .. }) => {
                return Err((
                    lua,
                    tlua::WrongType::info(ERROR_INFO)
                        .expected_type::<T>()
                        .actual(format!(
                            "error: {error}; when decoding msgpack {}",
                            crate::util::DisplayAsHexBytes(mp.as_bytes())
                        )),
                ))
            }
            Err(e) => {
                return Err((
                    lua,
                    tlua::WrongType::info(ERROR_INFO)
                        .expected("successful msgpack conversion")
                        .actual(format!("error: {e}")),
                ))
            }
            Ok(v) => {
                return Ok(ViaMsgpack(v));
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// test
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use super::*;
    use rmpv::Value;

    #[test]
    fn skip_value() {
        let data = [
            Value::Map(vec![(Value::from("something"), Value::from(true))]),
            Value::Array(vec![Value::from(42), Value::from(52)]),
            Value::from("anything"),
            Value::from(4019497904u32),
            Value::Ext(42, vec![1, 2, 3, 4, 5]),
            Value::Nil,
        ];
        let len = data.len() as usize;

        let mut bytes = Vec::new();
        for info in &data {
            rmp_serde::encode::write(&mut bytes, &info).unwrap();
        }

        let mut reader = Cursor::new(bytes);
        for info in &data[..(len - 1)] {
            let value: Value = rmp_serde::from_read(reader.clone()).unwrap();
            assert_eq!(&value, info);
            super::skip_value(&mut reader).unwrap();
        }

        super::skip_value(&mut reader).unwrap();
        let err = rmp_serde::from_read::<_, Value>(reader.clone()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "IO error while reading marker: failed to fill whole buffer"
        );
    }

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
        assert_eq!(iter.len(), None);
        assert_eq!(iter.next(), None);

        let mut iter = ValueIter::new(b"*");
        assert_eq!(iter.len(), None);
        assert_eq!(iter.next(), Some(&b"*"[..]));
        assert_eq!(iter.next(), None);

        let err = ValueIter::from_array(b"").unwrap_err();
        assert_eq!(err.to_string(), "failed to read MessagePack marker");

        let mut iter = ValueIter::from_array(b"\x99").unwrap();
        assert_eq!(iter.len(), Some(9));
        assert_eq!(iter.next(), None);

        let mut iter = ValueIter::from_array(b"\x99*").unwrap();
        assert_eq!(iter.len(), Some(9));
        assert_eq!(iter.next(), Some(&b"*"[..]));
        assert_eq!(iter.next(), None);

        let data = b"\x93*\x93\xc0\xc2\xc3\xa3sup";

        let mut iter = ValueIter::from_array(data).unwrap();
        assert_eq!(iter.len(), Some(3));
        let v: u32 = iter.decode_next().unwrap().unwrap();
        assert_eq!(v, 42);
        let v: Vec<Option<bool>> = iter.decode_next().unwrap().unwrap();
        assert_eq!(v, [None, Some(false), Some(true)]);
        let v: String = iter.decode_next().unwrap().unwrap();
        assert_eq!(v, "sup");

        let mut iter = ValueIter::from_array(data).unwrap();
        assert_eq!(iter.len(), Some(3));
        let v = iter.next().unwrap();
        assert_eq!(v, b"*");
        let v = iter.next().unwrap();
        assert_eq!(v, b"\x93\xc0\xc2\xc3");
        let v = iter.next().unwrap();
        assert_eq!(v, b"\xa3sup");

        let mut iter = ValueIter::new(data);
        assert_eq!(iter.len(), None);
        let v: (u32, Vec<Option<bool>>, String) =
            rmp_serde::from_slice(iter.next().unwrap()).unwrap();
        assert_eq!(v, (42, vec![None, Some(false), Some(true)], "sup".into()));
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::result::Result;

    #[crate::test(tarantool = "crate")]
    fn via_msgpack() {
        let lua = crate::lua_state();

        //
        // serialize
        //

        // struct
        #[derive(serde::Serialize)]
        struct S {
            a: String,
            b: i32,
            c: (bool, (), f32),
        }

        let v = ViaMsgpack(S {
            a: "foo".into(),
            b: 420,
            c: (true, (), 13.37),
        });
        let t: tlua::LuaTable<_> = lua.eval_with("return ...", v).unwrap();
        let s: String = t.get("a").unwrap();
        assert_eq!(s, "foo");
        let b: i32 = t.get("b").unwrap();
        assert_eq!(b, 420);
        {
            let c: tlua::LuaTable<_> = t.get("c").unwrap();
            let c_1: bool = c.get(1).unwrap();
            assert_eq!(c_1, true);
            let _c_2: tlua::Null = c.get(2).unwrap();
            let c_3: f32 = c.get(3).unwrap();
            assert_eq!(c_3, 13.37);
        }

        {
            // enum
            #[derive(serde::Serialize)]
            enum E {
                A,
                B(i32, i32),
                C {
                    foo: &'static str,
                    bar: &'static str,
                },
            }

            let j: String = lua
                .eval_with("return require'json'.encode(...)", ViaMsgpack(E::A))
                .unwrap();
            assert_eq!(j, "\"A\"");

            let j: String = lua
                .eval_with("return require'json'.encode(...)", ViaMsgpack(E::B(13, 37)))
                .unwrap();
            assert_eq!(j, r#"{"B":[13,37]}"#);

            let j: String = lua
                .eval_with(
                    "return require'json'.encode(...)",
                    ViaMsgpack(E::C {
                        foo: "hello",
                        bar: "jason",
                    }),
                )
                .unwrap();
            assert_eq!(j, r#"{"C":{"foo":"hello","bar":"jason"}}"#);
        }

        //
        // deserialize
        //

        #[derive(Debug, PartialEq, serde::Deserialize)]
        struct D {
            a: String,
            b: i32,
            c: (bool, (), f32),
        }

        let d: ViaMsgpack<D> = lua
            .eval("return { a = 'bar', b = 69, c = { true, box.NULL, 3.14 } }")
            .unwrap();
        assert_eq!(
            d.0,
            D {
                a: "bar".into(),
                b: 69,
                c: (true, (), 3.14)
            },
        );

        // errors
        let d: Result<ViaMsgpack<D>, _> = lua.eval("return { a = 'bar' }");
        assert_eq!(
            d.unwrap_err().to_string(),
            r#"failed reading lua value via msgpack: tarantool::msgpack::tests::via_msgpack::D expected, got error: missing field `b`; when decoding msgpack b"\x81\xa1a\xa3bar"
    while reading value(s) returned by Lua: tarantool::msgpack::ViaMsgpack<tarantool::msgpack::tests::via_msgpack::D> expected, got table"#,
        );

        let d: Result<ViaMsgpack<D>, _> = lua
            // tostring is a function, it can't be encoded as msgpack
            .eval("return { a = tostring }");
        assert_eq!(
            d.unwrap_err().to_string(),
            r#"failed reading lua value via msgpack: successful conversion to msgpack expected, got Lua error: unsupported Lua type 'function'
    while reading value(s) returned by Lua: tarantool::msgpack::ViaMsgpack<tarantool::msgpack::tests::via_msgpack::D> expected, got table"#,
        );

        // This is ok in the rmp_serde version we're using.
        let d: Result<ViaMsgpack<D>, _> =
            lua.eval("return { a = '', b = 0, c = { true, box.NULL, 0 }, d = 'unexpected' }");
        assert!(d.is_ok());

        {
            // enum
            #[derive(serde::Deserialize, PartialEq, Debug)]
            enum E {
                Foo,
                Bar(i32, i32),
                Car { foo: String, bar: String },
            }

            let e: ViaMsgpack<E> = lua.eval("return 'Foo'").unwrap();
            assert_eq!(e.0, E::Foo);

            let e: ViaMsgpack<E> = lua.eval("return { Bar = { 18, 84 } }").unwrap();
            assert_eq!(e.0, E::Bar(18, 84));

            let e: ViaMsgpack<E> = lua
                .eval("return { Car = { foo = 'f', bar = 'u' } }")
                .unwrap();
            assert_eq!(
                e.0,
                E::Car {
                    foo: "f".into(),
                    bar: "u".into()
                }
            );

            let e: Result<ViaMsgpack<E>, _> = lua.eval("return { NoSuchTag = { 1, 2, 3 } }");
            assert_eq!(
                e.unwrap_err().to_string(),
                r#"failed reading lua value via msgpack: tarantool::msgpack::tests::via_msgpack::E expected, got error: unknown variant `NoSuchTag`, expected one of `Foo`, `Bar`, `Car`; when decoding msgpack b"\x81\xa9NoSuchTag\x93\x01\x02\x03"
    while reading value(s) returned by Lua: tarantool::msgpack::ViaMsgpack<tarantool::msgpack::tests::via_msgpack::E> expected, got table"#,
            );
        }

        //
        // support for values from external crates (std is not external though...)
        //

        // serialize
        use std::net::*;
        let a = SocketAddr::from(([93, 184, 216, 34], 80));
        let j: String = lua
            .eval_with("return require'json'.encode(...)", ViaMsgpack(a))
            .unwrap();
        assert_eq!(j, r#"{"V4":[[93,184,216,34],80]}"#);

        // deserialize
        let a: ViaMsgpack<SocketAddr> = lua
            .eval(
                "return { V6 = { { 32, 1, 13, 184, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 }, 8080 } }",
            )
            .unwrap();
        assert_eq!(
            a.0,
            SocketAddr::from(SocketAddrV6::new(
                [0x2001, 0xdb8, 0, 0, 0, 0, 0, 1].into(),
                8080,
                0,
                0
            )),
        );

        let d: ViaMsgpack<std::time::Duration> = lua.eval("return {420, 69}").unwrap();
        assert_eq!(d.0.as_secs(), 420);
        assert_eq!(d.0.subsec_nanos(), 69);
    }
}
