use crate::ffi::datetime as ffi;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use time::{Duration, UtcOffset};

type Inner = time::OffsetDateTime;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("incorrect timestamp value")]
    WrongUnixTimestamp(time::error::ComponentRange),
    #[error("incorrect offset value")]
    WrongUtcOffset(time::error::ComponentRange),
}

/// A Datetime type implemented using the builtin tarantool api. **Note** that
/// this api is not available in all versions of tarantool.
/// Use [`tarantool::ffi::has_datetime`] to check if it is supported in your
/// case.
/// If `has_datetime` return `false`, using functions from this module
/// may result in a **panic**.
///
/// [`tarantool::ffi::has_datetime`]: crate::ffi::has_datetime
#[derive(Debug, Copy, Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Datetime {
    inner: Inner,
}

impl Datetime {
    #[inline(always)]
    pub fn from_inner(inner: Inner) -> Self {
        inner.into()
    }

    #[inline(always)]
    pub fn into_inner(self) -> Inner {
        self.into()
    }

    /// Convert an array of bytes (internal tarantool msgpack ext)
    /// in the little endian order into a `DateTime`.
    #[inline(always)]
    fn from_bytes_tt(bytes: &[u8]) -> Result<Self, Error> {
        let mut sec_bytes: [u8; 8] = [0; 8];
        sec_bytes.copy_from_slice(&bytes[0..8]);

        let mut nsec_bytes: [u8; 4] = [0; 4];
        let mut tzoffest_bytes: [u8; 2] = [0; 2];
        if bytes.len() == 16 {
            nsec_bytes.copy_from_slice(&bytes[8..12]);
            tzoffest_bytes.copy_from_slice(&bytes[12..14]);
        }

        let secs = i64::from_le_bytes(sec_bytes);
        let nsecs = u32::from_le_bytes(nsec_bytes);
        let tzoffset: i32 = i16::from_le_bytes(tzoffest_bytes).into();

        let utc_offset =
            UtcOffset::from_whole_seconds(tzoffset * 60).map_err(Error::WrongUtcOffset)?;
        let dt = Inner::from_unix_timestamp(secs)
            .map_err(Error::WrongUnixTimestamp)?
            .to_offset(utc_offset)
            + Duration::nanoseconds(nsecs as i64);

        Ok(dt.into())
    }

    /// Return an array of bytes (internal tarantool msgpack ext) in the little endian order.
    #[inline(always)]
    fn as_bytes_tt(&self) -> [u8; 16] {
        let mut buf: [u8; 16] = [0; 16];

        buf[0..8].copy_from_slice(&self.inner.unix_timestamp().to_le_bytes());
        buf[8..12].copy_from_slice(&self.inner.nanosecond().to_le_bytes());
        buf[12..14].copy_from_slice(&self.inner.offset().whole_minutes().to_le_bytes());

        buf
    }

    #[inline(always)]
    pub fn from_ffi_dt(inner: ffi::datetime) -> Result<Self, Error> {
        let utc_offset = UtcOffset::from_whole_seconds((inner.tzoffset * 60).into())
            .map_err(Error::WrongUtcOffset)?;
        let dt = Inner::from_unix_timestamp(inner.epoch as i64)
            .map_err(Error::WrongUnixTimestamp)?
            .to_offset(utc_offset)
            + Duration::nanoseconds(inner.nsec as i64);

        Ok(dt.into())
    }

    #[inline(always)]
    pub fn as_ffi_dt(&self) -> ffi::datetime {
        ffi::datetime {
            epoch: self.inner.unix_timestamp() as f64,
            nsec: self.inner.nanosecond() as i32,
            tzoffset: self.inner.offset().whole_minutes(),
            tzindex: 0,
        }
    }
}

impl From<Inner> for Datetime {
    #[inline(always)]
    fn from(inner: Inner) -> Self {
        Self { inner }
    }
}

impl From<Datetime> for Inner {
    #[inline(always)]
    fn from(dt: Datetime) -> Self {
        dt.inner
    }
}

impl Display for Datetime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Tuple
////////////////////////////////////////////////////////////////////////////////

impl serde::Serialize for Datetime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct _ExtStruct<'a>((i8, &'a serde_bytes::Bytes));

        let data = self.as_bytes_tt();
        let mut data = data.as_slice();
        if data[8..] == [0, 0, 0, 0, 0, 0, 0, 0] {
            data = &data[..8];
        }
        _ExtStruct((ffi::MP_DATETIME, serde_bytes::Bytes::new(data))).serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Datetime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct _ExtStruct((i8, serde_bytes::ByteBuf));

        let _ExtStruct((kind, bytes)) = serde::Deserialize::deserialize(deserializer)?;

        if kind != ffi::MP_DATETIME {
            return Err(serde::de::Error::custom(format!(
                "Expected Datetime, found msgpack ext #{}",
                kind
            )));
        }

        let data = bytes.as_slice();
        if data.len() != 8 && data.len() != 16 {
            return Err(serde::de::Error::custom(format!(
                "Unexpected number of bytes for Datetime: expected 8 or 16, got {}",
                data.len()
            )));
        }

        Self::from_bytes_tt(data)
            .map_err(|_| serde::de::Error::custom("Error decoding msgpack bytes"))
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Lua
////////////////////////////////////////////////////////////////////////////////

static CTID_DATETIME: Lazy<u32> = Lazy::new(|| {
    if !crate::ffi::has_datetime() {
        panic!("datetime is not supported in current tarantool version")
    }
    use tlua::AsLua;
    let lua = crate::global_lua();
    let ctid_datetime =
        unsafe { tlua::ffi::luaL_ctypeid(lua.as_lua(), crate::c_ptr!("struct datetime")) };
    ctid_datetime
});

unsafe impl tlua::AsCData for ffi::datetime {
    fn ctypeid() -> tlua::ffi::CTypeID {
        *CTID_DATETIME
    }
}

impl<L> tlua::LuaRead<L> for Datetime
where
    L: tlua::AsLua,
{
    fn lua_read_at_position(lua: L, index: std::num::NonZeroI32) -> tlua::ReadResult<Self, L> {
        let res = tlua::LuaRead::lua_read_at_position(&lua, index);
        let tlua::CData(datetime) = crate::unwrap_ok_or!(res,
            Err((_, e)) => {
                return Err((lua, e));
            }
        );
        match Self::from_ffi_dt(datetime) {
            Ok(v) => Ok(v),
            Err(err) => {
                let e = tlua::WrongType::info("reading tarantool datetime")
                    .expected_type::<Self>()
                    .actual(format!("datetime failing to convert: {}", err));
                Err((lua, e))
            }
        }
    }
}

impl<L: tlua::AsLua> tlua::Push<L> for Datetime {
    type Err = tlua::Void;

    fn push_to_lua(&self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        Ok(lua.push_one(tlua::CData(self.as_ffi_dt())))
    }
}

impl<L: tlua::AsLua> tlua::PushOne<L> for Datetime {}

impl<L: tlua::AsLua> tlua::PushInto<L> for Datetime {
    type Err = tlua::Void;

    fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        Ok(lua.push_one(tlua::CData(self.as_ffi_dt())))
    }
}

impl<L: tlua::AsLua> tlua::PushOneInto<L> for Datetime {}

#[cfg(test)]
mod tests {
    use super::*;
    use time_macros::datetime;

    #[test]
    fn serialize() {
        let datetime: Datetime = datetime!(2023-11-11 2:03:19.35421 -3).into();
        let data = rmp_serde::to_vec(&datetime).unwrap();
        let expected = b"\xd8\x04\x17\x0b\x4f\x65\x00\x00\x00\x00\xd0\xd0\x1c\x15\x4c\xff\x00\x00";
        assert_eq!(data, expected);

        let only_date: Datetime = datetime!(1993-05-19 0:00:0.0000 +0).into();
        let data = rmp_serde::to_vec(&only_date).unwrap();
        let expected = b"\xd7\x04\x80\x78\xf9\x2b\x00\x00\x00\x00";
        assert_eq!(data, expected);
    }

    #[test]
    fn deserialize() {
        let data = b"\xd8\x04\x46\x9f\r\x66\x00\x00\x00\x00\x50\x41\x7e\x3b\x4c\xff\x00\x00";
        let datetime: Datetime = rmp_serde::from_slice(data).unwrap();
        let expected: Datetime = datetime!(2024-04-03 15:26:14.99813 -3).into();
        assert_eq!(datetime, expected);

        let data = b"\xd7\x04\x00\xc4\x4e\x65\x00\x00\x00\x00";
        let only_date: Datetime = rmp_serde::from_slice(data).unwrap();
        let expected: Datetime = datetime!(2023-11-11 0:00:0.0000 -0).into();
        assert_eq!(only_date, expected);
    }
}

#[cfg(feature = "internal_test")]
mod test {
    use super::*;

    unsafe fn encode_via_ffi(datetime: &Datetime) -> Vec<u8> {
        let ffi_datetime = datetime.as_ffi_dt();
        let capacity = crate::ffi::datetime::tnt_mp_sizeof_datetime(&ffi_datetime);
        let mut buffer = Vec::with_capacity(capacity as _);
        let end = crate::ffi::datetime::tnt_mp_encode_datetime(buffer.as_mut_ptr(), &ffi_datetime);
        buffer.set_len(end.offset_from(buffer.as_ptr()) as _);
        buffer
    }

    #[crate::test(tarantool = "crate")]
    fn datetime_encoding_matches() {
        if !crate::ffi::has_datetime() {
            return;
        }

        let datetime: Datetime = Inner::UNIX_EPOCH
            .replace_date(time::Date::from_ordinal_date(2000, 1).unwrap())
            .replace_time(time::Time::from_hms_micro(1, 2, 3, 4).unwrap())
            .replace_offset(time::UtcOffset::from_whole_seconds(42069).unwrap())
            .into();

        let tnt_data = unsafe { encode_via_ffi(&datetime) };
        assert_eq!(tnt_data.len(), 18);

        let our_data = rmp_serde::to_vec(&datetime).unwrap();
        assert_eq!(tnt_data, our_data);

        let datetime: Datetime = Inner::UNIX_EPOCH
            .replace_date(time::Date::from_ordinal_date(1968, 158).unwrap())
            .into();

        let tnt_data = unsafe { encode_via_ffi(&datetime) };
        assert_eq!(tnt_data.len(), 10);

        let our_data = rmp_serde::to_vec(&datetime).unwrap();
        assert_eq!(tnt_data, our_data);
    }
}
