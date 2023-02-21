use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fmt::Display;
use std::os::raw::c_char;
use time::{Duration, UtcOffset};

type Inner = time::OffsetDateTime;

const MP_DATETIME: std::os::raw::c_char = 4;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("incorrect timestamp value")]
    WrongUnixTimestamp(time::error::ComponentRange),
    #[error("incorrect offset value")]
    WrongUtcOffset(time::error::ComponentRange),
}

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
    fn from_bytes_tt(bytes: &[u8; 16]) -> Result<Self, Error> {
        let mut sec_bytes: [u8; 8] = [0; 8];
        sec_bytes.copy_from_slice(&bytes[0..8]);
        let mut nsec_bytes: [u8; 4] = [0; 4];
        nsec_bytes.copy_from_slice(&bytes[8..12]);
        let mut tzoffest_bytes: [u8; 2] = [0; 2];
        tzoffest_bytes.copy_from_slice(&bytes[12..14]);

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
        struct _ExtStruct((c_char, serde_bytes::ByteBuf));

        let data = self.as_bytes_tt();
        _ExtStruct((MP_DATETIME, serde_bytes::ByteBuf::from(&data as &[_]))).serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Datetime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct _ExtStruct((c_char, serde_bytes::ByteBuf));

        let _ExtStruct((kind, bytes)) = serde::Deserialize::deserialize(deserializer)?;

        if kind != MP_DATETIME {
            return Err(serde::de::Error::custom(format!(
                "Expected Datetime, found msgpack ext #{}",
                kind
            )));
        }

        let data = bytes.as_slice();
        let data = data.try_into().map_err(|_| {
            serde::de::Error::custom(format!(
                "Not enough bytes for Datetime: expected 16, got {}",
                data.len()
            ))
        })?;

        Self::from_bytes_tt(&data)
            .map_err(|_| serde::de::Error::custom("Error decoding msgpack bytes"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time_macros::datetime;

    #[test]
    pub fn serialize() {
        let exp = [
            216, 4, 23, 11, 79, 101, 0, 0, 0, 0, 208, 208, 28, 21, 76, 255, 0, 0,
        ];
        let dt: Datetime = datetime!(2023-11-11 2:03:19.35421 -3).into();
        let result = rmp_serde::to_vec(&dt).unwrap();

        assert_eq!(result, &exp);
    }

    #[test]
    pub fn deserialize() {
        let exp: Datetime = datetime!(2023-11-11 2:03:19.35421 -3).into();
        let data: &[u8] = &[
            216, 4, 23, 11, 79, 101, 0, 0, 0, 0, 208, 208, 28, 21, 76, 255, 0, 0,
        ];
        let result = rmp_serde::from_slice(data).unwrap();

        assert_eq!(exp, result);
    }
}
