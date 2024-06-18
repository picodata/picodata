use crate::system;
use abi_stable::std_types::{RString, RVec};
use abi_stable::StableAbi;
use tarantool::ffi::datetime::datetime;

/// *For internal usage, don't use it in your code*.
#[derive(StableAbi)]
#[repr(C)]
pub struct RawDecimal {
    pub digits: i32,
    pub exponent: i32,
    pub bits: u8,
    pub lsu: [u16; tarantool::ffi::decimal::DECNUMUNITS as _],
}

impl From<RawDecimal> for tarantool::ffi::decimal::decNumber {
    fn from(value: RawDecimal) -> Self {
        tarantool::ffi::decimal::decNumber {
            digits: value.digits,
            exponent: value.exponent,
            bits: value.bits,
            lsu: value.lsu,
        }
    }
}

impl From<tarantool::ffi::decimal::decNumber> for RawDecimal {
    fn from(value: tarantool::ffi::decimal::decNumber) -> Self {
        RawDecimal {
            digits: value.digits,
            exponent: value.exponent,
            bits: value.bits,
            lsu: value.lsu,
        }
    }
}

/// *For internal usage, don't use it in your code*.
#[derive(StableAbi)]
#[repr(C)]
pub struct RawDatetime {
    pub epoch: f64,
    pub nsec: i32,
    pub tzoffset: i16,
    pub tzindex: i16,
}

impl From<RawDatetime> for tarantool::ffi::datetime::datetime {
    fn from(dt: RawDatetime) -> Self {
        tarantool::ffi::datetime::datetime {
            epoch: dt.epoch,
            nsec: dt.nsec,
            tzoffset: dt.tzoffset,
            tzindex: dt.tzindex,
        }
    }
}

impl From<tarantool::ffi::datetime::datetime> for RawDatetime {
    fn from(dt: datetime) -> Self {
        Self {
            epoch: dt.epoch,
            nsec: dt.nsec,
            tzoffset: dt.tzoffset,
            tzindex: dt.tzindex,
        }
    }
}

/// *For internal usage, don't use it in your code*.
#[derive(StableAbi)]
#[repr(C)]
pub enum SqlValueInner {
    /// Boolean type.
    Boolean(bool),
    /// Fixed point type.
    Decimal(RawDecimal),
    /// Floating point type.
    Double(f64),
    /// Datetime type,
    Datetime(RawDatetime),
    /// Signed integer type.
    Integer(i64),
    /// SQL NULL ("unknown" in the terms of three-valued logic).
    Null,
    /// String type.
    String(RString),
    /// Unsigned integer type.
    Unsigned(u64),
    /// Tuple type
    Array(RVec<SqlValue>),
    /// Uuid type
    Uuid([u8; 16]),
}

#[derive(StableAbi)]
#[repr(C)]
pub struct SqlValue(SqlValueInner);

impl SqlValue {
    /// *For internal usage, don't use it in your code*.
    pub fn into_inner(self) -> SqlValueInner {
        self.0
    }

    /// Create SQL value of a bool type.
    pub fn boolean(b: bool) -> Self {
        SqlValue(SqlValueInner::Boolean(b))
    }

    /// Create SQL value of a [tarantool::decimal::Decimal] type.
    pub fn decimal(dec: tarantool::decimal::Decimal) -> Self {
        let dec = dec.into_raw();
        SqlValue(SqlValueInner::Decimal(RawDecimal::from(dec)))
    }

    /// Create SQL value of a double (64bit float) type.
    pub fn double(d: f64) -> Self {
        SqlValue(SqlValueInner::Double(d))
    }

    /// Create SQL value of a [`tarantool::datetime::Datetime`] type.
    pub fn datetime(dt: tarantool::datetime::Datetime) -> Self {
        SqlValue(SqlValueInner::Datetime(RawDatetime::from(dt.as_ffi_dt())))
    }

    /// Create SQL value of an integer type.
    pub fn integer(i: i64) -> Self {
        SqlValue(SqlValueInner::Integer(i))
    }

    /// Create SQL value of a NULL type.
    pub fn null() -> Self {
        SqlValue(SqlValueInner::Null)
    }

    /// Create SQL value of a string type.
    pub fn string(s: impl Into<String>) -> Self {
        SqlValue(SqlValueInner::String(RString::from(s.into())))
    }

    /// Create SQL value of an unsigned integer type.
    pub fn unsigned(u: u64) -> Self {
        SqlValue(SqlValueInner::Unsigned(u))
    }

    /// Create SQL value of an array type (array of SQL values).
    pub fn array(arr: impl Into<Vec<Self>>) -> Self {
        SqlValue(SqlValueInner::Array(RVec::from(arr.into())))
    }

    /// Create SQL value of a [`system::tarantool::uuid::Uuid`] type.
    pub fn uuid(uuid: system::tarantool::uuid::Uuid) -> Self {
        SqlValue(SqlValueInner::Uuid(*uuid.as_bytes()))
    }
}
