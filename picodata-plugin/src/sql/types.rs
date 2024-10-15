use crate::system;
use abi_stable::std_types::{RString, RVec};
use abi_stable::StableAbi;
use tarantool::datetime::Datetime;
use tarantool::decimal::Decimal;
use tarantool::ffi::datetime::datetime;
use tarantool::uuid::Uuid;

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

impl From<bool> for SqlValue {
    fn from(b: bool) -> Self {
        SqlValue::boolean(b)
    }
}

impl From<Decimal> for SqlValue {
    fn from(dec: Decimal) -> Self {
        SqlValue::decimal(dec)
    }
}

macro_rules! impl_float {
    ($t:ty) => {
        impl From<$t> for SqlValue {
            fn from(f: $t) -> Self {
                SqlValue::double(f as f64)
            }
        }
    };
}

impl_float!(f32);
impl_float!(f64);

impl From<Datetime> for SqlValue {
    fn from(dt: Datetime) -> Self {
        SqlValue::datetime(dt)
    }
}

macro_rules! impl_int {
    ($t:ty) => {
        impl From<$t> for SqlValue {
            fn from(i: $t) -> Self {
                SqlValue::integer(i as i64)
            }
        }
    };
}

impl_int!(i8);
impl_int!(i16);
impl_int!(i32);
impl_int!(i64);
impl_int!(isize);
impl_int!(i128);

impl<T: Into<SqlValue>> From<Option<T>> for SqlValue {
    fn from(opt: Option<T>) -> Self {
        match opt {
            None => SqlValue::null(),
            Some(t) => t.into(),
        }
    }
}

impl From<String> for SqlValue {
    fn from(s: String) -> Self {
        SqlValue::string(s)
    }
}

impl From<&str> for SqlValue {
    fn from(s: &str) -> Self {
        SqlValue::string(s)
    }
}

macro_rules! impl_uint {
    ($t:ty) => {
        impl From<$t> for SqlValue {
            fn from(u: $t) -> Self {
                SqlValue::unsigned(u as u64)
            }
        }
    };
}

impl_uint!(u8);
impl_uint!(u16);
impl_uint!(u32);
impl_uint!(u64);
impl_uint!(usize);
impl_uint!(u128);

impl<T: Into<SqlValue>> From<Vec<T>> for SqlValue {
    fn from(vec: Vec<T>) -> Self {
        let array: Vec<_> = vec.into_iter().map(|el| el.into()).collect();
        SqlValue::array(array)
    }
}

impl<T: Into<SqlValue> + Clone> From<&[T]> for SqlValue {
    fn from(slice: &[T]) -> Self {
        let array: Vec<_> = slice.iter().map(|el| el.clone().into()).collect();
        SqlValue::array(array)
    }
}

impl<T: Into<SqlValue>, const N: usize> From<[T; N]> for SqlValue {
    fn from(slice: [T; N]) -> Self {
        let array: Vec<_> = slice.into_iter().map(|el| el.into()).collect();
        SqlValue::array(array)
    }
}

impl From<Uuid> for SqlValue {
    fn from(uuid: Uuid) -> Self {
        SqlValue::uuid(uuid)
    }
}
