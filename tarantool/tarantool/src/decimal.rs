use crate::ffi::decimal as ffi;

use serde::{Serialize, Deserialize};

#[derive(Debug, Copy, Clone)]
pub struct Decimal {
    pub(crate) inner: ffi::decNumber,
}

impl Decimal {
    /// Initialize a `Decimal` instance from a raw [`decNumber`] struct
    ///
    /// [`decNumber`]: crate::ffi::decimal::decNumber
    ///
    /// # Safety
    /// `inner` must a be valid instance of `decNumber` struct
    #[inline(always)]
    pub unsafe fn from_raw(inner: ffi::decNumber) -> Self {
        Self { inner }
    }

    /// Return a zero decimal number.
    #[inline(always)]
    pub fn zero() -> Self {
        unsafe {
            let mut dec = std::mem::MaybeUninit::uninit();
            let res = ffi::decimal_zero(dec.as_mut_ptr());
            debug_assert!(!res.is_null());
            Self::from_raw(dec.assume_init())
        }
    }

    /// Return decimal precision, i.e. the amount of decimal digits in its
    /// representation.
    #[inline(always)]
    pub fn precision(&self) -> i32 {
        unsafe { ffi::decimal_precision(&self.inner) }
    }

    /// Return decimal scale, i.e. the number of decimal digits after the
    /// decimal separator.
    #[inline(always)]
    pub fn scale(&self) -> i32 {
        unsafe { ffi::decimal_scale(&self.inner) }
    }

    /// Check if the fractional part of the number is `0`
    #[inline(always)]
    pub fn is_int(&self) -> bool {
        unsafe { ffi::decimal_is_int(&self.inner) }
    }

    /// Remove trailing zeros from the fractional part of a number.
    #[inline(always)]
    pub fn trim(mut self) -> Self {
        let res = unsafe { ffi::decimal_trim(&mut self.inner) };
        debug_assert!(!res.is_null());
        self
    }

    /// Round a given decimal to have zero digits after the decimal point.
    #[inline(always)]
    pub fn round(self) -> Self {
        self.round_to(0).unwrap()
    }

    /// Floor a given decimal towards zero to have zero digits after the decimal
    /// point.
    #[inline(always)]
    pub fn floor(self) -> Self {
        self.floor_to(0).unwrap()
    }

    /// Round a given decimal to have not more than `scale` digits after the
    /// decimal point.  If `scale` if greater than current `self.scale()`,
    /// return `self` unchanged. Scale must be in range `[0..=
    /// ffi::DECIMAL_MAX_DIGITS]`. Return `None` if `scale` if out of bounds.
    #[inline(always)]
    pub fn round_to(mut self, scale: u8) -> Option<Self> {
        unsafe {
            if ffi::decimal_round(&mut self.inner, scale as _).is_null() {
                None
            } else {
                Some(self)
            }
        }
    }

    /// Like [`Decimal::round`] but rounds the number towards zero.
    #[inline(always)]
    pub fn floor_to(mut self, scale: u8) -> Option<Self> {
        unsafe {
            if ffi::decimal_floor(&mut self.inner, scale as _).is_null() {
                None
            } else {
                Some(self)
            }
        }
    }

    /// Set scale of `self` to `scale`. If `scale` < `self.scale()`, performs
    /// the equivalent of `self.`[`round`]`(scale)`.  Otherwise appends a
    /// sufficient amount of trailing fractional zeros. Return `None` if `scale`
    /// < `0` or too big.
    ///
    /// [`round`]: Decimal::round
    #[inline(always)]
    pub fn rescale(mut self, scale: u8) -> Option<Self> {
        unsafe {
            if ffi::decimal_rescale(&mut self.inner, scale as _).is_null() {
                None
            } else {
                Some(self)
            }
        }
    }

    /// Return the absolute value of the number.
    #[inline(always)]
    pub fn abs(mut self) -> Self {
        let res = unsafe { ffi::decimal_abs(&mut self.inner, &self.inner) };
        debug_assert!(!res.is_null());
        self
    }

    /// Compute logarithm base 10.
    #[inline(always)]
    pub fn log10(mut self) -> Self {
        let res = unsafe { ffi::decimal_log10(&mut self.inner, &self.inner) };
        debug_assert!(!res.is_null());
        self
    }

    /// Compute natural logarithm.
    #[inline(always)]
    pub fn ln(mut self) -> Self {
        let res = unsafe { ffi::decimal_ln(&mut self.inner, &self.inner) };
        debug_assert!(!res.is_null());
        self
    }

    /// Exponentiate `self`. Return `None` if the result is out of range.
    #[inline(always)]
    pub fn exp(mut self) -> Option<Self> {
        let res = unsafe { ffi::decimal_exp(&mut self.inner, &self.inner) };
        if res.is_null() {
            None
        } else {
            Some(self)
        }
    }

    /// Compute square root of `self`. Return `None` if the result is imaginary
    /// or out of range.
    #[inline(always)]
    pub fn sqrt(mut self) -> Option<Self> {
        let res = unsafe { ffi::decimal_sqrt(&mut self.inner, &self.inner) };
        if res.is_null() {
            None
        } else {
            Some(self)
        }
    }

    /// Compute `self` raised to the power of `pow`. Return `None` if the result
    /// is out of range.
    #[inline(always)]
    pub fn pow(mut self, pow: impl Into<Self>) -> Option<Self> {
        let res = unsafe {
            ffi::decimal_pow(&mut self.inner, &self.inner, &pow.into().inner)
        };
        if res.is_null() {
            None
        } else {
            Some(self)
        }
    }

    /// Convert `self` to i64. Return `None` if `self` is not an integer or the
    /// value is out of range.
    #[inline(always)]
    pub fn to_i64(self) -> Option<i64> {
        std::convert::TryInto::try_into(self).ok()
    }

    /// Convert `self` to u64. Return `None` if `self` is not an integer or the
    /// value is out of range.
    #[inline(always)]
    pub fn to_u64(self) -> Option<u64> {
        std::convert::TryInto::try_into(self).ok()
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Cmp
////////////////////////////////////////////////////////////////////////////////

impl std::cmp::PartialOrd for Decimal {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            unsafe {
                match ffi::decimal_compare(&self.inner, &other.inner) {
                    0 => std::cmp::Ordering::Equal,
                    _neg if _neg < 0 => std::cmp::Ordering::Less,
                    _pos => std::cmp::Ordering::Greater,
                }
            }
        )
    }
}

impl std::cmp::Ord for Decimal {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl std::cmp::PartialEq for Decimal {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl std::cmp::Eq for Decimal {}

macro_rules! impl_cmp_int {
    ($($t:ty)+) => {
        $(
            impl std::cmp::PartialEq<$t> for Decimal {
                #[inline(always)]
                fn eq(&self, &other: &$t) -> bool {
                    self.is_int() && self.cmp(&other.into()) == std::cmp::Ordering::Equal
                }
            }
        )+
    }
}

impl_cmp_int!{i8 i16 i32 i64 isize u8 u16 u32 u64 usize}

////////////////////////////////////////////////////////////////////////////////
/// Ops
////////////////////////////////////////////////////////////////////////////////

macro_rules! impl_bin_op {
    ($m:ident, $trait:ident, $op:ident, $ass_trait:ident, $ass_op:ident, $ffi:path) => {
        impl Decimal {
            #[inline(always)]
            pub fn $m(mut self, rhs: impl Into<Self>) -> Option<Self> {
                let res = unsafe {
                    $ffi(&mut self.inner, &self.inner, &rhs.into().inner)
                };
                if res.is_null() {
                    None
                } else {
                    Some(self)
                }
            }
        }

        impl<T: Into<Decimal>> std::ops::$trait<T> for Decimal {
            type Output = Self;

            #[inline(always)]
            fn $op(self, rhs: T) -> Self {
                self.$m(rhs).expect("overflow")
            }
        }

        impl<T: Into<Decimal>> std::ops::$ass_trait<T> for Decimal {
            #[inline(always)]
            fn $ass_op(&mut self, rhs: T) {
                *self = self.$m(rhs).expect("overlow")
            }
        }
    }
}

impl_bin_op!{checked_add, Add, add, AddAssign, add_assign, ffi::decimal_add}
impl_bin_op!{checked_sub, Sub, sub, SubAssign, sub_assign, ffi::decimal_sub}
impl_bin_op!{checked_mul, Mul, mul, MulAssign, mul_assign, ffi::decimal_mul}
impl_bin_op!{checked_div, Div, div, DivAssign, div_assign, ffi::decimal_div}
impl_bin_op!{checked_rem, Rem, rem, RemAssign, rem_assign, ffi::decimal_remainder}

impl std::ops::Neg for Decimal {
    type Output = Self;

    #[inline(always)]
    fn neg(mut self) -> Self {
        let res = unsafe { ffi::decimal_minus(&mut self.inner, &self.inner) };
        debug_assert!(!res.is_null());
        self
    }
}

////////////////////////////////////////////////////////////////////////////////
/// String conversions
////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for Decimal {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe {
            let mut buf = Vec::with_capacity((ffi::DECIMAL_MAX_DIGITS + 14 + 1) as _);
            let c_ptr = ffi::decNumberToString(&self.inner, buf.as_mut_ptr());
            let c_str = std::ffi::CStr::from_ptr(c_ptr);
            let r_str = std::str::from_utf8_unchecked(c_str.to_bytes());
            f.write_str(r_str)
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DecimalFromStrError;

impl std::str::FromStr for Decimal {
    type Err = DecimalFromStrError;

    #[inline(always)]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // The underlying `decNumberFromString` api only supports null
        // terminated strings so there is no way to avoid a copy here
        // Therefore you should use `std::ffi::CStr` whenever possible
        let data = s.bytes().chain(std::iter::once(0)).collect::<Vec<_>>();
        let c_str = unsafe {
            std::ffi::CStr::from_bytes_with_nul_unchecked(&data)
        };
        std::convert::TryFrom::try_from(c_str)
    }
}

impl std::convert::TryFrom<&str> for Decimal {
    type Error = <Decimal as std::str::FromStr>::Err;

    #[inline(always)]
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl std::convert::TryFrom<&std::ffi::CStr> for Decimal {
    type Error = DecimalFromStrError;

    #[inline(always)]
    fn try_from(s: &std::ffi::CStr) -> Result<Self, Self::Error> {
        unsafe {
            let mut dec = std::mem::MaybeUninit::uninit();
            let res = ffi::decimal_from_string(dec.as_mut_ptr(), s.as_ptr());

            if res.is_null() {
                Err(DecimalFromStrError)
            } else {
                Ok(Self::from_raw(dec.assume_init()))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Lua
////////////////////////////////////////////////////////////////////////////////

impl<L> tlua::LuaRead<L> for Decimal
where
    L: tlua::AsLua,
{
    fn lua_read_at_position(lua: L, index: std::num::NonZeroI32) -> Result<Self, L> {
        let raw_lua = lua.as_lua();
        let index = index.get();
        unsafe {
            if tlua::ffi::lua_type(raw_lua, index) != tlua::ffi::LUA_TCDATA {
                return Err(lua)
            }
            let mut ctypeid = std::mem::MaybeUninit::uninit();
            let cdata = tlua::ffi::luaL_checkcdata(raw_lua, index, ctypeid.as_mut_ptr());
            if ctypeid.assume_init() != ffi::CTID_DECIMAL {
                return Err(lua)
            }

            Ok(Self::from_raw(*cdata.cast::<ffi::decNumber>()))
        }
    }
}

#[inline(always)]
fn push_decimal<L: tlua::AsLua>(lua: L, d: ffi::decNumber) -> tlua::PushGuard<L> {
    unsafe {
        let dec = tlua::ffi::luaL_pushcdata(lua.as_lua(), ffi::CTID_DECIMAL);
        std::ptr::write(dec.cast::<ffi::decNumber>(), d);
        tlua::PushGuard::new(lua, 1)
    }
}

impl<L: tlua::AsLua> tlua::Push<L> for Decimal {
    type Err = tlua::Void;

    fn push_to_lua(&self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        Ok(push_decimal(lua, self.inner))
    }
}

impl<L: tlua::AsLua> tlua::PushOne<L> for Decimal {}

impl<L: tlua::AsLua> tlua::PushInto<L> for Decimal {
    type Err = tlua::Void;

    fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        Ok(push_decimal(lua, self.inner))
    }
}

impl<L: tlua::AsLua> tlua::PushOneInto<L> for Decimal {}

////////////////////////////////////////////////////////////////////////////////
/// Number conversions
////////////////////////////////////////////////////////////////////////////////

macro_rules! impl_from_int {
    ($($t:ty)+ => $f:path) => {
        $(
            impl From<$t> for Decimal {
                #[inline(always)]
                fn from(num: $t) -> Self {
                    unsafe {
                        let mut dec = std::mem::MaybeUninit::uninit();
                        $f(dec.as_mut_ptr(), num as _);
                        Self::from_raw(dec.assume_init())
                    }
                }
            }
        )+
    }
}

impl_from_int!{i8 i16 i32 i64 isize => ffi::decimal_from_int64}
impl_from_int!{u8 u16 u32 u64 usize => ffi::decimal_from_uint64}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum DecimalFromfloatError<T> {
    OutOfRange(T),
    Infinite,
    Nan,
}

macro_rules! impl_error_from_float {
    ($($t:ty)+) => {
        $(
            impl From<$t> for DecimalFromfloatError<$t> {
                #[inline(always)]
                fn from(num: $t) -> Self {
                    match num.classify() {
                        std::num::FpCategory::Infinite => DecimalFromfloatError::Infinite,
                        std::num::FpCategory::Nan => DecimalFromfloatError::Nan,
                        std::num::FpCategory::Normal => DecimalFromfloatError::OutOfRange(num),
                        std::num::FpCategory::Zero => {
                            unreachable!("conversion cannot fail if number is zero")
                        }
                        std::num::FpCategory::Subnormal => {
                            unreachable!("subnormal floats are usually converted to zero")
                        }
                    }
                }
            }
        )+
    }
}

impl_error_from_float!{f32 f64}

impl<T: std::fmt::Display> std::fmt::Display for DecimalFromfloatError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfRange(num) => {
                write!(f,
                    "float `{}` cannot be represented using {} digits",
                    num,
                    ffi::DECIMAL_MAX_DIGITS,
                )
            },
            Self::Infinite => f.write_str("float is infinite"),
            Self::Nan => f.write_str("float is NaN"),
        }
    }
}

impl<T> std::error::Error for DecimalFromfloatError<T>
where
    T: std::fmt::Debug + std::fmt::Display,
{
    fn description(&self) -> &'static str {
        match self {
            Self::OutOfRange(_) => "float is out of range",
            Self::Infinite => "float is infinite",
            Self::Nan => "float is NaN",
        }
    }
}

macro_rules! impl_tryfrom_float {
    ($($t:ty)+) => {
        $(
            impl std::convert::TryFrom<$t> for Decimal {
                type Error = DecimalFromfloatError<$t>;

                #[inline(always)]
                fn try_from(num: $t) -> Result<Self, Self::Error> {
                    unsafe {
                        let mut dec = std::mem::MaybeUninit::uninit();
                        let res = ffi::decimal_from_double(dec.as_mut_ptr(), num as _);
                        if res.is_null() {
                            Err(DecimalFromfloatError::from(num))
                        } else {
                            Ok(Self::from_raw(dec.assume_init()))
                        }
                    }
                }
            }
        )+
    }
}

impl_tryfrom_float!{f32 f64}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum DecimalToIntError {
    OutOfRange,
    NonInteger,
}

#[allow(deprecated)]
impl std::fmt::Display for DecimalToIntError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(std::error::Error::description(self))
    }
}

impl std::error::Error for DecimalToIntError {
    fn description(&self) -> &'static str {
        match self {
            Self::OutOfRange => "decimal is out of range",
            Self::NonInteger => "decimal is not an integer",
        }
    }
}

macro_rules! impl_try_into_int {
    ($($t:ty)+ => $f:path) => {
        $(
            impl std::convert::TryFrom<Decimal> for $t {
                type Error = DecimalToIntError;

                fn try_from(dec: Decimal) -> Result<Self, Self::Error> {
                    if !dec.is_int() {
                        return Err(DecimalToIntError::NonInteger)
                    }
                    unsafe {
                        let mut num = std::mem::MaybeUninit::uninit();
                        let res = $f(&dec.inner, num.as_mut_ptr());
                        if res.is_null() {
                            Err(DecimalToIntError::OutOfRange)
                        } else {
                            Ok(num.assume_init() as _)
                        }
                    }
                }
            }
        )+
    }
}

impl_try_into_int!{i64 isize => ffi::decimal_to_int64}
impl_try_into_int!{u64 usize => ffi::decimal_to_uint64}

////////////////////////////////////////////////////////////////////////////////
/// Tuple
////////////////////////////////////////////////////////////////////////////////

impl serde::Serialize for Decimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct _ExtStruct((std::os::raw::c_char, serde_bytes::ByteBuf));

        let data = unsafe {
            let len = ffi::decimal_len(&self.inner) as usize;
            let mut data = Vec::<u8>::with_capacity(len);
            ffi::decimal_pack(data.as_mut_ptr() as _, &self.inner);
            data.set_len(len);
            data
        };
        _ExtStruct((ffi::MP_DECIMAL, serde_bytes::ByteBuf::from(data)))
            .serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Decimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct _ExtStruct((std::os::raw::c_char, serde_bytes::ByteBuf));

        match serde::Deserialize::deserialize(deserializer)? {
            _ExtStruct((ffi::MP_DECIMAL, bytes)) => {
                let data = bytes.into_vec();
                let data_p = &mut data.as_ptr().cast();
                let mut dec = std::mem::MaybeUninit::uninit();
                let res = unsafe {
                    ffi::decimal_unpack(data_p, data.len() as _, dec.as_mut_ptr())
                };
                if res.is_null() {
                    Err(serde::de::Error::custom("Decimal out of range or corrupt"))
                } else {
                    unsafe { Ok(Self::from_raw(dec.assume_init())) }
                }
            }
            _ExtStruct((kind, _)) => {
                Err(serde::de::Error::custom(
                    format!("Expected Decimal, found msgpack ext #{}", kind)
                ))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// decimal!
////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! decimal {
    ($($num:tt)+) => {
        {
            let r_str = ::std::concat![$(::std::stringify!($num)),+, "\0"];
            let c_str = unsafe {
                ::std::ffi::CStr::from_bytes_with_nul_unchecked(r_str.as_bytes())
            };
            let dec: crate::decimal::Decimal = ::std::convert::TryFrom::try_from(c_str)
                .expect(
                    ::std::concat![
                        "failed to convert '",
                        $(::std::stringify!($num)),+,
                        "' to decimal",
                    ]
                );
            dec
        }
    }
}

