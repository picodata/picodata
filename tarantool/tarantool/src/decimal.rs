#![cfg(any(feature = "picodata", feature = "standalone_decimal"))]

use once_cell::sync::Lazy;

use crate::ffi::decimal as ffi;

#[cfg(all(not(feature = "standalone_decimal"), feature = "picodata"))]
mod tarantool_decimal {
    use super::ffi;
    use serde::{Deserialize, Serialize};

    /// A Decimal number implemented using the builtin tarantool api.
    ///
    /// ## Availability
    /// This api is not available in all versions of tarantool.
    /// Use [`tarantool::ffi::has_decimal`] to check if it is supported in your
    /// case.
    /// If `has_decimal` returns `false`, using any function from this module
    /// will result in a **panic**.
    ///
    /// [`tarantool::ffi::has_decimal`]: crate::ffi::has_decimal
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

        /// Return a raw [`ffi::decNumber`] struct
        pub fn into_raw(self) -> ffi::decNumber {
            self.inner
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
        /// Round a given decimal to have no more than `scale` digits after the
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
        pub fn log10(mut self) -> Option<Self> {
            let res = unsafe { ffi::decimal_log10(&mut self.inner, &self.inner) };
            if res.is_null() {
                None
            } else {
                Some(self)
            }
        }

        /// Compute natural logarithm.
        #[inline(always)]
        pub fn ln(mut self) -> Option<Self> {
            let res = unsafe { ffi::decimal_ln(&mut self.inner, &self.inner) };
            if res.is_null() {
                None
            } else {
                Some(self)
            }
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
            let res = unsafe { ffi::decimal_pow(&mut self.inner, &self.inner, &pow.into().inner) };
            if res.is_null() {
                None
            } else {
                Some(self)
            }
        }
    }

    #[allow(clippy::non_canonical_partial_ord_impl)]
    impl std::cmp::PartialOrd for Decimal {
        #[inline(always)]
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(unsafe {
                match ffi::decimal_compare(&self.inner, &other.inner) {
                    0 => std::cmp::Ordering::Equal,
                    _neg if _neg < 0 => std::cmp::Ordering::Less,
                    _pos => std::cmp::Ordering::Greater,
                }
            })
        }
    }

    impl std::hash::Hash for Decimal {
        #[inline(always)]
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.trim().inner.hash(state);
        }
    }

    macro_rules! impl_bin_op {
        ($m:ident, $trait:ident, $op:ident, $ass_trait:ident, $ass_op:ident, $ffi:path) => {
            impl Decimal {
                #[inline(always)]
                pub fn $m(mut self, rhs: impl Into<Self>) -> Option<Self> {
                    let res = unsafe { $ffi(&mut self.inner, &self.inner, &rhs.into().inner) };
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
                    *self = self.$m(rhs).expect("overflow")
                }
            }
        };
    }

    impl_bin_op! {checked_add, Add, add, AddAssign, add_assign, ffi::decimal_add}
    impl_bin_op! {checked_sub, Sub, sub, SubAssign, sub_assign, ffi::decimal_sub}
    impl_bin_op! {checked_mul, Mul, mul, MulAssign, mul_assign, ffi::decimal_mul}
    impl_bin_op! {checked_div, Div, div, DivAssign, div_assign, ffi::decimal_div}
    impl_bin_op! {checked_rem, Rem, rem, RemAssign, rem_assign, ffi::decimal_remainder}

    impl std::ops::Neg for Decimal {
        type Output = Self;

        #[inline(always)]
        fn neg(mut self) -> Self {
            let res = unsafe { ffi::decimal_minus(&mut self.inner, &self.inner) };
            debug_assert!(!res.is_null());
            self
        }
    }

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
            let c_str = unsafe { std::ffi::CStr::from_bytes_with_nul_unchecked(&data) };
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

    #[inline(always)]
    fn push_decimal<L: tlua::AsLua>(lua: L, d: ffi::decNumber) -> tlua::PushGuard<L> {
        unsafe {
            let dec = tlua::ffi::luaL_pushcdata(lua.as_lua(), *super::CTID_DECIMAL);
            std::ptr::write(dec.cast::<ffi::decNumber>(), d);
            tlua::PushGuard::new(lua, 1)
        }
    }

    impl<L: tlua::AsLua> tlua::Push<L> for Decimal {
        type Err = tlua::Void;

        #[inline]
        fn push_to_lua(&self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
            Ok(push_decimal(lua, self.inner))
        }
    }

    impl<L: tlua::AsLua> tlua::PushOne<L> for Decimal {}

    impl<L: tlua::AsLua> tlua::PushInto<L> for Decimal {
        type Err = tlua::Void;

        #[inline]
        fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
            Ok(push_decimal(lua, self.inner))
        }
    }

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

    impl_from_int! {i8 i16 i32 i64 isize => ffi::decimal_from_int64}
    impl_from_int! {u8 u16 u32 u64 usize => ffi::decimal_from_uint64}

    macro_rules! impl_tryfrom_float {
        ($($t:ty)+) => {
            $(
                impl std::convert::TryFrom<$t> for Decimal {
                    type Error = super::DecimalFromfloatError<$t>;

                    #[inline(always)]
                    fn try_from(num: $t) -> Result<Self, Self::Error> {
                        unsafe {
                            let mut dec = std::mem::MaybeUninit::uninit();
                            let res = ffi::decimal_from_double(dec.as_mut_ptr(), num as _);
                            if res.is_null() {
                                Err(super::DecimalFromfloatError::from(num))
                            } else {
                                Ok(Self::from_raw(dec.assume_init()))
                            }
                        }
                    }
                }
            )+
        }
    }

    impl_tryfrom_float! {f32 f64}

    macro_rules! impl_try_into_int {
        ($($t:ty)+ => $f:path) => {
            $(
                impl std::convert::TryFrom<Decimal> for $t {
                    type Error = super::DecimalToIntError;

                    #[inline]
                    fn try_from(dec: Decimal) -> Result<Self, Self::Error> {
                        if !dec.is_int() {
                            return Err(super::DecimalToIntError::NonInteger)
                        }
                        unsafe {
                            let mut num = std::mem::MaybeUninit::uninit();
                            let res = $f(&dec.inner, num.as_mut_ptr());
                            if res.is_null() {
                                Err(super::DecimalToIntError::OutOfRange)
                            } else {
                                Ok(num.assume_init() as _)
                            }
                        }
                    }
                }
            )+
        }
    }

    impl_try_into_int! {i64 isize => ffi::decimal_to_int64}
    impl_try_into_int! {u64 usize => ffi::decimal_to_uint64}

    impl serde::Serialize for Decimal {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            #[derive(Serialize)]
            struct _ExtStruct((i8, serde_bytes::ByteBuf));

            let data = unsafe {
                let len = ffi::decimal_len(&self.inner) as usize;
                let mut data = Vec::<u8>::with_capacity(len);
                ffi::decimal_pack(data.as_mut_ptr() as _, &self.inner);
                data.set_len(len);
                data
            };
            _ExtStruct((ffi::MP_DECIMAL, serde_bytes::ByteBuf::from(data))).serialize(serializer)
        }
    }

    impl<'de> serde::Deserialize<'de> for Decimal {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            #[derive(Deserialize)]
            struct _ExtStruct((i8, serde_bytes::ByteBuf));

            match serde::Deserialize::deserialize(deserializer)? {
                _ExtStruct((ffi::MP_DECIMAL, bytes)) => {
                    let data = bytes.into_vec();
                    let data_p = &mut data.as_ptr().cast();
                    let mut dec = std::mem::MaybeUninit::uninit();
                    let res =
                        unsafe { ffi::decimal_unpack(data_p, data.len() as _, dec.as_mut_ptr()) };
                    if res.is_null() {
                        Err(serde::de::Error::custom("Decimal out of range or corrupt"))
                    } else {
                        unsafe { Ok(Self::from_raw(dec.assume_init())) }
                    }
                }
                _ExtStruct((kind, _)) => Err(serde::de::Error::custom(format!(
                    "Expected Decimal, found msgpack ext #{}",
                    kind
                ))),
            }
        }
    }

    #[macro_export]
    macro_rules! decimal {
        ($($num:tt)+) => {
            {
                let r_str = ::std::concat![$(::std::stringify!($num)),+, "\0"];
                let c_str = unsafe {
                    ::std::ffi::CStr::from_bytes_with_nul_unchecked(r_str.as_bytes())
                };
                let dec: $crate::decimal::Decimal = ::std::convert::TryFrom::try_from(c_str)
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
}

#[cfg(feature = "standalone_decimal")]
mod standalone_decimal {
    use std::convert::TryInto;
    use std::{convert::TryFrom, mem::size_of};

    use once_cell::sync::Lazy;

    use super::ffi;

    /// A Decimal number implemented using the builtin tarantool api.
    ///
    /// ## Availability
    /// This api is not available in all versions of tarantool.
    /// Use [`tarantool::ffi::has_decimal`] to check if it is supported in your
    /// case.
    /// If `has_decimal` returns `false`, using any function from this module
    /// will result in a **panic**.
    ///
    /// This API is thread-safe unlike the original tarantool decimal API.
    ///
    /// [`tarantool::ffi::has_decimal`]: crate::ffi::has_decimal
    #[derive(Debug, Copy, Clone)]
    pub struct Decimal {
        pub(crate) inner: DecimalImpl,
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
            let inner = std::mem::transmute(inner);

            Self { inner }
        }

        /// Return a raw [`ffi::decNumber`] struct
        pub fn into_raw(self) -> ffi::decNumber {
            self.to_ffi()
        }

        #[inline(always)]
        #[allow(clippy::wrong_self_convention)]
        fn to_ffi(&self) -> ffi::decNumber {
            let (digits, exponent, bits, lsu) = self.inner.to_raw_parts();
            ffi::decNumber {
                digits: digits as _,
                exponent,
                bits,
                lsu,
            }
        }

        #[cfg(feature = "standalone_decimal")]
        unsafe fn from_inner_unchecked(inner: DecimalImpl) -> Self {
            Self { inner }
        }

        /// Return a zero decimal number.
        #[inline(always)]
        pub fn zero() -> Self {
            unsafe { Self::from_inner_unchecked(DecimalImpl::zero()) }
        }

        /// Return decimal precision, i.e. the amount of decimal digits in its
        /// representation.
        #[inline(always)]
        pub fn precision(&self) -> i32 {
            let digits = self.inner.digits() as i32;
            let exponent = self.inner.exponent();
            if exponent <= 0 {
                digits.max(-exponent)
            } else {
                digits + exponent
            }
        }

        /// Return decimal scale, i.e. the number of decimal digits after the
        /// decimal separator.
        #[inline(always)]
        pub fn scale(&self) -> i32 {
            if self.inner.exponent() < 0 {
                -self.inner.exponent()
            } else {
                0
            }
        }

        /// Check if the fractional part of the number is `0`
        #[inline(always)]
        pub fn is_int(&self) -> bool {
            // https://github.com/tarantool/decNumber/blob/c123821c11b981cba0113a031e555582ad1b3731/decNumber.c#L504
            let (_, exponent, _, lsu) = self.inner.to_raw_parts();
            if exponent >= 0 {
                return true;
            }
            let mut count = -exponent as usize;
            let mut uit = 0;
            // spin up whole units until reach the Unit with the unit digit
            while count >= ffi::DECDPUN {
                if lsu[uit] != 0 {
                    return false;
                };
                count -= ffi::DECDPUN;
                uit += 1;
            }
            if count == 0 {
                return true; // [a multiple of DECDPUN]
            } else {
                // [not multiple of DECDPUN]
                const POWERS: [u16; ffi::DECDPUN] = [1, 10, 100];
                // slice off fraction digits and check for non-zero
                let rem = lsu[uit] % POWERS[count]; // slice off discards
                if rem != 0 {
                    return false;
                }
            }
            true
        }

        /// Remove trailing zeros from the fractional part of a number.
        #[inline(always)]
        pub fn trim(mut self) -> Self {
            self.inner.trim();
            self
        }

        fn round_to_with_mode(mut self, scale: u8, mode: dec::Rounding) -> Option<Self> {
            // https://github.com/tarantool/tarantool/blob/c78cc10338d7ea62597798c329a1628ae6802be6/src/lib/core/decimal.c#L242
            if scale > ffi::DECIMAL_MAX_DIGITS as _ {
                return None;
            }

            if scale >= self.scale() as _ {
                return Some(self);
            }

            let ndig = (self.precision() - self.scale() + scale as i32).max(1);
            CONTEXT.with(|ctx| {
                let Context(mut ctx) = ctx.borrow().clone();
                ctx.set_precision(ndig as _).unwrap();
                ctx.set_max_exponent(ndig as _).unwrap();
                ctx.set_min_exponent(if scale != 0 { -1 } else { 0 })
                    .unwrap();
                ctx.set_rounding(mode);

                ctx.plus(&mut self.inner);
                check_status(ctx.status()).ok()
            })?;
            Self::try_from(self.inner).ok()
        }

        /// Round a given decimal to have zero digits after the decimal point.
        #[inline(always)]
        pub fn round(self) -> Self {
            self.round_to_with_mode(0, dec::Rounding::HalfUp).unwrap()
        }

        /// Floor a given decimal towards zero to have zero digits after the decimal
        /// point.
        #[inline(always)]
        pub fn floor(self) -> Self {
            self.round_to_with_mode(0, dec::Rounding::Down).unwrap()
        }

        /// Round a given decimal to have no more than `scale` digits after the
        /// decimal point.  If `scale` if greater than current `self.scale()`,
        /// return `self` unchanged. Scale must be in range `[0..=
        /// ffi::DECIMAL_MAX_DIGITS]`. Return `None` if `scale` if out of bounds.
        #[inline(always)]
        pub fn round_to(self, scale: u8) -> Option<Self> {
            self.round_to_with_mode(scale, dec::Rounding::HalfUp)
        }

        /// Like [`Decimal::round`] but rounds the number towards zero.
        #[inline(always)]
        pub fn floor_to(self, scale: u8) -> Option<Self> {
            self.round_to_with_mode(scale, dec::Rounding::Down)
        }

        /// Set scale of `self` to `scale`. If `scale` < `self.scale()`, performs
        /// the equivalent of `self.`[`round`]`(scale)`.  Otherwise appends a
        /// sufficient amount of trailing fractional zeros. Return `None` if `scale`
        /// < `0` or too big.
        ///
        /// [`round`]: Decimal::round
        #[inline(always)]
        pub fn rescale(mut self, scale: u8) -> Option<Self> {
            // https://github.com/tarantool/tarantool/blob/c78cc10338d7ea62597798c329a1628ae6802be6/src/lib/core/decimal.c#L289
            if scale <= self.scale() as _ {
                return self.round_to(scale);
            }
            if scale as u32 > ffi::DECIMAL_MAX_DIGITS {
                return None;
            }
            /* how much zeros shoud we append. */
            let delta = scale as i32 + self.inner.exponent();
            if self.inner.digits() + delta as u32 > ffi::DECIMAL_MAX_DIGITS {
                return None;
            }
            // This `Self::from()` call may also acquire the context, so it must
            // not be done in the callback passed into `with_context`.
            let scale = Self::from(-(scale as i32));
            with_context(|ctx| ctx.rescale(&mut self.inner, &scale.inner))?;
            Self::try_from(self.inner).ok()
        }

        /// Return the absolute value of the number.
        #[inline(always)]
        pub fn abs(mut self) -> Self {
            with_context(|ctx| ctx.abs(&mut self.inner)).expect("abs is a safe operation");
            unsafe { Self::from_inner_unchecked(self.inner) }
        }

        /// Compute logarithm base 10.
        #[inline(always)]
        pub fn log10(mut self) -> Option<Self> {
            with_context(|ctx| ctx.log10(&mut self.inner))?;
            Self::try_from(self.inner).ok()
        }

        /// Compute natural logarithm.
        #[inline(always)]
        pub fn ln(mut self) -> Option<Self> {
            with_context(|ctx| ctx.ln(&mut self.inner))?;
            Self::try_from(self.inner).ok()
        }

        /// Exponentiate `self`. Return `None` if the result is out of range.
        #[inline(always)]
        pub fn exp(mut self) -> Option<Self> {
            with_context(|ctx| ctx.exp(&mut self.inner))?;
            Self::try_from(self.inner).ok()
        }

        /// Compute square root of `self`. Return `None` if the result is imaginary
        /// or out of range.
        #[inline(always)]
        pub fn sqrt(mut self) -> Option<Self> {
            with_context(|ctx| ctx.sqrt(&mut self.inner))?;
            Self::try_from(self.inner).ok()
        }

        /// Compute `self` raised to the power of `pow`. Return `None` if the result
        /// is out of range.
        #[inline(always)]
        pub fn pow(mut self, pow: impl Into<Self>) -> Option<Self> {
            // This `.into()` call may also acquire the context, so it must
            // not be done in the callback passed into `with_context`.
            let pow = pow.into();
            with_context(|ctx| ctx.pow(&mut self.inner, &pow.inner))?;
            Self::try_from(self.inner).ok()
        }
    }

    type DecimalImpl = dec::Decimal<{ ffi::DECNUMUNITS as _ }>;

    #[derive(Debug, Copy, Clone, PartialEq, Eq, thiserror::Error)]
    pub enum ToDecimalError {
        #[error("Infinite decimals are not supported")]
        Infinite,
        #[error("NaN decimals are not supported")]
        Nan,
    }

    impl TryFrom<DecimalImpl> for Decimal {
        type Error = ToDecimalError;

        #[inline(always)]
        fn try_from(inner: DecimalImpl) -> Result<Self, Self::Error> {
            if inner.is_finite() {
                Ok(Self { inner })
            } else if inner.is_nan() {
                Err(ToDecimalError::Nan)
            } else if inner.is_infinite() {
                Err(ToDecimalError::Infinite)
            } else {
                unreachable!()
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////
    /// Context
    ////////////////////////////////////////////////////////////////////////////////

    #[derive(Clone)]
    struct Context(dec::Context<DecimalImpl>);

    impl Default for Context {
        fn default() -> Self {
            let mut ctx = dec::Context::default();
            ctx.set_rounding(dec::Rounding::HalfUp);
            ctx.set_precision(ffi::DECIMAL_MAX_DIGITS as _).unwrap();
            ctx.set_clamp(false);
            ctx.set_max_exponent((ffi::DECIMAL_MAX_DIGITS - 1) as _)
                .unwrap();
            ctx.set_min_exponent(-1).unwrap();
            Self(ctx)
        }
    }

    // This makes Decimals thread safe in exchange for some performance penalty.
    thread_local! {
        static CONTEXT: Lazy<std::cell::RefCell<Context>> = Lazy::new(std::cell::RefCell::default);
    }

    /// # Panics
    ///
    /// If callback also borrows the static `CONTEXT`.
    #[inline(always)]
    fn with_context<F, T>(f: F) -> Option<T>
    where
        F: FnOnce(&mut dec::Context<DecimalImpl>) -> T,
    {
        CONTEXT.with(|ctx| {
            let Context(ctx) = &mut *ctx.borrow_mut();
            let res = f(ctx);
            let status = ctx.status();
            ctx.set_status(Default::default());
            check_status(status).map(|()| res).ok()
        })
    }

    ////////////////////////////////////////////////////////////////////////////////
    /// Status
    ////////////////////////////////////////////////////////////////////////////////

    const _: () = {
        if size_of::<dec::Status>() != size_of::<u32>()
            || size_of::<dec::Status>() != size_of::<Status>()
        {
            panic!("unsupported layout")
        }
    };

    #[derive(Clone, Copy)]
    pub struct Status {
        inner: u32,
    }

    impl From<dec::Status> for Status {
        fn from(s: dec::Status) -> Self {
            unsafe { std::mem::transmute(s) }
        }
    }

    impl From<Status> for dec::Status {
        fn from(s: Status) -> Self {
            unsafe { std::mem::transmute(s) }
        }
    }

    impl std::fmt::Debug for Status {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let status = dec::Status::from(*self);
            let mut s = f.debug_struct("Status");
            if status.conversion_syntax() {
                s.field("conversion_syntax", &true);
            }
            if status.division_by_zero() {
                s.field("division_by_zero", &true);
            }
            if status.division_impossible() {
                s.field("division_impossible", &true);
            }
            if status.division_undefined() {
                s.field("division_undefined", &true);
            }
            if status.insufficient_storage() {
                s.field("insufficient_storage", &true);
            }
            if status.inexact() {
                s.field("inexact", &true);
            }
            if status.invalid_context() {
                s.field("invalid_context", &true);
            }
            if status.invalid_operation() {
                s.field("invalid_operation", &true);
            }
            if status.overflow() {
                s.field("overflow", &true);
            }
            if status.clamped() {
                s.field("clamped", &true);
            }
            if status.rounded() {
                s.field("rounded", &true);
            }
            if status.subnormal() {
                s.field("subnormal", &true);
            }
            if status.underflow() {
                s.field("underflow", &true);
            }
            s.finish()
        }
    }

    #[track_caller]
    fn check_status(status: dec::Status) -> Result<(), Status> {
        // https://github.com/tarantool/tarantool/blob/c78cc10338d7ea62597798c329a1628ae6802be6/src/lib/core/decimal.c#L80
        let mut ignore = dec::Status::default();
        ignore.set_inexact();
        ignore.set_rounded();
        ignore.set_underflow();
        ignore.set_subnormal();
        ignore.set_clamped();
        let ignore = Status::from(ignore).inner;
        let mut status = Status::from(status);
        status.inner &= !ignore;
        (status.inner == 0).then_some(()).ok_or(status)
    }

    #[allow(clippy::non_canonical_partial_ord_impl)]
    impl std::cmp::PartialOrd for Decimal {
        #[inline(always)]
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            with_context(|ctx| ctx.partial_cmp(&self.inner, &other.inner)).flatten()
        }
    }

    impl std::hash::Hash for Decimal {
        #[inline(always)]
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            let (digits, exponent, bits, lsu) = self.trim().inner.to_raw_parts();
            digits.hash(state);
            exponent.hash(state);
            bits.hash(state);
            for u in &lsu[0..digits as usize / ffi::DECDPUN] {
                u.hash(state);
            }
        }
    }

    macro_rules! impl_bin_op {
        ($m:ident, $trait:ident, $op:ident, $ass_trait:ident, $ass_op:ident) => {
            impl Decimal {
                #[inline(always)]
                #[track_caller]
                pub fn $m(mut self, rhs: impl Into<Self>) -> Option<Self> {
                    // This `.into()` call may also acquire the context, so it must
                    // not be done in the callback passed into `with_context`.
                    let rhs = rhs.into();
                    with_context(|ctx| ctx.$op(&mut self.inner, &rhs.inner))?;
                    Self::try_from(self.inner).ok()
                }
            }

            impl<T: Into<Decimal>> std::ops::$trait<T> for Decimal {
                type Output = Self;

                #[inline(always)]
                #[track_caller]
                fn $op(self, rhs: T) -> Self {
                    self.$m(rhs).expect("overflow")
                }
            }

            impl<T: Into<Decimal>> std::ops::$ass_trait<T> for Decimal {
                #[inline(always)]
                #[track_caller]
                fn $ass_op(&mut self, rhs: T) {
                    *self = self.$m(rhs).expect("overflow")
                }
            }
        };
    }

    impl_bin_op! {checked_add, Add, add, AddAssign, add_assign}
    impl_bin_op! {checked_sub, Sub, sub, SubAssign, sub_assign}
    impl_bin_op! {checked_mul, Mul, mul, MulAssign, mul_assign}
    impl_bin_op! {checked_div, Div, div, DivAssign, div_assign}
    impl_bin_op! {checked_rem, Rem, rem, RemAssign, rem_assign}

    impl std::ops::Neg for Decimal {
        type Output = Self;

        #[inline(always)]
        fn neg(self) -> Self {
            unsafe { Self::from_inner_unchecked(self.inner.neg()) }
        }
    }

    impl std::fmt::Display for Decimal {
        #[inline(always)]
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.inner.fmt(f)
        }
    }

    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub struct DecimalFromStrError;

    impl std::str::FromStr for Decimal {
        type Err = DecimalFromStrError;

        #[inline(always)]
        fn from_str(s: &str) -> Result<Self, Self::Err> {
            with_context(|ctx| ctx.parse(s).ok())
                .flatten()
                .and_then(|d| Self::try_from(d).ok())
                .ok_or(DecimalFromStrError)
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
            s.to_str()
                .map_err(|_| DecimalFromStrError)
                .and_then(str::parse)
        }
    }

    impl<L: tlua::AsLua> tlua::Push<L> for Decimal {
        type Err = tlua::Void;

        #[inline]
        fn push_to_lua(&self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
            Ok(lua.push_one(tlua::CData(self.to_ffi())))
        }
    }

    impl<L: tlua::AsLua> tlua::PushOne<L> for Decimal {}

    impl<L: tlua::AsLua> tlua::PushInto<L> for Decimal {
        type Err = tlua::Void;

        #[inline]
        fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
            Ok(lua.push_one(tlua::CData(self.to_ffi())))
        }
    }

    macro_rules! impl_from_int {
        ($($t:ty)+ => $f:expr) => {
            $(
                impl From<$t> for Decimal {
                    #[inline(always)]
                    fn from(num: $t) -> Self {
                        unsafe {
                            Self::from_inner_unchecked($f(num))
                        }
                    }
                }
            )+
        }
    }

    impl_from_int! {i8 i16 i32 u8 u16 u32 => DecimalImpl::from}
    impl_from_int! {
        i64 isize => |num| {
            CONTEXT.with(|ctx| ctx.borrow_mut().0.from_i64(num as _))
        }
    }
    impl_from_int! {
        u64 usize => |num| {
            CONTEXT.with(|ctx| ctx.borrow_mut().0.from_u64(num as _))
        }
    }

    use super::DecimalFromfloatError;

    macro_rules! impl_tryfrom_float {
        ($($t:ty => $f:ident)+) => {
            $(
                impl std::convert::TryFrom<$t> for Decimal {
                    type Error = DecimalFromfloatError<$t>;

                    #[inline(always)]
                    fn try_from(num: $t) -> Result<Self, Self::Error> {
                        with_context(|ctx| ctx.$f(num))
                            .and_then(|inner| Self::try_from(inner).ok())
                            .ok_or_else(|| DecimalFromfloatError::from(num))
                    }
                }
            )+
        }
    }

    impl_tryfrom_float! {
        f32 => from_f32
        f64 => from_f64
    }

    use super::DecimalToIntError;

    macro_rules! impl_try_into_int {
        ($($t:ty => $f:ident)+) => {
            $(
                impl std::convert::TryFrom<Decimal> for $t {
                    type Error = DecimalToIntError;

                    #[inline]
                    fn try_from(dec: Decimal) -> Result<Self, Self::Error> {
                        with_context(|ctx| ctx.$f(dec.inner).ok())
                            .flatten()
                            .ok_or_else(||
                                if dec.is_int() {
                                    DecimalToIntError::OutOfRange
                                } else {
                                    DecimalToIntError::NonInteger
                                }
                            )
                    }
                }
            )+
        }
    }

    impl_try_into_int! {
        i64   => try_into_i64
        isize => try_into_isize
        u64   => try_into_u64
        usize => try_into_usize
    }

    impl serde::Serialize for Decimal {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            #[derive(serde::Serialize)]
            struct _ExtStruct((i8, serde_bytes::ByteBuf));

            let data = {
                let mut data = vec![];
                let (bcd, scale) = self.inner.clone().to_packed_bcd().unwrap();
                rmp::encode::write_sint(&mut data, scale as i64).unwrap();
                data.extend(bcd);
                data
            };
            _ExtStruct((ffi::MP_DECIMAL, serde_bytes::ByteBuf::from(data))).serialize(serializer)
        }
    }

    impl<'de> serde::Deserialize<'de> for Decimal {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            use serde::de::Error;
            #[derive(serde::Deserialize)]
            struct _ExtStruct((i8, serde_bytes::ByteBuf));

            match serde::Deserialize::deserialize(deserializer)? {
                _ExtStruct((ffi::MP_DECIMAL, bytes)) => {
                    let mut data = bytes.as_slice();
                    let scale = rmp::decode::read_int(&mut data).unwrap();
                    let bcd = data;
                    DecimalImpl::from_packed_bcd(bcd, scale)
                        .map_err(|e| Error::custom(format!("Failed to unpack decimal: {e}")))?
                        .try_into()
                        .map_err(|e| Error::custom(format!("Failed to unpack decimal: {e}")))
                }
                _ExtStruct((kind, _)) => Err(serde::de::Error::custom(format!(
                    "Expected Decimal, found msgpack ext #{}",
                    kind
                ))),
            }
        }
    }

    #[macro_export]
    macro_rules! decimal {
        ($($num:tt)+) => {
            {
                let r_str = ::std::concat![$(::std::stringify!($num)),+];
                let dec: $crate::decimal::Decimal = ::std::convert::TryFrom::try_from(r_str)
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
}

#[cfg(not(feature = "standalone_decimal"))]
pub use tarantool_decimal::*;

#[cfg(feature = "standalone_decimal")]
pub use standalone_decimal::*;

impl Decimal {
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
                    self.is_int() && self == &Self::from(other)
                }
            }
        )+
    }
}

impl_cmp_int! {i8 i16 i32 i64 isize u8 u16 u32 u64 usize}

////////////////////////////////////////////////////////////////////////////////
/// Lua
////////////////////////////////////////////////////////////////////////////////
static CTID_DECIMAL: Lazy<u32> = Lazy::new(|| {
    use tlua::AsLua;
    let lua = crate::global_lua();
    let ctid_decimal = unsafe { tlua::ffi::luaL_ctypeid(lua.as_lua(), crate::c_ptr!("decimal_t")) };
    debug_assert!(ctid_decimal != 0);
    ctid_decimal
});

unsafe impl tlua::AsCData for ffi::decNumber {
    #[inline(always)]
    fn ctypeid() -> tlua::ffi::CTypeID {
        *CTID_DECIMAL
    }
}

impl<L> tlua::LuaRead<L> for Decimal
where
    L: tlua::AsLua,
{
    #[inline]
    fn lua_read_at_position(lua: L, index: std::num::NonZeroI32) -> tlua::ReadResult<Self, L> {
        let tlua::CData(dec) = lua.read_at_nz(index)?;
        unsafe { Ok(Self::from_raw(dec)) }
    }
}

impl<L: tlua::AsLua> tlua::PushOneInto<L> for Decimal {}

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

impl_error_from_float! {f32 f64}

impl<T: std::fmt::Display> std::fmt::Display for DecimalFromfloatError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfRange(num) => {
                write!(
                    f,
                    "float `{}` cannot be represented using {} digits",
                    num,
                    ffi::DECIMAL_MAX_DIGITS,
                )
            }
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

#[cfg(feature = "internal_test")]
mod tests {
    use std::convert::TryFrom;

    use crate::{decimal, decimal::Decimal, tuple::Tuple};

    #[crate::test(tarantool = "crate")]
    pub fn from_lua() {
        let d: Decimal = crate::lua_state()
            .eval("return require('decimal').new('-8.11')")
            .unwrap();
        assert_eq!(d.to_string(), "-8.11");
    }

    #[crate::test(tarantool = "crate")]
    pub fn to_lua() {
        let lua = crate::lua_state();
        let tostring: tlua::LuaFunction<_> = lua.eval("return tostring").unwrap();
        let d: Decimal = "-8.11".parse().unwrap();
        let s: String = tostring.call_with_args(d).unwrap();
        assert_eq!(s, "-8.11");
    }

    #[crate::test(tarantool = "crate")]
    pub fn from_tuple() {
        let t: Tuple = crate::lua_state()
            .eval("return box.tuple.new(require('decimal').new('-8.11'))")
            .unwrap();
        let (d,): (Decimal,) = t.decode().unwrap();
        assert_eq!(d.to_string(), "-8.11");
    }

    #[crate::test(tarantool = "crate")]
    pub fn to_tuple() {
        let d = decimal!(-8.11);
        let t = Tuple::new(&[d]).unwrap();
        let lua = crate::lua_state();
        let f: tlua::LuaFunction<_> = lua.eval("return box.tuple.unpack").unwrap();
        let d: Decimal = f.call_with_args(&t).unwrap();
        assert_eq!(d.to_string(), "-8.11");
    }

    #[crate::test(tarantool = "crate")]
    fn from_string() {
        let d: Decimal = "-81.1e-1".parse().unwrap();
        assert_eq!(d.to_string(), "-8.11");
        assert_eq!(decimal!(-81.1e-1).to_string(), "-8.11");

        assert_eq!("foobar".parse::<Decimal>().ok(), None::<Decimal>);
        assert_eq!("".parse::<Decimal>().ok(), None::<Decimal>);

        // tarantool decimals don't support infinity or NaN
        assert_eq!("inf".parse::<Decimal>().ok(), None::<Decimal>);
        assert_eq!("infinity".parse::<Decimal>().ok(), None::<Decimal>);
        assert_eq!("NaN".parse::<Decimal>().ok(), None::<Decimal>);
    }

    #[crate::test(tarantool = "crate")]
    fn from_num() {
        assert_eq!(Decimal::from(0i8), Decimal::zero());
        assert_eq!(Decimal::from(42i8).to_string(), "42");
        assert_eq!(Decimal::from(i8::MAX).to_string(), "127");
        assert_eq!(Decimal::from(i8::MIN).to_string(), "-128");
        assert_eq!(Decimal::from(0i16), Decimal::zero());
        assert_eq!(Decimal::from(42i16).to_string(), "42");
        assert_eq!(Decimal::from(i16::MAX).to_string(), "32767");
        assert_eq!(Decimal::from(i16::MIN).to_string(), "-32768");
        assert_eq!(Decimal::from(0i32), Decimal::zero());
        assert_eq!(Decimal::from(42i32).to_string(), "42");
        assert_eq!(Decimal::from(i32::MAX).to_string(), "2147483647");
        assert_eq!(Decimal::from(i32::MIN).to_string(), "-2147483648");
        assert_eq!(Decimal::from(0i64), Decimal::zero());
        assert_eq!(Decimal::from(42i64).to_string(), "42");
        assert_eq!(Decimal::from(i64::MAX).to_string(), "9223372036854775807");
        assert_eq!(Decimal::from(i64::MIN).to_string(), "-9223372036854775808");
        assert_eq!(Decimal::from(0isize), Decimal::zero());
        assert_eq!(Decimal::from(42isize).to_string(), "42");
        assert_eq!(Decimal::from(isize::MAX).to_string(), "9223372036854775807");
        assert_eq!(
            Decimal::from(isize::MIN).to_string(),
            "-9223372036854775808"
        );

        assert_eq!(Decimal::from(0u8), Decimal::zero());
        assert_eq!(Decimal::from(42u8).to_string(), "42");
        assert_eq!(Decimal::from(u8::MAX).to_string(), "255");
        assert_eq!(Decimal::from(0u16), Decimal::zero());
        assert_eq!(Decimal::from(42u16).to_string(), "42");
        assert_eq!(Decimal::from(u16::MAX).to_string(), "65535");
        assert_eq!(Decimal::from(0u32), Decimal::zero());
        assert_eq!(Decimal::from(42u32).to_string(), "42");
        assert_eq!(Decimal::from(u32::MAX).to_string(), "4294967295");
        assert_eq!(Decimal::from(0u64), Decimal::zero());
        assert_eq!(Decimal::from(42u64).to_string(), "42");
        assert_eq!(Decimal::from(u64::MAX).to_string(), "18446744073709551615");
        assert_eq!(Decimal::from(0usize), Decimal::zero());
        assert_eq!(Decimal::from(42usize).to_string(), "42");
        assert_eq!(
            Decimal::from(usize::MAX).to_string(),
            "18446744073709551615"
        );

        assert_eq!(Decimal::try_from(0f32).unwrap(), Decimal::zero());

        // with standalone decimal formatting is slightly different, i e it favors scientific notation
        if cfg!(feature = "standalone_decimal") {
            assert_eq!(Decimal::try_from(-8.11f32).unwrap().to_string(), "-8.11");
        } else {
            assert_eq!(
                Decimal::try_from(-8.11f32).unwrap().to_string(),
                "-8.10999965667725"
            );
        }
        assert_eq!(
            Decimal::try_from(f32::INFINITY).unwrap_err().to_string(),
            "float is infinite"
        );
        assert_eq!(
            Decimal::try_from(f32::NEG_INFINITY)
                .unwrap_err()
                .to_string(),
            "float is infinite"
        );
        assert_eq!(
            Decimal::try_from(f32::NAN).unwrap_err().to_string(),
            "float is NaN"
        );
        if cfg!(feature = "standalone_decimal") {
            assert_eq!(
                Decimal::try_from(f32::EPSILON).unwrap().to_string(),
                "1.1920929E-7"
            );
        } else {
            assert_eq!(
                Decimal::try_from(f32::EPSILON).unwrap().to_string(),
                "0.000000119209289550781"
            );
        }
        assert_eq!(
            Decimal::try_from(f32::MIN).unwrap_err().to_string(),
            "float `-340282350000000000000000000000000000000` cannot be represented using 38 digits"
        );
        assert_eq!(
            Decimal::try_from(f32::MAX).unwrap_err().to_string(),
            "float `340282350000000000000000000000000000000` cannot be represented using 38 digits"
        );
        assert_eq!(Decimal::try_from(1.0e-40_f32).unwrap(), Decimal::zero());

        if cfg!(feature = "standalone_decimal") {
            assert_eq!(
                Decimal::try_from(1e37_f32).unwrap().to_string(),
                "10000000000000000000000000000000000000"
            );
        } else {
            assert_eq!(
                Decimal::try_from(1e37_f32).unwrap().to_string(),
                "9999999933815810000000000000000000000"
            );
        }
        assert_eq!(
            Decimal::try_from(1e38_f64).unwrap_err().to_string(),
            "float `100000000000000000000000000000000000000` cannot be represented using 38 digits"
        );

        assert_eq!(Decimal::try_from(0f64).unwrap(), Decimal::zero());
        assert_eq!(Decimal::try_from(-8.11f64).unwrap().to_string(), "-8.11");
        assert_eq!(
            Decimal::try_from(f64::INFINITY).unwrap_err().to_string(),
            "float is infinite"
        );
        assert_eq!(
            Decimal::try_from(f64::NEG_INFINITY)
                .unwrap_err()
                .to_string(),
            "float is infinite"
        );
        assert_eq!(
            Decimal::try_from(f64::NAN).unwrap_err().to_string(),
            "float is NaN"
        );

        if cfg!(feature = "standalone_decimal") {
            assert_eq!(
                Decimal::try_from(f64::EPSILON).unwrap().to_string(),
                "2.220446049250313E-16"
            );
        } else {
            assert_eq!(
                Decimal::try_from(f64::EPSILON).unwrap().to_string(),
                "0.000000000000000222044604925031"
            );
        }

        assert_eq!(Decimal::try_from(f64::MIN).unwrap_err().to_string(),
            "float `-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000` cannot be represented using 38 digits"
        );
        assert_eq!(Decimal::try_from(f64::MAX).unwrap_err().to_string(),
            "float `179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000` cannot be represented using 38 digits"
        );
        assert_eq!(Decimal::try_from(1.0e-40_f64).unwrap(), Decimal::zero());
        assert_eq!(
            Decimal::try_from(1e38_f64).unwrap_err().to_string(),
            "float `100000000000000000000000000000000000000` cannot be represented using 38 digits"
        );
    }

    #[crate::test(tarantool = "crate")]
    pub fn to_num() {
        assert_eq!(i64::try_from(decimal!(420)).unwrap(), 420);
        assert_eq!(
            i64::try_from(decimal!(9223372036854775807)).unwrap(),
            i64::MAX
        );
        assert_eq!(
            i64::try_from(decimal!(9223372036854775808))
                .unwrap_err()
                .to_string(),
            "decimal is out of range"
        );
        assert_eq!(
            i64::try_from(decimal!(-9223372036854775808)).unwrap(),
            i64::MIN
        );
        assert_eq!(
            i64::try_from(decimal!(-9223372036854775809))
                .unwrap_err()
                .to_string(),
            "decimal is out of range"
        );
        assert_eq!(
            i64::try_from(decimal!(3.14)).unwrap_err().to_string(),
            "decimal is not an integer"
        );

        assert_eq!(isize::try_from(decimal!(420)).unwrap(), 420);
        assert_eq!(
            isize::try_from(decimal!(9223372036854775807)).unwrap(),
            isize::MAX
        );
        assert_eq!(
            isize::try_from(decimal!(9223372036854775808))
                .unwrap_err()
                .to_string(),
            "decimal is out of range"
        );
        assert_eq!(
            isize::try_from(decimal!(-9223372036854775808)).unwrap(),
            isize::MIN
        );
        assert_eq!(
            isize::try_from(decimal!(-9223372036854775809))
                .unwrap_err()
                .to_string(),
            "decimal is out of range"
        );
        assert_eq!(
            isize::try_from(decimal!(3.14)).unwrap_err().to_string(),
            "decimal is not an integer"
        );

        assert_eq!(u64::try_from(decimal!(420)).unwrap(), 420);
        assert_eq!(
            u64::try_from(decimal!(18446744073709551615)).unwrap(),
            u64::MAX
        );
        assert_eq!(
            u64::try_from(decimal!(18446744073709551616))
                .unwrap_err()
                .to_string(),
            "decimal is out of range"
        );
        assert_eq!(
            u64::try_from(decimal!(-1)).unwrap_err().to_string(),
            "decimal is out of range"
        );
        assert_eq!(
            u64::try_from(decimal!(3.14)).unwrap_err().to_string(),
            "decimal is not an integer"
        );

        assert_eq!(usize::try_from(decimal!(420)).unwrap(), 420);
        assert_eq!(
            usize::try_from(decimal!(18446744073709551615)).unwrap(),
            usize::MAX
        );
        assert_eq!(
            usize::try_from(decimal!(18446744073709551616))
                .unwrap_err()
                .to_string(),
            "decimal is out of range"
        );
        assert_eq!(
            usize::try_from(decimal!(-1)).unwrap_err().to_string(),
            "decimal is out of range"
        );
        assert_eq!(
            usize::try_from(decimal!(3.14)).unwrap_err().to_string(),
            "decimal is not an integer"
        );
    }

    #[crate::test(tarantool = "crate")]
    pub fn cmp() {
        assert!(decimal!(.1) < decimal!(.2));
        assert!(decimal!(.1) <= decimal!(.2));
        assert!(decimal!(.2) > decimal!(.1));
        assert!(decimal!(.2) >= decimal!(.1));

        assert_eq!(decimal!(0), 0);
        assert_eq!(decimal!(1), 1);
        assert_eq!(decimal!(-3), -3);
        assert_ne!(decimal!(-8.11), -8);
    }

    #[crate::test(tarantool = "crate")]
    pub fn hash() {
        fn to_hash<T: std::hash::Hash>(t: &T) -> u64 {
            let mut s = std::collections::hash_map::DefaultHasher::new();
            t.hash(&mut s);
            std::hash::Hasher::finish(&s)
        }

        assert_eq!(to_hash(&decimal!(1)), to_hash(&decimal!(1.000)));
        assert_eq!(to_hash(&decimal!(1.00)), to_hash(&decimal!(1.00)));
        assert_eq!(to_hash(&decimal!(1)), to_hash(&decimal!(1)));
        assert_eq!(to_hash(&decimal!(1.000000)), to_hash(&decimal!(1.0)));
        assert_eq!(to_hash(&decimal!(1.000000000000000)), to_hash(&decimal!(1)));
        assert_ne!(
            to_hash(&decimal!(1.000000000000000)),
            to_hash(&decimal!(1.000000000000001))
        );
        assert_ne!(
            to_hash(&decimal!(1.000000000000000)),
            to_hash(&decimal!(0.999999999999999))
        );
        assert_eq!(
            to_hash(&decimal!(99999999999999999999999999999999999999)),
            to_hash(&decimal!(99999999999999999999999999999999999999))
        );
        assert_ne!(
            to_hash(&decimal!(9999999999999999999999999999999999999.0)),
            to_hash(&decimal!(9999999999999999999999999999999999998.9))
        );
        assert_eq!(to_hash(&decimal!(0)), to_hash(&decimal!(0.000)));
        assert_eq!(
            to_hash(&decimal!(-99999999999999999999999999999999999999)),
            to_hash(&decimal!(-99999999999999999999999999999999999999))
        );
        assert_eq!(to_hash(&decimal!(-1)), to_hash(&decimal!(-1.000)));
        assert_ne!(
            to_hash(&decimal!(-1.000)),
            to_hash(&decimal!(-0.9999999999999999999999999999999999999))
        );
    }

    #[crate::test(tarantool = "crate")]
    #[allow(clippy::bool_assert_comparison)]
    pub fn ops() {
        let a = decimal!(.1);
        let b = decimal!(.2);
        let c = decimal!(.3);
        assert_eq!(a + b, c);
        assert_eq!(c - b, a);
        assert_eq!(c - a, b);
        assert_eq!(b * c, decimal!(.06));
        assert_eq!(c / b, decimal!(1.5));

        let mut x = decimal!(.5);
        x += 1;
        assert_eq!(x, decimal!(1.5));
        x -= 2;
        assert_eq!(x, decimal!(-.5));
        x *= 3;
        assert_eq!(x, decimal!(-1.5));
        x /= 4;
        assert_eq!(x, decimal!(-.375));
        x %= 5;
        assert_eq!(x, decimal!(-.375));
        x += 12;
        assert_eq!(x, decimal!(11.625));
        assert_eq!(x % 5, decimal!(1.625));

        let x: Decimal = decimal!(99999999999999999999999999999999999999);
        let y: Decimal = 1.into();
        assert_eq!(x.checked_add(y), None::<Decimal>);

        let x: Decimal = decimal!(10000000000000000000000000000000000000);
        let y: Decimal = 10.into();
        assert_eq!(x.checked_mul(y), None::<Decimal>);

        let x = decimal!(-8.11);
        let y = x.abs();
        assert_eq!(y, -x);
        assert_eq!(y, decimal!(8.11));

        let x = decimal!(1.000);
        assert_eq!(x.to_string(), "1.000");
        assert_eq!(x.precision(), 4);
        assert_eq!(x.scale(), 3);
        let x = x.trim();
        assert_eq!(x.to_string(), "1");
        assert_eq!(x.precision(), 1);
        assert_eq!(x.scale(), 0);
        let x = x.rescale(3).unwrap();
        assert_eq!(x.to_string(), "1.000");
        assert_eq!(x.precision(), 4);
        assert_eq!(x.scale(), 3);

        assert_eq!(decimal!(-1).log10(), None);
        assert_eq!(decimal!(0).log10(), None);
        assert_eq!(decimal!(100).log10(), Some(decimal!(2)));
        assert_eq!(decimal!(.01).log10(), Some(decimal!(-2)));

        let e = decimal!(1).exp().unwrap();
        assert_eq!(e, decimal!(2.7182818284590452353602874713526624978));
        assert_eq!(decimal!(1000).exp(), None::<Decimal>);

        assert_eq!(e.precision(), 38);
        assert_eq!(e.scale(), 37);
        assert_eq!(e.is_int(), false);
        assert_eq!(e.round().precision(), 1);
        assert_eq!(e.round().scale(), 0);
        assert_eq!(e.round().is_int(), true);

        assert_eq!(Decimal::from(usize::MAX).precision(), 20);

        assert_eq!(e.round_to(4), Some(decimal!(2.7183)));
        assert_eq!(e.floor_to(4), Some(decimal!(2.7182)));
        assert_eq!(e.round_to(40), None::<Decimal>);
        assert_eq!(e.floor_to(40), None::<Decimal>);

        assert_eq!(decimal!(-1).ln(), None);
        assert_eq!(decimal!(0).ln(), None);
        assert_eq!(decimal!(1).ln(), Some(decimal!(0)));
        assert_eq!(e.ln(), Some(decimal!(1)));

        assert_eq!(decimal!(4).sqrt(), Some(decimal!(2)));
        assert_eq!(decimal!(-1).sqrt(), None::<Decimal>);

        assert_eq!(decimal!(2).pow(64), Some(decimal!(18446744073709551616)));
        assert_eq!(decimal!(2).pow(-2), Some(decimal!(.25)));
        assert_eq!(decimal!(10).pow(39), None::<Decimal>);
    }

    #[crate::test(tarantool = "crate")]
    #[allow(clippy::modulo_one)]
    fn no_context_contention() {
        let _should_not_panic = Decimal::from(1) + 1_usize;
        let _should_not_panic = Decimal::from(1) + 1_isize;
        let _should_not_panic = Decimal::from(1) + 1_u64;
        let _should_not_panic = Decimal::from(1) + 1_i64;
        let _should_not_panic = Decimal::from(1) + 1_u32;
        let _should_not_panic = Decimal::from(1) + 1_i32;
        let _should_not_panic = Decimal::from(1) + 1_u16;
        let _should_not_panic = Decimal::from(1) + 1_i16;
        let _should_not_panic = Decimal::from(1) + 1_u8;
        let _should_not_panic = Decimal::from(1) + 1_i8;

        let _should_not_panic = Decimal::from(1) - 1_usize;
        let _should_not_panic = Decimal::from(1) - 1_isize;
        let _should_not_panic = Decimal::from(1) - 1_u64;
        let _should_not_panic = Decimal::from(1) - 1_i64;
        let _should_not_panic = Decimal::from(1) - 1_u32;
        let _should_not_panic = Decimal::from(1) - 1_i32;
        let _should_not_panic = Decimal::from(1) - 1_u16;
        let _should_not_panic = Decimal::from(1) - 1_i16;
        let _should_not_panic = Decimal::from(1) - 1_u8;
        let _should_not_panic = Decimal::from(1) - 1_i8;

        let _should_not_panic = Decimal::from(1) * 1_usize;
        let _should_not_panic = Decimal::from(1) * 1_isize;
        let _should_not_panic = Decimal::from(1) * 1_u64;
        let _should_not_panic = Decimal::from(1) * 1_i64;
        let _should_not_panic = Decimal::from(1) * 1_u32;
        let _should_not_panic = Decimal::from(1) * 1_i32;
        let _should_not_panic = Decimal::from(1) * 1_u16;
        let _should_not_panic = Decimal::from(1) * 1_i16;
        let _should_not_panic = Decimal::from(1) * 1_u8;
        let _should_not_panic = Decimal::from(1) * 1_i8;

        let _should_not_panic = Decimal::from(1) / 1_usize;
        let _should_not_panic = Decimal::from(1) / 1_isize;
        let _should_not_panic = Decimal::from(1) / 1_u64;
        let _should_not_panic = Decimal::from(1) / 1_i64;
        let _should_not_panic = Decimal::from(1) / 1_u32;
        let _should_not_panic = Decimal::from(1) / 1_i32;
        let _should_not_panic = Decimal::from(1) / 1_u16;
        let _should_not_panic = Decimal::from(1) / 1_i16;
        let _should_not_panic = Decimal::from(1) / 1_u8;
        let _should_not_panic = Decimal::from(1) / 1_i8;

        let _should_not_panic = Decimal::from(1) % 1_usize;
        let _should_not_panic = Decimal::from(1) % 1_isize;
        let _should_not_panic = Decimal::from(1) % 1_u64;
        let _should_not_panic = Decimal::from(1) % 1_i64;
        let _should_not_panic = Decimal::from(1) % 1_u32;
        let _should_not_panic = Decimal::from(1) % 1_i32;
        let _should_not_panic = Decimal::from(1) % 1_u16;
        let _should_not_panic = Decimal::from(1) % 1_i16;
        let _should_not_panic = Decimal::from(1) % 1_u8;
        let _should_not_panic = Decimal::from(1) % 1_i8;

        let _should_not_panic = Decimal::from(1).pow(1_usize);
        let _should_not_panic = Decimal::from(1).pow(1_isize);
        let _should_not_panic = Decimal::from(1).pow(1_u64);
        let _should_not_panic = Decimal::from(1).pow(1_i64);
        let _should_not_panic = Decimal::from(1).pow(1_u32);
        let _should_not_panic = Decimal::from(1).pow(1_i32);
        let _should_not_panic = Decimal::from(1).pow(1_u16);
        let _should_not_panic = Decimal::from(1).pow(1_i16);
        let _should_not_panic = Decimal::from(1).pow(1_u8);
        let _should_not_panic = Decimal::from(1).pow(1_i8);
    }

    #[crate::test(tarantool = "crate")]
    fn decimal_msgpack_exact_bytes() {
        // Serialized version is obtained like that:
        // decimal = require('decimal')
        // msgpack = require('msgpack')
        // msgpack.encode(decimal.new('3333.3333'))
        let examples: &[(Decimal, &[u8])] = &[
            (decimal!(0.33), &[199, 3, 1, 2, 3, 60]),
            (decimal!(0.3333), &[214, 1, 4, 3, 51, 60]),
            (decimal!(3.33), &[199, 3, 1, 2, 51, 60]),
            (decimal!(33333.33), &[199, 5, 1, 2, 51, 51, 51, 60]),
            (decimal!(0), &[213, 1, 0, 12]),
            (decimal!(14), &[199, 3, 1, 0, 1, 76]),
        ];

        for (value, serialized) in examples {
            // serialized value has the same representation as expected
            let new_serialized = rmp_serde::encode::to_vec(value).expect("cant serialize decimal");
            assert_eq!(
                serialized, &new_serialized,
                "{value} was not encoded correctly"
            );
            // we can deserialize from expected bytes to the same value
            let new_value: Decimal =
                rmp_serde::decode::from_slice(serialized).expect("cant deserialize decimal");
            assert_eq!(&new_value, value);
        }

        // separately test that we can decode from the shape we used to encode decimals with standalonoe_decimal
        // see https://git.picodata.io/picodata/picodata/tarantool-module/-/issues/200
        let value: Decimal = rmp_serde::decode::from_slice(&[199, 7, 1, 210, 0, 0, 0, 2, 3, 60])
            .expect("cant deserialize decimal");
        assert_eq!(value, decimal!(0.33));
    }
}
