use std::{
    os::raw::c_char,
    hash::{Hash, Hasher},
};

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct decNumber {
    pub digits: i32,
    pub exponent: i32,
    pub bits: u8,
    pub lsu: [u16; DECNUMUNITS as _],
}

impl Hash for decNumber {
    #[inline(always)]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.digits.hash(state);
        self.exponent.hash(state);
        self.bits.hash(state);
        for i in 0..self.digits as usize / DECDPUN {
            self.lsu[i].hash(state);
        }
    }
}

extern "C" {
    pub static CTID_DECIMAL: u32;
}

pub const DECDPUN: usize = 3;
pub const DECNUMUNITS: u32 = 13;
pub const DECIMAL_MAX_DIGITS: u32 = 38;
pub const MP_DECIMAL: c_char = 1;

extern "C" {
    /// Return decimal precision,
    /// i.e. the amount of decimal digits in
    /// its representation.
    pub fn decimal_precision(dec: *const decNumber) -> ::std::os::raw::c_int;

    /// Return decimal scale,
    /// i.e. the number of decimal digits after
    /// the decimal separator.
    pub fn decimal_scale(dec: *const decNumber) -> ::std::os::raw::c_int;

    /// Initialize a zero decimal number.
    pub fn decimal_zero(dec: *mut decNumber) -> *mut decNumber;

    /// Return `true` if the fractional part of the number is `0`,
    /// `false` otherwise.
    pub fn decimal_is_int(dec: *const decNumber) -> bool;

    /// Initialize a decimal with a value from the string.
    ///
    /// If the number is less, than `10^DECIMAL_MAX_DIGITS`,
    /// but has excess digits in fractional part, it will be rounded.
    ///
    /// Return `NULL` if string is invalid or
    /// the number is too big (>= `10^DECIMAL_MAX_DIGITS`)
    pub fn decimal_from_string(
        dec: *mut decNumber,
        s: *const ::std::os::raw::c_char,
    ) -> *mut decNumber;

    /// Initialize a decimal with a value from the valid beginning
    /// of the string.
    /// If `endptr` is not `NULL`, store the address of the first
    /// invalid character in *endptr.
    ///
    /// If the number is less, than `10^DECIMAL_MAX_DIGITS`,
    /// but has excess digits in fractional part, it will be rounded.
    ///
    /// Return `NULL` if string is invalid or
    /// the number is too big (>= `10^DECIMAL_MAX_DIGITS`)
    pub fn strtodec(
        dec: *mut decNumber,
        s: *const ::std::os::raw::c_char,
        endptr: *mut *const ::std::os::raw::c_char,
    ) -> *mut decNumber;

    /// Initialize a decimal from double.
    ///
    /// Return
    /// - `NULL` if double is `NaN` or `Infinity`,
    ///          or is greater than `10^DECIMAL_MAX_DIGITS`.
    /// - `dec` otherwise.
    pub fn decimal_from_double(dec: *mut decNumber, d: f64) -> *mut decNumber;

    /// Initialize a decimal with an integer value.
    pub fn decimal_from_int64(dec: *mut decNumber, num: i64) -> *mut decNumber;

    /// Initialize a decimal with an integer value.
    pub fn decimal_from_uint64(dec: *mut decNumber, num: u64) -> *mut decNumber;

    /// `dn` is the decNumber to convert
    /// `string` is the string where the result will be laid out
    ///
    /// `string` must be at least `digits count + 14` characters long
    ///
    /// No error is possible, and no status can be set.
    pub fn decNumberToString(dn: *const decNumber, string: *mut c_char) -> *mut c_char;

    /// Convert a given decimal to `i64`.
    /// `num` - the result.
    /// Return `NULL` if `dec` doesn't fit into `i64`
    pub fn decimal_to_int64(dec: *const decNumber, num: *mut i64) -> *const decNumber;

    /// Convert a given decimal to `u64`.
    /// `num` - the result.
    /// Return `NULL` if `dec` doesn't fit into `u64`
    pub fn decimal_to_uint64(dec: *const decNumber, num: *mut u64) -> *const decNumber;

    /// Compare 2 decimal values.
    /// Return
    /// - -1, `lhs` < `rhs`,
    /// -  0, `lhs` = `rhs`,
    /// -  1, `lhs` > `rhs`
    pub fn decimal_compare(lhs: *const decNumber, rhs: *const decNumber) -> ::std::os::raw::c_int;

    /// Round a given decimal to have not more than
    /// `scale` digits after the decimal point.
    /// If `scale` if greater than current `dec` scale, do nothing.
    /// Scale must be in range `[0, DECIMAL_MAX_DIGITS]`
    ///
    /// Return `NULL`, if scale is out of bounds.
    pub fn decimal_round(dec: *mut decNumber, scale: ::std::os::raw::c_int) -> *mut decNumber;

    /// Round a decimal towards zero.
    /// See also [`decimal_round`]
    pub fn decimal_floor(dec: *mut decNumber, scale: ::std::os::raw::c_int) -> *mut decNumber;

    /// Remove trailing zeros from the fractional part of a number.
    /// Return `dec` with trimmed fractional zeros.
    pub fn decimal_trim(dec: *mut decNumber) -> *mut decNumber;

    /// Set scale of `dec` to `scale`.
    /// If `scale` < `scale(dec)`,
    /// performs [`decimal_round`]`()`.
    /// Otherwise appends a sufficient amount of trailing
    /// fractional zeros.
    /// Return
    /// - `NULL`, scale < `0` or too big.
    /// - `dec` with set scale otherwise.
    pub fn decimal_rescale(dec: *mut decNumber, scale: ::std::os::raw::c_int) -> *mut decNumber;

    /// `res` is set to the remainder of dividing `lhs` by `rhs`.
    pub fn decimal_remainder(
        res: *mut decNumber,
        lhs: *const decNumber,
        rhs: *const decNumber,
    ) -> *mut decNumber;

    /// `res` is set to the absolute value of `dec`
    /// `decimal_abs(&a, &a)` is allowed.
    pub fn decimal_abs(res: *mut decNumber, dec: *const decNumber) -> *mut decNumber;

    /// `res` is set to `-dec`.
    pub fn decimal_minus(res: *mut decNumber, dec: *const decNumber) -> *mut decNumber;

    pub fn decimal_add(
        res: *mut decNumber,
        lhs: *const decNumber,
        rhs: *const decNumber,
    ) -> *mut decNumber;

    pub fn decimal_sub(
        res: *mut decNumber,
        lhs: *const decNumber,
        rhs: *const decNumber,
    ) -> *mut decNumber;

    pub fn decimal_mul(
        res: *mut decNumber,
        lhs: *const decNumber,
        rhs: *const decNumber,
    ) -> *mut decNumber;

    pub fn decimal_div(
        res: *mut decNumber,
        lhs: *const decNumber,
        rhs: *const decNumber,
    ) -> *mut decNumber;

    pub fn decimal_log10(res: *mut decNumber, lhs: *const decNumber) -> *mut decNumber;

    pub fn decimal_ln(res: *mut decNumber, lhs: *const decNumber) -> *mut decNumber;

    pub fn decimal_pow(
        res: *mut decNumber,
        lhs: *const decNumber,
        rhs: *const decNumber,
    ) -> *mut decNumber;

    pub fn decimal_exp(res: *mut decNumber, lhs: *const decNumber) -> *mut decNumber;

    pub fn decimal_sqrt(res: *mut decNumber, lhs: *const decNumber) -> *mut decNumber;

    /// Return The length in bytes decimal packed representation will take.
    pub fn decimal_len(dec: *const decNumber) -> u32;

    /// Convert a decimal `dec` to its packed representation.
    ///
    /// Return `data + `[`decimal_len`]`(dec)`;
    pub fn decimal_pack(
        data: *mut ::std::os::raw::c_char,
        dec: *const decNumber,
    ) -> *mut ::std::os::raw::c_char;

    /// Using a packed representation of size `len` pointed to by
    /// `*data`, unpack it to `dec`.
    ///
    /// Advances `data`: `*data = *data + `[`decimal_len`]`(dec);`
    ///
    /// Return
    /// - `NULL` if value encoding is incorrect
    /// - `dec` otherwise.
    pub fn decimal_unpack(
        data: *mut *const ::std::os::raw::c_char,
        len: u32,
        dec: *mut decNumber,
    ) -> *mut decNumber;
}
