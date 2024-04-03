use std::{
    hash::{Hash, Hasher},
    os::raw::c_char,
};

#[repr(C)]
#[derive(Debug, Copy, Clone)]
#[allow(non_camel_case_types)]
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

pub const DECDPUN: usize = 3;
pub const DECNUMUNITS: u32 = 13;
pub const DECIMAL_MAX_DIGITS: u32 = 38;
pub const MP_DECIMAL: c_char = 1;

#[cfg(feature = "internal_test")]
mod test {
    use super::*;
    use crate::offset_of;

    #[crate::test(tarantool = "crate")]
    fn decimal_ffi_definition() {
        if !crate::ffi::has_decimal() {
            return;
        }

        let lua = crate::lua_state();
        let [
            size_of_decimal,
            offset_of_digits,
            offset_of_exponent,
            offset_of_bits,
            offset_of_lsu,
        ]: [usize; 5] = lua.eval(
            "local ffi = require 'ffi'
            return {
                ffi.sizeof('decimal_t'),
                ffi.offsetof('decimal_t', 'digits'),
                ffi.offsetof('decimal_t', 'exponent'),
                ffi.offsetof('decimal_t', 'bits'),
                ffi.offsetof('decimal_t', 'lsu'),
            }",
        ).unwrap();

        // TODO: could also check the actual types of fields using
        // `ffi.typeinfo`, but this requires more work

        assert_eq!(size_of_decimal, std::mem::size_of::<decNumber>());
        assert_eq!(offset_of_digits, offset_of!(decNumber, digits));
        assert_eq!(offset_of_exponent, offset_of!(decNumber, exponent));
        assert_eq!(offset_of_bits, offset_of!(decNumber, bits));
        assert_eq!(offset_of_lsu, offset_of!(decNumber, lsu));
    }
}
