use std::{
    hash::{Hash, Hasher},
    os::raw::c_char,
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

pub const DECDPUN: usize = 3;
pub const DECNUMUNITS: u32 = 13;
pub const DECIMAL_MAX_DIGITS: u32 = 38;
pub const MP_DECIMAL: c_char = 1;
