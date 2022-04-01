pub const MP_UUID: std::os::raw::c_char = 2;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct tt_uuid {
    pub tl: u32,
    pub tm: u16,
    pub th: u16,
    pub csh: u8,
    pub csl: u8,
    pub n: [u8; 6],
}

extern "C" {
    /// Generate a random uuid (v4)
    pub fn tt_uuid_create(uu: *mut tt_uuid);
}
