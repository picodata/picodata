pub const MP_UUID: i8 = 2;

#[repr(C)]
#[derive(Copy, Clone)]
#[allow(non_camel_case_types)]
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

#[cfg(feature = "internal_test")]
mod test {
    use super::*;
    use crate::offset_of;

    #[crate::test(tarantool = "crate")]
    fn uuid_ffi_definition() {
        let lua = crate::lua_state();
        let [
            size_of_uuid,
            offset_of_time_low,
            offset_of_time_mid,
            offset_of_time_hi_and_version,
            offset_of_clock_seq_hi_and_reserved,
            offset_of_clock_seq_low,
            offset_of_node,
        ]: [usize; 7] = lua.eval(
            "local ffi = require 'ffi'
            return {
                ffi.sizeof('struct tt_uuid'),
                ffi.offsetof('struct tt_uuid', 'time_low'),
                ffi.offsetof('struct tt_uuid', 'time_mid'),
                ffi.offsetof('struct tt_uuid', 'time_hi_and_version'),
                ffi.offsetof('struct tt_uuid', 'clock_seq_hi_and_reserved'),
                ffi.offsetof('struct tt_uuid', 'clock_seq_low'),
                ffi.offsetof('struct tt_uuid', 'node'),
            }",
        ).unwrap();

        // TODO: could also check the actual types of fields using
        // `ffi.typeinfo`, but this requires more work

        assert_eq!(size_of_uuid, std::mem::size_of::<tt_uuid>());
        assert_eq!(offset_of_time_low, offset_of!(tt_uuid, tl));
        assert_eq!(offset_of_time_mid, offset_of!(tt_uuid, tm));
        assert_eq!(offset_of_time_hi_and_version, offset_of!(tt_uuid, th));
        assert_eq!(
            offset_of_clock_seq_hi_and_reserved,
            offset_of!(tt_uuid, csh)
        );
        assert_eq!(offset_of_clock_seq_low, offset_of!(tt_uuid, csl));
        assert_eq!(offset_of_node, offset_of!(tt_uuid, n));
    }
}
