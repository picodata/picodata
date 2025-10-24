pub const MP_DATETIME: i8 = 4;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
#[allow(non_camel_case_types)]
pub struct datetime {
    pub epoch: f64,
    pub nsec: i32,
    pub tzoffset: i16,
    pub tzindex: i16,
}

crate::define_dlsym_reloc! {
    /// Returns the number of bytes required to store a msgpack encoding for `date`.
    pub fn tnt_mp_sizeof_datetime(date: *const datetime) -> u32;

    /// Encodes `date` into msgpack, writes the result into the buffer pointed
    /// to by `data`, which must have at least `tnt_mp_sizeof_datetime(date)`
    /// bytes allocated in it.
    ///
    /// Returns a pointer to the first byte after the encoded data.
    pub fn tnt_mp_encode_datetime(data: *mut u8, date: *const datetime) -> *mut u8;
}

#[cfg(feature = "internal_test")]
mod test {
    use super::*;
    use crate::offset_of;

    #[crate::test(tarantool = "crate")]
    fn datetime_ffi_definition() {
        if !crate::ffi::has_datetime() {
            return;
        }

        let lua = crate::lua_state();
        let [
            size_of_datetime,
            offset_of_epoch,
            offset_of_nsec,
            offset_of_tzoffset,
            offset_of_tzindex,
        ]: [usize; 5] = lua.eval(
            "local ffi = require 'ffi'
            return {
                ffi.sizeof('struct datetime'),
                ffi.offsetof('struct datetime', 'epoch'),
                ffi.offsetof('struct datetime', 'nsec'),
                ffi.offsetof('struct datetime', 'tzoffset'),
                ffi.offsetof('struct datetime', 'tzindex'),
            }",
        ).unwrap();

        // TODO: could also check the actual types of fields using
        // `ffi.typeinfo`, but this requires more work

        assert_eq!(size_of_datetime, std::mem::size_of::<datetime>());
        assert_eq!(offset_of_epoch, offset_of!(datetime, epoch));
        assert_eq!(offset_of_nsec, offset_of!(datetime, nsec));
        assert_eq!(offset_of_tzoffset, offset_of!(datetime, tzoffset));
        assert_eq!(offset_of_tzindex, offset_of!(datetime, tzindex));
    }
}
