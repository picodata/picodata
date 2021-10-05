use std::ffi::CString;

pub unsafe fn new_c_str(s: &str) -> CString {
    return CString::new(s).unwrap();
}

#[macro_export]
macro_rules! c_str {
    ($s:literal) => {
        ::std::ffi::CStr::from_bytes_with_nul_unchecked(
            ::std::concat!($s, "\0").as_bytes()
        )
    };
}

#[macro_export]
macro_rules! c_ptr {
    ($s:literal) => {
        crate::c_str!($s).as_ptr()
    };
}
