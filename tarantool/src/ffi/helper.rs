use std::ffi::CString;

pub unsafe fn new_c_str(s: &str) -> CString {
    return CString::new(s).unwrap();
}
