use std::os::raw::c_int;

#[no_mangle]
pub extern "C" fn luaopen_picodata(_l: std::ffi::c_void) -> c_int {
    println!("Hello from rust lib");
    0
}
