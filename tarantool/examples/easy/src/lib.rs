use std::os::raw::c_int;
use tarantool::proc;

#[proc]
fn easy() {
    println!("hello world");
}

#[proc]
fn easy2() {
    println!("hello world -- easy2");
}

#[no_mangle]
pub extern "C" fn luaopen_easy(_l: std::ffi::c_void) -> c_int {
    // Tarantool calls this function upon require("easy")
    println!("easy module loaded");
    0
}
