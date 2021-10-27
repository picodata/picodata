use std::os::raw::c_int;
use tarantool::tuple::{FunctionArgs, FunctionCtx};

#[no_mangle]
pub extern "C" fn easy(_: FunctionCtx, _: FunctionArgs) -> c_int {
    println!("hello world");
    0
}

#[no_mangle]
pub extern "C" fn easy2(_: FunctionCtx, _: FunctionArgs) -> c_int {
    println!("hello world -- easy2");
    0
}

pub extern "C" fn luaopen_easy(_l: std::ffi::c_void) -> c_int {
    // Tarantool calls this function upon require("easy")
    println!("easy module loaded");
    0
}
