use std::os::raw::c_int;
use tarantool_module::tuple::{FunctionArgs, FunctionCtx};

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
