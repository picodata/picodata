use std::os::raw::c_int;

use serde::{Deserialize, Serialize};

use tarantool::tuple::{AsTuple, FunctionArgs, FunctionCtx, Tuple};

#[derive(Serialize, Deserialize)]
struct Args {
    pub fields: Vec<i32>,
}

impl AsTuple for Args {}

#[no_mangle]
pub extern "C" fn harder(_: FunctionCtx, args: FunctionArgs) -> c_int {
    let args: Tuple = args.into();
    let args = args.into_struct::<Args>().unwrap();
    println!("field_count = {}", args.fields.len());

    for val in args.fields {
        println!("val={}", val);
    }

    0
}
