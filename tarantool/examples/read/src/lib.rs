use std::os::raw::c_int;

use serde::{Deserialize, Serialize};

use tarantool_module::space::Space;
use tarantool_module::tuple::{AsTuple, FunctionArgs, FunctionCtx};

#[derive(Serialize, Deserialize, Debug)]
struct Row {
    pub int_field: i32,
    pub str_field: String,
}

impl AsTuple for Row {}

#[no_mangle]
pub extern "C" fn read(_: FunctionCtx, _: FunctionArgs) -> c_int {
    let space = Space::find_by_name("capi_test").unwrap().unwrap();
    let index = space.primary_key();

    let key = 10000;
    let result = index.get(&(key,)).unwrap();
    assert!(result.is_some());

    let result = result.unwrap().into_struct::<Row>().unwrap();
    println!("value={:?}", result);

    0
}
