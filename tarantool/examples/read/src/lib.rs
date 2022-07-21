use std::os::raw::c_int;

use serde::{Deserialize, Serialize};

use tarantool::space::Space;
use tarantool::tuple::{Encode, FunctionArgs, FunctionCtx};

#[derive(Serialize, Deserialize, Debug)]
struct Row {
    pub int_field: i32,
    pub str_field: String,
}

impl Encode for Row {}

#[no_mangle]
pub extern "C" fn read(_: FunctionCtx, _: FunctionArgs) -> c_int {
    let space = Space::find("capi_test").unwrap();

    let key = 10000;
    let result = space.get(&(key,)).unwrap();
    assert!(result.is_some());

    let result = result.unwrap().into_struct::<Row>().unwrap();
    println!("value={:?}", result);

    0
}
