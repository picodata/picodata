use std::os::raw::c_int;

use serde::{Deserialize, Serialize};

use tarantool_module::space::Space;
use tarantool_module::tuple::{AsTuple, FunctionArgs, FunctionCtx};

#[derive(Serialize, Deserialize)]
struct Row {
    pub int_field: i32,
    pub str_field: String,
}

impl AsTuple for Row {}

#[no_mangle]
pub extern "C" fn hardest(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    let mut space = Space::find_by_name("capi_test").unwrap().unwrap();
    let result = space.insert(
        &Row {
            int_field: 10000,
            str_field: "String 2".to_string(),
        },
        true,
    );
    ctx.return_tuple(result.unwrap().unwrap()).unwrap()
}
