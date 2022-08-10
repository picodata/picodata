use serde::{Deserialize, Serialize};

use tarantool::{
    proc,
    space::Space,
    tuple::{Encode, Tuple},
};

#[derive(Serialize, Deserialize)]
struct Row {
    pub int_field: i32,
    pub str_field: String,
}

impl Encode for Row {}

#[proc]
fn hardest() -> Tuple {
    let mut space = Space::find("capi_test").unwrap();
    let result = space.insert(&Row {
        int_field: 10000,
        str_field: "String 2".to_string(),
    });
    result.unwrap()
}
