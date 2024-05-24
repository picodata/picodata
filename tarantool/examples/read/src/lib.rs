use serde::{Deserialize, Serialize};

use tarantool::{proc, space::Space, tuple::Encode};

#[derive(Serialize, Deserialize, Debug)]
struct Row {
    pub int_field: i32,
    pub str_field: String,
}

impl Encode for Row {}

#[proc]
fn read() {
    let space = Space::find("capi_test").unwrap();

    let key = 10000;
    let result = space.get(&(key,)).unwrap();
    assert!(result.is_some());

    let result = result.unwrap().decode::<Row>().unwrap();
    println!("value={:?}", result);
}
