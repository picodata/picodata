use serde::{Deserialize, Serialize};
use tarantool_module::AsTuple;

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S1Record {
    pub id: u32,
    pub text: String,
}

impl AsTuple for S1Record {}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S2Record {
    pub id: u32,
    pub key: String,
    pub value: String,
    pub a: i32,
    pub b: i32,
}

impl AsTuple for S2Record {}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S2Key {
    pub id: u32,
    pub a: i32,
    pub b: i32,
}

impl AsTuple for S2Key {}

