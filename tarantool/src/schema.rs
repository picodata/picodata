//! Box: schema

use crate::serde_json::{Map, Value};
use crate::tuple::AsTuple;

/// SpaceInternal is tuple, hodiing space metdata in system `_space` space.
/// For details see internal Space::insert_new_space function.
#[derive(Serialize)]
pub struct SpaceMetadata {
    pub id: u32,
    pub uid: u32,
    pub name: String,
    pub engine: String,
    pub field_count: u32,
    pub options: Map<String, Value>,
    pub format: Vec<Value>,
}

impl AsTuple for SpaceMetadata {}
