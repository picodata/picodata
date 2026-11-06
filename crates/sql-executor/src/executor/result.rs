//! Result module.
//! Result is everything that is returned from the query execution.
//!
//! When executing DML (INSERT) we will get `ConsumerResult`, which fields are:
//! * `row_count` (u64): the number of tuples inserted (that may be equal to 0)

use core::fmt::Debug;
use serde::ser::{Serialize, SerializeMap, Serializer};
use serde::{Deserialize, Deserializer};
use smol_str::{SmolStr, ToSmolStr};
use std::collections::HashMap;
use tarantool::msgpack;
use tarantool::tlua::{self, LuaRead};
use tarantool::tuple::Encode;

use crate::errors::SbroadError;
use crate::ir::relation::{Column, ColumnRole};
use crate::ir::types::{DerivedType, NestedType, UnrestrictedType};
use crate::ir::value::Value;

pub type ExecutorTuple = Vec<Value>;

#[derive(LuaRead, Debug, PartialEq, Eq, Clone, msgpack::Encode, msgpack::Decode)]
#[encode(as_map)]
pub struct MetadataColumn {
    pub name: SmolStr,
    pub r#type: SmolStr,
}

impl std::fmt::Display for MetadataColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

impl MetadataColumn {
    /// Metadata of a column of type `ty`.
    #[must_use]
    pub fn new(name: SmolStr, ty: DerivedType) -> Self {
        MetadataColumn {
            name,
            r#type: metadata_type_name(ty),
        }
    }
}

/// Name of type `ty` in result metadata.
///
/// Type names here are part of the iproto/Lua result format that client
/// connectors consume, so `text` keeps its historical `string` spelling
/// even though SQL itself prints the type as `text`. Every writer of
/// result metadata must name column types with this function.
#[must_use]
pub fn metadata_type_name(ty: DerivedType) -> SmolStr {
    match ty.get() {
        Some(UnrestrictedType::String) => SmolStr::new_static("string"),
        Some(UnrestrictedType::Array(NestedType::Text)) => SmolStr::new_static("string[]"),
        Some(t) => match t.static_name() {
            Some(name) => SmolStr::new_static(name),
            None => t.to_smolstr(),
        },
        None => SmolStr::new_static("unknown"),
    }
}

impl Serialize for MetadataColumn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("type", &self.r#type)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for MetadataColumn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut map = HashMap::<String, SmolStr>::deserialize(deserializer)?;
        let err = |missing_key: &str| -> D::Error {
            serde::de::Error::custom(format!("expected metadata to have key: {missing_key}"))
        };
        let name = map.remove("name").ok_or_else(|| err("name"))?;
        let r#type = map.remove("type").ok_or_else(|| err("type"))?;
        Ok(MetadataColumn { name, r#type })
    }
}

impl TryInto<Column> for &MetadataColumn {
    type Error = SbroadError;

    fn try_into(self) -> Result<Column, Self::Error> {
        let col_type = UnrestrictedType::new(&self.r#type)?;
        Ok(Column::new(
            &self.name,
            DerivedType::new(col_type),
            ColumnRole::User,
            true,
        ))
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, PartialEq, Deserialize, Eq, Clone)]
pub struct ConsumerResult {
    pub row_count: u64,
}

impl Default for ConsumerResult {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsumerResult {
    /// Create an empty result set for a query consuming tuples.
    #[allow(dead_code)]
    #[must_use]
    pub fn new() -> Self {
        ConsumerResult { row_count: 0 }
    }
}

impl Serialize for ConsumerResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("row_count", &self.row_count)?;
        map.end()
    }
}

/// This impl allows to convert `ConsumerResult` into `Tuple`, using `Tuple::new` method.
impl Encode for ConsumerResult {}

#[cfg(test)]
mod tests {
    use super::metadata_type_name;
    use crate::ir::types::{DerivedType, NestedType, UnrestrictedType};

    #[test]
    fn metadata_type_keeps_string_spelling() {
        let of = metadata_type_name;

        assert_eq!(of(DerivedType::new(UnrestrictedType::String)), "string");
        assert_eq!(
            of(DerivedType::new(UnrestrictedType::Array(NestedType::Text))),
            "string[]"
        );
        assert_eq!(of(DerivedType::new(UnrestrictedType::Integer)), "int");
        assert_eq!(of(DerivedType::unknown()), "unknown");
    }
}
