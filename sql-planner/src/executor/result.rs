//! Result module.
//! Result is everything that is returned from the query execution.
//!
//! When executing DML (INSERT) we will get `ConsumerResult`, which fields are:
//! * `row_count` (u64): the number of tuples inserted (that may be equal to 0)

use core::fmt::Debug;
use serde::ser::{Serialize, SerializeMap, Serializer};
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use tarantool::msgpack;
use tarantool::tlua::{self, LuaRead};
use tarantool::tuple::Encode;

use crate::errors::SbroadError;
use crate::ir::node::relational::Relational;
use crate::ir::node::{Node, NodeId};
use crate::ir::relation::{Column, ColumnRole};
use crate::ir::tree::traversal::{PostOrderWithFilter, REL_CAPACITY};
use crate::ir::types::{DerivedType, UnrestrictedType};
use crate::ir::value::Value;
use crate::ir::Plan;

pub type ExecutorTuple = Vec<Value>;

#[derive(LuaRead, Debug, PartialEq, Eq, Clone, msgpack::Encode, msgpack::Decode)]
#[encode(as_map)]
pub struct MetadataColumn {
    pub name: String,
    pub r#type: String,
}

impl std::fmt::Display for MetadataColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

impl MetadataColumn {
    #[must_use]
    pub fn new(name: String, r#type: String) -> Self {
        MetadataColumn { name, r#type }
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
        let mut map = HashMap::<String, String>::deserialize(deserializer)?;
        let err = |missing_key: &str| -> D::Error {
            serde::de::Error::custom(format!("expected metadata to have key: {missing_key}"))
        };
        let name = map.remove("name").ok_or_else(|| err("name"))?;
        let r#type = map.remove("type").ok_or_else(|| err("type"))?;
        Ok(MetadataColumn::new(name, r#type))
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

impl Plan {
    /// Checks if the plan contains a `Values` node.
    ///
    /// # Errors
    /// - If relational iterator fails to return a correct node.
    pub fn subtree_contains_values(&self, top_id: NodeId) -> Result<bool, SbroadError> {
        let filter = |node_id: NodeId| -> bool {
            if let Ok(Node::Relational(Relational::Values(_))) = self.get_node(node_id) {
                return true;
            }
            false
        };
        let rel_tree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.rel_iter(node),
            REL_CAPACITY,
            Box::new(filter),
        );
        Ok(rel_tree.into_iter(top_id).next().is_some())
    }
}
