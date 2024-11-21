//! Result module.
//! Result is everything that is returned from the query execution.
//!
//! When executing DQL (SELECT) we will get `ProducerResult`, which fields are:
//! * `metadata` (Vec of `MetadataColumn`): information about
//!   names and types of gotten columns (even if the number of returned columns is 0)
//! * `rows` (Vec of `ExecutorTuple` (Vec of `LuaValue`)): resulting tuples of values
//!
//! When executing DML (INSERT) we will get `ConsumerResult`, which fields are:
//! * `row_count` (u64): the number of tuples inserted (that may be equal to 0)

use core::fmt::Debug;
use serde::ser::{Serialize, SerializeMap, Serializer};
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use tarantool::tlua::{self, LuaRead};
use tarantool::tuple::Encode;

use crate::debug;
use crate::errors::SbroadError;
use crate::executor::vtable::{VTableColumn, VTableTuple, VirtualTable};
use crate::ir::node::relational::Relational;
use crate::ir::node::{Node, NodeId};
use crate::ir::relation::{Column, ColumnRole, Type};
use crate::ir::tree::traversal::{PostOrderWithFilter, REL_CAPACITY};
use crate::ir::value::{LuaValue, Value};
use crate::ir::Plan;

pub type ExecutorTuple = Vec<LuaValue>;

#[derive(LuaRead, Debug, PartialEq, Eq, Clone)]
pub struct MetadataColumn {
    name: String,
    r#type: String,
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
        let col_type = Type::new(&self.r#type)?;
        Ok(Column::new(&self.name, col_type, ColumnRole::User, true))
    }
}

/// Results of query execution for `SELECT`.
/// Infromation returned to as from local Tarantool query execution.
#[allow(clippy::module_name_repetitions)]
#[derive(LuaRead, Debug, Deserialize, PartialEq, Clone)]
pub struct ProducerResult {
    pub metadata: Vec<MetadataColumn>,
    pub rows: Vec<ExecutorTuple>,
}

#[allow(clippy::module_name_repetitions)]
#[derive(LuaRead, Debug, Deserialize, PartialEq, Clone)]
pub struct DQLQueryResult {
    pub metadata: Vec<MetadataColumn>,
    pub rows: Vec<ExecutorTuple>,
}

impl From<ProducerResult> for DQLQueryResult {
    fn from(value: ProducerResult) -> Self {
        Self {
            metadata: value.metadata,
            rows: value.rows,
        }
    }
}

impl Serialize for DQLQueryResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("metadata", &self.metadata)?;
        map.serialize_entry("rows", &self.rows)?;
        map.end()
    }
}

/// This impl allows to convert `DQLQueryResult` into `Tuple`, using `Tuple::new` method.
impl Encode for DQLQueryResult {}

impl Default for ProducerResult {
    fn default() -> Self {
        Self::new()
    }
}

impl ProducerResult {
    /// Create an empty result set for a query producing tuples.
    #[allow(dead_code)]
    #[must_use]
    pub fn new() -> Self {
        ProducerResult {
            metadata: Vec::new(),
            rows: Vec::new(),
            // cache_miss: None,
        }
    }

    /// Converts result to virtual table for linker.
    ///
    /// # Errors
    /// - convert to virtual table error
    pub fn as_virtual_table(&mut self) -> Result<VirtualTable, SbroadError> {
        let mut vtable = VirtualTable::new();

        // Decode metadata
        for col in &self.metadata {
            let column: Column = col.try_into()?;
            vtable.add_column(VTableColumn {
                r#type: column.r#type,
                role: column.role,
                is_nullable: column.is_nullable,
            });
        }
        debug!(
            Option::from("as_virtual_table"),
            &format!("virtual table columns: {:?}", vtable.get_columns())
        );

        // Decode data
        let mut data: Vec<VTableTuple> = Vec::with_capacity(self.rows.len());
        let columns = vtable.get_columns();
        for mut encoded_tuple in self.rows.drain(..) {
            let mut tuple = Vec::with_capacity(encoded_tuple.len());
            for (i, value) in encoded_tuple.drain(..).enumerate() {
                let column = &columns[i];
                let value = Value::from(value);
                if value.get_type() == column.r#type {
                    tuple.push(value);
                } else {
                    tuple.push(Value::from(value.cast(&column.r#type)?));
                }
            }
            data.push(tuple);
        }
        std::mem::swap(vtable.get_mut_tuples(), &mut data);

        Ok(vtable)
    }
}

impl Serialize for ProducerResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("metadata", &self.metadata)?;
        map.serialize_entry("rows", &self.rows)?;
        // map.serialize_entry("cache_miss", &self.cache_miss)?;
        map.end()
    }
}

/// This impl allows to convert `ProducerResult` into `Tuple`, using `Tuple::new` method.
impl Encode for ProducerResult {}

/// Results of query execution for `INSERT`.
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
        let mut rel_tree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.rel_iter(node),
            REL_CAPACITY,
            Box::new(filter),
        );
        Ok(rel_tree.iter(top_id).next().is_some())
    }
}

#[cfg(test)]
mod tests;
