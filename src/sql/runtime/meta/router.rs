//! Metadata (table definitions) module for the clusterwide SQL.

use crate::schema::{Distribution, ShardingFn, SpaceDef};

use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::engine::helpers::storage::meta::{
    DEFAULT_JAEGER_AGENT_HOST, DEFAULT_JAEGER_AGENT_PORT,
};
use sbroad::executor::engine::helpers::{
    normalize_name_for_space_api, normalize_name_from_schema, normalize_name_from_sql,
};
use sbroad::executor::engine::Metadata;
use sbroad::executor::lru::DEFAULT_CAPACITY;
use sbroad::ir::function::Function;
use sbroad::ir::relation::{Column, ColumnRole, Table, Type};

use std::borrow::Cow;
use std::collections::HashMap;

use tarantool::space::Space;
use tarantool::util::Value;

pub const DEFAULT_BUCKET_COLUMN: &str = "bucket_id";

/// Router runtime configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::module_name_repetitions)]
pub struct RouterMetadata {
    /// Execute response waiting timeout in seconds.
    pub waiting_timeout: u64,

    /// Query cache capacity.
    pub cache_capacity: usize,

    /// Sharding column names.
    pub sharding_column: String,

    /// Jaeger agent host.
    pub jaeger_agent_host: &'static str,

    /// Jaeger agent port.
    pub jaeger_agent_port: u16,

    /// IR functions
    pub functions: HashMap<String, Function>,
}

impl Default for RouterMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl RouterMetadata {
    #[must_use]
    pub fn new() -> Self {
        RouterMetadata {
            waiting_timeout: 360,
            cache_capacity: DEFAULT_CAPACITY,
            jaeger_agent_host: DEFAULT_JAEGER_AGENT_HOST,
            jaeger_agent_port: DEFAULT_JAEGER_AGENT_PORT,
            sharding_column: DEFAULT_BUCKET_COLUMN.to_string(),
            functions: HashMap::new(),
        }
    }
}

impl Metadata for RouterMetadata {
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    fn table(&self, table_name: &str) -> Result<Table, SbroadError> {
        let name = normalize_name_for_space_api(table_name);

        // // Get the space columns and engine of the space.
        let space = Space::find(&name)
            .ok_or_else(|| SbroadError::NotFound(Entity::Space, name.to_string()))?;
        let meta = space.meta().map_err(|e| {
            SbroadError::FailedTo(Action::Get, Some(Entity::SpaceMetadata), e.to_string())
        })?;
        let engine = meta.engine;
        let mut columns: Vec<Column> = Vec::with_capacity(meta.format.len());
        for column_meta in &meta.format {
            let name_value = column_meta.get(&Cow::from("name")).ok_or_else(|| {
                SbroadError::FailedTo(
                    Action::Get,
                    Some(Entity::SpaceMetadata),
                    format!("column name not found in the space format: {column_meta:?}"),
                )
            })?;
            let col_name = if let Value::Str(name) = name_value {
                name
            } else {
                return Err(SbroadError::FailedTo(
                    Action::Get,
                    Some(Entity::SpaceMetadata),
                    format!("column name is not a string: {name_value:?}"),
                ));
            };
            let type_value = column_meta.get(&Cow::from("type")).ok_or_else(|| {
                SbroadError::FailedTo(
                    Action::Get,
                    Some(Entity::SpaceMetadata),
                    format!("column type not found in the space format: {column_meta:?}"),
                )
            })?;
            let col_type: Type = if let Value::Str(col_type) = type_value {
                Type::new(col_type)?
            } else {
                return Err(SbroadError::FailedTo(
                    Action::Get,
                    Some(Entity::SpaceMetadata),
                    format!("column type is not a string: {type_value:?}"),
                ));
            };
            let role = if col_name == DEFAULT_BUCKET_COLUMN {
                ColumnRole::Sharding
            } else {
                ColumnRole::User
            };
            let column = Column {
                name: normalize_name_from_schema(col_name),
                r#type: col_type,
                role,
            };
            columns.push(column);
        }

        // Try to find the sharding columns of the space in "_pico_space".
        // If nothing found then the space is local and we can't query it with
        // distributed SQL.
        let pico_space = Space::find("_pico_space")
            .ok_or_else(|| SbroadError::NotFound(Entity::Space, "_pico_space".to_string()))?;
        let tuple = pico_space.get(&[meta.id]).map_err(|e| {
            SbroadError::FailedTo(
                Action::Get,
                Some(Entity::ShardingKey),
                format!("space id {}: {e}", meta.id),
            )
        })?;
        let tuple =
            tuple.ok_or_else(|| SbroadError::NotFound(Entity::ShardingKey, name.to_string()))?;
        let space_def: SpaceDef = tuple.decode().map_err(|e| {
            SbroadError::FailedTo(
                Action::Deserialize,
                Some(Entity::SpaceMetadata),
                format!("serde error: {e}"),
            )
        })?;
        let keys: Vec<_> = match &space_def.distribution {
            Distribution::Global => {
                return Err(SbroadError::Invalid(
                    Entity::Distribution,
                    Some("global distribution is not supported".into()),
                ));
            }
            Distribution::ShardedImplicitly {
                sharding_key,
                sharding_fn,
            } => {
                if !matches!(sharding_fn, ShardingFn::Murmur3) {
                    return Err(SbroadError::NotImplemented(
                        Entity::Distribution,
                        format!("by hash function {sharding_fn}"),
                    ));
                }
                sharding_key
                    .iter()
                    .map(|field| normalize_name_from_schema(field))
                    .collect()
            }
            Distribution::ShardedByField { field } => {
                return Err(SbroadError::NotImplemented(
                    Entity::Distribution,
                    format!("explicitly by field '{field}'"),
                ));
            }
        };
        let sharding_keys: &[&str] = &keys.iter().map(String::as_str).collect::<Vec<_>>();
        Table::new_seg(
            &normalize_name_from_sql(table_name),
            columns,
            sharding_keys,
            engine.into(),
        )
    }

    fn function(&self, fn_name: &str) -> Result<&Function, SbroadError> {
        let name = normalize_name_from_sql(fn_name);
        match self.functions.get(&name) {
            Some(v) => Ok(v),
            None => Err(SbroadError::NotFound(Entity::SQLFunction, name)),
        }
    }

    /// Get response waiting timeout for executor
    fn waiting_timeout(&self) -> u64 {
        self.waiting_timeout
    }

    fn sharding_column(&self) -> &str {
        self.sharding_column.as_str()
    }

    /// Get sharding key's column names by a space name
    fn sharding_key_by_space(&self, space: &str) -> Result<Vec<String>, SbroadError> {
        let table = self.table(space)?;
        table.get_sharding_column_names()
    }

    fn sharding_positions_by_space(&self, space: &str) -> Result<Vec<usize>, SbroadError> {
        let table = self.table(space)?;
        Ok(table.get_sharding_positions().to_vec())
    }
}
