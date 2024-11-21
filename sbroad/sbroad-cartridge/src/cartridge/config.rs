//! Cartridge configuration cache module.

extern crate yaml_rust;

use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::collections::HashMap;
use yaml_rust::{Yaml, YamlLoader};

use sbroad::errors::{Entity, SbroadError};
use sbroad::executor::engine::helpers::normalize_name_from_sql;
use sbroad::executor::engine::{get_builtin_functions, Metadata};
use sbroad::executor::lru::DEFAULT_CAPACITY;
use sbroad::ir::function::Function;
use sbroad::ir::relation::{Column, ColumnRole, SpaceEngine, Table, Type};
use sbroad::{debug, warn};

pub const DEFAULT_WAITING_TIMEOUT: u64 = 360;

/// Cluster metadata information
///
/// Information based on tarantool cartridge schema. Cache knows nothing about bucket distribution in the cluster,
/// as it is managed by Tarantool's vshard module.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouterConfiguration {
    /// Execute response waiting timeout in seconds.
    waiting_timeout: u64,

    /// Query cache capacity.
    cache_capacity: usize,

    /// Sharding column names.
    sharding_column: SmolStr,

    /// IR table segments from the cluster spaces
    tables: HashMap<SmolStr, Table>,

    /// IR functions
    functions: HashMap<SmolStr, Function>,
}

impl Default for RouterConfiguration {
    fn default() -> Self {
        Self::new()
    }
}

impl RouterConfiguration {
    #[must_use]
    pub fn new() -> Self {
        let builtins = get_builtin_functions();
        let mut functions = HashMap::with_capacity(builtins.len());
        for f in builtins {
            functions.insert(f.name.clone(), f.clone());
        }

        RouterConfiguration {
            waiting_timeout: DEFAULT_WAITING_TIMEOUT,
            cache_capacity: DEFAULT_CAPACITY,
            tables: HashMap::new(),
            sharding_column: SmolStr::default(),
            functions,
        }
    }

    /// Parse and load yaml cartridge schema to cache
    ///
    /// # Errors
    /// Returns `SbroadError` when process was terminated.
    pub fn load_schema(&mut self, s: &str) -> Result<(), SbroadError> {
        if let Ok(docs) = YamlLoader::load_from_str(s) {
            if let Some(schema) = docs.first() {
                self.init_table_segments(schema)?;
                return Ok(());
            }
        }

        Err(SbroadError::Invalid(Entity::ClusterSchema, None))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Transform space information from schema to table segments
    ///
    /// # Errors
    /// Returns `SbroadError` when schema contains errors.
    #[allow(clippy::too_many_lines)]
    fn init_table_segments(&mut self, schema: &Yaml) -> Result<(), SbroadError> {
        self.tables.clear();
        let Some(spaces) = schema["spaces"].as_hash() else {
            return Err(SbroadError::Invalid(
                Entity::ClusterSchema,
                Some("schema.spaces is invalid".into()),
            ));
        };

        for (space_name, params) in spaces {
            if let Some(current_space_name) = space_name.as_str() {
                let fields = if let Some(fields) = params["format"].as_vec() {
                    let mut result = Vec::new();
                    for val in fields {
                        let name: &str = match val["name"].as_str() {
                            Some(s) => s,
                            None => {
                                return Err(SbroadError::Invalid(
                                    Entity::ClusterSchema,
                                    Some(format_smolstr!(
                                        "column name of table {current_space_name} is invalid"
                                    )),
                                ))
                            }
                        };
                        let t = match val["type"].as_str() {
                            Some(t) => Type::new(t)?,
                            None => {
                                return Err(SbroadError::Invalid(
                                    Entity::ClusterSchema,
                                    Some(format_smolstr!(
                                        "Type not found for columns {name} of table {current_space_name}"
                                    )),
                                ))
                            }
                        };
                        let is_nullable: bool = match val["is_nullable"].as_bool() {
                            Some(b) => b,
                            None => {
                                return Err(SbroadError::Invalid(
                                    Entity::ClusterSchema,
                                    Some(format_smolstr!(
                                    "column is_nullable of table {current_space_name} is invalid"
                                )),
                                ))
                            }
                        };
                        let role = if self.sharding_column().eq(name) {
                            ColumnRole::Sharding
                        } else {
                            ColumnRole::User
                        };
                        let col = Column::new(name, t, role, is_nullable);
                        result.push(col);
                    }
                    result
                } else {
                    warn!(
                        Option::from("configuration parsing"),
                        &format!("Skip space {current_space_name}: fields not found."),
                    );
                    continue;
                };

                let shard_key: Vec<SmolStr> = if let Some(shard_key) =
                    params["sharding_key"].as_vec()
                {
                    let mut result = Vec::new();
                    for k in shard_key {
                        let key: &str = if let Some(k) = k.as_str() {
                            k
                        } else {
                            warn!(
                                Option::from("configuration parsing"),
                                &format!(
                                    "Skip space {current_space_name}: failed to convert key {k:?} to string."
                                ),
                            );
                            continue;
                        };
                        result.push(key.to_smolstr());
                    }
                    result
                } else {
                    warn!(
                        Option::from("configuration parsing"),
                        &format!("Skip space {current_space_name}: keys not found."),
                    );
                    continue;
                };

                let primary_key = if let Some(indexes) = params["indexes"].as_vec() {
                    let pk = indexes.first().ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::PrimaryKey,
                            format_smolstr!("for space {current_space_name}"),
                        )
                    })?;
                    let pk_parts = pk["parts"].as_vec().ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::PrimaryKey,
                            Some(format_smolstr!(
                                "for space {current_space_name}: failed to get index parts"
                            )),
                        )
                    })?;

                    pk_parts.iter().map(|p| {
                        let name = p["path"].as_str().ok_or_else(|| SbroadError::Invalid(
                           Entity::PrimaryKey,
                           Some(format_smolstr!("for space {current_space_name}: failed to get index part field")))
                        )?;
                        Ok(name.to_smolstr())
                    }).collect::<Result<Vec<SmolStr>, SbroadError>>()?
                } else {
                    warn!(
                        Option::from("configuration parsing"),
                        &format!("Skip space {current_space_name}: primary key not found."),
                    );
                    continue;
                };

                let engine: SpaceEngine = if let Some(engine) = params["engine"].as_str() {
                    if let Ok(v) = SpaceEngine::try_from(engine) {
                        v
                    } else {
                        warn!(
                            Option::from("configuration parsing"),
                            &format!("Skip space {current_space_name}: unknown engine {engine}."),
                        );
                        continue;
                    }
                } else {
                    warn!(
                        Option::from("configuration parsing"),
                        &format!("Skip space {current_space_name}: engine not found."),
                    );
                    continue;
                };

                let table_name: SmolStr = current_space_name.to_smolstr();
                debug!(
                    Option::from("configuration parsing"),
                    &format!(
                        "Table's original name: {current_space_name}, qualified name {table_name}"
                    ),
                );
                let shard_key_str = shard_key.iter().map(SmolStr::as_str).collect::<Vec<&str>>();
                let primary_key_str = primary_key
                    .iter()
                    .map(SmolStr::as_str)
                    .collect::<Vec<&str>>();
                let t = Table::new_sharded(
                    &table_name,
                    fields,
                    shard_key_str.as_slice(),
                    primary_key_str.as_slice(),
                    engine,
                )?;
                self.tables.insert(table_name, t);
            } else {
                return Err(SbroadError::Invalid(
                    Entity::ClusterSchema,
                    Some("space name is invalid".into()),
                ));
            }
        }

        Ok(())
    }

    /// Setup response waiting timeout for executor
    pub fn set_waiting_timeout(&mut self, timeout: u64) {
        if timeout > 0 {
            self.waiting_timeout = timeout;
        }
    }

    pub fn set_cache_capacity(&mut self, capacity: usize) {
        if capacity > 0 {
            self.cache_capacity = capacity;
        } else {
            self.cache_capacity = DEFAULT_CAPACITY;
        }
    }

    pub fn set_sharding_column(&mut self, column: SmolStr) {
        self.sharding_column = column;
    }
}

impl Metadata for RouterConfiguration {
    /// Get table segment form cache by table name
    ///
    /// # Errors
    /// Returns `SbroadError` when table was not found.
    #[allow(dead_code)]
    fn table(&self, table_name: &str) -> Result<Table, SbroadError> {
        let name = table_name.to_smolstr();
        match self.tables.get(&name) {
            Some(v) => Ok(v.clone()),
            None => Err(SbroadError::NotFound(Entity::Space, name)),
        }
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
    fn sharding_key_by_space(&self, space: &str) -> Result<Vec<SmolStr>, SbroadError> {
        let table = self.table(space)?;
        table.get_sharding_column_names()
    }

    fn sharding_positions_by_space(&self, space: &str) -> Result<Vec<usize>, SbroadError> {
        let table = self.table(space)?;
        Ok(table.get_sk()?.to_vec())
    }
}

pub struct StorageConfiguration {
    /// Prepared statements cache capacity (on the storage).
    pub storage_capacity: usize,
    /// Prepared statements cache size in bytes (on the storage).
    /// If a new statement is bigger doesn't fit into the cache,
    /// it would not be cached but executed directly.
    pub storage_size_bytes: usize,
}

impl Default for StorageConfiguration {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageConfiguration {
    #[must_use]
    pub fn new() -> Self {
        StorageConfiguration {
            storage_capacity: 0,
            storage_size_bytes: 0,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.storage_capacity == 0 && self.storage_size_bytes == 0
    }
}

#[cfg(test)]
mod tests;
