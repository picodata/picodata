//! Storage runtime of the clusterwide SQL.
//! Implements the `sbroad` crate infrastructure
//! for execution of the dispatched query plan subtrees.

use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::helpers::storage::meta::StorageMetadata;
use sbroad::executor::engine::helpers::storage::runtime::unprepare;
use sbroad::executor::engine::helpers::storage::PreparedStmt;
use sbroad::executor::engine::helpers::vshard::get_random_bucket;
use sbroad::executor::engine::helpers::{self, normalize_name_for_space_api};
use sbroad::executor::engine::{QueryCache, StorageCache, Vshard};
use sbroad::executor::ir::{ConnectionType, ExecutionPlan, QueryType};
use sbroad::executor::lru::{Cache, EvictFn, LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::protocol::{Binary, RequiredData, SchemaInfo};
use sbroad::ir::value::Value;

use crate::sql::router::{get_table_version, VersionMap};
use crate::traft::node;
use std::collections::HashMap;
use std::{any::Any, cell::RefCell, rc::Rc};

use super::{router::calculate_bucket_id, DEFAULT_BUCKET_COUNT};

thread_local!(
    static STATEMENT_CACHE: Rc<RefCell<PicoStorageCache>> = Rc::new(
        RefCell::new(PicoStorageCache::new(DEFAULT_CAPACITY, Some(Box::new(unprepare))).unwrap())
    )
);

#[allow(clippy::module_name_repetitions)]
pub struct StorageRuntime {
    pub metadata: RefCell<StorageMetadata>,
    bucket_count: u64,
    cache: Rc<RefCell<PicoStorageCache>>,
}

pub struct PicoStorageCache(LRUCache<String, (PreparedStmt, VersionMap)>);

impl PicoStorageCache {
    pub fn new(
        capacity: usize,
        evict_fn: Option<EvictFn<PreparedStmt>>,
    ) -> Result<Self, SbroadError> {
        let new_fn: Option<EvictFn<(PreparedStmt, VersionMap)>> = if let Some(evict_fn) = evict_fn {
            let new_fn = move |val: &mut (PreparedStmt, VersionMap)| -> Result<(), SbroadError> {
                evict_fn(&mut val.0)
            };
            Some(Box::new(new_fn))
        } else {
            None
        };
        Ok(PicoStorageCache(LRUCache::new(capacity, new_fn)?))
    }

    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }
}

impl StorageCache for PicoStorageCache {
    fn put(
        &mut self,
        plan_id: String,
        stmt: PreparedStmt,
        schema_info: &SchemaInfo,
    ) -> Result<(), SbroadError> {
        let mut version_map: HashMap<String, u64> =
            HashMap::with_capacity(schema_info.router_version_map.len());
        let node = node::global()
            .map_err(|e| SbroadError::FailedTo(Action::Get, None, format!("raft node: {}", e)))?;
        let storage_spaces = &node.storage.spaces;
        for table_name in schema_info.router_version_map.keys() {
            let space_name = normalize_name_for_space_api(table_name);
            let current_version = if let Some(space_def) =
                storage_spaces.by_name(space_name.as_str()).map_err(|e| {
                    SbroadError::FailedTo(Action::Get, None, format!("space_def: {}", e))
                })? {
                space_def.schema_version
            } else {
                return Err(SbroadError::NotFound(
                    Entity::SpaceMetadata,
                    format!("for space: {}", space_name),
                ));
            };
            version_map.insert(space_name, current_version);
        }

        self.0.put(plan_id, (stmt, version_map))
    }

    fn get(&mut self, plan_id: &String) -> Result<Option<&PreparedStmt>, SbroadError> {
        let Some((ir, version_map)) = self.0.get(plan_id)? else {
            return Ok(None)
        };
        // check Plan's tables have up to date schema
        let node = node::global()
            .map_err(|e| SbroadError::FailedTo(Action::Get, None, format!("raft node: {}", e)))?;
        let storage_spaces = &node.storage.spaces;
        for (table_name, cached_version) in version_map {
            let space_name = normalize_name_for_space_api(table_name);
            let Some(space_def) = storage_spaces.by_name(space_name.as_str()).map_err(|e|
                SbroadError::FailedTo(Action::Get, None, format!("space_def: {}", e))
            )? else {
                return Ok(None)
            };
            // The outdated entry will be replaced when
            // `put` is called (which is always called
            // after cache miss).
            if *cached_version != space_def.schema_version {
                return Ok(None);
            }
        }
        Ok(Some(ir))
    }

    fn clear(&mut self) -> Result<(), SbroadError> {
        self.0.clear()
    }
}

impl QueryCache for StorageRuntime {
    type Cache = PicoStorageCache;

    fn cache(&self) -> &RefCell<Self::Cache> {
        &self.cache
    }

    fn cache_capacity(&self) -> Result<usize, SbroadError> {
        Ok(self
            .cache()
            .try_borrow()
            .map_err(|e| {
                SbroadError::FailedTo(Action::Borrow, Some(Entity::Cache), format!("{e:?}"))
            })?
            .capacity())
    }

    fn clear_cache(&self) -> Result<(), SbroadError> {
        *self.cache.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Clear, Some(Entity::Cache), format!("{e:?}"))
        })? = Self::Cache::new(DEFAULT_CAPACITY, None)?;
        Ok(())
    }

    fn provides_versions(&self) -> bool {
        true
    }

    fn get_table_version(&self, space_name: &str) -> Result<u64, SbroadError> {
        get_table_version(space_name)
    }
}

impl Vshard for StorageRuntime {
    fn exec_ir_on_all(
        &self,
        _required: Binary,
        _optional: Binary,
        _query_type: QueryType,
        _conn_type: ConnectionType,
        _vtable_max_rows: u64,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_on_all is not supported on the storage".to_string()),
        ))
    }

    fn bucket_count(&self) -> u64 {
        self.bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError> {
        calculate_bucket_id(s, self.bucket_count())
    }

    fn exec_ir_on_some(
        &self,
        _sub_plan: ExecutionPlan,
        _buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_on_some is not supported on the storage".to_string()),
        ))
    }
}

impl StorageRuntime {
    /// Build a new storage runtime.
    ///
    /// # Errors
    /// - Failed to initialize the LRU cache.
    pub fn new() -> Result<Self, SbroadError> {
        let runtime = STATEMENT_CACHE.with(|cache| StorageRuntime {
            metadata: RefCell::new(StorageMetadata::new()),
            bucket_count: DEFAULT_BUCKET_COUNT,
            cache: cache.clone(),
        });
        Ok(runtime)
    }

    /// Execute dispatched plan (divided into required and optional parts).
    ///
    /// # Errors
    /// - Something went wrong while executing the plan.
    #[allow(unused_variables)]
    pub fn execute_plan(
        &self,
        required: &mut RequiredData,
        raw_optional: &mut Vec<u8>,
    ) -> Result<Box<dyn Any>, SbroadError> {
        // Check router schema version hasn't changed.
        for (table, version) in &required.schema_info.router_version_map {
            let normalized = normalize_name_for_space_api(table);
            // TODO: if storage version is smaller than router's version
            // wait until state catches up.
            if *version != get_table_version(normalized.as_str())? {
                return Err(SbroadError::OutdatedStorageSchema);
            }
        }
        match required.query_type {
            QueryType::DML => helpers::execute_dml(self, required, raw_optional),
            QueryType::DQL => {
                if required.can_be_cached {
                    helpers::execute_cacheable_dql_with_raw_optional(self, required, raw_optional)
                } else {
                    helpers::execute_non_cacheable_dql_with_raw_optional(
                        raw_optional,
                        required.options.vtable_max_rows,
                        std::mem::take(&mut required.options.execute_options),
                    )
                }
            }
        }
    }
}
