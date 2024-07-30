//! Storage runtime of the clusterwide SQL.
//! Implements the `sbroad` crate infrastructure
//! for execution of the dispatched query plan subtrees.

use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::helpers::storage::{unprepare, StorageMetadata};
use sbroad::executor::engine::helpers::vshard::get_random_bucket;
use sbroad::executor::engine::helpers::{self, read_or_prepare, EncodedQueryInfo, QueryInfo};
use sbroad::executor::engine::{QueryCache, StorageCache, Vshard};
use sbroad::executor::ir::{ConnectionType, ExecutionPlan, QueryType};
use sbroad::executor::lru::{Cache, EvictFn, LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::protocol::{Binary, OptionalData, RequiredData, SchemaInfo};
use sbroad::ir::value::Value;
use tarantool::fiber::Mutex;
use tarantool::sql::Statement;

use crate::sql::router::{get_table_version, VersionMap};
use crate::traft::node;
use sbroad::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use sbroad::ir::tree::Snapshot;
use sbroad::ir::NodeId;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::collections::HashMap;
use std::{any::Any, cell::RefCell, rc::Rc};

use super::{router::calculate_bucket_id, DEFAULT_BUCKET_COUNT};

thread_local!(
    static STATEMENT_CACHE: Rc<Mutex<PicoStorageCache>> = Rc::new(
        Mutex::new(PicoStorageCache::new(DEFAULT_CAPACITY, Some(Box::new(evict))).unwrap())
    )
);

#[allow(clippy::module_name_repetitions)]
pub struct StorageRuntime {
    pub metadata: RefCell<StorageMetadata>,
    bucket_count: u64,
    cache: Rc<Mutex<PicoStorageCache>>,
}

type StorageCacheValue = (Statement, VersionMap, Vec<NodeId>);

fn evict(plan_id: &SmolStr, val: &mut StorageCacheValue) -> Result<(), SbroadError> {
    let (stmt, _, table_ids) = std::mem::take(val);
    unprepare(plan_id, &mut (stmt, table_ids))
}

pub struct PicoStorageCache(LRUCache<SmolStr, StorageCacheValue>);

impl PicoStorageCache {
    pub fn new(
        capacity: usize,
        evict_fn: Option<EvictFn<SmolStr, StorageCacheValue>>,
    ) -> Result<Self, SbroadError> {
        let new_fn: Option<EvictFn<SmolStr, StorageCacheValue>> = if let Some(evict_fn) = evict_fn {
            let new_fn = move |key: &SmolStr,
                               val: &mut StorageCacheValue|
                  -> Result<(), SbroadError> { evict_fn(key, val) };
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
        plan_id: SmolStr,
        stmt: Statement,
        schema_info: &SchemaInfo,
        table_ids: Vec<NodeId>,
    ) -> Result<(), SbroadError> {
        let mut version_map: HashMap<SmolStr, u64> =
            HashMap::with_capacity(schema_info.router_version_map.len());
        let node = node::global().map_err(|e| {
            SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
        })?;
        let storage_tables = &node.storage.tables;
        for table_name in schema_info.router_version_map.keys() {
            let current_version = if let Some(space_def) =
                storage_tables.by_name(table_name.as_str()).map_err(|e| {
                    SbroadError::FailedTo(Action::Get, None, format_smolstr!("space_def: {}", e))
                })? {
                space_def.schema_version
            } else {
                return Err(SbroadError::NotFound(
                    Entity::SpaceMetadata,
                    format_smolstr!("for space: {}", table_name),
                ));
            };
            version_map.insert(table_name.clone(), current_version);
        }

        self.0.put(plan_id, (stmt, version_map, table_ids))?;
        Ok(())
    }

    fn get(&mut self, plan_id: &SmolStr) -> Result<Option<(&Statement, &[NodeId])>, SbroadError> {
        let Some((ir, version_map, table_ids)) = self.0.get(plan_id)? else {
            return Ok(None);
        };
        // check Plan's tables have up to date schema
        let node = node::global().map_err(|e| {
            SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
        })?;
        let storage_tables = &node.storage.tables;
        for (table_name, cached_version) in version_map {
            let Some(space_def) = storage_tables.by_name(table_name.as_str()).map_err(|e| {
                SbroadError::FailedTo(Action::Get, None, format_smolstr!("space_def: {}", e))
            })?
            else {
                return Ok(None);
            };
            // The outdated entry will be replaced when `put` is called (that is always
            // called after the cache miss).
            if *cached_version != space_def.schema_version {
                return Ok(None);
            }
        }
        Ok(Some((ir, table_ids)))
    }

    fn clear(&mut self) -> Result<(), SbroadError> {
        self.0.clear()
    }
}

impl QueryCache for StorageRuntime {
    type Cache = PicoStorageCache;
    type Mutex = Mutex<Self::Cache>;

    fn cache(&self) -> &Self::Mutex {
        &self.cache
    }

    fn cache_capacity(&self) -> Result<usize, SbroadError> {
        Ok(self.cache().lock().capacity())
    }

    fn clear_cache(&self) -> Result<(), SbroadError> {
        *self.cache.lock() = Self::Cache::new(self.cache_capacity()?, Some(Box::new(evict)))?;
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
            Some("exec_ir_on_all is not supported on the storage".to_smolstr()),
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
            Some("exec_ir_on_some is not supported on the storage".to_smolstr()),
        ))
    }

    fn exec_ir_on_any_node(
        &self,
        mut sub_plan: ExecutionPlan,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let options = std::mem::take(&mut sub_plan.get_mut_ir_plan().options);
        let plan = sub_plan.get_ir_plan();
        let top_id = plan.get_top()?;
        let plan_id = plan.pattern_id(top_id)?;
        let sp = SyntaxPlan::new(&sub_plan, top_id, Snapshot::Oldest)?;
        let ordered = OrderedSyntaxNodes::try_from(sp)?;
        let nodes = ordered.to_syntax_data()?;
        let parameters = sub_plan.to_params(&nodes)?;
        let tables = sub_plan.encode_vtables();
        let schema_info = SchemaInfo::new(sub_plan.get_ir_plan().version_map.clone());
        let query_type = sub_plan.query_type()?;
        assert_eq!(query_type, QueryType::DQL, "Only DQL is supported");

        let mut required = RequiredData {
            plan_id,
            parameters,
            query_type,
            options,
            schema_info,
            tracing_meta: None,
            tables,
        };
        let mut optional = OptionalData {
            exec_plan: sub_plan,
            ordered,
        };
        let mut info = QueryInfo::new(&mut optional, &mut required);
        read_or_prepare(self, &mut info)
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
            // TODO: if storage version is smaller than router's version
            // wait until state catches up.
            if *version != get_table_version(table.as_str())? {
                return Err(SbroadError::OutdatedStorageSchema);
            }
        }
        match required.query_type {
            QueryType::DML => helpers::execute_dml(self, required, raw_optional),
            QueryType::DQL => {
                let mut info = EncodedQueryInfo::new(std::mem::take(raw_optional), required);
                read_or_prepare(self, &mut info)
            }
        }
    }
}
