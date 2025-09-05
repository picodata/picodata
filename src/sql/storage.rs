//! Storage runtime of the clusterwide SQL.
//! Implements the `sbroad` crate infrastructure
//! for execution of the dispatched query plan subtrees.

use sbroad::backend::sql::ir::PatternWithParams;
use sbroad::backend::sql::space::{TableGuard, ADMIN_ID};
use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::helpers::storage::{unprepare, StorageReturnFormat};
use sbroad::executor::engine::helpers::vshard::{get_random_bucket, CacheInfo};
use sbroad::executor::engine::helpers::{
    self, execute_first_cacheable_request, execute_second_cacheable_request, read_or_prepare,
    EncodedQueryInfo, FullPlanInfo, OptionalBytes, RequiredPlanInfo,
};
use sbroad::executor::engine::{DispatchReturnFormat, QueryCache, StorageCache, Vshard};
use sbroad::executor::ir::{ExecutionPlan, QueryType};
use sbroad::executor::lru::{Cache, EvictFn, LRUCache};
use sbroad::executor::protocol::{EncodedVTables, RequiredData, SchemaInfo};
use sbroad::executor::result::ProducerResult;
use sbroad::ir::value::Value;
use tarantool::fiber::Mutex;
use tarantool::sql::Statement;
use tarantool::tuple::{Tuple, TupleBuffer};

use crate::sql::router::{calculate_bucket_id, get_table_version, VersionMap};
use crate::traft::node;
use once_cell::sync::Lazy;
use sbroad::backend::sql::tree::{OrderedSyntaxNodes, SyntaxData, SyntaxPlan};
use sbroad::ir::node::NodeId;
use sbroad::ir::tree::Snapshot;
use smol_str::{format_smolstr, SmolStr};
use std::collections::HashMap;
use std::{any::Any, rc::Rc};
use tarantool::msgpack;
use tarantool::session::with_su;

thread_local!(
    pub static BUCKET_COUNT: Lazy<u64> = Lazy::new(|| {
        let node = node::global().expect("node should be initialized at this moment");
        let tier_name = node.topology_cache.my_tier_name();
        let tier = with_su(ADMIN_ID, || node.storage.tiers.by_name(tier_name))
                            .expect("su shouldn't fail")
                            .expect("storage shouldn't fail")
                            .expect("tier should exists");
        tier.bucket_count
    });

    // We need Lazy, because cache can be initialized only after raft node.
    pub static STATEMENT_CACHE: Lazy<Rc<Mutex<PicoStorageCache>>> = Lazy::new(|| {
        let node = node::global().expect("node should be initialized at this moment");
        let tier = node.raft_storage.tier().expect("storage shouldn't fail").expect("tier for instance should exists");
        let capacity = node.storage.db_config.sql_storage_cache_count_max(&tier).expect("storage shouldn't fail");
        let cache_impl = Rc::new(Mutex::new(PicoStorageCache::new(capacity, Some(Box::new(evict))).unwrap()));
        cache_impl
    })
);

#[allow(clippy::module_name_repetitions)]
pub struct StorageRuntime {
    bucket_count: u64,
    cache: Rc<Mutex<PicoStorageCache>>,
}

type StorageCacheValue = (Statement, VersionMap, Vec<NodeId>);

fn evict(plan_id: &SmolStr, val: &mut StorageCacheValue) -> Result<(), SbroadError> {
    let (stmt, _, table_ids) = std::mem::take(val);
    unprepare(plan_id, &mut (stmt, table_ids))
}

pub struct PicoStorageCache {
    pub cache: LRUCache<SmolStr, StorageCacheValue>,
    pub capacity: usize,
}

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

        Ok(PicoStorageCache {
            cache: LRUCache::new(capacity, new_fn)?,
            capacity,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn adjust_capacity(&mut self, target_capacity: usize) -> Result<(), SbroadError> {
        self.cache.adjust_capacity(target_capacity)
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
        let pico_table = &node.storage.pico_table;
        for table_name in schema_info.router_version_map.keys() {
            let current_version = if let Some(table_def) =
                pico_table.by_name(table_name.as_str()).map_err(|e| {
                    SbroadError::FailedTo(Action::Get, None, format_smolstr!("table_def: {}", e))
                })? {
                table_def.schema_version
            } else {
                return Err(SbroadError::NotFound(
                    Entity::SpaceMetadata,
                    format_smolstr!("for space: {}", table_name),
                ));
            };
            version_map.insert(table_name.clone(), current_version);
        }

        self.cache.put(plan_id, (stmt, version_map, table_ids))?;
        Ok(())
    }

    fn get(&mut self, plan_id: &SmolStr) -> Result<Option<(&Statement, &[NodeId])>, SbroadError> {
        let Some((ir, version_map, table_ids)) = self.cache.get(plan_id)? else {
            return Ok(None);
        };
        // check Plan's tables have up to date schema
        let node = node::global().map_err(|e| {
            SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
        })?;
        let pico_table = &node.storage.pico_table;
        for (table_name, cached_version) in version_map {
            let Some(table_def) = pico_table.by_name(table_name.as_str()).map_err(|e| {
                SbroadError::FailedTo(Action::Get, None, format_smolstr!("table_def: {}", e))
            })?
            else {
                return Ok(None);
            };
            // The outdated entry will be replaced when `put` is called (that is always
            // called after the cache miss).
            if *cached_version != table_def.schema_version {
                return Ok(None);
            }
        }
        Ok(Some((ir, table_ids)))
    }

    fn clear(&mut self) -> Result<(), SbroadError> {
        self.cache.clear()
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

struct LocalExecutionQueryInfo<'sn> {
    exec_plan: ExecutionPlan,
    plan_id: SmolStr,
    nodes: Vec<&'sn SyntaxData>,
    params: Vec<Value>,
    schema_info: SchemaInfo,
}

impl RequiredPlanInfo for LocalExecutionQueryInfo<'_> {
    fn id(&self) -> &SmolStr {
        &self.plan_id
    }

    fn params(&self) -> &Vec<Value> {
        &self.params
    }

    fn schema_info(&self) -> &SchemaInfo {
        &self.schema_info
    }

    fn extract_data(&mut self) -> EncodedVTables {
        self.exec_plan.encode_vtables()
    }

    fn sql_vdbe_opcode_max(&self) -> u64 {
        self.exec_plan
            .get_ir_plan()
            .effective_options
            .sql_vdbe_opcode_max as u64
    }

    fn sql_motion_row_max(&self) -> u64 {
        self.exec_plan.get_sql_motion_row_max()
    }
}

impl FullPlanInfo for LocalExecutionQueryInfo<'_> {
    fn extract_query_and_table_guard(
        &mut self,
    ) -> Result<(PatternWithParams, Vec<TableGuard>), SbroadError> {
        self.exec_plan.to_sql(&self.nodes, &self.plan_id, None)
    }
}

impl Vshard for StorageRuntime {
    fn bucket_count(&self) -> u64 {
        self.bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError> {
        calculate_bucket_id(s, self.bucket_count())
    }

    fn exec_ir_on_any_node(
        &self,
        sub_plan: ExecutionPlan,
        return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let plan = sub_plan.get_ir_plan();
        let top_id = plan.get_top()?;
        let plan_id = plan.pattern_id(top_id)?;
        let sp = SyntaxPlan::new(&sub_plan, top_id, Snapshot::Oldest)?;
        let ordered = OrderedSyntaxNodes::try_from(sp)?;
        let nodes = ordered.to_syntax_data()?;
        let params = sub_plan.to_params().to_vec();
        let version_map = sub_plan.get_ir_plan().version_map.clone();
        let schema_info = SchemaInfo::new(version_map);
        let mut info = LocalExecutionQueryInfo {
            exec_plan: sub_plan,
            plan_id,
            nodes,
            params,
            schema_info,
        };
        let mut locked_cache = self.cache().lock();
        let boxed_bytes: Box<dyn Any> = read_or_prepare::<Self, <Self as QueryCache>::Mutex>(
            &mut locked_cache,
            &mut info,
            &StorageReturnFormat::DqlRaw,
        )?;
        let bytes = boxed_bytes.downcast::<Vec<u8>>().map_err(|e| {
            SbroadError::Invalid(
                Entity::MsgPack,
                Some(format_smolstr!("expected Tuple as result: {e:?}")),
            )
        })?;
        // TODO: introduce a wrapper type, do not use raw Vec<u8> and
        // implement convert trait
        let res: Box<dyn Any> = match return_format {
            DispatchReturnFormat::Tuple => {
                let tup_buf = TupleBuffer::try_from_vec(*bytes)
                    .expect("failed to convert raw dql result to tuple buffer");
                Box::new(Tuple::from(&tup_buf))
            }
            DispatchReturnFormat::Inner => {
                let mut data: Vec<ProducerResult> =
                    msgpack::decode(bytes.as_slice()).map_err(|e| {
                        SbroadError::Other(format_smolstr!("decode bytes into inner format: {e:?}"))
                    })?;
                let inner = data.get_mut(0).ok_or_else(|| {
                    SbroadError::NotFound(Entity::ProducerResult, "from the tuple".into())
                })?;
                let inner_owned = std::mem::take(inner);
                Box::new(inner_owned)
            }
        };
        Ok(res)
    }

    fn exec_ir_on_buckets(
        &self,
        _sub_plan: ExecutionPlan,
        _buckets: &Buckets,
        _return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError> {
        return Err(SbroadError::Other(
            "storage runtime can't execute vshard queries".into(),
        ));
    }
}

impl StorageRuntime {
    /// Build a new storage runtime.
    ///
    /// # Errors
    /// - Failed to initialize the LRU cache.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        BUCKET_COUNT.with(|count| Self::new_with_bucket_count(*(*count)))
    }

    fn new_with_bucket_count(bucket_count: u64) -> Self {
        STATEMENT_CACHE.with(|cache| StorageRuntime {
            bucket_count,
            cache: (*cache).clone(),
        })
    }

    /// Execute dispatched plan (divided into required and optional parts).
    ///
    /// # Errors
    /// - Something went wrong while executing the plan.
    #[allow(unused_variables)]
    pub fn execute_plan(
        &self,
        required: &mut RequiredData,
        mut raw_optional: OptionalBytes,
        cache_info: CacheInfo,
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
            QueryType::DML => helpers::execute_dml(self, required, raw_optional.get_mut()?),
            QueryType::DQL => {
                let mut info: EncodedQueryInfo<'_> = EncodedQueryInfo::new(raw_optional, required);
                match cache_info {
                    CacheInfo::CacheableFirstRequest => {
                        execute_first_cacheable_request(self, &mut info)
                    }
                    CacheInfo::CacheableSecondRequest => {
                        execute_second_cacheable_request(self, &mut info)
                    }
                }
            }
        }
    }
}
