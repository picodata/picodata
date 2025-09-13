//! Storage runtime of the clusterwide SQL.
//! Implements the `sbroad` crate infrastructure
//! for execution of the dispatched query plan subtrees.

use crate::sql::dispatch::port_write_metadata;
use crate::sql::execute::{
    dml_execute, dql_execute_first_round, dql_execute_second_round, explain_execute,
};
use crate::sql::router::{
    calculate_bucket_id, get_table_version, get_table_version_by_id, VersionMap,
};
use crate::traft::node;
use once_cell::sync::Lazy;
use sbroad::backend::sql::ir::PatternWithParams;
use sbroad::backend::sql::space::TableGuard;
use sbroad::backend::sql::tree::{OrderedSyntaxNodes, SyntaxData, SyntaxPlan};
use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::helpers::storage::unprepare;
use sbroad::executor::engine::helpers::vshard::get_random_bucket;
use sbroad::executor::engine::helpers::{
    table_name, EncodedQueryInfo, FullPlanInfo, RequiredPlanInfo,
};
use sbroad::executor::engine::{QueryCache, StorageCache, Vshard};
use sbroad::executor::ir::{ExecutionPlan, QueryType};
use sbroad::executor::lru::{Cache, EvictFn, LRUCache};
use sbroad::executor::protocol::{EncodedVTables, RequiredData, SchemaInfo, VTablesMeta};
use sbroad::executor::{Port, PortType};
use sbroad::ir::ExplainType;

use crate::metrics::{
    STORAGE_CACHE_STATEMENTS_ADDED_TOTAL, STORAGE_CACHE_STATEMENTS_EVICTED_TOTAL,
};
use sbroad::ir::node::NodeId;
use sbroad::ir::tree::Snapshot;
use sbroad::ir::value::Value;
use smol_str::{format_smolstr, SmolStr};
use std::collections::HashMap;
use std::rc::Rc;
use tarantool::fiber::Mutex;
use tarantool::space::SpaceId;

use tarantool::sql::Statement;

thread_local!(
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
    STORAGE_CACHE_STATEMENTS_EVICTED_TOTAL.inc();
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
        STORAGE_CACHE_STATEMENTS_ADDED_TOTAL.inc();
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
}

impl QueryCache for StorageRuntime {
    type Cache = PicoStorageCache;
    type Mutex = Mutex<Self::Cache>;

    fn cache(&self) -> &Self::Mutex {
        &self.cache
    }

    fn provides_versions(&self) -> bool {
        true
    }

    fn get_table_version(&self, table_name: &str) -> Result<u64, SbroadError> {
        get_table_version(table_name)
    }

    fn get_table_version_by_id(&self, table_id: SpaceId) -> Result<u64, SbroadError> {
        get_table_version_by_id(table_id)
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

    fn take_query_meta(&mut self) -> Result<(String, Vec<NodeId>, VTablesMeta), SbroadError> {
        let vtables = self.exec_plan.get_vtables();
        let mut meta = VTablesMeta::with_capacity(vtables.len());
        for (id, table) in vtables.iter() {
            meta.insert(*id, table.metadata());
        }

        let (local_sql, motion_ids) = self.exec_plan.generate_sql(
            &self.nodes,
            self.plan_id.as_str(),
            Some(&meta),
            |name: &str, id| table_name(name, id),
        )?;

        Ok((local_sql, motion_ids, meta))
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

    fn exec_ir_on_any_node<'p>(
        &self,
        ex_plan: ExecutionPlan,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        if !ex_plan.get_ir_plan().is_raw_explain() {
            port_write_metadata(port, &ex_plan)?;
        }
        let plan = ex_plan.get_ir_plan();
        let query_type = ex_plan.query_type()?;
        let top_id = plan.get_top()?;
        let explain_type = plan.get_explain_type();
        let plan_id = plan.pattern_id(top_id)?;
        let sp = SyntaxPlan::new(&ex_plan, top_id, Snapshot::Oldest)?;
        let ordered = OrderedSyntaxNodes::try_from(sp)?;
        let nodes = ordered.to_syntax_data()?;
        let params = ex_plan.to_params().to_vec();
        let version_map = ex_plan.get_ir_plan().version_map.clone();
        let schema_info = SchemaInfo::new(version_map);
        let mut info = LocalExecutionQueryInfo {
            exec_plan: ex_plan,
            plan_id,
            nodes,
            params,
            schema_info,
        };

        match explain_type {
            None => {
                if let QueryType::DML = query_type {
                    // DML queries are not supported on arbitrary nodes
                    return Err(SbroadError::Other(
                        "DML queries are not supported on arbitrary nodes".into(),
                    ));
                }
                dql_execute_second_round(self, &mut info, port)?;
            }
            Some(ExplainType::Explain) => unreachable!("Explain should already be handled."),
            Some(ExplainType::ExplainQueryPlan) => {
                let location = buckets.determine_exec_location();
                explain_execute(self, &mut info, false, location, port)?;
            }
            Some(ExplainType::ExplainQueryPlanFmt) => {
                let location = buckets.determine_exec_location();
                explain_execute(self, &mut info, true, location, port)?;
            }
        }
        Ok(())
    }

    fn exec_ir_on_buckets<'p>(
        &self,
        _sub_plan: ExecutionPlan,
        _buckets: &Buckets,
        _port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
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
        let node = node::global().expect("node should be initialized at this moment");
        let topology_ref = node.topology_cache.get();
        let tier = topology_ref.this_tier();
        STATEMENT_CACHE.with(|cache| StorageRuntime {
            bucket_count: tier.bucket_count,
            cache: (*cache).clone(),
        })
    }

    /// Execute dispatched plan (divided into required and optional parts).
    ///
    /// # Errors
    /// - Something went wrong while executing the plan.
    #[allow(unused_variables)]
    pub fn execute_plan<'p>(
        &self,
        required: &mut RequiredData,
        raw_optional: Option<&[u8]>,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        // Compare router's schema versions with storage's ones.
        for (table, version) in &required.schema_info.router_version_map {
            // TODO: if storage version is smaller than router's version
            // wait until state catches up.
            if *version != get_table_version(table.as_str())? {
                return Err(SbroadError::OutdatedStorageSchema);
            }
        }
        match required.query_type {
            QueryType::DML => {
                let Some(bytes) = raw_optional else {
                    return Err(SbroadError::Other(
                        "DML query must have a non-empty optional part".into(),
                    ));
                };
                dml_execute(self, required, bytes, port)?;
            }
            QueryType::DQL => {
                let is_first_round = raw_optional.is_none();
                let mut info: EncodedQueryInfo<'_> = EncodedQueryInfo::new(raw_optional, required);
                if is_first_round {
                    dql_execute_first_round(self, &mut info, port)?;
                } else {
                    port.set_type(PortType::ExecuteDql);
                    dql_execute_second_round(self, &mut info, port)?;
                }
            }
        }
        Ok(())
    }
}
