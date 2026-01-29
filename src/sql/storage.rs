//! Storage runtime of the clusterwide SQL.
//! Implements the `sbroad` crate infrastructure
//! for execution of the dispatched query plan subtrees.

use crate::sql::dispatch::port_write_metadata;
use crate::sql::execute::{dml_execute, dql_execute, explain_execute};
use crate::sql::router::{
    calculate_bucket_id, get_index_version_by_pk, get_table_name_and_version, get_table_version,
    get_table_version_by_id, VersionMap,
};
use crate::traft::node;
use serde::{Deserialize, Serialize};
use sql::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use sql::errors::{Action, Entity, SbroadError};
use sql::executor::bucket::Buckets;
use sql::executor::engine::helpers::new_table_name;
use sql::executor::engine::helpers::vshard::get_random_bucket;
use sql::executor::engine::{QueryCache, StorageCache, Vshard};
use sql::executor::ir::{ExecutionPlan, QueryType};
use sql::executor::lru::{Cache, EvictFn, LRUCache};
use sql::executor::protocol::SchemaInfo;
use sql::executor::Port;
use sql::ir::helpers::RepeatableState;
use sql::ir::options::Options;
use sql::ir::ExplainType;
use std::cell::OnceCell;

use crate::metrics::{
    report_storage_cache_hit, report_storage_cache_miss, STORAGE_CACHE_STATEMENTS_ADDED_TOTAL,
    STORAGE_CACHE_STATEMENTS_EVICTED_TOTAL,
};
use smol_str::{format_smolstr, SmolStr};
use sql::executor::vdbe::SqlStmt;
use sql::executor::vtable::{VirtualTable, VirtualTableTupleEncoder};
use sql::ir::tree::Snapshot;
use sql::ir::value::Value;
use sql_protocol::decode::{ProtocolMessage, ProtocolMessageIter, ProtocolMessageType};
use sql_protocol::dql_encoder::ColumnType;
use sql_protocol::error::ProtocolError;
use sql_protocol::iterators::TupleIterator;
use std::collections::HashMap;
use std::rc::Rc;
use tarantool::space::{Space, SpaceId};

use crate::schema::ADMIN_ID;
use crate::sql::execute::{
    sql_execute, stmt_execute, LazyVirtualTableEncoder, LendingTupleIterator,
};
use crate::tlog;
use tarantool::fiber::Mutex;
use tarantool::msgpack;
use tarantool::session::with_su;

thread_local!(
    // OnceCell is used for interior mutability
    pub static STATEMENT_CACHE: OnceCell<Rc<Mutex<PicoStorageCache>>> = const { OnceCell::new() };
);

pub fn init_statement_cache(count_max: usize, size_max: usize) {
    STATEMENT_CACHE.with(|cache| {
        assert!(cache.get().is_none(), "must be initialized only once");
        cache.get_or_init(|| {
            Rc::new(Mutex::new(
                PicoStorageCache::new(count_max, size_max, Some(Box::new(evict))).unwrap(),
            ))
        });
    });
}

#[allow(clippy::module_name_repetitions)]
pub struct StorageRuntime {
    bucket_count: u64,
    cache: Rc<Mutex<PicoStorageCache>>,
}

type StorageCacheValue = (
    SqlStmt,
    VersionMap,
    HashMap<[u32; 2], u64, RepeatableState>,
    Vec<SmolStr>,
);

fn evict(_plan_id: &u64, val: &mut StorageCacheValue) -> Result<(), SbroadError> {
    STORAGE_CACHE_STATEMENTS_EVICTED_TOTAL.inc();
    // Remove temporary tables from the instance.
    for table in &val.3 {
        Space::find(table.as_str()).map(|space| {
            with_su(ADMIN_ID, || {
                space
                    .drop()
                    .inspect_err(|e| tlog!(Error, "failed to drop temporary table {table}: {e:?}"))
            })
        });
    }
    Ok(())
}

pub struct PicoStorageCache {
    pub cache: LRUCache<u64, StorageCacheValue>,
    /// Amount of memory currently used by SQL statements.
    /// NB: We track only SQL statements because they occupy the most memory.
    mem_used: usize,
    /// Maximum amount of memory that can be used by SQL statements.
    /// NB: We track only SQL statements because they occupy the most memory.
    mem_limit: usize,
}

impl PicoStorageCache {
    pub fn new(
        count_max: usize,
        size_max: usize,
        evict_fn: Option<EvictFn<u64, StorageCacheValue>>,
    ) -> Result<Self, SbroadError> {
        let new_fn: Option<EvictFn<u64, StorageCacheValue>> = if let Some(evict_fn) = evict_fn {
            let new_fn = move |key: &u64, val: &mut StorageCacheValue| -> Result<(), SbroadError> {
                evict_fn(key, val)
            };
            Some(Box::new(new_fn))
        } else {
            None
        };

        Ok(PicoStorageCache {
            cache: LRUCache::new(count_max, new_fn)?,
            mem_limit: size_max,
            mem_used: 0,
        })
    }

    pub fn capacity(&self) -> usize {
        self.cache.capacity()
    }

    pub fn adjust_count_max(&mut self, count_max: usize) -> Result<(), SbroadError> {
        debug_assert!(count_max > 0);

        while self.cache.len() > count_max {
            self.pop()?;
        }

        self.cache.adjust_capacity(count_max)
    }

    pub fn adjust_size_max(&mut self, size_max: usize) -> Result<(), SbroadError> {
        debug_assert!(size_max > 0);

        while self.mem_used > size_max {
            self.pop()?;
        }
        self.mem_limit = size_max;
        Ok(())
    }

    fn pop(&mut self) -> Result<Option<StorageCacheValue>, SbroadError> {
        let removed = self.cache.pop()?;
        Ok(removed.inspect(|x| self.mem_used -= x.0.estimated_size()))
    }
}

impl StorageCache for PicoStorageCache {
    fn put(
        &mut self,
        plan_id: u64,
        stmt: SqlStmt,
        schema_info: &SchemaInfo,
        table_names: Vec<SmolStr>,
    ) -> Result<(), SbroadError> {
        let mut table_version_map =
            HashMap::with_capacity_and_hasher(schema_info.table_version_map.len(), RepeatableState);
        let mut index_version_map =
            HashMap::with_capacity_and_hasher(schema_info.index_version_map.len(), RepeatableState);
        let node = node::global().map_err(|e| {
            SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
        })?;
        let pico_table = &node.storage.pico_table;
        for table_id in schema_info.table_version_map.keys() {
            let current_version = if let Some(table_def) = with_su(ADMIN_ID, || {
                pico_table.by_id(*table_id).map_err(|e| {
                    SbroadError::FailedTo(Action::Get, None, format_smolstr!("table_def: {}", e))
                })
            })?? {
                table_def.schema_version
            } else {
                return Err(SbroadError::NotFound(
                    Entity::SpaceMetadata,
                    format_smolstr!("for space: {}", table_id),
                ));
            };

            table_version_map.insert(*table_id, current_version);
        }

        for index in schema_info.index_version_map.keys() {
            let current_version = get_index_version_by_pk(index[0], index[1])?;
            index_version_map.insert(*index, current_version);
        }

        let mem_added = stmt.estimated_size();
        let removed = self.cache.put(
            plan_id,
            (stmt, table_version_map, index_version_map, table_names),
        )?;
        let mem_removed = removed.map(|x| x.0.estimated_size()).unwrap_or(0);

        self.mem_used += mem_added;
        self.mem_used -= mem_removed;

        STORAGE_CACHE_STATEMENTS_ADDED_TOTAL.inc();
        Ok(())
    }
    fn get(&mut self, plan_id: &u64) -> Result<Option<(&mut SqlStmt, &[SmolStr])>, SbroadError> {
        let Some((ir, table_version_map, index_version_map, table_ids)) =
            self.cache.get_mut(plan_id)
        else {
            return Ok(None);
        };
        // check Plan's tables have up to date schema
        let node = node::global().map_err(|e| {
            SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
        })?;
        let pico_table = &node.storage.pico_table;
        for (table_id, cached_version) in table_version_map {
            let Some(table_def) = with_su(ADMIN_ID, || {
                pico_table.by_id(*table_id).map_err(|e| {
                    SbroadError::FailedTo(Action::Get, None, format_smolstr!("table_def: {}", e))
                })
            })??
            else {
                return Ok(None);
            };
            // The outdated entry will be replaced when `put` is called (that is always
            // called after the cache miss).
            if *cached_version != table_def.schema_version {
                return Ok(None);
            }
        }
        for (pk, cached_version) in index_version_map {
            let version = get_index_version_by_pk(pk[0], pk[1])?;
            if *cached_version != version {
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

    fn get_table_name_and_version(&self, table_id: SpaceId) -> Result<(SmolStr, u64), SbroadError> {
        get_table_name_and_version(table_id)
    }

    fn get_index_version_by_pk(&self, space_id: u32, index_id: u32) -> Result<u64, SbroadError> {
        get_index_version_by_pk(space_id, index_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FullDeleteInfo {
    plan_id: u64,
    schema_info: SchemaInfo,
    options: Options,
    sql: SmolStr,
}

impl FullDeleteInfo {
    pub fn new(plan_id: u64, schema_info: SchemaInfo, options: Options, table_name: &str) -> Self {
        Self {
            plan_id,
            schema_info,
            options,
            sql: format_smolstr!("DELETE FROM \"{}\"", table_name),
        }
    }
}

impl PlanInfo for FullDeleteInfo {
    fn vtables(
        &self,
    ) -> impl ExactSizeIterator<Item = Result<(&str, impl LendingTupleIterator), ProtocolError>>
    {
        HashMap::<&str, TupleIterator>::new().into_iter().map(Ok)
    }

    fn sql_vdbe_opcode_max(&self) -> u64 {
        self.options.sql_vdbe_opcode_max as u64
    }

    fn sql_motion_row_max(&self) -> u64 {
        self.options.sql_motion_row_max as u64
    }

    fn params(&self) -> &[u8] {
        &[0x90]
    }
}

impl ExpandedPlanInfo for FullDeleteInfo {
    fn schema_info(&self) -> &SchemaInfo {
        &self.schema_info
    }

    fn plan_id(&self) -> u64 {
        self.plan_id
    }

    fn vtable_metadata(
        &self,
    ) -> impl ExactSizeIterator<Item = Result<(&str, Vec<(&str, ColumnType)>), ProtocolError>> {
        HashMap::new().into_iter().map(Ok)
    }

    fn sql(&self) -> &str {
        self.sql.as_str()
    }
}

pub trait PlanInfo {
    fn vtables(
        &self,
    ) -> impl ExactSizeIterator<Item = Result<(&str, impl LendingTupleIterator), ProtocolError>>;
    fn sql_vdbe_opcode_max(&self) -> u64;
    fn sql_motion_row_max(&self) -> u64;
    fn params(&self) -> &[u8];
}

pub trait ExpandedPlanInfo {
    fn schema_info(&self) -> &SchemaInfo;
    fn plan_id(&self) -> u64;
    #[allow(clippy::type_complexity)]
    fn vtable_metadata(
        &self,
    ) -> impl ExactSizeIterator<Item = Result<(&str, Vec<(&str, ColumnType)>), ProtocolError>>;
    fn sql(&self) -> &str;
}

pub struct LocalExecutionInfo<'a> {
    vtables: &'a HashMap<SmolStr, Rc<VirtualTable>>,
    sql_motion_row_max: u64,
    sql_vdbe_opcode_max: u64,
    params: Vec<u8>,
}

impl<'a> LocalExecutionInfo<'a> {
    pub fn new(
        vtables: &'a HashMap<SmolStr, Rc<VirtualTable>>,
        sql_motion_row_max: u64,
        sql_vdbe_opcode_max: u64,
        params: Vec<u8>,
    ) -> Self {
        Self {
            vtables,
            sql_motion_row_max,
            sql_vdbe_opcode_max,
            params,
        }
    }
}

impl PlanInfo for LocalExecutionInfo<'_> {
    fn vtables(
        &self,
    ) -> impl ExactSizeIterator<Item = Result<(&str, impl LendingTupleIterator), ProtocolError>>
    {
        self.vtables.iter().map(|(name, table)| {
            Ok((
                name.as_str(),
                LazyVirtualTableEncoder::new(
                    table
                        .get_tuples()
                        .iter()
                        .enumerate()
                        .map(|(idx, tuple)| VirtualTableTupleEncoder::new(tuple, idx as u64)),
                ),
            ))
        })
    }

    fn params(&self) -> &[u8] {
        self.params.as_slice()
    }

    fn sql_vdbe_opcode_max(&self) -> u64 {
        self.sql_vdbe_opcode_max
    }

    fn sql_motion_row_max(&self) -> u64 {
        self.sql_motion_row_max
    }
}

pub struct ExpandedLocalExecutionInfo<'a> {
    schema_info: SchemaInfo,
    plan_id: u64,
    vtables: &'a HashMap<SmolStr, Rc<VirtualTable>>,
    sql: String,
}

impl<'a> ExpandedLocalExecutionInfo<'a> {
    pub fn new(
        schema_info: SchemaInfo,
        plan_id: u64,
        vtables: &'a HashMap<SmolStr, Rc<VirtualTable>>,
        sql: String,
    ) -> Self {
        Self {
            schema_info,
            plan_id,
            vtables,
            sql,
        }
    }
}

impl ExpandedPlanInfo for ExpandedLocalExecutionInfo<'_> {
    fn schema_info(&self) -> &SchemaInfo {
        &self.schema_info
    }

    fn plan_id(&self) -> u64 {
        self.plan_id
    }

    fn vtable_metadata(
        &self,
    ) -> impl ExactSizeIterator<Item = Result<(&str, Vec<(&str, ColumnType)>), ProtocolError>> {
        self.vtables.iter().map(|(name, table)| {
            Ok((
                name.as_str(),
                table
                    .get_columns()
                    .iter()
                    .map(|column| (column.name.as_str(), column.r#type.into()))
                    .collect(),
            ))
        })
    }

    fn sql(&self) -> &str {
        self.sql.as_str()
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
        mut ex_plan: ExecutionPlan,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        let plan = ex_plan.get_ir_plan();
        let explain_type = plan.get_explain_type();
        let top_id = plan.get_top()?;

        if let Some(explain_type) = explain_type {
            let is_fmt = match explain_type {
                ExplainType::Explain => unreachable!("Explain should already be handled."),
                ExplainType::ExplainQueryPlan => false,
                ExplainType::ExplainQueryPlanFmt => true,
            };

            let sql_vdbe_opcode_max = plan.effective_options.sql_vdbe_opcode_max as u64;

            let plan_id = plan.new_pattern_id(top_id)?;
            let vtables = ex_plan
                .get_vtables()
                .iter()
                .map(|(node_id, table)| (new_table_name(plan_id, *node_id), table.clone()))
                .collect::<HashMap<_, _>>();

            let sp = SyntaxPlan::new(&ex_plan, top_id, Snapshot::Oldest)?;
            let ordered = OrderedSyntaxNodes::try_from(sp)?;
            let nodes = ordered.to_syntax_data()?;
            let (local_sql, _) = ex_plan.generate_sql(&nodes, plan_id, None, new_table_name)?;

            let schema_info = SchemaInfo::new(
                std::mem::take(&mut ex_plan.get_mut_ir_plan().table_version_map),
                std::mem::take(&mut ex_plan.get_mut_ir_plan().index_version_map),
            );

            let miss_info = ExpandedLocalExecutionInfo {
                schema_info,
                plan_id,
                vtables: &vtables,
                sql: local_sql,
            };

            let location = buckets.determine_exec_location();
            explain_execute(
                self,
                miss_info,
                ex_plan.to_params(),
                sql_vdbe_opcode_max,
                is_fmt,
                location,
                port,
            )?;

            return Ok(());
        }

        port_write_metadata(port, &ex_plan)?;

        let query_type = ex_plan.query_type()?;
        if let QueryType::DML = query_type {
            // DML queries are not supported on arbitrary nodes
            return Err(SbroadError::Other(
                "DML queries are not supported on arbitrary nodes".into(),
            ));
        }

        let plan_id = plan.new_pattern_id(top_id)?;
        let vtables = ex_plan
            .get_vtables()
            .iter()
            .map(|(node_id, table)| (new_table_name(plan_id, *node_id), table.clone()))
            .collect::<HashMap<_, _>>();
        let info = LocalExecutionInfo::new(
            &vtables,
            plan.effective_options.sql_motion_row_max as u64,
            plan.effective_options.sql_vdbe_opcode_max as u64,
            msgpack::encode(&ex_plan.to_params()),
        );

        use sql::executor::vdbe::ExecutionInsight::*;
        let mut cache_guarded = self.cache().lock();

        if let Some((stmt, _)) = cache_guarded.get(&plan_id)? {
            // Transaction rollbacks are very expensive in Tarantool, so we're going to
            // avoid transactions for DQL queries. We can achieve atomicity by truncating
            // temporary tables. Isolation is guaranteed by keeping a lock on the cache.
            match stmt_execute(stmt, &info, port)? {
                Nothing => report_storage_cache_hit("dql", "local"),
                BusyStmt => report_storage_cache_miss("dql", "local", "busy"),
                StaleStmt => report_storage_cache_miss("dql", "local", "stale"),
            }
        } else {
            let sp = SyntaxPlan::new(&ex_plan, top_id, Snapshot::Oldest)?;
            let ordered = OrderedSyntaxNodes::try_from(sp)?;
            let nodes = ordered.to_syntax_data()?;
            let (local_sql, _) = ex_plan.generate_sql(&nodes, plan_id, None, new_table_name)?;

            let schema_info = SchemaInfo::new(
                std::mem::take(&mut ex_plan.get_mut_ir_plan().table_version_map),
                std::mem::take(&mut ex_plan.get_mut_ir_plan().index_version_map),
            );

            let miss_info = ExpandedLocalExecutionInfo {
                plan_id,
                schema_info,
                vtables: &vtables,
                sql: local_sql,
            };

            match sql_execute::<Self>(&mut cache_guarded, &info, &miss_info, port)? {
                Nothing => report_storage_cache_miss("dql", "local", "true"),
                BusyStmt => report_storage_cache_miss("dql", "local", "busy"),
                StaleStmt => report_storage_cache_miss("dql", "local", "stale"),
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
        let cache = STATEMENT_CACHE.with(|cache| {
            cache
                .get()
                .expect("should be initialized by this point")
                .clone()
        });
        Self {
            bucket_count: tier.bucket_count,
            cache,
        }
    }

    /// Execute dispatched plan
    ///
    /// # Errors
    /// - Something went wrong while executing the plan.
    #[allow(unused_variables)]
    pub fn execute_plan<'p>(
        &self,
        package: ProtocolMessage,
        port: &mut impl Port<'p>,
        timeout: f64,
    ) -> Result<(), SbroadError> {
        match package.msg_type {
            ProtocolMessageType::Dql => {
                let ProtocolMessageIter::Dql(info) = package.get_iter()? else {
                    unreachable!("should be dql iterator")
                };
                dql_execute(self, package.request_id, info, port, timeout)?;
            }
            ProtocolMessageType::Dml(_) | ProtocolMessageType::LocalDml(_) => {
                dml_execute(self, &package, port, timeout)?;
            }
        }
        Ok(())
    }
}
