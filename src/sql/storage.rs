//! Storage runtime of the clusterwide SQL.
//! Implements the `sbroad` crate infrastructure
//! for execution of the dispatched query plan subtrees.

use crate::sql::dispatch::port_write_metadata;
use crate::sql::router::{
    calculate_bucket_id, get_index_version_by_pk, get_table_name_and_version, get_table_version,
    get_table_version_by_id, VersionMap,
};
use crate::traft::node;
use serde::{Deserialize, Serialize};
use sql::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use sql::errors::{Action, Entity, SbroadError};
use sql::executor::engine::helpers::table_name;
use sql::executor::engine::helpers::vshard::get_random_bucket;
use sql::executor::engine::{CachedStmt, CachedStmtRef, QueryCache, StorageCache, Vshard};
use sql::executor::ir::{ExecutionPlan, QueryType};
use sql::executor::lru::{Cache, EvictFn, LRUCache};
use sql::executor::protocol::SchemaInfo;
use sql::executor::{Port, PortType};
use sql::ir::bucket::Buckets;
use sql::ir::helpers::RepeatableState;
use sql::ir::options::Options;
use std::cell::{OnceCell, RefCell};

use crate::metrics::{
    report_storage_cache_hit, report_storage_cache_miss, STORAGE_CACHE_STATEMENTS_ADDED_TOTAL,
    STORAGE_CACHE_STATEMENTS_EVICTED_TOTAL,
};
use smol_str::{format_smolstr, SmolStr};
use sql::executor::vdbe::SqlStmt;
use sql::executor::vtable::{VirtualTable, VirtualTableTupleEncoder};
use sql::ir::node::BlockStatement;
use sql::ir::tree::Snapshot;
use sql::ir::value::Value;
use sql_protocol::decode::{ProtocolMessage, ProtocolMessageIter, ProtocolMessageType};
use sql_protocol::dql_encoder::ColumnType;
use sql_protocol::error::ProtocolError;
use sql_protocol::iterators::TupleIterator;
use std::collections::{hash_map::Entry, HashMap};
use std::rc::Rc;
use tarantool::space::SpaceId;

use super::execute::port_write_execute_dml;
use crate::schema::ADMIN_ID;
use crate::sql::execute::{
    acquire_cached_stmt_or_retry, dml_execute, dql_execute, drop_temp_tables, explain_execute,
    explain_execute_guarded, sql_execute, stmt_execute, LazyVirtualTableEncoder,
    LendingTupleIterator,
};
use crate::sql::lock::{
    new_temp_table_lock, try_lock_temp_table, TempTableLockRef, TempTableLockWeak,
};
use crate::sql::port::PicoPortOwned;
use crate::tlog;
use sql::executor::engine::BlockExecData;
use sql::executor::result::MetadataColumn;
use tarantool::fiber::Mutex;
use tarantool::msgpack;
use tarantool::session::with_su;

thread_local!(
    // OnceCell is used for interior mutability
    pub static STATEMENT_CACHE: OnceCell<Rc<Mutex<PicoStorageCache>>> = const { OnceCell::new() };
    static RETIRED_PLANS: RefCell<HashMap<u64, RetiredPlan>> = RefCell::new(HashMap::new());
    static RECENTLY_RETIRED: RefCell<Vec<RecentlyRetiredPlan>> = const { RefCell::new(Vec::new()) };
);

type TableLocksMap = Rc<RefCell<HashMap<u64, TempTableLockWeak>>>;

struct RetiredPlan {
    motion_ids: Vec<SmolStr>,
}

struct RecentlyRetiredPlan {
    plan_id: u64,
    table_lock: TempTableLockRef,
}

fn take_recently_retired() -> Vec<RecentlyRetiredPlan> {
    RECENTLY_RETIRED.with(|cell| std::mem::take(&mut *cell.borrow_mut()))
}

fn retire_plan(plan_id: u64, entry: &mut StorageCacheEntry) {
    if entry.motion_ids.is_empty() {
        return;
    }

    let mut retired = RetiredPlan {
        motion_ids: std::mem::take(&mut entry.motion_ids),
    };
    RETIRED_PLANS.with(|cell| {
        let mut retired_plans = cell.borrow_mut();
        match retired_plans.entry(plan_id) {
            Entry::Occupied(mut current) => {
                tlog!(
                    Warning,
                    "SQL plan retirement is already pending cleanup; merging temp tables";
                    "plan_id" => plan_id
                );
                current.get_mut().motion_ids.append(&mut retired.motion_ids);
            }
            Entry::Vacant(slot) => {
                slot.insert(retired);
            }
        }
    });
    RECENTLY_RETIRED.with(|cell| {
        cell.borrow_mut().push(RecentlyRetiredPlan {
            plan_id,
            table_lock: Rc::clone(&entry.table_lock),
        });
    });
}

pub(crate) fn finalize_retired_plan(plan_id: u64) {
    crate::error_injection!(block "BLOCK_PROCESS_DEFERRED_EVICTIONS");
    let retired = RETIRED_PLANS.with(|cell| cell.borrow_mut().remove(&plan_id));
    let Some(retired) = retired else {
        return;
    };

    drop_temp_tables(&retired.motion_ids);
}

pub fn init_statement_cache(count_max: usize, size_max: usize) {
    STATEMENT_CACHE.with(|cache| {
        assert!(cache.get().is_none(), "must be initialized only once");
        cache.get_or_init(|| {
            Rc::new(Mutex::new(
                PicoStorageCache::new(count_max, size_max).unwrap(),
            ))
        });
    });
}

/// Try to finalize plans retired under the cache mutex.
///
/// Must be called after releasing the cache mutex whenever evictions may
/// have occurred (after `put`, `adjust_count_max`, `adjust_size_max`).
/// It only finalizes idle retired plans immediately. Active same-plan owners
/// finalize their retired generations in `PlanGuard::drop`.
pub fn process_deferred_evictions() -> Result<(), SbroadError> {
    for retired in take_recently_retired() {
        if let Some(_lease) = try_lock_temp_table(&retired.table_lock) {
            finalize_retired_plan(retired.plan_id);
        }
    }
    Ok(())
}

#[allow(clippy::module_name_repetitions)]
pub struct StorageRuntime {
    bucket_count: u64,
    cache: Rc<Mutex<PicoStorageCache>>,
}

pub(crate) struct StorageCacheEntry {
    stmt: CachedStmt,
    table_lock: TempTableLockRef,
    stmt_size: usize,
    table_versions: VersionMap,
    index_versions: HashMap<[u32; 2], u64, RepeatableState>,
    motion_ids: Vec<SmolStr>,
}

/// Build a lightweight eviction closure that defers actual cleanup.
///
/// It only records per-plan retirement metadata under the cache mutex.
/// Actual table drops are attempted later outside the mutex.
fn make_evict_fn() -> EvictFn<u64, StorageCacheEntry> {
    Box::new(move |plan_id: &u64, val: &mut StorageCacheEntry| {
        STORAGE_CACHE_STATEMENTS_EVICTED_TOTAL.inc();
        retire_plan(*plan_id, val);
        Ok(())
    })
}

pub struct PicoStorageCache {
    pub(crate) cache: LRUCache<u64, StorageCacheEntry>,
    table_locks: TableLocksMap,
    /// Amount of memory currently used by SQL statements.
    /// NB: We track only SQL statements because they occupy the most memory.
    mem_used: usize,
    /// Maximum amount of memory that can be used by SQL statements.
    /// NB: We track only SQL statements because they occupy the most memory.
    mem_limit: usize,
}

impl PicoStorageCache {
    pub(crate) fn new(count_max: usize, size_max: usize) -> Result<Self, SbroadError> {
        let table_locks: TableLocksMap = Rc::new(RefCell::new(HashMap::new()));
        let evict_fn = make_evict_fn();

        Ok(PicoStorageCache {
            cache: LRUCache::new(count_max, Some(evict_fn))?,
            table_locks,
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

    fn pop(&mut self) -> Result<Option<StorageCacheEntry>, SbroadError> {
        let removed = self.cache.pop()?;
        Ok(removed.inspect(|x| self.mem_used -= x.stmt_size))
    }

    pub(crate) fn get_or_create_table_lock(&mut self, plan_id: u64) -> TempTableLockRef {
        if let Some(entry) = self.cache.get_mut(&plan_id) {
            return Rc::clone(&entry.table_lock);
        }

        if self.table_locks.borrow().len() > self.capacity().saturating_mul(4).max(1024) {
            self.table_locks
                .borrow_mut()
                .retain(|_, weak| weak.upgrade().is_some());
        }

        let locks = self.table_locks.borrow();
        if let Some(lock) = locks.get(&plan_id).and_then(|weak| weak.upgrade()) {
            return lock;
        }
        drop(locks);

        let lock = new_temp_table_lock();
        self.table_locks
            .borrow_mut()
            .insert(plan_id, Rc::downgrade(&lock));
        lock
    }
}

impl StorageCache for PicoStorageCache {
    type LockRef = TempTableLockRef;

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

        let table_lock = self.get_or_create_table_lock(plan_id);
        let mem_added = stmt.estimated_size();
        let stmt = Rc::new(Mutex::new(stmt));
        let removed = self.cache.put(
            plan_id,
            StorageCacheEntry {
                stmt,
                table_lock,
                stmt_size: mem_added,
                table_versions: table_version_map,
                index_versions: index_version_map,
                motion_ids: table_names,
            },
        )?;
        let mem_removed = removed.map(|x| x.stmt_size).unwrap_or(0);

        self.mem_used += mem_added;
        self.mem_used -= mem_removed;

        STORAGE_CACHE_STATEMENTS_ADDED_TOTAL.inc();
        Ok(())
    }
    fn get(&mut self, plan_id: &u64) -> Result<Option<CachedStmtRef<Self::LockRef>>, SbroadError> {
        let Some(entry) = self.cache.get_mut(plan_id) else {
            return Ok(None);
        };
        // check Plan's tables have up to date schema
        let node = node::global().map_err(|e| {
            SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
        })?;
        let pico_table = &node.storage.pico_table;
        for (table_id, cached_version) in &entry.table_versions {
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
        for (pk, cached_version) in &entry.index_versions {
            let version = get_index_version_by_pk(pk[0], pk[1])?;
            if *cached_version != version {
                return Ok(None);
            }
        }

        Ok(Some(CachedStmtRef::new(
            Rc::clone(&entry.stmt),
            Rc::clone(&entry.table_lock),
        )))
    }

    fn get_or_create_lock(&mut self, plan_id: u64) -> Self::LockRef {
        self.get_or_create_table_lock(plan_id)
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
        let top_id = plan.get_top()?;

        if plan.is_raw_explain() {
            let sql_vdbe_opcode_max = plan.effective_options.sql_vdbe_opcode_max as u64;

            let plan_id = ex_plan.get_plan_id()?;
            let vtables = ex_plan
                .get_vtables()
                .iter()
                .map(|(node_id, table)| (table_name(plan_id, *node_id), table.clone()))
                .collect::<HashMap<_, _>>();

            let sp = SyntaxPlan::new(&ex_plan, top_id, Snapshot::Oldest, false)?;
            let ordered = OrderedSyntaxNodes::try_from(sp)?;
            let nodes = ordered.to_syntax_data()?;
            let local_sql = ex_plan.generate_sql(&nodes, plan_id, table_name, None)?;

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
                ex_plan.get_ir_plan().explain_options,
                location,
                port,
            )?;

            return Ok(());
        }

        let query_type = ex_plan.query_type()?;
        if let QueryType::DML = query_type {
            // DML queries are not supported on arbitrary nodes
            return Err(SbroadError::Other(
                "DML queries are not supported on arbitrary nodes".into(),
            ));
        }

        port_write_metadata(port, &ex_plan)?;

        let plan_id = ex_plan.get_plan_id()?;
        let vtables = ex_plan
            .get_vtables()
            .iter()
            .map(|(node_id, table)| (table_name(plan_id, *node_id), table.clone()))
            .collect::<HashMap<_, _>>();
        let info = LocalExecutionInfo::new(
            &vtables,
            plan.effective_options.sql_motion_row_max as u64,
            plan.effective_options.sql_vdbe_opcode_max as u64,
            msgpack::encode(&ex_plan.to_params()),
        );

        use sql::executor::vdbe::ExecutionInsight::*;
        if let Some((cached, _table_lease)) = acquire_cached_stmt_or_retry(self, plan_id)? {
            // Transaction rollbacks are very expensive in Tarantool, so we're going to
            // avoid transactions for DQL queries. We can achieve atomicity by truncating
            // temporary tables. Isolation is guaranteed by a lock tied to plan_id.
            let mut stmt_guard = cached.stmt.lock();
            match stmt_execute(&mut stmt_guard, &info, port)? {
                Nothing => report_storage_cache_hit("dql", "local"),
                BusyStmt => report_storage_cache_miss("dql", "local", "busy"),
                StaleStmt => report_storage_cache_miss("dql", "local", "stale"),
            }
        } else {
            let sp = SyntaxPlan::new(&ex_plan, top_id, Snapshot::Oldest, false)?;
            let ordered = OrderedSyntaxNodes::try_from(sp)?;
            let nodes = ordered.to_syntax_data()?;
            let local_sql = ex_plan.generate_sql(&nodes, plan_id, table_name, None)?;

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

            match sql_execute::<Self>(self, &info, &miss_info, port)? {
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

    fn exec_block_on_buckets<'p>(
        &self,
        _metadata: Vec<MetadataColumn>,
        _block: BlockExecData,
        _buckets: &Buckets,
        _request_id: &str,
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
            ProtocolMessageType::Block => {
                let ProtocolMessageIter::Block(bytes) = package.get_iter()? else {
                    unreachable!("must be block")
                };
                let block: BlockExecData = rmp_serde::from_slice(bytes).map_err(|e| {
                    SbroadError::Other(format_smolstr!(
                        "failed to parse block message payload: {e}"
                    ))
                })?;
                self.execute_block(block, port)?;
            }
        }
        Ok(())
    }

    pub fn execute_block<'p>(
        &self,
        block: BlockExecData,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        let is_dql = block.returns_rows;
        self.validate_block_schema(&block)?;

        execute_block_locally(block, port)?;

        if is_dql {
            port.set_type(PortType::ExecuteDql);
        } else {
            port.set_type(PortType::ExecuteDml);
        }

        Ok(())
    }

    pub(crate) fn validate_block_schema(&self, block: &BlockExecData) -> Result<(), SbroadError> {
        for (table_id, version) in &block.table_versions {
            if self.get_table_version_by_id(*table_id)? != *version {
                return Err(SbroadError::OutdatedStorageSchema);
            }
        }

        for (ids, version) in &block.index_versions {
            let (table_id, index_id) = (ids[0], ids[1]);
            if self.get_index_version_by_pk(table_id, index_id)? != *version {
                return Err(SbroadError::OutdatedStorageSchema);
            }
        }

        Ok(())
    }
}

pub fn explain_execute_block<'p>(
    block: BlockExecData,
    location: &str,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError> {
    for stmt in block.statements.into_iter() {
        let stmt_kind = stmt.kind();
        let pattern = stmt.take();
        let (sql, params) = pattern.into_parts();
        explain_execute_guarded(
            &sql,
            &params,
            block.vdbe_max_steps,
            block.explain_options,
            stmt_kind,
            location,
            port,
        )?;
    }

    Ok(())
}

pub fn execute_block_locally<'p>(
    block: BlockExecData,
    port: &mut impl Port<'p>,
) -> Result<(), SbroadError> {
    let is_dml = !block
        .statements
        .iter()
        .any(|stmt| matches!(&stmt, BlockStatement::ReturnQuery(_)));

    let mut row_count = 0;
    tarantool::transaction::transaction(|| -> Result<(), SbroadError> {
        for stmt in block.statements {
            match stmt {
                BlockStatement::ReturnQuery(pattern) => {
                    let (sql, params) = pattern.into_parts();
                    let mut vdbe = SqlStmt::compile(&sql)?;
                    port.process_stmt(&mut vdbe, &params, block.vdbe_max_steps)?;
                }
                BlockStatement::Query(pattern) => {
                    let (sql, params) = pattern.into_parts();
                    let mut vdbe = SqlStmt::compile(&sql)?;
                    let mut tmp_port = PicoPortOwned::new();
                    tmp_port.process_stmt(&mut vdbe, &params, block.vdbe_max_steps)?;
                    row_count += parse_row_count_from_port(&tmp_port);
                }
            }
        }
        Ok(())
    })?;

    if is_dml {
        port_write_execute_dml(port, row_count);
    }

    Ok(())
}

/// Extract row_count written by vdbe into the port.
fn parse_row_count_from_port(port: &PicoPortOwned) -> u64 {
    let mp = port.iter().next().expect("port must not be emtpy");
    let mut cursor = std::io::Cursor::new(mp);
    let array_len = rmp::decode::read_array_len(&mut cursor).expect("malformed port");
    assert_eq!(array_len, 1);
    let row_count = rmp::decode::read_int(&mut cursor).expect("malformed port");
    row_count
}
