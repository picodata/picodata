//! Router runtime module for the clusterwide SQL.
//! Implements infrastructure to build a distributed
//! query plan and dispatch it to the storage nodes.

use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use sql::errors::{Action, Entity, SbroadError};
use sql::executor::bucket::Buckets;
use sql::executor::engine::helpers::vshard::get_random_bucket;
use sql::executor::engine::helpers::{
    dispatch_impl, empty_plan_write, explain_format, materialize_motion, materialize_values,
};
use sql::executor::engine::helpers::{sharding_key_from_map, sharding_key_from_tuple};
use sql::executor::engine::{get_builtin_functions, QueryCache, Router, Vshard};
use sql::executor::ir::ExecutionPlan;
use sql::executor::lru::{Cache, EvictFn, LRUCache, DEFAULT_CAPACITY};
use sql::executor::preemption::SchedulerOptions;
use sql::executor::vtable::VirtualTable;
use sql::frontend::sql::ast::AbstractSyntaxTree;
use sql::ir::helpers::RepeatableState;
use sql::ir::node::NodeId;
use sql::ir::value::{MsgPackValue, Value};
use sql::ir::Plan;
use sql::utils::MutexLike;
use tarantool::fiber::Mutex;
use tarantool::session::with_su;

use std::collections::HashMap;
use std::rc::Rc;

use crate::audit;
use crate::preemption::scheduler_options;
use crate::schema::{Distribution, ShardingFn, ADMIN_ID};
use crate::storage::{self, Catalog};

use sql::executor::engine::helpers::normalize_name_from_sql;
use sql::executor::engine::Metadata;
use sql::executor::Port;
use sql::ir::function::Function;
use sql::ir::relation::{space_pk_columns, Column, ColumnRole, Table};
use sql::ir::types::{DerivedType, UnrestrictedType};

use crate::sql::storage::StorageRuntime;
use crate::traft::node;

use crate::metrics::{
    ROUTER_CACHE_HITS_TOTAL, ROUTER_CACHE_MISSES_TOTAL, ROUTER_CACHE_STATEMENTS_ADDED_TOTAL,
    ROUTER_CACHE_STATEMENTS_EVICTED_TOTAL,
};
use ::tarantool::tuple::{KeyDef, Tuple};
use tarantool::space::SpaceId;

use super::dispatch::{custom_plan_dispatch, single_plan_dispatch};
use super::port::PicoPortOwned;

pub type VersionMap = HashMap<u32, u64, RepeatableState>;

thread_local! {
    static PLAN_CACHE: Rc<Mutex<PicoRouterCache>> = Rc::new(
        Mutex::new(PicoRouterCache::new(DEFAULT_CAPACITY).unwrap()));
}

pub fn get_tier_info(tier_name: &str) -> Result<Tier, SbroadError> {
    let node = node::global().map_err(|e| {
        SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
    })?;

    let topology_ref = node.topology_cache.get();
    let tier = topology_ref.tier_by_name(tier_name).map_err(|e| {
        SbroadError::FailedTo(
            Action::Get,
            None,
            format_smolstr!("tier object by tier name: {e}"),
        )
    })?;

    Ok(Tier {
        bucket_count: tier.bucket_count,
        name: tier.name.clone(),
    })
}

fn get_current_tier_name() -> Result<&'static str, SbroadError> {
    let node = node::global().map_err(|e| {
        SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
    })?;
    let tier_name = node.topology_cache.my_tier_name();

    Ok(tier_name)
}

#[derive(Default)]
pub struct Tier {
    bucket_count: u64,
    name: SmolStr,
}

impl Tier {
    fn name(&self) -> Option<SmolStr> {
        Some(self.name.clone())
    }
}

pub const DEFAULT_BUCKET_COLUMN: &str = "bucket_id";

/// Get schema version of given index.
///
/// # Arguments:
/// * `space_id` - id of the table.
/// * `index_id` - id of the index.
///
/// # Errors:
/// - errors on access to system table
pub fn get_index_version_by_pk(space_id: u32, index_id: u32) -> Result<u64, SbroadError> {
    let node = node::global().map_err(|e| {
        SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
    })?;
    let indexes = &node.storage.indexes;
    let index = indexes.get(space_id, index_id).map_err(|e| {
        SbroadError::FailedTo(Action::Get, None, format_smolstr!("index_def: {}", e))
    })?;

    let Some(index) = index else {
        return Err(SbroadError::NotFound(
            Entity::Index,
            format_smolstr!("with id: {}", index_id),
        ));
    };

    Ok(index.schema_version)
}

/// Get the schema version for the given table.
///
/// # Arguments:
/// * `table_name` - name of the table. The name must not
///   be enclosed in quotes as in sql. If in sql user uses
///   `"t"`, here `t` must be passed.
///
/// # Errors:
/// - errors on access to system table
/// - table with given name not found
pub fn get_table_version(table_name: &str) -> Result<u64, SbroadError> {
    let node = node::global().map_err(|e| {
        SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
    })?;
    let pico_table = &node.storage.pico_table;
    if let Some(table_def) = pico_table.by_name(table_name).map_err(|e| {
        SbroadError::FailedTo(Action::Get, None, format_smolstr!("table_def: {}", e))
    })? {
        Ok(table_def.schema_version)
    } else {
        Err(SbroadError::NotFound(
            Entity::SpaceMetadata,
            format_smolstr!("for table: {}", table_name),
        ))
    }
}

/// Get the schema version for the given table.
///
/// # Arguments:
/// * `id` - id of the table
///
/// # Errors:
/// - errors on access to system table
/// - table with given id not found
pub fn get_table_version_by_id(id: SpaceId) -> Result<u64, SbroadError> {
    let node = node::global().map_err(|e| {
        SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
    })?;
    let storage_tables = &node.storage.pico_table;
    if let Some(table_def) = storage_tables.by_id(id).map_err(|e| {
        SbroadError::FailedTo(Action::Get, None, format_smolstr!("table_def: {}", e))
    })? {
        Ok(table_def.schema_version)
    } else {
        Err(SbroadError::NotFound(
            Entity::SpaceMetadata,
            format_smolstr!("for table: {}", id),
        ))
    }
}

pub fn get_index_id(index_name: &str, table_name: &str) -> Result<u32, SbroadError> {
    let storage = Catalog::try_get(false).expect("storage should be initialized");
    let index = storage
        .indexes
        .by_name(index_name)?
        .ok_or_else(|| SbroadError::NotFound(Entity::Index, index_name.to_smolstr()))?;

    let table = storage.pico_table.by_id(index.table_id)?.ok_or_else(|| {
        SbroadError::NotFound(Entity::Table, format_smolstr!("with id {}", index.table_id))
    })?;

    if table.name != table_name {
        return Err(SbroadError::NotFound(
            Entity::Index,
            index_name.to_smolstr(),
        ));
    }

    Ok(index.id)
}

/// Get the name and the schema version for the given table.
///
/// # Arguments:
/// * `id` - id of the table
///
/// # Errors:
/// - errors on access to system table
/// - table with given id not found
pub fn get_table_name_and_version(id: SpaceId) -> Result<(SmolStr, u64), SbroadError> {
    let node = node::global().map_err(|e| {
        SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
    })?;
    let storage_tables = &node.storage.pico_table;
    if let Some(table_def) = storage_tables.by_id(id).map_err(|e| {
        SbroadError::FailedTo(Action::Get, None, format_smolstr!("table_def: {}", e))
    })? {
        Ok((table_def.name, table_def.schema_version))
    } else {
        Err(SbroadError::NotFound(
            Entity::SpaceMetadata,
            format_smolstr!("for table: {}", id),
        ))
    }
}

type IsAuditEnabledFunc = fn(&Plan) -> Result<bool, SbroadError>;

#[allow(clippy::module_name_repetitions)]
pub struct RouterRuntime {
    metadata: Mutex<RouterMetadata>,
    ir_cache: Rc<Mutex<PicoRouterCache>>,
    is_audit_enabled_func: IsAuditEnabledFunc,
}

impl RouterRuntime {
    /// Build a new router runtime.
    ///
    /// # Errors
    /// - If the cache cannot be initialized.
    #[expect(clippy::new_without_default)]
    pub fn new() -> Self {
        let metadata = RouterMetadata::default();
        let runtime = PLAN_CACHE.with(|cache| RouterRuntime {
            metadata: Mutex::new(metadata),
            ir_cache: cache.clone(),
            is_audit_enabled_func: audit::policy::is_dml_audit_enabled_for_user,
        });
        runtime
    }
}

pub type PlanCache = LRUCache<SmolStr, Rc<Plan>>;

/// Wrapper around default LRU cache, that
/// checks schema version.
pub struct PicoRouterCache {
    inner: PlanCache,
}

impl PicoRouterCache {
    pub fn new(capacity: usize) -> Result<Self, SbroadError> {
        fn evict(_key: &SmolStr, _value: &mut Rc<Plan>) -> Result<(), SbroadError> {
            ROUTER_CACHE_STATEMENTS_EVICTED_TOTAL.inc();
            Ok(())
        }

        Ok(PicoRouterCache {
            inner: PlanCache::new(capacity, Some(Box::new(evict)))?,
        })
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}

impl Cache<SmolStr, Rc<Plan>> for PicoRouterCache {
    fn new(
        capacity: usize,
        evict_fn: Option<EvictFn<SmolStr, Rc<Plan>>>,
    ) -> Result<Self, SbroadError>
    where
        Self: Sized,
    {
        Ok(PicoRouterCache {
            inner: PlanCache::new(capacity, evict_fn)?,
        })
    }

    fn get(&mut self, key: &SmolStr) -> Result<Option<&Rc<Plan>>, SbroadError> {
        fn do_get<'a>(
            cache: &'a mut PlanCache,
            key: &SmolStr,
        ) -> Result<Option<&'a Rc<Plan>>, SbroadError> {
            let Some(ir) = cache.get(key)? else {
                return Ok(None);
            };
            // check Plan's tables have up to date schema
            let node = node::global().map_err(|e| {
                SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
            })?;
            let pico_table = &node.storage.pico_table;
            for tbl in ir.relations.tables.values() {
                if tbl.is_system() {
                    continue;
                }
                let cached_version = *ir.table_version_map.get(&tbl.id).ok_or_else(|| {
                    SbroadError::NotFound(
                        Entity::Table,
                        format_smolstr!("in version map with name: {}", tbl.name),
                    )
                })?;
                let Some(space_def) = pico_table.by_id(tbl.id).map_err(|e| {
                    SbroadError::FailedTo(Action::Get, None, format_smolstr!("space_def: {}", e))
                })?
                else {
                    return Ok(None);
                };
                // The outdated entry will be replaced when
                // `put` is called (which is always called
                // after cache miss).
                if cached_version != space_def.schema_version {
                    return Ok(None);
                }
            }

            for index in ir.indexes.indexes.values() {
                let version = get_index_version_by_pk(index.table_id, index.id)?;
                let Some(cached_version) = ir.index_version_map.get(&[index.table_id, index.id])
                else {
                    return Ok(None);
                };
                if *cached_version != version {
                    return Ok(None);
                }
            }

            Ok(Some(ir))
        }

        let value = do_get(&mut self.inner, key)?;

        if value.is_some() {
            ROUTER_CACHE_HITS_TOTAL.inc();
        } else {
            ROUTER_CACHE_MISSES_TOTAL.inc();
        }

        Ok(value)
    }

    fn put(&mut self, key: SmolStr, value: Rc<Plan>) -> Result<Option<Rc<Plan>>, SbroadError> {
        let removed = self.inner.put(key, value)?;
        ROUTER_CACHE_STATEMENTS_ADDED_TOTAL.inc();
        Ok(removed)
    }
}

impl QueryCache for RouterRuntime {
    type Cache = PicoRouterCache;
    type Mutex = Mutex<Self::Cache>;

    fn cache(&self) -> &Self::Mutex {
        &self.ir_cache
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

    fn get_index_version_by_pk(&self, space_id: u32, index_id: u32) -> Result<u64, SbroadError> {
        get_index_version_by_pk(space_id, index_id)
    }

    fn get_table_name_and_version(&self, table_id: SpaceId) -> Result<(SmolStr, u64), SbroadError> {
        get_table_name_and_version(table_id)
    }
}

impl Router for RouterRuntime {
    type ParseTree = AbstractSyntaxTree;
    type MetadataProvider = RouterMetadata;
    type VshardImplementor = Tier;

    fn metadata(&self) -> &impl MutexLike<Self::MetadataProvider> {
        &self.metadata
    }

    fn with_admin_su<T>(&self, f: impl FnOnce() -> T) -> Result<T, SbroadError> {
        with_su(ADMIN_ID, f).map_err(|e| e.into())
    }

    fn new_port<'p>(&self) -> impl Port<'p> {
        PicoPortOwned::new()
    }

    fn materialize_motion(
        &self,
        plan: &mut sql::executor::ir::ExecutionPlan,
        motion_node_id: &NodeId,
        buckets: &sql::executor::bucket::Buckets,
    ) -> Result<sql::executor::vtable::VirtualTable, SbroadError> {
        materialize_motion(self, plan, *motion_node_id, buckets)
    }

    fn dispatch<'p>(
        &self,
        plan: &mut sql::executor::ir::ExecutionPlan,
        top_id: NodeId,
        buckets: &sql::executor::bucket::Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        dispatch_impl(self, plan, top_id, buckets, port)
    }

    fn explain_format(&self, explain: SmolStr) -> Result<Box<dyn std::any::Any>, SbroadError> {
        explain_format(&explain)
    }

    fn extract_sharding_key_from_map<'rec>(
        &self,
        space: SmolStr,
        args: &'rec HashMap<SmolStr, Value>,
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_key_from_map(&*self.metadata().lock(), &space, args)
    }

    fn extract_sharding_key_from_tuple<'rec>(
        &self,
        space: SmolStr,
        args: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_key_from_tuple(&*self.metadata().lock(), &space, args)
    }

    fn get_current_tier_name(&self) -> Result<Option<SmolStr>, SbroadError> {
        Ok(Some(SmolStr::new_static(get_current_tier_name()?)))
    }

    fn get_vshard_object_by_tier(
        &self,
        tier_name: Option<&SmolStr>,
    ) -> Result<Self::VshardImplementor, SbroadError> {
        if let Some(tier_name) = tier_name {
            get_tier_info(tier_name)
        } else {
            get_tier_info(get_current_tier_name()?)
        }
    }

    fn materialize_values(
        &self,
        exec_plan: &mut ExecutionPlan,
        values_id: NodeId,
    ) -> Result<VirtualTable, SbroadError> {
        materialize_values(self, exec_plan, values_id)
    }

    fn is_audit_enabled(&self, plan: &Plan) -> Result<bool, SbroadError> {
        (self.is_audit_enabled_func)(plan)
    }

    fn is_sql_log_enabled(&self, plan: &Plan) -> Result<bool, SbroadError> {
        let config_enabled = crate::config::DYNAMIC_CONFIG
            .sql_log
            .try_current_value()
            .unwrap_or(crate::config::DEFAULT_SQL_LOG);
        // Do not want to log passwords from ACL.
        Ok(config_enabled && !plan.is_acl()?)
    }

    fn get_scheduler_options(&self) -> SchedulerOptions {
        scheduler_options()
    }
}

pub(crate) fn calculate_bucket_id(tuple: &[&Value], bucket_count: u64) -> Result<u64, SbroadError> {
    let wrapped_tuple = tuple
        .iter()
        .map(|v| MsgPackValue::from(*v))
        .collect::<Vec<_>>();
    let tnt_tuple = Tuple::new(&wrapped_tuple).map_err(|e| {
        SbroadError::FailedTo(
            Action::Create,
            Some(Entity::Tuple),
            format_smolstr!("{e:?}"),
        )
    })?;
    let mut key_parts = Vec::with_capacity(tuple.len());
    for (pos, value) in tuple.iter().enumerate() {
        let pos = u32::try_from(pos).map_err(|_| {
            SbroadError::FailedTo(
                Action::Create,
                Some(Entity::KeyDef),
                "Tuple is too long".to_smolstr(),
            )
        })?;
        key_parts.push(value.as_key_def_part(pos));
    }
    let key = KeyDef::new(key_parts.as_slice()).map_err(|e| {
        SbroadError::FailedTo(
            Action::Create,
            Some(Entity::KeyDef),
            format_smolstr!("{e:?}"),
        )
    })?;
    Ok(u64::from(key.hash(&tnt_tuple)) % bucket_count + 1)
}

impl Vshard for Tier {
    fn exec_ir_on_buckets<'p>(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        let tier = self.name();
        bucket_dispatch(
            port,
            self,
            sub_plan,
            buckets,
            tier.as_ref().map(|s| s.as_str()),
        )?;
        Ok(())
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

    fn exec_ir_on_any_node<'p>(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        let runtime = StorageRuntime::new();
        runtime.exec_ir_on_any_node(sub_plan, buckets, port)?;
        Ok(())
    }
}

impl Vshard for &Tier {
    fn bucket_count(&self) -> u64 {
        self.bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError> {
        calculate_bucket_id(s, self.bucket_count())
    }

    fn exec_ir_on_buckets<'p>(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        let tier = self.name();
        bucket_dispatch(
            port,
            self,
            sub_plan,
            buckets,
            tier.as_ref().map(|s| s.as_str()),
        )?;
        Ok(())
    }

    fn exec_ir_on_any_node<'p>(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
        port: &mut impl Port<'p>,
    ) -> Result<(), SbroadError> {
        let runtime = StorageRuntime::new();
        runtime.exec_ir_on_any_node(sub_plan, buckets, port)?;
        Ok(())
    }
}

/// Router runtime configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::module_name_repetitions)]
pub struct RouterMetadata {
    /// Execute response waiting timeout in seconds.
    pub waiting_timeout: u64,

    /// Query cache capacity.
    pub cache_capacity: usize,

    /// Bucket column name.
    pub sharding_column: String,

    /// IR functions
    pub functions: HashMap<SmolStr, Function>,
}

impl Default for RouterMetadata {
    fn default() -> Self {
        Self::new()
    }
}

pub const DEFAULT_QUERY_TIMEOUT: u64 = 360;

impl RouterMetadata {
    #[must_use]
    pub fn new() -> Self {
        let builtins = get_builtin_functions();
        let mut functions = HashMap::with_capacity(builtins.len());
        for f in builtins {
            functions.insert(f.name.clone(), f.clone());
        }

        RouterMetadata {
            waiting_timeout: DEFAULT_QUERY_TIMEOUT,
            cache_capacity: DEFAULT_CAPACITY,
            sharding_column: DEFAULT_BUCKET_COLUMN.to_string(),
            functions,
        }
    }
}

impl Metadata for RouterMetadata {
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    fn table(&self, table_name: &str) -> Result<Table, SbroadError> {
        let name = table_name.to_smolstr();
        let storage = Catalog::try_get(false).expect("storage should be initialized");

        // // Get the space columns and engine of the space from global metatable.
        let table = storage
            .pico_table
            .by_name(&name)?
            .ok_or_else(|| SbroadError::NotFound(Entity::Space, name.to_smolstr()))?;

        let engine = table.engine;
        let mut columns: Vec<Column> = Vec::with_capacity(table.format.len());
        for column_meta in &table.format {
            let col_name = &column_meta.name;
            let is_nullable = column_meta.is_nullable;
            let col_type = UnrestrictedType::new(column_meta.field_type.as_str())?;
            let role = if col_name == DEFAULT_BUCKET_COLUMN {
                ColumnRole::Sharding
            } else {
                ColumnRole::User
            };
            let column = Column {
                name: col_name.to_smolstr(),
                r#type: DerivedType::new(col_type),
                role,
                is_nullable,
            };
            columns.push(column);
        }

        let pk_cols = space_pk_columns(&name, &columns)?;
        let pk_cols_str: &[&str] = &pk_cols.iter().map(SmolStr::as_str).collect::<Vec<_>>();

        let is_system_table = storage::SYSTEM_TABLES_ID_RANGE.contains(&table.id);
        if is_system_table {
            return Table::new_system(table.id, &name, columns, pk_cols_str);
        }

        // Try to find the sharding columns of the space in "_pico_table".
        // If nothing found then the space is local and we can't query it with
        // distributed SQL.
        match table.distribution {
            Distribution::Global => Table::new_global(table.id, &name, columns, pk_cols_str),
            Distribution::ShardedImplicitly {
                sharding_key,
                sharding_fn,
                tier: tier_name,
            } => {
                if !matches!(sharding_fn, ShardingFn::Murmur3) {
                    return Err(SbroadError::NotImplemented(
                        Entity::Distribution,
                        format_smolstr!("by hash function {sharding_fn}"),
                    ));
                }

                let tier = Some(tier_name.to_smolstr());
                let sharding_key_cols = sharding_key
                    .iter()
                    .map(|field| field.to_smolstr())
                    .collect::<Vec<_>>();

                let sharding_key_cols = sharding_key_cols
                    .iter()
                    .map(SmolStr::as_str)
                    .collect::<Vec<_>>();

                Table::new_sharded_in_tier(
                    table.id,
                    &name,
                    columns,
                    &sharding_key_cols,
                    pk_cols_str,
                    engine.into(),
                    tier,
                )
            }
            Distribution::ShardedByField { field, .. } => Err(SbroadError::NotImplemented(
                Entity::Distribution,
                format_smolstr!("explicitly by field '{field}'"),
            )),
        }
    }

    fn get_index_id(&self, index_name: &str, table_name: &str) -> Result<u32, SbroadError> {
        get_index_id(index_name, table_name)
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

fn bucket_dispatch<'p>(
    port: &mut impl Port<'p>,
    runtime: &impl Vshard,
    ex_plan: ExecutionPlan,
    buckets: &Buckets,
    tier: Option<&str>,
) -> Result<(), SbroadError> {
    if let Buckets::Filtered(bucket_set) = buckets {
        if bucket_set.is_empty() {
            empty_plan_write(port, &ex_plan)?;
            return Ok(());
        }
    }
    let timeout = DEFAULT_QUERY_TIMEOUT;
    if !ex_plan.has_segmented_tables() && !ex_plan.has_customization_opcodes() {
        single_plan_dispatch(port, ex_plan, buckets, timeout, tier)?;
    } else {
        custom_plan_dispatch(port, runtime, ex_plan, buckets, timeout, tier)?;
    }
    Ok(())
}
