//! Router runtime module for the clusterwide SQL.
//! Implements infrastructure to build a distributed
//! query plan and dispatch it to the storage nodes.

use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::helpers::vshard::{get_random_bucket, impl_exec_ir_on_buckets};
use sbroad::executor::engine::helpers::{dispatch_impl, explain_format, materialize_motion};
use sbroad::executor::engine::helpers::{sharding_key_from_map, sharding_key_from_tuple};
use sbroad::executor::engine::{
    get_builtin_functions, DispatchReturnFormat, QueryCache, Router, Vshard,
};
use sbroad::executor::ir::ExecutionPlan;
use sbroad::executor::lru::{Cache, EvictFn, LRUCache, DEFAULT_CAPACITY};
use sbroad::frontend::sql::ast::AbstractSyntaxTree;
use sbroad::ir::value::{MsgPackValue, Value};
use sbroad::ir::Plan;
use sbroad::utils::MutexLike;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use tarantool::fiber::Mutex;

use std::any::Any;

use std::collections::HashMap;
use std::rc::Rc;

use crate::sql::DEFAULT_BUCKET_COUNT;

use crate::schema::{Distribution, ShardingFn};
use crate::storage::{Clusterwide, ClusterwideTable};

use sbroad::executor::engine::helpers::normalize_name_from_sql;
use sbroad::executor::engine::Metadata;
use sbroad::ir::function::Function;
use sbroad::ir::relation::{space_pk_columns, Column, ColumnRole, Table, Type};

use crate::sql::storage::StorageRuntime;
use crate::traft::node;

use ::tarantool::tuple::{KeyDef, Tuple};

pub type VersionMap = HashMap<SmolStr, u64>;

thread_local! {
    static PLAN_CACHE: Rc<Mutex<PicoRouterCache>> = Rc::new(
        Mutex::new(PicoRouterCache::new(DEFAULT_CAPACITY).unwrap()));
}

pub const DEFAULT_BUCKET_COLUMN: &str = "bucket_id";

/// Get the schema version for the given space.
///
/// # Arguments:
/// * `space_name` - name of the space. The name must not
///   be enclosed in quotes as in sql. If in sql user uses
///   `"t"`, here `t` must be passed.
///
/// # Errors:
/// - errors on access to system space
/// - space with given name not found
pub fn get_table_version(space_name: &str) -> Result<u64, SbroadError> {
    let node = node::global().map_err(|e| {
        SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
    })?;
    let storage_tables = &node.storage.tables;
    if let Some(space_def) = storage_tables.by_name(space_name).map_err(|e| {
        SbroadError::FailedTo(Action::Get, None, format_smolstr!("space_def: {}", e))
    })? {
        Ok(space_def.schema_version)
    } else {
        Err(SbroadError::NotFound(
            Entity::SpaceMetadata,
            format_smolstr!("for space: {}", space_name),
        ))
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct RouterRuntime {
    metadata: Mutex<RouterMetadata>,
    bucket_count: u64,
    ir_cache: Rc<Mutex<PicoRouterCache>>,
}

impl RouterRuntime {
    /// Build a new router runtime.
    ///
    /// # Errors
    /// - If the cache cannot be initialized.
    pub fn new() -> Result<Self, SbroadError> {
        let metadata = RouterMetadata::default();
        let bucket_count = DEFAULT_BUCKET_COUNT;
        let runtime = PLAN_CACHE.with(|cache| RouterRuntime {
            metadata: Mutex::new(metadata),
            bucket_count,
            ir_cache: cache.clone(),
        });
        Ok(runtime)
    }
}

pub type PlanCache = LRUCache<SmolStr, Plan>;

/// Wrapper around default LRU cache, that
/// checks schema version.
pub struct PicoRouterCache {
    inner: PlanCache,
}

impl PicoRouterCache {
    pub fn new(capacity: usize) -> Result<Self, SbroadError> {
        Ok(PicoRouterCache {
            inner: PlanCache::new(capacity, None)?,
        })
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}

impl Cache<SmolStr, Plan> for PicoRouterCache {
    fn new(capacity: usize, evict_fn: Option<EvictFn<SmolStr, Plan>>) -> Result<Self, SbroadError>
    where
        Self: Sized,
    {
        Ok(PicoRouterCache {
            inner: PlanCache::new(capacity, evict_fn)?,
        })
    }

    fn get(&mut self, key: &SmolStr) -> Result<Option<&Plan>, SbroadError> {
        let Some(ir) = self.inner.get(key)? else {
            return Ok(None);
        };
        // check Plan's tables have up to date schema
        let node = node::global().map_err(|e| {
            SbroadError::FailedTo(Action::Get, None, format_smolstr!("raft node: {}", e))
        })?;
        let storage_tables = &node.storage.tables;
        for (tbl_name, tbl) in &ir.relations.tables {
            if tbl.is_system() {
                continue;
            }
            let cached_version = *ir.version_map.get(tbl_name.as_str()).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Table,
                    format_smolstr!("in version map with name: {}", tbl_name),
                )
            })?;
            let Some(space_def) = storage_tables.by_name(tbl_name.as_str()).map_err(|e| {
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
        Ok(Some(ir))
    }

    fn put(&mut self, key: SmolStr, value: Plan) -> Result<(), SbroadError> {
        self.inner.put(key, value)
    }

    fn clear(&mut self) -> Result<(), SbroadError> {
        self.inner.clear()
    }
}

impl QueryCache for RouterRuntime {
    type Cache = PicoRouterCache;
    type Mutex = Mutex<Self::Cache>;

    fn cache(&self) -> &Self::Mutex {
        &self.ir_cache
    }

    fn cache_capacity(&self) -> Result<usize, SbroadError> {
        Ok(self.cache().lock().capacity())
    }

    fn clear_cache(&self) -> Result<(), SbroadError> {
        *self.ir_cache.lock() = Self::Cache::new(self.cache_capacity()?)?;
        Ok(())
    }

    fn provides_versions(&self) -> bool {
        true
    }

    fn get_table_version(&self, space_name: &str) -> Result<u64, SbroadError> {
        get_table_version(space_name)
    }
}

impl Router for RouterRuntime {
    type ParseTree = AbstractSyntaxTree;
    type MetadataProvider = RouterMetadata;

    fn metadata(&self) -> &impl MutexLike<Self::MetadataProvider> {
        &self.metadata
    }

    fn materialize_motion(
        &self,
        plan: &mut sbroad::executor::ir::ExecutionPlan,
        motion_node_id: usize,
        buckets: &sbroad::executor::bucket::Buckets,
    ) -> Result<sbroad::executor::vtable::VirtualTable, SbroadError> {
        materialize_motion(self, plan, motion_node_id, buckets)
    }

    fn dispatch(
        &self,
        plan: &mut sbroad::executor::ir::ExecutionPlan,
        top_id: usize,
        buckets: &sbroad::executor::bucket::Buckets,
        return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn std::any::Any>, SbroadError> {
        dispatch_impl(self, plan, top_id, buckets, return_format)
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

impl Vshard for RouterRuntime {
    fn exec_ir_on_buckets(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
        return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError> {
        impl_exec_ir_on_buckets(self, sub_plan, buckets, return_format)
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

    fn exec_ir_on_any_node(
        &self,
        sub_plan: ExecutionPlan,
        return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let runtime = StorageRuntime::new()?;
        runtime.exec_ir_on_any_node(sub_plan, return_format)
    }
}

impl Vshard for &RouterRuntime {
    fn bucket_count(&self) -> u64 {
        self.bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError> {
        calculate_bucket_id(s, self.bucket_count())
    }

    fn exec_ir_on_buckets(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
        return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError> {
        impl_exec_ir_on_buckets(*self, sub_plan, buckets, return_format)
    }

    fn exec_ir_on_any_node(
        &self,
        sub_plan: ExecutionPlan,
        return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let runtime = StorageRuntime::new()?;
        runtime.exec_ir_on_any_node(sub_plan, return_format)
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
        let storage = Clusterwide::try_get(false).expect("storage should be initialized");

        // // Get the space columns and engine of the space from global metatable.
        let table = storage
            .tables
            .by_name(&name)?
            .ok_or_else(|| SbroadError::NotFound(Entity::Space, name.to_smolstr()))?;

        let engine = table.engine;
        let mut columns: Vec<Column> = Vec::with_capacity(table.format.len());
        for column_meta in &table.format {
            let col_name = &column_meta.name;
            let is_nullable = column_meta.is_nullable;
            let col_type = Type::new(column_meta.field_type.as_str())?;
            let role = if col_name == DEFAULT_BUCKET_COLUMN {
                ColumnRole::Sharding
            } else {
                ColumnRole::User
            };
            let column = Column {
                name: col_name.to_smolstr(),
                r#type: col_type,
                role,
                is_nullable,
            };
            columns.push(column);
        }

        let pk_cols = space_pk_columns(&name, &columns)?;
        let pk_cols_str: &[&str] = &pk_cols.iter().map(SmolStr::as_str).collect::<Vec<_>>();

        // Try to find the sharding columns of the space in "_pico_table".
        // If nothing found then the space is local and we can't query it with
        // distributed SQL.
        let is_system_table = ClusterwideTable::values()
            .iter()
            .any(|sys_name| *sys_name == name.as_str());

        if is_system_table {
            return Table::new_system(&name, columns, pk_cols_str);
        }

        match table.distribution {
            Distribution::Global => Table::new_global(&name, columns, pk_cols_str),
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
