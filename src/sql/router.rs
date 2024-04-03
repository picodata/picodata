//! Router runtime module for the clusterwide SQL.
//! Implements infrastructure to build a distributed
//! query plan and dispatch it to the storage nodes.

use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::helpers::vshard::{
    exec_ir_on_all_buckets, exec_ir_on_some_buckets, get_random_bucket,
};
use sbroad::executor::engine::helpers::{dispatch_impl, explain_format, materialize_motion};
use sbroad::executor::engine::helpers::{sharding_key_from_map, sharding_key_from_tuple};
use sbroad::executor::engine::{get_builtin_functions, QueryCache, Router, Vshard};
use sbroad::executor::ir::{ConnectionType, ExecutionPlan, QueryType};
use sbroad::executor::lru::{Cache, EvictFn, LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::protocol::Binary;
use sbroad::frontend::sql::ast::AbstractSyntaxTree;
use sbroad::ir::value::{MsgPackValue, Value};
use sbroad::ir::Plan;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};

use std::any::Any;
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use crate::sql::DEFAULT_BUCKET_COUNT;

use crate::schema::{Distribution, ShardingFn};
use crate::storage::{Clusterwide, ClusterwideTable};

use sbroad::executor::engine::helpers::storage::meta::{
    DEFAULT_JAEGER_AGENT_HOST, DEFAULT_JAEGER_AGENT_PORT,
};
use sbroad::executor::engine::helpers::{
    normalize_name_for_space_api, normalize_name_from_schema, normalize_name_from_sql,
};
use sbroad::executor::engine::Metadata;
use sbroad::ir::function::Function;
use sbroad::ir::relation::{space_pk_columns, Column, ColumnRole, Table, Type};

use std::borrow::Cow;

use crate::sql::storage::StorageRuntime;
use crate::traft::node;

use ::tarantool::space::Space;
use ::tarantool::tuple::{KeyDef, Tuple};
use ::tarantool::util::Value as TarantoolValue;

pub type VersionMap = HashMap<SmolStr, u64>;

thread_local! {
    static PLAN_CACHE: Rc<RefCell<PicoRouterCache >> = Rc::new(
        RefCell::new(PicoRouterCache::new(DEFAULT_CAPACITY).unwrap()));
}

pub const DEFAULT_BUCKET_COLUMN: &str = "bucket_id";

/// Get the schema version for the given space.
///
/// # Arguments:
/// * `space_name` - name of the space. The name must not
/// be enclosed in quotes as in sql. If in sql user uses
/// `"t"`, here `t` must be passed.
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
    metadata: RefCell<RouterMetadata>,
    bucket_count: u64,
    ir_cache: Rc<RefCell<PicoRouterCache>>,
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
            metadata: RefCell::new(metadata),
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
    fn new(capacity: usize, evict_fn: Option<EvictFn<Plan>>) -> Result<Self, SbroadError>
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
            let space_name = normalize_name_for_space_api(tbl_name);
            let cached_version = *ir.version_map.get(space_name.as_str()).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Table,
                    format_smolstr!("in version map with name: {}", space_name),
                )
            })?;
            let Some(space_def) = storage_tables.by_name(space_name.as_str()).map_err(|e| {
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

    fn cache(&self) -> &RefCell<Self::Cache> {
        &self.ir_cache
    }

    fn cache_capacity(&self) -> Result<usize, SbroadError> {
        Ok(self
            .ir_cache
            .try_borrow()
            .map_err(|e| {
                SbroadError::FailedTo(Action::Get, Some(Entity::Cache), format_smolstr!("{e:?}"))
            })?
            .capacity())
    }

    fn clear_cache(&self) -> Result<(), SbroadError> {
        *self.ir_cache.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Clear, Some(Entity::Cache), format_smolstr!("{e:?}"))
        })? = Self::Cache::new(self.cache_capacity()?)?;
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

    fn metadata(&self) -> Result<Ref<Self::MetadataProvider>, SbroadError> {
        self.metadata.try_borrow().map_err(|e| {
            SbroadError::FailedTo(
                Action::Get,
                Some(Entity::Metadata),
                format_smolstr!("{e:?}"),
            )
        })
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
    ) -> Result<Box<dyn std::any::Any>, SbroadError> {
        dispatch_impl(self, plan, top_id, buckets)
    }

    fn explain_format(&self, explain: SmolStr) -> Result<Box<dyn std::any::Any>, SbroadError> {
        explain_format(&explain)
    }

    fn extract_sharding_key_from_map<'rec>(
        &self,
        space: SmolStr,
        args: &'rec HashMap<SmolStr, Value>,
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        let metadata = self.metadata.try_borrow().map_err(|e| {
            SbroadError::FailedTo(
                Action::Borrow,
                Some(Entity::Metadata),
                format_smolstr!("{e:?}"),
            )
        })?;
        sharding_key_from_map(&*metadata, &space, args)
    }

    fn extract_sharding_key_from_tuple<'rec>(
        &self,
        space: SmolStr,
        args: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_key_from_tuple(&*self.metadata()?, &space, args)
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
    fn exec_ir_on_all(
        &self,
        required: Binary,
        optional: Binary,
        query_type: QueryType,
        conn_type: ConnectionType,
        vtable_max_rows: u64,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_all_buckets(
            &*self.metadata()?,
            required,
            optional,
            query_type,
            conn_type,
            vtable_max_rows,
        )
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
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_some_buckets(self, sub_plan, buckets)
    }

    fn exec_ir_on_any_node(&self, sub_plan: ExecutionPlan) -> Result<Box<dyn Any>, SbroadError> {
        let runtime = StorageRuntime::new()?;
        runtime.exec_ir_on_any_node(sub_plan)
    }
}

impl Vshard for &RouterRuntime {
    fn exec_ir_on_all(
        &self,
        required: Binary,
        optional: Binary,
        query_type: QueryType,
        conn_type: ConnectionType,
        vtable_max_rows: u64,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_all_buckets(
            &*self.metadata()?,
            required,
            optional,
            query_type,
            conn_type,
            vtable_max_rows,
        )
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
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_some_buckets(*self, sub_plan, buckets)
    }

    fn exec_ir_on_any_node(&self, sub_plan: ExecutionPlan) -> Result<Box<dyn Any>, SbroadError> {
        let runtime = StorageRuntime::new()?;
        runtime.exec_ir_on_any_node(sub_plan)
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

    /// Jaeger agent host.
    pub jaeger_agent_host: &'static str,

    /// Jaeger agent port.
    pub jaeger_agent_port: u16,

    /// IR functions
    pub functions: HashMap<SmolStr, Function>,
}

impl Default for RouterMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl RouterMetadata {
    #[must_use]
    pub fn new() -> Self {
        let builtins = get_builtin_functions();
        let mut functions = HashMap::with_capacity(builtins.len());
        for f in builtins {
            functions.insert(f.name.clone(), f.clone());
        }

        RouterMetadata {
            waiting_timeout: 360,
            cache_capacity: DEFAULT_CAPACITY,
            jaeger_agent_host: DEFAULT_JAEGER_AGENT_HOST,
            jaeger_agent_port: DEFAULT_JAEGER_AGENT_PORT,
            sharding_column: DEFAULT_BUCKET_COLUMN.to_string(),
            functions,
        }
    }

    fn get_shard_cols(
        name: &str,
        meta: &tarantool::space::Metadata,
    ) -> Result<Vec<SmolStr>, SbroadError> {
        let storage = Clusterwide::try_get(false).expect("storage should be initialized");
        let table_def = storage
            .tables
            .get(meta.id)
            .map_err(|e| {
                SbroadError::FailedTo(
                    Action::Get,
                    Some(Entity::ShardingKey),
                    format_smolstr!("space id {}: {e}", meta.id),
                )
            })?
            .ok_or_else(|| SbroadError::NotFound(Entity::ShardingKey, name.to_smolstr()))?;
        let shard_cols: Vec<SmolStr> = match &table_def.distribution {
            Distribution::Global => {
                vec![]
            }
            Distribution::ShardedImplicitly {
                sharding_key,
                sharding_fn,
            } => {
                if !matches!(sharding_fn, ShardingFn::Murmur3) {
                    return Err(SbroadError::NotImplemented(
                        Entity::Distribution,
                        format_smolstr!("by hash function {sharding_fn}"),
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
                    format_smolstr!("explicitly by field '{field}'"),
                ));
            }
        };
        Ok(shard_cols)
    }
}

impl Metadata for RouterMetadata {
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    fn table(&self, table_name: &str) -> Result<Table, SbroadError> {
        let name = normalize_name_for_space_api(table_name);

        // // Get the space columns and engine of the space.
        let space = Space::find(&name)
            .ok_or_else(|| SbroadError::NotFound(Entity::Space, name.to_smolstr()))?;
        let meta = space.meta().map_err(|e| {
            SbroadError::FailedTo(Action::Get, Some(Entity::SpaceMetadata), e.to_smolstr())
        })?;
        let engine = meta.engine;
        let mut columns: Vec<Column> = Vec::with_capacity(meta.format.len());
        for column_meta in &meta.format {
            let name_value = column_meta.get(&Cow::from("name")).ok_or_else(|| {
                SbroadError::FailedTo(
                    Action::Get,
                    Some(Entity::SpaceMetadata),
                    format_smolstr!("column name not found in the space format: {column_meta:?}"),
                )
            })?;
            let col_name = if let TarantoolValue::Str(name) = name_value {
                name
            } else {
                return Err(SbroadError::FailedTo(
                    Action::Get,
                    Some(Entity::SpaceMetadata),
                    format_smolstr!("column name is not a string: {name_value:?}"),
                ));
            };
            let is_nullable_value =
                column_meta.get(&Cow::from("is_nullable")).ok_or_else(|| {
                    SbroadError::FailedTo(
                        Action::Get,
                        Some(Entity::SpaceMetadata),
                        format_smolstr!(
                            "column nullability attribute was not found in the space metadata: {column_meta:?}"
                        ),
                    )
                })?;
            let is_nullable = if let TarantoolValue::Bool(is_nullable) = is_nullable_value {
                *is_nullable
            } else {
                return Err(SbroadError::FailedTo(
                    Action::Get,
                    Some(Entity::SpaceMetadata),
                    format_smolstr!(
                        "column nullability attribute is not a boolean: {is_nullable_value:?}"
                    ),
                ));
            };
            let type_value = column_meta.get(&Cow::from("type")).ok_or_else(|| {
                SbroadError::FailedTo(
                    Action::Get,
                    Some(Entity::SpaceMetadata),
                    format_smolstr!("column type not found in the space format: {column_meta:?}"),
                )
            })?;
            let col_type: Type = if let TarantoolValue::Str(col_type) = type_value {
                Type::new(col_type)?
            } else {
                return Err(SbroadError::FailedTo(
                    Action::Get,
                    Some(Entity::SpaceMetadata),
                    format_smolstr!("column type is not a string: {type_value:?}"),
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
                is_nullable,
            };
            columns.push(column);
        }

        let normalized_name = normalize_name_from_sql(table_name);
        let pk_cols = space_pk_columns(&name, &columns)?;
        let pk_cols_str: &[&str] = &pk_cols.iter().map(SmolStr::as_str).collect::<Vec<_>>();

        // Try to find the sharding columns of the space in "_pico_table".
        // If nothing found then the space is local and we can't query it with
        // distributed SQL.
        let is_system_table = ClusterwideTable::values()
            .iter()
            .any(|sys_name| *sys_name == name.as_str());

        if is_system_table {
            return Table::new_system(&normalized_name, columns, pk_cols_str);
        }
        let sharded_columns = Self::get_shard_cols(&name, &meta)?;
        if sharded_columns.is_empty() {
            return Table::new_global(&normalized_name, columns, pk_cols_str);
        }
        let sharding_columns_str: &[&str] = &sharded_columns
            .iter()
            .map(SmolStr::as_str)
            .collect::<Vec<_>>();
        Table::new_sharded(
            &normalized_name,
            columns,
            sharding_columns_str,
            pk_cols_str,
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
    fn sharding_key_by_space(&self, space: &str) -> Result<Vec<SmolStr>, SbroadError> {
        let table = self.table(space)?;
        table.get_sharding_column_names()
    }

    fn sharding_positions_by_space(&self, space: &str) -> Result<Vec<usize>, SbroadError> {
        let table = self.table(space)?;
        Ok(table.get_sk()?.to_vec())
    }
}
