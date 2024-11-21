//! Coordinator module.
//!
//! Traits that define an execution engine interface.

use base64ct::{Base64, Encoding};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use tarantool::tuple::Tuple;

use crate::cbo::histogram::Scalar;
use crate::cbo::{ColumnStats, TableColumnPair, TableStats};
use crate::ir::node::NodeId;
use crate::utils::MutexLike;
use std::any::Any;

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::OnceLock;

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::ir::ExecutionPlan;
use crate::executor::protocol::SchemaInfo;
use crate::executor::vtable::VirtualTable;
use crate::ir::function::Function;
use crate::ir::relation::Table;
use crate::ir::relation::Type;
use crate::ir::value::Value;

use super::result::ProducerResult;

use tarantool::sql::Statement;

pub mod helpers;
#[cfg(feature = "mock")]
pub mod mock;

/// A metadata trait of the cluster (getters for tables, functions, etc.).
pub trait Metadata: Sized {
    /// Get a table by normalized name that contains:
    /// * list of the columns,
    /// * distribution key of the output tuples (column positions),
    /// * table name.
    ///
    /// # Errors
    /// - Failed to get table by name from the metadata.
    fn table(&self, table_name: &str) -> Result<Table, SbroadError>;

    /// Lookup for a function in the metadata cache.
    ///
    /// # Errors
    /// - Failed to get function by name from the metadata.
    fn function(&self, fn_name: &str) -> Result<&Function, SbroadError>;

    /// Get the wait timeout for the query execution.
    fn waiting_timeout(&self) -> u64;

    /// Get the name of the sharding column (usually it is `bucket_id`).
    fn sharding_column(&self) -> &str;

    /// Provides vector of the sharding key column names or an error
    ///
    /// # Errors
    /// - Metadata does not contain space
    /// - Metadata contains incorrect sharding key format
    fn sharding_key_by_space(&self, space: &str) -> Result<Vec<SmolStr>, SbroadError>;

    /// Provides vector of the sharding key column positions in a tuple or an error
    ///
    /// # Errors
    /// - Metadata does not contain space
    fn sharding_positions_by_space(&self, space: &str) -> Result<Vec<usize>, SbroadError>;
}

pub fn get_builtin_functions() -> &'static [Function] {
    // Once lock is used because of concurrent access in tests.
    static mut BUILTINS: OnceLock<Vec<Function>> = OnceLock::new();

    unsafe {
        BUILTINS.get_or_init(|| {
            vec![
                Function::new_stable("to_date".into(), Type::Datetime, false),
                Function::new_stable("to_char".into(), Type::String, false),
                Function::new_stable("substr".into(), Type::String, true),
                Function::new_stable("lower".into(), Type::String, true),
                Function::new_stable("upper".into(), Type::String, true),
                Function::new_stable("coalesce".into(), Type::Scalar, true),
            ]
        })
    }
}

pub trait StorageCache {
    /// Put the prepared statement with given key in cache,
    /// remembering its version.
    ///
    /// # Errors
    /// - failed to get schema version for some table
    /// - invalid `schema_info`
    fn put(
        &mut self,
        plan_id: SmolStr,
        stmt: Statement,
        schema_info: &SchemaInfo,
        table_ids: Vec<NodeId>,
    ) -> Result<(), SbroadError>;

    /// Get the prepared statement and a list of temporary tables from cache.
    /// If the schema version for some table has been changed, `None` is returned.
    ///
    /// # Errors
    /// - failed to get schema version for some table
    #[allow(clippy::ptr_arg)]
    fn get(&mut self, plan_id: &SmolStr) -> Result<Option<(&Statement, &[NodeId])>, SbroadError>;

    /// Clears the cache.
    ///
    /// # Errors
    /// - internal errors from implementation
    fn clear(&mut self) -> Result<(), SbroadError>;
}

pub type TableVersionMap = HashMap<SmolStr, u64>;

pub trait QueryCache {
    type Cache;
    type Mutex: MutexLike<Self::Cache> + Sized;

    /// Get the cache.
    ///
    /// # Errors
    /// - Failed to get the cache.
    fn cache(&self) -> &Self::Mutex;

    /// Get the cache capacity.
    ///
    /// # Errors
    /// - Failed to get the cache capacity.
    fn cache_capacity(&self) -> Result<usize, SbroadError>;

    /// Clear the cache.
    ///
    /// # Errors
    /// - Failed to clear the cache.
    fn clear_cache(&self) -> Result<(), SbroadError>;

    /// `true` if cache can provide a schema version
    /// for given table. Only used for picodata,
    /// cartridge does not need this.
    fn provides_versions(&self) -> bool;

    /// Return current schema version of given table.
    ///
    /// Must be called only if `provides_versions` returns
    /// `true`.
    ///
    /// # Errors
    /// - table was not found in system space
    /// - could not access the system space
    fn get_table_version(&self, _: &str) -> Result<u64, SbroadError>;
}

/// Calculate a key in the query cache for a given pattern.
#[inline]
#[must_use]
pub fn query_id(pattern: &str) -> SmolStr {
    Base64::encode_string(blake3::hash(pattern.as_bytes()).to_hex().as_bytes()).to_smolstr()
}

/// Helper struct specifying in which format
/// DQL subtree dispatch should return data.
#[derive(Clone, PartialEq, Eq)]
pub enum DispatchReturnFormat {
    /// When we executed the last subtree,
    /// we must return result to user in form
    /// of a tuple. This allows to do some optimisations
    /// see `build_final_dql_result`.
    ///
    /// HACK: This is also critical for reading from
    /// system tables. Currently we don't support
    /// arrays or maps in tables, but we still can
    /// allow users to read those values from tables.
    /// This is because read from a system table is
    /// executed on a single node and we don't decode
    /// returned tuple, instead we return it as is.
    Tuple,
    /// Return value as `ProducerResult`. This is used
    /// for non-final subtrees, when we need to create
    /// a virtual table from the result.
    Inner,
}

pub trait ConvertToDispatchResult {
    /// Convert self to specified format
    ///
    /// # Errors
    /// - Implementation errors
    fn convert(self, format: DispatchReturnFormat) -> Result<Box<dyn Any>, SbroadError>;
}

impl ConvertToDispatchResult for ProducerResult {
    fn convert(self, format: DispatchReturnFormat) -> Result<Box<dyn Any>, SbroadError> {
        let res: Box<dyn Any> = match format {
            DispatchReturnFormat::Tuple => {
                let wrapped = vec![self];
                #[cfg(feature = "mock")]
                {
                    Box::new(wrapped)
                }
                #[cfg(not(feature = "mock"))]
                {
                    Box::new(Tuple::new(&wrapped).map_err(|e| {
                        SbroadError::Other(format_smolstr!(
                            "create tuple from producer result: {e}"
                        ))
                    })?)
                }
            }
            DispatchReturnFormat::Inner => Box::new(self),
        };
        Ok(res)
    }
}

impl ConvertToDispatchResult for Tuple {
    fn convert(self, format: DispatchReturnFormat) -> Result<Box<dyn Any>, SbroadError> {
        let res: Box<dyn Any> = match format {
            DispatchReturnFormat::Tuple => Box::new(self),
            DispatchReturnFormat::Inner => {
                let wrapped = self.decode::<Vec<ProducerResult>>().map_err(|e| {
                    SbroadError::FailedTo(
                        Action::Decode,
                        Some(Entity::Tuple),
                        format_smolstr!("into producer result: {e:?}"),
                    )
                })?;
                let res = wrapped.into_iter().next().ok_or_else(|| {
                    SbroadError::Other(
                        "failed to convert tuple into ProducerResult: tuple is empty".into(),
                    )
                })?;
                Box::new(res)
            }
        };
        Ok(res)
    }
}

/// A router trait.
pub trait Router: QueryCache {
    type ParseTree;
    type MetadataProvider: Metadata;
    type VshardImplementor: Vshard;

    /// Get the metadata provider (tables, functions, etc.).
    fn metadata(&self) -> &impl MutexLike<Self::MetadataProvider>;

    /// Setup output format of query explain
    ///
    /// # Errors
    /// - Internal error. Under normal conditions we should always return
    ///   formatted explain successfully.
    fn explain_format(&self, explain: SmolStr) -> Result<Box<dyn Any>, SbroadError>;

    /// Extract a list of the sharding key values from a map for the given space.
    ///
    /// # Errors
    /// - Columns are not present in the sharding key of the space.
    fn extract_sharding_key_from_map<'rec>(
        &self,
        space: SmolStr,
        args: &'rec HashMap<SmolStr, Value>,
    ) -> Result<Vec<&'rec Value>, SbroadError>;

    /// Extract a list of the sharding key values from a tuple for the given space.
    ///
    /// # Errors
    /// - Internal error in the table (should never happen, but we recheck).
    fn extract_sharding_key_from_tuple<'rec>(
        &self,
        space: SmolStr,
        args: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, SbroadError>;

    /// Dispatch a sql query to the shards in cluster and get the results.
    ///
    /// # Errors
    /// - internal executor errors
    fn dispatch(
        &self,
        plan: &mut ExecutionPlan,
        top_id: NodeId,
        buckets: &Buckets,
        return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError>;

    /// Materialize result motion node to virtual table
    ///
    /// # Errors
    /// - internal executor errors
    fn materialize_motion(
        &self,
        plan: &mut ExecutionPlan,
        motion_node_id: &NodeId,
        buckets: &Buckets,
    ) -> Result<VirtualTable, SbroadError>;

    /// Get tier name to which the coordinator belongs
    ///
    /// # Errors
    /// - internal executor errors
    /// - storage errors
    fn get_current_tier_name(&self) -> Result<Option<SmolStr>, SbroadError>;

    /// Get vshard object which is responsible for tier with corresponding name
    ///
    /// # Errors
    /// - internal executor errors
    fn get_vshard_object_by_tier(
        &self,
        tier_name: Option<&SmolStr>,
    ) -> Result<Self::VshardImplementor, SbroadError>;

    /// Get vshard object name to which the coordinator belongs
    ///
    /// # Errors
    /// - internal executor errors
    fn get_current_vshard_object(&self) -> Result<Self::VshardImplementor, SbroadError> {
        self.get_vshard_object_by_tier(self.get_current_tier_name()?.as_ref())
    }

    /// Materialize values (on router).
    /// We have two scenarios of vtable materialization:
    /// 1.) In case we're working with VALUES containing **only** constants, we copy them
    ///     directly into the structure of Vtable.
    /// 2.) In case we met under VALUES smth different from constants we execute local
    ///     SQL on the router and fill the vtable with result.
    ///
    /// # Errors
    /// - Values of inconsistent types met.
    fn materialize_values(
        &self,
        exec_plan: &mut ExecutionPlan,
        values_id: NodeId,
    ) -> Result<VirtualTable, SbroadError>;
}

/// Struct representing table stats that we get directly from replicaset system table.
/// **Note**: `*_counter` fields are specific for each of the replicasets and should not be merged.
/// Node that will make statistics calculations need only the `rows_number` field.
///
/// **Note**: All the statistics structures with a __Storage__ prefix represent structures that we construct
/// on storages and then send to the router node (C structure may look differently).
#[allow(dead_code)]
pub struct StorageTableStats {
    /// Number of rows in the table.
    rows_number: u64,
    /// Counter of executed INSERT operations.
    ///
    /// We need to store counter in order to understand when to
    /// actualize table statistics.
    insert_counter: u64,
    /// Counter of executed UPDATE operations.
    update_counter: u64,
    /// Counter of executed REMOVE operations.
    remove_counter: u64,
}

/// A `CostBased` statistics trait.
pub trait Statistics {
    /// Get `TableStats` for table by its name from storages.
    ///
    /// # Errors
    /// - Low-level statistics retriever error.
    fn get_table_stats(&self, table_name: &str) -> Result<Option<Rc<TableStats>>, SbroadError>;

    /// Get upcasted `ColumnStats` for column by its table name and column name from storages.
    ///
    /// # Errors
    /// - Low-level statistics retriever error.
    fn get_column_stats(
        &self,
        table_column_pair: &TableColumnPair,
    ) -> Result<Option<Rc<Box<dyn Any>>>, SbroadError>;

    /// Update `TableStats` cache with given table statistics.
    ///
    /// # Errors
    /// - Table statistics couldn't be mutually borrowed.
    fn update_table_stats(
        &mut self,
        table_name: SmolStr,
        table_stats: TableStats,
    ) -> Result<(), SbroadError>;

    /// Update `ColumnStats` cache with given initial column statistics.
    ///
    /// # Errors
    /// - Initial column statistics couldn't be mutually borrowed.
    fn update_column_stats<T: Scalar>(
        &self,
        table_column_pair: TableColumnPair,
        column_stats: ColumnStats<T>,
    ) -> Result<(), SbroadError>;
}

pub trait Vshard {
    /// Execute a query on a given buckets.
    ///
    /// # Errors
    /// - Execution errors
    fn exec_ir_on_buckets(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
        return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError>;

    /// Execute query on any node.
    /// All the data needed to execute query
    /// is already in the plan.
    ///
    /// # Errors
    /// - Execution errors
    fn exec_ir_on_any_node(
        &self,
        sub_plan: ExecutionPlan,
        return_format: DispatchReturnFormat,
    ) -> Result<Box<dyn Any>, SbroadError>;

    /// Get the amount of buckets in the cluster.
    fn bucket_count(&self) -> u64;

    /// Get a random bucket from the cluster.
    fn get_random_bucket(&self) -> Buckets;

    /// Determine shard for query execution by sharding key value
    ///
    /// # Errors
    /// - Internal error. Under normal conditions we should always return
    ///   bucket id successfully.
    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError>;
}
