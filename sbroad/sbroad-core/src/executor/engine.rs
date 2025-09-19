//! Coordinator module.
//!
//! Traits that define an execution engine interface.

use base64ct::{Base64, Encoding};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use tarantool::tuple::Tuple;

use crate::frontend::sql::get_real_function_name;
use crate::ir::node::NodeId;
use crate::ir::types::DerivedType;
use crate::utils::MutexLike;
use std::any::Any;

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::ir::ExecutionPlan;
use crate::executor::protocol::SchemaInfo;
use crate::executor::vtable::VirtualTable;
use crate::ir::function::Function;
use crate::ir::relation::Table;
use crate::ir::types::UnrestrictedType;
use crate::ir::value::Value;

use tarantool::msgpack;

use super::result::ProducerResult;

use std::hash::{DefaultHasher, Hash, Hasher};
use tarantool::space::SpaceId;
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
    static BUILTINS: OnceLock<Vec<Function>> = OnceLock::new();

    BUILTINS.get_or_init(|| {
        vec![
            // stable functions
            Function::new_stable(
                get_real_function_name("version")
                    .expect("shouldn't fail")
                    .into(),
                DerivedType::new(UnrestrictedType::String),
                false,
            ),
            Function::new_stable(
                "to_date".into(),
                DerivedType::new(UnrestrictedType::Datetime),
                false,
            ),
            Function::new_stable(
                "to_char".into(),
                DerivedType::new(UnrestrictedType::String),
                false,
            ),
            Function::new_stable(
                "substring".into(),
                DerivedType::new(UnrestrictedType::String),
                false,
            ),
            // stable system functions
            Function::new_stable(
                "substr".into(),
                DerivedType::new(UnrestrictedType::String),
                true,
            ),
            Function::new_stable(
                "lower".into(),
                DerivedType::new(UnrestrictedType::String),
                true,
            ),
            Function::new_stable(
                "upper".into(),
                DerivedType::new(UnrestrictedType::String),
                true,
            ),
            Function::new_stable(
                "coalesce".into(),
                DerivedType::new(UnrestrictedType::Any),
                true,
            ),
            Function::new_stable(
                "abs".into(),
                DerivedType::new(UnrestrictedType::Any), // any numeric type
                true,
            ),
            // volatile functions
            Function::new_volatile(
                // TODO: deprecated, remove in future version
                get_real_function_name("instance_uuid")
                    .expect("shouldn't fail")
                    .into(),
                // TODO: use `Type::UUID`, to learn more see
                // <https://git.picodata.io/core/picodata/-/issues/2027>
                DerivedType::new(UnrestrictedType::String), // TODO: use `Type::UUID`
                false,
            ),
            Function::new_volatile(
                get_real_function_name("pico_instance_uuid")
                    .expect("shouldn't fail")
                    .into(),
                DerivedType::new(UnrestrictedType::String), // TODO: use `Type::UUID`
                false,
            ),
            Function::new_volatile(
                get_real_function_name("pico_raft_leader_uuid")
                    .expect("shouldn't fail")
                    .into(),
                DerivedType::new(UnrestrictedType::String), // TODO: use `Type::UUID`
                false,
            ),
            Function::new_volatile(
                get_real_function_name("pico_raft_leader_id")
                    .expect("shouldn't fail")
                    .into(),
                DerivedType::new(UnrestrictedType::Integer),
                false,
            ),
        ]
    })
}

pub trait StorageCache {
    /// Put the prepared statement with given key in cache,
    /// remembering its version.
    fn put(
        &mut self,
        plan_id: SmolStr,
        stmt: Statement,
        schema_info: &SchemaInfo,
        motion_ids: Vec<NodeId>,
    ) -> Result<(), SbroadError>;

    /// Get the prepared statement and a list of motion ids from cache.
    /// If the schema version for some virtual table (corresponding to some Motion)
    /// has been changed, `None` is returned.
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

    /// Return current schema version of given table.
    ///
    /// Must be called only if `provides_versions` returns
    /// `true`.
    ///
    /// # Errors
    /// - table was not found in system space
    /// - could not access the system space
    fn get_table_version_by_id(&self, _: SpaceId) -> Result<u64, SbroadError>;
}

/// Compute a query cache key from the query pattern and parameter types.
/// Parameter types affect column types and query validity, so they must be included.
/// Some parameter types can be left unspecified. Such parameters will be inferred during
/// type analysis and they are uniquely determined by the query and initial parameters.
#[inline]
#[must_use]
pub fn query_id(pattern: &str, params: &[DerivedType]) -> SmolStr {
    let params_hash = {
        let mut hasher = DefaultHasher::new();
        params.hash(&mut hasher);
        hasher.finish()
    };

    let mut hasher = blake3::Hasher::new();
    hasher.update(pattern.as_bytes());
    hasher.update(&params_hash.to_ne_bytes());
    let hash = hasher.finalize();
    Base64::encode_string(hash.to_hex().as_bytes()).to_smolstr()
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
                    let data = msgpack::encode(&wrapped);
                    Box::new(Tuple::try_from_slice(&data).map_err(|e| {
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
                let wrapped = msgpack::decode::<Vec<ProducerResult>>(self.data()).map_err(|e| {
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

    fn with_admin_su<T>(&self, f: impl FnOnce() -> T) -> Result<T, SbroadError>;

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

    /// Determines whether audit logging should be performed for the given query plan.
    ///
    /// This function evaluates the query plan against configured audit policies
    /// of current user to decide if the operation requires audit trail generation.
    fn is_audit_enabled(&self, plan: &crate::ir::Plan) -> Result<bool, SbroadError>;
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
