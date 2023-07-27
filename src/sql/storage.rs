//! Storage runtime of the clusterwide SQL.
//! Implements the `sbroad` crate infrastructure
//! for execution of the dispatched query plan subtrees.

use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::helpers::storage::meta::StorageMetadata;
use sbroad::executor::engine::helpers::storage::runtime::unprepare;
use sbroad::executor::engine::helpers::storage::PreparedStmt;
use sbroad::executor::engine::helpers::vshard::get_random_bucket;
use sbroad::executor::engine::helpers::{self};
use sbroad::executor::engine::{QueryCache, Vshard};
use sbroad::executor::ir::{ConnectionType, ExecutionPlan, QueryType};
use sbroad::executor::lru::{Cache, LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::protocol::{Binary, RequiredData};
use sbroad::ir::value::Value;

use std::{any::Any, cell::RefCell, rc::Rc};

use super::{router::calculate_bucket_id, DEFAULT_BUCKET_COUNT};

thread_local!(
    static STATEMENT_CACHE: Rc<RefCell<LRUCache<String, PreparedStmt>>> = Rc::new(
        RefCell::new(LRUCache::new(DEFAULT_CAPACITY, Some(Box::new(unprepare))).unwrap())
    )
);

#[allow(clippy::module_name_repetitions)]
pub struct StorageRuntime {
    pub metadata: RefCell<StorageMetadata>,
    bucket_count: u64,
    cache: Rc<RefCell<LRUCache<String, PreparedStmt>>>,
}

impl QueryCache for StorageRuntime {
    type Cache = LRUCache<String, PreparedStmt>;

    fn cache(&self) -> &RefCell<Self::Cache> {
        &self.cache
    }

    fn cache_capacity(&self) -> Result<usize, SbroadError> {
        Ok(self
            .cache()
            .try_borrow()
            .map_err(|e| {
                SbroadError::FailedTo(Action::Borrow, Some(Entity::Cache), format!("{e:?}"))
            })?
            .capacity())
    }

    fn clear_cache(&self) -> Result<(), SbroadError> {
        *self.cache.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Clear, Some(Entity::Cache), format!("{e:?}"))
        })? = Self::Cache::new(DEFAULT_CAPACITY, None)?;
        Ok(())
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
            Some("exec_ir_on_all is not supported on the storage".to_string()),
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
            Some("exec_ir_on_some is not supported on the storage".to_string()),
        ))
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
        match required.query_type {
            QueryType::DML => helpers::execute_dml(self, required, raw_optional),
            QueryType::DQL => {
                if required.can_be_cached {
                    helpers::execute_cacheable_dql_with_raw_optional(self, required, raw_optional)
                } else {
                    helpers::execute_non_cacheable_dql_with_raw_optional(
                        raw_optional,
                        required.options.vtable_max_rows,
                        std::mem::take(&mut required.options.execute_options),
                    )
                }
            }
        }
    }
}
