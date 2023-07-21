//! Storage runtime of the clusterwide SQL.
//! Implements the `sbroad` crate infrastructure
//! for execution of the dispatched query plan subtrees.

use sbroad::debug;
use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::helpers::storage::meta::StorageMetadata;
use sbroad::executor::engine::helpers::storage::runtime::{
    prepare, read_prepared, read_unprepared, unprepare, write_prepared, write_unprepared,
};
use sbroad::executor::engine::helpers::storage::PreparedStmt;
use sbroad::executor::engine::helpers::vshard::get_random_bucket;
use sbroad::executor::engine::helpers::{compile_encoded_optional, execute_dml};
use sbroad::executor::engine::{QueryCache, Vshard};
use sbroad::executor::ir::{ConnectionType, ExecutionPlan, QueryType};
use sbroad::executor::lru::{Cache, LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::protocol::{Binary, RequiredData};
use sbroad::ir::value::Value;
use sbroad::warn;

use std::{any::Any, cell::RefCell, rc::Rc};

use super::{router::calculate_bucket_id, DEFAULT_BUCKET_COUNT};

use tarantool::tuple::Tuple;

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
            QueryType::DML => self.execute_dml(required, raw_optional),
            QueryType::DQL => {
                if required.can_be_cached {
                    self.execute_cacheable_dql(required, raw_optional)
                } else {
                    execute_non_cacheable_dql(required, raw_optional)
                }
            }
        }
    }

    #[allow(unused_variables)]
    fn execute_dml(
        &self,
        required: &mut RequiredData,
        raw_optional: &mut Vec<u8>,
    ) -> Result<Box<dyn Any>, SbroadError> {
        if required.query_type != QueryType::DML {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some("Expected a DML plan.".to_string()),
            ));
        }

        let result = execute_dml(self, raw_optional)?;
        let tuple = Tuple::new(&(result,))
            .map_err(|e| SbroadError::Invalid(Entity::Tuple, Some(format!("{e:?}"))))?;
        Ok(Box::new(tuple) as Box<dyn Any>)
    }

    #[allow(unused_variables)]
    fn execute_cacheable_dql(
        &self,
        required: &mut RequiredData,
        raw_optional: &mut Vec<u8>,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let plan_id = required.plan_id.clone();

        if !required.can_be_cached {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some("Expected a plan that can be cached.".to_string()),
            ));
        }

        // Look for the prepared statement in the cache.
        if let Some(stmt) = self
            .cache
            .try_borrow_mut()
            .map_err(|e| {
                SbroadError::FailedTo(Action::Borrow, Some(Entity::Cache), format!("{e}"))
            })?
            .get(&plan_id)?
        {
            let stmt_id = stmt.id()?;
            // The statement was found in the cache, so we can execute it.
            debug!(
                Option::from("execute plan"),
                &format!("Execute prepared statement: {stmt:?}"),
            );
            let result = match required.query_type {
                QueryType::DML => write_prepared(stmt_id, "", &required.parameters),
                QueryType::DQL => read_prepared(stmt_id, "", &required.parameters),
            };

            // If prepared statement is invalid for some reason, fallback to the long pass
            // and recompile the query.
            if result.is_ok() {
                return result;
            }
        }
        debug!(
            Option::from("execute plan"),
            &format!("Failed to find a plan (id {plan_id}) in the cache."),
        );

        let (pattern_with_params, _tmp_spaces) = compile_encoded_optional(raw_optional)?;
        let result = match prepare(&pattern_with_params.pattern) {
            Ok(stmt) => {
                let stmt_id = stmt.id()?;
                debug!(
                    Option::from("execute plan"),
                    &format!(
                        "Created prepared statement {} for the pattern {}",
                        stmt_id,
                        stmt.pattern()?
                    ),
                );
                self.cache
                    .try_borrow_mut()
                    .map_err(|e| {
                        SbroadError::FailedTo(
                            Action::Put,
                            None,
                            format!("prepared statement {stmt:?} into the cache: {e:?}"),
                        )
                    })?
                    .put(plan_id, stmt)?;
                // The statement was found in the cache, so we can execute it.
                debug!(
                    Option::from("execute plan"),
                    &format!("Execute prepared statement: {stmt_id}"),
                );
                if required.query_type == QueryType::DML {
                    write_prepared(
                        stmt_id,
                        &pattern_with_params.pattern,
                        &pattern_with_params.params,
                    )
                } else {
                    read_prepared(
                        stmt_id,
                        &pattern_with_params.pattern,
                        &pattern_with_params.params,
                    )
                }
            }
            Err(e) => {
                // Possibly the statement is correct, but doesn't fit into
                // Tarantool's prepared statements cache (`sql_cache_size`).
                // So we try to execute it bypassing the cache.
                warn!(
                    Option::from("execute"),
                    &format!(
                        "Failed to prepare the statement: {}, error: {e}",
                        pattern_with_params.pattern
                    ),
                );
                if required.query_type == QueryType::DML {
                    write_unprepared(&pattern_with_params.pattern, &pattern_with_params.params)
                } else {
                    read_unprepared(&pattern_with_params.pattern, &pattern_with_params.params)
                }
            }
        };

        result
    }
}

fn execute_non_cacheable_dql(
    required: &mut RequiredData,
    raw_optional: &mut Vec<u8>,
) -> Result<Box<dyn Any>, SbroadError> {
    if required.can_be_cached || required.query_type != QueryType::DQL {
        return Err(SbroadError::Invalid(
            Entity::Plan,
            Some("Expected a DQL plan that can not be cached.".to_string()),
        ));
    }

    let (pattern_with_params, _tmp_spaces) = compile_encoded_optional(raw_optional)?;
    debug!(
        Option::from("execute"),
        &format!(
            "Failed to execute the statement: {}",
            pattern_with_params.pattern
        ),
    );
    warn!(
        Option::from("execute"),
        &format!("SQL pattern: {}", pattern_with_params.pattern),
    );
    read_unprepared(&pattern_with_params.pattern, &pattern_with_params.params)
}
