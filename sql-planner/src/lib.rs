//! Tarantool planner and executor for a distributed SQL.

use crate::errors::SbroadError;
use crate::executor::engine::{query_id, Router, VersionMap};
use crate::executor::lru::Cache;

use crate::ir::helpers::RepeatableState;
use crate::ir::options::Options;
use crate::ir::types::{DerivedType, UnrestrictedType};
use crate::ir::value::Value;
use crate::ir::Plan;
use crate::utils::MutexLike;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::rc::Rc;

#[cfg(feature = "mock")]
pub mod helpers;

pub use sql_executor::{backend, executor};
pub use sql_frontend::frontend;
pub use sql_ir::{
    collection, crit, debug, error, fatal, info, system, verbose, warn, write_explain_header1,
    write_explain_header2,
};
pub use sql_ir::{errors, ir, log, utils};

/// A parsed parameterized query. It still has parameter placeholders instead of actual parameter values.
#[derive(Debug)]
pub struct PreparedStatement {
    // This Rc shares the reference with the query cache
    plan: Rc<Plan>,
    /// This is for audit logging of SQL statements.
    query_for_audit: Option<String>,
    /// This is for SQL statement logging.
    query_for_logging: Option<String>,
}

impl PreparedStatement {
    /// Parse an SQL query into an optimized intermediate representation.
    ///
    /// This function will attempt to cache the optimized plan into the router's query plan cache.
    pub fn parse<R>(
        router: &R,
        query_text: &str,
        param_types: &[DerivedType],
    ) -> Result<PreparedStatement, SbroadError>
    where
        R: Router,
        R::Cache: Cache<SmolStr, Rc<Plan>>,
    {
        let mut cache = router.cache().lock();

        // FIXME: converting the hash to string is suboptimal, we could use a newtype over `[u8; HASH_LEN]` directly
        let cache_key = query_id(query_text, param_types);

        if let Some(cached_plan) = router.with_admin_su(|| cache.get(&cache_key))?? {
            let query_for_audit = if router.is_audit_enabled(cached_plan)? {
                Some(query_text.to_string())
            } else {
                None
            };
            let query_for_logging = if router.is_sql_log_enabled(cached_plan)? {
                Some(query_text.to_string())
            } else {
                None
            };
            return Ok(PreparedStatement {
                plan: cached_plan.clone(),
                query_for_audit,
                query_for_logging,
            });
        }

        let new_plan = router.with_admin_su(|| -> Result<Plan, SbroadError> {
            let metadata = router.metadata().lock();

            let mut plan = frontend::sql::transform_into_plan(query_text, param_types, &*metadata)?;

            if router.provides_versions() {
                let mut table_version_map = VersionMap::with_capacity_and_hasher(
                    plan.relations.tables.len(),
                    RepeatableState,
                );
                let mut index_version_map = HashMap::with_capacity_and_hasher(
                    plan.index_version_map.iter().len(),
                    RepeatableState,
                );

                for table in plan.relations.tables.values() {
                    let version = router.get_table_version_by_id(table.id)?;
                    table_version_map.insert(table.id, version);
                }

                for (index_pk, _) in plan.index_version_map.iter() {
                    let version = router.get_index_version_by_pk(index_pk[0], index_pk[1])?;
                    index_version_map.insert(*index_pk, version);
                }

                plan.table_version_map = table_version_map;
                plan.index_version_map = index_version_map;
            }

            plan.optimize_statement()
        })??;

        let new_plan = Rc::new(new_plan);

        // Only DQL and DML is cached, because it is on the hot path
        // other types of queries are much less likely to be queried again
        //
        // EXPLAIN (RAW) queries contain DQL and DML, we don't want them to be cached
        if new_plan.is_dql_or_dml()? && !new_plan.is_raw_explain() {
            cache.put(cache_key, new_plan.clone())?;
        }

        let query_for_audit = if router.is_audit_enabled(&new_plan)? {
            Some(query_text.to_string())
        } else {
            None
        };
        let query_for_logging = if router.is_sql_log_enabled(&new_plan)? {
            Some(query_text.to_string())
        } else {
            None
        };

        Ok(PreparedStatement {
            plan: new_plan,
            query_for_audit,
            query_for_logging,
        })
    }

    /// A shorthand method for [`Plan::collect_parameter_types`]
    pub fn collect_parameter_types(&self) -> Vec<UnrestrictedType> {
        self.plan.collect_parameter_types()
    }

    /// Retrieve the plan IR for this prepared statement.
    pub fn as_plan(&self) -> &Plan {
        &self.plan
    }

    /// Gets the SQL statement text stored for output to the audit log.
    pub fn query_for_audit(&self) -> Option<&str> {
        self.query_for_audit.as_deref()
    }

    /// Gets the SQL statement text stored for output to the SQL log.
    pub fn query_for_logging(&self) -> Option<&str> {
        self.query_for_logging.as_deref()
    }

    /// Provide concrete values for query parameters, creating a [`BoundStatement`] as a result.
    pub fn bind(
        &self,
        params: Vec<Value>,
        default_options: Options,
    ) -> Result<BoundStatement, SbroadError> {
        let params_for_audit = if self.query_for_audit.is_some() {
            // TODO: Try to find a way to avoid this cloning.
            Some(params.clone())
        } else {
            None
        };

        let plan = self
            .plan
            .as_ref()
            .clone()
            .bind_statement(params, default_options)?;

        Ok(BoundStatement {
            plan: Box::new(plan),
            params_for_audit,
        })
    }
}

/// A query with parameter values supplied. This representation is ready for execution.
#[derive(Debug, Clone)]
pub struct BoundStatement {
    // wrap the plan into a `Box` to reduce the size of `BoundStatement`
    // this is important for `pgproto`, which stores it in `PortalState`
    plan: Box<Plan>,
    /// This is for audit logging of SQL statement params.
    params_for_audit: Option<Vec<Value>>,
}

/// Constructors of [`executor::ExecutingQuery`] that start from SQL text or a
/// [`BoundStatement`]. They live in a separate trait because `ExecutingQuery`
/// is defined below the statement types in the crate stack.
pub trait ExecutingQueryExt<'a, C>: Sized
where
    C: Router,
{
    /// Create a query ready for execution from a bound statement.
    fn from_bound_statement(runtime: &'a C, statement: BoundStatement) -> Self;

    /// A shorthand to create a query directly from SQL text.
    /// Equivalent to chaining [`BoundStatement::parse_and_bind`] and
    /// [`ExecutingQueryExt::from_bound_statement`].
    fn from_text_and_params(
        coordinator: &'a C,
        query_text: &str,
        params: Vec<Value>,
    ) -> Result<Self, SbroadError>
    where
        C::Cache: Cache<SmolStr, Rc<Plan>>;
}

impl<'a, C> ExecutingQueryExt<'a, C> for executor::ExecutingQuery<'a, C>
where
    C: Router,
{
    fn from_bound_statement(runtime: &'a C, statement: BoundStatement) -> Self {
        Self::from_plan(runtime, *statement.plan)
    }

    fn from_text_and_params(
        coordinator: &'a C,
        query_text: &str,
        params: Vec<Value>,
    ) -> Result<Self, SbroadError>
    where
        C::Cache: Cache<SmolStr, Rc<Plan>>,
    {
        let bound_statement =
            BoundStatement::parse_and_bind(coordinator, query_text, params, Options::default())?;

        Ok(Self::from_bound_statement(coordinator, bound_statement))
    }
}

impl BoundStatement {
    /// Parses an SQL query and immediately provides parameter values.
    /// A shortcut to chaining [`PreparedStatement::parse`] and [`PreparedStatement::bind`].
    pub fn parse_and_bind<R>(
        router: &R,
        query_text: &str,
        params: Vec<Value>,
        default_options: Options,
    ) -> Result<Self, SbroadError>
    where
        R: Router,
        R::Cache: Cache<SmolStr, Rc<Plan>>,
    {
        let param_types: Vec<_> = params.iter().map(|v| v.get_type()).collect();

        let prepared = PreparedStatement::parse(router, query_text, &param_types)?;

        prepared.bind(params, default_options)
    }

    pub fn as_plan(&self) -> &Plan {
        &self.plan
    }

    pub fn params_for_audit(&self) -> Option<&[Value]> {
        self.params_for_audit.as_deref()
    }
}
