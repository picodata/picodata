//! Tarantool planner and executor for a distributed SQL.
#[macro_use]
extern crate pest_derive;
extern crate core;

use crate::errors::SbroadError;
use crate::executor::engine::{query_id, Metadata, Router, TableVersionMap};
use crate::executor::lru::Cache;
use crate::frontend::Ast;
use crate::ir::options::Options;
use crate::ir::types::{DerivedType, UnrestrictedType};
use crate::ir::value::Value;
use crate::ir::Plan;
use crate::utils::MutexLike;
use smol_str::SmolStr;
use std::rc::Rc;

pub mod backend;
pub mod cbo;
pub mod errors;
pub mod executor;
pub mod frontend;
pub mod ir;
pub mod log;
pub mod utils;

/// A parsed parameterized query. It still has parameter placeholders instead of actual parameter values.
#[derive(Debug)]
pub struct PreparedStatement {
    // This Rc shares the reference with the query cache
    plan: Rc<Plan>,
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
        R::MetadataProvider: Metadata,
        R::Cache: Cache<SmolStr, Rc<Plan>>,
        R::ParseTree: Ast,
    {
        let mut cache = router.cache().lock();

        // FIXME: converting the hash to string is suboptimal, we could use a newtype over `[u8; HASH_LEN]` directly
        let cache_key = query_id(query_text, param_types);

        if let Some(cached_plan) = router.with_admin_su(|| cache.get(&cache_key))?? {
            return Ok(PreparedStatement {
                plan: cached_plan.clone(),
            });
        }

        let new_plan = router.with_admin_su(|| -> Result<Plan, SbroadError> {
            let metadata = router.metadata().lock();
            let mut plan = R::ParseTree::transform_into_plan(query_text, param_types, &*metadata)?;
            if router.provides_versions() {
                let mut table_version_map =
                    TableVersionMap::with_capacity(plan.relations.tables.len());
                for table in plan.relations.tables.keys() {
                    let version = router.get_table_version(table.as_str())?;
                    table_version_map.insert(table.clone(), version);
                }
                plan.version_map = table_version_map;
            }

            if plan.is_dql_or_dml()? {
                plan.check_raw_options()?;
                plan = plan.optimize()?;
            }

            Ok(plan)
        })??;

        let new_plan = Rc::new(new_plan);

        // Only DQL and DML is cached, because it is on the hot path
        // other types of queries are much less likely to be queried again
        if new_plan.is_dql_or_dml()? {
            cache.put(cache_key, new_plan.clone())?;
        }

        Ok(PreparedStatement { plan: new_plan })
    }

    /// A shorthand method for [`Plan::collect_parameter_types`]
    pub fn collect_parameter_types(&self) -> Vec<UnrestrictedType> {
        self.plan.collect_parameter_types()
    }

    /// Retrieve the plan IR for this prepared statement.
    pub fn as_plan(&self) -> &Plan {
        &self.plan
    }

    /// Provide concrete values for query parameters, creating a [`BoundStatement`] as a result.
    pub fn bind(
        &self,
        params: Vec<Value>,
        default_options: Options,
    ) -> Result<BoundStatement, SbroadError> {
        let mut plan = Box::new(self.plan.as_ref().clone());

        if plan.is_empty() {
            // Empty query, do nothing
        } else if plan.is_block()? {
            plan.bind_params(&params, default_options)?;
        } else if plan.is_dql_or_dml()? {
            plan.bind_params(&params, default_options)?;
            *plan = plan
                .update_timestamps()?
                .cast_constants()?
                .fold_boolean_tree()?;
        }

        Ok(BoundStatement { plan })
    }
}

/// A query with parameter values supplied. This representation is ready for execution.
#[derive(Debug, Clone)]
pub struct BoundStatement {
    // wrap the plan into a `Box` to reduce the size of `BoundStatement`
    // this is important for `pgproto`, which stores it in `PortalState`
    plan: Box<Plan>,
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
        R::MetadataProvider: Metadata,
        R::Cache: Cache<SmolStr, Rc<Plan>>,
        R::ParseTree: Ast,
    {
        let param_types: Vec<_> = params.iter().map(|v| v.get_type()).collect();

        let prepared = PreparedStatement::parse(router, query_text, &param_types)?;

        prepared.bind(params, default_options)
    }

    pub fn as_plan(&self) -> &Plan {
        &self.plan
    }
}
