//! Executor module.
//!
//! The executor is located on the coordinator node in the cluster.
//! It collects all the intermediate results of the plan execution
//! in memory and executes the IR plan tree in the bottom-up manner.
//! It goes like this:
//!
//! 1. The executor collects all the motion nodes from the bottom layer.
//!    In theory all the motions in the same layer can be executed in parallel
//!    (this feature is yet to come).
//! 2. For every motion the executor:
//!    - inspects the IR sub-tree and detects the buckets to execute the query for.
//!    - builds a valid SQL query from the IR sub-tree.
//!    - performs map-reduce for that SQL query (we send it to the shards deduced from the buckets).
//!    - builds a virtual table with query results that correspond to the original motion.
//! 3. Moves to the next motion layer in the IR tree.
//! 4. For every motion the executor then:
//!    - links the virtual table results of the motion from the previous layer we depend on.
//!    - inspects the IR sub-tree and detects the buckets to execute the query.
//!    - builds a valid SQL query from the IR sub-tree.
//!    - performs map-reduce for that SQL query.
//!    - builds a virtual table with query results that correspond to the original motion.
//! 5. Repeats step 3 till we are done with motion layers.
//! 6. Executes the final IR top subtree and returns the final result to the user.

use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

use self::engine::query_id;
use crate::errors::{Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::engine::{Router, TableVersionMap, Vshard};
use crate::executor::ir::ExecutionPlan;
use crate::executor::lru::Cache;
use crate::frontend::Ast;
use crate::ir::node::relational::Relational;
use crate::ir::node::{Motion, NodeId};
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::value::Value;
use crate::ir::{Plan, Slices};
use crate::utils::MutexLike;
use smol_str::SmolStr;

pub mod bucket;
pub mod engine;
pub mod hash;
pub mod ir;
pub mod lru;
pub mod protocol;
pub mod result;
pub mod vtable;

impl Plan {
    /// Apply optimization rules to the plan.
    ///
    /// # Errors
    /// - Failed to optimize the plan.
    pub fn optimize(&mut self) -> Result<(), SbroadError> {
        self.replace_in_operator()?;
        self.push_down_not()?;
        self.split_columns()?;
        self.set_dnf()?;
        self.derive_equalities()?;
        self.merge_tuples()?;
        self.add_motions()?;
        Ok(())
    }
}

/// Query to execute.
#[derive(Debug)]
pub struct Query<'a, C>
where
    C: Router,
{
    /// Explain flag
    is_explain: bool,
    /// Execution plan
    exec_plan: ExecutionPlan,
    /// Coordinator runtime
    coordinator: &'a C,
    /// Bucket map of view { plan output_id (Expression::Row) -> `Buckets` }.
    /// It's supposed to denote relational nodes' output buckets destination.
    bucket_map: HashMap<NodeId, Buckets>,
}

impl<'a, C> Query<'a, C>
where
    C: Router,
{
    pub fn from_parts(
        is_explain: bool,
        exec_plan: ExecutionPlan,
        coordinator: &'a C,
        bucket_map: HashMap<NodeId, Buckets>,
    ) -> Self {
        Self {
            is_explain,
            exec_plan,
            coordinator,
            bucket_map,
        }
    }

    /// Create a new query.
    ///
    /// # Errors
    /// - Failed to parse SQL.
    /// - Failed to build AST.
    /// - Failed to build IR plan.
    /// - Failed to apply optimizing transformations to IR plan.
    pub fn new(coordinator: &'a C, sql: &str, params: Vec<Value>) -> Result<Self, SbroadError>
    where
        C::Cache: Cache<SmolStr, Plan>,
        C::ParseTree: Ast,
    {
        let key = query_id(sql);
        let mut cache = coordinator.cache().lock();

        let mut plan = Plan::new();
        if let Some(cached_plan) = cache.get(&key)? {
            plan = cached_plan.clone();
        }
        if plan.is_empty() {
            let metadata = coordinator.metadata().lock();
            plan = C::ParseTree::transform_into_plan(sql, &*metadata)?;
            // Empty query.
            if plan.is_empty() {
                return Ok(Query::empty(coordinator));
            }

            if coordinator.provides_versions() {
                let mut table_version_map =
                    TableVersionMap::with_capacity(plan.relations.tables.len());
                for (tbl_name, tbl) in &plan.relations.tables {
                    if tbl.is_system() {
                        continue;
                    }
                    let normalized = tbl_name;
                    let version = coordinator.get_table_version(normalized.as_str())?;
                    table_version_map.insert(normalized.clone(), version);
                }
                plan.version_map = table_version_map;
            }
            if !plan.is_ddl()? && !plan.is_acl()? && !plan.is_plugin()? {
                cache.put(key, plan.clone())?;
            }
        }
        if plan.is_block()? {
            plan.bind_params(params)?;
        } else if !plan.is_ddl()? && !plan.is_acl()? && !plan.is_plugin()? {
            plan.bind_params(params)?;
            plan.apply_options()?;
            plan.optimize()?;
        }
        let query = Query {
            is_explain: plan.is_explain(),
            exec_plan: ExecutionPlan::from(plan),
            coordinator,
            bucket_map: HashMap::new(),
        };
        Ok(query)
    }

    fn empty(coordinator: &'a C) -> Self {
        Self {
            is_explain: false,
            exec_plan: Plan::empty().into(),
            coordinator,
            bucket_map: Default::default(),
        }
    }

    /// Get the execution plan of the query.
    #[must_use]
    pub fn get_exec_plan(&self) -> &ExecutionPlan {
        &self.exec_plan
    }

    /// Get the mutable reference to the execution plan of the query.
    #[must_use]
    pub fn get_mut_exec_plan(&mut self) -> &mut ExecutionPlan {
        &mut self.exec_plan
    }

    /// Get the coordinator runtime of the query.
    #[must_use]
    pub fn get_coordinator(&self) -> &C {
        self.coordinator
    }

    pub fn materialize_subtree(&mut self, slices: Slices) -> Result<(), SbroadError> {
        let tier = self.exec_plan.get_ir_plan().tier.as_ref();
        // all tables from one tier, so we can use corresponding vshard object
        let vshard = self.coordinator.get_vshard_object_by_tier(tier)?;

        for slice in slices.slices() {
            // TODO: make it work in parallel
            for motion_id in slice.positions() {
                if let Some(vtables_map) = self.exec_plan.get_vtables() {
                    if vtables_map.contains_key(motion_id) {
                        continue;
                    }
                }
                let motion = self.exec_plan.get_ir_plan().get_relation_node(*motion_id)?;
                if let Relational::Motion(Motion { policy, .. }) = motion {
                    match policy {
                        MotionPolicy::Segment(_) => {
                            // If child is values, then we can materialize it
                            // on the router.
                            let motion_child_id =
                                self.get_exec_plan().get_motion_child(*motion_id)?;
                            let motion_child = self
                                .get_exec_plan()
                                .get_ir_plan()
                                .get_relation_node(motion_child_id)?;

                            if matches!(motion_child, Relational::Values { .. }) {
                                let virtual_table = self
                                    .coordinator
                                    .materialize_values(&mut self.exec_plan, motion_child_id)?;
                                self.exec_plan.set_motion_vtable(
                                    motion_id,
                                    virtual_table,
                                    &vshard,
                                )?;
                                self.get_mut_exec_plan().unlink_motion_subtree(*motion_id)?;
                                continue;
                            }
                        }
                        // Skip it and dispatch the query to the segments
                        // (materialization would be done on the segments). Note that we
                        // will operate with vtables for LocalSegment motions via calls like
                        // `self.exec_plan.contains_vtable_for_motion(node_id)`
                        // in order to define whether virtual table was materialized for values.
                        MotionPolicy::LocalSegment(_) => {
                            continue;
                        }
                        // Local policy should be skipped and dispatched to the segments:
                        // materialization would be done there.
                        MotionPolicy::Local => continue,
                        _ => {}
                    }
                }

                let top_id = self.exec_plan.get_motion_subtree_root(*motion_id)?;

                let buckets = self.bucket_discovery(top_id)?;
                let virtual_table = self.coordinator.materialize_motion(
                    &mut self.exec_plan,
                    motion_id,
                    &buckets,
                )?;
                self.exec_plan
                    .set_motion_vtable(motion_id, virtual_table, &vshard)?;
            }
        }

        Ok(())
    }

    /// Builds explain from current query
    ///
    /// # Errors
    /// - Failed to build explain
    pub fn produce_explain(&mut self) -> Result<Box<dyn Any>, SbroadError> {
        self.coordinator.explain_format(self.to_explain()?)
    }

    /// Dispatch a distributed query from coordinator to the segments.
    ///
    /// # Errors
    /// - Failed to get a motion subtree.
    /// - Failed to discover buckets.
    /// - Failed to materialize motion result and build a virtual table.
    /// - Failed to get plan top.
    pub fn dispatch(&mut self) -> Result<Box<dyn Any>, SbroadError> {
        if self.is_explain() {
            return self.produce_explain();
        }
        self.get_mut_exec_plan()
            .get_mut_ir_plan()
            .restore_constants()?;

        let slices = self.exec_plan.get_ir_plan().clone_slices();
        self.materialize_subtree(slices)?;
        let ir_plan = self.exec_plan.get_ir_plan();
        let top_id = ir_plan.get_top()?;
        if ir_plan.get_relation_node(top_id)?.is_motion() {
            let err =
                |s: &str| -> SbroadError { SbroadError::Invalid(Entity::Plan, Some(s.into())) };
            let motion_aliases = ir_plan.get_relational_aliases(top_id)?;
            let Some(vtables) = self.exec_plan.get_mut_vtables() else {
                return Err(err("no vtables in plan with motion top"));
            };
            let Some(mut vtable) = vtables.remove(&top_id) else {
                return Err(err(&format!("no motion on top_id: {top_id:?}")));
            };
            let Some(v) = Rc::get_mut(&mut vtable) else {
                return Err(err("there are other refs to vtable"));
            };
            return v.to_output(&motion_aliases);
        }
        let buckets = self.bucket_discovery(top_id)?;
        self.coordinator.dispatch(
            &mut self.exec_plan,
            top_id,
            &buckets,
            engine::DispatchReturnFormat::Tuple,
        )
    }

    /// Query explain
    ///
    /// # Errors
    /// - Failed to build explain
    pub fn to_explain(&mut self) -> Result<SmolStr, SbroadError> {
        self.as_explain()
    }

    /// Checks that query is explain and have not to be executed
    pub fn is_explain(&self) -> bool {
        self.is_explain
    }

    /// Checks that query is a statement block.
    ///
    /// # Errors
    /// - plan is invalid
    pub fn is_block(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_block()
    }

    /// Checks that query is DDL.
    ///
    /// # Errors
    /// - Plan is invalid.
    pub fn is_ddl(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_ddl()
    }

    /// Checks that query is ACL.
    ///
    /// # Errors
    /// - Plan is invalid
    pub fn is_acl(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_acl()
    }

    #[cfg(test)]
    pub fn get_motion_id(&self, slice_id: usize, pos_idx: usize) -> NodeId {
        *self
            .exec_plan
            .get_ir_plan()
            .clone_slices()
            .slice(slice_id)
            .unwrap()
            .position(pos_idx)
            .unwrap()
    }

    #[must_use]
    pub fn get_buckets(&self, output_id: NodeId) -> Option<&Buckets> {
        self.bucket_map.get(&output_id)
    }

    /// Checks that query is for plugin.
    ///
    /// # Errors
    /// - Plan is invalid
    pub fn is_plugin(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_plugin()
    }

    /// Checks that query is an empty query.
    pub fn is_empty(&self) -> bool {
        self.exec_plan.get_ir_plan().is_empty()
    }
}

#[cfg(test)]
mod tests;
