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
use crate::errors::{Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::engine::{Router, Vshard};
use crate::executor::ir::ExecutionPlan;
use crate::ir::node::relational::Relational;
use crate::ir::node::{Motion, NodeId};
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::{Plan, Slices};
use crate::BoundStatement;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

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
    pub fn optimize(self) -> Result<Self, SbroadError> {
        self.replace_in_operator()?
            .push_down_not()?
            // In the case if the query was not fully parameterized
            // and contains some constants, lets apply constant folding.
            .cast_constants()?
            .fold_boolean_tree()?
            .split_columns()?
            .set_dnf()?
            .derive_equalities()?
            .merge_tuples()?
            .add_motions()?
            .update_substring()
    }
}

/// Query to execute.
#[derive(Debug)]
pub struct ExecutingQuery<'a, C>
where
    C: Router,
{
    /// Execution plan
    exec_plan: ExecutionPlan,
    /// Coordinator runtime
    coordinator: &'a C,
    /// Bucket map of view { plan output_id (Expression::Row) -> `Buckets` }.
    /// It's supposed to denote relational nodes' output buckets destination.
    bucket_map: HashMap<NodeId, Buckets>,
}

impl<'a, C> ExecutingQuery<'a, C>
where
    C: Router,
{
    pub fn from_bound_statement(runtime: &'a C, statement: BoundStatement) -> Self {
        Self {
            exec_plan: ExecutionPlan::from(*statement.plan),
            coordinator: runtime,
            bucket_map: HashMap::new(),
        }
    }

    /// A shorthand to create a [`ExecutingQuery`] directly from SQL text.
    /// Equivalent to chaining [`BoundStatement::parse_and_bind`] and [`ExecutingQuery::from_bound_statement`].
    pub fn from_text_and_params(
        coordinator: &'a C,
        query_text: &str,
        params: Vec<crate::ir::value::Value>,
    ) -> Result<Self, SbroadError>
    where
        C::Cache: lru::Cache<SmolStr, Rc<Plan>>,
        C::ParseTree: crate::frontend::Ast,
    {
        let bound_statement = BoundStatement::parse_and_bind(
            coordinator,
            query_text,
            params,
            crate::ir::options::Options::default(),
        )?;

        Ok(Self::from_bound_statement(coordinator, bound_statement))
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

    pub fn materialize_subtree(
        &mut self,
        slices: Slices,
        explain_data: Option<&mut Vec<String>>,
    ) -> Result<(), SbroadError> {
        let tier = self.exec_plan.get_ir_plan().tier.as_ref();
        // all tables from one tier, so we can use corresponding vshard object
        let vshard = self.coordinator.get_vshard_object_by_tier(tier)?;
        let mut explain_vec = Vec::new();

        for slice in slices.slices() {
            // TODO: make it work in parallel
            for motion_id in slice.positions() {
                if self.exec_plan.get_vtables().contains_key(motion_id) {
                    continue;
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
                let mut explain_result = String::new();

                let buckets = self.bucket_discovery(top_id)?;
                let virtual_table = self.coordinator.materialize_motion(
                    &mut self.exec_plan,
                    motion_id,
                    &buckets,
                    Some(&mut explain_result),
                )?;

                explain_vec.push(explain_result);

                self.exec_plan
                    .set_motion_vtable(motion_id, virtual_table, &vshard)?;
            }
        }

        if let Some(explain_data) = explain_data {
            *explain_data = explain_vec;
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

        let mut explain_vec = Vec::new();
        let slices = self.exec_plan.get_ir_plan().clone_slices();
        self.materialize_subtree(slices, Some(&mut explain_vec))?;
        let ir_plan = self.exec_plan.get_ir_plan();
        let top_id = ir_plan.get_top()?;
        if ir_plan.get_relation_node(top_id)?.is_motion() {
            let err =
                |s: &str| -> SbroadError { SbroadError::Invalid(Entity::Plan, Some(s.into())) };
            let motion_aliases = ir_plan.get_relational_aliases(top_id)?;
            let Some(mut vtable) = self.exec_plan.get_mut_vtables().remove(&top_id) else {
                return Err(err(&format!("no motion on top_id: {top_id:?}")));
            };
            let Some(v) = Rc::get_mut(&mut vtable) else {
                return Err(err("there are other refs to vtable"));
            };
            return v.to_output(&motion_aliases);
        }
        let buckets = self.bucket_discovery(top_id)?;
        let res = self.coordinator.dispatch(
            &mut self.exec_plan,
            top_id,
            &buckets,
            engine::DispatchReturnFormat::Tuple,
        )?;
        if self.exec_plan.get_ir_plan().is_raw_explain() {
            let explain_res = res.downcast::<String>().map_err(|e| {
                SbroadError::Invalid(
                    Entity::MsgPack,
                    Some(format_smolstr!("expected Tuple as result: {e:?}")),
                )
            })?;

            let explain_res = vec![*explain_res];

            explain_vec.extend_from_slice(&explain_res);
            let mut explain_str = String::new();
            for (idx, explain_res) in explain_vec.iter().enumerate() {
                explain_str.push_str(&format!("{}. {}\n", idx + 1, explain_res));
            }

            return self.coordinator.explain_format(explain_str.to_smolstr());
        }

        Ok(res)
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
        self.exec_plan.get_ir_plan().is_plain_explain()
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

    /// Checks that query is TCL.
    ///
    /// # Errors
    /// - Plan is invalid
    pub fn is_tcl(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_tcl()
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

    /// Checks that query is Deallocate.
    ///
    /// # Errors
    /// - Plan is invalid
    pub fn is_deallocate(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_deallocate()
    }

    /// Checks that query is an empty query.
    pub fn is_empty(&self) -> bool {
        self.exec_plan.get_ir_plan().is_empty()
    }
}

#[cfg(feature = "mock")]
#[cfg(test)]
pub mod tests;
