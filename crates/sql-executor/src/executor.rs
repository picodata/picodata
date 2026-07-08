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
use crate::executor::engine::{Router, Vshard};
use crate::executor::ir::ExecutionPlan;
use crate::executor::vdbe::ExecutionInsight;
use crate::ir::bucket::Buckets;
use crate::ir::node::block::BlockOwned;
use crate::ir::node::relational::Relational;
use crate::ir::node::{AnonymousBlock, BlockEntries, StatementLocation};
use crate::ir::node::{Insert, Motion, NodeId};
use crate::ir::options::Forward;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::value::Value;
use crate::ir::{Plan, Slices};
use smol_str::format_smolstr;
use sql_ir::ir::bucket::BucketSet;
use sql_protocol::dml::insert::ConflictPolicy;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use tarantool::msgpack;
use vdbe::{SqlError, SqlStmt};

pub mod bucket_discovery;
pub mod engine;
pub mod hash;
pub mod ir;
pub mod lru;
pub mod preemption;
pub mod protocol;
pub mod result;
pub mod vdbe;
pub mod vtable;

// The optimizer pipeline (`Stage`, `optimize_before` and friends) lives in
// `sql_ir::ir::transformation`; re-export `Stage` for the executor tests.
pub use crate::ir::transformation::Stage;

pub enum PortType {
    DispatchDql,
    DispatchDml,
    DispatchExplain,
    ExecuteDql,
    ExecuteDml,
    ExecuteMiss,
}

pub trait Port<'p>: io::Write {
    fn add_mp(&mut self, data: &[u8]);

    fn process_stmt(
        &mut self,
        stmt: &mut SqlStmt,
        params: &[Value],
        max_vdbe: u64,
    ) -> Result<ExecutionInsight, SqlError>
    where
        Self: Sized;

    fn process_stmt_with_raw_params(
        &mut self,
        stmt: &mut SqlStmt,
        params: &[u8],
        max_vdbe: u64,
    ) -> Result<ExecutionInsight, SqlError>;

    fn iter(&self) -> impl Iterator<Item = &[u8]>
    where
        Self: Sized;

    fn set_type(&mut self, port_type: PortType);

    fn size(&self) -> u32;

    /// Execute an assembled block VDBE inside a transaction. The returned
    /// [`ExecutionInsight`] lets the caller report cache metrics (a stale or
    /// busy statement is recompiled and counts as a miss).
    fn process_txn(
        &mut self,
        stmt: &mut SqlStmt,
        params: &[&Value],
        vdbe_max_steps: u64,
    ) -> Result<ExecutionInsight, SbroadError>
    where
        Self: Sized;
}

/// The purpose of that structure is to persist data
/// across query execution process.
#[derive(Debug, Default)]
struct ExecutionContext {
    /// Target replicaset uuid to validate `ro_to_rw` forward option.
    target_replicaset: Option<String>,
}

/// Query to execute.
#[derive(Debug)]
pub struct ExecutingQuery<'a, C> {
    /// Execution plan
    pub(crate) exec_plan: ExecutionPlan,
    /// Coordinator runtime
    coordinator: &'a C,
    /// Bucket map of view { relational node id -> `Buckets` }.
    /// It denotes the buckets where the output of the relational node is located.
    bucket_map: HashMap<NodeId, Buckets>,
    exec_ctx: ExecutionContext,
}

impl<'a, C> ExecutingQuery<'a, C>
where
    C: Router,
{
    pub fn from_plan(runtime: &'a C, plan: Plan) -> Self {
        Self {
            exec_plan: ExecutionPlan::new(plan),
            coordinator: runtime,
            bucket_map: HashMap::new(),
            exec_ctx: ExecutionContext::default(),
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

    fn materialize_subtree_impl<'p>(
        &mut self,
        slices: Slices,
        mut port: Option<&mut impl Port<'p>>,
        vtab_count: &mut usize,
    ) -> Result<(), SbroadError> {
        let tier = self.exec_plan.get_ir_plan().tier.as_ref();
        let coordinator = self.coordinator;
        // all tables from one tier, so we can use corresponding vshard object
        let vshard = coordinator.get_vshard_object_by_tier(tier)?;

        for slice in slices.slices() {
            // TODO: make it work in parallel
            for motion_id in slice.positions() {
                if self.exec_plan.get_vtables().contains_key(motion_id) {
                    continue;
                }

                let motion = self.exec_plan.get_ir_plan().get_relation_node(*motion_id)?;
                if let Relational::Motion(Motion { policy, .. }) = motion {
                    match policy {
                        MotionPolicy::Segment(_)
                        // EXPLAIN(RAW) will be executed on the storage node
                        // We don't want to materialize VALUES on router and lose them in the execution plan.
                            if !self.exec_plan.get_ir_plan().is_raw_explain() =>
                        {
                            // If child is values, then we can materialize it
                            // on the router.
                            let plan = self.get_exec_plan().get_ir_plan();
                            let motion_child_id = plan.get_motion_child(*motion_id)?;
                            let motion_child = plan.get_relation_node(motion_child_id)?;

                            if matches!(motion_child, Relational::Values { .. }) {
                                *vtab_count += 1;
                                let virtual_table = coordinator.materialize_values(&mut self.exec_plan, motion_child_id)?;

                                self.exec_plan.set_motion_vtable(
                                    motion_id,
                                    virtual_table,
                                    &vshard,
                                )?;
                                self.get_mut_exec_plan()
                                    .mark_motion_subtree_unlinked(*motion_id)?;
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

                *vtab_count += 1;

                let top_id = self
                    .exec_plan
                    .get_ir_plan()
                    .get_motion_subtree_root(*motion_id)?;

                let buckets = self.bucket_discovery(top_id)?;
                self.enforce_forward_option(&buckets)?;

                let mut virtual_table =
                    coordinator.materialize_motion(&mut self.exec_plan, motion_id, &buckets)?;

                if self.exec_plan.get_ir_plan().is_raw_explain() {
                    // Take the tuples from the virtual table and encode them into
                    // explain msgpack.
                    let tuples = std::mem::take(virtual_table.get_mut_tuples());
                    for tuple in tuples.into_iter() {
                        let mp = msgpack::encode(&tuple);
                        if let Some(p) = port.as_mut() {
                            p.add_mp(mp.as_slice())
                        }
                    }
                }

                self.exec_plan
                    .set_motion_vtable(motion_id, virtual_table, &vshard)?;
            }
        }

        Ok(())
    }

    pub fn materialize_subtree<'p>(
        &mut self,
        slices: Slices,
        port: Option<&mut impl Port<'p>>,
    ) -> Result<(), SbroadError> {
        let mut vtab_count = 0;
        self.materialize_subtree_impl(slices, port, &mut vtab_count)
            .map_err(|err| match err {
                SbroadError::ExecutionError(err) | SbroadError::VdbeError(err) => {
                    SbroadError::TaggedExecutionError(vtab_count, err)
                }
                _ => err,
            })
    }

    pub fn calculate_block_buckets(
        &mut self,
        block: &AnonymousBlock,
    ) -> Result<Buckets, SbroadError> {
        let mut block_buckets: Option<(StatementLocation, Buckets)> = None;
        for entry in BlockEntries::new(&block.statements) {
            let buckets = entry.with(|query_id| {
                let buckets = self.bucket_discovery(*query_id)?;
                match &buckets {
                    Buckets::All => {
                        return Err(SbroadError::Other(
                            "transaction cannot be executed on all buckets".into(),
                        ))
                    }
                    Buckets::Filtered(BucketSet::Exact(filtered)) if filtered.len() != 1 => {
                        return Err(SbroadError::Other(format_smolstr!(
                            "transaction can only be executed on a single bucket, got {buckets}"
                        )));
                    }
                    Buckets::Filtered(BucketSet::Exact(_)) | Buckets::Any => {}
                    Buckets::Filtered(_) => {
                        return Err(SbroadError::Other(
                            "buckets cannot be filtered for this statement".into(),
                        ))
                    }
                }
                Ok(buckets)
            })?;

            // Cross-statement check carries two locations, so it lives outside the closure.
            if matches!(buckets, Buckets::Filtered(_)) {
                if let Some((prev_location, prev_buckets)) = &block_buckets {
                    if prev_buckets != &buckets {
                        return Err(prev_location.wrap_error_with(
                            &entry.location,
                            SbroadError::Other(format_smolstr!(
                                "different buckets: {prev_buckets} and {buckets}"
                            )),
                        ));
                    }
                } else {
                    block_buckets = Some((entry.location, buckets));
                }
            }
        }

        let buckets = block_buckets.map(|(_, b)| b).unwrap_or(Buckets::Any);
        Ok(buckets)
    }

    /// Dispatch a distributed query from coordinator to the segments.
    ///
    /// # Errors
    /// - Failed to get a motion subtree.
    /// - Failed to discover buckets.
    /// - Failed to materialize motion result and build a virtual table.
    /// - Failed to get plan top.
    pub fn dispatch<'p>(&mut self, port: &mut impl Port<'p>) -> Result<(), SbroadError> {
        let top_id = self.exec_plan.get_ir_plan().get_top()?;
        if self.exec_plan.get_ir_plan().is_block()? {
            let block = self.exec_plan.get_ir_plan().get_owned_block_node(top_id)?;
            let BlockOwned::Anonymous(block) = block else {
                unreachable!("plan.is_block() returned true, but top is {block:?}")
            };

            let buckets = self.calculate_block_buckets(&block)?;
            self.enforce_forward_option(&buckets)?;

            return self
                .coordinator
                .dispatch(&mut self.exec_plan, top_id, &buckets, port);
        }

        if let Relational::Insert(Insert {
            conflict_strategy, ..
        }) = self.exec_plan.get_ir_plan().get_relation_node(top_id)?
        {
            let _: ConflictPolicy = conflict_strategy.try_into()?;
        }

        let slices = self.exec_plan.get_ir_plan().clone_slices();
        self.materialize_subtree(slices, Some(port))?;
        let ir_plan = self.exec_plan.get_ir_plan();
        if ir_plan.get_relation_node(top_id)?.is_motion() {
            let err =
                |s: &str| -> SbroadError { SbroadError::Invalid(Entity::Plan, Some(s.into())) };
            let aliases = ir_plan.get_relational_aliases(top_id)?;
            let Some(mut ref_table) = self.exec_plan.get_mut_vtables().remove(&top_id) else {
                return Err(err(&format!("no virtual table for motion id {top_id:?}")));
            };
            let Some(table) = Rc::get_mut(&mut ref_table) else {
                return Err(err("there are other references for the virtual table"));
            };

            // Skip metadata in case of `EXPLAIN (RAW)`
            if !self.exec_plan.get_ir_plan().is_raw_explain() {
                table
                    .dump_mp(aliases.iter().map(|s| s.as_str()), port)
                    .map_err(|e| {
                        SbroadError::Invalid(Entity::VirtualTable, Some(format_smolstr!("{e}")))
                    })?;
            }
            return Ok(());
        }

        let buckets = self.bucket_discovery(top_id)?;
        self.enforce_forward_option(&buckets)?;

        self.exec_plan.normalize_for_dispatch(top_id, &buckets)?;
        let plan_id_target = self.exec_plan.get_plan_id_target()?;
        if let Some(node_id) = plan_id_target {
            self.exec_plan.set_plan_id(node_id)?;
        }

        let query_num = self.exec_plan.get_vtables().len();
        self.coordinator
            .dispatch(&mut self.exec_plan, top_id, &buckets, port)
            .map_err(|err| match err {
                SbroadError::ExecutionError(err) | SbroadError::VdbeError(err) => {
                    SbroadError::TaggedExecutionError(query_num + 1, err)
                }
                _ => err,
            })?;

        Ok(())
    }

    pub fn is_explain(&self) -> bool {
        self.exec_plan.get_ir_plan().is_explain()
    }

    pub fn is_block(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_block()
    }

    pub fn is_ddl(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_ddl()
    }

    pub fn is_acl(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_acl()
    }

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

    pub fn is_plugin(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_plugin()
    }

    pub fn is_backup(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_backup()
    }

    pub fn is_deallocate(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_deallocate()
    }

    pub fn is_empty(&self) -> bool {
        self.exec_plan.get_ir_plan().is_empty()
    }

    /// Enforces the requested `FORWARD` option against the actual
    /// buckets for an execution step.
    ///
    /// When query contains RAW mode of EXPLAIN and FORWARD option is specified
    /// the check is skipped so that the explain output is always produced
    /// regardless of whether the requested forward level is achievable. Only
    /// RAW mode must be skipped here since other EXPLAIN modes do not trigger
    /// dispatch or motion materialization machinery.
    fn enforce_forward_option(&mut self, buckets: &Buckets) -> Result<(), SbroadError> {
        let ir_plan = self.exec_plan.get_ir_plan();
        let forward_option = ir_plan.effective_options.forward;

        // `forward = on` is the default and is always satisfiable,
        // so skip the expensive replicaset resolution.
        if matches!(forward_option, Forward::On) {
            return Ok(());
        }

        if self.get_exec_plan().get_ir_plan().is_raw_explain() {
            return Ok(());
        }

        self.coordinator.enforce_forward_option(
            forward_option,
            buckets,
            &mut self.exec_ctx.target_replicaset,
        )
    }
}

#[cfg(test)]
pub mod tests;
