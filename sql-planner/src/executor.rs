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
use crate::executor::engine::helpers::generate_pattern_with_params_for_block;
use crate::executor::engine::{Router, Vshard};
use crate::executor::ir::ExecutionPlan;
use crate::executor::vdbe::ExecutionInsight;
use crate::ir::bucket::{BucketSet, Buckets};
use crate::ir::explain::{execution_info::BucketsInfo, LogicalExplain};
use crate::ir::node::block::{BlockOwned, MutBlock};
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{
    AnonymousBlock, BlockEntries, BlockEntriesMut, BlockStatement, Insert, Motion, NodeId,
    StatementLocation, Values, ValuesRow,
};
use crate::ir::options::OptionKind;
use crate::ir::transformation::redistribution::{MotionPolicy, Target};
use crate::ir::tree::traversal::{PostOrder, REL_CAPACITY};
use crate::ir::value::Value;
use crate::ir::{ExplainOptions, Plan, Slices};
use crate::utils::{indent, indent_custom, indent_with_prefix};
use crate::{write_explain_header1, write_explain_header2, BoundStatement};
use smol_str::{format_smolstr, SmolStr};
use std::collections::HashMap;
use std::fmt::{self, Write as _};
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

impl Plan {
    /// Apply optimization rules to the plan.
    ///
    /// # Errors
    /// - Failed to optimize the plan.
    pub fn optimize(self) -> Result<Self, SbroadError> {
        let top_id = self.get_top()?;
        self.optimize_subtree(top_id)
    }

    pub fn optimize_subtree(self, top_id: NodeId) -> Result<Self, SbroadError> {
        let mut plan = self
            .replace_in_operator_in_subtree(top_id)?
            .push_down_not_in_subtree(top_id)?
            // In the case if the query was not fully parameterized
            // and contains some constants, lets apply constant folding.
            .cast_constants()?
            .fold_boolean_tree()?
            .split_columns_in_subtree(top_id)?
            .set_dnf_in_subtree(top_id)?
            .analyze_equality_facts_in_subtree(top_id)?
            .derive_equalities_in_subtree(top_id)?
            .merge_tuples_in_subtree(top_id)?
            .add_motions_to_subtree(top_id)?
            .update_substring()?
            // After all transformations we can finally determine what parameters are unique.
            .mark_unique_parameters()?;

        // Facts are only used during planning. Afterward they're dead state. Drop them
        // so they don't bloat the plan clones made on the execution/dispatch path.
        plan.facts = None;

        Ok(plan)
    }

    pub fn optimize_block(self) -> Result<Self, SbroadError> {
        // Eliminate motions from the plan so it can be executed locally within a block.
        fn eliminate_motions_in_subtree(
            plan: &mut Plan,
            top_id: NodeId,
        ) -> Result<(), SbroadError> {
            let top = plan.get_relation_node(top_id)?;
            if matches!(top, Relational::Insert(_)) {
                return eliminate_insert_motion(plan, top_id);
            }
            if top.is_dml() {
                // Ensure motion is local.
                let top_children = plan.get_relation_children(top_id)?;
                let Some(motion_id) = top_children.get(0) else {
                    return Ok(());
                };
                let motion = plan.get_relation_node(*motion_id)?;
                if !matches!(
                    motion,
                    Relational::Motion(Motion {
                        policy: MotionPolicy::Local,
                        ..
                    })
                ) {
                    return Ok(());
                }

                // Unlink the DML node's motion child.
                let motion_child_id = plan.get_rel_child(*motion_id, 0)?;
                match plan.get_mut_relation_node(top_id)? {
                    MutRelational::Update(update) => update.child = motion_child_id,
                    MutRelational::Delete(delete) => delete.child = Some(motion_child_id),
                    _ => {}
                }
            }

            Ok(())
        }

        // Eliminate the motion under an INSERT in a transaction.
        //
        // Expected shape: Insert -> Motion(Segment | LocalSegment) -> Values -> ValuesRow*.
        // Each ValuesRow cell at a sharding-key position must be a Constant or Parameter
        // (parameters become constants after binding). Other cells may hold arbitrary
        // expressions — they will be evaluated by the storage's local SQL.
        fn eliminate_insert_motion(plan: &mut Plan, insert_id: NodeId) -> Result<(), SbroadError> {
            // INSERT into a global table gets MotionPolicy::Full and is not supported in blocks;
            // leave the motion in place so the block optimizer reports it.
            if plan.dml_node_table(insert_id)?.is_global() {
                return Ok(());
            }

            let motion_id = plan.dml_child_id(insert_id)?;
            let Relational::Motion(Motion { policy, .. }) = plan.get_relation_node(motion_id)?
            else {
                return Ok(());
            };
            let (MotionPolicy::Segment(key) | MotionPolicy::LocalSegment(key)) = policy else {
                return Ok(());
            };

            let values_id = plan.get_rel_child(motion_id, 0)?;
            let Relational::Values(Values {
                children: values_rows,
                ..
            }) = plan.get_relation_node(values_id)?
            else {
                return Ok(());
            };

            // Collect cell positions in each ValuesRow's data tuple that correspond to
            // sharding-key columns.
            let key_positions: Vec<usize> = key
                .targets
                .iter()
                .filter_map(|t| match t {
                    Target::Reference(pos) => Some(*pos),
                    Target::Value(_) => None,
                })
                .collect();

            for row_id in values_rows {
                let Relational::ValuesRow(ValuesRow {
                    data, subqueries, ..
                }) = plan.get_relation_node(*row_id)?
                else {
                    panic!("Expected ValuesRow under Values, got {row_id:?}");
                };
                if !subqueries.is_empty() {
                    return Err(SbroadError::other(
                        "INSERT in transaction does not support subqueries in VALUES",
                    ));
                }
                let row_list = plan.get_row_list(*data)?;
                for pos in &key_positions {
                    let cell_id = *row_list
                        .get(*pos)
                        .expect("sharding-key position must exist in values row");
                    let cell = plan.get_expression_node(cell_id)?;
                    if !matches!(cell, Expression::Constant(_) | Expression::Parameter(_)) {
                        return Err(SbroadError::other(
                            "INSERT in transaction requires constant or parameter values for sharding-key columns"
                        ));
                    }
                }
            }

            // Re-point Insert's child directly to Values, dropping the Motion node.
            if let MutRelational::Insert(Insert { child, .. }) =
                plan.get_mut_relation_node(insert_id)?
            {
                *child = values_id;
            }

            Ok(())
        }

        let block_id = self.get_top()?;
        let block = self.get_block_node(block_id)?.get_block_owned();
        let BlockOwned::Anonymous(AnonymousBlock { mut statements, .. }) = block else {
            panic!("can't opmitize {block:?} as block");
        };

        fn optimize_block_subtree(mut plan: Plan, query: &mut NodeId) -> Result<Plan, SbroadError> {
            plan.set_top(*query).expect("top node can't be missed");
            plan = plan.optimize_subtree(*query)?;
            eliminate_motions_in_subtree(&mut plan, *query)?;
            let new_top = plan.get_top().expect("just set");
            *query = new_top;
            if plan.subtree_has_motions(new_top)? {
                return Err(SbroadError::Other(format_smolstr!(
                    "cannot run in a transactional block because it requires \
                     cross-shard data movement; restrict by the sharding key, \
                     or move it outside the block"
                )));
            }
            Ok(plan)
        }

        let mut plan = self;
        for entry in BlockEntriesMut::new(&mut statements) {
            plan = entry.with_state(plan, optimize_block_subtree)?;
        }

        // Set block as the top node back.
        plan.set_top(block_id).expect("block node can't be missed");

        // Update statements since as a result of optimizations each subtree can have a new top.
        if let MutBlock::Anonymous(AnonymousBlock {
            statements: ref mut plan_statements,
            ..
        }) = plan.get_mut_block_node(block_id)?
        {
            *plan_statements = statements;
        }

        Ok(plan)
    }

    fn subtree_has_motions(&self, top_id: NodeId) -> Result<bool, SbroadError> {
        let post_tree = PostOrder::new(|node| self.nodes.rel_iter(node), REL_CAPACITY);
        for node in post_tree.traverse_into_iter(top_id) {
            if let Relational::Motion(_) = self.get_relation_node(node.1)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

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

    /// Execute an assembled block VDBE inside a transaction.
    fn process_txn(
        &mut self,
        stmt: &mut SqlStmt,
        params: &[&Value],
        vdbe_max_steps: u64,
    ) -> Result<(), SbroadError>
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
    exec_plan: ExecutionPlan,
    /// Coordinator runtime
    coordinator: &'a C,
    /// Bucket map of view { plan output_id (Expression::Row) -> `Buckets` }.
    /// It's supposed to denote relational nodes' output buckets destination.
    bucket_map: HashMap<NodeId, Buckets>,
    exec_ctx: ExecutionContext,
}

pub fn format_let_entry(is_unused: bool, var_name: &str) -> String {
    if is_unused {
        format!("**Unused** let \"{var_name}\"")
    } else {
        format!("Let \"{var_name}\"")
    }
}

impl<'a, C> ExecutingQuery<'a, C>
where
    C: Router,
{
    pub fn from_bound_statement(runtime: &'a C, statement: BoundStatement) -> Self {
        Self {
            exec_plan: ExecutionPlan::new(*statement.plan),
            coordinator: runtime,
            bucket_map: HashMap::new(),
            exec_ctx: ExecutionContext::default(),
        }
    }

    /// A shorthand to create a [`ExecutingQuery`] directly from SQL text.
    /// Equivalent to chaining [`BoundStatement::parse_and_bind`] and [`ExecutingQuery::from_bound_statement`].
    pub fn from_text_and_params(
        coordinator: &'a C,
        query_text: &str,
        params: Vec<Value>,
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

    fn materilize_subtree_impl<'p>(
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

                *vtab_count += 1;

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
        self.materilize_subtree_impl(slices, port, &mut vtab_count)
            .map_err(|err| match err {
                SbroadError::ExecutionError(err) | SbroadError::VdbeError(err) => {
                    SbroadError::TaggedExecutionError(vtab_count, err)
                }
                _ => err,
            })
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

        let plan_id_target = self.exec_plan.get_plan_id_target()?;
        if let Some(node_id) = plan_id_target {
            self.exec_plan.set_plan_id(node_id)?;
        }

        let buckets = self.bucket_discovery(top_id)?;
        self.enforce_forward_option(&buckets)?;

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

    pub fn is_logical_explain(&self) -> bool {
        self.exec_plan.get_ir_plan().is_logical_explain()
    }

    pub fn is_raw_explain(&self) -> bool {
        self.exec_plan.get_ir_plan().is_raw_explain()
    }

    pub fn is_buckets_explain(&self) -> bool {
        self.exec_plan.get_ir_plan().is_buckets_explain()
    }

    pub fn is_explain_forward(&self) -> bool {
        self.exec_plan.get_ir_plan().is_explain_forward()
    }

    pub fn is_explain_context(&self) -> bool {
        self.exec_plan.get_ir_plan().is_explain_context()
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

    fn get_block_logical(
        &self,
        block: &AnonymousBlock,
    ) -> Result<Vec<LogicalExplain>, SbroadError> {
        let mut explain = Vec::with_capacity(block.statements.len());
        let plan = self.get_exec_plan().get_ir_plan();
        for entry in BlockEntries::new(&block.statements) {
            let explain_entry = entry.with(|query_id| LogicalExplain::new(plan, *query_id))?;
            explain.push(explain_entry);
        }

        Ok(explain)
    }

    #[allow(clippy::type_complexity)]
    fn generate_block_patterns(
        &self,
        block: AnonymousBlock,
        buckets: &Buckets,
    ) -> Result<Vec<BlockStatement<(String, Vec<Value>)>>, SbroadError> {
        let block_bucket = match buckets {
            Buckets::Filtered(BucketSet::Exact(set)) => {
                assert!(set.len() == 1);
                set.iter().copied().next()
            }
            _ => None,
        };

        let mut statements = Vec::with_capacity(block.statements.len());
        for stmt in block.statements {
            statements.push(stmt.try_map(|id| {
                generate_pattern_with_params_for_block(
                    self.get_exec_plan(),
                    id,
                    block_bucket,
                    false,
                )
            })?);
        }

        Ok(statements)
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

    pub fn explain_logical(&mut self) -> Result<String, SbroadError> {
        let mut buf = String::new();
        let explain_options = self.get_exec_plan().get_ir_plan().explain_options;
        if !explain_options.has_single_facet() {
            write_explain_header1!(&mut buf, "# Logical plan").unwrap();
            writeln!(&mut buf).unwrap();
        }

        if self.is_block()? {
            let top_id = self.exec_plan.get_ir_plan().get_top()?;
            let block = self.exec_plan.get_ir_plan().get_owned_block_node(top_id)?;
            let BlockOwned::Anonymous(block) = block else {
                unreachable!("plan.is_block() returned true, but top is {block:?}")
            };

            let logical_explains = self.get_block_logical(&block)?;
            let unused_lets = block.get_unused_lets();

            let buckets = self.calculate_block_buckets(&block)?;
            let block_statements = self.generate_block_patterns(block, &buckets)?;

            let explain_options = self.exec_plan.get_ir_plan().explain_options;
            let should_fmt = explain_options.contains(ExplainOptions::Fmt);

            let mut stmt_idx = 0;
            let mut statements = block_statements.iter().enumerate().peekable();
            while let Some((idx, stmt)) = statements.next() {
                let mut explain_one = |buf: &mut String,
                                       query: &(String, Vec<Value>),
                                       kind: &str| {
                    let (sql, params) = query;
                    let source = buckets.determine_exec_location();
                    write_explain_header2!(buf, "{}. {} ({source})", stmt_idx + 1, kind).unwrap();
                    writeln!(buf).unwrap();

                    let sql = format_sql(sql, params, should_fmt);
                    writeln!(buf, "{sql}").unwrap();
                    writeln!(buf).unwrap();

                    write!(buf, "{}", logical_explains[stmt_idx]).unwrap();

                    stmt_idx += 1;
                };

                match stmt {
                    BlockStatement::ReturnQuery(query) => {
                        explain_one(&mut buf, query, "Return query")
                    }
                    BlockStatement::Query(query) => explain_one(&mut buf, query, "Query"),
                    BlockStatement::Let { query, var } => {
                        let var = var.strip_prefix(':').unwrap_or(var.as_str());
                        let kind = format_let_entry(unused_lets.contains(&idx), var);
                        explain_one(&mut buf, query, &kind)
                    }
                    BlockStatement::If { cond, body } => {
                        explain_one(&mut buf, cond, "If cond");
                        let mut body_iter = body.iter().peekable();
                        writeln!(&mut buf).unwrap();
                        writeln!(&mut buf).unwrap();

                        while let Some(body_query) = body_iter.next() {
                            explain_one(&mut buf, body_query, "If body");

                            let has_next = body_iter.peek().is_some();
                            if has_next {
                                writeln!(&mut buf).unwrap();
                                writeln!(&mut buf).unwrap();
                            }
                        }
                    }
                };

                let has_next = statements.peek().is_some();
                if has_next {
                    writeln!(&mut buf).unwrap();
                    writeln!(&mut buf).unwrap();
                }
            }
        } else {
            let plan = self.get_exec_plan().get_ir_plan();
            let top_id = plan.get_top()?;
            let explain = LogicalExplain::new(plan, top_id)?;
            write!(&mut buf, "{explain}").unwrap();
        }

        Ok(buf)
    }

    pub fn explain_forward(&mut self) -> Result<String, SbroadError> {
        let info = BucketsInfo::new_from_query(self)?;
        let forward = match info {
            BucketsInfo::Unknown => crate::ir::options::Forward::On,
            BucketsInfo::Calculated(calculated) => self
                .get_coordinator()
                .get_possible_forward_option(&calculated.buckets, &mut None)?,
        };

        let mut buf = String::new();
        let explain_options = self.get_exec_plan().get_ir_plan().explain_options;
        if !explain_options.has_single_facet() {
            write_explain_header1!(&mut buf, "# Forward").unwrap();
            writeln!(&mut buf).unwrap();
        }
        writeln!(&mut buf, "forward analysis (on > ro_to_rw > off):").unwrap();
        write!(indent(&mut buf), "forward = {forward}").unwrap();

        Ok(buf)
    }

    pub fn explain_raw<'p>(&mut self, port: &mut impl Port<'p>) -> Result<String, SbroadError> {
        let explain_options = self.get_exec_plan().get_ir_plan().explain_options;
        let should_fmt = explain_options.contains(ExplainOptions::Fmt);
        let is_block = self.get_exec_plan().get_ir_plan().is_block()?;
        let show_buckets = explain_options.contains(ExplainOptions::Buckets);

        let raw_explain = RawExplain::from_port(port, should_fmt, show_buckets && !is_block)?;
        let mut buf = String::new();
        if !explain_options.has_single_facet() {
            write_explain_header1!(&mut buf, "# Raw plan").unwrap();
            writeln!(&mut buf).unwrap();
        }
        write!(&mut buf, "{raw_explain}").unwrap();

        Ok(buf)
    }

    pub fn explain_buckets(&mut self) -> Result<String, SbroadError> {
        let info = BucketsInfo::new_from_query(self)?;
        let mut buf = String::new();
        let explain_options = self.get_exec_plan().get_ir_plan().explain_options;
        if !explain_options.has_single_facet() {
            write_explain_header1!(&mut buf, "# Buckets").unwrap();
            writeln!(&mut buf).unwrap();
        }

        write!(&mut buf, "{info}").unwrap();

        Ok(buf)
    }

    pub fn explain_context(&mut self) -> Result<String, SbroadError> {
        let mut buf = String::new();

        let explain_options = self.get_exec_plan().get_ir_plan().explain_options;
        if !explain_options.has_single_facet() {
            write_explain_header1!(&mut buf, "# Context").unwrap();
            writeln!(&mut buf).unwrap();
        }

        let plan = self.get_exec_plan().get_ir_plan();
        let opcode_max = plan.effective_options.sql_vdbe_opcode_max;
        let row_max = plan.effective_options.sql_motion_row_max;

        writeln!(&mut buf, "{} = {opcode_max}", OptionKind::VdbeOpcodeMax).unwrap();
        write!(&mut buf, "{} = {row_max}", OptionKind::MotionRowMax).unwrap();

        Ok(buf)
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
        if self.is_raw_explain() {
            return Ok(());
        }

        let ir_plan = self.exec_plan.get_ir_plan();
        let forward_option = ir_plan.effective_options.forward;
        self.coordinator.enforce_forward_option(
            forward_option,
            buckets,
            &mut self.exec_ctx.target_replicaset,
        )
    }
}

#[derive(Debug, Clone, msgpack::Encode, msgpack::Decode)]
struct RawExplainTuple {
    selectid: i64,
    order: i64,
    from: i64,
    detail: String,
}

impl RawExplainTuple {
    fn try_decode_from_mp(mp: &[u8]) -> Result<Self, String> {
        if let Ok(tuple) = msgpack::decode::<RawExplainTuple>(mp) {
            return Ok(tuple);
        }

        match msgpack::decode::<Vec<String>>(mp) {
            Ok(mut err) => Err(err.pop().unwrap()),
            Err(err) => Err(format!("BUG: failed to decode error: {err}")),
        }
    }
}

#[derive(Debug)]
struct RawExplainEntry {
    query: String,
    location: String,
    buckets: String,
    sql: String,
    params: Vec<Value>,
    tuples: Result<Vec<RawExplainTuple>, String>,
}

const LINE_WIDTH: usize = 80;

fn format_raw_plan_node(node: &str, should_fmt: bool) -> String {
    let mut node = node.to_owned();
    if should_fmt && node.len() > LINE_WIDTH {
        node = node.replace("USING", "\n USING");
        node = node.replace("(", "\n (");
    }

    node
}

fn format_raw_plan(tuples: &[RawExplainTuple], should_fmt: bool) -> String {
    let mut plan = String::new();

    let mut tuples = tuples.iter().peekable();
    while let Some(tuple) = tuples.next() {
        let has_next = tuples.peek().is_some();
        let sep = if has_next { "\n" } else { "" };

        let idx = tuple.selectid;
        let level = tuple.order.max(0) as usize + 1;
        let node = format_raw_plan_node(&tuple.detail, should_fmt);
        let prefix = format_smolstr!("[{idx}] ");

        write!(
            indent_custom(&mut plan, &mut indent_with_prefix(level * 2, prefix)),
            "{node}{sep}"
        )
        .unwrap();
    }

    plan
}

fn format_sql(explain: &str, params: &[Value], should_fmt: bool) -> String {
    let sql = explain
        .strip_prefix("EXPLAIN QUERY PLAN ")
        .unwrap_or(explain);

    let mut fmt_options = sqlformat::FormatOptions::<'_> {
        joins_as_top_level: true,
        inline: true,
        ..Default::default()
    };

    if should_fmt && sql.len() >= LINE_WIDTH {
        fmt_options.joins_as_top_level = false;
        fmt_options.inline = false;
    }

    let params = params.iter().map(|p| p.to_string()).collect();
    let indexed_params = sqlformat::QueryParams::Indexed(params);

    sqlformat::format(sql, &indexed_params, &fmt_options)
}

fn write_raw_explain_entry(
    f: &mut fmt::Formatter<'_>,
    entry: &RawExplainEntry,
    idx: usize,
    should_fmt: bool,
    show_buckets: bool,
) -> fmt::Result {
    let sql = format_sql(&entry.sql, &entry.params, should_fmt);
    let plan = match &entry.tuples {
        Ok(tuples) => format_raw_plan(tuples, should_fmt),
        Err(err) => err.clone(),
    };

    let (kind, source) = (&entry.query, &entry.location);

    write_explain_header2!(f, "{idx}. {kind} ({source})")?;
    write!(f, "\n{sql}\n\n")?;
    write!(f, "plan:\n{plan}")?;

    if show_buckets {
        write!(f, "\n\nbuckets = {}", entry.buckets)?;
    }

    Ok(())
}

#[derive(Debug)]
struct RawExplain {
    entries: Vec<RawExplainEntry>,
    show_buckets: bool,
    should_fmt: bool,
}

impl fmt::Display for RawExplain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entries = self.entries.iter().enumerate().peekable();
        while let Some((idx, entry)) = entries.next() {
            write_raw_explain_entry(f, entry, idx + 1, self.should_fmt, self.show_buckets)?;

            // Since raw explain entries don't include a trailing newline,
            // the first writeln! terminates the previous entry's last line,
            // and the second writeln! adds a blank separator line between entries.
            let has_next = entries.peek().is_some();
            if has_next {
                write!(f, "\n\n")?;
            }
        }

        Ok(())
    }
}

impl RawExplain {
    pub fn from_port<'p>(
        port: &mut impl Port<'p>,
        should_fmt: bool,
        show_buckets: bool,
    ) -> Result<RawExplain, SbroadError> {
        // vec![string] - query
        // vec![string] - location
        // vec![string] - buckets_repr
        // vec![string] - sql
        // vec![params] - params
        // vec![int] - num of raw lines
        // vec![vec[tuple]] - raw
        let mut port_iter = port.iter();
        let mut entries = Vec::new();
        while let Some(query_mp) = port_iter.next() {
            let query_wrapped: Vec<String> = msgpack::decode(query_mp).map_err(|err| {
                SbroadError::Other(format_smolstr!("unable to decode query: {err}"))
            })?;
            let query = query_wrapped[0].clone();

            let location_mp = port_iter.next().expect("location must be in port");
            let location_wrapped: Vec<String> = msgpack::decode(location_mp).map_err(|err| {
                SbroadError::Other(format_smolstr!("unable to decode location: {err}"))
            })?;
            let location = location_wrapped[0].clone();

            let buckets_mp = port_iter.next().expect("buckets must be in port");
            let buckets_wrapped: Vec<String> = msgpack::decode(buckets_mp).map_err(|err| {
                SbroadError::Other(format_smolstr!("unable to decode buckets: {err}"))
            })?;
            let buckets = buckets_wrapped[0].clone();

            let sql_mp = port_iter.next().expect("sql query must be in port");
            let sql_wrapped: Vec<String> = msgpack::decode(sql_mp).map_err(|err| {
                SbroadError::Other(format_smolstr!("unable to decode sql query: {err}"))
            })?;
            let sql = sql_wrapped[0].clone();

            let params_mp = port_iter.next().expect("params must be in port");
            let params: Vec<Value> = msgpack::decode(params_mp).map_err(|err| {
                SbroadError::Other(format_smolstr!("unable to decode params: {err}"))
            })?;

            let num_mp = port_iter.next().expect("num must be in port");
            let num_wrapped: Vec<usize> = msgpack::decode(num_mp).map_err(|err| {
                SbroadError::Other(format_smolstr!(
                    "unable to decode the number of rows: {err}"
                ))
            })?;
            let num = num_wrapped[0];

            let items = &mut port_iter;
            let mut tuples: Result<Vec<RawExplainTuple>, String> = items
                .take(num)
                .map(RawExplainTuple::try_decode_from_mp)
                .collect();

            // Provide a fallback for empty raw plans.
            if let Ok(items) = &mut tuples {
                if items.is_empty() {
                    items.push(RawExplainTuple {
                        selectid: 0,
                        order: 0,
                        from: 0,
                        detail: "TRIVIAL".into(),
                    });
                }
            }

            entries.push(RawExplainEntry {
                query,
                location,
                buckets,
                sql,
                params,
                tuples,
            });
        }

        Ok(Self {
            entries,
            should_fmt,
            show_buckets,
        })
    }
}

#[cfg(feature = "mock")]
#[cfg(test)]
pub mod tests;
