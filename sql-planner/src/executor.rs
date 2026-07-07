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
use crate::executor::engine::{BlockQuery, Router, Vshard};
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
use bitflags::bitflags;
use smol_str::{format_smolstr, SmolStr};
use sql_protocol::dml::insert::ConflictPolicy;
use std::collections::HashMap;
use std::fmt::{self, Write as _};
use std::io;
use std::iter::Peekable;
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

/// A pass in the [`Plan::optimize_before`] transformation pipeline, used as a
/// stop point when running the pipeline only partway (e.g. in tests):
/// `optimize_before(Stage::X)` runs everything up to but *not including* `X`.
/// The variants are listed in the order the passes run.
//
// Only referenced by the `cfg(test)` early-returns in `optimize_before`, so the
// pass variants read as dead code in production builds.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stage {
    ReplaceInOperator,
    PushDownNot,
    CastConstants,
    FoldBooleanTree,
    SplitColumns,
    Restrictions,
    Dnf,
    EqualityFacts,
    MergeTuples,
    AddMotions,
    UpdateSubstring,
    MarkUniqueParameters,
    /// Terminal sentinel: comes after every pass, so `optimize_before(End)`
    /// stops before nothing and runs the whole pipeline. This is what
    /// production (`optimize_subtree`) passes.
    End,
}

impl Plan {
    /// Apply optimization rules to the plan.
    ///
    /// # Errors
    /// - Failed to optimize the plan.
    pub fn optimize(self) -> Result<Self, SbroadError> {
        let top_id = self.get_top()?;
        self.optimize_subtree(top_id)
    }

    /// Run the optimizer transformation passes in order, stopping *right before*
    /// `stop` runs — so `stop` itself is NOT applied. This is the single source
    /// of truth for the pass sequence: [`Plan::optimize_subtree`] runs the whole
    /// chain, while a test uses it to build the input for the pass it exercises
    /// (e.g. `optimize_before(top, Stage::Dnf)`), then runs that pass itself and
    /// inspects the result. Deriving the prefix here keeps the test input in
    /// lockstep with production and robust to pass reordering.
    ///
    /// # Errors
    /// - A pass failed.
    // Outside test builds `stop` is always `Stage::End` (from `optimize_subtree`)
    // and the early-returns are compiled out, so production runs a plain pass
    // chain. The param is only honored under `cfg(test)`.
    #[cfg_attr(not(test), allow(unused_variables))]
    pub(crate) fn optimize_before(self, top_id: NodeId, stop: Stage) -> Result<Self, SbroadError> {
        let mut plan = self;
        // Each `stage!`, in test builds, returns the current plan right before
        // its pass would run. The order of these lines *is* the pipeline order.
        // `plan` is threaded through as an ident arg so the assignment target and
        // the `$call` both bind to this function's `plan` (macro hygiene).
        macro_rules! stage {
            ($plan:ident, $s:expr, $call:expr) => {
                #[cfg(test)]
                if stop == $s {
                    return Ok($plan);
                }
                $plan = $call;
            };
        }

        stage!(
            plan,
            Stage::ReplaceInOperator,
            plan.replace_in_operator_in_subtree(top_id)?
        );
        stage!(
            plan,
            Stage::PushDownNot,
            plan.push_down_not_in_subtree(top_id)?
        );
        // In the case if the query was not fully parameterized
        // and contains some constants, lets apply constant folding.
        stage!(plan, Stage::CastConstants, plan.cast_constants()?);
        stage!(plan, Stage::FoldBooleanTree, plan.fold_boolean_tree()?);
        stage!(
            plan,
            Stage::SplitColumns,
            plan.split_columns_in_subtree(top_id)?
        );
        // Build per-node restrictions over the raw boolean tree (before DNF blow-up).
        stage!(
            plan,
            Stage::Restrictions,
            plan.analyze_restrictions_in_subtree(top_id)?
        );
        stage!(plan, Stage::Dnf, plan.set_dnf_in_subtree(top_id)?);
        stage!(
            plan,
            Stage::EqualityFacts,
            plan.analyze_equality_facts_in_subtree(top_id)?
        );
        stage!(
            plan,
            Stage::MergeTuples,
            plan.merge_tuples_in_subtree(top_id)?
        );
        stage!(
            plan,
            Stage::AddMotions,
            plan.add_motions_to_subtree(top_id)?
        );
        stage!(plan, Stage::UpdateSubstring, plan.update_substring()?);
        // After all transformations we can finally determine what parameters are unique.
        stage!(
            plan,
            Stage::MarkUniqueParameters,
            plan.mark_unique_parameters()?
        );

        Ok(plan)
    }

    pub fn optimize_subtree(self, top_id: NodeId) -> Result<Self, SbroadError> {
        let mut plan = self.optimize_before(top_id, Stage::End)?;

        // Facts are only used during planning. Afterward they're dead state. Drop them
        // so they don't bloat the plan clones made on the execution/dispatch path.
        plan.facts = None;
        // Restrictions are planning-only too. Clear them
        // for the same reason.
        plan.restrictions = None;

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
    exec_plan: ExecutionPlan,
    /// Coordinator runtime
    coordinator: &'a C,
    /// Bucket map of view { plan output_id (Expression::Row) -> `Buckets` }.
    /// It's supposed to denote relational nodes' output buckets destination.
    bucket_map: HashMap<NodeId, Buckets>,
    exec_ctx: ExecutionContext,
}

/// Helper struct which holds the query execution location.
#[derive(Debug)]
pub enum ExplainQueryLocation {
    /// Query is executed exactly on N replicasets.
    ConstFiltered { fraction: (usize, usize) },
    /// Query execution replicasets are computed in runtime. In case when it
    /// is possible to calculate an upper bound, estimation "<= N/M" is added.
    DynFiltered { fraction: Option<(usize, usize)> },
    /// Query is executed locally.
    Router,
    /// Query is executed on every replicaset.
    Whole,
}

impl std::fmt::Display for ExplainQueryLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExplainQueryLocation::ConstFiltered { fraction } => {
                write!(f, "CONST-FILTERED STORAGE, {}/{}", fraction.0, fraction.1)
            }
            ExplainQueryLocation::DynFiltered { fraction } if fraction.is_some() => {
                let fraction = fraction.unwrap();
                write!(f, "DYN-FILTERED STORAGE, <= {}/{}", fraction.0, fraction.1)
            }
            ExplainQueryLocation::DynFiltered { .. } => write!(f, "DYN-FILTERED STORAGE"),
            ExplainQueryLocation::Router => write!(f, "ROUTER"),
            ExplainQueryLocation::Whole => write!(f, "WHOLE STORAGE"),
        }
    }
}

/// Helper struct which is used for EXPLAIN (RAW) output generation.
#[derive(Clone, Copy)]
pub struct MotionInfo {
    ///  If subtree has segment motion, its buckets are calculated from the
    ///  contents of that motion virtual table. Save that to further reflect in
    ///  EXPLAIN (RAW).
    pub has_segment_motion: bool,
    ///  `SerializeAsEmpty` is a motion opcode. If it is present in subtree,
    ///  there could possibly be generated two different local SQLs. The meaning
    ///  of possible values:
    ///  * `None` - motion subtree does not contain `SerializeAsEmpty` opcode.
    ///  * `Some(true)` - the generated SQL from such subtree is going to be simple scan
    ///    of sharded table:
    ///    `SELECT "t"."a" FROM "t" UNION ALL select cast(null as int) as "b" where false`.
    ///  * `Some(false)` - the generated SQL performs UNION(UNION ALL) of global and
    ///    sharded tables:
    ///    `SELECT * FROM t UNION ALL SELECT * FROM g`
    pub has_serialize_as_empty_opcode: Option<bool>,
}

impl MotionInfo {
    // Queries in transactional blocks can not
    // have motions.
    pub fn new_for_transaction() -> Self {
        Self {
            has_segment_motion: false,
            has_serialize_as_empty_opcode: None,
        }
    }

    pub fn new_for_query(
        has_segment_motion: bool,
        has_serialize_as_empty_opcode: Option<bool>,
    ) -> Self {
        Self {
            has_segment_motion,
            has_serialize_as_empty_opcode,
        }
    }
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
    ) -> Result<Vec<BlockStatement<(BlockQuery, Vec<Value>)>>, SbroadError> {
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
                generate_pattern_with_params_for_block(&self.exec_plan, id, block_bucket, false)
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
                                       query: &(BlockQuery, Vec<Value>),
                                       kind: &str| {
                    let (query, params) = query;
                    let motion_info = MotionInfo::new_for_transaction();
                    let source = C::build_explain_query_location(&buckets, &motion_info);
                    write_explain_header2!(buf, "{}. {} ({source})", stmt_idx + 1, kind).unwrap();
                    writeln!(buf).unwrap();

                    let sql = format_sql(&query.pattern, params, should_fmt);
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
            BucketsInfo::Calculated {
                bounded_buckets, ..
            } => self
                .get_coordinator()
                .get_possible_forward_option(&bounded_buckets.buckets, &mut None)?,
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
        let mut format_options = RawExplainOptions::empty();
        if explain_options.contains(ExplainOptions::Fmt) {
            format_options.insert(RawExplainOptions::Fmt);
        }
        let is_block = self.get_exec_plan().get_ir_plan().is_block()?;
        if explain_options.contains(ExplainOptions::Buckets) && !is_block {
            format_options.insert(RawExplainOptions::ShowBuckets);
        }

        let raw_explain = RawExplain::from_port(port, format_options)?;
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
enum RawExplainEntry {
    Multiple(Vec<QueryEntry>),
    Single(QueryEntry),
}

#[derive(Debug)]
struct QueryEntry {
    query: String,
    location: String,
    buckets: String,
    sql: String,
    params: Vec<Value>,
    tuples: Result<Vec<RawExplainTuple>, String>,
}

impl QueryEntry {
    fn decode_entry<'p>(
        port_iter: &mut Peekable<impl Iterator<Item = &'p [u8]>>,
    ) -> Result<QueryEntry, SbroadError> {
        let query_mp = port_iter.next().expect("query must be in port");
        let query_wrapped: Vec<String> = msgpack::decode(query_mp)
            .map_err(|err| SbroadError::Other(format_smolstr!("unable to decode query: {err}")))?;
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
        let params: Vec<Value> = msgpack::decode(params_mp)
            .map_err(|err| SbroadError::Other(format_smolstr!("unable to decode params: {err}")))?;

        let num_mp = port_iter.next().expect("num must be in port");
        let num_wrapped: Vec<usize> = msgpack::decode(num_mp).map_err(|err| {
            SbroadError::Other(format_smolstr!(
                "unable to decode the number of rows: {err}"
            ))
        })?;
        let num = num_wrapped[0];

        let mut tuples: Result<Vec<RawExplainTuple>, String> = port_iter
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

        Ok(QueryEntry {
            query,
            location,
            buckets,
            sql,
            params,
            tuples,
        })
    }
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

struct ExplainIndex(usize, Option<usize>);

impl std::fmt::Display for ExplainIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(idx) = self.1 {
            write!(f, "{}.{idx}.", self.0)
        } else {
            write!(f, "{}.", self.0)
        }
    }
}

fn write_raw_explain_entry(
    f: &mut fmt::Formatter<'_>,
    entry: &QueryEntry,
    idx: ExplainIndex,
    format_options: RawExplainOptions,
) -> fmt::Result {
    let should_fmt = format_options.contains(RawExplainOptions::Fmt);
    let sql = format_sql(&entry.sql, &entry.params, should_fmt);
    let plan = match &entry.tuples {
        Ok(tuples) => format_raw_plan(tuples, should_fmt),
        Err(err) => err.clone(),
    };

    let (kind, source) = (&entry.query, &entry.location);

    write_explain_header2!(f, "{idx} {kind} ({source})")?;
    write!(f, "\n{sql}\n\n")?;
    write!(f, "plan:\n{plan}")?;

    let show_buckets = format_options.contains(RawExplainOptions::ShowBuckets);
    if show_buckets {
        write!(f, "\n\n{}", entry.buckets)?;
    }

    Ok(())
}

bitflags! {
    /// Helper struct which specifies the options of `RawExplain` formatting.
    #[derive(Clone, Copy, Debug)]
    struct RawExplainOptions: u8 {
        const ShowBuckets = 1;
        const Fmt = 1 << 1;
    }
}

#[derive(Debug)]
struct RawExplain {
    entries: Vec<RawExplainEntry>,
    format_options: RawExplainOptions,
}

impl fmt::Display for RawExplain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entries = self.entries.iter().enumerate().peekable();
        while let Some((idx, entry)) = entries.next() {
            match entry {
                RawExplainEntry::Single(entry) => {
                    write_raw_explain_entry(
                        f,
                        entry,
                        ExplainIndex(idx + 1, None),
                        self.format_options,
                    )?;
                }
                RawExplainEntry::Multiple(entries) => {
                    let mut entry_iter = entries.iter().enumerate().peekable();
                    while let Some((i, entry)) = entry_iter.next() {
                        write_raw_explain_entry(
                            f,
                            entry,
                            ExplainIndex(idx + 1, Some(i + 1)),
                            self.format_options,
                        )?;

                        let has_next = entry_iter.peek().is_some();
                        if has_next {
                            write!(f, "\n\n")?;
                        }
                    }
                }
            };

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
        format_options: RawExplainOptions,
    ) -> Result<RawExplain, SbroadError> {
        let mut port_iter = port.iter().peekable();
        let mut explain_entries = Vec::new();
        while let Some(mp) = port_iter.peek() {
            if let Ok(num_of_entries_wrapped) = msgpack::decode::<Vec<usize>>(mp)
                .map_err(|err| SbroadError::Other(format_smolstr!("unable to decode query: {err}")))
            {
                // Skip the value since it's been already handled.
                let _ = port_iter.next().expect("peek() returned true");

                let num_of_entries = num_of_entries_wrapped[0];
                let mut entries = Vec::new();
                for _ in 0..num_of_entries {
                    let entry = QueryEntry::decode_entry(&mut port_iter)?;
                    entries.push(entry);
                }

                explain_entries.push(RawExplainEntry::Multiple(entries));
            } else {
                let entry = QueryEntry::decode_entry(&mut port_iter)?;
                explain_entries.push(RawExplainEntry::Single(entry));
            }
        }

        Ok(Self {
            entries: explain_entries,
            format_options,
        })
    }
}

#[cfg(feature = "mock")]
#[cfg(test)]
pub mod tests;
