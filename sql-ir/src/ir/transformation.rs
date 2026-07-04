//! Plan tree transformation module.
//!
//! Contains rule-based transformations.

mod bool_in;
mod cast_constants;
mod constant_folding;
mod dnf;
pub mod equality_facts;
mod merge_tuples;
mod not_push_down;
pub mod redistribution;
pub mod restriction;
mod split_columns;

use ahash::AHashMap;
use smol_str::format_smolstr;

use super::node::expression::Expression;
use super::node::relational::{MutRelational, Relational};
use super::tree::traversal::{PostOrder, PostOrderWithFilter, EXPR_CAPACITY, REL_CAPACITY};
use crate::errors::{Entity, SbroadError};
use crate::ir::node::block::{Block, BlockOwned, MutBlock};
use crate::ir::node::{
    AnonymousBlock, BlockEntriesMut, BoolExpr, Insert, Join, Motion, NodeId, Selection, Values,
    ValuesRow,
};
use crate::ir::operator::Bool;
use crate::ir::options::Options;
use crate::ir::subtree_cloner::SubtreeCloner;
use crate::ir::transformation::redistribution::{MotionPolicy, Target};
use crate::ir::value::Value;
use crate::ir::{Node, Plan};

pub type ExprId = NodeId;
type RelationId = NodeId;

/// Pair of:
/// * node which should be considered an old version of expression tree
///   (sometimes it's a cloned subtree of previous old tree)
/// * node that is a new version of expression tree after transformation
///   application
pub struct TransformationOldNewPair {
    old_id: ExprId,
    new_id: ExprId,
}

/// Helper struct representing map of (`old_expr_id` -> `new_expr_id`)
/// used during transformation application.
///
/// Because of the borrow checker we can't change children during recursive
/// traversal and have to do it using this map after it's collected.
struct OldNewTransformationMap {
    /// Map of { initial_expression_node -> transformed_expression }.
    inner: AHashMap<ExprId, ExprId>,
}

impl OldNewTransformationMap {
    fn new() -> Self {
        OldNewTransformationMap {
            inner: AHashMap::new(),
        }
    }

    fn insert(&mut self, old_id: ExprId, new_id: ExprId) {
        self.inner.insert(old_id, new_id);
    }

    fn replace(&self, old_id: &mut ExprId) {
        if let Some(new_id) = self.inner.get(old_id) {
            *old_id = *new_id;
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn get(&self, key: ExprId) -> Option<&ExprId> {
        self.inner.get(&key)
    }
}

/// Function passed for being applied on WHERE/ON condition expressions.
/// Returns pair of (old_top_id, new_top_id).
pub type TransformFunctionOldNew<'func> =
    &'func dyn Fn(&mut Plan, RelationId, ExprId) -> Result<TransformationOldNewPair, SbroadError>;

/// Function passed for being applied on WHERE/ON condition expressions.
/// Returns only new_top_id (unlike `TransformFunctionOldNew`).
pub type TransformFunctionNew<'func> =
    &'func dyn Fn(&mut Plan, ExprId) -> Result<ExprId, SbroadError>;

impl Plan {
    /// Concatenates trivalents (boolean or NULL expressions) to the AND node.
    ///
    /// # Errors
    /// - If the left or right child is not a trivalent.
    pub fn concat_and(
        &mut self,
        left_expr_id: NodeId,
        right_expr_id: NodeId,
    ) -> Result<NodeId, SbroadError> {
        if !self.is_trivalent(left_expr_id)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format_smolstr!(
                    "Left expression is not a boolean expression or NULL: {:?}",
                    self.get_expression_node(left_expr_id)?
                )),
            ));
        }
        if !self.is_trivalent(right_expr_id)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format_smolstr!(
                    "Right expression is not a boolean expression or NULL: {:?}",
                    self.get_expression_node(right_expr_id)?
                )),
            ));
        }
        self.add_cond(left_expr_id, Bool::And, right_expr_id)
    }

    /// Concatenates trivalents (boolean or NULL expressions) to the OR node.
    ///
    /// # Errors
    /// - If the left or right child is not a trivalent.
    pub fn concat_or(
        &mut self,
        left_expr_id: NodeId,
        right_expr_id: NodeId,
    ) -> Result<NodeId, SbroadError> {
        if !self.is_trivalent(left_expr_id)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format_smolstr!(
                    "left expression is not a boolean expression or NULL: {:?}",
                    self.get_expression_node(left_expr_id)?
                )),
            ));
        }
        if !self.is_trivalent(right_expr_id)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format_smolstr!(
                    "right expression is not a boolean expression or NULL: {:?}",
                    self.get_expression_node(right_expr_id)?
                )),
            ));
        }
        self.add_cond(left_expr_id, Bool::Or, right_expr_id)
    }

    /// Apply given transformation to all expressions that are:
    /// * Join conditions
    /// * Selection filters
    pub fn transform_expr_trees(
        &mut self,
        top_id: NodeId,
        f: TransformFunctionOldNew,
    ) -> Result<(), SbroadError> {
        let ir_tree = PostOrderWithFilter::new(
            |node| self.nodes.rel_iter(node),
            |node| {
                matches!(
                    self.get_node(node),
                    Ok(Node::Relational(Relational::Join(_)))
                        | Ok(Node::Relational(Relational::Selection(_)))
                )
            },
            EXPR_CAPACITY,
        );
        let nodes = ir_tree.traverse_into_vec(top_id);
        for level_node in &nodes {
            let id: NodeId = level_node.1;
            let rel: Relational<'_> = self.get_relation_node(id)?;
            let tree_id = match rel {
                Relational::Selection(Selection {
                    filter: tree_id, ..
                })
                | Relational::Join(Join {
                    condition: tree_id, ..
                }) => *tree_id,
                _ => {
                    unreachable!("Selection or Join nodes expected for transformation application")
                }
            };
            let TransformationOldNewPair { old_id, new_id } = f(self, id, tree_id)?;

            if old_id == new_id {
                // Nothing has changed.
                continue;
            }

            self.undo.add(old_id, new_id);

            let rel = self.get_mut_relation_node(id)?;
            match rel {
                MutRelational::Selection(Selection {
                    filter: tree_id, ..
                })
                | MutRelational::Join(Join {
                    condition: tree_id, ..
                }) => {
                    *tree_id = new_id;
                }
                _ => {
                    unreachable!("Selection or Join nodes expected for transformation application")
                }
            }
        }
        Ok(())
    }

    /// Replaces boolean operators in an expression subtree with the
    /// new boolean expressions produced by the user defined function.
    ///
    /// `ops` arguments is a set of boolean operators which we'd like
    /// to transform. In case it's empty, transformation would be applied
    /// to every boolean operator.
    #[allow(clippy::too_many_lines)]
    pub fn expr_tree_replace_bool(
        &mut self,
        top_id: NodeId,
        f: TransformFunctionNew,
        ops: &[Bool],
    ) -> Result<TransformationOldNewPair, SbroadError> {
        let mut map: OldNewTransformationMap = OldNewTransformationMap::new();
        // TODO: Review nodes that are present in the filter. Seems like
        //       we don't need most of them (transformations under them won't
        //       influence filtration and distribution).
        //
        // Note, that filter accepts nodes:
        // * On which we'd like to apply transformation
        // * That will contain transformed nodes as children
        let filter_certain_exprs = |node_id| {
            matches!(
                self.get_node(node_id),
                Ok(Node::Expression(
                    Expression::Bool(_)
                        | Expression::Arithmetic(_)
                        | Expression::Alias(_)
                        | Expression::Row(_)
                        | Expression::Cast(_)
                        | Expression::Case(_)
                        | Expression::ScalarFunction(_)
                        | Expression::Unary(_),
                ))
            )
        };
        let subtree = PostOrderWithFilter::new(
            |node_id| self.nodes.expr_iter(node_id, false),
            filter_certain_exprs,
            EXPR_CAPACITY,
        );
        let nodes = subtree.traverse_into_vec(top_id);
        for level_node in &nodes {
            let bool_id = level_node.1;
            let expr = self.get_expression_node(bool_id)?;
            if let Expression::Bool(BoolExpr { op, .. }) = expr {
                if ops.contains(op) || ops.is_empty() {
                    let new_bool_id = f(self, bool_id)?;
                    if bool_id != new_bool_id {
                        map.insert(bool_id, new_bool_id);
                    }
                }
            }
        }

        if map.is_empty() {
            // None of boolean nodes were changed.
            return Ok(TransformationOldNewPair {
                old_id: top_id,
                new_id: top_id,
            });
        };

        if let Some(new_top_id) = map.get(top_id) {
            // Passed `top_id` is a boolean node that was transformed.
            // It's assumed that new tree doesn't reuse node from
            // previous tree.
            return Ok(TransformationOldNewPair {
                old_id: top_id,
                new_id: *new_top_id,
            });
        }

        // * Top node wasn't changed or
        // * There were other changes other then top_id.
        //
        // In both cases we have to clone subtree because we
        // are going to apply transformations from the `map` to
        // already existing one.
        let remember_old_top_id = SubtreeCloner::clone_subtree(self, top_id)?;

        // Traverse top id and fix references got from the map.
        for level_node in &nodes {
            let id = level_node.1;
            let mut expr = self.get_mut_expression_node(id)?;
            for child in expr.expr_children_mut() {
                map.replace(child);
            }
        }

        Ok(TransformationOldNewPair {
            old_id: remember_old_top_id,
            new_id: top_id,
        })
    }
}

/// A pass in the [`Plan::optimize_before`] transformation pipeline, used as a
/// stop point when running the pipeline only partway (e.g. in tests):
/// `optimize_before(Stage::X)` runs everything up to but *not including* `X`.
/// The variants are listed in the order the passes run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
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

    /// The parse-time half of the statement compilation pipeline: validate raw
    /// options and optimize DQL/DML plans and anonymous blocks. Other statement
    /// kinds pass through unchanged.
    ///
    /// This is the single definition of the sequence shared by
    /// `PreparedStatement::parse` in the `sql-planner` facade and the mock
    /// `ExecutingQueryExt` test helper, so the two cannot drift.
    ///
    /// # Errors
    /// - Invalid options or a failed optimizer pass.
    pub fn optimize_statement(mut self) -> Result<Self, SbroadError> {
        if self.is_dql_or_dml()? {
            self.check_raw_options()?;
            self = self.optimize()?;
        } else if self.is_block()? {
            self.check_raw_options()?;
            let top = self.get_top().expect("must be set");
            if let Block::Anonymous(_) = self.get_block_node(top)? {
                self = self.optimize_block()?;
            }
        }

        Ok(self)
    }

    /// The bind-time half of the statement compilation pipeline: resolve
    /// default timeouts, bind parameter values and run the post-bind passes.
    ///
    /// This is the single definition of the sequence shared by
    /// `PreparedStatement::bind` in the `sql-planner` facade and the mock
    /// `ExecutingQueryExt` test helper, so the two cannot drift.
    ///
    /// # Errors
    /// - Parameter binding or a post-bind pass failed.
    pub fn bind_statement(
        mut self,
        params: Vec<Value>,
        default_options: Options,
    ) -> Result<Self, SbroadError> {
        // Replace parser default timeouts with system defaults
        // from ALTER SYSTEM settings. Must be done before bind_params
        // which consumes default_options.
        self.resolve_default_timeout(&default_options);

        if self.is_empty() {
            // Empty query, do nothing
        } else if self.is_dql_or_dml()? || self.is_block()? {
            self.bind_params(params, default_options)?;
            self = self
                .update_timestamps()?
                .cast_constants()?
                .fold_boolean_tree()?;
        }

        Ok(self)
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
    // Production (`optimize_subtree`) passes `Stage::End`, which no pass matches,
    // so the whole chain runs; the per-pass stop check is a trivial enum compare.
    // (Tests live in downstream crates now, so the check can't be `cfg(test)`.)
    pub fn optimize_before(self, top_id: NodeId, stop: Stage) -> Result<Self, SbroadError> {
        let mut plan = self;
        // Each `stage!` returns the current plan right before its pass would run
        // if that pass is the stop point. The order of these lines *is* the
        // pipeline order. `plan` is threaded through as an ident arg so the
        // assignment target and the `$call` both bind to this function's `plan`
        // (macro hygiene).
        macro_rules! stage {
            ($plan:ident, $s:expr, $call:expr) => {
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
