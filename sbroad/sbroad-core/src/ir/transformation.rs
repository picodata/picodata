//! Plan tree transformation module.
//!
//! Contains rule-based transformations.

pub mod bool_in;
pub mod cast_constants;
pub mod constant_folding;
pub mod dnf;
pub mod equality_propagation;
pub mod merge_tuples;
pub mod not_push_down;
pub mod redistribution;
pub mod split_columns;

use ahash::AHashMap;
use smol_str::format_smolstr;

use super::node::expression::{Expression, MutExpression};
use super::node::relational::{MutRelational, Relational};
use super::node::{Bound, BoundType, Over, Window};
use super::operator::OrderByEntity;
use super::tree::traversal::{PostOrderWithFilter, EXPR_CAPACITY};
use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ir::SubtreeCloner;
use crate::ir::node::{
    Alias, ArithmeticExpr, BoolExpr, Case, Cast, Join, NodeId, Row, ScalarFunction, Selection,
    Trim, UnaryExpr,
};
use crate::ir::operator::Bool;
use crate::ir::{Node, Plan};

pub type ExprId = NodeId;
pub type RelationId = NodeId;

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
    pub fn transform_expr_trees(&mut self, f: TransformFunctionOldNew) -> Result<(), SbroadError> {
        let top_id = self.get_top()?;
        let filter = |id: NodeId| -> bool {
            matches!(
                self.get_node(id),
                Ok(Node::Relational(Relational::Join(_)))
                    | Ok(Node::Relational(Relational::Selection(_)))
            )
        };
        let mut ir_tree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.rel_iter(node),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        ir_tree.populate_nodes(top_id);
        let nodes = ir_tree.take_nodes();
        drop(ir_tree);
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
        let filter = |node_id: NodeId| -> bool {
            if let Ok(Node::Expression(
                Expression::Bool(_)
                | Expression::Arithmetic(_)
                | Expression::Alias(_)
                | Expression::Row(_)
                | Expression::Cast(_)
                | Expression::Case(_)
                | Expression::ScalarFunction(_)
                | Expression::Unary(_),
            )) = self.get_node(node_id)
            {
                return true;
            }
            false
        };
        let mut subtree = PostOrderWithFilter::with_capacity(
            |node_id| self.nodes.expr_iter(node_id, false),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        subtree.populate_nodes(top_id);
        let nodes = subtree.take_nodes();
        drop(subtree);
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
            let expr = self.get_mut_expression_node(id)?;
            // For all expressions in the subtree tries to replace their children
            // with the new nodes from the map.
            //
            // XXX: If you add a new expression type to the match, make sure to
            // add it to the filter above.
            match expr {
                MutExpression::Window(Window {
                    partition,
                    ordering,
                    frame,
                    ..
                }) => {
                    if let Some(partition) = partition {
                        for id in partition {
                            map.replace(id);
                        }
                    }
                    if let Some(ordering) = ordering {
                        for o_elem in ordering {
                            if let OrderByEntity::Expression { mut expr_id } = o_elem.entity {
                                map.replace(&mut expr_id);
                            }
                        }
                    }
                    if let Some(frame) = frame {
                        match &frame.bound {
                            Bound::Single(bound) => {
                                if let BoundType::PrecedingOffset(mut node_id)
                                | BoundType::FollowingOffset(mut node_id) = bound
                                {
                                    map.replace(&mut node_id);
                                }
                            }
                            Bound::Between(bound_from, bound_to) => {
                                for bound in &[bound_from, bound_to] {
                                    if let BoundType::PrecedingOffset(mut node_id)
                                    | BoundType::FollowingOffset(mut node_id) = bound
                                    {
                                        map.replace(&mut node_id);
                                    }
                                }
                            }
                        }
                    }
                }
                MutExpression::Over(Over {
                    stable_func,
                    filter,
                    window,
                    ..
                }) => {
                    map.replace(stable_func);
                    if let Some(filter) = filter {
                        map.replace(filter);
                    }
                    map.replace(window);
                }
                MutExpression::Alias(Alias { child, .. })
                | MutExpression::Cast(Cast { child, .. })
                | MutExpression::Unary(UnaryExpr { child, .. }) => {
                    map.replace(child);
                }
                MutExpression::Case(Case {
                    search_expr,
                    when_blocks,
                    else_expr,
                }) => {
                    if let Some(search_expr) = search_expr {
                        map.replace(search_expr);
                    }
                    for (cond_expr, res_expr) in when_blocks {
                        map.replace(cond_expr);
                        map.replace(res_expr);
                    }
                    if let Some(else_expr) = else_expr {
                        map.replace(else_expr);
                    }
                }
                MutExpression::Bool(BoolExpr { left, right, .. })
                | MutExpression::Arithmetic(ArithmeticExpr { left, right, .. }) => {
                    map.replace(left);
                    map.replace(right);
                }
                MutExpression::Trim(Trim {
                    pattern, target, ..
                }) => {
                    if let Some(pattern) = pattern {
                        map.replace(pattern);
                    }
                    map.replace(target);
                }
                MutExpression::Row(Row { list, .. })
                | MutExpression::ScalarFunction(ScalarFunction { children: list, .. }) => {
                    for id in list {
                        map.replace(id);
                    }
                }
                MutExpression::Concat(_)
                | MutExpression::Constant(_)
                | MutExpression::Like(_)
                | MutExpression::Reference(_)
                | MutExpression::SubQueryReference(_)
                | MutExpression::CountAsterisk(_)
                | MutExpression::Timestamp(_)
                | MutExpression::Parameter(_) => {}
            }
        }

        Ok(TransformationOldNewPair {
            old_id: remember_old_top_id,
            new_id: top_id,
        })
    }
}

#[cfg(feature = "mock")]
#[cfg(test)]
pub mod helpers;
