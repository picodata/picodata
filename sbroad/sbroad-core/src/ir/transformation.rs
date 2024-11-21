//! Plan tree transformation module.
//!
//! Contains rule-based transformations.

pub mod bool_in;
pub mod dnf;
pub mod equality_propagation;
pub mod merge_tuples;
pub mod not_push_down;
pub mod redistribution;
pub mod split_columns;

use smol_str::format_smolstr;

use super::node::expression::{Expression, MutExpression};
use super::node::relational::{MutRelational, Relational};
use super::tree::traversal::{PostOrder, PostOrderWithFilter, EXPR_CAPACITY};
use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ir::SubtreeCloner;
use crate::ir::node::{
    Alias, ArithmeticExpr, BoolExpr, Case, Cast, ExprInParentheses, Join, NodeId, Row, Selection,
    StableFunction, Trim, UnaryExpr,
};
use crate::ir::operator::Bool;
use crate::ir::{Node, Plan};
use std::collections::HashMap;

pub type ExprId = NodeId;
/// Helper struct representing map of (`old_expr_id` -> `changed_expr_id`).
struct OldNewExpressionMap {
    inner: HashMap<ExprId, ExprId>,
}

impl OldNewExpressionMap {
    fn new() -> Self {
        OldNewExpressionMap {
            inner: HashMap::new(),
        }
    }

    fn insert(&mut self, old_id: ExprId, new_id: ExprId) {
        self.inner.insert(old_id, new_id);
    }

    fn replace(&self, child: &mut ExprId) {
        if let Some(new_id) = self.inner.get(child) {
            *child = *new_id;
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn get(&self, key: ExprId) -> Option<&ExprId> {
        self.inner.get(&key)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Pair of (old tree id, transformed tree id).
pub type OldNewTopIdPair = (ExprId, ExprId);

/// Function passed for being applied on WHERE|ON condition roots.
/// It returns pair of (`old_tree_id`, `new_tree_id`).
pub type TransformFunction<'func> =
    &'func dyn Fn(&mut Plan, ExprId) -> Result<OldNewTopIdPair, SbroadError>;

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

    /// Transforms all expression subtrees in the plan
    /// for selection and join nodes using the
    /// defined function.
    ///
    /// # Errors
    /// - If failed to get the plan top node.
    /// - If relational iterator returns non-relational node.
    /// - If failed to transform the expression subtree.
    pub fn transform_expr_trees(&mut self, f: TransformFunction) -> Result<(), SbroadError> {
        let top_id = self.get_top()?;
        let mut ir_tree = PostOrder::with_capacity(|node| self.nodes.rel_iter(node), EXPR_CAPACITY);
        ir_tree.populate_nodes(top_id);
        let nodes = ir_tree.take_nodes();
        for level_node in &nodes {
            let id = level_node.1;
            let rel = self.get_relation_node(id)?;
            let (old_tree_id, new_tree_id) = match rel {
                Relational::Selection(Selection {
                    filter: tree_id, ..
                })
                | Relational::Join(Join {
                    condition: tree_id, ..
                }) => f(self, *tree_id)?,
                _ => continue,
            };
            if old_tree_id != new_tree_id {
                self.undo.add(new_tree_id, old_tree_id);
            }
            let rel = self.get_mut_relation_node(id)?;
            match rel {
                MutRelational::Selection(Selection {
                    filter: tree_id, ..
                })
                | MutRelational::Join(Join {
                    condition: tree_id, ..
                }) => {
                    *tree_id = new_tree_id;
                }
                _ => continue,
            }
        }
        Ok(())
    }

    /// Replaces boolean operators in an expression subtree with the
    /// new boolean expressions produced by the user defined function.
    ///
    /// # Errors
    ///  If expression subtree iterator returns a non-expression node.
    #[allow(clippy::too_many_lines)]
    pub fn expr_tree_replace_bool(
        &mut self,
        top_id: NodeId,
        f: TransformFunction,
        ops: &[Bool],
    ) -> Result<OldNewTopIdPair, SbroadError> {
        let mut map = OldNewExpressionMap::new();
        // Note, that filter accepts nodes:
        // * On which we'd like to apply transformation
        // * That will contain transformed nodes as children
        let filter = |node_id: NodeId| -> bool {
            if let Ok(Node::Expression(
                Expression::Bool(_)
                | Expression::ExprInParentheses(_)
                | Expression::Arithmetic(_)
                | Expression::Alias(_)
                | Expression::Row(_)
                | Expression::Cast(_)
                | Expression::Case(_)
                | Expression::StableFunction(_)
                | Expression::Unary(_),
            )) = self.get_node(node_id)
            {
                return true;
            }
            false
        };
        let mut subtree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.expr_iter(node, false),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        subtree.populate_nodes(top_id);
        let nodes = subtree.take_nodes();
        drop(subtree);
        for level_node in &nodes {
            let id = level_node.1;
            let expr = self.get_expression_node(id)?;
            if let Expression::Bool(BoolExpr { op, .. }) = expr {
                if ops.contains(op) || ops.is_empty() {
                    let (old_top_id, new_top_id) = f(self, id)?;
                    if old_top_id != new_top_id {
                        map.insert(id, new_top_id);
                    }
                }
            }
        }
        let (old_top_id, new_top_id) = if map.is_empty() {
            (top_id, top_id)
        } else {
            let old_top_id = if map.get(top_id).is_some() && map.len() == 1 {
                top_id
            } else {
                SubtreeCloner::clone_subtree(self, top_id)?
            };
            let mut new_top_id = top_id;
            for level_node in &nodes {
                let id = level_node.1;
                let expr = self.get_mut_expression_node(id)?;
                // For all expressions in the subtree tries to replace their children
                // with the new nodes from the map.
                //
                // XXX: If you add a new expression type to the match, make sure to
                // add it to the filter above.
                match expr {
                    MutExpression::Alias(Alias { child, .. })
                    | MutExpression::ExprInParentheses(ExprInParentheses { child, .. })
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
                    | MutExpression::StableFunction(StableFunction { children: list, .. }) => {
                        for id in list {
                            map.replace(id);
                        }
                    }
                    MutExpression::Concat(_)
                    | MutExpression::Constant(_)
                    | MutExpression::Like(_)
                    | MutExpression::Reference(_)
                    | MutExpression::CountAsterisk(_) => {}
                }
            }
            // Checks if the top node is a new node.
            if let Some(new_id) = map.get(top_id) {
                new_top_id = *new_id;
            }
            (old_top_id, new_top_id)
        };
        Ok((old_top_id, new_top_id))
    }
}

#[cfg(test)]
pub mod helpers;
