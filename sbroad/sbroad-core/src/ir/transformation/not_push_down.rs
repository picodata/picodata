//! Push down `Not` operator.
//!
//! # Example
//! * From: `select * from "t" where not ("a" != 1 or "b" != 2)`
//! * To:   `select * from "t" where "a" = 1 and "b" = 2`

use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ir::SubtreeCloner;
use crate::ir::node::expression::{Expression, MutExpression};
use crate::ir::node::{BoolExpr, Constant, ExprInParentheses, NodeId, Row, UnaryExpr};
use crate::ir::operator::{Bool, Unary};
use crate::ir::transformation::{OldNewExpressionMap, OldNewTopIdPair};
use crate::ir::tree::traversal::{PostOrderWithFilter, EXPR_CAPACITY};
use crate::ir::value::Value;
use crate::ir::{Node, Plan};
use smol_str::{format_smolstr, SmolStr};

/// Enum representing status of Not push down traversal.
/// It may be in two states:
/// * None -- which means we haven't met `Not` operator and don't want to negate anything.
/// * Active -- which means we want to negate current expression and pass it deeper.
enum NotState {
    /// Not doesn't work.
    ///
    /// E.g. in expression `(true and false) or true` there is no `Not` operator and the state
    /// will be `Off` during the whole tree traversal.
    Off,
    /// Not works.
    /// Contains an id of parent `Not` operator if such exists.
    ///
    /// E.g. in expression `not (true and cast(foo(x) as bool))` this status will be `On` during
    /// the whole tree traveral. Here is detailed states for each node:
    /// * and  -> On { parent_not_op: Some(id) } -> change to Or and don't pass `parent_not_op`
    ///           to children, because it has alredy been "used" for negating And itself.
    /// * true -> On { parent_not_op: None } -> change to false and do nothing.
    /// * cast -> On { parent_not_op: None } -> don't change self, because we don't know, what
    ///           value will cast return. As soon as parent_not_op is None, create one as a parent
    ///           node.
    On { parent_not_op: Option<NodeId> },
}

impl NotState {
    /// Create new `NotState::On`.
    fn on(parent_not_op: Option<NodeId>) -> Self {
        NotState::On { parent_not_op }
    }
}

fn call_expr_tree_not_push_down(
    plan: &mut Plan,
    top_id: NodeId,
) -> Result<OldNewTopIdPair, SbroadError> {
    // Because of the borrow checker we can't change `Bool` and `Row` children during recursive
    // traversal and have to do it using this map after transformation.
    let mut old_new_expression_map = OldNewExpressionMap::new();
    let new_top_id =
        plan.push_down_not_for_expression(top_id, NotState::Off, &mut old_new_expression_map)?;
    let (old_top_id, new_top_id) = if new_top_id == top_id && old_new_expression_map.is_empty() {
        (top_id, top_id)
    } else {
        let old_top_id = SubtreeCloner::clone_subtree(plan, top_id)?;
        let filter = |node_id: NodeId| -> bool {
            matches!(
                plan.get_node(node_id),
                Ok(Node::Expression(
                    Expression::ExprInParentheses(_) | Expression::Bool(_) | Expression::Row(_)
                ))
            )
        };
        let mut subtree = PostOrderWithFilter::with_capacity(
            |node| plan.nodes.expr_iter(node, false),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        subtree.populate_nodes(top_id);
        let nodes = subtree.take_nodes();
        drop(subtree);
        for level_node in &nodes {
            let id = level_node.1;
            let expr = plan.get_mut_expression_node(id)?;
            match expr {
                MutExpression::ExprInParentheses(ExprInParentheses { child }) => {
                    old_new_expression_map.replace(child);
                }
                MutExpression::Bool(BoolExpr { left, right, .. }) => {
                    old_new_expression_map.replace(left);
                    old_new_expression_map.replace(right);
                }
                MutExpression::Row(Row { list, .. }) => {
                    for id in list {
                        old_new_expression_map.replace(id);
                    }
                }
                _ => {}
            }
        }
        (old_top_id, new_top_id)
    };

    Ok((old_top_id, new_top_id))
}

impl Bool {
    /// Negate self with `Not` operator.
    ///
    /// # Returns:
    /// * `None` in case it is impossible to negate self
    /// * `Some` of pair (`negated_self`, `should_proceed_not_push_down`)
    fn negate(&self) -> Option<(Bool, bool)> {
        match self {
            Bool::Or => Some((Bool::And, true)),
            Bool::And => Some((Bool::Or, true)),
            Bool::Eq => Some((Bool::NotEq, false)),
            Bool::Gt => Some((Bool::LtEq, false)),
            Bool::GtEq => Some((Bool::Lt, false)),
            Bool::Lt => Some((Bool::GtEq, false)),
            Bool::LtEq => Some((Bool::Gt, false)),
            Bool::NotEq => Some((Bool::Eq, false)),
            Bool::In => None,
            Bool::Between => unreachable!("Between in not pushdown"),
        }
    }
}

impl Plan {
    /// Helper function to cover expression with Not operator:
    /// * `not_state` is off -> just return expression id.
    /// * `not_state` is on and parent `Not` operator is present -> return parent operator id.
    /// * `not_state` is on and parent `Not` operator is absent  -> create new not node.
    fn cover_with_not(
        &mut self,
        expr_id: NodeId,
        not_state: &NotState,
    ) -> Result<NodeId, SbroadError> {
        if let NotState::On { parent_not_op } = not_state {
            if let Some(parent_not_op) = parent_not_op {
                let parent_not_expr = self.get_mut_expression_node(*parent_not_op)?;
                if let MutExpression::Unary(UnaryExpr {
                    op: Unary::Not,
                    child,
                }) = parent_not_expr
                {
                    *child = expr_id;
                    Ok(*parent_not_op)
                } else {
                    Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some(SmolStr::from(
                            "Expected to get Unary::Not, got: {parent_not_op:?}",
                        )),
                    ))
                }
            } else {
                self.add_unary(Unary::Not, expr_id)
            }
        } else {
            Ok(expr_id)
        }
    }

    // SELECT * FROM (values (1)) where true and not (true and false)
    // SELECT "COLUMN_1" FROM (VALUES (?)) WHERE ((?) or (?))

    /// Recursive push down of `Not` operator.
    fn push_down_not_for_expression(
        &mut self,
        expr_id: NodeId,
        not_state: NotState,
        map: &mut OldNewExpressionMap,
    ) -> Result<NodeId, SbroadError> {
        let expr = self.get_expression_node(expr_id)?;
        let new_expr_id = match expr {
            Expression::ExprInParentheses(ExprInParentheses { child }) => {
                // In case we have expression `true and not (true and false)` we would like to
                // save parentheses over not child:
                // `true and false or true` != `true and (false or true)`.
                let remember_old_child = *child;
                let new_child = self.push_down_not_for_expression(*child, not_state, map)?;
                if new_child != remember_old_child {
                    map.insert(remember_old_child, new_child);
                }
                expr_id
            }
            Expression::Constant(Constant { value }) => {
                if let NotState::Off = not_state {
                    expr_id
                } else {
                    match value {
                        Value::Boolean(b) => {
                            let new_value = Value::from(!*b);
                            self.add_const(new_value)
                        }
                        Value::Null => expr_id,
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Node,
                                Some(format_smolstr!(
                                    "Unexpected constant node under Not: {expr:?}"
                                )),
                            ));
                        }
                    }
                }
            }
            Expression::Bool(BoolExpr { op, left, right }) => {
                let (remember_left, remember_right) = (*left, *right);

                if let NotState::On { .. } = not_state {
                    let negated_op = op.negate();
                    let Some((negated_op, should_proceed)) = negated_op else {
                        return self.cover_with_not(expr_id, &not_state);
                    };
                    if !should_proceed {
                        return self.add_bool(*left, negated_op, *right);
                    }
                    let negated_left =
                        self.push_down_not_for_expression(remember_left, NotState::on(None), map)?;
                    let negated_right =
                        self.push_down_not_for_expression(remember_right, NotState::on(None), map)?;
                    self.add_bool(negated_left, negated_op, negated_right)?
                } else {
                    let new_left =
                        self.push_down_not_for_expression(remember_left, NotState::Off, map)?;
                    if remember_left != new_left {
                        map.insert(remember_left, new_left);
                    }
                    let new_right =
                        self.push_down_not_for_expression(remember_right, NotState::Off, map)?;
                    if remember_right != new_right {
                        map.insert(remember_right, new_right);
                    }
                    expr_id
                }
            }
            Expression::StableFunction(_) | Expression::Cast(_) | Expression::Reference(_) => {
                self.cover_with_not(expr_id, &not_state)?
            }
            Expression::Row(Row { list, .. }) => {
                let list_len = list.len();
                if list_len == 1 {
                    let child_id = *list.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(SmolStr::from(
                            "Row under Not doesn't have children.",
                        ))
                    })?;

                    let new_child = self.push_down_not_for_expression(child_id, not_state, map)?;
                    if child_id != new_child {
                        map.insert(child_id, new_child);
                    }
                    expr_id
                } else {
                    self.cover_with_not(expr_id, &not_state)?
                }
            }
            Expression::Unary(UnaryExpr { op, child }) => match op {
                Unary::Not => {
                    if let NotState::On { .. } = not_state {
                        self.push_down_not_for_expression(*child, NotState::Off, map)?
                    } else {
                        self.push_down_not_for_expression(*child, NotState::on(Some(expr_id)), map)?
                    }
                }
                Unary::IsNull | Unary::Exists => self.cover_with_not(expr_id, &not_state)?,
            },
            _ => expr_id,
        };
        Ok(new_expr_id)
    }

    pub fn push_down_not(&mut self) -> Result<(), SbroadError> {
        self.transform_expr_trees(&call_expr_tree_not_push_down)
    }
}

#[cfg(test)]
mod tests;
