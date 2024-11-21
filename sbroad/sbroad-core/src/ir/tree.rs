//! IR tree traversal module.

use super::{
    node::{expression::Expression, Like},
    Nodes, Plan,
};
use crate::ir::node::{
    Alias, ArithmeticExpr, BoolExpr, Case, Cast, Concat, ExprInParentheses, NodeId, Trim, UnaryExpr,
};
use std::cell::RefCell;

trait TreeIterator<'nodes> {
    fn get_current(&self) -> NodeId;
    fn get_child(&self) -> &RefCell<usize>;
    fn get_nodes(&self) -> &'nodes Nodes;

    fn handle_trim(&mut self, expr: Expression) -> Option<NodeId> {
        let Expression::Trim(Trim {
            pattern, target, ..
        }) = expr
        else {
            panic!("Trim expected")
        };
        let child_step = *self.get_child().borrow();
        match child_step {
            0 => {
                *self.get_child().borrow_mut() += 1;
                match pattern {
                    Some(_) => *pattern,
                    None => Some(*target),
                }
            }
            1 => {
                *self.get_child().borrow_mut() += 1;
                pattern.as_ref().map(|_| *target)
            }
            _ => None,
        }
    }

    fn handle_like(&mut self, expr: Expression) -> Option<NodeId> {
        let Expression::Like(Like {
            left,
            right,
            escape: escape_id,
        }) = expr
        else {
            panic!("Like expected")
        };
        let child_step = *self.get_child().borrow();
        let res = match child_step {
            0 => Some(*left),
            1 => Some(*right),
            2 => Some(*escape_id),
            _ => None,
        };
        *self.get_child().borrow_mut() += 1;
        res
    }

    fn handle_left_right_children(&mut self, expr: Expression) -> Option<NodeId> {
        let (Expression::Bool(BoolExpr { left, right, .. })
        | Expression::Arithmetic(ArithmeticExpr { left, right, .. })
        | Expression::Concat(Concat { left, right, .. })) = expr
        else {
            panic!("Expected expression with left and right children")
        };
        let child_step = *self.get_child().borrow();
        if child_step == 0 {
            *self.get_child().borrow_mut() += 1;
            return Some(*left);
        } else if child_step == 1 {
            *self.get_child().borrow_mut() += 1;
            return Some(*right);
        }
        None
    }

    fn handle_single_child(&mut self, expr: Expression) -> Option<NodeId> {
        let (Expression::Alias(Alias { child, .. })
        | Expression::ExprInParentheses(ExprInParentheses { child })
        | Expression::Cast(Cast { child, .. })
        | Expression::Unary(UnaryExpr { child, .. })) = expr
        else {
            panic!("Expected expression with single child")
        };
        let step = *self.get_child().borrow();
        *self.get_child().borrow_mut() += 1;
        if step == 0 {
            return Some(*child);
        }
        None
    }

    fn handle_case_iter(&mut self, expr: Expression) -> Option<NodeId> {
        let Expression::Case(Case {
            search_expr,
            when_blocks,
            else_expr,
        }) = expr
        else {
            panic!("Case expression expected");
        };
        let mut child_step = *self.get_child().borrow();
        *self.get_child().borrow_mut() += 1;
        if let Some(search_expr) = search_expr {
            if child_step == 0 {
                return Some(*search_expr);
            }
            child_step -= 1;
        }

        let when_blocks_index = child_step / 2;
        let index_reminder = child_step % 2;
        return if when_blocks_index < when_blocks.len() {
            let (cond_expr, res_expr) = when_blocks
                .get(when_blocks_index)
                .expect("When block must have been found.");
            return match index_reminder {
                0 => Some(*cond_expr),
                1 => Some(*res_expr),
                _ => unreachable!("Impossible reminder"),
            };
        } else if when_blocks_index == when_blocks.len() && index_reminder == 0 {
            else_expr.as_ref().copied()
        } else {
            None
        };
    }
}

trait PlanTreeIterator<'plan>: TreeIterator<'plan> {
    fn get_plan(&self) -> &'plan Plan;
}

/// A snapshot describes the version of the plan
/// subtree to iterate over.
#[derive(Debug, Clone)]
pub enum Snapshot {
    Latest,
    Oldest,
}

pub mod and;
pub mod expression;
pub mod relation;
pub mod subtree;
pub mod traversal;

#[cfg(test)]
mod tests;
