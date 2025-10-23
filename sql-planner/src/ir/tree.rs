//! IR tree traversal module.

use super::{
    node::{expression::Expression, Bound, BoundType, Like, Over, Window},
    operator::{OrderByElement, OrderByEntity},
    Nodes, Plan,
};
use crate::ir::node::{
    Alias, ArithmeticExpr, BoolExpr, Case, Cast, Concat, IndexExpr, NodeId, Trim, UnaryExpr,
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
        | Expression::Concat(Concat { left, right, .. })
        | Expression::Index(IndexExpr {
            child: left,
            which: right,
        })) = expr
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
        let index_remainder = child_step % 2;
        if when_blocks_index < when_blocks.len() {
            let (cond_expr, res_expr) = when_blocks
                .get(when_blocks_index)
                .expect("block must have been found");
            match index_remainder {
                0 => Some(*cond_expr),
                1 => Some(*res_expr),
                other => unreachable!("remainder {other} should not be possible"),
            }
        } else if when_blocks_index == when_blocks.len() && index_remainder == 0 {
            else_expr.as_ref().copied()
        } else {
            None
        }
    }

    fn handle_window_iter(&mut self, expr: Expression) -> Option<NodeId> {
        let Expression::Window(Window {
            partition,
            ordering,
            frame,
            ..
        }) = expr
        else {
            panic!("Expected WINDOW rel node for iteration.")
        };

        let mut step = *self.get_child().borrow();
        *self.get_child().borrow_mut() += 1;

        if let Some(partition) = partition {
            if step < partition.len() {
                return Some(partition[step]);
            }
            step -= partition.len();
        }

        if let Some(ordering) = ordering {
            while step < ordering.len() {
                if let OrderByElement {
                    entity: OrderByEntity::Expression { expr_id },
                    ..
                } = ordering[step]
                {
                    return Some(expr_id);
                }

                *self.get_child().borrow_mut() += 1;
                step += 1;
            }
            step -= ordering.len();
        }

        if let Some(frame) = frame {
            let b_type_expr_id = |bound: &BoundType| match bound {
                BoundType::PrecedingOffset(node_id) | BoundType::FollowingOffset(node_id) => {
                    Some(*node_id)
                }
                _ => None,
            };
            match &frame.bound {
                Bound::Single(bound) => {
                    if step == 0 {
                        if let Some(node_id) = b_type_expr_id(bound) {
                            return Some(node_id);
                        }
                    }
                }
                Bound::Between(bound_from, bound_to) => {
                    match (b_type_expr_id(bound_from), b_type_expr_id(bound_to)) {
                        (Some(node_id_from), Some(node_id_to)) => {
                            if step == 0 {
                                return Some(node_id_from);
                            } else if step == 1 {
                                return Some(node_id_to);
                            }
                        }
                        (Some(node_id), None) | (None, Some(node_id)) => {
                            if step == 0 {
                                return Some(node_id);
                            }
                        }
                        (None, None) => {}
                    }
                }
            }
        }
        None
    }

    fn handle_over_iter(&mut self, expr: Expression) -> Option<NodeId> {
        let Expression::Over(Over {
            stable_func,
            filter,
            window,
            ..
        }) = expr
        else {
            panic!("Over expression expected");
        };
        let mut step = *self.get_child().borrow();
        *self.get_child().borrow_mut() += 1;

        if step == 0 {
            return Some(*stable_func);
        }
        step -= 1;

        if let Some(filter) = filter {
            if step == 0 {
                return Some(*filter);
            }
            step -= 1;
        }

        if step == 0 {
            // We iterate over windows without names in Projection.
            return Some(*window);
        }
        None
    }
}

trait PlanTreeIterator<'plan>: TreeIterator<'plan> {
    fn get_plan(&self) -> &'plan Plan;
}

/// A snapshot describes the version of the plan
/// subtree to iterate over.
#[derive(Debug, Clone, Copy)]
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
