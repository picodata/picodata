use std::cell::RefCell;

use super::TreeIterator;
use crate::ir::node::expression::Expression;
use crate::ir::node::{NodeId, Row, StableFunction};
use crate::ir::{Node, Nodes};

trait ExpressionTreeIterator<'nodes>: TreeIterator<'nodes> {
    fn get_make_row_leaf(&self) -> bool;
}

/// Expression node's children iterator.
///
/// The iterator returns the next child for expression
/// nodes. It is required to use `traversal` crate.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct ExpressionIterator<'n> {
    current: NodeId,
    child: RefCell<usize>,
    nodes: &'n Nodes,
    make_row_leaf: bool,
}

pub struct AggregateIterator<'p> {
    inner: ExpressionIterator<'p>,
    must_stop: bool,
}

impl<'n> Nodes {
    #[must_use]
    pub fn expr_iter(&'n self, current: NodeId, make_row_leaf: bool) -> ExpressionIterator<'n> {
        ExpressionIterator {
            current,
            child: RefCell::new(0),
            nodes: self,
            make_row_leaf,
        }
    }

    #[must_use]
    pub fn aggregate_iter(&'n self, current: NodeId, make_row_leaf: bool) -> AggregateIterator<'n> {
        let must_stop =
            if let Some(Node::Expression(Expression::StableFunction(StableFunction {
                name, ..
            }))) = self.get(current)
            {
                Expression::is_aggregate_name(name)
            } else {
                false
            };
        AggregateIterator {
            inner: ExpressionIterator {
                current,
                child: RefCell::new(0),
                nodes: self,
                make_row_leaf,
            },
            must_stop,
        }
    }
}

impl<'nodes> TreeIterator<'nodes> for ExpressionIterator<'nodes> {
    fn get_current(&self) -> NodeId {
        self.current
    }

    fn get_child(&self) -> &RefCell<usize> {
        &self.child
    }

    fn get_nodes(&self) -> &'nodes Nodes {
        self.nodes
    }
}

impl<'nodes> ExpressionTreeIterator<'nodes> for ExpressionIterator<'nodes> {
    fn get_make_row_leaf(&self) -> bool {
        self.make_row_leaf
    }
}

impl<'n> Iterator for ExpressionIterator<'n> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        expression_next(self)
    }
}

impl<'n> Iterator for AggregateIterator<'n> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.must_stop {
            return None;
        }
        expression_next(&mut self.inner)
    }
}

#[allow(clippy::too_many_lines)]
fn expression_next<'nodes>(iter: &mut impl ExpressionTreeIterator<'nodes>) -> Option<NodeId> {
    let node = iter.get_nodes().get(iter.get_current());
    match node {
        Some(node) => {
            match node {
                Node::Expression(expr) => {
                    match expr {
                        Expression::Alias { .. }
                        | Expression::ExprInParentheses { .. }
                        | Expression::Cast { .. }
                        | Expression::Unary { .. } => iter.handle_single_child(expr),
                        Expression::Bool { .. }
                        | Expression::Arithmetic { .. }
                        | Expression::Concat { .. } => iter.handle_left_right_children(expr),
                        Expression::Row(Row { list, .. }) => {
                            let child_step = *iter.get_child().borrow();
                            let mut is_leaf = false;

                            // Check on the first step, if the row contains only leaf nodes.
                            if child_step == 0 {
                                is_leaf = true;
                                for col in list {
                                    if !matches!(
                                        iter.get_nodes().get(*col),
                                        Some(Node::Expression(
                                            Expression::Reference { .. }
                                                | Expression::Constant { .. }
                                        ))
                                    ) {
                                        is_leaf = false;
                                        break;
                                    }
                                }
                            }

                            // If the row contains only leaf nodes (or we don't want to go deeper
                            // into the row tree for some reasons), skip traversal.
                            if !is_leaf || !iter.get_make_row_leaf() {
                                match list.get(child_step) {
                                    None => return None,
                                    Some(child) => {
                                        *iter.get_child().borrow_mut() += 1;
                                        return Some(*child);
                                    }
                                }
                            }

                            None
                        }
                        Expression::StableFunction(StableFunction { children, .. }) => {
                            let child_step = *iter.get_child().borrow();
                            match children.get(child_step) {
                                None => None,
                                Some(child) => {
                                    *iter.get_child().borrow_mut() += 1;
                                    Some(*child)
                                }
                            }
                        }
                        Expression::Trim { .. } => iter.handle_trim(expr),
                        Expression::Like { .. } => iter.handle_like(expr),
                        Expression::Case { .. } => iter.handle_case_iter(expr),
                        Expression::Constant { .. }
                        | Expression::Reference { .. }
                        | Expression::CountAsterisk { .. } => None,
                    }
                }
                Node::Acl(_)
                | Node::Block(_)
                | Node::Ddl(_)
                | Node::Relational(_)
                | Node::Invalid(_)
                | Node::Plugin(_)
                | Node::Parameter(_) => None,
            }
        }
        None => None,
    }
}
