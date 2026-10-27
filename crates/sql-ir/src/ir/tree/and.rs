use std::cell::Cell;

use super::traversal::PostOrder;
use super::TreeIterator;
use crate::ir::node::expression::Expression;
use crate::ir::node::{BoolExpr, NodeId};
use crate::ir::operator::Bool;
use crate::ir::{Node, Nodes};

trait AndTreeIterator<'nodes>: TreeIterator<'nodes> {}

/// Children iterator for "and"-ed expression chains.
///
/// The iterator returns the next child for the chained `Bool::And` nodes.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct AndIterator<'n> {
    node: Option<Node<'n>>,
    child: Cell<usize>,
    nodes: &'n Nodes,
}

impl<'nodes> TreeIterator<'nodes> for AndIterator<'nodes> {
    fn get_node(&self) -> Option<Node<'nodes>> {
        self.node.clone()
    }

    fn get_child(&self) -> &Cell<usize> {
        &self.child
    }

    fn get_nodes(&self) -> &'nodes Nodes {
        self.nodes
    }
}

impl<'nodes> AndTreeIterator<'nodes> for AndIterator<'nodes> {}

impl<'n> Nodes {
    #[must_use]
    pub fn and_iter(&'n self, current: NodeId) -> AndIterator<'n> {
        AndIterator {
            node: self.get(current),
            child: Cell::new(0),
            nodes: self,
        }
    }

    /// Flatten the top-level `AND` spine rooted at `top` into one node id per
    /// conjunct (the leaves of the chain). `and_iter` descends only through
    /// `Bool::And` nodes, so the filter keeps everything that is not an `And` --
    /// i.e. the conjuncts themselves. A non-`And` `top` yields just `[top]`.
    #[must_use]
    pub fn and_conjuncts(&'n self, top: NodeId) -> Vec<NodeId> {
        match self.get(top) {
            None => return vec![],
            Some(Node::Expression(Expression::Bool(BoolExpr { op: Bool::And, .. }))) => {
                // just continue
            }
            Some(_) => return vec![top],
        }
        PostOrder::new(self, |node| self.and_iter(node), top)
            .filter(|(_, node)| {
                !matches!(
                    node,
                    Node::Expression(Expression::Bool(BoolExpr { op: Bool::And, .. }))
                )
            })
            .map(|(id, _)| id)
            .collect()
    }
}

impl Iterator for AndIterator<'_> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        and_next(self).copied()
    }
}

fn and_next<'nodes>(iter: &mut impl AndTreeIterator<'nodes>) -> Option<&'nodes NodeId> {
    let node = iter.get_node();
    if let Some(Node::Expression(Expression::Bool(BoolExpr {
        left, op, right, ..
    }))) = node
    {
        if *op != Bool::And {
            return None;
        }
        let child_step = iter.get_child().get();
        if child_step == 0 {
            iter.advance();
            return Some(left);
        } else if child_step == 1 {
            iter.advance();
            return Some(right);
        }
        None
    } else {
        None
    }
}
