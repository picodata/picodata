use std::cell::RefCell;

use super::TreeIterator;
use crate::ir::node::relational::Relational;
use crate::ir::node::{ArenaType, Limit, NodeId, Projection, ScanCte};
use crate::ir::{Node, Nodes};

trait RelationalTreeIterator<'nodes>: TreeIterator<'nodes> {}

/// Relational node's child iterator.
///
/// The iterator returns the next relational node in the plan tree.
#[derive(Debug)]
pub struct RelationalIterator<'n> {
    current: NodeId,
    child: RefCell<usize>,
    nodes: &'n Nodes,
}

impl<'n> Nodes {
    #[must_use]
    pub fn rel_iter(&'n self, current: NodeId) -> RelationalIterator<'n> {
        RelationalIterator {
            current,
            child: RefCell::new(0),
            nodes: self,
        }
    }

    #[must_use]
    pub fn empty_rel_iter(&'n self) -> RelationalIterator<'n> {
        RelationalIterator {
            current: self.next_id(ArenaType::Arena64),
            child: RefCell::new(0),
            nodes: self,
        }
    }
}

impl<'nodes> TreeIterator<'nodes> for RelationalIterator<'nodes> {
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

impl<'nodes> RelationalTreeIterator<'nodes> for RelationalIterator<'nodes> {}

impl Iterator for RelationalIterator<'_> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        relational_next(self)
    }
}

fn relational_next<'nodes>(iter: &mut impl RelationalTreeIterator<'nodes>) -> Option<NodeId> {
    let next = iter.get_nodes().get(iter.get_current());
    match next {
        Some(node) => match node {
            Node::Relational(rel_node) => match rel_node {
                node @ (Relational::Except { .. }
                | Relational::Join { .. }
                | Relational::Insert { .. }
                | Relational::Intersect { .. }
                | Relational::Delete { .. }
                | Relational::Motion { .. }
                | Relational::ScanSubQuery { .. }
                | Relational::Selection { .. }
                | Relational::SelectWithoutScan { .. }
                | Relational::Having { .. }
                | Relational::GroupBy { .. }
                | Relational::OrderBy { .. }
                | Relational::Union { .. }
                | Relational::UnionAll { .. }
                | Relational::Update { .. }
                | Relational::Values { .. }
                | Relational::ValuesRow { .. }) => {
                    let step = *iter.get_child().borrow();
                    let children = node.children();
                    if step < children.len() {
                        *iter.get_child().borrow_mut() += 1;
                        return children.get(step).copied();
                    }
                    None
                }
                Relational::Projection(Projection {
                    children,
                    group_by,
                    having,
                    ..
                }) => {
                    let step = *iter.get_child().borrow();
                    *iter.get_child().borrow_mut() += 1;
                    let mut step_shift: usize = 0;
                    if having.is_some() {
                        if step == 0 {
                            return *having;
                        }
                        step_shift += 1;
                    } else if group_by.is_some() {
                        if step - step_shift == 0 {
                            return *group_by;
                        }
                        step_shift += 1;
                    }
                    let child_idx = step - step_shift;
                    if child_idx < children.len() {
                        return children.get(child_idx).copied();
                    }
                    None
                }
                Relational::ScanCte(ScanCte { child, .. })
                | Relational::Limit(Limit { child, .. }) => {
                    let step = *iter.get_child().borrow();
                    if step == 0 {
                        *iter.get_child().borrow_mut() += 1;
                        return Some(child).copied();
                    }
                    None
                }
                Relational::ScanRelation { .. } => None,
            },
            Node::Expression(_)
            | Node::Invalid(_)
            | Node::Ddl(_)
            | Node::Acl(_)
            | Node::Tcl(_)
            | Node::Block(_)
            | Node::Plugin(_)
            | Node::Deallocate(_) => None,
        },
        None => None,
    }
}
