use arrayvec::ArrayVec;

use crate::ir::node::NodeId;

pub type Children = ArrayVec<NodeId, 2>;
pub type MutChildren<'a> = ArrayVec<&'a mut NodeId, 2>;

/// Builds the children of a node with one child.
#[inline]
pub fn one<T>(child: T) -> ArrayVec<T, 2> {
    let mut children = ArrayVec::new();
    children.push(child);
    children
}
