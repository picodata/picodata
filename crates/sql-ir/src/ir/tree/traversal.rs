use crate::ir::node::expression::Expression;
use crate::ir::node::relational::Relational;
use crate::ir::node::NodeId;
use crate::ir::{Node, Nodes};
use std::collections::VecDeque;

pub const EXPR_CAPACITY: usize = 64;
pub const REL_CAPACITY: usize = 32;
/// The usual depth of a plan tree.
const STACK_CAPACITY: usize = 16;

pub struct BreadthFirst<ChildrenFn, T> {
    children_fn: ChildrenFn,
    queue: VecDeque<T>,
    nodes: Vec<T>,
}

impl<ChildrenFn, T> BreadthFirst<ChildrenFn, T> {
    pub fn new(iter_children: ChildrenFn, node_capacity: usize, queue_capacity: usize) -> Self {
        Self {
            children_fn: iter_children,
            queue: VecDeque::with_capacity(queue_capacity),
            nodes: Vec::with_capacity(node_capacity),
        }
    }
}

impl<ChildrenFn, I, T> BreadthFirst<ChildrenFn, T>
where
    ChildrenFn: FnMut(T) -> I,
    I: Iterator<Item = T>,
    T: Copy,
{
    pub fn traverse_into_iter(self, root: T) -> impl Iterator<Item = T> {
        self.traverse_into_vec(root).into_iter()
    }

    pub fn traverse_into_vec(mut self, root: T) -> Vec<T> {
        self.queue.push_back(root);
        while let Some(node) = self.queue.pop_front() {
            self.nodes.push(node);
            for child in (self.children_fn)(node) {
                self.queue.push_back(child);
            }
        }
        self.nodes
    }
}

/// A tree arena that [`PostOrder`] takes the nodes from.
pub trait Arena: Copy {
    type Id: Copy;
    type Node;

    /// `None` skips the node with its subtree.
    fn node(self, id: Self::Id) -> Option<Self::Node>;
}

impl<'nodes> Arena for &'nodes Nodes {
    type Id = NodeId;
    type Node = Node<'nodes>;

    #[inline]
    fn node(self, id: NodeId) -> Option<Node<'nodes>> {
        self.get(id)
    }
}

/// Post-order traversal that yields each node with its id.
///
/// Each node is taken from the arena only once, so the consumer does not
/// need to get it again. The traversal is lazy and borrows the arena, so the
/// tree cannot change while it runs.
pub struct PostOrder<A: Arena, C, I> {
    arena: A,
    children: C,
    /// The path from the root: each node with an iterator over its children
    /// that are not visited yet. It replaces recursion, which cannot pause
    /// between the calls of `next()`. A heap vector is cheaper than an inline
    /// one here: the frames are large, and an inline stack is copied each
    /// time the traversal is moved.
    stack: Vec<(A::Id, A::Node, I)>,
}

/// The children of a node for [`PostOrder`].
pub trait Children<A: Arena, I> {
    fn children(&mut self, id: A::Id, node: &A::Node) -> I;
}

/// Children found by the node id. The child iterator gets the node from the
/// arena again.
pub struct ById<F>(F);

impl<A, F, I> Children<A, I> for ById<F>
where
    A: Arena,
    F: FnMut(A::Id) -> I,
{
    fn children(&mut self, id: A::Id, _: &A::Node) -> I {
        (self.0)(id)
    }
}

/// Children found from the node that the traversal already has.
pub struct ByNode<F>(F);

impl<A, F, I> Children<A, I> for ByNode<F>
where
    A: Arena,
    A::Node: Clone,
    F: FnMut(A::Id, A::Node) -> I,
{
    fn children(&mut self, id: A::Id, node: &A::Node) -> I {
        (self.0)(id, node.clone())
    }
}

impl<A, F, I> PostOrder<A, ById<F>, I>
where
    A: Arena,
    F: FnMut(A::Id) -> I,
    I: Iterator<Item = A::Id>,
{
    pub fn new(arena: A, children_fn: F, root: A::Id) -> Self {
        Self::with_children(arena, ById(children_fn), root)
    }
}

impl<A, F, I> PostOrder<A, ByNode<F>, I>
where
    A: Arena,
    A::Node: Clone,
    F: FnMut(A::Id, A::Node) -> I,
    I: Iterator<Item = A::Id>,
{
    /// Give the node to `children_fn`, so that the child iterator does not
    /// get it from the arena again.
    pub fn new_by_node(arena: A, children_fn: F, root: A::Id) -> Self {
        Self::with_children(arena, ByNode(children_fn), root)
    }
}

impl<A, C, I> PostOrder<A, C, I>
where
    A: Arena,
    C: Children<A, I>,
    I: Iterator<Item = A::Id>,
{
    fn with_children(arena: A, children: C, root: A::Id) -> Self {
        let mut traversal = Self {
            arena,
            children,
            stack: Vec::with_capacity(STACK_CAPACITY),
        };
        traversal.push(root);
        traversal
    }

    /// A node that is absent from the arena is skipped with its subtree,
    /// as the child iterators do.
    fn push(&mut self, id: A::Id) {
        if let Some(node) = self.arena.node(id) {
            let children = self.children.children(id, &node);
            self.stack.push((id, node, children));
        }
    }
}

impl<A, C, I> Iterator for PostOrder<A, C, I>
where
    A: Arena,
    C: Children<A, I>,
    I: Iterator<Item = A::Id>,
{
    type Item = (A::Id, A::Node);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (_, _, children) = self.stack.last_mut()?;
            match children.next() {
                Some(child) => self.push(child),
                None => {
                    let (id, node, _) = self.stack.pop()?;
                    return Some((id, node));
                }
            }
        }
    }
}

/// Typed views of an iterator over plan nodes.
pub trait NodeIter<'nodes>: Iterator<Item = (NodeId, Node<'nodes>)> + Sized {
    /// Only the relational nodes.
    fn relational(self) -> impl Iterator<Item = (NodeId, Relational<'nodes>)> {
        self.filter_map(|(id, node)| match node {
            Node::Relational(rel) => Some((id, rel)),
            _ => None,
        })
    }

    /// Only the expression nodes.
    fn expressions(self) -> impl Iterator<Item = (NodeId, Expression<'nodes>)> {
        self.filter_map(|(id, node)| match node {
            Node::Expression(expr) => Some((id, expr)),
            _ => None,
        })
    }
}

impl<'nodes, T> NodeIter<'nodes> for T where T: Iterator<Item = (NodeId, Node<'nodes>)> {}
