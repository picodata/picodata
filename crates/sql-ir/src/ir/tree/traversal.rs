use crate::ir::node::expression::Expression;
use crate::ir::node::relational::Relational;
use crate::ir::node::NodeId;
use crate::ir::{Node, Nodes};
use smallvec::SmallVec;
use std::collections::VecDeque;

pub const EXPR_CAPACITY: usize = 64;
pub const REL_CAPACITY: usize = 32;

pub struct PostOrder<ChildrenFn, T> {
    inner: PostOrderWithFilter<ChildrenFn, fn(T) -> bool, T>,
}

impl<ChildrenFn, T> PostOrder<ChildrenFn, T> {
    pub fn new(children_fn: ChildrenFn, capacity: usize) -> Self {
        Self {
            inner: PostOrderWithFilter::new(children_fn, |_| true, capacity),
        }
    }
}

impl<ChildrenFn, I, T> PostOrder<ChildrenFn, T>
where
    ChildrenFn: FnMut(T) -> I,
    I: Iterator<Item = T>,
    T: Copy,
{
    pub fn traverse_into_iter(self, root: T) -> impl Iterator<Item = T> {
        self.traverse_into_vec(root).into_iter()
    }

    pub fn traverse_into_vec(self, root: T) -> Vec<T> {
        self.inner.traverse_into_vec(root)
    }
}

pub struct PostOrderWithFilter<ChildrenFn, FilterFn, T> {
    children_fn: ChildrenFn,
    filter_fn: FilterFn,
    nodes: Vec<T>,
}

impl<ChildrenFn, FilterFn, T> PostOrderWithFilter<ChildrenFn, FilterFn, T> {
    pub fn new(children_fn: ChildrenFn, filter_fn: FilterFn, capacity: usize) -> Self {
        Self {
            children_fn,
            filter_fn,
            nodes: Vec::with_capacity(capacity),
        }
    }
}

impl<ChildrenFn, FilterFn, I, T> PostOrderWithFilter<ChildrenFn, FilterFn, T>
where
    ChildrenFn: FnMut(T) -> I,
    FilterFn: FnMut(T) -> bool,
    I: Iterator<Item = T>,
    T: Copy,
{
    fn traverse(&mut self, root: T) {
        for child in (self.children_fn)(root) {
            self.traverse(child);
        }
        if (self.filter_fn)(root) {
            self.nodes.push(root);
        }
    }

    pub fn traverse_into_iter(self, root: T) -> impl Iterator<Item = T> {
        self.traverse_into_vec(root).into_iter()
    }

    pub fn traverse_into_vec(mut self, root: T) -> Vec<T> {
        self.traverse(root);
        self.nodes
    }
}

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

/// Post-order traversal that yields each node with its id.
///
/// Each node is taken from the arena only once, so the consumer does not
/// need to call `Plan::get_node()` again. The traversal is lazy and borrows
/// the nodes, so the plan cannot change while it runs.
pub struct NodePostOrder<'nodes, ChildrenFn, I> {
    nodes: &'nodes Nodes,
    children_fn: ChildrenFn,
    /// The path from the root: each node with an iterator over its children
    /// that are not visited yet. It replaces the recursion of `PostOrder`,
    /// which cannot pause between the calls of `next()`.
    stack: SmallVec<[(NodeId, Node<'nodes>, I); 16]>,
}

impl<'nodes, ChildrenFn, I> NodePostOrder<'nodes, ChildrenFn, I>
where
    ChildrenFn: FnMut(NodeId) -> I,
    I: Iterator<Item = NodeId>,
{
    pub fn new(nodes: &'nodes Nodes, children_fn: ChildrenFn, root: NodeId) -> Self {
        let mut traversal = Self {
            nodes,
            children_fn,
            stack: SmallVec::new(),
        };
        traversal.push(root);
        traversal
    }

    /// A node that is absent from the arena is skipped with its subtree,
    /// as the child iterators do.
    fn push(&mut self, id: NodeId) {
        if let Some(node) = self.nodes.get(id) {
            let children = (self.children_fn)(id);
            self.stack.push((id, node, children));
        }
    }
}

impl<'nodes, ChildrenFn, I> Iterator for NodePostOrder<'nodes, ChildrenFn, I>
where
    ChildrenFn: FnMut(NodeId) -> I,
    I: Iterator<Item = NodeId>,
{
    type Item = (NodeId, Node<'nodes>);

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
