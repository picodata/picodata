use std::collections::VecDeque;

pub const EXPR_CAPACITY: usize = 64;
pub const REL_CAPACITY: usize = 32;

/// Pair of (Level of the node in traversal algorithm, `node_id`).
#[derive(Debug, PartialEq)]
pub struct LevelNode<T>(pub usize, pub T);

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
    pub fn traverse_into_iter(self, root: T) -> impl Iterator<Item = LevelNode<T>> {
        self.traverse_into_vec(root).into_iter()
    }

    pub fn traverse_into_vec(self, root: T) -> Vec<LevelNode<T>> {
        self.inner.traverse_into_vec(root)
    }
}

pub struct PostOrderWithFilter<ChildrenFn, FilterFn, T> {
    children_fn: ChildrenFn,
    filter_fn: FilterFn,
    nodes: Vec<LevelNode<T>>,
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
    fn traverse(&mut self, root: T, level: usize) {
        for child in (self.children_fn)(root) {
            self.traverse(child, level + 1);
        }
        if (self.filter_fn)(root) {
            self.nodes.push(LevelNode(level, root));
        }
    }

    pub fn traverse_into_iter(self, root: T) -> impl Iterator<Item = LevelNode<T>> {
        self.traverse_into_vec(root).into_iter()
    }

    pub fn traverse_into_vec(mut self, root: T) -> Vec<LevelNode<T>> {
        self.traverse(root, 0);
        self.nodes
    }
}

pub struct BreadthFirst<ChildrenFn, T> {
    children_fn: ChildrenFn,
    queue: VecDeque<LevelNode<T>>,
    nodes: Vec<LevelNode<T>>,
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
    pub fn traverse_into_iter(self, root: T) -> impl Iterator<Item = LevelNode<T>> {
        self.traverse_into_vec(root).into_iter()
    }

    pub fn traverse_into_vec(mut self, root: T) -> Vec<LevelNode<T>> {
        self.queue.push_back(LevelNode(0, root));
        while let Some(LevelNode(level, node)) = self.queue.pop_front() {
            self.nodes.push(LevelNode(level, node));
            for child in (self.children_fn)(node) {
                self.queue.push_back(LevelNode(level + 1, child));
            }
        }
        self.nodes
    }
}
