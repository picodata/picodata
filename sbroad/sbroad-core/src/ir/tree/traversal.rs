use std::collections::VecDeque;

pub const EXPR_CAPACITY: usize = 64;
pub const REL_CAPACITY: usize = 32;

/// Pair of (Level of the node in traversal algorithm, `node_id`).
#[derive(Debug, PartialEq)]
pub struct LevelNode<T>(pub usize, pub T)
where
    T: Copy;

pub struct PostOrder<F, I, T>
where
    F: FnMut(T) -> I,
    I: Iterator<Item = T>,
    T: Copy,
{
    inner: PostOrderWithFilter<'static, F, I, T>,
}

impl<F, I, T> PostOrder<F, I, T>
where
    F: FnMut(T) -> I,
    I: Iterator<Item = T>,
    T: Copy,
{
    pub fn iter(&mut self, root: T) -> impl Iterator<Item = LevelNode<T>> {
        self.inner.iter(root)
    }

    pub fn new(iter_children: F, nodes: Vec<LevelNode<T>>) -> Self {
        Self {
            inner: PostOrderWithFilter::new(iter_children, nodes, Box::new(|_| true)),
        }
    }

    pub fn populate_nodes(&mut self, root: T) {
        self.inner.populate_nodes(root);
    }

    pub fn take_nodes(&mut self) -> Vec<LevelNode<T>> {
        self.inner.take_nodes()
    }

    pub fn with_capacity(iter_children: F, capacity: usize) -> Self {
        Self {
            inner: PostOrderWithFilter::with_capacity(iter_children, capacity, Box::new(|_| true)),
        }
    }
}

pub type FilterFn<'filter, T> = Box<dyn FnMut(T) -> bool + 'filter>;

pub struct PostOrderWithFilter<'filter, F, I, T>
where
    F: FnMut(T) -> I,
    I: Iterator<Item = T>,
    T: Copy,
{
    iter_children: F,
    nodes: Vec<LevelNode<T>>,
    filter_fn: FilterFn<'filter, T>,
}

impl<'filter, F, I, T> PostOrderWithFilter<'filter, F, I, T>
where
    F: FnMut(T) -> I,
    I: Iterator<Item = T>,
    T: Copy,
{
    pub fn iter(&mut self, root: T) -> impl Iterator<Item = LevelNode<T>> {
        self.populate_nodes(root);
        self.take_nodes().into_iter()
    }

    pub fn new(
        iter_children: F,
        nodes: Vec<LevelNode<T>>,
        filter_fn: FilterFn<'filter, T>,
    ) -> Self {
        Self {
            iter_children,
            nodes,
            filter_fn,
        }
    }

    pub fn populate_nodes(&mut self, root: T) {
        self.nodes.clear();
        self.traverse(root, 0);
    }

    pub fn take_nodes(&mut self) -> Vec<LevelNode<T>> {
        std::mem::take(&mut self.nodes)
    }

    fn traverse(&mut self, root: T, level: usize) {
        for child in (self.iter_children)(root) {
            self.traverse(child, level + 1);
        }
        if (self.filter_fn)(root) {
            self.nodes.push(LevelNode(level, root));
        }
    }

    pub fn with_capacity(iter_children: F, capacity: usize, filter: FilterFn<'filter, T>) -> Self {
        Self {
            iter_children,
            nodes: Vec::with_capacity(capacity),
            filter_fn: filter,
        }
    }
}

pub struct BreadthFirst<F, I, T>
where
    F: FnMut(T) -> I,
    I: Iterator<Item = T>,
    T: Copy,
{
    iter_children: F,
    queue: VecDeque<LevelNode<T>>,
    nodes: Vec<LevelNode<T>>,
}

impl<F, I, T> BreadthFirst<F, I, T>
where
    F: FnMut(T) -> I,
    I: Iterator<Item = T>,
    T: Copy,
{
    pub fn iter(&mut self, root: T) -> impl Iterator<Item = LevelNode<T>> {
        self.populate_nodes(root);
        self.take_nodes().into_iter()
    }

    pub fn new(iter_children: F, queue: VecDeque<LevelNode<T>>, nodes: Vec<LevelNode<T>>) -> Self {
        Self {
            iter_children,
            queue,
            nodes,
        }
    }

    pub fn populate_nodes(&mut self, root: T) {
        self.queue.push_back(LevelNode(0, root));
        while let Some(LevelNode(level, node)) = self.queue.pop_front() {
            self.nodes.push(LevelNode(level, node));
            for child in (self.iter_children)(node) {
                self.queue.push_back(LevelNode(level + 1, child));
            }
        }
    }

    pub fn take_nodes(&mut self) -> Vec<LevelNode<T>> {
        std::mem::take(&mut self.nodes)
    }

    pub fn with_capacity(iter_children: F, node_capacity: usize, queue_capacity: usize) -> Self {
        Self {
            iter_children,
            queue: VecDeque::with_capacity(queue_capacity),
            nodes: Vec::with_capacity(node_capacity),
        }
    }
}
