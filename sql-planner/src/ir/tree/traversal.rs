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
    T: Copy + 'static,
{
    pub fn into_iter(self, root: T) -> impl Iterator<Item = LevelNode<T>> {
        self.inner.into_iter(root)
    }

    pub fn populate_nodes(self, root: T) -> Vec<LevelNode<T>> {
        self.inner.populate_nodes(root)
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
    T: Copy + 'filter,
{
    pub fn into_iter(self, root: T) -> impl Iterator<Item = LevelNode<T>> {
        let nodes = self.populate_nodes(root);
        nodes.into_iter()
    }

    pub fn populate_nodes(mut self, root: T) -> Vec<LevelNode<T>> {
        self.nodes.clear();
        self.traverse(root, 0);
        self.nodes
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
    pub fn into_iter(self, root: T) -> impl Iterator<Item = LevelNode<T>> {
        self.populate_nodes(root).into_iter()
    }

    pub fn populate_nodes(mut self, root: T) -> Vec<LevelNode<T>> {
        self.queue.push_back(LevelNode(0, root));
        while let Some(LevelNode(level, node)) = self.queue.pop_front() {
            self.nodes.push(LevelNode(level, node));
            for child in (self.iter_children)(node) {
                self.queue.push_back(LevelNode(level + 1, child));
            }
        }
        self.nodes
    }

    pub fn with_capacity(iter_children: F, node_capacity: usize, queue_capacity: usize) -> Self {
        Self {
            iter_children,
            queue: VecDeque::with_capacity(queue_capacity),
            nodes: Vec::with_capacity(node_capacity),
        }
    }
}
