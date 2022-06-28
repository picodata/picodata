use std::cell::Cell;

pub struct CachedCell<K, T> {
    cell: Cell<Option<(K, T)>>,
}

impl<K, T> CachedCell<K, T>
where
    K: PartialEq,
{
    pub fn new() -> Self {
        Self {
            cell: Cell::default(),
        }
    }

    pub fn put(&self, key: K, value: T) {
        self.cell.set(Some((key, value)));
    }

    pub fn pop(&self, key: &K) -> Option<T> {
        match self.cell.take() {
            Some((k, v)) if k == *key => Some(v),
            Some(_) => None,
            None => None,
        }
    }
}
