//! Container for a single key-value pair.

/// A cell containing optional value accessed by key.
#[derive(Default)]
pub struct KVCell<K, T> {
    content: Option<(K, T)>,
}

impl<K, T> KVCell<K, T>
where
    K: PartialEq,
{
    /// Creates an empty `KVCell`.
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self { content: None }
    }

    /// Returns `true` if cell contains no value.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.content.is_none()
    }

    /// Sets the contained key and value, then returns mutable reference
    /// to the value.
    ///
    /// If the cell already contains some value, the old one is dropped.
    #[allow(dead_code)]
    pub fn insert(&mut self, key: K, value: T) -> &mut T {
        &mut self.content.insert((key, value)).1
    }

    /// Takes the value out of the cell (if some) regardless of the
    /// contained key. The cell becomes empty.
    #[allow(dead_code)]
    pub fn take(&mut self) -> Option<T> {
        self.content.take().map(|(_, v)| v)
    }

    /// If `key` matches the contained one, takes the value out of the
    /// cell, leaving it empty.
    ///
    /// Otherwise, if `key` doesn't match, retains the old value and key
    /// and returns `None`.
    #[allow(dead_code)]
    pub fn take_or_keep(&mut self, key: &K) -> Option<T> {
        if matches!(&self.content, Some((k, _)) if k == key) {
            self.content.take().map(|(_, v)| v)
        } else {
            None
        }
    }

    /// If `key` matches the contained one, takes the value out of the
    /// cell, leaving it empty.
    ///
    /// Otherwise drops the contained value and returns `None`.
    ///
    /// Anyway, the cell becomes empty.
    #[must_use]
    #[allow(dead_code)]
    pub fn take_or_drop(&mut self, key: &K) -> Option<T> {
        match self.content.take() {
            Some((k, v)) if &k == key => Some(v),
            Some(_) => None,
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    type KVCell = super::KVCell<u8, u32>;

    #[test]
    fn kvcell() {
        let mut kv = KVCell::new();
        assert!(kv.is_empty());

        kv.insert(1, 101);
        assert_eq!(kv.is_empty(), false);
        assert_eq!(kv.take(), Some(101));
        assert_eq!(kv.is_empty(), true);
        assert_eq!(kv.take(), None);

        kv.insert(2, 102);
        assert_eq!(kv.take_or_drop(&2), Some(102));
        assert_eq!(kv.is_empty(), true);

        kv.insert(3, 103);
        assert_eq!(kv.take_or_drop(&0), None);
        assert_eq!(kv.is_empty(), true);

        kv.insert(4, 104);
        assert_eq!(kv.take_or_keep(&4), Some(104));
        assert_eq!(kv.is_empty(), true);

        kv.insert(5, 105);
        assert_eq!(kv.take_or_keep(&0), None);
        assert_eq!(kv.is_empty(), false);

        let v: &mut u32 = kv.insert(6, 106);
        *v = 666;
        assert_eq!(kv.take(), Some(666));
    }
}
