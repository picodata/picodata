use smol_str::ToSmolStr;

use crate::errors::{Entity, SbroadError};
use lru::LruCache as Lru;
use std::hash::Hash;

pub const DEFAULT_CAPACITY: usize = 50;

pub type EvictFn<Key, Value> = Box<dyn Fn(&Key, &mut Value) -> Result<(), SbroadError>>;

pub trait Cache<Key, Value> {
    /// Builds a new cache with the given capacity.
    ///
    /// # Errors
    /// - Capacity is not valid (zero).
    fn new(capacity: usize, evict_fn: Option<EvictFn<Key, Value>>) -> Result<Self, SbroadError>
    where
        Self: Sized;

    /// Returns a value from the cache.
    ///
    /// # Errors
    /// - Internal error (should never happen).
    fn get(&mut self, key: &Key) -> Result<Option<&Value>, SbroadError>;

    /// Inserts a key-value pair into the cache.
    ///
    /// # Errors
    /// - Internal error (should never happen).
    fn put(&mut self, key: Key, value: Value) -> Result<(), SbroadError>;

    /// Clears the cache, eviction function is
    /// applied to each element (in unspecified order)
    ///
    /// # Errors
    /// - errors caused by eviction function
    fn clear(&mut self) -> Result<(), SbroadError>;
}

pub struct LRUCache<Key, Value>
where
    Key: Eq + Hash,
{
    lru: Lru<Key, Value>,
    // A function applied to the value before evicting it from the cache.
    evict_fn: Option<EvictFn<Key, Value>>,
}

impl<Key, Value> LRUCache<Key, Value>
where
    Key: Eq + Hash,
{
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.lru.cap().into()
    }

    /// Empties the cache, `evict_fn` is applied
    /// to every element in the cache in unspecified
    /// order.
    ///
    ///
    /// # Errors
    /// - error on applying `evict_fn` to some value
    pub fn clear(&mut self) -> Result<(), SbroadError> {
        while let Some((k, mut v)) = self.lru.pop_lru() {
            if let Some(ref f) = self.evict_fn {
                f(&k, &mut v)?
            }
        }

        Ok(())
    }

    pub fn adjust_capacity(&mut self, target_capacity: usize) -> Result<(), SbroadError> {
        debug_assert!(target_capacity > 0);

        for _ in target_capacity..self.lru.len() {
            if let Some((k, mut v)) = self.lru.pop_lru() {
                if let Some(ref f) = self.evict_fn {
                    f(&k, &mut v)?
                }
            }
        }

        self.lru.resize(target_capacity.try_into().unwrap());

        Ok(())
    }
}

impl<Key, Value> Cache<Key, Value> for LRUCache<Key, Value>
where
    Key: Eq + Hash,
{
    fn new(capacity: usize, evict_fn: Option<EvictFn<Key, Value>>) -> Result<Self, SbroadError> {
        if capacity == 0 {
            return Err(SbroadError::Invalid(
                Entity::Cache,
                Some("LRU cache capacity must be greater than zero".to_smolstr()),
            ));
        }
        Ok(LRUCache {
            lru: Lru::new(capacity.try_into().unwrap()),
            evict_fn,
        })
    }

    fn get(&mut self, key: &Key) -> Result<Option<&Value>, SbroadError> {
        Ok(self.lru.get(key))
    }

    fn put(&mut self, key: Key, value: Value) -> Result<(), SbroadError> {
        if let Some((k, mut v)) = self.lru.push(key, value) {
            if let Some(ref f) = self.evict_fn {
                f(&k, &mut v)?;
            }
        }
        Ok(())
    }

    fn clear(&mut self) -> Result<(), SbroadError> {
        self.clear()
    }
}

#[cfg(test)]
mod tests;
