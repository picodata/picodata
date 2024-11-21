use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, RandomState};
use std::io::{Result, Write};
use std::ops::DerefMut;

use rmp::encode::RmpWrite;
use rmp::Marker;
use tarantool::fiber::mutex::MutexGuard as TMutexGuard;
use tarantool::fiber::Mutex as TMutex;

/// [`MutexLike`] is a mutex abstraction to work with different mutexes in general manner.
///
/// It is needed as sometimes we are unable to use tarantool's [`TMutex`].
/// For instance, if we run ordinary Rust unit tests, tarantool symbols would be missing.
pub trait MutexLike<T> {
    type Guard<'a>: DerefMut<Target = T>
    where
        Self: 'a,
        T: 'a;

    fn lock(&self) -> Self::Guard<'_>;
}

impl<T> MutexLike<T> for TMutex<T> {
    type Guard<'a> = TMutexGuard<'a, T> where T: 'a;

    fn lock(&self) -> Self::Guard<'_> {
        self.lock()
    }
}

impl<T> MutexLike<T> for RefCell<T> {
    type Guard<'a> = RefMut<'a, T>
    where T: 'a;

    fn lock(&self) -> Self::Guard<'_> {
        self.borrow_mut()
    }
}

#[derive(Debug, Default)]
pub struct ByteCounter(usize);

impl ByteCounter {
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.0
    }
}

impl Write for ByteCounter {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

pub fn write_u32_array_len<W: Write>(stream: &mut W, len: u32) -> Result<()> {
    stream.write_bytes(&[Marker::Array32.to_u8()])?;
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&len.to_be_bytes());
    stream.write_bytes(&buf)?;
    Ok(())
}

pub struct OrderedMap<K, V, S = RandomState> {
    map: HashMap<K, V, S>,
    order: Vec<(K, V)>,
}

pub struct OrderedMapIterator<'map, K, V, S> {
    map: &'map OrderedMap<K, V, S>,
    index: usize,
}

impl<'map, K: Clone + Hash + Eq, V: Clone, S: BuildHasher> OrderedMapIterator<'map, K, V, S> {
    fn new(map: &'map OrderedMap<K, V, S>) -> Self {
        Self { map, index: 0 }
    }
}

impl<'map, K: Clone + Hash + Eq, V: Clone, S: BuildHasher> Iterator
    for OrderedMapIterator<'map, K, V, S>
{
    type Item = &'map (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.map.len() {
            let res = &self.map.order[self.index];
            self.index += 1;
            Some(res)
        } else {
            None
        }
    }
}

impl<K: Clone + Hash + Eq, V: Clone, S: BuildHasher> OrderedMap<K, V, S> {
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            map: HashMap::<K, V, S>::with_hasher(hasher),
            order: Vec::<(K, V)>::new(),
        }
    }

    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        Self {
            map: HashMap::<K, V, S>::with_capacity_and_hasher(capacity, hasher),
            order: Vec::<(K, V)>::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.order.retain(|(k, _)| k != key);
        self.map.remove(key)
    }

    pub fn insert(&mut self, key: K, value: V) {
        if self.map.insert(key.clone(), value.clone()).is_none() {
            self.order.push((key, value));
        }
    }

    pub fn iter(&self) -> OrderedMapIterator<K, V, S> {
        OrderedMapIterator::new(self)
    }
}

pub struct OrderedSet<V, S = RandomState> {
    map: OrderedMap<V, (), S>,
}

pub struct OrderedSetIterator<'set, V, S> {
    map_iterator: OrderedMapIterator<'set, V, (), S>,
}

impl<'set, V: Clone + Hash + Eq, S: BuildHasher> Iterator for OrderedSetIterator<'set, V, S> {
    type Item = &'set V;

    fn next(&mut self) -> Option<Self::Item> {
        self.map_iterator.next().map(|(k, _)| k)
    }
}

impl<V: Clone + Hash + Eq, S: BuildHasher> OrderedSet<V, S> {
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        Self {
            map: OrderedMap::<V, (), S>::with_capacity_and_hasher(capacity, hasher),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn insert(&mut self, value: V) {
        self.map.insert(value, ())
    }

    pub fn iter(&self) -> OrderedSetIterator<V, S> {
        OrderedSetIterator {
            map_iterator: OrderedMapIterator::new(&self.map),
        }
    }
}
