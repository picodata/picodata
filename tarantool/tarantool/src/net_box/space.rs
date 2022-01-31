use std::rc::Rc;

use crate::error::Error;
use crate::index::IteratorType;
use crate::tuple::{AsTuple, Tuple};

use super::index::{RemoteIndex, RemoteIndexIterator};
use super::inner::ConnInner;
use super::options::Options;
use super::protocol;

/// Remote space
pub struct RemoteSpace {
    conn_inner: Rc<ConnInner>,
    space_id: u32,
}

impl RemoteSpace {
    pub(crate) fn new(conn_inner: Rc<ConnInner>, space_id: u32) -> Self {
        RemoteSpace {
            conn_inner,
            space_id,
        }
    }

    /// Find index by name (on remote space)
    pub fn index(&self, name: &str) -> Result<Option<RemoteIndex>, Error> {
        Ok(self
            .conn_inner
            .lookup_index(name, self.space_id)?
            .map(|index_id| RemoteIndex::new(self.conn_inner.clone(), self.space_id, index_id)))
    }

    /// Returns index with id = 0
    #[inline(always)]
    pub fn primary_key(&self) -> RemoteIndex {
        RemoteIndex::new(self.conn_inner.clone(), self.space_id, 0)
    }

    /// The remote-call equivalent of the local call `Space::get(...)`
    /// (see [details](../space/struct.Space.html#method.get)).
    pub fn get<K>(&self, key: &K, options: &Options) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        self.primary_key().get(key, options)
    }

    /// The remote-call equivalent of the local call `Space::select(...)`
    /// (see [details](../space/struct.Space.html#method.select)).
    pub fn select<K>(
        &self,
        iterator_type: IteratorType,
        key: &K,
        options: &Options,
    ) -> Result<RemoteIndexIterator, Error>
    where
        K: AsTuple,
    {
        self.primary_key().select(iterator_type, key, options)
    }

    /// The remote-call equivalent of the local call `Space::insert(...)`
    /// (see [details](../space/struct.Space.html#method.insert)).
    pub fn insert<T>(&mut self, value: &T, options: &Options) -> Result<Option<Tuple>, Error>
    where
        T: AsTuple,
    {
        self.conn_inner.request(
            |buf, sync| protocol::encode_insert(buf, sync, self.space_id, value),
            protocol::decode_single_row,
            options,
        )
    }

    /// The remote-call equivalent of the local call `Space::replace(...)`
    /// (see [details](../space/struct.Space.html#method.replace)).
    pub fn replace<T>(&mut self, value: &T, options: &Options) -> Result<Option<Tuple>, Error>
    where
        T: AsTuple,
    {
        self.conn_inner.request(
            |buf, sync| protocol::encode_replace(buf, sync, self.space_id, value),
            protocol::decode_single_row,
            options,
        )
    }

    /// The remote-call equivalent of the local call `Space::update(...)`
    /// (see [details](../space/struct.Space.html#method.update)).
    pub fn update<K, Op>(
        &mut self,
        key: &K,
        ops: &[Op],
        options: &Options,
    ) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
        Op: AsTuple,
    {
        self.primary_key().update(key, ops, options)
    }

    /// The remote-call equivalent of the local call `Space::upsert(...)`
    /// (see [details](../space/struct.Space.html#method.upsert)).
    pub fn upsert<T, Op>(
        &mut self,
        value: &T,
        ops: &[Op],
        options: &Options,
    ) -> Result<Option<Tuple>, Error>
    where
        T: AsTuple,
        Op: AsTuple,
    {
        self.primary_key().upsert(value, ops, options)
    }

    /// The remote-call equivalent of the local call `Space::delete(...)`
    /// (see [details](../space/struct.Space.html#method.delete)).
    pub fn delete<K>(&mut self, key: &K, options: &Options) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        self.primary_key().delete(key, options)
    }
}
