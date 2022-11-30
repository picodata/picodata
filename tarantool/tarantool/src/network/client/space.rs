use std::rc::Rc;

use crate::error::Error;
use crate::index::IteratorType;
use crate::network::protocol::options::Options;
use crate::tuple::{Encode, ToTupleBuffer, Tuple};

use super::index::{RemoteIndex, RemoteIndexIterator};
use super::inner::ConnInner;
use crate::network::protocol::codec;

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

    /// Returns index with id = 0
    #[inline(always)]
    pub fn primary_key(&self) -> RemoteIndex {
        RemoteIndex::new(self.conn_inner.clone(), self.space_id, 0)
    }

    /// The remote-call equivalent of the local call `Space::get(...)`
    /// (see [details](../space/struct.Space.html#method.get)).
    pub fn get<K>(&self, key: &K, options: &Options) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer,
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
        K: ToTupleBuffer,
    {
        self.primary_key().select(iterator_type, key, options)
    }

    /// The remote-call equivalent of the local call `Space::insert(...)`
    /// (see [details](../space/struct.Space.html#method.insert)).
    pub fn insert<T>(&self, value: &T, options: &Options) -> Result<Option<Tuple>, Error>
    where
        T: ToTupleBuffer,
    {
        self.conn_inner.request(
            |buf, sync| codec::encode_insert(buf, sync, self.space_id, value),
            codec::decode_single_row,
            options,
        )
    }

    /// The remote-call equivalent of the local call `Space::replace(...)`
    /// (see [details](../space/struct.Space.html#method.replace)).
    pub fn replace<T>(&self, value: &T, options: &Options) -> Result<Option<Tuple>, Error>
    where
        T: ToTupleBuffer,
    {
        self.conn_inner.request(
            |buf, sync| codec::encode_replace(buf, sync, self.space_id, value),
            codec::decode_single_row,
            options,
        )
    }

    /// The remote-call equivalent of the local call `Space::update(...)`
    /// (see [details](../space/struct.Space.html#method.update)).
    pub fn update<K, Op>(
        &self,
        key: &K,
        ops: &[Op],
        options: &Options,
    ) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer,
        Op: Encode,
    {
        self.primary_key().update(key, ops, options)
    }

    /// The remote-call equivalent of the local call `Space::upsert(...)`
    /// (see [details](../space/struct.Space.html#method.upsert)).
    pub fn upsert<T, Op>(
        &self,
        value: &T,
        ops: &[Op],
        options: &Options,
    ) -> Result<Option<Tuple>, Error>
    where
        T: ToTupleBuffer,
        Op: Encode,
    {
        self.primary_key().upsert(value, ops, options)
    }

    /// The remote-call equivalent of the local call `Space::delete(...)`
    /// (see [details](../space/struct.Space.html#method.delete)).
    pub fn delete<K>(&self, key: &K, options: &Options) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer,
    {
        self.primary_key().delete(key, options)
    }
}
