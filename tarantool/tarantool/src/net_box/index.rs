use std::rc::Rc;
use std::vec::IntoIter;

use crate::error::Error;
use crate::index::IteratorType;
use crate::tuple::{Encode, ToTupleBuffer, Tuple};

use super::inner::ConnInner;
use super::protocol;
use super::Options;

/// Remote index (a group of key values and pointers)
pub struct RemoteIndex {
    conn_inner: Rc<ConnInner>,
    space_id: u32,
    index_id: u32,
}

impl RemoteIndex {
    pub(crate) fn new(conn_inner: Rc<ConnInner>, space_id: u32, index_id: u32) -> Self {
        RemoteIndex {
            conn_inner,
            space_id,
            index_id,
        }
    }

    /// The remote-call equivalent of the local call `Index::get(...)`
    /// (see [details](../index/struct.Index.html#method.get)).
    pub fn get<K>(&self, key: &K, options: &Options) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer,
    {
        Ok(self
            .select(
                IteratorType::Eq,
                key,
                &Options {
                    offset: 0,
                    limit: Some(1),
                    ..options.clone()
                },
            )?
            .next())
    }

    /// The remote-call equivalent of the local call `Index::select(...)`
    /// (see [details](../index/struct.Index.html#method.select)).
    pub fn select<K>(
        &self,
        iterator_type: IteratorType,
        key: &K,
        options: &Options,
    ) -> Result<RemoteIndexIterator, Error>
    where
        K: ToTupleBuffer,
    {
        self.conn_inner.request(
            |buf, sync| {
                protocol::encode_select(
                    buf,
                    sync,
                    self.space_id,
                    self.index_id,
                    options.limit.unwrap_or(u32::max_value()),
                    options.offset,
                    iterator_type,
                    key,
                )
            },
            |buf, _| {
                protocol::decode_multiple_rows(buf, None).map(|result| RemoteIndexIterator {
                    inner: result.into_iter(),
                })
            },
            options,
        )
    }

    /// The remote-call equivalent of the local call `Space::update(...)`
    /// (see [details](../index/struct.Index.html#method.update)).
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
        self.conn_inner.request(
            |buf, sync| protocol::encode_update(buf, sync, self.space_id, self.index_id, key, ops),
            protocol::decode_single_row,
            options,
        )
    }

    /// The remote-call equivalent of the local call `Space::upsert(...)`
    /// (see [details](../index/struct.Index.html#method.upsert)).
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
        self.conn_inner.request(
            |buf, sync| {
                protocol::encode_upsert(buf, sync, self.space_id, self.index_id, value, ops)
            },
            protocol::decode_single_row,
            options,
        )
    }

    /// The remote-call equivalent of the local call `Space::delete(...)`
    /// (see [details](../index/struct.Index.html#method.delete)).
    pub fn delete<K>(&self, key: &K, options: &Options) -> Result<Option<Tuple>, Error>
    where
        K: ToTupleBuffer,
    {
        self.conn_inner.request(
            |buf, sync| protocol::encode_delete(buf, sync, self.space_id, self.index_id, key),
            protocol::decode_single_row,
            options,
        )
    }
}

/// Remote index iterator. Can be used with `for` statement
pub struct RemoteIndexIterator {
    inner: IntoIter<Tuple>,
}

impl Iterator for RemoteIndexIterator {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
