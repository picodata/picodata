use std::rc::Rc;
use std::vec::IntoIter;

use crate::error::Error;
use crate::index::IteratorType;
use crate::network::protocol;
use crate::tuple::{Encode, ToTuple, Tuple};

use super::inner::ConnInner;
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
    #[inline(always)]
    pub fn get<K>(&self, key: &K, options: &Options) -> Result<Option<Tuple>, Error>
    where
        K: ToTuple + ?Sized,
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
    #[inline(always)]
    pub fn select<K>(
        &self,
        iterator_type: IteratorType,
        key: &K,
        options: &Options,
    ) -> Result<RemoteIndexIterator, Error>
    where
        K: ToTuple + ?Sized,
    {
        let rows = self.conn_inner.request(
            &protocol::Select {
                space_id: self.space_id,
                index_id: self.index_id,
                limit: options.limit.unwrap_or(u32::MAX),
                offset: options.offset,
                iterator_type,
                key,
            },
            options,
        )?;
        Ok(RemoteIndexIterator {
            inner: rows.into_iter(),
        })
    }

    /// The remote-call equivalent of the local call `Space::update(...)`
    /// (see [details](../index/struct.Index.html#method.update)).
    #[inline(always)]
    pub fn update<K, Op>(
        &self,
        key: &K,
        ops: &[Op],
        options: &Options,
    ) -> Result<Option<Tuple>, Error>
    where
        K: ToTuple + ?Sized,
        Op: Encode,
    {
        self.conn_inner.request(
            &protocol::Update {
                space_id: self.space_id,
                index_id: self.index_id,
                key,
                ops,
            },
            options,
        )
    }

    /// The remote-call equivalent of the local call `Space::upsert(...)`
    /// (see [details](../index/struct.Index.html#method.upsert)).
    #[inline(always)]
    pub fn upsert<T, Op>(
        &self,
        value: &T,
        ops: &[Op],
        options: &Options,
    ) -> Result<Option<Tuple>, Error>
    where
        T: ToTuple + ?Sized,
        Op: Encode,
    {
        self.conn_inner.request(
            &protocol::Upsert {
                space_id: self.space_id,
                index_id: self.index_id,
                value,
                ops,
            },
            options,
        )
    }

    /// The remote-call equivalent of the local call `Space::delete(...)`
    /// (see [details](../index/struct.Index.html#method.delete)).
    pub fn delete<K>(&self, key: &K, options: &Options) -> Result<Option<Tuple>, Error>
    where
        K: ToTuple + ?Sized,
    {
        self.conn_inner.request(
            &protocol::Delete {
                space_id: self.space_id,
                index_id: self.index_id,
                key,
            },
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
