use std::rc::Rc;

use crate::error::Error;
use crate::index::IteratorType;
use crate::net_box::inner::ConnInner;
use crate::net_box::{protocol, Options};
use crate::tuple::{AsTuple, Tuple};

/// Remote index (a group of key values and pointers)
pub struct RemoteIndex {
    conn_inner: Rc<ConnInner>,
    space_id: u32,
    index_id: u32,
}

impl RemoteIndex {
    /// The remote-call equivalent of the local call `Index::get(...)`
    /// (see [details](../index/struct.Index.html#method.get)).
    pub fn get<K>(&self, key: &K, options: &Options) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
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
        K: AsTuple,
    {
        unimplemented!()
    }

    /// The remote-call equivalent of the local call `Space::update(...)`
    /// (see [details](../index/struct.Index.html#method.update)).
    pub fn update<K, Op>(
        &mut self,
        key: &K,
        ops: &Vec<Op>,
        options: &Options,
    ) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
        Op: AsTuple,
    {
        unimplemented!()
    }

    /// The remote-call equivalent of the local call `Space::upsert(...)`
    /// (see [details](../index/struct.Index.html#method.upsert)).
    pub fn upsert<T, Op>(
        &mut self,
        value: &T,
        ops: &Vec<Op>,
        options: &Options,
    ) -> Result<Option<Tuple>, Error>
    where
        T: AsTuple,
        Op: AsTuple,
    {
        unimplemented!()
    }

    /// The remote-call equivalent of the local call `Space::delete(...)`
    /// (see [details](../index/struct.Index.html#method.delete)).
    pub fn delete<K>(&mut self, key: &K, options: &Options) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        unimplemented!()
    }
}

/// Remote index iterator. Can be used with `for` statement
pub struct RemoteIndexIterator {
    inner: Option<protocol::ResponseIterator>,
}

impl<'a> Iterator for RemoteIndexIterator {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner {
            None => None,
            Some(ref mut inner) => inner.next_tuple(),
        }
    }
}
