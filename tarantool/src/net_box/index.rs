use crate::error::Error;
use crate::index::IteratorType;
use crate::net_box::inner::ConnInner;
use crate::net_box::{protocol, Options};
use crate::tuple::{AsTuple, Tuple};
use std::io::Cursor;
use std::rc::Rc;

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
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);

        let sync = self.conn_inner.next_sync();
        protocol::encode_select(
            &mut cur,
            sync,
            self.space_id,
            self.index_id,
            u32::max_value(),
            0,
            iterator_type,
            key,
        )?;
        let response = self
            .conn_inner
            .communicate(&cur.into_inner(), sync, options)?;

        Ok(RemoteIndexIterator {
            inner: response.into_iter()?,
        })
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
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);

        let sync = self.conn_inner.next_sync();
        protocol::encode_update(&mut cur, sync, self.space_id, self.index_id, key, ops)?;
        Ok(self
            .conn_inner
            .communicate(&cur.into_inner(), sync, options)?
            .into_iter()?
            .and_then(|ref mut iter| iter.next_tuple()))
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
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);

        let sync = self.conn_inner.next_sync();
        protocol::encode_upsert(&mut cur, sync, self.space_id, self.index_id, value, ops)?;
        Ok(self
            .conn_inner
            .communicate(&cur.into_inner(), sync, options)?
            .into_iter()?
            .and_then(|ref mut iter| iter.next_tuple()))
    }

    /// The remote-call equivalent of the local call `Space::delete(...)`
    /// (see [details](../index/struct.Index.html#method.delete)).
    pub fn delete<K>(&mut self, key: &K, options: &Options) -> Result<Option<Tuple>, Error>
    where
        K: AsTuple,
    {
        let buf = Vec::new();
        let mut cur = Cursor::new(buf);

        let sync = self.conn_inner.next_sync();
        protocol::encode_delete(&mut cur, sync, self.space_id, self.index_id, key)?;
        Ok(self
            .conn_inner
            .communicate(&cur.into_inner(), sync, options)?
            .into_iter()?
            .and_then(|ref mut iter| iter.next_tuple()))
    }
}

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
