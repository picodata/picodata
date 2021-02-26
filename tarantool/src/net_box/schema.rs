#![allow(non_upper_case_globals)]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;

use crate::error::Error;
use crate::fiber::{Latch, LatchGuard};
use crate::index::IteratorType;
use crate::space::SystemSpace;
use crate::tuple::Tuple;

use super::inner::ConnInner;
use super::options::Options;
use super::protocol::{decode_data, encode_select};

pub struct ConnSchema {
    version: Cell<Option<u32>>,
    is_updating: Cell<bool>,
    space_ids: RefCell<HashMap<String, u32>>,
    index_ids: RefCell<HashMap<(u32, String), u32>>,
    lock: Latch,
}

impl ConnSchema {
    pub fn acquire(addrs: &Vec<SocketAddr>) -> Rc<ConnSchema> {
        let mut cache = schema_cache.cache.borrow_mut();

        for addr in addrs {
            if let Some(schema) = cache.get(addr) {
                return schema.clone();
            }
        }

        let schema = Rc::new(ConnSchema {
            version: Cell::new(None),
            is_updating: Cell::new(false),
            space_ids: Default::default(),
            index_ids: Default::default(),
            lock: Latch::new(),
        });

        for addr in addrs {
            cache.insert(addr.clone(), schema.clone());
        }

        schema
    }

    pub fn refresh(
        &self,
        conn_inner: &ConnInner,
        actual_version: Option<u32>,
    ) -> Result<bool, Error> {
        let mut _lock: Option<LatchGuard> = None;

        if self.is_updating.get() {
            _lock = Some(self.lock.lock());
        }

        let result = if self.is_outdated(actual_version) {
            if let None = _lock {
                _lock = Some(self.lock.lock());
            }

            self.update(conn_inner)?;
            true
        } else {
            false
        };
        Ok(result)
    }

    pub fn update(&self, conn_inner: &ConnInner) -> Result<(), Error> {
        self.is_updating.set(true);
        let (spaces_data, actual_schema_version) = self.fetch_schema_spaces(conn_inner)?;
        for row in spaces_data {
            let (id, _, name) = row.into_struct::<(u32, u32, String)>()?;
            self.space_ids.borrow_mut().insert(name, id);
        }

        for row in self.fetch_schema_indexes(conn_inner)? {
            let (space_id, index_id, name) = row.into_struct::<(u32, u32, String)>()?;
            self.index_ids
                .borrow_mut()
                .insert((space_id, name), index_id);
        }

        self.version.set(Some(actual_schema_version));
        self.is_updating.set(false);
        Ok(())
    }

    pub fn lookup_space(&self, name: &str) -> Option<u32> {
        self.space_ids.borrow().get(name).map(|id| id.clone())
    }

    pub fn lookup_index(&self, name: &str, space_id: u32) -> Option<u32> {
        self.index_ids
            .borrow()
            .get(&(space_id, name.to_string()))
            .map(|id| id.clone())
    }

    fn is_outdated(&self, actual_version: Option<u32>) -> bool {
        match actual_version {
            None => true,
            Some(actual_version) => match self.version.get() {
                None => true,
                Some(cached_version) => actual_version > cached_version,
            },
        }
    }

    fn fetch_schema_spaces(&self, conn_inner: &ConnInner) -> Result<(Vec<Tuple>, u32), Error> {
        conn_inner.request(
            |buf, sync| {
                encode_select(
                    buf,
                    sync,
                    SystemSpace::VSpace as u32,
                    0,
                    u32::max_value(),
                    0,
                    IteratorType::GT,
                    &(SystemSpace::SystemIdMax as u32,),
                )
            },
            |buf, header| Ok((decode_data(buf, None)?, header.schema_version)),
            &Options::default(),
        )
    }

    fn fetch_schema_indexes(&self, conn_inner: &ConnInner) -> Result<Vec<Tuple>, Error> {
        conn_inner.request(
            |buf, sync| {
                encode_select(
                    buf,
                    sync,
                    SystemSpace::VIndex as u32,
                    0,
                    u32::max_value(),
                    0,
                    IteratorType::All,
                    &Vec::<()>::new(),
                )
            },
            |buf, _| decode_data(buf, None),
            &Options::default(),
        )
    }
}

struct ConnSchemaCache {
    cache: RefCell<HashMap<SocketAddr, Rc<ConnSchema>>>,
}

unsafe impl Sync for ConnSchemaCache {}

lazy_static! {
    static ref schema_cache: ConnSchemaCache = ConnSchemaCache {
        cache: RefCell::new(HashMap::new()),
    };
}
