use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;

use crate::error::Error;
use crate::fiber::Latch;
use crate::index::IteratorType;
use crate::space::SystemSpace;
use crate::tuple::Tuple;

use super::inner::ConnInner;
use super::options::Options;
use super::protocol::{decode_data, encode_select};

pub struct ConnSchema {
    pub(crate) version: Cell<u32>,
    space_ids: RefCell<HashMap<String, u32>>,
    index_ids: RefCell<HashMap<(u32, String), u32>>,
    lock: Latch,
}

impl ConnSchema {
    pub fn acquire(addrs: &Vec<SocketAddr>) -> Rc<ConnSchema> {
        let _lock = schema_cache.lock.lock();
        let mut cache = schema_cache.cache.borrow_mut();

        for addr in addrs {
            if let Some(schema) = cache.get(addr) {
                return schema.clone();
            }
        }

        let schema = Rc::new(ConnSchema {
            version: Cell::new(0),
            space_ids: Default::default(),
            index_ids: Default::default(),
            lock: Latch::new(),
        });

        for addr in addrs {
            cache.insert(addr.clone(), schema.clone());
        }

        schema
    }

    pub fn update(&self, conn_inner: &ConnInner) -> Result<(), Error> {
        let _lock = self.lock.lock();

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

        self.version.set(actual_schema_version);
        Ok(())
    }

    pub fn cached_version(&self) -> u32 {
        self.version.get()
    }

    pub fn lookup_space(&self, name: &str) -> Option<u32> {
        let _lock = self.lock.lock();
        self.space_ids.borrow().get(name).map(|id| id.clone())
    }

    pub fn lookup_index(&self, name: &str, space_id: u32) -> Option<u32> {
        let _lock = self.lock.lock();
        self.index_ids
            .borrow()
            .get(&(space_id, name.to_string()))
            .map(|id| id.clone())
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
    lock: Latch,
}

unsafe impl Sync for ConnSchemaCache {}

lazy_static! {
    static ref schema_cache: ConnSchemaCache = ConnSchemaCache {
        cache: RefCell::new(HashMap::new()),
        lock: Latch::new(),
    };
}
