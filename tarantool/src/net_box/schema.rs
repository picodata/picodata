use std::collections::HashMap;

use crate::error::Error;
use crate::index::IteratorType;
use crate::net_box::inner::ConnInner;
use crate::net_box::protocol::{decode_data, encode_select};
use crate::net_box::Options;
use crate::space::SystemSpace;
use crate::tuple::Tuple;

#[derive(Default)]
pub struct ConnSchema {
    pub(crate) version: u32,
    space_ids: HashMap<String, u32>,
    index_ids: HashMap<(u32, String), u32>,
}

impl ConnSchema {
    pub fn update(&mut self, conn_inner: &ConnInner) -> Result<(), Error> {
        for row in self.fetch_schema_spaces(conn_inner)? {
            let (id, _, name) = row.into_struct::<(u32, u32, String)>()?;
            self.space_ids.insert(name, id);
        }

        for row in self.fetch_schema_indexes(conn_inner)? {
            let (space_id, index_id, name) = row.into_struct::<(u32, u32, String)>()?;
            self.index_ids.insert((space_id, name), index_id);
        }

        Ok(())
    }

    pub fn lookup_space(&self, name: &str) -> Option<u32> {
        self.space_ids.get(name).map(|id| id.clone())
    }

    pub fn lookup_index(&self, name: &str, space_id: u32) -> Option<u32> {
        self.index_ids
            .get(&(space_id, name.to_string()))
            .map(|id| id.clone())
    }

    fn fetch_schema_spaces(&self, conn_inner: &ConnInner) -> Result<Vec<Tuple>, Error> {
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
            |buf| decode_data(buf, None),
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
            |buf| decode_data(buf, None),
            &Options::default(),
        )
    }
}
