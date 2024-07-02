use crate::pgproto::backend::describe::CommandTag;
use crate::pgproto::error::PgResult;
use crate::pgproto::value::PgValue;
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::messages::data::{DataRow, RowDescription};
use std::vec::IntoIter;

#[derive(Debug)]
pub struct Rows {
    desc: Vec<FieldInfo>,
    rows: IntoIter<Vec<PgValue>>,
}

impl Rows {
    pub fn new(rows: Vec<Vec<PgValue>>, row_desc: Vec<FieldInfo>) -> Self {
        Self {
            rows: rows.into_iter(),
            desc: row_desc,
        }
    }

    pub fn encode_next(&mut self) -> PgResult<Option<DataRow>> {
        let Some(values) = self.rows.next() else {
            return Ok(None);
        };

        let mut encoder = DataRowEncoder::new(Default::default());
        for (i, value) in values.iter().enumerate() {
            let format = self.desc.get(i).unwrap().format();
            value.encode(format, &mut encoder)?;
        }

        Ok(Some(encoder.finish()?))
    }

    pub fn describe(&self) -> RowDescription {
        RowDescription::new(self.desc.iter().map(Into::into).collect())
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn values(&self) -> Vec<Vec<PgValue>> {
        self.rows.clone().collect()
    }
}

#[derive(Debug)]
pub enum ExecuteResult {
    AclOrDdl {
        tag: CommandTag,
    },
    Dml {
        tag: CommandTag,
        row_count: usize,
    },
    SuspendedDql {
        rows: Rows,
    },
    FinishedDql {
        rows: Rows,
        tag: CommandTag,
        row_count: usize,
    },
}
