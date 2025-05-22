use crate::pgproto::{backend::describe::CommandTag, error::EncodingError, value::PgValue};
use pgwire::{
    api::results::{DataRowEncoder, FieldInfo},
    messages::data::{DataRow, RowDescription},
};
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

    pub fn encode_next(&mut self) -> Result<Option<DataRow>, EncodingError> {
        let Some(values) = self.rows.next() else {
            return Ok(None);
        };

        let mut encoder = DataRowEncoder::new(Default::default());
        for (i, value) in values.iter().enumerate() {
            let format = self.desc.get(i).unwrap().format();
            value.encode(format, &mut encoder)?;
        }

        Ok(Some(encoder.finish().map_err(EncodingError::new)?))
    }

    pub fn describe(&self) -> RowDescription {
        RowDescription::new(self.desc.iter().map(Into::into).collect())
    }

    pub fn values(&self) -> Vec<Vec<PgValue>> {
        self.rows.clone().collect()
    }
}

#[derive(Debug)]
pub enum ExecuteResult {
    AclOrDdl {
        /// Tag of the command.
        tag: CommandTag,
    },
    Dml {
        /// Tag of the command.
        tag: CommandTag,
        row_count: usize,
    },
    Tcl {
        /// Tag of the command.
        tag: CommandTag,
    },
    SuspendedDql {
        /// Rows we'll send to the client.
        rows: Rows,
    },
    FinishedDql {
        /// Tag of the command.
        tag: CommandTag,
        /// Rows we'll send to the client.
        rows: Rows,
        /// Cached number of rows in result.
        /// Note: Rows is an iterator that contains only remaining rows. So it's
        /// necessary to cache the number of rows before retrieving them.
        row_count: usize,
    },
    /// Result of an empty query.
    Empty,
}
