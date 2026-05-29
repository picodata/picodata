use crate::pgproto::{backend::describe::CommandTag, error::EncodingError, value::PgValue};
use pgwire::{
    api::results::{DataRowEncoder, FieldInfo},
    messages::data::{DataRow, RowDescription},
};
use std::{sync::Arc, vec::IntoIter};

pub struct Rows {
    desc: Arc<Vec<FieldInfo>>,
    encoder: DataRowEncoder,
    rows: IntoIter<Vec<PgValue>>,
}

impl Rows {
    pub fn new(rows: Vec<Vec<PgValue>>, row_desc: Vec<FieldInfo>) -> Self {
        let desc = Arc::new(row_desc);
        let encoder = DataRowEncoder::new(Arc::clone(&desc));
        Self {
            desc,
            encoder,
            rows: rows.into_iter(),
        }
    }

    pub fn encode_next(&mut self) -> Result<Option<DataRow>, EncodingError> {
        let Some(values) = self.rows.next() else {
            return Ok(None);
        };

        for value in &values {
            self.encoder
                .encode_field(value)
                .map_err(EncodingError::new)?;
        }

        Ok(Some(self.encoder.take_row()))
    }

    pub fn describe(&self) -> RowDescription {
        RowDescription::new(self.desc.iter().map(Into::into).collect())
    }

    pub fn values(&self) -> Vec<Vec<PgValue>> {
        self.rows.clone().collect()
    }
}

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
