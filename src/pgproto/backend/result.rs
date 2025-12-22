use crate::pgproto::{backend::describe::CommandTag, error::EncodingError, value::PgValue};
use pgwire::{
    api::results::{DataRowEncoder, FieldInfo},
    messages::data::{DataRow, RowDescription},
    types::ToSqlText,
};
use postgres_types::ToSql;
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

        fn encode_value<V>(encoder: &mut DataRowEncoder, value: &V) -> Result<(), EncodingError>
        where
            V: ToSql + ToSqlText + Sized,
        {
            encoder.encode_field(value).map_err(EncodingError::new)
        }

        for value in &values {
            match value {
                PgValue::Float(v) => encode_value(&mut self.encoder, v)?,
                PgValue::Integer(v) => encode_value(&mut self.encoder, v)?,
                PgValue::Boolean(v) => encode_value(&mut self.encoder, v)?,
                PgValue::Text(v) => encode_value(&mut self.encoder, v)?,
                PgValue::Timestamptz(v) => encode_value(&mut self.encoder, v)?,
                PgValue::Json(v) => encode_value(&mut self.encoder, v)?,
                PgValue::Uuid(v) => encode_value(&mut self.encoder, v)?,
                PgValue::Numeric(v) => encode_value(&mut self.encoder, v)?,
                PgValue::Null => encode_value(&mut self.encoder, &None::<i64>)?,
            }
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
