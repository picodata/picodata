use crate::pgproto::{
    backend::{
        describe::CommandTag,
        port_rows::{Row, RowSource},
    },
    error::{EncodingError, PgError, PgResult},
    value::PgValue,
};
use pgwire::{
    api::results::{DataRowEncoder, FieldInfo},
    messages::data::{DataRow, RowDescription},
};
use sql::ir::value::Value as SbroadValue;
use std::{io::Cursor, sync::Arc};

/// Rows of a query result on their way to the client.
///
/// The rows are decoded one by one, right before they are encoded into
/// [`DataRow`] messages, so neither the result set nor a single row is ever
/// materialized as a collection of [`PgValue`]s.
pub struct Rows {
    desc: Arc<Vec<FieldInfo>>,
    encoder: DataRowEncoder,
    source: RowSource,
}

impl Rows {
    pub fn new(source: RowSource, desc: Arc<Vec<FieldInfo>>) -> Self {
        let encoder = DataRowEncoder::new(Arc::clone(&desc));
        Self {
            desc,
            encoder,
            source,
        }
    }

    /// Decode the next row and encode it into a [`DataRow`] message.
    pub fn encode_next(&mut self) -> PgResult<Option<DataRow>> {
        // Destructured to borrow the source and the encoder independently.
        let Self {
            desc,
            encoder,
            source,
        } = self;

        let Some(row) = source.next_row() else {
            return Ok(None);
        };

        match row {
            Row::ExplainLine(line) => {
                let field = desc
                    .first()
                    .ok_or_else(|| PgError::other("an explain has no columns to send"))?;
                encode_field(encoder, field, &PgValue::Text(line.into()))?;
            }
            Row::Tuple(msgpack) => {
                let mut cursor = row_cursor(msgpack, desc)?;

                // Decode each column and encode it right away, so that no value
                // outlives the field it belongs to.
                for field in desc.iter() {
                    let value = PgValue::decode_mp(&mut cursor, field.datatype())?;
                    encode_field(encoder, field, &value)?;
                }
            }
        }

        Ok(Some(encoder.take_row()))
    }

    pub fn describe(&self) -> RowDescription {
        RowDescription::new(self.desc.iter().map(Into::into).collect())
    }

    /// Decode all the remaining rows into sbroad values.
    ///
    /// Note: unlike [`Rows::encode_next`], this materializes the whole batch,
    /// so it's only meant for the internal `.proc_pg_execute` API used in tests.
    pub fn into_sbroad_values(&mut self) -> PgResult<Vec<Vec<SbroadValue>>> {
        let Self { desc, source, .. } = self;

        let mut rows = Vec::with_capacity(source.len());
        while let Some(row) = source.next_row() {
            let values = match row {
                Row::ExplainLine(line) => vec![SbroadValue::from(line)],
                Row::Tuple(msgpack) => {
                    let mut cursor = row_cursor(msgpack, desc)?;
                    desc.iter()
                        .map(|field| {
                            let value = PgValue::decode_mp(&mut cursor, field.datatype())?;
                            SbroadValue::try_from(value)
                        })
                        .collect::<PgResult<Vec<_>>>()?
                }
            };
            rows.push(values);
        }

        Ok(rows)
    }
}

/// Encode a single value of a row.
///
/// Note: this is [`DataRowEncoder::encode_field`] without the clone of the type
/// and of the format options it makes for every field. It clones them because
/// it reads them from the schema it borrows from itself, while we hold that
/// schema separately and can just lend them out.
fn encode_field(encoder: &mut DataRowEncoder, field: &FieldInfo, value: &PgValue) -> PgResult<()> {
    encoder
        .encode_field_with_type_and_format(
            value,
            field.datatype(),
            field.format(),
            field.format_options(),
        )
        .map_err(EncodingError::new)?;

    Ok(())
}

/// Check that the row is an array with a value per column and return a cursor
/// positioned at its first value.
fn row_cursor<'mp>(msgpack: &'mp [u8], desc: &[FieldInfo]) -> PgResult<Cursor<&'mp [u8]>> {
    let mut cursor = Cursor::new(msgpack);

    let len = rmp::decode::read_array_len(&mut cursor).map_err(PgError::other)? as usize;
    if len != desc.len() {
        return Err(PgError::other(format!(
            "Expected {} columns, got {}",
            desc.len(),
            len
        )));
    }

    Ok(cursor)
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
