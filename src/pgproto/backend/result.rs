use crate::pgproto::backend::describe::{CommandTag, PortalDescribe, QueryType};
use crate::pgproto::error::PgResult;
use crate::pgproto::value::{Format, PgValue};
use bytes::BytesMut;
use pgwire::messages::data::{DataRow, RowDescription};
use std::iter::zip;
use std::vec::IntoIter;

fn encode_row(values: Vec<PgValue>, formats: &[Format], buf: &mut BytesMut) -> PgResult<DataRow> {
    let row = zip(values, formats)
        .map(|(v, f)| v.encode(f, buf))
        .collect::<PgResult<_>>()?;
    Ok(DataRow::new(row))
}

#[derive(Debug)]
pub struct ExecuteResult {
    describe: PortalDescribe,
    values_stream: IntoIter<Vec<PgValue>>,
    row_count: usize,
    is_portal_finished: bool,
    buf: BytesMut,
}

impl ExecuteResult {
    /// Create a new finished result. It is used for non-dql queries.
    pub fn new(row_count: usize, describe: PortalDescribe) -> Self {
        Self {
            values_stream: Default::default(),
            describe,
            row_count,
            is_portal_finished: true,
            buf: BytesMut::default(),
        }
    }

    /// Create a query result with rows. It is used for dql-like queries.
    pub fn with_rows(self, rows: Vec<Vec<PgValue>>, is_portal_finished: bool) -> Self {
        let values_stream = rows.into_iter();
        Self {
            values_stream,
            row_count: 0,
            is_portal_finished,
            ..self
        }
    }

    pub fn command_tag(&self) -> &CommandTag {
        self.describe.command_tag()
    }

    pub fn is_portal_finished(&self) -> bool {
        self.is_portal_finished
    }

    pub fn row_description(&self) -> PgResult<Option<RowDescription>> {
        self.describe.row_description()
    }

    pub fn row_count(&self) -> Option<usize> {
        match self.describe.query_type() {
            QueryType::Dml | QueryType::Dql | QueryType::Explain => Some(self.row_count),
            _ => None,
        }
    }

    /// Try to get next row from the result. If there are no rows, None is returned.
    ///
    /// Error is returned in case of an encoding error.
    pub fn next_row(&mut self) -> PgResult<Option<DataRow>> {
        let Some(row) = self.values_stream.next() else {
            return Ok(None);
        };
        let row = encode_row(row, self.describe.output_format(), &mut self.buf)?;
        self.buf.clear();
        self.row_count += 1;
        Ok(Some(row))
    }
}

impl ExecuteResult {
    pub fn query_type(&self) -> &QueryType {
        self.describe.describe.query_type()
    }

    pub fn into_values_stream(self) -> IntoIter<Vec<PgValue>> {
        self.values_stream
    }
}
