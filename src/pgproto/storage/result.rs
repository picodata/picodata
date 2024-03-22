use super::{
    describe::{CommandTag, PortalDescribe, QueryType},
    value::{Format, PgValue},
};
use crate::pgproto::error::PgResult;
use bytes::BytesMut;
use pgwire::messages::data::{DataRow, RowDescription};
use std::iter::zip;
use std::vec::IntoIter;

fn encode_row(values: Vec<PgValue>, formats: &[Format], buf: &mut BytesMut) -> DataRow {
    let row = zip(values, formats)
        .map(|(v, f)| v.encode(f, buf).unwrap())
        .collect();
    DataRow::new(row)
}

pub struct ExecuteResult {
    describe: PortalDescribe,
    values_stream: IntoIter<Vec<PgValue>>,
    row_count: usize,
    is_portal_finished: bool,
    buf: BytesMut,
}

impl ExecuteResult {
    pub fn new(
        rows: Vec<Vec<PgValue>>,
        describe: PortalDescribe,
        is_portal_finished: bool,
    ) -> Self {
        let values_stream = rows.into_iter();
        Self {
            values_stream,
            describe,
            row_count: 0,
            is_portal_finished,
            buf: BytesMut::default(),
        }
    }

    pub fn empty(row_count: usize, describe: PortalDescribe) -> Self {
        Self {
            values_stream: Default::default(),
            describe,
            row_count,
            is_portal_finished: true,
            buf: BytesMut::default(),
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
}

impl Iterator for ExecuteResult {
    type Item = DataRow;

    fn next(&mut self) -> Option<DataRow> {
        self.values_stream.next().map(|row| {
            let row = encode_row(row, self.describe.output_format(), &mut self.buf);
            self.buf.clear();
            self.row_count += 1;
            row
        })
    }
}
