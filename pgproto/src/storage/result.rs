use super::{
    describe::{CommandTag, Describe, QueryType},
    value::PgValue,
};
use crate::error::PgResult;
use bytes::BytesMut;
use pgwire::messages::data::{DataRow, RowDescription};
use std::vec::IntoIter;

fn encode_row(values: Vec<PgValue>, buf: &mut BytesMut) -> DataRow {
    let row = values.into_iter().map(|v| v.encode(buf).unwrap()).collect();
    DataRow::new(row)
}

pub struct ExecuteResult {
    describe: Describe,
    values_stream: IntoIter<Vec<PgValue>>,
    row_count: usize,
    buf: BytesMut,
}

impl ExecuteResult {
    pub fn new(rows: Vec<Vec<PgValue>>, describe: Describe) -> Self {
        let values_stream = rows.into_iter();
        Self {
            values_stream,
            describe,
            row_count: 0,
            buf: BytesMut::default(),
        }
    }

    pub fn empty(row_count: usize, describe: Describe) -> Self {
        Self {
            values_stream: Default::default(),
            describe,
            row_count,
            buf: BytesMut::default(),
        }
    }

    pub fn command_tag(&self) -> &CommandTag {
        self.describe.command_tag()
    }

    pub fn is_empty(&self) -> bool {
        self.values_stream.len() == 0
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
            let row = encode_row(row, &mut self.buf);
            self.buf.clear();
            self.row_count += 1;
            row
        })
    }
}
