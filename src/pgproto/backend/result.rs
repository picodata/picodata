use crate::pgproto::backend::describe::CommandTag;
use crate::pgproto::error::PgResult;
use crate::pgproto::value::PgValue;
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::messages::data::{DataRow, RowDescription};
use std::sync::Arc;
use std::vec::IntoIter;

#[derive(Debug)]
pub struct Rows {
    desc: Arc<Vec<FieldInfo>>,
    rows: IntoIter<Vec<PgValue>>,
}

impl Rows {
    pub fn new(rows: Vec<Vec<PgValue>>, row_desc: Vec<FieldInfo>) -> Self {
        Self {
            rows: rows.into_iter(),
            desc: Arc::new(row_desc),
        }
    }

    pub fn encode_next(&mut self) -> PgResult<Option<DataRow>> {
        let Some(values) = self.rows.next() else {
            return Ok(None);
        };

        let mut encoder = DataRowEncoder::new(Arc::clone(&self.desc));
        for value in values {
            encoder.encode_field(&value)?;
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
pub struct AclOrDdlResult {
    pub tag: CommandTag,
}

#[derive(Debug)]
pub struct DmlResult {
    pub tag: CommandTag,
    pub row_count: usize,
}

#[derive(Debug)]
pub struct SuspendedDqlResult {
    pub rows: Rows,
}

#[derive(Debug)]
pub struct FinishedDqlResult {
    pub rows: Rows,
    pub tag: CommandTag,
    pub row_count: usize,
}

#[derive(Debug)]
pub enum ExecuteResult {
    AclOrDdl(AclOrDdlResult),
    Dml(DmlResult),
    SuspendedDql(SuspendedDqlResult),
    FinishedDql(FinishedDqlResult),
}

impl ExecuteResult {
    pub fn acl_or_ddl(tag: CommandTag) -> Self {
        Self::AclOrDdl(AclOrDdlResult { tag })
    }

    pub fn dml(row_count: usize, tag: CommandTag) -> Self {
        Self::Dml(DmlResult { row_count, tag })
    }

    pub fn suspended_dql(rows: Rows) -> Self {
        Self::SuspendedDql(SuspendedDqlResult { rows })
    }

    pub fn finished_dql(rows: Rows, tag: CommandTag) -> Self {
        let row_count = rows.row_count();
        Self::FinishedDql(FinishedDqlResult {
            rows,
            tag,
            row_count,
        })
    }
}
