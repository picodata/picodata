use super::describe::{CommandTag, QueryType};
use super::handle::Handle;
use super::statement::Statement;
use super::value::PgValue;
use crate::error::PgResult;
use bytes::BytesMut;
use pgwire::messages::data::{DataRow, RowDescription};
use serde_json::Value;
use std::vec::IntoIter;

enum PortalState {
    /// Freshly created, not executed yet.
    New,
    /// Query is executing, can be deleted.
    Running,
    /// Query execution is finished.
    Finished,
}

/// Portal is a result of statement binding.
/// It allows you to execute queries and acts as a cursor over the resulting tuples.
pub struct Portal {
    // TODO: consider using statement name or reference
    state: PortalState,
    statement: Statement,
    values_stream: IntoIter<Vec<PgValue>>,
    row_count: usize,
}

impl Portal {
    pub fn new(statement: Statement) -> Portal {
        Portal {
            statement,
            state: PortalState::New,
            values_stream: IntoIter::default(),
            row_count: 0,
        }
    }

    /// Take next tuple.
    pub fn execute_one(&mut self) -> PgResult<Option<DataRow>> {
        if let PortalState::New = self.state {
            // NOTE: this changes the state.
            self.execute_all_and_store()?;
        }

        if let PortalState::Finished = self.state {
            return Ok(None);
        }

        let mut buf = BytesMut::new();
        if let Some(values) = self.values_stream.next() {
            let row = encode_row(values, &mut buf);
            self.row_count += 1;
            buf.clear();
            Ok(Some(row))
        } else {
            self.state = PortalState::Finished;
            Ok(None)
        }
    }

    /// Get the format of the output tuples.
    pub fn row_description(&self) -> PgResult<RowDescription> {
        self.statement.describe().row_description()
    }

    pub fn command_tag(&self) -> &CommandTag {
        self.statement.describe().command_tag()
    }

    pub fn sends_rows(&self) -> bool {
        let query_type = self.statement.describe().query_type();
        matches!(query_type, QueryType::Dql | QueryType::Explain)
    }

    /// Get the number of returned or modified tuples.
    /// None is returned if portal doesn't return or modify tuples.
    pub fn row_count(&self) -> Option<usize> {
        let query_type = self.statement.describe().query_type();
        match query_type {
            QueryType::Dml | QueryType::Dql | QueryType::Explain => Some(self.row_count),
            _ => None,
        }
    }

    /// Execute statement and store result in portal.
    fn execute_all_and_store(&mut self) -> PgResult<()> {
        let query_type = self.statement.describe().query_type();
        let handle = self.statement.handle();
        match query_type {
            QueryType::Dql => {
                self.values_stream = execute_dql(handle)?.into_iter();
                self.state = PortalState::Running;
            }
            QueryType::Acl | QueryType::Ddl | QueryType::Dml => {
                self.row_count = execute_acl_ddl_or_dml(handle)?;
                self.state = PortalState::Finished;
            }
            QueryType::Explain => {
                self.values_stream = execute_explain(handle)?.into_iter();
                self.state = PortalState::Running;
            }
        }
        Ok(())
    }
}

fn encode_row(values: Vec<PgValue>, buf: &mut BytesMut) -> DataRow {
    let row = values.into_iter().map(|v| v.encode(buf).unwrap()).collect();
    DataRow::new(row)
}

fn execute_dql(handle: &Handle) -> PgResult<Vec<Vec<PgValue>>> {
    let raw: Vec<Vec<Value>> = handle.execute_and_get("rows")?;
    Ok(raw
        .into_iter()
        .map(|row| row.into_iter().map(PgValue::from).collect())
        .collect())
}

fn execute_acl_ddl_or_dml(handle: &Handle) -> PgResult<usize> {
    let row_count: usize = handle.execute_and_get("row_count")?;
    Ok(row_count)
}

fn execute_explain(handle: &Handle) -> PgResult<Vec<Vec<PgValue>>> {
    let explain: Vec<Value> = handle.execute()?;
    Ok(explain
        .into_iter()
        // every row must be a vector
        .map(|val| vec![PgValue::from(val)])
        .collect())
}
