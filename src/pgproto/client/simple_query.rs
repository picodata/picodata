use crate::pgproto::backend::result::{
    AclOrDdlResult, DmlResult, ExecuteResult, FinishedDqlResult,
};
use crate::pgproto::backend::Backend;
use crate::pgproto::error::PgError;
use crate::pgproto::{error::PgResult, messages, stream::PgStream};
use pgwire::messages::simplequery::Query;
use std::io::{Read, Write};

pub fn process_query_message(
    stream: &mut PgStream<impl Read + Write>,
    backend: &Backend,
    query: Query,
) -> PgResult<()> {
    match backend.simple_query(query.query)? {
        ExecuteResult::AclOrDdl(AclOrDdlResult { tag }) => {
            stream.write_message(messages::command_complete(&tag))?;
        }
        ExecuteResult::Dml(DmlResult { tag, row_count }) => {
            stream.write_message(messages::command_complete_with_row_count(&tag, row_count))?;
        }
        ExecuteResult::FinishedDql(FinishedDqlResult {
            mut rows,
            tag,
            row_count,
        }) => {
            stream.write_message_noflush(messages::row_description(rows.describe()))?;
            while let Some(row) = rows.encode_next()? {
                stream.write_message_noflush(messages::data_row(row))?;
            }
            stream.write_message(messages::command_complete_with_row_count(&tag, row_count))?;
        }
        ExecuteResult::SuspendedDql(_) => {
            return Err(PgError::InternalError(
                "portal cannot be suspended in simple query".into(),
            ))
        }
    }

    Ok(())
}
