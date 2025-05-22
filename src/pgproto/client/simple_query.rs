use crate::pgproto::backend::result::ExecuteResult;
use crate::pgproto::backend::Backend;
use crate::pgproto::{error::PgResult, messages, stream::PgStream};
use pgwire::messages::simplequery::Query;
use std::io::{Read, Write};

pub fn process_query_message(
    stream: &mut PgStream<impl Read + Write>,
    backend: &Backend,
    query: Query,
) -> PgResult<()> {
    match backend.simple_query(&query.query)? {
        ExecuteResult::AclOrDdl { tag } => {
            stream.write_message(messages::command_complete(&tag))?;
        }
        ExecuteResult::Tcl { tag } => {
            stream.write_message(messages::command_complete(&tag))?;
        }
        ExecuteResult::Dml { tag, row_count } => {
            stream.write_message(messages::command_complete_with_row_count(&tag, row_count))?;
        }
        ExecuteResult::FinishedDql {
            tag,
            mut rows,
            row_count,
        } => {
            stream.write_message_noflush(messages::row_description(rows.describe()))?;
            while let Some(row) = rows.encode_next()? {
                stream.write_message_noflush(messages::data_row(row))?;
            }
            stream.write_message(messages::command_complete_with_row_count(&tag, row_count))?;
        }
        ExecuteResult::Empty => {
            stream.write_message(messages::empty_query_response())?;
        }
        ExecuteResult::SuspendedDql { .. } => {
            unreachable!("portal cannot be suspended in simple query")
        }
    }

    Ok(())
}
