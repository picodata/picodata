use crate::pgproto::backend::Backend;
use crate::pgproto::{error::PgResult, messages, stream::PgStream};
use pgwire::messages::simplequery::Query;
use std::io::{Read, Write};

pub fn process_query_message(
    stream: &mut PgStream<impl Read + Write>,
    backend: &Backend,
    query: Query,
) -> PgResult<()> {
    let mut query_result = backend.simple_query(query.query)?;

    if let Some(row_description) = query_result.row_description()? {
        let row_description = messages::row_description(row_description);
        stream.write_message_noflush(row_description)?;
    }

    while let Some(row) = query_result.next_row()? {
        stream.write_message_noflush(messages::data_row(row))?;
    }

    let tag = query_result.command_tag().as_str();
    let command_complete = messages::command_complete(tag, query_result.row_count());
    stream.write_message(command_complete)?;

    Ok(())
}
