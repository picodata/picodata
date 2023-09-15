use pgwire::messages::simplequery::Query;
use std::io;

use crate::{error::PgResult, messages, sql::statement::Statement, stream::PgStream};

pub fn process_query_message(
    stream: &mut PgStream<impl io::Read + io::Write>,
    query: Query,
) -> PgResult<()> {
    let statement = Statement::prepare(query.query())?;
    let mut portal = statement.bind()?;

    if portal.sends_rows() {
        let row_description = messages::row_description(portal.row_description()?);
        stream.write_message_noflush(row_description)?;
    }

    while let Some(data_row) = portal.execute_one()? {
        let data_row = messages::data_row(data_row);
        stream.write_message_noflush(data_row)?;
    }

    let tag = portal.command_tag().as_str();
    let command_complete = messages::command_complete(tag, portal.row_count());
    stream.write_message(command_complete)?;
    Ok(())
}
