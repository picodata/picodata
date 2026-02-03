use crate::pgproto::backend::result::ExecuteResult;
use crate::pgproto::backend::storage::{self, build_prepared_statement_metadata, PG_STATEMENTS};
use crate::pgproto::backend::Backend;
use crate::pgproto::error::EncodingError;
use crate::pgproto::stream::{BeMessage, FeMessage};
use crate::pgproto::{
    error::{PgError, PgResult},
    messages,
    stream::PgStream,
};
use pgwire::messages::extendedquery::{Bind, Close, Describe, Execute, Parse};
use smol_str::format_smolstr;
use std::io::{Read, Write};

pub fn query_metadata_message(
    backend: &Backend,
    name: Option<String>,
    query: &str,
) -> PgResult<BeMessage> {
    let key = storage::Key(backend.client_id(), name.unwrap_or_default().into());
    let statement = PG_STATEMENTS
        .with(|storage| storage.borrow().get(&key).map(|holder| holder.statement()))
        .ok_or_else(|| PgError::other(format!("Couldn't find statement '{}'.", key.1)))?;

    let metadata = build_prepared_statement_metadata(statement.prepared_statement(), query)?;
    let message = serde_json::to_string(&metadata).map_err(EncodingError::new)?;

    Ok(messages::notice(message))
}

pub fn process_parse_message(
    stream: &mut PgStream<impl Read + Write>,
    backend: &Backend,
    parse: Parse,
) -> PgResult<()> {
    backend.parse(parse.name.clone(), &parse.query, parse.type_oids)?;

    if backend.params().query_metadata_enabled() {
        let metadata = query_metadata_message(backend, parse.name, &parse.query)?;
        stream.write_message_noflush(metadata)?;
    }

    stream.write_message_noflush(messages::parse_complete())?;
    Ok(())
}

pub fn process_bind_message(
    stream: &mut PgStream<impl Read + Write>,
    backend: &Backend,
    bind: Bind,
) -> PgResult<()> {
    backend.bind(
        bind.statement_name,
        bind.portal_name,
        bind.parameters,
        &bind.parameter_format_codes,
        &bind.result_column_format_codes,
    )?;

    stream.write_message_noflush(messages::bind_complete())?;
    Ok(())
}

pub fn process_execute_message(
    stream: &mut PgStream<impl Read + Write>,
    backend: &Backend,
    execute: Execute,
) -> PgResult<()> {
    match backend.execute(execute.name, execute.max_rows as i64)? {
        ExecuteResult::AclOrDdl { tag } => {
            stream.write_message_noflush(messages::command_complete(&tag))?;
        }
        ExecuteResult::Tcl { tag } => {
            stream.write_message_noflush(messages::command_complete(&tag))?;
        }
        ExecuteResult::Dml { tag, row_count } => {
            stream.write_message_noflush(messages::command_complete_with_row_count(
                &tag, row_count,
            ))?;
        }
        ExecuteResult::FinishedDql {
            tag,
            mut rows,
            row_count,
        } => {
            while let Some(row) = rows.encode_next()? {
                stream.write_message_noflush(messages::data_row(row))?;
            }
            stream.write_message_noflush(messages::command_complete_with_row_count(
                &tag, row_count,
            ))?;
        }
        ExecuteResult::SuspendedDql { mut rows } => {
            while let Some(row) = rows.encode_next()? {
                stream.write_message_noflush(messages::data_row(row))?;
            }
            stream.write_message_noflush(messages::portal_suspended())?;
        }
        ExecuteResult::Empty => {
            stream.write_message(messages::empty_query_response())?;
        }
    }

    Ok(())
}

fn describe_statement(
    backend: &Backend,
    statement: Option<&str>,
) -> PgResult<(BeMessage, BeMessage)> {
    let stmt_describe = backend.describe_statement(statement)?;
    let param_oids = stmt_describe.param_oids;
    let describe = stmt_describe.describe;

    let parameter_description = messages::parameter_description(param_oids);
    if let Some(row_description) = describe.row_description() {
        Ok((
            parameter_description,
            messages::row_description(row_description),
        ))
    } else {
        Ok((parameter_description, messages::no_data()))
    }
}

fn describe_portal(backend: &Backend, portal: Option<&str>) -> PgResult<BeMessage> {
    let describe = backend.describe_portal(portal)?;
    if let Some(row_description) = describe.row_description() {
        Ok(messages::row_description(row_description))
    } else {
        Ok(messages::no_data())
    }
}

pub fn process_describe_message(
    stream: &mut PgStream<impl Read + Write>,
    backend: &Backend,
    describe: Describe,
) -> PgResult<()> {
    let name = describe.name.as_deref();
    match describe.target_type {
        b'S' => {
            let (params_desc, rows_desc) = describe_statement(backend, name)?;
            stream.write_message_noflush(params_desc)?;
            stream.write_message_noflush(rows_desc)?;
            Ok(())
        }
        b'P' => {
            let rows_desc = describe_portal(backend, name)?;
            stream.write_message_noflush(rows_desc)?;
            Ok(())
        }
        _ => Err(PgError::ProtocolViolation(format_smolstr!(
            "unknown describe type '{}'",
            describe.target_type
        ))),
    }
}

pub fn process_close_message(
    stream: &mut PgStream<impl Read + Write>,
    backend: &Backend,
    close: Close,
) -> PgResult<()> {
    let name = close.name.as_deref();
    match close.target_type {
        b'S' => backend.close_statement(name),
        b'P' => backend.close_portal(name),
        _ => {
            return Err(PgError::ProtocolViolation(format_smolstr!(
                "unknown close type '{}'",
                close.target_type
            )));
        }
    }
    stream.write_message_noflush(messages::close_complete())?;
    Ok(())
}

pub fn process_sync_message(backend: &Backend) {
    // By default, PG runs in autocommit mode, which means that every statement is ran inside its own transaction.
    // In simple query statement means the query inside a Query message.
    // In extended query statement means everything before a Sync message.
    // When PG gets a Sync mesage it finishes the current transaction by calling finish_xact_command,
    // which drops all non-holdable portals. We close all portals here because we don't have the holdable portals.
    backend.close_all_portals()
}

pub fn is_extended_query_message(message: &FeMessage) -> bool {
    matches!(
        message,
        FeMessage::Parse(_)
            | FeMessage::Close(_)
            | FeMessage::Bind(_)
            | FeMessage::Describe(_)
            | FeMessage::Execute(_)
            | FeMessage::Flush(_)
            | FeMessage::Sync(_)
    )
}
