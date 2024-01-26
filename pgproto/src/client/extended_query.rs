use crate::stream::{BeMessage, FeMessage};
use crate::{
    error::{PgError, PgResult},
    messages,
    storage::StorageManager,
    stream::PgStream,
};
use pgwire::messages::extendedquery::{Bind, Close, Describe, Execute, Parse};
use std::io;

pub fn process_parse_message(
    stream: &mut PgStream<impl io::Write>,
    manager: &StorageManager,
    parse: Parse,
) -> PgResult<()> {
    if !parse.type_oids().is_empty() {
        return Err(PgError::FeatureNotSupported("parameterized queries".into()));
    }
    manager.parse(parse.name().as_deref(), parse.query())?;
    stream.write_message_noflush(messages::parse_complete())?;
    Ok(())
}

pub fn process_bind_message(
    stream: &mut PgStream<impl io::Write>,
    manager: &StorageManager,
    bind: Bind,
) -> PgResult<()> {
    if !bind.parameters().is_empty() {
        return Err(PgError::FeatureNotSupported("parameterized queries".into()));
    }

    manager.bind(
        bind.statement_name().as_deref(),
        bind.portal_name().as_deref(),
    )?;
    stream.write_message_noflush(messages::bind_complete())?;
    Ok(())
}

pub fn process_execute_message(
    stream: &mut PgStream<impl io::Write>,
    manager: &StorageManager,
    execute: Execute,
) -> PgResult<()> {
    let mut count = *execute.max_rows() as i64;
    let mut execute_result = manager.execute(execute.name().as_deref())?;
    if count <= 0 {
        count = std::i64::MAX;
    }

    for _ in 0..count {
        if let Some(row) = execute_result.next() {
            stream.write_message_noflush(messages::data_row(row))?;
        } else {
            break;
        }
    }

    if execute_result.is_portal_finished() {
        let tag = execute_result.command_tag().as_str();
        stream
            .write_message_noflush(messages::command_complete(tag, execute_result.row_count()))?;
    } else {
        stream.write_message_noflush(messages::portal_suspended())?;
    }

    Ok(())
}

fn describe_statement(
    manager: &StorageManager,
    statement: Option<&str>,
) -> PgResult<(BeMessage, BeMessage)> {
    let stmt_describe = manager.describe_statement(statement)?;
    let param_oids = stmt_describe.param_oids;
    let describe = stmt_describe.describe;

    let parameter_description = messages::parameter_description(param_oids);
    if let Some(row_description) = describe.row_description()? {
        Ok((
            parameter_description,
            messages::row_description(row_description),
        ))
    } else {
        Ok((parameter_description, messages::no_data()))
    }
}

fn describe_portal(manager: &StorageManager, portal: Option<&str>) -> PgResult<BeMessage> {
    let describe = manager.describe_portal(portal)?;
    if let Some(row_description) = describe.row_description()? {
        Ok(messages::row_description(row_description))
    } else {
        Ok(messages::no_data())
    }
}

pub fn process_describe_message(
    stream: &mut PgStream<impl io::Write>,
    manager: &StorageManager,
    describe: Describe,
) -> PgResult<()> {
    let name = describe.name().as_deref();
    match describe.target_type() {
        b'S' => {
            let (params_desc, rows_desc) = describe_statement(manager, name)?;
            stream.write_message_noflush(params_desc)?;
            stream.write_message_noflush(rows_desc)?;
            Ok(())
        }
        b'P' => {
            let rows_desc = describe_portal(manager, name)?;
            stream.write_message_noflush(rows_desc)?;
            Ok(())
        }
        _ => Err(PgError::ProtocolViolation(format!(
            "unknown describe type \'{}\'",
            describe.target_type()
        ))),
    }
}

pub fn process_close_message(
    stream: &mut PgStream<impl io::Write>,
    manager: &StorageManager,
    close: Close,
) -> PgResult<()> {
    let name = close.name().as_deref();
    match close.target_type() {
        b'S' => manager.close_statement(name)?,
        b'P' => manager.close_portal(name)?,
        _ => {
            return Err(PgError::ProtocolViolation(format!(
                "unknown close type \'{}\'",
                close.target_type()
            )));
        }
    }
    stream.write_message_noflush(messages::close_complete())?;
    Ok(())
}

pub fn process_sync_mesage(manager: &StorageManager) -> PgResult<()> {
    // By default, PG runs in autocommit mode, which means that every statement is ran inside its own transaction.
    // In simple query statement means the query inside a Query message.
    // In extended query statement means everything before a Sync message.
    // When PG gets a Sync mesage it finishes the current transaction by calling finish_xact_command,
    // which drops all non-holdable portals. We close all portals here because we don't have the holdable portals.
    manager.close_all_portals()
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
