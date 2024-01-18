use crate::stream::BeMessage;
use pgwire::error::ErrorInfo;
use pgwire::messages::data::{self, DataRow, ParameterDescription, RowDescription};
use pgwire::messages::extendedquery::{
    BindComplete, CloseComplete, ParseComplete, PortalSuspended,
};
use pgwire::messages::response::SslResponse;
use pgwire::messages::{response, startup::*};
use postgres_types::Oid;

/// MD5AuthRequest requests md5 password from the frontend.
pub fn md5_auth_request(salt: &[u8; 4]) -> BeMessage {
    BeMessage::Authentication(Authentication::MD5Password(salt.to_vec()))
}

/// AuthOk informs the frontend that the authentication has been passed.
pub const fn auth_ok() -> BeMessage {
    BeMessage::Authentication(Authentication::Ok)
}

/// ReadyForQuery informs the frontend that it can safely send a new command.
pub fn ready_for_query() -> BeMessage {
    BeMessage::ReadyForQuery(response::ReadyForQuery::new(response::READY_STATUS_IDLE))
}

/// ErrorResponse informs the client about the error.
pub fn error_response(info: ErrorInfo) -> BeMessage {
    BeMessage::ErrorResponse(info.into())
}

/// CommandComplete informs the client that there are no more rows.
pub fn command_complete(tag: &str, row_count: Option<usize>) -> BeMessage {
    let tag = if let Some(count) = row_count {
        format!("{tag} {count}")
    } else {
        tag.to_owned()
    };
    BeMessage::CommandComplete(response::CommandComplete::new(tag))
}

/// RowDescription defines gow to parse the following DataRow messages.
pub fn row_description(row_description: RowDescription) -> BeMessage {
    BeMessage::RowDescription(row_description)
}

/// DataRow contains one row from response.
pub fn data_row(data_row: DataRow) -> BeMessage {
    BeMessage::DataRow(data_row)
}

pub fn ssl_refuse() -> BeMessage {
    BeMessage::SslResponse(SslResponse::Refuse)
}

pub fn parse_complete() -> BeMessage {
    BeMessage::ParseComplete(ParseComplete::new())
}

pub fn no_data() -> BeMessage {
    BeMessage::NoData(data::NoData::new())
}

pub fn bind_complete() -> BeMessage {
    BeMessage::BindComplete(BindComplete::new())
}

pub fn portal_suspended() -> BeMessage {
    BeMessage::PortalSuspended(PortalSuspended::new())
}

pub fn close_complete() -> BeMessage {
    BeMessage::CloseComplete(CloseComplete::new())
}

pub fn parameter_description(type_ids: Vec<Oid>) -> BeMessage {
    BeMessage::ParameterDescription(ParameterDescription::new(type_ids))
}
