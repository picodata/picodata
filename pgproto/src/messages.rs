use crate::stream::BeMessage;
use pgwire::error::ErrorInfo;
use pgwire::messages::{response, startup::*};

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
