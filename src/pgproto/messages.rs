use super::backend::describe::CommandTag;
use super::error::{PgError, PgResult};
use super::stream::BeMessage;
use bytes::Bytes;
use pgwire::error::ErrorInfo;
use pgwire::messages::copy::CopyInResponse;
use pgwire::messages::data::{self, DataRow, ParameterDescription, RowDescription};
use pgwire::messages::extendedquery::{
    BindComplete, CloseComplete, ParseComplete, PortalSuspended,
};
use pgwire::messages::response::{NoticeResponse, ReadyForQuery, SslResponse, TransactionStatus};
use pgwire::messages::{response, startup::*, Message};
use postgres_types::Oid;
use smol_str::format_smolstr;

/// Notice for the frontend.
pub fn notice(message: String) -> BeMessage {
    BeMessage::NoticeResponse(NoticeResponse::new(vec![
        // Notice response always has severity, code, and message fields.
        // See https://www.postgresql.org/docs/current/protocol-error-fields.html#PROTOCOL-ERROR-FIELDS
        (b'S', "NOTICE".to_string()),
        (b'C', "00000".to_string()),
        (b'M', message),
    ]))
}

/// MD5AuthRequest requests md5 password from the frontend.
pub fn md5_auth_request(salt: &[u8; 4]) -> BeMessage {
    BeMessage::Authentication(Authentication::MD5Password(salt.to_vec()))
}

/// CleartextPassword retrieves password from client in open form.
/// Can be used not only with open password auth but with LDAP too.
pub fn cleartext_auth_request() -> BeMessage {
    BeMessage::Authentication(Authentication::CleartextPassword)
}

pub fn sasl_auth_request() -> BeMessage {
    // TODO: use `crate::scram::METHODS` to enable channel binding.
    BeMessage::Authentication(Authentication::SASL(
        crate::scram::METHODS_WITHOUT_PLUS
            .iter()
            .map(|s| s.to_string())
            .collect(),
    ))
}

pub fn sasl_continue(data: impl AsRef<[u8]>) -> BeMessage {
    let bytes = Bytes::copy_from_slice(data.as_ref());
    BeMessage::Authentication(Authentication::SASLContinue(bytes))
}

pub fn sasl_final(data: impl AsRef<[u8]>) -> BeMessage {
    let bytes = Bytes::copy_from_slice(data.as_ref());
    BeMessage::Authentication(Authentication::SASLFinal(bytes))
}

/// AuthOk informs the frontend that the authentication has been passed.
pub const fn auth_ok() -> BeMessage {
    BeMessage::Authentication(Authentication::Ok)
}

/// ReadyForQuery informs the frontend that it can safely send a new command.
pub fn ready_for_query() -> BeMessage {
    BeMessage::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle))
}

/// ErrorResponse informs the client about the error.
pub fn error_response(info: ErrorInfo) -> BeMessage {
    BeMessage::ErrorResponse(info.into())
}

/// CommandComplete informs the client that there are no more rows.
pub fn command_complete(tag: &CommandTag) -> BeMessage {
    BeMessage::CommandComplete(response::CommandComplete::new(tag.as_str().to_owned()))
}

pub fn empty_query_response() -> BeMessage {
    BeMessage::EmptyQueryResponse(response::EmptyQueryResponse::new())
}

pub fn command_complete_with_row_count(tag: &CommandTag, row_count: usize) -> BeMessage {
    let tag = format!("{} {}", tag.as_str(), row_count);
    BeMessage::CommandComplete(response::CommandComplete::new(tag))
}

pub fn copy_command_complete(row_count: usize) -> BeMessage {
    BeMessage::CommandComplete(response::CommandComplete::new(format!("COPY {row_count}")))
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

pub fn ssl_accept() -> BeMessage {
    BeMessage::SslResponse(SslResponse::Accept)
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

const COPY_IN_RESPONSE_OVERHEAD_BYTES: usize = 7;

fn copy_in_response_max_column_count() -> usize {
    (CopyInResponse::max_message_length() - COPY_IN_RESPONSE_OVERHEAD_BYTES) / 2
}

pub fn copy_in_response_text(column_count: usize) -> PgResult<BeMessage> {
    let response_column_count = i16::try_from(column_count).map_err(|_| {
        PgError::FeatureNotSupported(format_smolstr!(
            "COPY FROM STDIN does not support {column_count} columns: CopyInResponse encodes column count as Int16"
        ))
    })?;

    let max_column_count = copy_in_response_max_column_count();
    if column_count > max_column_count {
        return Err(PgError::FeatureNotSupported(format_smolstr!(
            "COPY FROM STDIN does not support {column_count} columns: CopyInResponse exceeds pgwire backend message limit of {} bytes (max {} columns)",
            CopyInResponse::max_message_length(),
            max_column_count
        )));
    }

    Ok(BeMessage::CopyInResponse(CopyInResponse::new(
        0,
        response_column_count,
        vec![0; column_count],
    )))
}

#[cfg(test)]
mod tests {
    use super::copy_in_response_text;
    use crate::pgproto::error::PgError;
    use pgwire::messages::copy::CopyInResponse;
    use pgwire::messages::Message;

    #[test]
    fn copy_in_response_text_rejects_column_count_above_i16_max() {
        let err = copy_in_response_text(i16::MAX as usize + 1).unwrap_err();

        assert!(matches!(err, PgError::FeatureNotSupported(_)));
        assert_eq!(
            err.to_string(),
            "feature is not supported: COPY FROM STDIN does not support 32768 columns: CopyInResponse encodes column count as Int16"
        );
    }

    #[test]
    fn copy_in_response_text_rejects_column_count_above_pgwire_backend_limit() {
        let backend_limit = CopyInResponse::max_message_length();
        let max_column_count = super::copy_in_response_max_column_count();
        let rejected_column_count = max_column_count + 1;
        let err = copy_in_response_text(rejected_column_count).unwrap_err();

        assert!(matches!(err, PgError::FeatureNotSupported(_)));
        assert_eq!(
            err.to_string(),
            format!(
                "feature is not supported: COPY FROM STDIN does not support {rejected_column_count} columns: CopyInResponse exceeds pgwire backend message limit of {backend_limit} bytes (max {max_column_count} columns)"
            )
        );
    }
}
