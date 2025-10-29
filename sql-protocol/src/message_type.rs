use rmp::encode::{write_array_len, write_str};
use std::fmt::Display;

#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum MessageType {
    DQL = 0,
    DML = 1,
    LocalDML = 2,
}

impl Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl TryFrom<u8> for MessageType {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MessageType::DQL),
            1 => Ok(MessageType::DML),
            2 => Ok(MessageType::LocalDML),
            n => Err(format!("Unknown message type: {n}")),
        }
    }
}

pub(crate) fn write_request_header(
    w: &mut impl std::io::Write,
    message_type: MessageType,
    request_id: &str,
) -> Result<(), std::io::Error> {
    write_array_len(w, 3)?;
    write_str(w, request_id)?;
    rmp::encode::write_pfix(w, message_type as u8)?;

    Ok(())
}
