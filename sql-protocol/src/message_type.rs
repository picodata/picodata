use rmp::encode::{write_array_len, write_str};

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum MessageType {
    DQL = 0,
    DML = 1,
    LocalDML = 2,
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
    mut w: impl std::io::Write,
    message_type: MessageType,
    request_id: &str,
) -> Result<(), std::io::Error> {
    write_array_len(&mut w, 3)?;
    write_str(&mut w, request_id)?;
    rmp::encode::write_pfix(&mut w, message_type as u8)?;

    Ok(())
}
