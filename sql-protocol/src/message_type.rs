#[repr(u8)]
#[derive(Copy, Clone)]
pub enum MessageType {
    DQL = 0,
}

impl TryFrom<u8> for MessageType {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MessageType::DQL),
            n => Err(format!("Unknown message type: {}", n)),
        }
    }
}
