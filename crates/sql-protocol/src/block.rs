use crate::message_type::{write_request_header, MessageType};
use std::io::Write;

pub fn write_block_packet(
    w: &mut impl Write,
    request_id: &str,
    data: &[u8],
) -> Result<(), std::io::Error> {
    write_request_header(w, MessageType::Block, request_id)?;
    w.write_all(data)?;
    Ok(())
}
