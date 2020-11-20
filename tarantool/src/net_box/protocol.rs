use std::io::{Cursor, Write};

use crate::error::Error;
use crate::tuple::AsTuple;

enum IProtoKey {
    RequestType = 0x00,
    Sync = 0x01,
    Tuple = 0x21,
    FunctionName = 0x22,
}

enum IProtoType {
    Call16 = 6,
    Call = 10,
    Ping = 64,
}

fn prepare_request(
    buf: &mut Cursor<Vec<u8>>,
    sync: u64,
    request_type: IProtoType,
) -> Result<u64, Error> {
    rmp::encode::write_u32(buf, 0)?;
    let header_offset = buf.position();

    // Header fields
    rmp::encode::write_map_len(buf, 2)?;
    rmp::encode::write_pfix(buf, IProtoKey::RequestType as u8)?;
    rmp::encode::write_pfix(buf, request_type as u8)?;
    rmp::encode::write_pfix(buf, IProtoKey::Sync as u8)?;
    rmp::encode::write_uint(buf, sync)?;

    Ok(header_offset)
}

pub fn encode_request(buf: &mut Cursor<Vec<u8>>, header_offset: u64) -> Result<(), Error> {
    let len = buf.position() - header_offset;
    buf.set_position(0);
    rmp::encode::write_u32(buf, len as u32)?;
    Ok(())
}

pub fn encode_ping(buf: &mut Cursor<Vec<u8>>, sync: u64) -> Result<(), Error> {
    let header_offset = prepare_request(buf, sync, IProtoType::Ping)?;
    rmp::encode::write_map_len(buf, 0)?;
    encode_request(buf, header_offset)?;
    Ok(())
}

pub fn encode_call<T>(
    buf: &mut Cursor<Vec<u8>>,
    sync: u64,
    function_name: &str,
    args: &T,
) -> Result<(), Error>
where
    T: AsTuple,
{
    let header_offset = prepare_request(buf, sync, IProtoType::Ping)?;
    rmp::encode::write_map_len(buf, 2)?;
    rmp::encode::write_pfix(buf, IProtoKey::FunctionName as u8)?;
    rmp::encode::write_str(buf, function_name)?;
    rmp::encode::write_pfix(buf, IProtoKey::Tuple as u8)?;
    rmp_serde::encode::write(buf, args)?;
    encode_request(buf, header_offset)?;
    Ok(())
}
