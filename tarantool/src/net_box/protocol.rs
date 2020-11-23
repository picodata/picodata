use std::io::{Cursor, Read, Write};

use crate::error::Error;
use crate::tuple::AsTuple;
use std::str::from_utf8;

const REQUEST_TYPE: u8 = 0x00;
const SYNC: u8 = 0x01;
const TUPLE: u8 = 0x21;
const FUNCTION_NAME: u8 = 0x22;

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
    rmp::encode::write_pfix(buf, REQUEST_TYPE)?;
    rmp::encode::write_pfix(buf, request_type as u8)?;
    rmp::encode::write_pfix(buf, SYNC)?;
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
    let header_offset = prepare_request(buf, sync, IProtoType::Call)?;
    rmp::encode::write_map_len(buf, 2)?;
    rmp::encode::write_pfix(buf, FUNCTION_NAME as u8)?;
    rmp::encode::write_str(buf, function_name)?;
    rmp::encode::write_pfix(buf, TUPLE as u8)?;
    rmp_serde::encode::write(buf, args)?;
    encode_request(buf, header_offset)?;
    Ok(())
}

pub fn decode_greeting(stream: &mut dyn Read) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::with_capacity(128);
    buf.resize(128, 0);

    stream.read_exact(&mut *buf)?;
    let salt = base64::decode(&buf[64..108]).unwrap();
    Ok(salt)
}

pub struct Response {}

pub fn decode_response<R: Read>(stream: &mut R) -> Result<Response, Error> {
    let response_len = rmp::decode::read_u32(stream)? as usize;
    let mut buf = Vec::with_capacity(response_len);
    buf.resize(response_len, 0);

    stream.read_exact(&mut *buf)?;
    let mut cur = Cursor::new(buf);

    let mut status_code: Option<u32> = None;
    let mut sync: Option<u32> = None;

    // decode header
    let header_len = rmp::decode::read_map_len(&mut cur)?;
    for _ in 0..header_len {
        let key = rmp::decode::read_pfix(&mut cur)?;
        match key {
            0 => status_code = Some(rmp::decode::read_int(&mut cur).unwrap()),
            SYNC => sync = Some(rmp::decode::read_int(&mut cur).unwrap()),
            _ => {
                rmpv::decode::read_value(&mut cur).unwrap();
            }
        };
    }

    // decode payload
    rmpv::decode::read_value(&mut cur).unwrap();

    Ok(Response {})
}
