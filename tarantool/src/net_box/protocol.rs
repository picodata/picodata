use core::str::from_utf8;
use std::cmp::min;
use std::fmt::{Display, Formatter};
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::os::raw::c_char;

use byteorder::{BigEndian, ReadBytesExt};
use sha1::{Digest, Sha1};

use crate::error::Error;
use crate::index::IteratorType;
use crate::tuple::{AsTuple, Tuple};

const REQUEST_TYPE: u8 = 0x00;
const SYNC: u8 = 0x01;
const SCHEMA_VERSION: u8 = 0x05;

const SPACE_ID: u8 = 0x10;
const INDEX_ID: u8 = 0x11;
const LIMIT: u8 = 0x12;
const OFFSET: u8 = 0x13;
const ITERATOR: u8 = 0x14;
const INDEX_BASE: u8 = 0x15;

const KEY: u8 = 0x20;
const TUPLE: u8 = 0x21;
const FUNCTION_NAME: u8 = 0x22;
const USER_NAME: u8 = 0x23;
const EXPR: u8 = 0x27;
const OPS: u8 = 0x28;

const DATA: u8 = 0x30;
const ERROR: u8 = 0x31;

enum IProtoType {
    Select = 1,
    Insert = 2,
    Replace = 3,
    Update = 4,
    Delete = 5,
    Auth = 7,
    Eval = 8,
    Upsert = 9,
    Call = 10,
    Ping = 64,
}

fn encode_header(
    stream: &mut impl Write,
    sync: u64,
    request_type: IProtoType,
) -> Result<(), Error> {
    rmp::encode::write_map_len(stream, 2)?;
    rmp::encode::write_pfix(stream, REQUEST_TYPE)?;
    rmp::encode::write_pfix(stream, request_type as u8)?;
    rmp::encode::write_pfix(stream, SYNC)?;
    rmp::encode::write_uint(stream, sync)?;
    Ok(())
}

pub fn encode_auth(
    stream: &mut impl Write,
    user: &str,
    password: &str,
    salt: &Vec<u8>,
    sync: u64,
) -> Result<(), Error> {
    // prepare 'chap-sha1' scramble:
    // salt = base64_decode(encoded_salt);
    // step_1 = sha1(password);
    // step_2 = sha1(step_1);
    // step_3 = sha1(first_20_bytes_of_salt, step_2);
    // scramble = xor(step_1, step_3);

    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let mut step_1_and_scramble = hasher.finalize();

    let mut hasher = Sha1::new();
    hasher.update(step_1_and_scramble);
    let step_2 = hasher.finalize();

    let mut hasher = Sha1::new();
    hasher.update(&salt[0..20]);
    hasher.update(step_2);
    let step_3 = hasher.finalize();

    step_1_and_scramble
        .iter_mut()
        .zip(step_3.iter())
        .for_each(|(a, b)| *a ^= *b);

    encode_header(stream, sync, IProtoType::Auth)?;
    rmp::encode::write_map_len(stream, 2)?;

    // username:
    rmp::encode::write_pfix(stream, USER_NAME)?;
    rmp::encode::write_str(stream, user)?;

    // encrypted password:
    rmp::encode::write_pfix(stream, TUPLE)?;
    rmp::encode::write_array_len(stream, 2)?;
    rmp::encode::write_str(stream, "chap-sha1")?;
    rmp::encode::write_str_len(stream, 20)?;
    stream.write_all(&step_1_and_scramble)?;
    Ok(())
}

pub fn encode_ping(stream: &mut impl Write, sync: u64) -> Result<(), Error> {
    encode_header(stream, sync, IProtoType::Ping)?;
    rmp::encode::write_map_len(stream, 0)?;
    Ok(())
}

pub fn encode_call<T>(
    stream: &mut impl Write,
    sync: u64,
    function_name: &str,
    args: &T,
) -> Result<(), Error>
where
    T: AsTuple,
{
    encode_header(stream, sync, IProtoType::Call)?;
    rmp::encode::write_map_len(stream, 2)?;
    rmp::encode::write_pfix(stream, FUNCTION_NAME)?;
    rmp::encode::write_str(stream, function_name)?;
    rmp::encode::write_pfix(stream, TUPLE)?;
    rmp_serde::encode::write(stream, args)?;
    Ok(())
}

pub fn encode_eval<T>(
    stream: &mut impl Write,
    sync: u64,
    expression: &str,
    args: &T,
) -> Result<(), Error>
where
    T: AsTuple,
{
    encode_header(stream, sync, IProtoType::Eval)?;
    rmp::encode::write_map_len(stream, 2)?;
    rmp::encode::write_pfix(stream, EXPR)?;
    rmp::encode::write_str(stream, expression)?;
    rmp::encode::write_pfix(stream, TUPLE)?;
    rmp_serde::encode::write(stream, args)?;
    Ok(())
}

pub fn encode_select<K>(
    stream: &mut impl Write,
    sync: u64,
    space_id: u32,
    index_id: u32,
    limit: u32,
    offset: u32,
    iterator_type: IteratorType,
    key: &K,
) -> Result<(), Error>
where
    K: AsTuple,
{
    encode_header(stream, sync, IProtoType::Select)?;
    rmp::encode::write_map_len(stream, 6)?;
    rmp::encode::write_pfix(stream, SPACE_ID)?;
    rmp::encode::write_u32(stream, space_id)?;
    rmp::encode::write_pfix(stream, INDEX_ID)?;
    rmp::encode::write_u32(stream, index_id)?;
    rmp::encode::write_pfix(stream, LIMIT)?;
    rmp::encode::write_u32(stream, limit)?;
    rmp::encode::write_pfix(stream, OFFSET)?;
    rmp::encode::write_u32(stream, offset)?;
    rmp::encode::write_pfix(stream, ITERATOR)?;
    rmp::encode::write_u32(stream, iterator_type as u32)?;
    rmp::encode::write_pfix(stream, KEY)?;
    rmp_serde::encode::write(stream, key)?;
    Ok(())
}

pub fn encode_insert<T>(
    stream: &mut impl Write,
    sync: u64,
    space_id: u32,
    value: &T,
) -> Result<(), Error>
where
    T: AsTuple,
{
    encode_header(stream, sync, IProtoType::Insert)?;
    rmp::encode::write_map_len(stream, 2)?;
    rmp::encode::write_pfix(stream, SPACE_ID)?;
    rmp::encode::write_u32(stream, space_id)?;
    rmp::encode::write_pfix(stream, TUPLE)?;
    rmp_serde::encode::write(stream, value)?;
    Ok(())
}

pub fn encode_replace<T>(
    stream: &mut impl Write,
    sync: u64,
    space_id: u32,
    value: &T,
) -> Result<(), Error>
where
    T: AsTuple,
{
    encode_header(stream, sync, IProtoType::Replace)?;
    rmp::encode::write_map_len(stream, 2)?;
    rmp::encode::write_pfix(stream, SPACE_ID)?;
    rmp::encode::write_u32(stream, space_id)?;
    rmp::encode::write_pfix(stream, TUPLE)?;
    rmp_serde::encode::write(stream, value)?;
    Ok(())
}

pub fn encode_update<K, Op>(
    stream: &mut impl Write,
    sync: u64,
    space_id: u32,
    index_id: u32,
    key: &K,
    ops: &Op,
) -> Result<(), Error>
where
    K: AsTuple,
    Op: AsTuple,
{
    encode_header(stream, sync, IProtoType::Update)?;
    rmp::encode::write_map_len(stream, 4)?;
    rmp::encode::write_pfix(stream, SPACE_ID)?;
    rmp::encode::write_u32(stream, space_id)?;
    rmp::encode::write_pfix(stream, INDEX_ID)?;
    rmp::encode::write_u32(stream, index_id)?;
    rmp::encode::write_pfix(stream, KEY)?;
    rmp_serde::encode::write(stream, key)?;
    rmp::encode::write_pfix(stream, TUPLE)?;
    rmp_serde::encode::write(stream, ops)?;
    Ok(())
}

pub fn encode_upsert<T, Op>(
    stream: &mut impl Write,
    sync: u64,
    space_id: u32,
    index_id: u32,
    value: &T,
    ops: &Op,
) -> Result<(), Error>
where
    T: AsTuple,
    Op: AsTuple,
{
    encode_header(stream, sync, IProtoType::Upsert)?;
    rmp::encode::write_map_len(stream, 4)?;
    rmp::encode::write_pfix(stream, SPACE_ID)?;
    rmp::encode::write_u32(stream, space_id)?;
    rmp::encode::write_pfix(stream, INDEX_BASE)?;
    rmp::encode::write_u32(stream, index_id)?;
    rmp::encode::write_pfix(stream, OPS)?;
    rmp_serde::encode::write(stream, ops)?;
    rmp::encode::write_pfix(stream, TUPLE)?;
    rmp_serde::encode::write(stream, value)?;
    Ok(())
}

pub fn encode_delete<K>(
    stream: &mut impl Write,
    sync: u64,
    space_id: u32,
    index_id: u32,
    key: &K,
) -> Result<(), Error>
where
    K: AsTuple,
{
    encode_header(stream, sync, IProtoType::Delete)?;
    rmp::encode::write_map_len(stream, 3)?;
    rmp::encode::write_pfix(stream, SPACE_ID)?;
    rmp::encode::write_u32(stream, space_id)?;
    rmp::encode::write_pfix(stream, INDEX_ID)?;
    rmp::encode::write_u32(stream, index_id)?;
    rmp::encode::write_pfix(stream, KEY)?;
    rmp_serde::encode::write(stream, key)?;
    Ok(())
}

#[derive(Debug)]
pub struct Header {
    pub sync: u64,
    pub status_code: u32,
    pub schema_version: u32,
}

pub struct Response<T> {
    pub header: Header,
    pub payload: T,
}

pub fn decode_header(stream: &mut (impl Read + Seek)) -> Result<Header, Error> {
    let mut sync: Option<u64> = None;
    let mut status_code: Option<u32> = None;
    let mut schema_version: Option<u32> = None;

    let map_len = rmp::decode::read_map_len(stream)?;
    for _ in 0..map_len {
        let key = rmp::decode::read_pfix(stream)?;
        match key {
            0 => status_code = Some(rmp::decode::read_int(stream)?),
            SYNC => sync = Some(rmp::decode::read_int(stream)?),
            SCHEMA_VERSION => schema_version = Some(rmp::decode::read_int(stream)?),
            _ => skip_msgpack(stream)?,
        }
    }

    if sync.is_none() || status_code.is_none() || schema_version.is_none() {
        return Err(io::Error::from(io::ErrorKind::InvalidData).into());
    }

    Ok(Header {
        sync: sync.unwrap(),
        status_code: status_code.unwrap(),
        schema_version: schema_version.unwrap(),
    })
}

pub fn decode_error(stream: &mut impl Read) -> Result<ResponseError, Error> {
    let mut message: Option<String> = None;

    let map_len = rmp::decode::read_map_len(stream)?;
    for _ in 0..map_len {
        if rmp::decode::read_pfix(stream)? == ERROR {
            let str_len = rmp::decode::read_str_len(stream)? as usize;
            let mut str_buf = vec![0u8; str_len];
            stream.read_exact(&mut str_buf)?;
            message = Some(from_utf8(&mut str_buf)?.to_string());
        }
    }

    Ok(ResponseError {
        message: message.ok_or(io::Error::from(io::ErrorKind::InvalidData))?,
    })
}

pub fn decode_greeting(stream: &mut impl Read) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::with_capacity(128);
    buf.resize(128, 0);

    stream.read_exact(&mut *buf)?;
    let salt = base64::decode(&buf[64..108]).unwrap();
    Ok(salt)
}

pub fn decode_tuple(buffer: &mut Cursor<Vec<u8>>, _: &Header) -> Result<Option<Tuple>, Error> {
    let payload_len = rmp::decode::read_map_len(buffer)?;
    for _ in 0..payload_len {
        let key = rmp::decode::read_pfix(buffer)?;
        match key {
            DATA => {
                let payload_offset = buffer.position();
                let buf = buffer.get_mut();
                let payload_len = buf.len() as u64 - payload_offset;
                unsafe {
                    return Ok(Some(Tuple::from_raw_data(
                        buf.as_slice().as_ptr().add(payload_offset as usize) as *mut c_char,
                        payload_len as u32,
                    )));
                }
            }
            _ => {
                skip_msgpack(buffer)?;
            }
        };
    }
    Ok(None)
}

pub fn decode_data(
    buffer: &mut Cursor<Vec<u8>>,
    limit: Option<usize>,
) -> Result<Vec<Tuple>, Error> {
    let payload_len = rmp::decode::read_map_len(buffer)?;
    for _ in 0..payload_len {
        let key = rmp::decode::read_pfix(buffer)?;
        match key {
            DATA => unsafe {
                let items_count = rmp::decode::read_array_len(buffer)? as usize;
                let mut current_offset = buffer.position() as usize;
                let buf_ptr = buffer.get_mut().as_slice().as_ptr() as *mut c_char;
                let mut result = Vec::with_capacity(items_count);

                let items_count = match limit {
                    None => items_count,
                    Some(limit) => min(limit, items_count),
                };

                for _ in 0..items_count {
                    skip_msgpack(buffer)?;
                    let next_offset = buffer.position() as usize;
                    result.push(Tuple::from_raw_data(
                        buf_ptr.clone().add(current_offset) as *mut c_char,
                        (next_offset - current_offset) as u32,
                    ));
                    current_offset = next_offset;
                }
                return Ok(result);
            },
            _ => {
                skip_msgpack(buffer)?;
            }
        };
    }
    Ok(vec![])
}

pub fn decode_single_row(buffer: &mut Cursor<Vec<u8>>, _: &Header) -> Result<Option<Tuple>, Error> {
    decode_data(buffer, Some(1)).map(|result| result.into_iter().next())
}

fn skip_msgpack(cur: &mut (impl Read + Seek)) -> Result<(), Error> {
    use rmp::Marker;

    match rmp::decode::read_marker(cur)? {
        Marker::FixPos(_) | Marker::FixNeg(_) | Marker::Null | Marker::True | Marker::False => {}
        Marker::U8 | Marker::I8 => {
            cur.seek(SeekFrom::Current(1))?;
        }
        Marker::U16 | Marker::I16 => {
            cur.seek(SeekFrom::Current(2))?;
        }
        Marker::U32 | Marker::I32 | Marker::F32 => {
            cur.seek(SeekFrom::Current(4))?;
        }
        Marker::U64 | Marker::I64 | Marker::F64 => {
            cur.seek(SeekFrom::Current(8))?;
        }
        Marker::FixStr(len) => {
            cur.seek(SeekFrom::Current(len as i64))?;
        }
        Marker::Str8 | Marker::Bin8 => {
            let len = cur.read_u8()?;
            cur.seek(SeekFrom::Current(len as i64))?;
        }
        Marker::Str16 | Marker::Bin16 => {
            let len = cur.read_u16::<BigEndian>()?;
            cur.seek(SeekFrom::Current(len as i64))?;
        }
        Marker::Str32 | Marker::Bin32 => {
            let len = cur.read_u32::<BigEndian>()?;
            cur.seek(SeekFrom::Current(len as i64))?;
        }
        Marker::FixArray(len) => {
            for _ in 0..len {
                skip_msgpack(cur)?;
            }
        }
        Marker::Array16 => {
            let len = cur.read_u16::<BigEndian>()?;
            for _ in 0..len {
                skip_msgpack(cur)?;
            }
        }
        Marker::Array32 => {
            let len = cur.read_u32::<BigEndian>()?;
            for _ in 0..len {
                skip_msgpack(cur)?;
            }
        }
        Marker::FixMap(len) => {
            let len = len * 2;
            for _ in 0..len {
                skip_msgpack(cur)?;
            }
        }
        Marker::Map16 => {
            let len = cur.read_u16::<BigEndian>()? * 2;
            for _ in 0..len {
                skip_msgpack(cur)?;
            }
        }
        Marker::Map32 => {
            let len = cur.read_u32::<BigEndian>()? * 2;
            for _ in 0..len {
                skip_msgpack(cur)?;
            }
        }
        Marker::FixExt1 => {
            cur.seek(SeekFrom::Current(2))?;
        }
        Marker::FixExt2 => {
            cur.seek(SeekFrom::Current(3))?;
        }
        Marker::FixExt4 => {
            cur.seek(SeekFrom::Current(5))?;
        }
        Marker::FixExt8 => {
            cur.seek(SeekFrom::Current(9))?;
        }
        Marker::FixExt16 => {
            cur.seek(SeekFrom::Current(17))?;
        }
        Marker::Ext8 => {
            let len = cur.read_u8()?;
            cur.seek(SeekFrom::Current(len as i64 + 1))?;
        }
        Marker::Ext16 => {
            let len = cur.read_u16::<BigEndian>()?;
            cur.seek(SeekFrom::Current(len as i64 + 1))?;
        }
        Marker::Ext32 => {
            let len = cur.read_u32::<BigEndian>()?;
            cur.seek(SeekFrom::Current(len as i64 + 1))?;
        }
        Marker::Reserved => {
            return Err(rmp::decode::ValueReadError::TypeMismatch(Marker::Reserved).into())
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct ResponseError {
    message: String,
}

impl Display for ResponseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.message)
    }
}
