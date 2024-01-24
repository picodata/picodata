use std::cmp::min;
use std::io::{self, Cursor, Read, Seek, Write};
use std::os::raw::c_char;
use std::str::from_utf8;

use sha1::{Digest, Sha1};

use super::Error;
use crate::auth::AuthMethod;
use crate::index::IteratorType;
use crate::msgpack;
use crate::tuple::{ToTupleBuffer, Tuple};

use super::{ResponseError, SyncIndex};

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

const SQL_TEXT: u8 = 0x40;
const SQL_BIND: u8 = 0x41;

pub enum IProtoType {
    Select = 1,
    Insert = 2,
    Replace = 3,
    Update = 4,
    Delete = 5,
    Auth = 7,
    Eval = 8,
    Upsert = 9,
    Call = 10,
    Execute = 11,
    Ping = 64,
}

pub fn encode_header(
    stream: &mut impl Write,
    sync: SyncIndex,
    request_type: IProtoType,
) -> Result<(), Error> {
    rmp::encode::write_map_len(stream, 2)?;
    rmp::encode::write_pfix(stream, REQUEST_TYPE)?;
    rmp::encode::write_pfix(stream, request_type as u8)?;
    rmp::encode::write_pfix(stream, SYNC)?;
    rmp::encode::write_uint(stream, sync.0)?;
    Ok(())
}

pub fn chap_sha1_auth_data(password: &str, salt: &[u8]) -> Vec<u8> {
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

    let scramble_bytes = &step_1_and_scramble.as_slice();
    debug_assert_eq!(scramble_bytes.len(), 20);

    // 5 is the maximum possible MP_STR header size
    let mut res = Vec::with_capacity(scramble_bytes.len() + 5);
    rmp::encode::write_str_len(&mut res, scramble_bytes.len() as _).expect("Can't fail for a Vec");
    res.write_all(scramble_bytes).expect("Can't fail for a Vec");
    return res;
}

pub fn encode_auth(
    stream: &mut impl Write,
    user: &str,
    password: &str,
    salt: &[u8],
    auth_method: AuthMethod,
) -> Result<(), Error> {
    let auth_data;
    match auth_method {
        AuthMethod::ChapSha1 => {
            auth_data = chap_sha1_auth_data(password, salt);
        }
        #[cfg(feature = "picodata")]
        _ => {
            return Err(Error::Tarantool(Box::new(crate::error::Error::other(
                format!("auth method '{auth_method}' is not implemented yet"),
            ))));
        }
    }

    rmp::encode::write_map_len(stream, 2)?;

    // username:
    rmp::encode::write_pfix(stream, USER_NAME)?;
    rmp::encode::write_str(stream, user)?;

    // encrypted password:
    rmp::encode::write_pfix(stream, TUPLE)?;
    rmp::encode::write_array_len(stream, 2)?;
    rmp::encode::write_str(stream, auth_method.as_str())?;
    stream.write_all(&auth_data)?;
    Ok(())
}

pub fn encode_ping(stream: &mut impl Write) -> Result<(), Error> {
    rmp::encode::write_map_len(stream, 0)?;
    Ok(())
}

pub fn encode_execute<P>(stream: &mut impl Write, sql: &str, bind_params: &P) -> Result<(), Error>
where
    P: ToTupleBuffer + ?Sized,
{
    rmp::encode::write_map_len(stream, 2)?;
    rmp::encode::write_pfix(stream, SQL_TEXT)?;
    rmp::encode::write_str(stream, sql)?;

    rmp::encode::write_pfix(stream, SQL_BIND)?;
    bind_params.write_tuple_data(stream)?;
    Ok(())
}

pub fn encode_call<T>(stream: &mut impl Write, function_name: &str, args: &T) -> Result<(), Error>
where
    T: ToTupleBuffer + ?Sized,
{
    rmp::encode::write_map_len(stream, 2)?;
    rmp::encode::write_pfix(stream, FUNCTION_NAME)?;
    rmp::encode::write_str(stream, function_name)?;
    rmp::encode::write_pfix(stream, TUPLE)?;
    args.write_tuple_data(stream)?;
    Ok(())
}

pub fn encode_eval<T>(stream: &mut impl Write, expression: &str, args: &T) -> Result<(), Error>
where
    T: ToTupleBuffer + ?Sized,
{
    rmp::encode::write_map_len(stream, 2)?;
    rmp::encode::write_pfix(stream, EXPR)?;
    rmp::encode::write_str(stream, expression)?;
    rmp::encode::write_pfix(stream, TUPLE)?;
    args.write_tuple_data(stream)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn encode_select<K>(
    stream: &mut impl Write,
    space_id: u32,
    index_id: u32,
    limit: u32,
    offset: u32,
    iterator_type: IteratorType,
    key: &K,
) -> Result<(), Error>
where
    K: ToTupleBuffer + ?Sized,
{
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
    key.write_tuple_data(stream)?;
    Ok(())
}

pub fn encode_insert<T>(stream: &mut impl Write, space_id: u32, value: &T) -> Result<(), Error>
where
    T: ToTupleBuffer + ?Sized,
{
    rmp::encode::write_map_len(stream, 2)?;
    rmp::encode::write_pfix(stream, SPACE_ID)?;
    rmp::encode::write_u32(stream, space_id)?;
    rmp::encode::write_pfix(stream, TUPLE)?;
    value.write_tuple_data(stream)?;
    Ok(())
}

pub fn encode_replace<T>(stream: &mut impl Write, space_id: u32, value: &T) -> Result<(), Error>
where
    T: ToTupleBuffer + ?Sized,
{
    rmp::encode::write_map_len(stream, 2)?;
    rmp::encode::write_pfix(stream, SPACE_ID)?;
    rmp::encode::write_u32(stream, space_id)?;
    rmp::encode::write_pfix(stream, TUPLE)?;
    value.write_tuple_data(stream)?;
    Ok(())
}

pub fn encode_update<K, Op>(
    stream: &mut impl Write,
    space_id: u32,
    index_id: u32,
    key: &K,
    ops: &Op,
) -> Result<(), Error>
where
    K: ToTupleBuffer + ?Sized,
    Op: ToTupleBuffer + ?Sized,
{
    rmp::encode::write_map_len(stream, 4)?;
    rmp::encode::write_pfix(stream, SPACE_ID)?;
    rmp::encode::write_u32(stream, space_id)?;
    rmp::encode::write_pfix(stream, INDEX_ID)?;
    rmp::encode::write_u32(stream, index_id)?;
    rmp::encode::write_pfix(stream, KEY)?;
    key.write_tuple_data(stream)?;
    rmp::encode::write_pfix(stream, TUPLE)?;
    ops.write_tuple_data(stream)?;
    Ok(())
}

pub fn encode_upsert<T, Op>(
    stream: &mut impl Write,
    space_id: u32,
    index_id: u32,
    value: &T,
    ops: &Op,
) -> Result<(), Error>
where
    T: ToTupleBuffer + ?Sized,
    Op: ToTupleBuffer + ?Sized,
{
    rmp::encode::write_map_len(stream, 4)?;
    rmp::encode::write_pfix(stream, SPACE_ID)?;
    rmp::encode::write_u32(stream, space_id)?;
    rmp::encode::write_pfix(stream, INDEX_BASE)?;
    rmp::encode::write_u32(stream, index_id)?;
    rmp::encode::write_pfix(stream, OPS)?;
    ops.write_tuple_data(stream)?;
    rmp::encode::write_pfix(stream, TUPLE)?;
    value.write_tuple_data(stream)?;
    Ok(())
}

pub fn encode_delete<K>(
    stream: &mut impl Write,
    space_id: u32,
    index_id: u32,
    key: &K,
) -> Result<(), Error>
where
    K: ToTupleBuffer + ?Sized,
{
    rmp::encode::write_map_len(stream, 3)?;
    rmp::encode::write_pfix(stream, SPACE_ID)?;
    rmp::encode::write_u32(stream, space_id)?;
    rmp::encode::write_pfix(stream, INDEX_ID)?;
    rmp::encode::write_u32(stream, index_id)?;
    rmp::encode::write_pfix(stream, KEY)?;
    key.write_tuple_data(stream)?;
    Ok(())
}

#[derive(Debug)]
pub struct Header {
    pub sync: SyncIndex,
    pub status_code: u32,
    pub schema_version: u64,
}

pub struct Response<T> {
    pub header: Header,
    pub payload: T,
}

pub fn decode_header(stream: &mut (impl Read + Seek)) -> Result<Header, Error> {
    let mut sync: Option<u64> = None;
    let mut status_code: Option<u32> = None;
    let mut schema_version: Option<u64> = None;

    let map_len = rmp::decode::read_map_len(stream)?;
    for _ in 0..map_len {
        let key = rmp::decode::read_pfix(stream)?;
        match key {
            0 => status_code = Some(rmp::decode::read_int(stream)?),
            SYNC => sync = Some(rmp::decode::read_int(stream)?),
            SCHEMA_VERSION => schema_version = Some(rmp::decode::read_int(stream)?),
            _ => msgpack::skip_value(stream)?,
        }
    }

    if sync.is_none() || status_code.is_none() || schema_version.is_none() {
        return Err(io::Error::from(io::ErrorKind::InvalidData).into());
    }

    Ok(Header {
        sync: SyncIndex(sync.unwrap()),
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
            message = Some(from_utf8(&str_buf)?.to_string());
        }
    }

    Ok(ResponseError {
        message: message.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?,
    })
}

pub fn decode_greeting(stream: &mut impl Read) -> Result<Vec<u8>, Error> {
    let mut buf = [0; 128];
    stream.read_exact(&mut buf)?;
    let salt = base64::decode(&buf[64..108]).unwrap();
    Ok(salt)
}

pub fn decode_call(buffer: &mut Cursor<Vec<u8>>) -> Result<Tuple, Error> {
    let payload_len = rmp::decode::read_map_len(buffer)?;
    for _ in 0..payload_len {
        let key = rmp::decode::read_pfix(buffer)?;
        match key {
            DATA => {
                return decode_tuple(buffer);
            }
            _ => {
                msgpack::skip_value(buffer)?;
            }
        };
    }
    Err(Error::ResponseDataNotFound)
}

pub fn decode_multiple_rows(
    buffer: &mut Cursor<Vec<u8>>,
    limit: Option<usize>,
) -> Result<Vec<Tuple>, Error> {
    let payload_len = rmp::decode::read_map_len(buffer)?;
    for _ in 0..payload_len {
        let key = rmp::decode::read_pfix(buffer)?;
        match key {
            DATA => {
                let items_count = rmp::decode::read_array_len(buffer)? as usize;
                let items_count = match limit {
                    None => items_count,
                    Some(limit) => min(limit, items_count),
                };

                let mut result = Vec::with_capacity(items_count);
                for _ in 0..items_count {
                    result.push(decode_tuple(buffer)?);
                }
                return Ok(result);
            }
            _ => {
                msgpack::skip_value(buffer)?;
            }
        };
    }
    Ok(vec![])
}

pub fn decode_single_row(buffer: &mut Cursor<Vec<u8>>) -> Result<Option<Tuple>, Error> {
    let payload_len = rmp::decode::read_map_len(buffer)?;
    for _ in 0..payload_len {
        let key = rmp::decode::read_pfix(buffer)?;
        match key {
            DATA => {
                let items_count = rmp::decode::read_array_len(buffer)? as usize;
                return Ok(if items_count == 0 {
                    None
                } else {
                    Some(decode_tuple(buffer)?)
                });
            }
            _ => {
                msgpack::skip_value(buffer)?;
            }
        }
    }
    Ok(None)
}

pub fn decode_tuple(buffer: &mut Cursor<Vec<u8>>) -> Result<Tuple, Error> {
    let payload_offset = buffer.position();
    msgpack::skip_value(buffer)?;
    let payload_len = buffer.position() - payload_offset;
    let buf = buffer.get_mut();
    unsafe {
        Ok(Tuple::from_raw_data(
            buf.as_slice().as_ptr().add(payload_offset as usize) as *mut c_char,
            payload_len as u32,
        ))
    }
}

pub fn value_slice(cursor: &mut Cursor<impl AsRef<[u8]>>) -> crate::Result<&[u8]> {
    let start = cursor.position() as usize;
    msgpack::skip_value(cursor)?;
    Ok(&cursor.get_ref().as_ref()[start..(cursor.position() as usize)])
}
