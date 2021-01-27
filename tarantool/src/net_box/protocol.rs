use core::str::from_utf8;
use std::collections::VecDeque;
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

pub fn encode_auth(
    buf: &mut Cursor<Vec<u8>>,
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

    let header_offset = prepare_request(buf, sync, IProtoType::Auth)?;
    rmp::encode::write_map_len(buf, 2)?;

    // username:
    rmp::encode::write_pfix(buf, USER_NAME)?;
    rmp::encode::write_str(buf, user)?;

    // encrypted password:
    rmp::encode::write_pfix(buf, TUPLE)?;
    rmp::encode::write_array_len(buf, 2)?;
    rmp::encode::write_str(buf, "chap-sha1")?;
    rmp::encode::write_str_len(buf, 20)?;
    buf.write_all(&step_1_and_scramble)?;

    encode_request(buf, header_offset)?;
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
    rmp::encode::write_pfix(buf, FUNCTION_NAME)?;
    rmp::encode::write_str(buf, function_name)?;
    rmp::encode::write_pfix(buf, TUPLE)?;
    rmp_serde::encode::write(buf, args)?;
    encode_request(buf, header_offset)?;
    Ok(())
}

pub fn encode_eval<T>(
    buf: &mut Cursor<Vec<u8>>,
    sync: u64,
    expression: &str,
    args: &T,
) -> Result<(), Error>
where
    T: AsTuple,
{
    let header_offset = prepare_request(buf, sync, IProtoType::Eval)?;
    rmp::encode::write_map_len(buf, 2)?;
    rmp::encode::write_pfix(buf, EXPR)?;
    rmp::encode::write_str(buf, expression)?;
    rmp::encode::write_pfix(buf, TUPLE)?;
    rmp_serde::encode::write(buf, args)?;
    encode_request(buf, header_offset)?;
    Ok(())
}

pub fn encode_select<K>(
    buf: &mut Cursor<Vec<u8>>,
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
    let header_offset = prepare_request(buf, sync, IProtoType::Select)?;
    rmp::encode::write_map_len(buf, 6)?;
    rmp::encode::write_pfix(buf, SPACE_ID)?;
    rmp::encode::write_u32(buf, space_id)?;
    rmp::encode::write_pfix(buf, INDEX_ID)?;
    rmp::encode::write_u32(buf, index_id)?;
    rmp::encode::write_pfix(buf, LIMIT)?;
    rmp::encode::write_u32(buf, limit)?;
    rmp::encode::write_pfix(buf, OFFSET)?;
    rmp::encode::write_u32(buf, offset)?;
    rmp::encode::write_pfix(buf, ITERATOR)?;
    rmp::encode::write_u32(buf, iterator_type as u32)?;
    rmp::encode::write_pfix(buf, KEY)?;
    rmp_serde::encode::write(buf, key)?;
    encode_request(buf, header_offset)?;
    Ok(())
}

pub fn encode_insert<T>(
    buf: &mut Cursor<Vec<u8>>,
    sync: u64,
    space_id: u32,
    value: &T,
) -> Result<(), Error>
where
    T: AsTuple,
{
    let header_offset = prepare_request(buf, sync, IProtoType::Insert)?;
    rmp::encode::write_map_len(buf, 2)?;
    rmp::encode::write_pfix(buf, SPACE_ID)?;
    rmp::encode::write_u32(buf, space_id)?;
    rmp::encode::write_pfix(buf, TUPLE)?;
    rmp_serde::encode::write(buf, value)?;
    encode_request(buf, header_offset)?;
    Ok(())
}

pub fn encode_replace<T>(
    buf: &mut Cursor<Vec<u8>>,
    sync: u64,
    space_id: u32,
    value: &T,
) -> Result<(), Error>
where
    T: AsTuple,
{
    let header_offset = prepare_request(buf, sync, IProtoType::Replace)?;
    rmp::encode::write_map_len(buf, 2)?;
    rmp::encode::write_pfix(buf, SPACE_ID)?;
    rmp::encode::write_u32(buf, space_id)?;
    rmp::encode::write_pfix(buf, TUPLE)?;
    rmp_serde::encode::write(buf, value)?;
    encode_request(buf, header_offset)?;
    Ok(())
}

pub fn encode_update<K, Op>(
    buf: &mut Cursor<Vec<u8>>,
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
    let header_offset = prepare_request(buf, sync, IProtoType::Update)?;
    rmp::encode::write_map_len(buf, 4)?;
    rmp::encode::write_pfix(buf, SPACE_ID)?;
    rmp::encode::write_u32(buf, space_id)?;
    rmp::encode::write_pfix(buf, INDEX_ID)?;
    rmp::encode::write_u32(buf, index_id)?;
    rmp::encode::write_pfix(buf, KEY)?;
    rmp_serde::encode::write(buf, key)?;
    rmp::encode::write_pfix(buf, TUPLE)?;
    rmp_serde::encode::write(buf, ops)?;
    encode_request(buf, header_offset)?;
    Ok(())
}

pub fn encode_upsert<T, Op>(
    buf: &mut Cursor<Vec<u8>>,
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
    let header_offset = prepare_request(buf, sync, IProtoType::Upsert)?;
    rmp::encode::write_map_len(buf, 4)?;
    rmp::encode::write_pfix(buf, SPACE_ID)?;
    rmp::encode::write_u32(buf, space_id)?;
    rmp::encode::write_pfix(buf, INDEX_BASE)?;
    rmp::encode::write_u32(buf, index_id)?;
    rmp::encode::write_pfix(buf, OPS)?;
    rmp_serde::encode::write(buf, ops)?;
    rmp::encode::write_pfix(buf, TUPLE)?;
    rmp_serde::encode::write(buf, value)?;
    encode_request(buf, header_offset)?;
    Ok(())
}

pub fn encode_delete<K>(
    buf: &mut Cursor<Vec<u8>>,
    sync: u64,
    space_id: u32,
    index_id: u32,
    key: &K,
) -> Result<(), Error>
where
    K: AsTuple,
{
    let header_offset = prepare_request(buf, sync, IProtoType::Delete)?;
    rmp::encode::write_map_len(buf, 3)?;
    rmp::encode::write_pfix(buf, SPACE_ID)?;
    rmp::encode::write_u32(buf, space_id)?;
    rmp::encode::write_pfix(buf, INDEX_ID)?;
    rmp::encode::write_u32(buf, index_id)?;
    rmp::encode::write_pfix(buf, KEY)?;
    rmp_serde::encode::write(buf, key)?;
    encode_request(buf, header_offset)?;
    Ok(())
}

fn decode_error(cur: &mut Cursor<Vec<u8>>) -> Result<ResponseError, Error> {
    let mut message: Option<String> = None;

    let map_len = rmp::decode::read_map_len(cur)?;
    for _ in 0..map_len {
        if rmp::decode::read_pfix(cur)? == ERROR {
            let str_len = rmp::decode::read_str_len(cur)? as usize;
            let mut str_buf = vec![0u8; str_len];
            cur.read_exact(&mut str_buf)?;
            message = Some(from_utf8(&mut str_buf)?.to_string());
        }
    }

    Ok(ResponseError {
        message: message.ok_or(io::Error::from(io::ErrorKind::InvalidData))?,
    })
}

pub fn decode_greeting(stream: &mut dyn Read) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::with_capacity(128);
    buf.resize(128, 0);

    stream.read_exact(&mut *buf)?;
    let salt = base64::decode(&buf[64..108]).unwrap();
    Ok(salt)
}

pub fn decode_response<R: Read>(stream: &mut R) -> Result<Response, Error> {
    let response_len = rmp::decode::read_u32(stream)? as usize;
    let mut buf = Vec::with_capacity(response_len);
    buf.resize(response_len, 0);

    stream.read_exact(&mut *buf)?;
    let mut cur = Cursor::new(buf);

    let mut sync: Option<u64> = None;
    let mut schema_version: Option<u32> = None;
    let mut status_code: Option<u32> = None;

    let map_len = rmp::decode::read_map_len(&mut cur)?;
    for _ in 0..map_len {
        let key = rmp::decode::read_pfix(&mut cur)?;
        match key {
            0 => status_code = Some(rmp::decode::read_int(&mut cur)?),
            SYNC => sync = Some(rmp::decode::read_int(&mut cur)?),
            SCHEMA_VERSION => schema_version = Some(rmp::decode::read_int(&mut cur)?),
            _ => skip_msgpack(&mut cur)?,
        }
    }

    let status_code = status_code.ok_or(io::Error::from(io::ErrorKind::InvalidData))?;
    let sync = sync.ok_or(io::Error::from(io::ErrorKind::InvalidData))?;
    let schema_version = schema_version.ok_or(io::Error::from(io::ErrorKind::InvalidData))?;

    Ok(Response {
        status_code,
        sync,
        schema_version,
        payload_cur: cur,
    })
}

fn skip_msgpack(cur: &mut Cursor<Vec<u8>>) -> Result<(), Error> {
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

pub struct Response {
    pub sync: u64,
    pub schema_version: u32,
    status_code: u32,
    payload_cur: Cursor<Vec<u8>>,
}

impl Response {
    pub fn into_tuple(mut self) -> Result<Option<Tuple>, Error> {
        if self.status_code == 0 {
            let payload_len = rmp::decode::read_map_len(&mut self.payload_cur)?;
            for _ in 0..payload_len {
                let key = rmp::decode::read_pfix(&mut self.payload_cur)?;
                match key {
                    DATA => {
                        let payload_offset = self.payload_cur.position();
                        let buf = self.payload_cur.into_inner();
                        let payload_len = buf.len() as u64 - payload_offset;
                        unsafe {
                            return Ok(Some(Tuple::from_raw_data(
                                Box::leak(buf.into_boxed_slice())
                                    .as_mut_ptr()
                                    .add(payload_offset as usize)
                                    as *mut c_char,
                                payload_len as u32,
                            )));
                        }
                    }
                    _ => {
                        skip_msgpack(&mut self.payload_cur)?;
                    }
                };
            }
            Ok(None)
        } else {
            Err(decode_error(&mut self.payload_cur)?.into())
        }
    }

    pub fn into_iter(mut self) -> Result<Option<ResponseIterator>, Error> {
        if self.status_code == 0 {
            let payload_len = rmp::decode::read_map_len(&mut self.payload_cur)?;
            for _ in 0..payload_len {
                let key = rmp::decode::read_pfix(&mut self.payload_cur)?;
                match key {
                    DATA => {
                        let items_count = rmp::decode::read_array_len(&mut self.payload_cur)?;
                        let start_offset = self.payload_cur.position() as usize;
                        let mut offsets = VecDeque::with_capacity(items_count as usize);
                        for _ in 0..items_count {
                            skip_msgpack(&mut self.payload_cur)?;
                            offsets.push_back(self.payload_cur.position() as usize);
                        }

                        return Ok(Some(ResponseIterator {
                            current_offset: start_offset,
                            rest_offsets: offsets,
                            buf_ptr: Box::leak(self.payload_cur.into_inner().into_boxed_slice())
                                .as_mut_ptr(),
                        }));
                    }
                    _ => {
                        skip_msgpack(&mut self.payload_cur)?;
                    }
                };
            }
            Ok(None)
        } else {
            Err(decode_error(&mut self.payload_cur)?.into())
        }
    }
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

pub struct ResponseIterator {
    current_offset: usize,
    rest_offsets: VecDeque<usize>,
    buf_ptr: *mut u8,
}

impl ResponseIterator {
    pub fn next_tuple(&mut self) -> Option<Tuple> {
        match self.rest_offsets.pop_front() {
            None => None,
            Some(next_offset) => {
                let current_offset = self.current_offset;
                self.current_offset = next_offset;

                Some(unsafe {
                    Tuple::from_raw_data(
                        self.buf_ptr.clone().add(current_offset) as *mut c_char,
                        (next_offset - current_offset) as u32,
                    )
                })
            }
        }
    }
}
