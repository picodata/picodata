use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::{self, Cursor, Read, Write};
use std::os::raw::c_char;

use sha1::{Digest, Sha1};

use crate::error::Error;
use crate::tuple::{AsTuple, Tuple};

const REQUEST_TYPE: u8 = 0x00;
const SYNC: u8 = 0x01;
const TUPLE: u8 = 0x21;
const FUNCTION_NAME: u8 = 0x22;
const USER_NAME: u8 = 0x23;

const DATA: u8 = 0x30;
const ERROR: u8 = 0x31;

enum IProtoType {
    Auth = 7,
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
    rmp::encode::write_pfix(buf, FUNCTION_NAME as u8)?;
    rmp::encode::write_str(buf, function_name)?;
    rmp::encode::write_pfix(buf, TUPLE as u8)?;
    rmp_serde::encode::write(buf, args)?;
    encode_request(buf, header_offset)?;
    Ok(())
}

fn decode_map(cur: &mut Cursor<Vec<u8>>) -> Result<HashMap<u8, rmpv::Value>, Error> {
    let mut map = HashMap::new();
    let map_len = rmp::decode::read_map_len(cur)?;
    for _ in 0..map_len {
        let key = rmp::decode::read_pfix(cur)?;
        let value = rmpv::decode::read_value(cur)?;
        map.insert(key, value);
    }
    Ok(map)
}

fn decode_error(cur: &mut Cursor<Vec<u8>>) -> Result<ResponseError, Error> {
    let mut err_data = decode_map(cur)?;
    Ok(ResponseError {
        message: err_data
            .remove(&ERROR)
            .ok_or(io::Error::from(io::ErrorKind::InvalidData))?
            .to_string(),
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

    // decode header
    let header = decode_map(&mut cur)?;
    let status_code = header
        .get(&0)
        .map_or(None, |val| val.as_i64())
        .ok_or(io::Error::from(io::ErrorKind::InvalidData))? as u32;

    let sync = header
        .get(&SYNC)
        .map_or(None, |val| val.as_i64())
        .ok_or(io::Error::from(io::ErrorKind::InvalidData))? as u32;

    Ok(Response {
        status_code,
        sync,
        payload_cur: cur,
    })
}

pub struct Response {
    pub sync: u32,
    status_code: u32,
    payload_cur: Cursor<Vec<u8>>,
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
                        rmpv::decode::read_value(&mut self.payload_cur).unwrap();
                    }
                };
            }
            Ok(None)
        } else {
            Err(decode_error(&mut self.payload_cur)?.into())
        }
    }
}
