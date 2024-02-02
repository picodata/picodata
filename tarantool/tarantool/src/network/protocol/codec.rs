use std::io::{self, Cursor, Read, Seek, Write};
use std::os::raw::c_char;

use sha1::{Digest, Sha1};

use crate::auth::AuthMethod;
use crate::error::Error;
use crate::error::TarantoolError;
use crate::index::IteratorType;
use crate::msgpack;
use crate::network::protocol::ProtocolError;
use crate::tuple::{ToTupleBuffer, Tuple};

use super::SyncIndex;

/// Keys of the HEADER and BODY maps in the iproto packets.
///
/// See `enum iproto_key` in \<tarantool>/src/box/iproto_constants.h for source
/// of truth.
pub(crate) mod iproto_key {
    pub const REQUEST_TYPE: u8 = 0x00;
    pub const SYNC: u8 = 0x01;
    // ...
    pub const SCHEMA_VERSION: u8 = 0x05;
    // ...
    pub const SPACE_ID: u8 = 0x10;
    pub const INDEX_ID: u8 = 0x11;
    pub const LIMIT: u8 = 0x12;
    pub const OFFSET: u8 = 0x13;
    pub const ITERATOR: u8 = 0x14;
    pub const INDEX_BASE: u8 = 0x15;
    // ...
    pub const KEY: u8 = 0x20;
    pub const TUPLE: u8 = 0x21;
    pub const FUNCTION_NAME: u8 = 0x22;
    pub const USER_NAME: u8 = 0x23;
    // ...
    pub const EXPR: u8 = 0x27;
    pub const OPS: u8 = 0x28;
    // ...
    pub const DATA: u8 = 0x30;
    pub const ERROR: u8 = 0x31;
    // ...
    pub const SQL_TEXT: u8 = 0x40;
    pub const SQL_BIND: u8 = 0x41;
    // ...
    pub const ERROR_EXT: u8 = 0x52;
    // ...
}
use iproto_key::*;

/// Iproto packet type.
///
/// See `enum iproto_type` in \<tarantool>/src/box/iproto_constants.h for source
/// of truth.
#[derive(Debug)]
#[non_exhaustive]
pub enum IProtoType {
    /// This packet is a response with status success.
    Ok = 0,
    Select = 1,
    Insert = 2,
    Replace = 3,
    Update = 4,
    Delete = 5,
    // LegacyCall = 6,
    Auth = 7,
    Eval = 8,
    Upsert = 9,
    Call = 10,
    Execute = 11,
    // ...
    Ping = 64,
    // ...
    /// Error marker. This value will be combined with the error code in the
    /// actual iproto response: `(IProtoType::Error | error_code)`.
    Error = 1 << 15,
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

#[cfg(feature = "picodata")]
#[inline]
pub fn ldap_auth_data(password: &str) -> Vec<u8> {
    // 5 is the maximum possible MP_STR header size
    let mut res = Vec::with_capacity(password.len() + 5);
    // Hopefully you're using an ssh tunnel or something ¯\_(ツ)_/¯
    rmp::encode::write_str(&mut res, password).expect("Can't fail for a Vec");
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
        AuthMethod::Ldap => {
            auth_data = ldap_auth_data(password);
        }
        #[cfg(feature = "picodata")]
        _ => {
            return Err(
                ProtocolError::Unimplemented(format!("auth method '{auth_method}'")).into(),
            );
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
    /// Type of the iproto packet.
    ///
    /// If the packet is an error response (see [`IProtoType::Error`]) then the
    /// error code is removed from it and assigned to [`Header::error_code`].
    ///
    /// This should be a value from `enum iproto_type` from tarantool sources,
    /// but it's practically impossible to keep our `IProtoType` up to date with
    /// the latest version of tarantool, so we just store it as a plain integer.
    pub iproto_type: u32,
    pub error_code: u32,
    pub schema_version: u64,
}

pub struct Response<T> {
    pub header: Header,
    pub payload: T,
}

pub fn decode_header(stream: &mut (impl Read + Seek)) -> Result<Header, Error> {
    let mut sync: Option<u64> = None;
    let mut iproto_type: Option<u32> = None;
    let mut error_code: u32 = 0;
    let mut schema_version: Option<u64> = None;

    let map_len = rmp::decode::read_map_len(stream)?;
    for _ in 0..map_len {
        let key = rmp::decode::read_pfix(stream)?;
        match key {
            REQUEST_TYPE => {
                let r#type: u32 = rmp::decode::read_int(stream)?;

                const IPROTO_TYPE_ERROR: u32 = IProtoType::Error as _;
                if (r#type & IPROTO_TYPE_ERROR) != 0 {
                    iproto_type = Some(IPROTO_TYPE_ERROR);
                    error_code = r#type & !IPROTO_TYPE_ERROR;
                } else {
                    iproto_type = Some(r#type);
                }
            }
            SYNC => sync = Some(rmp::decode::read_int(stream)?),
            SCHEMA_VERSION => schema_version = Some(rmp::decode::read_int(stream)?),
            _ => msgpack::skip_value(stream)?,
        }
    }

    if sync.is_none() || iproto_type.is_none() || schema_version.is_none() {
        return Err(io::Error::from(io::ErrorKind::InvalidData).into());
    }

    Ok(Header {
        sync: SyncIndex(sync.unwrap()),
        iproto_type: iproto_type.unwrap(),
        error_code,
        schema_version: schema_version.unwrap(),
    })
}

////////////////////////////////////////////////////////////////////////////////
// error decoding
////////////////////////////////////////////////////////////////////////////////

/// Constant definitions for keys of the extended error info. Currently there's
/// only one possible key - error stack, and the value associated with it is an
/// array of error info maps. These error info maps have fields from the
/// [`error_field`] module defined below.
///
/// See enum MP_ERROR_* \<tarantool>/src/box/mp_error.cc
mod extended_error_keys {
    /// Stack of error infos.
    pub const STACK: u8 = 0;
}

/// Constant definitions for extended error info fields.
///
/// See enum MP_ERROR_* \<tarantool>/src/box/mp_error.cc
mod error_field {
    /// Error type.
    pub const TYPE: u8 = 0x00;

    /// File name from trace.
    pub const FILE: u8 = 0x01;

    /// Line from trace.
    pub const LINE: u8 = 0x02;

    /// Error message.
    pub const MESSAGE: u8 = 0x03;

    /// Errno at the moment of error creation.
    pub const ERRNO: u8 = 0x04;

    /// Error code.
    pub const CODE: u8 = 0x05;

    /// Type-specific fields stored as a map
    /// {string key = value}.
    pub const FIELDS: u8 = 0x06;
}

/// Reads a IPROTO packet from the `stream` (i.e. a msgpack map with integer keys)
pub fn decode_error(stream: &mut impl Read, header: &Header) -> Result<TarantoolError, Error> {
    let mut error = TarantoolError::default();

    let map_len = rmp::decode::read_map_len(stream)?;
    for _ in 0..map_len {
        let key = rmp::decode::read_pfix(stream)?;
        match key {
            ERROR => {
                let message = decode_string(stream)?;
                error.message = Some(message.into());
                error.code = header.error_code;
            }
            ERROR_EXT => {
                if let Some(e) = decode_extended_error(stream)? {
                    error = e;
                } else {
                    crate::say_verbose!("empty ERROR_EXT field");
                }
            }
            _ => {
                crate::say_verbose!("unhandled iproto key {key} when decoding error");
            }
        }
    }

    if error.message.is_none() {
        return Err(ProtocolError::ResponseFieldNotFound {
            key: "ERROR",
            context: "required for error responses",
        }
        .into());
    }

    Ok(error)
}

pub fn decode_extended_error(stream: &mut impl Read) -> Result<Option<TarantoolError>, Error> {
    let extended_error_n_fields = rmp::decode::read_map_len(stream)? as usize;
    if extended_error_n_fields == 0 {
        return Ok(None);
    }

    let mut error_info = None;

    for _ in 0..extended_error_n_fields {
        let key = rmp::decode::read_pfix(stream)?;
        match key {
            extended_error_keys::STACK => {
                if error_info.is_some() {
                    crate::say_verbose!("duplicate error stack in response");
                }

                let error_stack_len = rmp::decode::read_array_len(stream)? as usize;
                if error_stack_len == 0 {
                    continue;
                }

                let mut stack_nodes = Vec::with_capacity(error_stack_len);
                for _ in 0..error_stack_len {
                    stack_nodes.push(decode_error_stack_node(stream)?);
                }

                for mut node in stack_nodes.into_iter().rev() {
                    if let Some(next_node) = error_info {
                        node.cause = Some(Box::new(next_node));
                    }
                    error_info = Some(node);
                }
            }
            _ => {
                crate::say_verbose!("unknown extended error key {key}");
            }
        }
    }

    Ok(error_info)
}

pub fn decode_error_stack_node(mut stream: &mut impl Read) -> Result<TarantoolError, Error> {
    let mut res = TarantoolError::default();

    let map_len = rmp::decode::read_map_len(stream)? as usize;
    for _ in 0..map_len {
        let key = rmp::decode::read_pfix(stream)?;
        match key {
            error_field::TYPE => {
                res.error_type = Some(decode_string(stream)?.into_boxed_str());
            }
            error_field::FILE => {
                res.file = Some(decode_string(stream)?.into_boxed_str());
            }
            error_field::LINE => {
                res.line = Some(rmp::decode::read_int(stream)?);
            }
            error_field::MESSAGE => {
                res.message = Some(decode_string(stream)?.into_boxed_str());
            }
            error_field::ERRNO => {
                let n = rmp::decode::read_int(stream)?;
                if n != 0 {
                    res.errno = Some(n);
                }
            }
            error_field::CODE => {
                res.code = rmp::decode::read_int(stream)?;
            }
            error_field::FIELDS => match rmp_serde::from_read(&mut stream) {
                Ok(f) => {
                    res.fields = f;
                }
                Err(e) => {
                    crate::say_verbose!("failed decoding error fields: {e}");
                }
            },
            _ => {
                crate::say_verbose!("unexpected error field {key}");
            }
        }
    }

    Ok(res)
}

////////////////////////////////////////////////////////////////////////////////
// ...
////////////////////////////////////////////////////////////////////////////////

pub fn decode_string(stream: &mut impl Read) -> Result<String, Error> {
    let len = rmp::decode::read_str_len(stream)? as usize;
    let mut str_buf = vec![0u8; len];
    stream.read_exact(&mut str_buf)?;
    let res = String::from_utf8(str_buf)?;
    Ok(res)
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
    Err(ProtocolError::ResponseFieldNotFound {
        key: "DATA",
        context: "required for CALL/EVAL responses",
    }
    .into())
}

pub fn decode_multiple_rows(buffer: &mut Cursor<Vec<u8>>) -> Result<Vec<Tuple>, Error> {
    let payload_len = rmp::decode::read_map_len(buffer)?;
    for _ in 0..payload_len {
        let key = rmp::decode::read_pfix(buffer)?;
        match key {
            DATA => {
                let items_count = rmp::decode::read_array_len(buffer)? as usize;
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
