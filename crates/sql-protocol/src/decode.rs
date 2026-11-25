use crate::dml::dml_type::DMLType;
use crate::error::ProtocolError;
use crate::message_type::MessageType;
use crate::msgpack::{shift_pos, skip_value};
use rmp::decode::{
    read_array_len, read_bool, read_f32, read_f64, read_int, read_map_len, read_pfix, read_str_len,
    RmpRead,
};
use rmp::Marker;
use std::fmt::Display;
use std::io::{Cursor, Error as IoError, Result as IoResult};
use std::str::from_utf8;
use std::time::Duration;

#[derive(Default)]
pub struct QueryMetaArgs<'bytes> {
    pub timeout: f64,
    pub request_id: &'bytes str,
    pub plan_id: u64,
}

pub struct ExecuteArgs<'bytes> {
    pub timeout: Duration,
    pub need_ref: bool,
    pub rid: i64,
    pub sid: &'bytes str,
    pub data: &'bytes [u8],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolMessageType {
    Dql,
    Dml(DMLType),
    LocalDml(DMLType),
    Block,
}

/// A decoded protocol message.
/// Consists of common data that identifies the request and message type.
/// `payload` is the rest of the message; the caller decodes it with the
/// iterator of `msg_type`.
pub struct ProtocolMessage<'bytes> {
    pub request_id: &'bytes str,
    pub msg_type: ProtocolMessageType,
    pub payload: &'bytes [u8],
}

impl<'bytes> ProtocolMessage<'bytes> {
    pub fn decode_from_bytes(mp: &'bytes [u8]) -> Result<Self, ProtocolError> {
        let mut stream = Cursor::new(mp);
        let len = read_array_len(&mut stream)?;
        if len != 3 {
            return Err(ProtocolError::DecodeError(
                "protocol message must have 3 elements".to_string(),
            ));
        }
        let len = read_str_len(&mut stream)?;
        let start = stream.position() as usize;
        let end = start + len as usize;
        let request_id = from_utf8(&mp[start..end])
            .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;

        stream.set_position(end as u64);

        let msg_type: MessageType = read_pfix(&mut stream)?
            .try_into()
            .map_err(ProtocolError::DecodeError)?;

        let msg_type = match msg_type {
            MessageType::Block => ProtocolMessageType::Block,
            MessageType::DQL => ProtocolMessageType::Dql,
            MessageType::DML | MessageType::LocalDML => {
                let len = read_array_len(&mut stream)?;
                if len != 2 {
                    return Err(ProtocolError::DecodeError(
                        "protocol dml header must have 2 elements".to_string(),
                    ));
                }

                let dml_type = read_pfix(&mut stream)?
                    .try_into()
                    .map_err(ProtocolError::DecodeError)?;

                match msg_type {
                    MessageType::DML => ProtocolMessageType::Dml(dml_type),
                    MessageType::LocalDML => ProtocolMessageType::LocalDml(dml_type),
                    _ => unreachable!(),
                }
            }
        };

        let pos = stream.position() as usize;

        Ok(Self {
            request_id,
            msg_type,
            payload: &mp[pos..],
        })
    }
}

fn decode_error<E: Display>(what: &'static str) -> impl FnOnce(E) -> IoError {
    move |e| IoError::other(format!("Failed to decode {what}: {e}"))
}

/// Reads a vshard timeout. Lua sends it as an integer or as a float.
fn read_timeout(stream: &mut Cursor<&[u8]>) -> IoResult<f64> {
    let marker = stream.get_ref().get(stream.position() as usize);
    match marker.map(|byte| Marker::from_u8(*byte)) {
        Some(Marker::F64) => read_f64(stream).map_err(IoError::other),
        Some(Marker::F32) => read_f32(stream).map(f64::from).map_err(IoError::other),
        _ => read_int(stream).map_err(IoError::other),
    }
}

/// Reads a string that borrows from the input.
fn read_str<'bytes>(stream: &mut Cursor<&'bytes [u8]>) -> IoResult<&'bytes str> {
    let len = read_str_len(stream).map_err(IoError::other)?;
    let mp: &'bytes [u8] = stream.get_ref();
    let start = stream.position() as usize;
    shift_pos(stream, len as u64)?;
    from_utf8(&mp[start..stream.position() as usize]).map_err(IoError::other)
}

pub fn execute_args_split(mp: &[u8]) -> IoResult<ExecuteArgs<'_>> {
    let mut stream = Cursor::new(mp);
    let elems = read_array_len(&mut stream).map_err(decode_error("arguments array length"))?;
    if elems != 5 {
        return Err(IoError::other(format!(
            "Expected an array of 5 elements, got: {elems}"
        )));
    }
    let timeout = read_timeout(&mut stream)
        .and_then(|secs| Duration::try_from_secs_f64(secs).map_err(IoError::other))
        .map_err(decode_error("vshard storage timeout"))?;
    let rid = read_int(&mut stream).map_err(decode_error("vshard storage reference ID"))?;
    let sid = read_str(&mut stream).map_err(decode_error("vshard storage session ID"))?;
    let need_ref = read_bool(&mut stream).map_err(decode_error("need_ref flag"))?;
    // `ProtocolMessage::decode_from_bytes` checks the message.
    let data = &mp[stream.position() as usize..];

    Ok(ExecuteArgs {
        timeout,
        need_ref,
        rid,
        sid,
        data,
    })
}

pub fn query_meta_args_split(mp: &[u8]) -> IoResult<QueryMetaArgs<'_>> {
    let mut stream = Cursor::new(mp);
    let elems = read_array_len(&mut stream).map_err(decode_error("arguments array length"))?;
    if elems != 3 {
        return Err(IoError::other(format!(
            "Expected an array of 3 elements, got: {elems}"
        )));
    }
    let timeout = read_timeout(&mut stream).map_err(decode_error("vshard storage timeout"))?;
    let request_id = read_str(&mut stream).map_err(decode_error("request_id"))?;
    let plan_id = read_int(&mut stream).map_err(decode_error("plan_id"))?;

    Ok(QueryMetaArgs {
        timeout,
        request_id,
        plan_id,
    })
}

/// Decode a proc_sql_execute response.
pub fn execute_read_response(stream: &[u8]) -> IoResult<SqlExecute<'_>> {
    let mut stream = Cursor::new(stream);
    let map_len = read_map_len(&mut stream).map_err(IoError::other)?;
    if map_len != 1 {
        return Err(IoError::other(format!(
            "Expected a map of 1 element, got: {map_len}"
        )));
    }

    // Decode required key.
    let key_len = read_str_len(&mut stream).map_err(IoError::other)? as usize;
    let mut data = [0; 4];
    stream.read_exact_buf(&mut data[..key_len])?;
    let response_type = from_utf8(&data[..key_len])
        .map_err(|e| IoError::other(format!("Invalid UTF-8 in response key: {e}")))?;
    match response_type {
        "dql" => {
            let _ = read_array_len(&mut stream)
                .map_err(|e| IoError::other(format!("Failed to decode tuples: {e}")))?;
            Ok(SqlExecute::Dql(TupleIter { msgpack: stream }))
        }
        "miss" => Ok(SqlExecute::Miss),
        "dml" => {
            let row_cnt = read_int(&mut stream)
                .map_err(|e| IoError::other(format!("Failed to decode changed rows: {e}")))?;
            Ok(SqlExecute::Dml(row_cnt))
        }
        _ => Err(IoError::other(
            "Expected response key to be 'dql', 'dml', or 'miss'",
        )),
    }
}

pub enum SqlExecute<'a> {
    Dml(u64),
    Dql(TupleIter<'a>),
    Miss,
}

pub struct TupleIter<'bytes> {
    pub msgpack: Cursor<&'bytes [u8]>,
}

impl<'a> Iterator for TupleIter<'a> {
    type Item = IoResult<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        let initial_pos = self.msgpack.position() as usize;
        if self.msgpack.get_ref().len() == initial_pos {
            return None;
        }

        let tuple_len = match read_array_len(&mut self.msgpack) {
            Ok(len) => len as usize,
            Err(e) => return Some(Err(IoError::other(e))),
        };
        for _ in 0..tuple_len {
            if let Err(e) = skip_value(&mut self.msgpack) {
                return Some(Err(IoError::other(e)));
            }
        }

        let current_pos = self.msgpack.position() as usize;
        let output = &self.msgpack.get_ref()[initial_pos..current_pos];
        Some(Ok(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_read_response_dql() {
        // { "dql": [ [1,"one"], [2,"two"] ] }
        let buf = b"\x81\xa3dql\x92\x92\x01\xa3one\x92\x02\xa3two";
        let response = execute_read_response(buf).unwrap();
        let SqlExecute::Dql(mut iter) = response else {
            panic!("Expected DQL response");
        };
        let slice1 = iter.next().unwrap().unwrap();
        assert_eq!(slice1, b"\x92\x01\xa3one");
        let slice2 = iter.next().unwrap().unwrap();
        assert_eq!(slice2, b"\x92\x02\xa3two");
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_execute_read_response_dml() {
        // { "dml": 567 }
        let buf = b"\x81\xa3dml\xcd\x02\x37";
        let response = execute_read_response(buf).unwrap();
        let SqlExecute::Dml(row_count) = response else {
            panic!("Expected DML response");
        };
        assert_eq!(row_count, 567);
    }

    #[test]
    fn test_execute_read_response_miss() {
        // { "miss": nil }
        let buf = b"\x81\xa4miss\x00";
        let response = execute_read_response(buf).unwrap();
        let SqlExecute::Miss = response else {
            panic!("Expected Miss response");
        };
    }

    #[test]
    fn test_execute_read_response_dml_encoded() {
        use crate::encode::execute_write_dml_response;

        let mut buf = Vec::new();
        execute_write_dml_response(&mut buf, 1234).unwrap();
        let response = execute_read_response(&buf).unwrap();
        let SqlExecute::Dml(row_count) = response else {
            panic!("Expected DML response");
        };
        assert_eq!(row_count, 1234);
    }

    #[test]
    fn test_execute_read_response_miss_encoded() {
        use crate::encode::execute_write_miss_response;

        let mut buf = Vec::new();
        execute_write_miss_response(&mut buf).unwrap();
        let response = execute_read_response(&buf).unwrap();
        let SqlExecute::Miss = response else {
            panic!("Expected Miss response");
        };
    }

    #[test]
    fn test_execute_read_response_dql_encoded() {
        use crate::encode::execute_write_dql_response;

        let mut buf = Vec::new();
        let tuples = vec![b"\x92\x01\xa3one".as_ref(), b"\x92\x02\xa3two".as_ref()];
        execute_write_dql_response(&mut buf, 2, tuples.into_iter()).unwrap();
        let response = execute_read_response(&buf).unwrap();
        let SqlExecute::Dql(mut iter) = response else {
            panic!("Expected DQL response");
        };
        let slice1 = iter.next().unwrap().unwrap();
        assert_eq!(slice1, b"\x92\x01\xa3one");
        let slice2 = iter.next().unwrap().unwrap();
        assert_eq!(slice2, b"\x92\x02\xa3two");
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_execute_args_split_truncated() {
        // [1.5, 7, "sid", true, ["rid", 0, nil]]
        let buf = b"\x95\xcb\x3f\xf8\x00\x00\x00\x00\x00\x00\x07\xa3sid\xc3\x93\xa3rid\x00\xc0";
        // The message is the last 7 bytes. The split does not examine it.
        for len in 0..buf.len() - 7 {
            assert!(execute_args_split(&buf[..len]).is_err(), "{len}");
        }
    }

    #[test]
    fn test_query_meta_args_split_truncated() {
        // [3, "rid", 42]
        let buf = b"\x93\x03\xa3rid\x2a";
        for len in 0..buf.len() {
            assert!(query_meta_args_split(&buf[..len]).is_err(), "{len}");
        }
    }
}
