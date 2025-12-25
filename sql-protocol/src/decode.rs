#[allow(deprecated)]
use crate::decode::ExecuteArgsData::{New, Old};
use crate::dml::delete::{DeleteFilteredIterator, DeleteFullIterator};
use crate::dml::dml_type::DMLType;
use crate::dml::insert::{InsertIterator, InsertMaterializedIterator};
use crate::dml::update::{UpdateIterator, UpdateSharedKeyIterator};
use crate::dql::DQLPacketPayloadIterator;
use crate::error::ProtocolError;
use crate::message_type::MessageType;
use crate::msgpack::{shift_pos, skip_value};
use rmp::decode::{
    read_array_len, read_bool, read_f32, read_f64, read_int, read_map_len, read_pfix, read_str_len,
    RmpRead,
};
use rmp::Marker;
use std::io::{Cursor, Error as IoError, Result as IoResult};
use std::str::from_utf8;

#[derive(Default)]
pub struct QueryMetaArgs<'bytes> {
    pub timeout: f64,
    pub request_id: &'bytes str,
    pub plan_id: u64,
}

#[allow(deprecated)]
pub struct ExecuteArgs<'bytes> {
    pub timeout: f64,
    pub need_ref: bool,
    pub rid: i64,
    pub sid: &'bytes str,
    pub data: ExecuteArgsData<'bytes>,
}
/// Temporary enum for protocol migration (remove after old protocol deprecation).
/// New: raw payload bytes for new protocol
/// Old: raw legacy protocol data
#[deprecated(note = "Remove in next release. Change to &[u8]. Used for smooth upgrade")]
#[allow(deprecated)]
pub enum ExecuteArgsData<'bytes> {
    New(&'bytes [u8]),
    Old(OldExecuteArgs<'bytes>),
}
#[deprecated(note = "Remove in next release. Used for smooth upgrade")]
pub struct OldExecuteArgs<'bytes> {
    pub required: &'bytes [u8],
    pub optional: Option<&'bytes [u8]>,
}

pub enum ProtocolMessageType {
    Dql,
    Dml(DMLType),
    LocalDml(DMLType),
}

/// A decoded protocol message.
/// Consists of common data that identifies the request and message type.
/// `bytes` contains the payload for this message.
pub struct ProtocolMessage<'bytes> {
    pub request_id: &'bytes str,
    pub msg_type: ProtocolMessageType,
    payload: &'bytes [u8],
}

pub enum ProtocolMessageIter<'bytes> {
    Dql(DQLPacketPayloadIterator<'bytes>),
    DmlInsert(InsertIterator<'bytes>),
    DmlUpdate(UpdateSharedKeyIterator<'bytes>),
    DmlDelete(DeleteFullIterator<'bytes>),
    LocalDmlInsert(InsertMaterializedIterator<'bytes>),
    LocalDmlUpdate(UpdateIterator<'bytes>),
    LocalDmlDelete(DeleteFilteredIterator<'bytes>),
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

    /// Returns an iterator over the message payload.
    pub fn get_iter(&self) -> Result<ProtocolMessageIter<'bytes>, ProtocolError> {
        match &self.msg_type {
            ProtocolMessageType::Dql => {
                DQLPacketPayloadIterator::new(self.payload).map(ProtocolMessageIter::Dql)
            }
            ProtocolMessageType::Dml(dml_type) => match dml_type {
                DMLType::Insert => {
                    InsertIterator::new(self.payload).map(ProtocolMessageIter::DmlInsert)
                }
                DMLType::Update => {
                    UpdateSharedKeyIterator::new(self.payload).map(ProtocolMessageIter::DmlUpdate)
                }
                DMLType::Delete => {
                    DeleteFullIterator::new(self.payload).map(ProtocolMessageIter::DmlDelete)
                }
            },
            ProtocolMessageType::LocalDml(dml_type) => match dml_type {
                DMLType::Insert => InsertMaterializedIterator::new(self.payload)
                    .map(ProtocolMessageIter::LocalDmlInsert),
                DMLType::Update => {
                    UpdateIterator::new(self.payload).map(ProtocolMessageIter::LocalDmlUpdate)
                }
                DMLType::Delete => DeleteFilteredIterator::new(self.payload)
                    .map(ProtocolMessageIter::LocalDmlDelete),
            },
        }
    }
}

#[allow(deprecated)]
pub fn execute_args_split(mp: &[u8]) -> IoResult<ExecuteArgs<'_>> {
    let mut stream = Cursor::new(mp);
    let elems = read_array_len(&mut stream)
        .map_err(|e| IoError::other(format!("Failed to decode arguments array length: {e}")))?
        as usize;
    if elems != 5 && elems != 6 {
        return Err(IoError::other(format!(
            "Expected an array of 5 or 6 elements, got: {elems}"
        )));
    }
    // Decode vshard storage timeout. It can be either integer or float.
    let marker = Marker::from_u8(mp[stream.position() as usize]);
    let timeout = match marker {
        Marker::F64 => read_f64(&mut stream)
            .map_err(|e| IoError::other(format!("Failed to decode vshard storage timeout: {e}")))?,
        Marker::F32 => read_f32(&mut stream)
            .map(|v| v as f64)
            .map_err(|e| IoError::other(format!("Failed to decode vshard storage timeout: {e}")))?,
        _ => read_int::<f64, _>(&mut stream)
            .map_err(|e| IoError::other(format!("Failed to decode vshard storage timeout: {e}")))?,
    };

    // Decode vshard storage reference ID.
    let rid = read_int(&mut stream).map_err(|e| {
        IoError::other(format!("Failed to decode vshard storage reference ID: {e}"))
    })?;

    // Decode vshard storage session ID.
    let sid_len = read_str_len(&mut stream).map_err(|e| {
        IoError::other(format!(
            "Failed to decode vshard storage session ID length: {e}"
        ))
    })?;
    let start = stream.position() as usize;
    let end = start + sid_len as usize;
    let sid_bytes = &mp[start..end];
    let sid = from_utf8(sid_bytes)
        .map_err(|e| IoError::other(format!("Invalid UTF-8 in vshard storage session ID: {e}")))?;
    stream.set_position(end as u64);

    let need_ref = read_bool(&mut stream).map_err(|e| {
        IoError::other(format!(
            "Failed to decode vshard storage read only flag: {e}"
        ))
    })?;

    let start_pos = stream.position() as usize;
    let array_len = read_array_len(&mut stream)
        .map_err(|e| IoError::other(format!("Failed to unpack required from array: {e}")))?;
    if array_len == 3 {
        Ok(ExecuteArgs {
            timeout,
            need_ref,
            rid,
            sid,
            data: New(&mp[start_pos..]),
        })
    } else {
        // Decode required data.
        if array_len != 1 {
            return Err(IoError::other(format!(
                "Expected an array of 1 element in required, got: {array_len}"
            )));
        }
        // TODO: use binary instead of string.
        let req_len = read_str_len(&mut stream)
            .map_err(|e| IoError::other(format!("Failed to unpack required bytes: {e}")))?
            as usize;
        let req_start = stream.position() as usize;
        let required = &mp[req_start..req_start + req_len];

        let mut args = ExecuteArgs {
            timeout,
            need_ref,
            rid,
            sid,
            data: Old(OldExecuteArgs {
                required,
                optional: None,
            }),
        };

        // Decode optional data if present.
        if elems == 6 {
            shift_pos(&mut stream, req_len as u64)?;
            let array_len = read_array_len(&mut stream).map_err(|e| {
                IoError::other(format!("Failed to unpack optional from array: {e}"))
            })?;
            if array_len != 1 {
                return Err(IoError::other(format!(
                    "Expected an array of 1 element in optional, got: {array_len}"
                )));
            }
            // TODO: use binary instead of string.
            let opt_len = read_str_len(&mut stream)
                .map_err(|e| IoError::other(format!("Failed to unpack optional bytes: {e}")))?
                as usize;
            let opt_start = stream.position() as usize;
            let Old(args) = &mut args.data else {
                unreachable!("Optional data must be present in old format")
            };
            args.optional = Some(&mp[opt_start..opt_start + opt_len]);
        }
        Ok(args)
    }
}

pub fn query_meta_args_split(mp: &[u8]) -> IoResult<QueryMetaArgs<'_>> {
    let mut stream = Cursor::new(mp);
    let elems = read_array_len(&mut stream)
        .map_err(|e| IoError::other(format!("Failed to decode arguments array length: {e}")))?
        as usize;
    if elems != 3 {
        return Err(IoError::other(format!(
            "Expected an array of 3 elements, got: {elems}"
        )));
    }
    // Decode vshard storage timeout. It can be either integer or float.
    let marker = Marker::from_u8(mp[stream.position() as usize]);
    let timeout = match marker {
        Marker::F64 => read_f64(&mut stream)
            .map_err(|e| IoError::other(format!("Failed to decode vshard storage timeout: {e}")))?,
        Marker::F32 => read_f32(&mut stream)
            .map(|v| v as f64)
            .map_err(|e| IoError::other(format!("Failed to decode vshard storage timeout: {e}")))?,
        _ => read_int::<f64, _>(&mut stream)
            .map_err(|e| IoError::other(format!("Failed to decode vshard storage timeout: {e}")))?,
    };

    let len = read_str_len(&mut stream)
        .map_err(|e| IoError::other(format!("Failed to decode request_id len: {e}")))?;
    let start = stream.position() as usize;
    let end = start + len as usize;
    let request_id = from_utf8(&mp[start..end])
        .map_err(|e| IoError::other(format!("Invalid UTF-8 in request_id: {e}")))?;
    stream.set_position(end as u64);

    let plan_id = read_int(&mut stream)
        .map_err(|e| IoError::other(format!("Failed to decode plan_id: {e}")))?;

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
}
