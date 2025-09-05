use crate::msgpack::skip_value;
use rmp::decode::{read_array_len, read_int, read_map_len, read_str_len, RmpRead};
use std::io::{Cursor, Error as IoError, Result as IoResult};
use std::str::from_utf8;

/// Decode a proc_sql_execute response.
pub fn execute_read_response(stream: &[u8]) -> IoResult<SqlExecute> {
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
                .map_err(|e| IoError::other(format!("Failed to decode DQL response: {e}")))?;
            Ok(SqlExecute::Dql(TupleIter { msgpack: stream }))
        }
        "miss" => Ok(SqlExecute::Miss),
        "dml" => {
            let row_cnt = read_int(&mut stream)
                .map_err(|e| IoError::other(format!("Failed to decode DML response: {e}")))?;
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
        let buf = b"\x81\xa4miss\xc0";
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
