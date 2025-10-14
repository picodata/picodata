use crate::dml::dml_type::write_dml_header;
use crate::dml::dml_type::DMLType::Delete;
use crate::error::ProtocolError;
use crate::iterators::TupleIterator;
use crate::msgpack::{skip_value, ByteCounter};
use crate::protocol_encoder::MsgpackWriter;
use rmp::decode::{read_array_len, read_int};
use rmp::encode::{write_array_len, write_uint};
use std::cmp::PartialEq;
use std::io::Cursor;

pub trait DeleteEncoder {
    fn get_request_id(&self) -> &str;
    fn get_target_table_id(&self) -> u32;
    fn get_target_table_version(&self) -> u64;
    fn has_tuples(&self) -> bool;
    fn get_tuples(&self) -> impl MsgpackWriter;
}

pub fn write_delete_package(
    mut w: impl std::io::Write,
    data: impl DeleteEncoder,
) -> Result<(), std::io::Error> {
    write_dml_header(&mut w, Delete, data.get_request_id())?;
    if data.has_tuples() {
        write_array_len(&mut w, 3)?;
    } else {
        write_array_len(&mut w, 2)?;
    }
    write_uint(&mut w, data.get_target_table_id() as u64)?;
    write_uint(&mut w, data.get_target_table_version())?;
    if data.has_tuples() {
        let mut tuples = data.get_tuples();
        write_array_len(&mut w, tuples.len() as u32)?;
        while tuples.next().is_some() {
            let mut tuple_counter = ByteCounter::default();
            tuples.write_current(&mut tuple_counter)?;
            rmp::encode::write_bin_len(&mut w, tuple_counter.bytes() as u32)?;
            tuples.write_current(&mut w)?;
        }
    }

    Ok(())
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
enum DeleteStates {
    TableId = 1,
    TableVersion,
    Tuples,
    End,
}

pub enum DeleteResult<'a> {
    TableId(u64),
    TableVersion(u64),
    Tuples(TupleIterator<'a>),
}

const DELETE_ALL_PACKAGE_SIZE: usize = 2;
const DELETE_PACKAGE_SIZE: usize = 3;
pub struct DeletePackageIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    have_tuples: bool,
    state: DeleteStates,
}

impl<'a> DeletePackageIterator<'a> {
    pub fn new(payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(payload);

        let have_tuples = match read_array_len(&mut cursor)? as usize {
            DELETE_ALL_PACKAGE_SIZE => false,
            DELETE_PACKAGE_SIZE => true,
            n => return Err(ProtocolError::DecodeError(format!(
                "DML Delete package is invalid: expected to have package array length {DELETE_ALL_PACKAGE_SIZE} or {DELETE_PACKAGE_SIZE}, got {n}"
            )))
        };

        Ok(Self {
            raw_payload: cursor,
            have_tuples,
            state: DeleteStates::TableId,
        })
    }

    fn get_target_table_id(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, DeleteStates::TableId);
        let target_table_id = read_int(&mut self.raw_payload)?;
        self.state = DeleteStates::TableVersion;
        Ok(target_table_id)
    }

    fn get_target_table_version(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, DeleteStates::TableVersion);
        let target_table_version = read_int(&mut self.raw_payload)?;
        if self.have_tuples {
            self.state = DeleteStates::Tuples;
        } else {
            self.state = DeleteStates::End;
        }
        Ok(target_table_version)
    }

    fn get_tuples(&mut self) -> Result<TupleIterator<'a>, ProtocolError> {
        assert_eq!(self.state, DeleteStates::Tuples);
        let rows = read_array_len(&mut self.raw_payload)? as usize;
        let start = self.raw_payload.position() as usize;
        for _ in 0..rows {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;

        let payload = &self.raw_payload.get_ref()[start..end];
        let tuples = TupleIterator::new(payload, rows);

        self.state = DeleteStates::End;
        Ok(tuples)
    }
}

impl<'a> Iterator for DeletePackageIterator<'a> {
    type Item = Result<DeleteResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            DeleteStates::TableId => Some(self.get_target_table_id().map(DeleteResult::TableId)),
            DeleteStates::TableVersion => Some(
                self.get_target_table_version()
                    .map(DeleteResult::TableVersion),
            ),
            DeleteStates::Tuples => Some(self.get_tuples().map(DeleteResult::Tuples)),
            DeleteStates::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = if self.have_tuples {
            debug_assert!(self.state != DeleteStates::Tuples);
            DELETE_ALL_PACKAGE_SIZE
        } else {
            DELETE_PACKAGE_SIZE
        };
        let len = size - self.state as usize;
        (len, Some(len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_type::MessageType;
    use rmp::decode::read_str_len;
    use std::io::Write;
    use std::str::from_utf8;
    struct TestTuplesWriter<'tw> {
        current: Option<&'tw Vec<u64>>,
        iter: std::slice::Iter<'tw, Vec<u64>>,
    }

    impl<'tw> TestTuplesWriter<'tw> {
        fn new(iter: std::slice::Iter<'tw, Vec<u64>>) -> Self {
            Self {
                current: None,
                iter,
            }
        }
    }

    impl MsgpackWriter for TestTuplesWriter<'_> {
        fn write_current(&self, mut w: impl Write) -> std::io::Result<()> {
            let Some(elem) = self.current else {
                return Ok(());
            };

            rmp::encode::write_array_len(&mut w, elem.len() as u32)?;

            for elem in elem {
                rmp::encode::write_uint(&mut w, *elem)?;
            }

            Ok(())
        }

        fn next(&mut self) -> Option<()> {
            self.current = self.iter.next();

            self.current?;

            Some(())
        }

        fn len(&self) -> usize {
            self.iter.len()
        }
    }

    struct TestDeleteEncoder {
        request_id: String,
        table_id: u32,
        table_version: u64,
        tuples: Option<Vec<Vec<u64>>>,
    }

    impl DeleteEncoder for TestDeleteEncoder {
        fn get_request_id(&self) -> &str {
            self.request_id.as_str()
        }

        fn get_target_table_id(&self) -> u32 {
            self.table_id
        }

        fn get_target_table_version(&self) -> u64 {
            self.table_version
        }

        fn has_tuples(&self) -> bool {
            self.tuples.is_some()
        }

        fn get_tuples(&self) -> impl MsgpackWriter {
            let Some(tuples) = &self.tuples else {
                unreachable!();
            };
            TestTuplesWriter::new(tuples.iter())
        }
    }

    #[test]
    fn test_encode_pure_delete() {
        let encoder = TestDeleteEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            tuples: None,
        };

        let expected: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x01\x92\x02\x92\xcc\x80\x01";
        let mut actual = Vec::new();

        write_delete_package(&mut actual, encoder).unwrap();

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_encode_delete_with_tuples() {
        let encoder = TestDeleteEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            tuples: Some(vec![vec![1, 2], vec![3, 4]]),
        };

        let expected: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x01\x92\x02\x93\xcc\x80\x01\x92\xc4\x03\x92\x01\x02\xc4\x03\x92\x03\x04";
        let mut actual = Vec::new();

        write_delete_package(&mut actual, encoder).unwrap();

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_decode_pure_delete() {
        let mut data: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x01\x92\x02\x92\xcc\x80\x01";

        let l = read_array_len(&mut data).unwrap();
        assert_eq!(l, 3);
        let str_len = read_str_len(&mut data).unwrap();
        let (request_id, new_data) = data.split_at(str_len as usize);
        let request_id = from_utf8(request_id).unwrap();
        assert_eq!(request_id, "d3763996-6d21-418d-987f-d7349d034da9");
        data = new_data;
        let msg_type = rmp::decode::read_pfix(&mut data).unwrap();
        assert_eq!(msg_type, MessageType::DML as u8);

        // dml specific
        let l = read_array_len(&mut data).unwrap();
        assert_eq!(l, 2);
        let dml_type = rmp::decode::read_pfix(&mut data).unwrap();
        assert_eq!(dml_type, Delete as u8);

        let package = DeletePackageIterator::new(data).unwrap();

        for elem in package {
            match elem.unwrap() {
                DeleteResult::TableId(table_id) => {
                    assert_eq!(table_id, 128);
                }
                DeleteResult::TableVersion(version) => {
                    assert_eq!(version, 1);
                }
                _ => panic!("Unexpected result"),
            }
        }
    }

    #[test]
    fn test_decode_delete_with_tuples() {
        let mut data: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x01\x92\x02\x93\xcc\x80\x01\x92\xc4\x03\x92\x01\x02\xc4\x03\x92\x03\x04";

        let l = read_array_len(&mut data).unwrap();
        assert_eq!(l, 3);
        let str_len = read_str_len(&mut data).unwrap();
        let (request_id, new_data) = data.split_at(str_len as usize);
        let request_id = from_utf8(request_id).unwrap();
        assert_eq!(request_id, "d3763996-6d21-418d-987f-d7349d034da9");
        data = new_data;
        let msg_type = rmp::decode::read_pfix(&mut data).unwrap();
        assert_eq!(msg_type, MessageType::DML as u8);

        // dml specific
        let l = read_array_len(&mut data).unwrap();
        assert_eq!(l, 2);
        let dml_type = rmp::decode::read_pfix(&mut data).unwrap();
        assert_eq!(dml_type, Delete as u8);

        let package = DeletePackageIterator::new(data).unwrap();

        for elem in package {
            match elem.unwrap() {
                DeleteResult::TableId(table_id) => {
                    assert_eq!(table_id, 128);
                }
                DeleteResult::TableVersion(version) => {
                    assert_eq!(version, 1);
                }
                DeleteResult::Tuples(tuples) => {
                    assert_eq!(tuples.len(), 2);
                    let mut actual = Vec::with_capacity(2);
                    for tuple in tuples {
                        let tuple = tuple.unwrap();
                        actual.push(tuple);
                    }
                    let expected = vec![[146, 1, 2], [146, 3, 4]];
                    assert_eq!(actual, expected);
                }
            }
        }
    }
}
