use crate::dml::dml_type::write_dml_header;
use crate::dml::dml_type::DMLType::Insert;
use crate::dql_encoder::MsgpackWriter;
use crate::error::ProtocolError;
use crate::iterators::TupleIterator;
use crate::msgpack::{skip_value, ByteCounter};
use rmp::decode::{read_array_len, read_int, read_pfix};
use rmp::encode::{write_array_len, write_pfix, write_uint};
use std::cmp::PartialEq;
use std::io::Cursor;

pub trait InsertEncoder {
    fn get_request_id(&self) -> &str;
    fn get_target_table_id(&self) -> u32;
    fn get_target_table_version(&self) -> u64;
    fn get_conflict_policy(&self) -> ConflictPolicy;
    fn get_tuples(&self) -> impl MsgpackWriter;
}

pub fn write_insert_package(
    mut w: impl std::io::Write,
    data: impl InsertEncoder,
) -> Result<(), std::io::Error> {
    write_dml_header(&mut w, Insert, data.get_request_id())?;
    write_array_len(&mut w, 4)?;

    write_uint(&mut w, data.get_target_table_id() as u64)?;
    write_uint(&mut w, data.get_target_table_version())?;
    write_pfix(&mut w, data.get_conflict_policy() as u8)?;

    let mut tuples = data.get_tuples();
    write_array_len(&mut w, tuples.len() as u32)?;
    while tuples.next().is_some() {
        let mut tuple_counter = ByteCounter::default();
        tuples.write_current(&mut tuple_counter)?;
        rmp::encode::write_bin_len(&mut w, tuple_counter.bytes() as u32)?;
        tuples.write_current(&mut w)?;
    }

    Ok(())
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
enum InsertStates {
    TableId = 1,
    TableVersion,
    ConflictPolicy,
    Tuples,
    End,
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
pub enum ConflictPolicy {
    DoFail = 0,
    DoNothing,
    DoReplace,
}

impl TryFrom<u8> for ConflictPolicy {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ConflictPolicy::DoFail),
            1 => Ok(ConflictPolicy::DoNothing),
            2 => Ok(ConflictPolicy::DoReplace),
            _ => Err(format!("Invalid conflict policy: {}", value)),
        }
    }
}

pub enum InsertResult<'a> {
    TableId(u64),
    TableVersion(u64),
    ConflictPolicy(ConflictPolicy),
    Tuples(TupleIterator<'a>),
}

const INSERT_PACKAGE_SIZE: usize = 4;
pub struct InsertPackageIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: InsertStates,
}

impl<'a> InsertPackageIterator<'a> {
    pub fn new(payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(payload);

        let size = read_array_len(&mut cursor)?;
        if size != INSERT_PACKAGE_SIZE as u32 {
            return Err(ProtocolError::DecodeError(format!(
                "DML Insert package is invalid: expected to have package array length {INSERT_PACKAGE_SIZE}, got {size}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: InsertStates::TableId,
        })
    }

    fn get_target_table_id(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, InsertStates::TableId);
        let target_table_id = read_int(&mut self.raw_payload)?;
        self.state = InsertStates::TableVersion;
        Ok(target_table_id)
    }

    fn get_target_table_version(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, InsertStates::TableVersion);
        let target_table_version = read_int(&mut self.raw_payload)?;
        self.state = InsertStates::ConflictPolicy;
        Ok(target_table_version)
    }

    fn get_conflict_policy(&mut self) -> Result<ConflictPolicy, ProtocolError> {
        let conflict_policy = read_pfix(&mut self.raw_payload)?
            .try_into()
            .map_err(ProtocolError::DecodeError)?;
        self.state = InsertStates::Tuples;
        Ok(conflict_policy)
    }

    fn get_tuples(&mut self) -> Result<TupleIterator<'a>, ProtocolError> {
        assert_eq!(self.state, InsertStates::Tuples);
        let rows = read_array_len(&mut self.raw_payload)? as usize;
        let start = self.raw_payload.position() as usize;
        for _ in 0..rows {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;

        let payload = &self.raw_payload.get_ref()[start..end];
        let tuples = TupleIterator::new(payload, rows);

        self.state = InsertStates::End;
        Ok(tuples)
    }
}

impl<'a> Iterator for InsertPackageIterator<'a> {
    type Item = Result<InsertResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            InsertStates::TableId => Some(self.get_target_table_id().map(InsertResult::TableId)),
            InsertStates::TableVersion => Some(
                self.get_target_table_version()
                    .map(InsertResult::TableVersion),
            ),
            InsertStates::ConflictPolicy => {
                Some(self.get_conflict_policy().map(InsertResult::ConflictPolicy))
            }
            InsertStates::Tuples => Some(self.get_tuples().map(InsertResult::Tuples)),
            InsertStates::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = INSERT_PACKAGE_SIZE - self.state as usize;
        (len, Some(len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_type::MessageType;
    use rmp::decode::read_str_len;
    use std::str::from_utf8;

    struct TestInsertEncoder {
        request_id: String,
        table_id: u32,
        table_version: u64,
        conflict_policy: ConflictPolicy,
        tuples: Vec<Vec<u64>>,
    }

    impl InsertEncoder for TestInsertEncoder {
        fn get_request_id(&self) -> &str {
            self.request_id.as_str()
        }

        fn get_target_table_id(&self) -> u32 {
            self.table_id
        }

        fn get_target_table_version(&self) -> u64 {
            self.table_version
        }

        fn get_conflict_policy(&self) -> ConflictPolicy {
            self.conflict_policy
        }

        fn get_tuples(&self) -> impl MsgpackWriter {
            crate::iterators::TestTuplesWriter::new(self.tuples.iter())
        }
    }

    #[test]
    fn test_encode_insert_with_tuples() {
        let encoder = TestInsertEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            conflict_policy: ConflictPolicy::DoNothing,
            tuples: vec![vec![1, 2], vec![3, 4]],
        };

        let expected: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x01\x92\x00\x94\xcc\x80\x01\x01\x92\xc4\x03\x92\x01\x02\xc4\x03\x92\x03\x04";
        let mut actual = Vec::new();

        write_insert_package(&mut actual, encoder).unwrap();

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_decode_insert_with_tuples() {
        let mut data: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x01\x92\x00\x94\xcc\x80\x01\x01\x92\xc4\x03\x92\x01\x02\xc4\x03\x92\x03\x04";

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
        assert_eq!(dml_type, Insert as u8);

        let package = InsertPackageIterator::new(data).unwrap();

        for elem in package {
            match elem.unwrap() {
                InsertResult::TableId(table_id) => {
                    assert_eq!(table_id, 128);
                }
                InsertResult::TableVersion(version) => {
                    assert_eq!(version, 1);
                }
                InsertResult::ConflictPolicy(conflict_policy) => {
                    assert_eq!(conflict_policy, ConflictPolicy::DoNothing);
                }
                InsertResult::Tuples(tuples) => {
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
