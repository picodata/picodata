use crate::dml::dml_type::write_dml_header;
use crate::dml::dml_type::DMLType::Update;
use crate::error::ProtocolError;
use crate::iterators::TupleIterator;
use crate::msgpack::{skip_value, ByteCounter};
use crate::protocol_encoder::MsgpackWriter;
use rmp::decode::{read_array_len, read_int};
use rmp::encode::{write_array_len, write_pfix, write_uint};
use std::cmp::PartialEq;
use std::io::Cursor;

pub trait UpdateEncoder {
    fn get_request_id(&self) -> &str;
    fn get_target_table_id(&self) -> u32;
    fn get_target_table_version(&self) -> u64;
    fn get_update_type(&self) -> UpdateType;
    fn get_tuples(&self) -> impl MsgpackWriter;
}

pub fn write_update_package(
    mut w: impl std::io::Write,
    data: impl UpdateEncoder,
) -> Result<(), std::io::Error> {
    write_dml_header(&mut w, Update, data.get_request_id())?;
    write_array_len(&mut w, 4)?;

    write_uint(&mut w, data.get_target_table_id() as u64)?;
    write_uint(&mut w, data.get_target_table_version())?;
    write_pfix(&mut w, data.get_update_type() as u8)?;

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
pub enum UpdateType {
    Shared = 0,
    Local,
}

impl TryFrom<u8> for UpdateType {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(UpdateType::Shared),
            1 => Ok(UpdateType::Local),
            _ => Err(format!("Invalid update type: {}", value)),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
enum UpdateStates {
    TableId = 1,
    TableVersion,
    UpdateType,
    Tuples,
    End,
}

pub enum UpdateResult<'a> {
    TableId(u64),
    TableVersion(u64),
    UpdateType(UpdateType),
    Tuples(TupleIterator<'a>),
}
const UPDATE_PACKAGE_SIZE: usize = 4;
pub struct UpdatePackageIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: UpdateStates,
}

impl<'a> UpdatePackageIterator<'a> {
    pub fn new(payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(payload);

        let size = read_array_len(&mut cursor)?;
        if size != UPDATE_PACKAGE_SIZE as u32 {
            return Err(ProtocolError::DecodeError(format!(
                "DML Update package is invalid: expected to have package array length {UPDATE_PACKAGE_SIZE}, got {size}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: UpdateStates::TableId,
        })
    }

    fn get_target_table_id(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, UpdateStates::TableId);
        let target_table_id = read_int(&mut self.raw_payload)?;
        self.state = UpdateStates::TableVersion;
        Ok(target_table_id)
    }

    fn get_target_table_version(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, UpdateStates::TableVersion);
        let target_table_version = read_int(&mut self.raw_payload)?;
        self.state = UpdateStates::UpdateType;
        Ok(target_table_version)
    }

    fn get_update_type(&mut self) -> Result<UpdateType, ProtocolError> {
        assert_eq!(self.state, UpdateStates::UpdateType);
        let update_type = rmp::decode::read_pfix(&mut self.raw_payload)?
            .try_into()
            .map_err(ProtocolError::DecodeError)?;
        self.state = UpdateStates::Tuples;
        Ok(update_type)
    }

    fn get_tuples(&mut self) -> Result<TupleIterator<'a>, ProtocolError> {
        assert_eq!(self.state, UpdateStates::Tuples);
        let rows = read_array_len(&mut self.raw_payload)? as usize;
        let start = self.raw_payload.position() as usize;
        for _ in 0..rows {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;

        let payload = &self.raw_payload.get_ref()[start..end];
        let tuples = TupleIterator::new(payload, rows);

        self.state = UpdateStates::End;
        Ok(tuples)
    }
}

impl<'a> Iterator for UpdatePackageIterator<'a> {
    type Item = Result<UpdateResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            UpdateStates::TableId => Some(self.get_target_table_id().map(UpdateResult::TableId)),
            UpdateStates::TableVersion => Some(
                self.get_target_table_version()
                    .map(UpdateResult::TableVersion),
            ),
            UpdateStates::UpdateType => Some(self.get_update_type().map(UpdateResult::UpdateType)),
            UpdateStates::Tuples => Some(self.get_tuples().map(UpdateResult::Tuples)),
            UpdateStates::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = UPDATE_PACKAGE_SIZE - self.state as usize;
        (len, Some(len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_type::MessageType;
    use rmp::decode::read_str_len;
    use std::str::from_utf8;

    struct TestUpdateEncoder {
        request_id: String,
        table_id: u32,
        table_version: u64,
        update_type: UpdateType,
        tuples: Vec<Vec<u64>>,
    }

    impl UpdateEncoder for TestUpdateEncoder {
        fn get_request_id(&self) -> &str {
            self.request_id.as_str()
        }

        fn get_target_table_id(&self) -> u32 {
            self.table_id
        }

        fn get_target_table_version(&self) -> u64 {
            self.table_version
        }

        fn get_update_type(&self) -> UpdateType {
            self.update_type
        }

        fn get_tuples(&self) -> impl MsgpackWriter {
            crate::iterators::TestTuplesWriter::new(self.tuples.iter())
        }
    }

    #[test]
    fn test_encode_update() {
        let encoder = TestUpdateEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            update_type: UpdateType::Shared,
            tuples: vec![vec![1, 2], vec![3, 4]],
        };

        let expected: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x01\x92\x01\x94\xcc\x80\x01\x00\x92\xc4\x03\x92\x01\x02\xc4\x03\x92\x03\x04";
        let mut actual = Vec::new();

        write_update_package(&mut actual, encoder).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_update() {
        let mut data: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x01\x92\x01\x94\xcc\x80\x01\x00\x92\xc4\x03\x92\x01\x02\xc4\x03\x92\x03\x04";

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
        assert_eq!(dml_type, Update as u8);

        let package = UpdatePackageIterator::new(data).unwrap();

        for elem in package {
            match elem.unwrap() {
                UpdateResult::TableId(table_id) => {
                    assert_eq!(table_id, 128);
                }
                UpdateResult::TableVersion(version) => {
                    assert_eq!(version, 1);
                }
                UpdateResult::UpdateType(update_type) => {
                    assert_eq!(update_type, UpdateType::Shared);
                }
                UpdateResult::Tuples(tuples) => {
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
