use crate::dml::dml_type::DMLType::Delete;
use crate::dml::dml_type::{write_dml_header, write_dml_with_sql_header};
use crate::dql::{
    get_options, get_params, get_plan_id, get_schema_info, get_sender_id, get_vtables,
    write_options, write_params, write_plan_id, write_schema_info, write_sender_id, write_tuples,
    write_vtables,
};
use crate::dql_encoder::{DQLEncoder, MsgpackWriter};
use crate::error::ProtocolError;
use crate::iterators::{MsgpackMapIterator, TupleIterator};
use crate::msgpack::skip_value;
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
        write_tuples(&mut w, data.get_tuples())?;
    }

    Ok(())
}

pub fn write_delete_with_sql_package(
    mut w: impl std::io::Write,
    data: impl DeleteEncoder + DQLEncoder,
) -> Result<(), std::io::Error> {
    write_dml_with_sql_header(&mut w, Delete, DeleteEncoder::get_request_id(&data))?;
    write_array_len(&mut w, 8)?;
    write_uint(&mut w, data.get_target_table_id() as u64)?;
    write_uint(&mut w, data.get_target_table_version())?;

    let schema_info = data.get_schema_info();
    write_schema_info(&mut w, schema_info)?;

    let plan_id = data.get_plan_id();
    write_plan_id(&mut w, plan_id)?;

    let sender_id = data.get_sender_id();
    write_sender_id(&mut w, sender_id)?;

    write_vtables(&mut w, data.get_vtables(plan_id))?;

    let options = data.get_options();
    write_options(&mut w, options.iter())?;

    let params = data.get_params();
    write_params(&mut w, params)?;

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

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
enum LocalDeleteStates {
    TableId = 1,
    TableVersion,
    SchemaInfo,
    PlanId,
    SenderId,
    Vtables,
    Options,
    Params,
    End,
}

pub enum LocalDeleteResult<'a> {
    TableId(u64),
    TableVersion(u64),
    SchemaInfo(MsgpackMapIterator<'a, u32, u64>),
    PlanId(u64),
    SenderId(&'a str),
    Vtables(MsgpackMapIterator<'a, &'a str, TupleIterator<'a>>),
    Options((u64, u64)),
    Params(&'a [u8]),
}

const LOCAL_DELETE_PACKAGE_SIZE: usize = 8;
pub struct LocalDeletePackageIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: LocalDeleteStates,
}

impl<'a> LocalDeletePackageIterator<'a> {
    pub fn new(payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(payload);

        let size = read_array_len(&mut cursor)? as usize;
        if size != LOCAL_DELETE_PACKAGE_SIZE {
            return Err(ProtocolError::DecodeError(format!(
                "DML Local Delete package is invalid: expected to have package array length {LOCAL_DELETE_PACKAGE_SIZE}, got {size}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: LocalDeleteStates::TableId,
        })
    }

    fn get_target_table_id(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, LocalDeleteStates::TableId);
        let target_table_id = read_int(&mut self.raw_payload)?;
        self.state = LocalDeleteStates::TableVersion;
        Ok(target_table_id)
    }

    fn get_target_table_version(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, LocalDeleteStates::TableVersion);
        let target_table_version = read_int(&mut self.raw_payload)?;
        self.state = LocalDeleteStates::End;
        Ok(target_table_version)
    }

    fn get_schema_info(&mut self) -> Result<MsgpackMapIterator<'a, u32, u64>, ProtocolError> {
        assert_eq!(self.state, LocalDeleteStates::SchemaInfo);
        let schema_info = get_schema_info(&mut self.raw_payload)?;
        self.state = LocalDeleteStates::PlanId;
        Ok(schema_info)
    }

    fn get_plan_id(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, LocalDeleteStates::PlanId);
        let plan_id = get_plan_id(&mut self.raw_payload)?;
        self.state = LocalDeleteStates::SenderId;
        Ok(plan_id)
    }

    fn get_sender_id(&mut self) -> Result<&'a str, ProtocolError> {
        assert_eq!(self.state, LocalDeleteStates::SenderId);
        let sender_id = get_sender_id(&mut self.raw_payload)?;
        self.state = LocalDeleteStates::Vtables;
        Ok(sender_id)
    }

    fn get_vtables(
        &mut self,
    ) -> Result<MsgpackMapIterator<'a, &'a str, TupleIterator<'a>>, ProtocolError> {
        assert_eq!(self.state, LocalDeleteStates::Vtables);
        let vtables = get_vtables(&mut self.raw_payload)?;
        self.state = LocalDeleteStates::Options;
        Ok(vtables)
    }

    fn get_options(&mut self) -> Result<(u64, u64), ProtocolError> {
        assert_eq!(self.state, LocalDeleteStates::Options);
        let options = get_options(&mut self.raw_payload)?;
        self.state = LocalDeleteStates::Params;
        Ok(options)
    }

    fn get_params(&mut self) -> Result<&'a [u8], ProtocolError> {
        assert_eq!(self.state, LocalDeleteStates::Params);
        let params = get_params(&mut self.raw_payload)?;
        self.state = LocalDeleteStates::End;
        Ok(params)
    }
}

impl<'a> Iterator for LocalDeletePackageIterator<'a> {
    type Item = Result<LocalDeleteResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            LocalDeleteStates::TableId => {
                Some(self.get_target_table_id().map(LocalDeleteResult::TableId))
            }
            LocalDeleteStates::TableVersion => Some(
                self.get_target_table_version()
                    .map(LocalDeleteResult::TableVersion),
            ),
            LocalDeleteStates::SchemaInfo => {
                Some(self.get_schema_info().map(LocalDeleteResult::SchemaInfo))
            }
            LocalDeleteStates::PlanId => Some(self.get_plan_id().map(LocalDeleteResult::PlanId)),
            LocalDeleteStates::SenderId => {
                Some(self.get_sender_id().map(LocalDeleteResult::SenderId))
            }
            LocalDeleteStates::Vtables => Some(self.get_vtables().map(LocalDeleteResult::Vtables)),
            LocalDeleteStates::Options => Some(self.get_options().map(LocalDeleteResult::Options)),
            LocalDeleteStates::Params => Some(self.get_params().map(LocalDeleteResult::Params)),
            LocalDeleteStates::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = LOCAL_DELETE_PACKAGE_SIZE - self.state as usize;
        (size, Some(size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dql_encoder::test::{TestDQLEncoder, TestDQLEncoderBuilder};
    use crate::dql_encoder::ColumnType;
    use crate::message_type::MessageType;
    use rmp::decode::read_str_len;
    use smol_str::{SmolStr, ToSmolStr};
    use std::collections::HashMap;
    use std::str::from_utf8;

    struct TestDeleteEncoder {
        request_id: String,
        table_id: u32,
        table_version: u64,
        tuples: Option<Vec<Vec<u64>>>,
        dql_encoder: Option<TestDQLEncoder>,
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
            crate::iterators::TestTuplesWriter::new(tuples.iter())
        }
    }

    impl DQLEncoder for TestDeleteEncoder {
        fn get_schema_info(&self) -> impl ExactSizeIterator<Item = (&u32, &u64)> {
            self.dql_encoder.as_ref().unwrap().get_schema_info()
        }

        fn get_plan_id(&self) -> u64 {
            self.dql_encoder.as_ref().unwrap().get_plan_id()
        }

        fn get_sender_id(&self) -> &str {
            self.dql_encoder.as_ref().unwrap().get_sender_id()
        }

        fn get_request_id(&self) -> &str {
            unreachable!("should not be called");
        }

        fn get_vtables_metadata(
            &self,
        ) -> impl ExactSizeIterator<
            Item = (
                SmolStr,
                impl ExactSizeIterator<Item = (&SmolStr, ColumnType)>,
            ),
        > {
            unreachable!("should not be called");
            // left here to satisfy the compiler
            #[allow(unreachable_code)]
            self.dql_encoder.as_ref().unwrap().get_vtables_metadata()
        }

        fn get_vtables(
            &self,
            plan_id: u64,
        ) -> impl ExactSizeIterator<Item = (SmolStr, impl MsgpackWriter)> {
            self.dql_encoder.as_ref().unwrap().get_vtables(plan_id)
        }

        fn get_options(&self) -> [u64; 2] {
            self.dql_encoder.as_ref().unwrap().get_options()
        }

        fn get_params(&self) -> impl MsgpackWriter {
            self.dql_encoder.as_ref().unwrap().get_params()
        }

        fn get_sql(&self) -> &SmolStr {
            unreachable!("should not be called");
        }
    }

    #[test]
    fn test_encode_pure_delete() {
        let encoder = TestDeleteEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            tuples: None,
            dql_encoder: None,
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
            dql_encoder: None,
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

    #[test]
    fn test_encode_delete_with_dql() {
        let dql_encoder = TestDQLEncoderBuilder::new()
            .set_plan_id(14235593344027757343)
            .set_schema_info(HashMap::from([(12, 138)]))
            .set_sender_id("some".to_string())
            .set_vtables(HashMap::from([(
                "TMP_1302_".to_smolstr(),
                vec![vec![1, 2, 3], vec![3, 2, 1]],
            )]))
            .set_options([123, 456])
            .set_params(vec![138, 123, 432])
            .build();
        let encoder = TestDeleteEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            tuples: None,
            dql_encoder: Some(dql_encoder),
        };

        let expected: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x02\x92\x02\x98\xcc\x80\x01\x81\x0c\xcc\x8a\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f\xc4\x04some\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";
        let mut actual = Vec::new();

        write_delete_with_sql_package(&mut actual, encoder).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_delete_with_dql() {
        let mut data: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x02\x92\x02\x98\xcc\x80\x01\x81\x0c\xcc\x8a\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f\xc4\x04some\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";

        let l = read_array_len(&mut data).unwrap();
        assert_eq!(l, 3);
        let str_len = read_str_len(&mut data).unwrap();
        let (request_id, new_data) = data.split_at(str_len as usize);
        let request_id = from_utf8(request_id).unwrap();
        assert_eq!(request_id, "d3763996-6d21-418d-987f-d7349d034da9");
        data = new_data;
        let msg_type = rmp::decode::read_pfix(&mut data).unwrap();
        assert_eq!(msg_type, MessageType::LocalDML as u8);

        // dml specific
        let l = read_array_len(&mut data).unwrap();
        assert_eq!(l, 2);
        let dml_type = rmp::decode::read_pfix(&mut data).unwrap();
        assert_eq!(dml_type, Delete as u8);

        let package = LocalDeletePackageIterator::new(data).unwrap();

        for elem in package {
            match elem.unwrap() {
                LocalDeleteResult::TableId(table_id) => {
                    assert_eq!(table_id, 128);
                }
                LocalDeleteResult::TableVersion(version) => {
                    assert_eq!(version, 1);
                }
                LocalDeleteResult::SchemaInfo(schema_info) => {
                    assert_eq!(schema_info.len(), 1);
                    for res in schema_info {
                        let (t_id, ver) = res.unwrap();
                        assert_eq!(t_id, 12);
                        assert_eq!(ver, 138);
                    }
                }
                LocalDeleteResult::PlanId(plan_id) => {
                    assert_eq!(plan_id, 14235593344027757343);
                }
                LocalDeleteResult::SenderId(sender_id) => {
                    assert_eq!(sender_id, "some");
                }
                LocalDeleteResult::Vtables(vtables) => {
                    assert_eq!(vtables.len(), 1);
                    for res in vtables {
                        let (name, tuples) = res.unwrap();
                        assert_eq!(name, "TMP_1302_".to_smolstr());
                        assert_eq!(tuples.len(), 2);
                        let mut actual = Vec::with_capacity(2);
                        for tuple in tuples {
                            let tuple = tuple.unwrap();
                            actual.push(tuple);
                        }
                    }
                }
                LocalDeleteResult::Options(options) => {
                    assert_eq!(options, (123, 456));
                }
                LocalDeleteResult::Params(params) => {
                    assert_eq!(params, vec![147, 204, 138, 123, 205, 1, 176]);
                }
            }
        }
    }
}
