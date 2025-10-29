use crate::dml::dml_type::DMLType::Insert;
use crate::dml::dml_type::{write_dml_header, write_dml_with_sql_header};
use crate::dql::{
    get_options, get_params, get_plan_id, get_schema_info, get_sender_id, get_vtables,
    write_options, write_params, write_plan_id, write_schema_info, write_sender_id, write_tuples,
    write_vtables,
};
use crate::dql_encoder::{DQLDataSource, MsgpackEncode};
use crate::error::ProtocolError;
use crate::iterators::{MsgpackArrayIterator, MsgpackMapIterator, TupleIterator};
use crate::msgpack::skip_value;
use rmp::decode::{read_array_len, read_int, read_pfix};
use rmp::encode::{write_array_len, write_pfix, write_uint};
use std::cmp::PartialEq;
use std::io::Cursor;

pub trait InsertEncoder {
    fn get_request_id(&self) -> &str;
    fn get_target_table_id(&self) -> u32;
    fn get_target_table_version(&self) -> u64;
    fn get_conflict_policy(&self) -> ConflictPolicy;
    fn get_columns(&self) -> impl ExactSizeIterator<Item = &usize>;
    fn get_tuples(&self) -> impl ExactSizeIterator<Item = impl MsgpackEncode>;
}

pub fn write_insert_package(
    w: &mut impl std::io::Write,
    data: impl InsertEncoder,
) -> Result<(), std::io::Error> {
    write_dml_header(w, Insert, data.get_request_id())?;
    write_array_len(w, 4)?;

    write_uint(w, data.get_target_table_id() as u64)?;
    write_uint(w, data.get_target_table_version())?;
    write_pfix(w, data.get_conflict_policy() as u8)?;

    write_tuples(w, data.get_tuples())?;

    Ok(())
}

pub fn write_insert_with_sql_package(
    w: &mut impl std::io::Write,
    data: impl InsertEncoder + DQLDataSource,
) -> Result<(), std::io::Error> {
    write_dml_with_sql_header(w, Insert, InsertEncoder::get_request_id(&data))?;
    write_array_len(w, 10)?;

    write_uint(w, data.get_target_table_id() as u64)?;
    write_uint(w, data.get_target_table_version())?;

    let columns = data.get_columns();
    write_array_len(w, columns.len() as u32)?;
    for column in columns {
        write_uint(w, *column as u64)?;
    }

    write_pfix(w, data.get_conflict_policy() as u8)?;

    write_schema_info(w, data.get_schema_info())?;

    write_plan_id(w, data.get_plan_id())?;

    write_sender_id(w, data.get_sender_id())?;

    write_vtables(w, data.get_vtables())?;

    write_options(w, data.get_options().iter())?;

    write_params(w, data.get_params())?;

    Ok(())
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
#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
enum InsertStates {
    TableId = 0,
    TableVersion,
    ConflictPolicy,
    Tuples,
    End,
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
        assert_eq!(self.state, InsertStates::ConflictPolicy);
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

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
enum LocalInsertStates {
    TableId = 0,
    TableVersion,
    Columns,
    ConflictPolicy,
    SchemaInfo,
    PlanId,
    SenderId,
    Vtables,
    Options,
    Params,
    End,
}
pub enum LocalInsertResult<'a> {
    TableId(u64),
    TableVersion(u64),
    Columns(MsgpackArrayIterator<'a, usize>),
    ConflictPolicy(ConflictPolicy),
    SchemaInfo(MsgpackMapIterator<'a, u32, u64>),
    PlanId(u64),
    SenderId(&'a str),
    Vtables(MsgpackMapIterator<'a, &'a str, TupleIterator<'a>>),
    Options((u64, u64)),
    Params(&'a [u8]),
}

const LOCAL_INSERT_PACKAGE_SIZE: usize = 10;
pub struct LocalInsertPackageIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: LocalInsertStates,
}

impl<'a> LocalInsertPackageIterator<'a> {
    pub fn new(payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(payload);

        let size = read_array_len(&mut cursor)?;
        if size != LOCAL_INSERT_PACKAGE_SIZE as u32 {
            return Err(ProtocolError::DecodeError(format!(
                "DML Local Insert package is invalid: expected to have package array length {LOCAL_INSERT_PACKAGE_SIZE}, got {size}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: LocalInsertStates::TableId,
        })
    }

    fn get_target_table_id(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, LocalInsertStates::TableId);
        let target_table_id = read_int(&mut self.raw_payload)?;
        self.state = LocalInsertStates::TableVersion;
        Ok(target_table_id)
    }

    fn get_target_table_version(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, LocalInsertStates::TableVersion);
        let target_table_version = read_int(&mut self.raw_payload)?;
        self.state = LocalInsertStates::Columns;
        Ok(target_table_version)
    }
    fn get_columns(&mut self) -> Result<MsgpackArrayIterator<'a, usize>, ProtocolError> {
        assert_eq!(self.state, LocalInsertStates::Columns);
        let len = read_array_len(&mut self.raw_payload)?;
        let start = self.raw_payload.position() as usize;
        for _ in 0..len {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        let payload = &self.raw_payload.get_ref()[start..end];
        self.state = LocalInsertStates::ConflictPolicy;
        Ok(MsgpackArrayIterator::new(payload, len, |cur| {
            read_int(cur).map_err(ProtocolError::from)
        }))
    }

    fn get_conflict_policy(&mut self) -> Result<ConflictPolicy, ProtocolError> {
        assert_eq!(self.state, LocalInsertStates::ConflictPolicy);
        let conflict_policy = read_pfix(&mut self.raw_payload)?
            .try_into()
            .map_err(ProtocolError::DecodeError)?;
        self.state = LocalInsertStates::SchemaInfo;
        Ok(conflict_policy)
    }

    fn get_schema_info(&mut self) -> Result<MsgpackMapIterator<'a, u32, u64>, ProtocolError> {
        assert_eq!(self.state, LocalInsertStates::SchemaInfo);
        let schema_info = get_schema_info(&mut self.raw_payload)?;
        self.state = LocalInsertStates::PlanId;
        Ok(schema_info)
    }

    fn get_plan_id(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, LocalInsertStates::PlanId);
        let plan_id = get_plan_id(&mut self.raw_payload)?;
        self.state = LocalInsertStates::SenderId;
        Ok(plan_id)
    }

    fn get_sender_id(&mut self) -> Result<&'a str, ProtocolError> {
        assert_eq!(self.state, LocalInsertStates::SenderId);
        let sender_id = get_sender_id(&mut self.raw_payload)?;
        self.state = LocalInsertStates::Vtables;
        Ok(sender_id)
    }

    fn get_vtables(
        &mut self,
    ) -> Result<MsgpackMapIterator<'a, &'a str, TupleIterator<'a>>, ProtocolError> {
        assert_eq!(self.state, LocalInsertStates::Vtables);
        let vtables = get_vtables(&mut self.raw_payload)?;
        self.state = LocalInsertStates::Options;
        Ok(vtables)
    }

    fn get_options(&mut self) -> Result<(u64, u64), ProtocolError> {
        assert_eq!(self.state, LocalInsertStates::Options);
        let options = get_options(&mut self.raw_payload)?;
        self.state = LocalInsertStates::Params;
        Ok(options)
    }

    fn get_params(&mut self) -> Result<&'a [u8], ProtocolError> {
        assert_eq!(self.state, LocalInsertStates::Params);
        let params = get_params(&mut self.raw_payload)?;
        self.state = LocalInsertStates::End;
        Ok(params)
    }
}

impl<'a> Iterator for LocalInsertPackageIterator<'a> {
    type Item = Result<LocalInsertResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            LocalInsertStates::TableId => {
                Some(self.get_target_table_id().map(LocalInsertResult::TableId))
            }
            LocalInsertStates::TableVersion => Some(
                self.get_target_table_version()
                    .map(LocalInsertResult::TableVersion),
            ),
            LocalInsertStates::Columns => Some(self.get_columns().map(LocalInsertResult::Columns)),
            LocalInsertStates::ConflictPolicy => Some(
                self.get_conflict_policy()
                    .map(LocalInsertResult::ConflictPolicy),
            ),
            LocalInsertStates::SchemaInfo => {
                Some(self.get_schema_info().map(LocalInsertResult::SchemaInfo))
            }
            LocalInsertStates::PlanId => Some(self.get_plan_id().map(LocalInsertResult::PlanId)),
            LocalInsertStates::SenderId => {
                Some(self.get_sender_id().map(LocalInsertResult::SenderId))
            }
            LocalInsertStates::Vtables => Some(self.get_vtables().map(LocalInsertResult::Vtables)),
            LocalInsertStates::Options => Some(self.get_options().map(LocalInsertResult::Options)),
            LocalInsertStates::Params => Some(self.get_params().map(LocalInsertResult::Params)),
            LocalInsertStates::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = LOCAL_INSERT_PACKAGE_SIZE - self.state as usize;
        (len, Some(len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dql_encoder::test::{TestDQLDataSource, TestDQLEncoderBuilder};
    use crate::iterators::TestPureTupleEncoder;
    use crate::message_type::MessageType;
    use rmp::decode::read_str_len;

    use std::collections::HashMap;
    use std::str::from_utf8;

    struct TestInsertEncoder {
        request_id: String,
        table_id: u32,
        table_version: u64,
        conflict_policy: ConflictPolicy,
        columns: Vec<usize>,
        tuples: Vec<Vec<u64>>,
        dql_encoder: Option<TestDQLDataSource>,
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

        fn get_columns(&self) -> impl ExactSizeIterator<Item = &usize> {
            self.columns.iter()
        }

        fn get_tuples(&self) -> impl ExactSizeIterator<Item = impl MsgpackEncode> {
            self.tuples.iter().map(TestPureTupleEncoder::new)
        }
    }

    impl DQLDataSource for TestInsertEncoder {
        fn get_schema_info(&self) -> impl ExactSizeIterator<Item = (u32, u64)> {
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

        fn get_vtables(
            &self,
        ) -> impl ExactSizeIterator<Item = (&str, impl ExactSizeIterator<Item = impl MsgpackEncode>)>
        {
            self.dql_encoder.as_ref().unwrap().get_vtables()
        }

        fn get_options(&self) -> [u64; 2] {
            self.dql_encoder.as_ref().unwrap().get_options()
        }

        fn get_params(&self) -> impl MsgpackEncode {
            self.dql_encoder.as_ref().unwrap().get_params()
        }
    }

    #[test]
    fn test_encode_insert_with_tuples() {
        let encoder = TestInsertEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            conflict_policy: ConflictPolicy::DoNothing,
            columns: vec![],
            tuples: vec![vec![1, 2], vec![3, 4]],
            dql_encoder: None,
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

    #[test]
    fn test_encode_insert_with_sql() {
        let dql_encoder = TestDQLEncoderBuilder::new()
            .set_plan_id(14235593344027757343)
            .set_schema_info(HashMap::from([(12, 138)]))
            .set_sender_id("some".to_string())
            .set_vtables(HashMap::from([(
                "TMP_1302_".to_string(),
                vec![vec![1, 2, 3], vec![3, 2, 1]],
            )]))
            .set_options([123, 456])
            .set_params(vec![138, 123, 432])
            .build();
        let encoder = TestInsertEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            conflict_policy: ConflictPolicy::DoNothing,
            columns: vec![1, 2],
            tuples: vec![],
            dql_encoder: Some(dql_encoder),
        };

        let expected: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x02\x92\x00\x9a\xcc\x80\x01\x92\x01\x02\x01\x81\x0c\xcc\x8a\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f\xc4\x04some\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";
        let mut actual = Vec::new();

        write_insert_with_sql_package(&mut actual, encoder).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_insert_with_sql() {
        let mut data: &[u8] = b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x02\x92\x00\x9a\xcc\x80\x01\x92\x01\x02\x01\x81\x0c\xcc\x8a\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f\xc4\x04some\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";

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
        let dml_type = read_pfix(&mut data).unwrap();
        assert_eq!(dml_type, Insert as u8);

        let package = LocalInsertPackageIterator::new(data).unwrap();

        for elem in package {
            match elem.unwrap() {
                LocalInsertResult::TableId(table_id) => {
                    assert_eq!(table_id, 128);
                }
                LocalInsertResult::TableVersion(version) => {
                    assert_eq!(version, 1);
                }
                LocalInsertResult::Columns(columns) => {
                    let columns = columns.map(|x| x.unwrap()).collect::<Vec<_>>();
                    assert_eq!(columns, vec![1, 2]);
                }
                LocalInsertResult::ConflictPolicy(conflict_policy) => {
                    assert_eq!(conflict_policy, ConflictPolicy::DoNothing);
                }
                LocalInsertResult::SchemaInfo(schema_info) => {
                    assert_eq!(schema_info.len(), 1);
                    for res in schema_info {
                        let (t_id, ver) = res.unwrap();
                        assert_eq!(t_id, 12);
                        assert_eq!(ver, 138);
                    }
                }
                LocalInsertResult::PlanId(plan_id) => {
                    assert_eq!(plan_id, 14235593344027757343);
                }
                LocalInsertResult::SenderId(sender_id) => {
                    assert_eq!(sender_id, "some");
                }
                LocalInsertResult::Vtables(vtables) => {
                    assert_eq!(vtables.len(), 1);
                    for res in vtables {
                        let (name, tuples) = res.unwrap();
                        assert_eq!(name, "TMP_1302_".to_string());
                        assert_eq!(tuples.len(), 2);
                        let mut actual = Vec::with_capacity(2);
                        for tuple in tuples {
                            let tuple = tuple.unwrap();
                            actual.push(tuple);
                        }
                    }
                }
                LocalInsertResult::Options(options) => {
                    assert_eq!(options, (123, 456));
                }
                LocalInsertResult::Params(params) => {
                    assert_eq!(params, vec![147, 204, 138, 123, 205, 1, 176]);
                }
            }
        }
    }
}
