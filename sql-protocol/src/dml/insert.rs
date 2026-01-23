use crate::dml::dml_type::DMLType::Insert;
use crate::dml::dml_type::{write_dml_header, write_dml_with_sql_header};
use crate::dql::{write_dql_packet_data, write_tuples, DQLPacketPayloadIterator};
use crate::dql_encoder::{ColumnType, DQLDataSource, MsgpackEncode};
use crate::error::ProtocolError;
use crate::iterators::{MsgpackArrayIterator, TupleIterator};
use crate::msgpack::skip_value;
use rmp::decode::{read_array_len, read_int, read_pfix};
use rmp::encode::{write_array_len, write_pfix, write_uint};
use std::cmp::PartialEq;
use std::fmt;
use std::io::{Cursor, Seek, SeekFrom};

pub trait CoreInsertDataSource {
    fn get_request_id(&self) -> &str;
    fn get_target_table_id(&self) -> u32;
    fn get_target_table_version(&self) -> u64;
    fn get_conflict_policy(&self) -> ConflictPolicy;
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

pub trait InsertDataSource: CoreInsertDataSource {
    fn get_tuples(&self) -> impl ExactSizeIterator<Item = impl MsgpackEncode>;
}

pub fn write_insert_packet(
    w: &mut impl std::io::Write,
    data: &impl InsertDataSource,
) -> Result<(), std::io::Error> {
    write_dml_header(w, Insert, data.get_request_id())?;
    write_array_len(w, INSERT_FIELD_COUNT as u32)?;

    write_uint(w, data.get_target_table_id() as u64)?;
    write_uint(w, data.get_target_table_version())?;
    write_pfix(w, data.get_conflict_policy() as u8)?;

    write_tuples(w, data.get_tuples())?;

    Ok(())
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
enum InsertState {
    TableId = 0,
    TableVersion,
    ConflictPolicy,
    Tuples,
    End,
}
pub enum InsertResult<'a> {
    TableId(u32),
    TableVersion(u64),
    ConflictPolicy(ConflictPolicy),
    Tuples(TupleIterator<'a>),
}

impl fmt::Display for InsertResult<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InsertResult::TableId(_) => f.write_str("TableId"),
            InsertResult::TableVersion(_) => f.write_str("TableVersion"),
            InsertResult::ConflictPolicy(_) => f.write_str("ConflictPolicy"),
            InsertResult::Tuples(_) => f.write_str("Tuples"),
        }
    }
}

const INSERT_FIELD_COUNT: usize = 4;
pub struct InsertIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: InsertState,
}

impl<'a> InsertIterator<'a> {
    pub fn new(payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(payload);

        let size = read_array_len(&mut cursor)?;
        if size != INSERT_FIELD_COUNT as u32 {
            return Err(ProtocolError::DecodeError(format!(
                "Invalid Tuple Insert payload: expected array length {INSERT_FIELD_COUNT}, got {size}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: InsertState::TableId,
        })
    }

    fn get_target_table_id(&mut self) -> Result<u32, ProtocolError> {
        debug_assert_eq!(self.state, InsertState::TableId);
        let target_table_id = read_int(&mut self.raw_payload)?;
        self.state = InsertState::TableVersion;
        Ok(target_table_id)
    }

    fn get_target_table_version(&mut self) -> Result<u64, ProtocolError> {
        debug_assert_eq!(self.state, InsertState::TableVersion);
        let target_table_version = read_int(&mut self.raw_payload)?;
        self.state = InsertState::ConflictPolicy;
        Ok(target_table_version)
    }

    fn get_conflict_policy(&mut self) -> Result<ConflictPolicy, ProtocolError> {
        debug_assert_eq!(self.state, InsertState::ConflictPolicy);
        let conflict_policy = read_pfix(&mut self.raw_payload)?
            .try_into()
            .map_err(ProtocolError::DecodeError)?;
        self.state = InsertState::Tuples;
        Ok(conflict_policy)
    }

    fn get_tuples(&mut self) -> Result<TupleIterator<'a>, ProtocolError> {
        debug_assert_eq!(self.state, InsertState::Tuples);
        let rows = read_array_len(&mut self.raw_payload)? as usize;
        let start = self.raw_payload.position() as usize;
        for _ in 0..rows {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;

        let payload = &self.raw_payload.get_ref()[start..end];
        let tuples = TupleIterator::new(payload, rows);

        self.state = InsertState::End;
        Ok(tuples)
    }
}

impl<'a> Iterator for InsertIterator<'a> {
    type Item = Result<InsertResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            InsertState::TableId => Some(self.get_target_table_id().map(InsertResult::TableId)),
            InsertState::TableVersion => Some(
                self.get_target_table_version()
                    .map(InsertResult::TableVersion),
            ),
            InsertState::ConflictPolicy => {
                Some(self.get_conflict_policy().map(InsertResult::ConflictPolicy))
            }
            InsertState::Tuples => Some(self.get_tuples().map(InsertResult::Tuples)),
            InsertState::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = INSERT_FIELD_COUNT.saturating_sub(self.state as usize);
        (len, Some(len))
    }
}

pub trait InsertMaterializedDataSource: CoreInsertDataSource {
    fn get_column_types(&self) -> impl ExactSizeIterator<Item = ColumnType>;
    fn get_builder(&self) -> impl MsgpackEncode;
    fn get_dql_data_source(&self) -> &impl DQLDataSource;
}

pub fn write_insert_materialized_packet(
    w: &mut impl std::io::Write,
    data: &impl InsertMaterializedDataSource,
) -> Result<(), std::io::Error> {
    write_dml_with_sql_header(w, Insert, CoreInsertDataSource::get_request_id(data))?;
    write_array_len(w, INSERT_MATERIALIZED_FIELD_COUNT as u32)?;

    write_uint(w, data.get_target_table_id() as u64)?;
    write_uint(w, data.get_target_table_version())?;
    write_pfix(w, data.get_conflict_policy() as u8)?;

    let types = data.get_column_types();
    write_array_len(w, types.len() as u32)?;
    for ty in types {
        write_pfix(w, ty as u8)?;
    }

    let builder = data.get_builder();
    builder.encode_into(w)?;

    write_dql_packet_data(w, data.get_dql_data_source())?;

    Ok(())
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
enum InsertMaterializedState {
    TableId = 0,
    TableVersion,
    ConflictPolicy,
    Columns,
    Builder,
    DqlInfo,
    End,
}
pub enum InsertMaterializedResult<'a> {
    TableId(u32),
    TableVersion(u64),
    ConflictPolicy(ConflictPolicy),
    Columns(MsgpackArrayIterator<'a, ColumnType>),
    Builder(&'a [u8]),
    DqlInfo(DQLPacketPayloadIterator<'a>),
}

impl fmt::Display for InsertMaterializedResult<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InsertMaterializedResult::TableId(_) => f.write_str("TableId"),
            InsertMaterializedResult::TableVersion(_) => f.write_str("TableVersion"),
            InsertMaterializedResult::ConflictPolicy(_) => f.write_str("ConflictPolicy"),
            InsertMaterializedResult::Columns(_) => f.write_str("Columns"),
            InsertMaterializedResult::Builder(_) => f.write_str("Builder"),
            InsertMaterializedResult::DqlInfo(_) => f.write_str("DqlInfo"),
        }
    }
}

const INSERT_MATERIALIZED_FIELD_COUNT: usize = 6;
pub struct InsertMaterializedIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: InsertMaterializedState,
}

impl<'a> InsertMaterializedIterator<'a> {
    pub fn new(payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(payload);

        let size = read_array_len(&mut cursor)?;
        if size != INSERT_MATERIALIZED_FIELD_COUNT as u32 {
            return Err(ProtocolError::DecodeError(format!(
                "Invalid Local Insert payload: expected array length {INSERT_MATERIALIZED_FIELD_COUNT}, got {size}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: InsertMaterializedState::TableId,
        })
    }

    fn get_target_table_id(&mut self) -> Result<u32, ProtocolError> {
        debug_assert_eq!(self.state, InsertMaterializedState::TableId);
        let target_table_id = read_int(&mut self.raw_payload)?;
        self.state = InsertMaterializedState::TableVersion;
        Ok(target_table_id)
    }

    fn get_target_table_version(&mut self) -> Result<u64, ProtocolError> {
        debug_assert_eq!(self.state, InsertMaterializedState::TableVersion);
        let target_table_version = read_int(&mut self.raw_payload)?;
        self.state = InsertMaterializedState::ConflictPolicy;
        Ok(target_table_version)
    }

    fn get_conflict_policy(&mut self) -> Result<ConflictPolicy, ProtocolError> {
        debug_assert_eq!(self.state, InsertMaterializedState::ConflictPolicy);
        let conflict_policy = read_pfix(&mut self.raw_payload)?
            .try_into()
            .map_err(ProtocolError::DecodeError)?;
        self.state = InsertMaterializedState::Columns;
        Ok(conflict_policy)
    }

    fn get_column_types(&mut self) -> Result<MsgpackArrayIterator<'a, ColumnType>, ProtocolError> {
        debug_assert_eq!(self.state, InsertMaterializedState::Columns);
        let size = read_array_len(&mut self.raw_payload)?;
        let start = self.raw_payload.position() as usize;
        for _ in 0..size {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        let payload = &self.raw_payload.get_ref()[start..end];
        self.state = InsertMaterializedState::Builder;
        Ok(MsgpackArrayIterator::new(payload, size, |cur| {
            read_pfix(cur)
                .map_err(ProtocolError::from)?
                .try_into()
                .map_err(ProtocolError::DecodeError)
        }))
    }

    fn get_builder(&mut self) -> Result<&'a [u8], ProtocolError> {
        debug_assert_eq!(self.state, InsertMaterializedState::Builder);
        let start = self.raw_payload.position() as usize;
        let size = read_array_len(&mut self.raw_payload)?;
        for _ in 0..size {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        let payload = &self.raw_payload.get_ref()[start..end];
        self.state = InsertMaterializedState::DqlInfo;
        Ok(payload)
    }

    fn get_dql_info(&mut self) -> Result<DQLPacketPayloadIterator<'a>, ProtocolError> {
        debug_assert_eq!(self.state, InsertMaterializedState::DqlInfo);
        let start = self.raw_payload.position() as usize;
        let data = &self.raw_payload.get_ref()[start..];
        let info = DQLPacketPayloadIterator::new(data)?;
        self.raw_payload
            .seek(SeekFrom::End(0))
            .expect("failed to seek");
        self.state = InsertMaterializedState::End;
        Ok(info)
    }
}

impl<'a> Iterator for InsertMaterializedIterator<'a> {
    type Item = Result<InsertMaterializedResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            InsertMaterializedState::TableId => Some(
                self.get_target_table_id()
                    .map(InsertMaterializedResult::TableId),
            ),
            InsertMaterializedState::TableVersion => Some(
                self.get_target_table_version()
                    .map(InsertMaterializedResult::TableVersion),
            ),
            InsertMaterializedState::ConflictPolicy => Some(
                self.get_conflict_policy()
                    .map(InsertMaterializedResult::ConflictPolicy),
            ),
            InsertMaterializedState::Columns => Some(
                self.get_column_types()
                    .map(InsertMaterializedResult::Columns),
            ),
            InsertMaterializedState::Builder => {
                Some(self.get_builder().map(InsertMaterializedResult::Builder))
            }
            InsertMaterializedState::DqlInfo => {
                Some(self.get_dql_info().map(InsertMaterializedResult::DqlInfo))
            }
            InsertMaterializedState::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = INSERT_MATERIALIZED_FIELD_COUNT.saturating_sub(self.state as usize);
        (len, Some(len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dql::DQLResult;
    use crate::dql_encoder::test::{TestDQLDataSource, TestDQLEncoderBuilder};
    use crate::message_type::MessageType;
    use rmp::decode::read_str_len;

    use crate::iterators::TestPureTupleEncoder;
    use std::collections::HashMap;
    use std::str::from_utf8;

    struct TestInsertEncoder {
        request_id: String,
        table_id: u32,
        table_version: u64,
        conflict_policy: ConflictPolicy,

        // Without DQL
        tuples: Vec<Vec<u64>>,

        // With DQL
        columns: Vec<ColumnType>,
        builder: Vec<u64>,
        dql_encoder: Option<TestDQLDataSource>,
    }

    impl CoreInsertDataSource for TestInsertEncoder {
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
    }

    impl InsertDataSource for TestInsertEncoder {
        fn get_tuples(&self) -> impl ExactSizeIterator<Item = impl MsgpackEncode> {
            self.tuples.iter().map(TestPureTupleEncoder::new)
        }
    }

    impl InsertMaterializedDataSource for TestInsertEncoder {
        fn get_column_types(&self) -> impl ExactSizeIterator<Item = ColumnType> {
            self.columns.iter().copied()
        }

        fn get_builder(&self) -> impl MsgpackEncode {
            TestPureTupleEncoder::new(&self.builder)
        }

        fn get_dql_data_source(&self) -> &impl DQLDataSource {
            self.dql_encoder.as_ref().unwrap()
        }
    }

    #[test]
    fn test_encode_insert() {
        let encoder = TestInsertEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            conflict_policy: ConflictPolicy::DoNothing,
            tuples: vec![vec![1, 2], vec![3, 4]],

            columns: vec![],
            builder: vec![],
            dql_encoder: None,
        };

        let expected: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x01\x92\x00\x94\xcc\x80\x01\x01\x92\xc4\x03\x92\x01\x02\xc4\x03\x92\x03\x04";
        let mut actual = Vec::new();

        write_insert_packet(&mut actual, &encoder).unwrap();

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_decode_insert() {
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

        let package = InsertIterator::new(data).unwrap();

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
    fn test_encode_insert_materialized() {
        let dql_encoder = TestDQLEncoderBuilder::new()
            .set_plan_id(14235593344027757343)
            .set_schema_info((HashMap::from([(12, 138)]), HashMap::from([([12, 12], 138)])))
            .set_sender_id(42)
            .set_vtables(HashMap::from([(
                "TMP_1302_".to_string(),
                vec![vec![1, 2, 3], vec![3, 2, 1]],
            )]))
            .set_options(crate::dql_encoder::DQLOptions {
                sql_motion_row_max: 123,
                sql_vdbe_opcode_max: 456,
            })
            .set_params(vec![138, 123, 432])
            .build();
        let encoder = TestInsertEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            conflict_policy: ConflictPolicy::DoNothing,
            columns: vec![ColumnType::Integer, ColumnType::Datetime],
            builder: vec![12, 14], // some commands
            dql_encoder: Some(dql_encoder),

            tuples: vec![],
        };

        let expected: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x02\x92\x00\x96\xcc\x80\x01\x01\x92\x05\x02\x92\x0c\x0e\x97\x81\x0c\xcc\x8a\x81\x92\x0c\x0c\xcc\x8a\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f*\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";
        let mut actual = Vec::new();

        write_insert_materialized_packet(&mut actual, &encoder).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_insert_materialized() {
        let mut data: &[u8] = b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x02\x92\x00\x96\xcc\x80\x01\x01\x92\x05\x02\x92\x0c\x0e\x97\x81\x0c\xcc\x8a\x81\x92\x0c\x0c\xcc\x8a\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f*\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";

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

        let package = InsertMaterializedIterator::new(data).unwrap();

        for elem in package {
            match elem.unwrap() {
                InsertMaterializedResult::TableId(table_id) => {
                    assert_eq!(table_id, 128);
                }
                InsertMaterializedResult::TableVersion(version) => {
                    assert_eq!(version, 1);
                }
                InsertMaterializedResult::ConflictPolicy(conflict_policy) => {
                    assert_eq!(conflict_policy, ConflictPolicy::DoNothing);
                }
                InsertMaterializedResult::Columns(columns) => {
                    let columns = columns.map(|x| x.unwrap()).collect::<Vec<_>>();
                    assert_eq!(columns, vec![ColumnType::Integer, ColumnType::Datetime]);
                }
                InsertMaterializedResult::Builder(actual) => {
                    assert_eq!(actual, vec![146, 12, 14]);
                }
                InsertMaterializedResult::DqlInfo(dql_info) => {
                    for elem in dql_info {
                        match elem.unwrap() {
                            DQLResult::TableSchemaInfo(schema_info) => {
                                assert_eq!(schema_info.len(), 1);
                                for res in schema_info {
                                    let (t_id, ver) = res.unwrap();
                                    assert_eq!(t_id, 12);
                                    assert_eq!(ver, 138);
                                }
                            }
                            DQLResult::IndexSchemaInfo(schema_info) => {
                                assert_eq!(schema_info.len(), 1);
                                for res in schema_info {
                                    let (t_id, ver) = res.unwrap();
                                    assert_eq!(t_id, [12, 12]);
                                    assert_eq!(ver, 138);
                                }
                            }
                            DQLResult::PlanId(plan_id) => {
                                assert_eq!(plan_id, 14235593344027757343);
                            }
                            DQLResult::SenderId(sender_id) => {
                                assert_eq!(sender_id, 42);
                            }
                            DQLResult::Vtables(vtables) => {
                                assert_eq!(vtables.len(), 1);
                                for res in vtables {
                                    let (name, tuples) = res.unwrap();
                                    assert_eq!(name, "TMP_1302_");
                                    assert_eq!(tuples.len(), 2);
                                    let mut actual = Vec::with_capacity(2);
                                    for tuple in tuples {
                                        let tuple = tuple.unwrap();
                                        actual.push(tuple);
                                    }
                                }
                            }
                            DQLResult::Options(options) => {
                                assert_eq!(options.sql_motion_row_max, 123);
                                assert_eq!(options.sql_vdbe_opcode_max, 456);
                            }
                            DQLResult::Params(params) => {
                                assert_eq!(params, vec![147, 204, 138, 123, 205, 1, 176]);
                            }
                        }
                    }
                }
            }
        }
    }
}
