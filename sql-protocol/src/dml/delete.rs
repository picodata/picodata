use crate::dml::dml_type::DMLType::Delete;
use crate::dml::dml_type::{write_dml_header, write_dml_with_sql_header};
use crate::dql::{get_options, write_dql_packet_data, write_options, DQLPacketPayloadIterator};
use crate::dql_encoder::{ColumnType, DQLDataSource, MsgpackEncode};
use crate::error::ProtocolError;
use crate::iterators::MsgpackArrayIterator;
use crate::msgpack::skip_value;
use rmp::decode::{read_array_len, read_int, read_pfix};
use rmp::encode::{write_array_len, write_pfix, write_uint};
use std::cmp::PartialEq;
use std::fmt;
use std::fmt::Formatter;
use std::io::{Cursor, Seek, SeekFrom};

pub trait CoreDeleteDataSource {
    fn get_request_id(&self) -> &str;
    fn get_target_table_id(&self) -> u32;
    fn get_target_table_version(&self) -> u64;
}

pub trait DeleteFullDataSource: CoreDeleteDataSource {
    fn get_plan_id(&self) -> u64;
    fn get_options(&self) -> [u64; 2];
}

pub fn write_delete_full_packet(
    w: &mut impl std::io::Write,
    data: &impl DeleteFullDataSource,
) -> Result<(), std::io::Error> {
    write_dml_header(w, Delete, data.get_request_id())?;

    write_array_len(w, DELETE_FULL_FIELD_COUNT as u32)?;

    write_uint(w, data.get_target_table_id() as u64)?;
    write_uint(w, data.get_target_table_version())?;
    write_uint(w, data.get_plan_id())?;
    write_options(w, data.get_options().iter())?;

    Ok(())
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
enum DeleteFullState {
    TableId = 0,
    TableVersion,
    PlanId,
    Options,
    End,
}

pub enum DeleteFullResult {
    TableId(u32),
    TableVersion(u64),
    PlanId(u64),
    Options((u64, u64)),
}

impl fmt::Display for DeleteFullResult {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            DeleteFullResult::TableId(_) => f.write_str("TableId"),
            DeleteFullResult::TableVersion(_) => f.write_str("TableVersion"),
            DeleteFullResult::PlanId(_) => f.write_str("PlanId"),
            DeleteFullResult::Options(_) => f.write_str("Options"),
        }
    }
}

const DELETE_FULL_FIELD_COUNT: usize = 4;
pub struct DeleteFullIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: DeleteFullState,
}

impl<'a> DeleteFullIterator<'a> {
    pub fn new(payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(payload);

        let len = read_array_len(&mut cursor)? as usize;
        if len != DELETE_FULL_FIELD_COUNT {
            return Err(ProtocolError::DecodeError(format!(
                "Invalid Full Delete payload: expected array length {DELETE_FULL_FIELD_COUNT}, got {len}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: DeleteFullState::TableId,
        })
    }

    fn get_target_table_id(&mut self) -> Result<u32, ProtocolError> {
        debug_assert_eq!(self.state, DeleteFullState::TableId);
        let target_table_id = read_int(&mut self.raw_payload)?;
        self.state = DeleteFullState::TableVersion;
        Ok(target_table_id)
    }

    fn get_target_table_version(&mut self) -> Result<u64, ProtocolError> {
        debug_assert_eq!(self.state, DeleteFullState::TableVersion);
        let target_table_version = read_int(&mut self.raw_payload)?;
        self.state = DeleteFullState::PlanId;
        Ok(target_table_version)
    }

    fn get_plan_id(&mut self) -> Result<u64, ProtocolError> {
        debug_assert_eq!(self.state, DeleteFullState::PlanId);
        let plan_id = read_int(&mut self.raw_payload)?;
        self.state = DeleteFullState::Options;
        Ok(plan_id)
    }
    fn get_options(&mut self) -> Result<(u64, u64), ProtocolError> {
        debug_assert_eq!(self.state, DeleteFullState::Options);
        let options = get_options(&mut self.raw_payload)?;
        self.state = DeleteFullState::End;
        Ok(options)
    }
}

impl Iterator for DeleteFullIterator<'_> {
    type Item = Result<DeleteFullResult, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            DeleteFullState::TableId => {
                Some(self.get_target_table_id().map(DeleteFullResult::TableId))
            }
            DeleteFullState::TableVersion => Some(
                self.get_target_table_version()
                    .map(DeleteFullResult::TableVersion),
            ),
            DeleteFullState::PlanId => Some(self.get_plan_id().map(DeleteFullResult::PlanId)),
            DeleteFullState::Options => Some(self.get_options().map(DeleteFullResult::Options)),
            DeleteFullState::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = DELETE_FULL_FIELD_COUNT.saturating_sub(self.state as usize);
        (len, Some(len))
    }
}

pub trait DeleteFilteredDataSource: CoreDeleteDataSource {
    fn get_column_types(&self) -> impl ExactSizeIterator<Item = ColumnType>;
    fn get_builder(&self) -> impl MsgpackEncode;
    fn get_dql_data_source(&self) -> &impl DQLDataSource;
}

pub fn write_delete_filtered_packet(
    w: &mut impl std::io::Write,
    data: &impl DeleteFilteredDataSource,
) -> Result<(), std::io::Error> {
    write_dml_with_sql_header(w, Delete, CoreDeleteDataSource::get_request_id(data))?;
    write_array_len(w, DELETE_FILTERED_FIELD_COUNT as u32)?;
    write_uint(w, data.get_target_table_id() as u64)?;
    write_uint(w, data.get_target_table_version())?;

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
enum DeleteFilteredState {
    TableId = 0,
    TableVersion,
    Columns,
    DqlInfo,
    Builder,
    End,
}

pub enum DeleteFilteredResult<'a> {
    TableId(u32),
    TableVersion(u64),
    Columns(MsgpackArrayIterator<'a, ColumnType>),
    Builder(&'a [u8]),
    DqlInfo(DQLPacketPayloadIterator<'a>),
}

impl fmt::Display for DeleteFilteredResult<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DeleteFilteredResult::TableId(_) => f.write_str("TableId"),
            DeleteFilteredResult::TableVersion(_) => f.write_str("TableVersion"),
            DeleteFilteredResult::Columns(_) => f.write_str("Columns"),
            DeleteFilteredResult::Builder(_) => f.write_str("Builder"),
            DeleteFilteredResult::DqlInfo(_) => f.write_str("DqlInfo"),
        }
    }
}

const DELETE_FILTERED_FIELD_COUNT: usize = 5;
pub struct DeleteFilteredIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: DeleteFilteredState,
}

impl<'a> DeleteFilteredIterator<'a> {
    pub fn new(payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(payload);

        let size = read_array_len(&mut cursor)? as usize;
        if size != DELETE_FILTERED_FIELD_COUNT {
            return Err(ProtocolError::DecodeError(format!(
                "Invalid Filtered Delete payload: expected array length {DELETE_FILTERED_FIELD_COUNT}, got {size}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: DeleteFilteredState::TableId,
        })
    }

    fn get_target_table_id(&mut self) -> Result<u32, ProtocolError> {
        debug_assert_eq!(self.state, DeleteFilteredState::TableId);
        let target_table_id = read_int(&mut self.raw_payload)?;
        self.state = DeleteFilteredState::TableVersion;
        Ok(target_table_id)
    }

    fn get_target_table_version(&mut self) -> Result<u64, ProtocolError> {
        debug_assert_eq!(self.state, DeleteFilteredState::TableVersion);
        let target_table_version = read_int(&mut self.raw_payload)?;
        self.state = DeleteFilteredState::Columns;
        Ok(target_table_version)
    }

    fn get_column_types(&mut self) -> Result<MsgpackArrayIterator<'a, ColumnType>, ProtocolError> {
        debug_assert_eq!(self.state, DeleteFilteredState::Columns);
        let size = read_array_len(&mut self.raw_payload)?;
        let start = self.raw_payload.position() as usize;
        for _ in 0..size {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        let payload = &self.raw_payload.get_ref()[start..end];
        self.state = DeleteFilteredState::Builder;
        Ok(MsgpackArrayIterator::new(payload, size, |cur| {
            read_pfix(cur)
                .map_err(ProtocolError::from)?
                .try_into()
                .map_err(ProtocolError::DecodeError)
        }))
    }

    fn get_builder(&mut self) -> Result<&'a [u8], ProtocolError> {
        debug_assert_eq!(self.state, DeleteFilteredState::Builder);
        let start = self.raw_payload.position() as usize;
        let size = read_array_len(&mut self.raw_payload)?;
        for _ in 0..size {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        let payload = &self.raw_payload.get_ref()[start..end];
        self.state = DeleteFilteredState::DqlInfo;
        Ok(payload)
    }

    fn get_dql_info(&mut self) -> Result<DQLPacketPayloadIterator<'a>, ProtocolError> {
        debug_assert_eq!(self.state, DeleteFilteredState::DqlInfo);
        let start = self.raw_payload.position() as usize;
        let data = &self.raw_payload.get_ref()[start..];
        let info = DQLPacketPayloadIterator::new(data)?;
        self.raw_payload
            .seek(SeekFrom::End(0))
            .expect("failed to seek");
        self.state = DeleteFilteredState::End;
        Ok(info)
    }
}

impl<'a> Iterator for DeleteFilteredIterator<'a> {
    type Item = Result<DeleteFilteredResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            DeleteFilteredState::TableId => Some(
                self.get_target_table_id()
                    .map(DeleteFilteredResult::TableId),
            ),
            DeleteFilteredState::TableVersion => Some(
                self.get_target_table_version()
                    .map(DeleteFilteredResult::TableVersion),
            ),
            DeleteFilteredState::Columns => {
                Some(self.get_column_types().map(DeleteFilteredResult::Columns))
            }
            DeleteFilteredState::Builder => {
                Some(self.get_builder().map(DeleteFilteredResult::Builder))
            }
            DeleteFilteredState::DqlInfo => {
                Some(self.get_dql_info().map(DeleteFilteredResult::DqlInfo))
            }
            DeleteFilteredState::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = DELETE_FILTERED_FIELD_COUNT.saturating_sub(self.state as usize);
        (size, Some(size))
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

    struct TestDeleteEncoder {
        request_id: String,
        table_id: u32,
        table_version: u64,

        // Full delete
        plan_id: u64,
        options: (u64, u64),

        // Local delete
        columns: Vec<ColumnType>,
        builder: Vec<u64>,
        dql_encoder: Option<TestDQLDataSource>,
    }

    impl CoreDeleteDataSource for TestDeleteEncoder {
        fn get_request_id(&self) -> &str {
            self.request_id.as_str()
        }

        fn get_target_table_id(&self) -> u32 {
            self.table_id
        }

        fn get_target_table_version(&self) -> u64 {
            self.table_version
        }
    }

    impl DeleteFullDataSource for TestDeleteEncoder {
        fn get_plan_id(&self) -> u64 {
            self.plan_id
        }
        fn get_options(&self) -> [u64; 2] {
            [self.options.0, self.options.1]
        }
    }

    impl DeleteFilteredDataSource for TestDeleteEncoder {
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
    fn test_encode_delete_full() {
        let encoder = TestDeleteEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_version: 1,
            table_id: 128,
            plan_id: 14235593344027757343,
            options: (123, 456),

            columns: vec![],
            builder: vec![],
            dql_encoder: None,
        };

        let expected: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x01\x92\x02\x94\xcc\x80\x01\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f\x92{\xcd\x01\xc8";
        let mut actual = Vec::new();

        write_delete_full_packet(&mut actual, &encoder).unwrap();

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_decode_delete_full() {
        let mut data: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x01\x92\x02\x94\xcc\x80\x01\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f\x92{\xcd\x01\xc8";

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

        let iterator = DeleteFullIterator::new(data).unwrap();

        for elem in iterator {
            match elem.unwrap() {
                DeleteFullResult::TableId(table_id) => {
                    assert_eq!(table_id, 128);
                }
                DeleteFullResult::TableVersion(version) => {
                    assert_eq!(version, 1);
                }
                DeleteFullResult::PlanId(plan_id) => {
                    assert_eq!(plan_id, 14235593344027757343);
                }
                DeleteFullResult::Options((rows, codes)) => {
                    assert_eq!(rows, 123);
                    assert_eq!(codes, 456);
                }
            }
        }
    }

    #[test]
    fn test_encode_delete_filtered() {
        let dql_encoder = TestDQLEncoderBuilder::new()
            .set_plan_id(14235593344027757343)
            .set_schema_info((HashMap::from([(12, 138)]), HashMap::from([([12, 12], 138)])))
            .set_sender_id(42)
            .set_vtables(HashMap::from([(
                "TMP_1302_".to_string(),
                vec![vec![1, 2, 3], vec![3, 2, 1]],
            )]))
            .set_options([123, 456])
            .set_params(vec![138, 123, 432])
            .build();
        let encoder = TestDeleteEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            columns: vec![ColumnType::Integer],
            builder: vec![12, 14],
            dql_encoder: Some(dql_encoder),

            plan_id: 0,
            options: (0, 0),
        };

        let expected: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x02\x92\x02\x95\xcc\x80\x01\x91\x05\x92\x0c\x0e\x97\x81\x0c\xcc\x8a\x81\x92\x0c\x0c\xcc\x8a\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f*\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";
        let mut actual = Vec::new();

        write_delete_filtered_packet(&mut actual, &encoder).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_delete_filtered() {
        let mut data: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x02\x92\x02\x95\xcc\x80\x01\x91\x05\x92\x0c\x0e\x97\x81\x0c\xcc\x8a\x81\x92\x0c\x0c\xcc\x8a\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f*\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";

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

        let iterator = DeleteFilteredIterator::new(data).unwrap();

        for elem in iterator {
            match elem.unwrap() {
                DeleteFilteredResult::TableId(table_id) => {
                    assert_eq!(table_id, 128);
                }
                DeleteFilteredResult::TableVersion(version) => {
                    assert_eq!(version, 1);
                }
                DeleteFilteredResult::Columns(columns) => {
                    let columns = columns.map(|x| x.unwrap()).collect::<Vec<_>>();
                    assert_eq!(columns, vec![ColumnType::Integer]);
                }
                DeleteFilteredResult::Builder(actual) => {
                    assert_eq!(actual, vec![146, 12, 14]);
                }
                DeleteFilteredResult::DqlInfo(dql_info) => {
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
                                assert_eq!(options, (123, 456));
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
