use crate::dml::dml_type::DMLType::Update;
use crate::dml::dml_type::{write_dml_header, write_dml_with_sql_header};
use crate::dql::{write_dql_packet_data, write_tuples, DQLPacketPayloadIterator};
use crate::dql_encoder::{ColumnType, DQLDataSource, MsgpackEncode};
use crate::error::ProtocolError;
use crate::iterators::{MsgpackArrayIterator, TupleIterator};
use crate::msgpack::skip_value;
use rmp::decode::{read_array_len, read_int, read_pfix};
use rmp::encode::{write_array_len, write_pfix, write_uint};
use std::cmp::PartialEq;
use std::fmt::{Display, Formatter};
use std::io::{Cursor, Seek, SeekFrom};

pub trait CoreUpdateDataSource {
    fn get_request_id(&self) -> &str;
    fn get_target_table_id(&self) -> u32;
    fn get_target_table_version(&self) -> u64;
    fn get_update_type(&self) -> UpdateType;
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
pub enum UpdateType {
    Shared = 0,
    Local,
}

impl Display for UpdateType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateType::Shared => write!(f, "Shared"),
            UpdateType::Local => write!(f, "Local"),
        }
    }
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

pub trait UpdateSharedKeyDataSource: CoreUpdateDataSource {
    fn get_del_tuples(&self) -> impl ExactSizeIterator<Item = impl MsgpackEncode>;

    fn get_tuples(&self) -> impl ExactSizeIterator<Item = impl MsgpackEncode>;
}

pub fn write_update_shared_key_packet(
    w: &mut impl std::io::Write,
    data: &impl UpdateSharedKeyDataSource,
) -> Result<(), std::io::Error> {
    write_dml_header(w, Update, data.get_request_id())?;
    write_array_len(w, UPDATE_SHARED_KEY_FIELD_COUNT as u32)?;

    write_uint(w, data.get_target_table_id() as u64)?;
    write_uint(w, data.get_target_table_version())?;
    write_pfix(w, data.get_update_type() as u8)?;

    write_tuples(w, data.get_del_tuples())?;
    write_tuples(w, data.get_tuples())?;

    Ok(())
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
enum UpdateSharedKeyState {
    TableId = 0,
    TableVersion,
    UpdateType,
    DelTuples,
    Tuples,
    End,
}

pub enum UpdateSharedKeyResult<'a> {
    TableId(u32),
    TableVersion(u64),
    UpdateType(UpdateType),
    DelTuples(TupleIterator<'a>),
    Tuples(TupleIterator<'a>),
}

impl Display for UpdateSharedKeyResult<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateSharedKeyResult::TableId(_) => f.write_str("TableId"),
            UpdateSharedKeyResult::TableVersion(_) => f.write_str("TableVersion"),
            UpdateSharedKeyResult::UpdateType(_) => f.write_str("UpdateType"),
            UpdateSharedKeyResult::DelTuples(_) => f.write_str("DelTuples"),
            UpdateSharedKeyResult::Tuples(_) => f.write_str("Tuples"),
        }
    }
}

const UPDATE_SHARED_KEY_FIELD_COUNT: usize = 5;
pub struct UpdateSharedKeyIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: UpdateSharedKeyState,
}

impl<'a> UpdateSharedKeyIterator<'a> {
    pub fn new(payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(payload);

        let size = read_array_len(&mut cursor)?;
        if size != UPDATE_SHARED_KEY_FIELD_COUNT as u32 {
            return Err(ProtocolError::DecodeError(format!(
                "Invalid Shared Update payload: expected array length {UPDATE_SHARED_KEY_FIELD_COUNT}, got {size}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: UpdateSharedKeyState::TableId,
        })
    }

    fn get_target_table_id(&mut self) -> Result<u32, ProtocolError> {
        debug_assert_eq!(self.state, UpdateSharedKeyState::TableId);
        let target_table_id = read_int(&mut self.raw_payload)?;
        self.state = UpdateSharedKeyState::TableVersion;
        Ok(target_table_id)
    }

    fn get_target_table_version(&mut self) -> Result<u64, ProtocolError> {
        debug_assert_eq!(self.state, UpdateSharedKeyState::TableVersion);
        let target_table_version = read_int(&mut self.raw_payload)?;
        self.state = UpdateSharedKeyState::UpdateType;
        Ok(target_table_version)
    }

    fn get_update_type(&mut self) -> Result<UpdateType, ProtocolError> {
        debug_assert_eq!(self.state, UpdateSharedKeyState::UpdateType);
        let update_type = rmp::decode::read_pfix(&mut self.raw_payload)?
            .try_into()
            .map_err(ProtocolError::DecodeError)?;
        self.state = UpdateSharedKeyState::DelTuples;
        Ok(update_type)
    }

    fn get_del_tuples(&mut self) -> Result<TupleIterator<'a>, ProtocolError> {
        debug_assert_eq!(self.state, UpdateSharedKeyState::DelTuples);
        let rows = read_array_len(&mut self.raw_payload)? as usize;
        let start = self.raw_payload.position() as usize;
        for _ in 0..rows {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;

        let payload = &self.raw_payload.get_ref()[start..end];
        let tuples = TupleIterator::new(payload, rows);

        self.state = UpdateSharedKeyState::Tuples;
        Ok(tuples)
    }

    fn get_tuples(&mut self) -> Result<TupleIterator<'a>, ProtocolError> {
        debug_assert_eq!(self.state, UpdateSharedKeyState::Tuples);
        let rows = read_array_len(&mut self.raw_payload)? as usize;
        let start = self.raw_payload.position() as usize;
        for _ in 0..rows {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;

        let payload = &self.raw_payload.get_ref()[start..end];
        let tuples = TupleIterator::new(payload, rows);

        self.state = UpdateSharedKeyState::End;
        Ok(tuples)
    }
}

impl<'a> Iterator for UpdateSharedKeyIterator<'a> {
    type Item = Result<UpdateSharedKeyResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            UpdateSharedKeyState::TableId => Some(
                self.get_target_table_id()
                    .map(UpdateSharedKeyResult::TableId),
            ),
            UpdateSharedKeyState::TableVersion => Some(
                self.get_target_table_version()
                    .map(UpdateSharedKeyResult::TableVersion),
            ),
            UpdateSharedKeyState::UpdateType => Some(
                self.get_update_type()
                    .map(UpdateSharedKeyResult::UpdateType),
            ),
            UpdateSharedKeyState::DelTuples => {
                Some(self.get_del_tuples().map(UpdateSharedKeyResult::DelTuples))
            }
            UpdateSharedKeyState::Tuples => {
                Some(self.get_tuples().map(UpdateSharedKeyResult::Tuples))
            }
            UpdateSharedKeyState::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = UPDATE_SHARED_KEY_FIELD_COUNT - self.state as usize;
        (len, Some(len))
    }
}

pub trait UpdateDataSource: CoreUpdateDataSource {
    fn get_column_types(&self) -> impl ExactSizeIterator<Item = ColumnType>;
    fn get_builder(&self) -> impl MsgpackEncode;
    fn get_dql_data_source(&self) -> &impl DQLDataSource;
}

pub fn write_update_packet(
    w: &mut impl std::io::Write,
    data: &impl UpdateDataSource,
) -> Result<(), std::io::Error> {
    write_dml_with_sql_header(w, Update, CoreUpdateDataSource::get_request_id(data))?;
    write_array_len(w, UPDATE_FIELD_COUNT as u32)?;
    write_uint(w, data.get_target_table_id() as u64)?;
    write_uint(w, data.get_target_table_version())?;

    let update_type = data.get_update_type();
    if update_type != UpdateType::Local {
        return Err(std::io::Error::other(format!(
            "cannot construct update with sql with non local update: {update_type}"
        )));
    }

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
enum UpdateState {
    TableId = 0,
    TableVersion,
    Columns,
    DqlInfo,
    Builder,
    End,
}

pub enum UpdateResult<'a> {
    TableId(u32),
    TableVersion(u64),
    Columns(MsgpackArrayIterator<'a, ColumnType>),
    Builder(&'a [u8]),
    DqlInfo(DQLPacketPayloadIterator<'a>),
}

impl Display for UpdateResult<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateResult::TableId(_) => f.write_str("TableId"),
            UpdateResult::TableVersion(_) => f.write_str("TableVersion"),
            UpdateResult::Columns(_) => f.write_str("Columns"),
            UpdateResult::Builder(_) => f.write_str("Builder"),
            UpdateResult::DqlInfo(_) => f.write_str("DqlInfo"),
        }
    }
}

const UPDATE_FIELD_COUNT: usize = 5;
pub struct UpdateIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: UpdateState,
}

impl<'a> UpdateIterator<'a> {
    pub fn new(payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(payload);

        let size = read_array_len(&mut cursor)?;
        if size != UPDATE_FIELD_COUNT as u32 {
            return Err(ProtocolError::DecodeError(format!(
                "Invalid Local Update payload: expected array length {UPDATE_FIELD_COUNT}, got {size}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: UpdateState::TableId,
        })
    }

    fn get_target_table_id(&mut self) -> Result<u32, ProtocolError> {
        debug_assert_eq!(self.state, UpdateState::TableId);
        let target_table_id = read_int(&mut self.raw_payload)?;
        self.state = UpdateState::TableVersion;
        Ok(target_table_id)
    }

    fn get_target_table_version(&mut self) -> Result<u64, ProtocolError> {
        debug_assert_eq!(self.state, UpdateState::TableVersion);
        let target_table_version = read_int(&mut self.raw_payload)?;
        self.state = UpdateState::Columns;
        Ok(target_table_version)
    }

    fn get_column_types(&mut self) -> Result<MsgpackArrayIterator<'a, ColumnType>, ProtocolError> {
        debug_assert_eq!(self.state, UpdateState::Columns);
        let size = read_array_len(&mut self.raw_payload)?;
        let start = self.raw_payload.position() as usize;
        for _ in 0..size {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        let payload = &self.raw_payload.get_ref()[start..end];
        self.state = UpdateState::Builder;
        Ok(MsgpackArrayIterator::new(payload, size, |cur| {
            read_pfix(cur)
                .map_err(ProtocolError::from)?
                .try_into()
                .map_err(ProtocolError::DecodeError)
        }))
    }

    fn get_builder(&mut self) -> Result<&'a [u8], ProtocolError> {
        debug_assert_eq!(self.state, UpdateState::Builder);
        let start = self.raw_payload.position() as usize;
        let size = read_array_len(&mut self.raw_payload)?;
        for _ in 0..size {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        let payload = &self.raw_payload.get_ref()[start..end];
        self.state = UpdateState::DqlInfo;
        Ok(payload)
    }

    fn get_dql_info(&mut self) -> Result<DQLPacketPayloadIterator<'a>, ProtocolError> {
        debug_assert_eq!(self.state, UpdateState::DqlInfo);
        let start = self.raw_payload.position() as usize;
        let data = &self.raw_payload.get_ref()[start..];
        let info = DQLPacketPayloadIterator::new(data)?;
        self.raw_payload
            .seek(SeekFrom::End(0))
            .expect("failed to seek");
        self.state = UpdateState::End;
        Ok(info)
    }
}

impl<'a> Iterator for UpdateIterator<'a> {
    type Item = Result<UpdateResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            UpdateState::TableId => Some(self.get_target_table_id().map(UpdateResult::TableId)),
            UpdateState::TableVersion => Some(
                self.get_target_table_version()
                    .map(UpdateResult::TableVersion),
            ),
            UpdateState::Columns => Some(self.get_column_types().map(UpdateResult::Columns)),
            UpdateState::Builder => Some(self.get_builder().map(UpdateResult::Builder)),
            UpdateState::DqlInfo => Some(self.get_dql_info().map(UpdateResult::DqlInfo)),
            UpdateState::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = UPDATE_FIELD_COUNT - self.state as usize;
        (len, Some(len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dql::DQLResult;
    use crate::dql_encoder::test::{TestDQLDataSource, TestDQLEncoderBuilder};
    use crate::iterators::TestPureTupleEncoder;
    use crate::message_type::MessageType;
    use rmp::decode::read_str_len;
    use std::collections::HashMap;
    use std::str::from_utf8;

    struct TestUpdateEncoder {
        request_id: String,
        table_id: u32,
        table_version: u64,
        update_type: UpdateType,

        // Shared
        del_tuples: Vec<Vec<u64>>,
        tuples: Vec<Vec<u64>>,

        // Local
        columns: Vec<ColumnType>,
        builder: Vec<u64>,
        dql_encoder: Option<TestDQLDataSource>,
    }

    impl CoreUpdateDataSource for TestUpdateEncoder {
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
    }

    impl UpdateSharedKeyDataSource for TestUpdateEncoder {
        fn get_del_tuples(&self) -> impl ExactSizeIterator<Item = impl MsgpackEncode> {
            self.del_tuples.iter().map(TestPureTupleEncoder::new)
        }

        fn get_tuples(&self) -> impl ExactSizeIterator<Item = impl MsgpackEncode> {
            self.tuples.iter().map(TestPureTupleEncoder::new)
        }
    }

    impl UpdateDataSource for TestUpdateEncoder {
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
    fn test_encode_update_shared_key() {
        let encoder = TestUpdateEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            update_type: UpdateType::Shared,
            del_tuples: vec![vec![1], vec![3]],
            tuples: vec![vec![1, 2], vec![3, 4]],

            columns: vec![],
            builder: vec![],
            dql_encoder: None,
        };

        let expected: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x01\x92\x01\x95\xcc\x80\x01\x00\x92\xc4\x02\x91\x01\xc4\x02\x91\x03\x92\xc4\x03\x92\x01\x02\xc4\x03\x92\x03\x04";
        let mut actual = Vec::new();

        write_update_shared_key_packet(&mut actual, &encoder).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_update_shared_key() {
        let mut data: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x01\x92\x01\x95\xcc\x80\x01\x00\x92\xc4\x02\x91\x01\xc4\x02\x91\x03\x92\xc4\x03\x92\x01\x02\xc4\x03\x92\x03\x04";

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

        let iterator = UpdateSharedKeyIterator::new(data).unwrap();

        for elem in iterator {
            match elem.unwrap() {
                UpdateSharedKeyResult::TableId(table_id) => {
                    assert_eq!(table_id, 128);
                }
                UpdateSharedKeyResult::TableVersion(version) => {
                    assert_eq!(version, 1);
                }
                UpdateSharedKeyResult::UpdateType(update_type) => {
                    assert_eq!(update_type, UpdateType::Shared);
                }
                UpdateSharedKeyResult::DelTuples(tuples) => {
                    assert_eq!(tuples.len(), 2);
                    let mut actual = Vec::with_capacity(2);
                    for tuple in tuples {
                        let tuple = tuple.unwrap();
                        actual.push(tuple);
                    }
                    let expected = vec![[145, 1], [145, 3]];
                    assert_eq!(actual, expected);
                }
                UpdateSharedKeyResult::Tuples(tuples) => {
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
    fn test_encode_update() {
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
                read_preference: 0,
            })
            .set_params(vec![138, 123, 432])
            .build();
        let encoder = TestUpdateEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            update_type: UpdateType::Local,
            columns: vec![ColumnType::Integer],
            builder: vec![138], // some command
            dql_encoder: Some(dql_encoder),

            del_tuples: vec![],
            tuples: vec![],
        };

        let expected: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x02\x92\x01\x95\xcc\x80\x01\x91\x05\x91\xcc\x8a\x97\x81\x0c\xcc\x8a\x81\x92\x0c\x0c\xcc\x8a\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f*\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x93{\xcd\x01\xc8\x00\x93\xcc\x8a{\xcd\x01\xb0";
        let mut actual = Vec::new();

        write_update_packet(&mut actual, &encoder).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_encode_update_but_update_shared() {
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
                read_preference: 0,
            })
            .set_params(vec![138, 123, 432])
            .build();
        let encoder = TestUpdateEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            update_type: UpdateType::Shared,
            columns: vec![ColumnType::Integer],
            builder: vec![138], // some command
            dql_encoder: Some(dql_encoder),

            del_tuples: vec![],
            tuples: vec![],
        };

        let expected = "cannot construct update with sql with non local update: Shared";

        let mut actual = Vec::new();
        let actual = write_update_packet(&mut actual, &encoder).err().unwrap();

        assert_eq!(actual.to_string(), expected);
    }

    #[test]
    fn test_decode_update() {
        let mut data: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x02\x92\x01\x95\xcc\x80\x01\x91\x05\x91\xcc\x8a\x97\x81\x0c\xcc\x8a\x81\x92\x0c\x0c\xcc\x8a\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f*\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x93{\xcd\x01\xc8\x00\x93\xcc\x8a{\xcd\x01\xb0";

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
        assert_eq!(dml_type, Update as u8);

        let iterator = UpdateIterator::new(data).unwrap();

        for elem in iterator {
            match elem.unwrap() {
                UpdateResult::TableId(table_id) => {
                    assert_eq!(table_id, 128);
                }
                UpdateResult::TableVersion(version) => {
                    assert_eq!(version, 1);
                }
                UpdateResult::Columns(columns) => {
                    let actual = columns.map(Result::unwrap).collect::<Vec<_>>();
                    let expected = vec![ColumnType::Integer];
                    assert_eq!(actual, expected);
                }
                UpdateResult::Builder(actual) => {
                    let expected = vec![145, 204, 138];
                    assert_eq!(actual, expected);
                }
                UpdateResult::DqlInfo(info) => {
                    for res in info {
                        let res = res.unwrap();

                        match res {
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
                                assert_eq!(options.read_preference, 0);
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
