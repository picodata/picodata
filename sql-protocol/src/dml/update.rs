use crate::dml::dml_type::DMLType::Update;
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
use rmp::decode::{read_array_len, read_int, read_map_len};
use rmp::encode::{write_array_len, write_map_len, write_pfix, write_uint};
use std::cmp::PartialEq;
use std::fmt::{Display, Formatter};
use std::io::Cursor;

pub trait UpdateEncoder {
    fn get_request_id(&self) -> &str;
    fn get_target_table_id(&self) -> u32;
    fn get_target_table_version(&self) -> u64;
    fn get_update_type(&self) -> UpdateType;
    fn get_mapping_columns(&self) -> impl ExactSizeIterator<Item = (&usize, &usize)>;
    fn get_pk_positions(&self) -> impl ExactSizeIterator<Item = &usize>;
    fn get_tuples(&self) -> impl ExactSizeIterator<Item = impl MsgpackEncode>;
}

pub fn write_update_package(
    w: &mut impl std::io::Write,
    data: impl UpdateEncoder,
) -> Result<(), std::io::Error> {
    write_dml_header(w, Update, data.get_request_id())?;
    write_array_len(w, UPDATE_PACKAGE_SIZE as u32)?;

    write_uint(w, data.get_target_table_id() as u64)?;
    write_uint(w, data.get_target_table_version())?;
    write_pfix(w, data.get_update_type() as u8)?;

    write_tuples(w, data.get_tuples())?;

    Ok(())
}

pub fn write_update_with_sql_package(
    w: &mut impl std::io::Write,
    data: impl UpdateEncoder + DQLDataSource,
) -> Result<(), std::io::Error> {
    write_dml_with_sql_header(w, Update, UpdateEncoder::get_request_id(&data))?;
    write_array_len(w, LOCAL_UPDATE_PACKAGE_SIZE as u32)?;
    write_uint(w, data.get_target_table_id() as u64)?;
    write_uint(w, data.get_target_table_version())?;

    let update_type = data.get_update_type();
    if update_type != UpdateType::Local {
        return Err(std::io::Error::other(format!(
            "cannot construct update with sql with non local update: {update_type}"
        )));
    }

    let mapping_columns = data.get_mapping_columns();
    write_map_len(w, mapping_columns.len() as u32)?;
    for (src, dst) in mapping_columns {
        write_uint(w, *src as u64)?;
        write_uint(w, *dst as u64)?;
    }

    let pk_positions = data.get_pk_positions();
    write_array_len(w, pk_positions.len() as u32)?;
    for pos in pk_positions {
        write_uint(w, *pos as u64)?;
    }

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

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
enum LocalUpdateStates {
    TableId = 1,
    TableVersion,
    ColumnMapping,
    PrimaryKey,
    SchemaInfo,
    PlanId,
    SenderId,
    Vtables,
    Options,
    Params,
    End,
}

pub enum LocalUpdateResult<'a> {
    TableId(u64),
    TableVersion(u64),
    ColumnMapping(MsgpackMapIterator<'a, usize, usize>),
    PrimaryKey(MsgpackArrayIterator<'a, usize>),
    SchemaInfo(MsgpackMapIterator<'a, u32, u64>),
    PlanId(u64),
    SenderId(u64),
    Vtables(MsgpackMapIterator<'a, &'a str, TupleIterator<'a>>),
    Options((u64, u64)),
    Params(&'a [u8]),
}
const LOCAL_UPDATE_PACKAGE_SIZE: usize = 10;
pub struct LocalUpdatePackageIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: LocalUpdateStates,
}

impl<'a> LocalUpdatePackageIterator<'a> {
    pub fn new(payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(payload);

        let size = read_array_len(&mut cursor)?;
        if size != LOCAL_UPDATE_PACKAGE_SIZE as u32 {
            return Err(ProtocolError::DecodeError(format!(
                "DML Update with sql package is invalid: expected to have package array length {LOCAL_UPDATE_PACKAGE_SIZE}, got {size}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: LocalUpdateStates::TableId,
        })
    }

    fn get_target_table_id(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, LocalUpdateStates::TableId);
        let target_table_id = read_int(&mut self.raw_payload)?;
        self.state = LocalUpdateStates::TableVersion;
        Ok(target_table_id)
    }

    fn get_target_table_version(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, LocalUpdateStates::TableVersion);
        let target_table_version = read_int(&mut self.raw_payload)?;
        self.state = LocalUpdateStates::ColumnMapping;
        Ok(target_table_version)
    }

    fn get_column_mapping(
        &mut self,
    ) -> Result<MsgpackMapIterator<'a, usize, usize>, ProtocolError> {
        assert_eq!(self.state, LocalUpdateStates::ColumnMapping);
        let size = read_map_len(&mut self.raw_payload)?;
        let start = self.raw_payload.position() as usize;
        for _ in 0..size * 2 {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        let payload = &self.raw_payload.get_ref()[start..end];
        self.state = LocalUpdateStates::PrimaryKey;
        Ok(MsgpackMapIterator::new(
            payload,
            size,
            |cur| read_int(cur).map_err(ProtocolError::from),
            |cur| read_int(cur).map_err(ProtocolError::from),
        ))
    }

    fn get_pk_positions(&mut self) -> Result<MsgpackArrayIterator<'a, usize>, ProtocolError> {
        assert_eq!(self.state, LocalUpdateStates::PrimaryKey);
        let size = read_array_len(&mut self.raw_payload)?;
        let start = self.raw_payload.position() as usize;
        for _ in 0..size {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        let payload = &self.raw_payload.get_ref()[start..end];
        self.state = LocalUpdateStates::SchemaInfo;
        Ok(MsgpackArrayIterator::new(payload, size, |cur| {
            read_int(cur).map_err(ProtocolError::from)
        }))
    }

    fn get_schema_info(&mut self) -> Result<MsgpackMapIterator<'a, u32, u64>, ProtocolError> {
        assert_eq!(self.state, LocalUpdateStates::SchemaInfo);
        let schema_info = get_schema_info(&mut self.raw_payload)?;
        self.state = LocalUpdateStates::PlanId;
        Ok(schema_info)
    }

    fn get_plan_id(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, LocalUpdateStates::PlanId);
        let plan_id = get_plan_id(&mut self.raw_payload)?;
        self.state = LocalUpdateStates::SenderId;
        Ok(plan_id)
    }

    fn get_sender_id(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, LocalUpdateStates::SenderId);
        let sender_id = get_sender_id(&mut self.raw_payload)?;
        self.state = LocalUpdateStates::Vtables;
        Ok(sender_id)
    }

    fn get_vtables(
        &mut self,
    ) -> Result<MsgpackMapIterator<'a, &'a str, TupleIterator<'a>>, ProtocolError> {
        assert_eq!(self.state, LocalUpdateStates::Vtables);
        let vtables = get_vtables(&mut self.raw_payload)?;
        self.state = LocalUpdateStates::Options;
        Ok(vtables)
    }

    fn get_options(&mut self) -> Result<(u64, u64), ProtocolError> {
        assert_eq!(self.state, LocalUpdateStates::Options);
        let options = get_options(&mut self.raw_payload)?;
        self.state = LocalUpdateStates::Params;
        Ok(options)
    }

    fn get_params(&mut self) -> Result<&'a [u8], ProtocolError> {
        assert_eq!(self.state, LocalUpdateStates::Params);
        let params = get_params(&mut self.raw_payload)?;
        self.state = LocalUpdateStates::End;
        Ok(params)
    }
}

impl<'a> Iterator for LocalUpdatePackageIterator<'a> {
    type Item = Result<LocalUpdateResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            LocalUpdateStates::TableId => {
                Some(self.get_target_table_id().map(LocalUpdateResult::TableId))
            }
            LocalUpdateStates::TableVersion => Some(
                self.get_target_table_version()
                    .map(LocalUpdateResult::TableVersion),
            ),
            LocalUpdateStates::ColumnMapping => Some(
                self.get_column_mapping()
                    .map(LocalUpdateResult::ColumnMapping),
            ),
            LocalUpdateStates::PrimaryKey => {
                Some(self.get_pk_positions().map(LocalUpdateResult::PrimaryKey))
            }
            LocalUpdateStates::SchemaInfo => {
                Some(self.get_schema_info().map(LocalUpdateResult::SchemaInfo))
            }
            LocalUpdateStates::PlanId => Some(self.get_plan_id().map(LocalUpdateResult::PlanId)),
            LocalUpdateStates::SenderId => {
                Some(self.get_sender_id().map(LocalUpdateResult::SenderId))
            }
            LocalUpdateStates::Vtables => Some(self.get_vtables().map(LocalUpdateResult::Vtables)),
            LocalUpdateStates::Options => Some(self.get_options().map(LocalUpdateResult::Options)),
            LocalUpdateStates::Params => Some(self.get_params().map(LocalUpdateResult::Params)),
            LocalUpdateStates::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = LOCAL_UPDATE_PACKAGE_SIZE - self.state as usize;
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

    use std::collections::{BTreeMap, HashMap};
    use std::str::from_utf8;

    struct TestUpdateEncoder {
        request_id: String,
        table_id: u32,
        table_version: u64,
        update_type: UpdateType,
        column_mapping: BTreeMap<usize, usize>,
        pk_positions: Vec<usize>,
        tuples: Vec<Vec<u64>>,
        dql_encoder: Option<TestDQLDataSource>,
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

        fn get_mapping_columns(&self) -> impl ExactSizeIterator<Item = (&usize, &usize)> {
            self.column_mapping.iter()
        }

        fn get_pk_positions(&self) -> impl ExactSizeIterator<Item = &usize> {
            self.pk_positions.iter()
        }

        fn get_tuples(&self) -> impl ExactSizeIterator<Item = impl MsgpackEncode> {
            self.tuples.iter().map(TestPureTupleEncoder::new)
        }
    }

    impl DQLDataSource for TestUpdateEncoder {
        fn get_schema_info(&self) -> impl ExactSizeIterator<Item = (u32, u64)> {
            self.dql_encoder.as_ref().unwrap().get_schema_info()
        }

        fn get_plan_id(&self) -> u64 {
            self.dql_encoder.as_ref().unwrap().get_plan_id()
        }

        fn get_sender_id(&self) -> u64 {
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
    fn test_encode_update() {
        let encoder = TestUpdateEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            update_type: UpdateType::Shared,
            column_mapping: BTreeMap::new(),
            pk_positions: Vec::new(),
            tuples: vec![vec![1, 2], vec![3, 4]],
            dql_encoder: None,
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

    #[test]
    fn test_encode_update_with_sql() {
        let dql_encoder = TestDQLEncoderBuilder::new()
            .set_plan_id(14235593344027757343)
            .set_schema_info(HashMap::from([(12, 138)]))
            .set_sender_id(42)
            .set_vtables(HashMap::from([(
                "TMP_1302_".to_string(),
                vec![vec![1, 2, 3], vec![3, 2, 1]],
            )]))
            .set_options([123, 456])
            .set_params(vec![138, 123, 432])
            .build();
        let encoder = TestUpdateEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            update_type: UpdateType::Local,
            column_mapping: BTreeMap::from([(0, 1), (1, 0)]),
            pk_positions: vec![1],
            tuples: vec![vec![1, 2], vec![3, 4]],
            dql_encoder: Some(dql_encoder),
        };

        let expected: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x02\x92\x01\x9a\xcc\x80\x01\x82\x00\x01\x01\x00\x91\x01\x81\x0c\xcc\x8a\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f*\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";
        let mut actual = Vec::new();

        write_update_with_sql_package(&mut actual, encoder).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_encode_update_with_sql_but_update_shared() {
        let dql_encoder = TestDQLEncoderBuilder::new()
            .set_plan_id(14235593344027757343)
            .set_schema_info(HashMap::from([(12, 138)]))
            .set_sender_id(42)
            .set_vtables(HashMap::from([(
                "TMP_1302_".to_string(),
                vec![vec![1, 2, 3], vec![3, 2, 1]],
            )]))
            .set_options([123, 456])
            .set_params(vec![138, 123, 432])
            .build();
        let encoder = TestUpdateEncoder {
            request_id: "d3763996-6d21-418d-987f-d7349d034da9".to_string(),
            table_id: 128,
            table_version: 1,
            update_type: UpdateType::Shared,
            column_mapping: BTreeMap::from([(0, 1), (1, 0)]),
            pk_positions: vec![1],
            tuples: vec![vec![1, 2], vec![3, 4]],
            dql_encoder: Some(dql_encoder),
        };

        let expected = "cannot construct update with sql with non local update: Shared";

        let mut actual = Vec::new();
        let actual = write_update_with_sql_package(&mut actual, encoder)
            .err()
            .unwrap();

        assert_eq!(actual.to_string(), expected);
    }

    #[test]
    fn test_decode_update_with_sql() {
        let mut data: &[u8] =
            b"\x93\xd9$d3763996-6d21-418d-987f-d7349d034da9\x02\x92\x01\x9a\xcc\x80\x01\x82\x00\x01\x01\x00\x91\x01\x81\x0c\xcc\x8a\xcf\xc5\x8e\xfc\xb9\x15\xb0\x8b\x1f*\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";

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

        let package = LocalUpdatePackageIterator::new(data).unwrap();

        for elem in package {
            match elem.unwrap() {
                LocalUpdateResult::TableId(table_id) => {
                    assert_eq!(table_id, 128);
                }
                LocalUpdateResult::TableVersion(version) => {
                    assert_eq!(version, 1);
                }
                LocalUpdateResult::ColumnMapping(column_mapping) => {
                    let column_mapping = column_mapping.map(Result::unwrap).collect::<Vec<_>>();
                    let expected = vec![(0, 1), (1, 0)];
                    assert_eq!(column_mapping, expected);
                }
                LocalUpdateResult::PrimaryKey(pk_positions) => {
                    let pk_positions = pk_positions.map(Result::unwrap).collect::<Vec<_>>();
                    let expected = vec![1];
                    assert_eq!(pk_positions, expected);
                }
                LocalUpdateResult::SchemaInfo(schema_info) => {
                    assert_eq!(schema_info.len(), 1);
                    for res in schema_info {
                        let (t_id, ver) = res.unwrap();
                        assert_eq!(t_id, 12);
                        assert_eq!(ver, 138);
                    }
                }
                LocalUpdateResult::PlanId(plan_id) => {
                    assert_eq!(plan_id, 14235593344027757343);
                }
                LocalUpdateResult::SenderId(sender_id) => {
                    assert_eq!(sender_id, 42);
                }
                LocalUpdateResult::Vtables(vtables) => {
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
                LocalUpdateResult::Options(options) => {
                    assert_eq!(options, (123, 456));
                }
                LocalUpdateResult::Params(params) => {
                    assert_eq!(params, vec![147, 204, 138, 123, 205, 1, 176]);
                }
            }
        }
    }
}
