use crate::error::ProtocolError;
use crate::iterators::{MsgpackMapIterator, TupleIterator};
use crate::message_type::MessageType;
use crate::msgpack::{skip_value, ByteCounter};
use crate::protocol_encoder::{ColumnType, MsgpackWriter, ProtocolEncoder};
use rmp::decode::{read_array_len, read_int, read_map_len, read_str_len};
use rmp::encode::{write_array_len, write_map_len, write_str, write_uint};
use smol_str::SmolStr;
use std::io::{Cursor, Write};
use std::str::from_utf8;

pub fn write_dql_package(
    mut w: impl Write,
    data: &impl ProtocolEncoder,
) -> Result<(), std::io::Error> {
    write_array_len(&mut w, 3)?;
    let request_id = data.get_request_id();
    write_str(&mut w, request_id.as_str())?;
    rmp::encode::write_pfix(&mut w, MessageType::DQL as u8)?;

    write_array_len(&mut w, 6)?;
    // Write schema info as map
    let schema_info = data.get_schema_info();
    write_map_len(&mut w, schema_info.len() as u32)?;
    for (key, value) in schema_info {
        write_uint(&mut w, *key as u64)?;
        write_uint(&mut w, *value)?;
    }

    let plan_id = data.get_plan_id();
    rmp::encode::write_u64(&mut w, plan_id)?;

    let sender_id = data.get_sender_id();
    rmp::encode::write_bin(&mut w, sender_id.as_bytes())?;

    write_vtables(&mut w, data.get_vtables(plan_id))?;

    let options = data.get_options();
    write_array_len(&mut w, options.len() as u32)?;
    for option in options {
        write_uint(&mut w, option)?;
    }

    let mut params = data.get_params();
    write_array_len(&mut w, params.len() as u32)?;
    while params.next().is_some() {
        params.write_current(&mut w)?;
    }

    Ok(())
}

fn write_vtables(
    mut w: impl Write,
    vtables: impl ExactSizeIterator<Item = (SmolStr, impl MsgpackWriter)>,
) -> Result<(), std::io::Error> {
    write_map_len(&mut w, vtables.len() as u32)?;

    for (key, mut tuples) in vtables {
        write_str(&mut w, key.as_str())?;
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

#[derive(PartialEq, Debug, Copy, Clone)]
#[repr(u8)]
enum DQLState {
    SchemaInfo = 0,
    PlanId,
    SenderId,
    Vtables,
    Options,
    Params,
    End,
}

pub enum DQLResult<'a> {
    SchemaInfo(MsgpackMapIterator<'a, u32, u64>),
    PlanId(u64),
    SenderId(&'a str),
    Vtables(MsgpackMapIterator<'a, &'a str, TupleIterator<'a>>),
    Options(u64, u64),
    Params(&'a [u8]),
}

const DQL_PACKAGE_SIZE: usize = 6;
pub struct DQLPackageIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: DQLState,
}

impl<'a> DQLPackageIterator<'a> {
    pub fn new(raw_payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(raw_payload);

        let l = read_array_len(&mut cursor)?;
        if l != DQL_PACKAGE_SIZE as u32 {
            return Err(ProtocolError::DecodeError(format!(
                "DQL package is invalid: expected to have package array length {DQL_PACKAGE_SIZE}, got {l}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: DQLState::SchemaInfo,
        })
    }

    fn get_schema_info(&mut self) -> Result<MsgpackMapIterator<'a, u32, u64>, ProtocolError> {
        assert_eq!(self.state, DQLState::SchemaInfo);
        let l = read_map_len(&mut self.raw_payload)?;

        let start = self.raw_payload.position() as usize;
        for _ in 0..l * 2 {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        self.state = DQLState::PlanId;

        Ok(MsgpackMapIterator::new(
            &self.raw_payload.get_ref()[start..end],
            l,
            |r| read_int(r).map_err(|err| ProtocolError::DecodeError(err.to_string())),
            |r| read_int(r).map_err(|err| ProtocolError::DecodeError(err.to_string())),
        ))
    }

    fn get_plan_id(&mut self) -> Result<u64, ProtocolError> {
        assert_eq!(self.state, DQLState::PlanId);
        let plan_id = rmp::decode::read_u64(&mut self.raw_payload)?;
        self.state = DQLState::SenderId;
        Ok(plan_id)
    }

    fn get_sender_id(&mut self) -> Result<&'a str, ProtocolError> {
        assert_eq!(self.state, DQLState::SenderId);
        let sender_id_len = rmp::decode::read_bin_len(&mut self.raw_payload)?;
        let start = self.raw_payload.position() as usize;
        let end = start + sender_id_len as usize;
        let sender_id = from_utf8(&self.raw_payload.get_ref()[start..end])
            .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        self.raw_payload.set_position(end as u64);
        self.state = DQLState::Vtables;
        Ok(sender_id)
    }

    fn get_vtables(
        &mut self,
    ) -> Result<MsgpackMapIterator<'a, &'a str, TupleIterator<'a>>, ProtocolError> {
        assert_eq!(self.state, DQLState::Vtables);

        let l = read_map_len(&mut self.raw_payload)?;
        let start = self.raw_payload.position() as usize;
        for _ in 0..l * 2 {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        self.state = DQLState::Options;

        let table_name_decoder = |r: &mut Cursor<&'a [u8]>| -> Result<&'a str, ProtocolError> {
            let l = read_str_len(r)?;
            let start = r.position() as usize;
            let end = start + l as usize;
            let vtable_name = from_utf8(&r.get_ref()[start..end])
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
            r.set_position(end as u64);
            Ok(vtable_name)
        };

        let tuple_iterator_decoder =
            |r: &mut Cursor<&'a [u8]>| -> Result<TupleIterator<'a>, ProtocolError> {
                let l = read_array_len(r)? as usize;
                let start = r.position() as usize;
                for _ in 0..l {
                    skip_value(r).map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
                }
                let end = r.position() as usize;

                Ok(TupleIterator::new(&r.get_ref()[start..end], l))
            };

        Ok(MsgpackMapIterator::new(
            &self.raw_payload.get_ref()[start..end],
            l,
            table_name_decoder,
            tuple_iterator_decoder,
        ))
    }

    fn get_options(&mut self) -> Result<(u64, u64), ProtocolError> {
        assert_eq!(self.state, DQLState::Options);
        let options = read_array_len(&mut self.raw_payload)?;
        if options != 2 {
            return Err(ProtocolError::DecodeError(format!(
                "DQL package is invalid: expected to have options array length 2, got {options}"
            )));
        }
        let sql_motion_row_max = read_int(&mut self.raw_payload)?;
        let sql_vdbe_opcode_max = read_int(&mut self.raw_payload)?;
        self.state = DQLState::Params;
        Ok((sql_motion_row_max, sql_vdbe_opcode_max))
    }

    fn get_params(&mut self) -> Result<&'a [u8], ProtocolError> {
        assert_eq!(self.state, DQLState::Params);
        let l = self.raw_payload.position() as usize;
        let params = &self.raw_payload.get_ref()[l..];
        self.state = DQLState::End;
        Ok(params)
    }
}

impl<'a> Iterator for DQLPackageIterator<'a> {
    type Item = Result<DQLResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            DQLState::SchemaInfo => match self.get_schema_info() {
                Ok(schema_info) => Some(Ok(DQLResult::SchemaInfo(schema_info))),
                Err(err) => Some(Err(err)),
            },
            DQLState::PlanId => match self.get_plan_id() {
                Ok(plan_id) => Some(Ok(DQLResult::PlanId(plan_id))),
                Err(err) => Some(Err(err)),
            },
            DQLState::SenderId => match self.get_sender_id() {
                Ok(sender_id) => Some(Ok(DQLResult::SenderId(sender_id))),
                Err(err) => Some(Err(err)),
            },
            DQLState::Vtables => match self.get_vtables() {
                Ok(vtables) => Some(Ok(DQLResult::Vtables(vtables))),
                Err(err) => Some(Err(err)),
            },
            DQLState::Options => match self.get_options() {
                Ok(options) => Some(Ok(DQLResult::Options(options.0, options.1))),
                Err(err) => Some(Err(err)),
            },
            DQLState::Params => match self.get_params() {
                Ok(params) => Some(Ok(DQLResult::Params(params))),
                Err(err) => Some(Err(err)),
            },
            DQLState::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = DQL_PACKAGE_SIZE - self.state as usize;
        (size, Some(size))
    }
}

pub fn write_dql_additional_data_package(
    mut w: impl Write,
    data: &impl ProtocolEncoder,
) -> Result<(), std::io::Error> {
    write_array_len(&mut w, 3)?;

    let schema_info = data.get_schema_info();
    write_map_len(&mut w, schema_info.len() as u32)?;
    for (key, value) in schema_info {
        write_uint(&mut w, *key as u64)?;
        write_uint(&mut w, *value)?;
    }

    let vtables_metadata = data.get_vtables_metadata();
    write_map_len(&mut w, vtables_metadata.len() as u32)?;
    for (key, columns) in vtables_metadata {
        write_str(&mut w, key.as_str())?;
        write_array_len(&mut w, columns.len() as u32)?;
        for (column, ty) in columns {
            write_array_len(&mut w, 2)?;
            write_str(&mut w, column.as_str())?;
            rmp::encode::write_pfix(&mut w, ty as u8)?;
        }
    }

    let sql = data.get_sql();
    write_str(&mut w, sql)?;

    Ok(())
}

#[derive(PartialEq, Debug, Copy, Clone)]
#[repr(u8)]
enum DQLCacheMissState {
    SchemaInfo = 0,
    VtablesMetadata,
    Sql,
    End,
}

pub enum DQLCacheMissResult<'a> {
    SchemaInfo(MsgpackMapIterator<'a, u32, u64>),
    VtablesMetadata(MsgpackMapIterator<'a, &'a str, Vec<(&'a str, ColumnType)>>),
    Sql(&'a str),
}

const DQL_CACHE_MISS_PACKAGE_SIZE: usize = 3;
pub struct DQLCacheMissIterator<'a> {
    raw_payload: Cursor<&'a [u8]>,
    state: DQLCacheMissState,
}

impl<'a> DQLCacheMissIterator<'a> {
    pub fn new(raw_payload: &'a [u8]) -> Result<Self, ProtocolError> {
        let mut cursor = Cursor::new(raw_payload);

        let l = read_array_len(&mut cursor)?;
        if l != DQL_CACHE_MISS_PACKAGE_SIZE as u32 {
            return Err(ProtocolError::DecodeError(format!(
                "DQL package is invalid: expected to have package array length {DQL_CACHE_MISS_PACKAGE_SIZE}, got {l}"
            )));
        }

        Ok(Self {
            raw_payload: cursor,
            state: DQLCacheMissState::SchemaInfo,
        })
    }

    fn get_schema_info(&mut self) -> Result<MsgpackMapIterator<'a, u32, u64>, ProtocolError> {
        assert_eq!(self.state, DQLCacheMissState::SchemaInfo);
        let l = read_map_len(&mut self.raw_payload)?;
        let start = self.raw_payload.position() as usize;
        for _ in 0..l * 2 {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        self.state = DQLCacheMissState::VtablesMetadata;
        Ok(MsgpackMapIterator::new(
            &self.raw_payload.get_ref()[start..end],
            l,
            |r| read_int(r).map_err(|err| ProtocolError::DecodeError(err.to_string())),
            |r| read_int(r).map_err(|err| ProtocolError::DecodeError(err.to_string())),
        ))
    }

    #[allow(clippy::type_complexity)]
    fn get_vtables_metadata(
        &mut self,
    ) -> Result<MsgpackMapIterator<'a, &'a str, Vec<(&'a str, ColumnType)>>, ProtocolError> {
        assert_eq!(self.state, DQLCacheMissState::VtablesMetadata);
        let l = read_map_len(&mut self.raw_payload)?;
        let start = self.raw_payload.position() as usize;
        for _ in 0..l * 2 {
            skip_value(&mut self.raw_payload)
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        }
        let end = self.raw_payload.position() as usize;
        self.state = DQLCacheMissState::Sql;

        let table_name_decoder = |r: &mut Cursor<&'a [u8]>| -> Result<&str, ProtocolError> {
            let l = read_str_len(r)?;
            let start = r.position() as usize;
            let end = start + l as usize;
            let vtable_name = from_utf8(&r.get_ref()[start..end])
                .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
            r.set_position(end as u64);
            Ok(vtable_name)
        };

        let metadata_decoder =
            |r: &mut Cursor<&'a [u8]>| -> Result<Vec<(&str, ColumnType)>, ProtocolError> {
                let l = read_array_len(r)? as usize;
                let mut res = Vec::with_capacity(l);
                for _ in 0..l {
                    let l = read_array_len(r)? as usize;
                    if l != 2 {
                        return Err(ProtocolError::DecodeError(format!(
                            "DQL Cache Miss package is invalid: expected to have array length 2, got {l}"
                        )));
                    }

                    let l = read_str_len(r)?;
                    let start = r.position() as usize;
                    let end = start + l as usize;
                    let column_name = from_utf8(&r.get_ref()[start..end])
                        .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
                    r.set_position(end as u64);

                    let ct = rmp::decode::read_pfix(r)?;
                    let ct = ColumnType::try_from(ct)
                        .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;

                    res.push((column_name, ct));
                }

                Ok(res)
            };

        Ok(MsgpackMapIterator::new(
            &self.raw_payload.get_ref()[start..end],
            l,
            table_name_decoder,
            metadata_decoder,
        ))
    }

    fn get_sql(&mut self) -> Result<&'a str, ProtocolError> {
        assert_eq!(self.state, DQLCacheMissState::Sql);
        let l = read_str_len(&mut self.raw_payload)?;
        let start = self.raw_payload.position() as usize;
        let end = start + l as usize;
        let sql = from_utf8(&self.raw_payload.get_ref()[start..end])
            .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        self.raw_payload.set_position(end as u64);
        self.state = DQLCacheMissState::End;
        Ok(sql)
    }
}

impl<'a> Iterator for DQLCacheMissIterator<'a> {
    type Item = Result<DQLCacheMissResult<'a>, ProtocolError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            DQLCacheMissState::SchemaInfo => match self.get_schema_info() {
                Ok(schema_info) => Some(Ok(DQLCacheMissResult::SchemaInfo(schema_info))),
                Err(err) => Some(Err(err)),
            },
            DQLCacheMissState::VtablesMetadata => match self.get_vtables_metadata() {
                Ok(vtables_metadata) => {
                    Some(Ok(DQLCacheMissResult::VtablesMetadata(vtables_metadata)))
                }
                Err(err) => Some(Err(err)),
            },
            DQLCacheMissState::Sql => match self.get_sql() {
                Ok(sql) => Some(Ok(DQLCacheMissResult::Sql(sql))),
                Err(err) => Some(Err(err)),
            },
            DQLCacheMissState::End => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = DQL_CACHE_MISS_PACKAGE_SIZE - self.state as usize;
        (size, Some(size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol_encoder::test::TestEncoderBuilder;
    use crate::protocol_encoder::ColumnType;
    use smol_str::ToSmolStr;
    use std::collections::HashMap;
    use std::str::from_utf8;

    #[test]
    fn test_encode_dql() {
        let data = TestEncoderBuilder::new()
            .set_plan_id(5264743718663535479)
            .set_request_id("14e84334-71df-4e69-8c85-dc2707a390c6".to_string())
            .set_schema_info(HashMap::from([(12, 138)]))
            .set_sender_id("some".to_string())
            .set_vtables(HashMap::from([(
                "TMP_1302_".to_smolstr(),
                vec![vec![1, 2, 3], vec![3, 2, 1]],
            )]))
            .set_options([123, 456])
            .set_params(vec![138, 123, 432])
            .build();

        let mut writer = Vec::new();

        write_dql_package(&mut writer, &data).unwrap();
        let expected: &[u8] = b"\x93\xd9$14e84334-71df-4e69-8c85-dc2707a390c6\x00\x96\x81\x0c\xcc\x8a\xcfI\x10 \x84\xb0h\xbbw\xc4\x04some\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";

        assert_eq!(writer, expected);
    }

    #[test]
    fn test_execute_dql_cache_hit() {
        let mut data: &[u8] = b"\x93\xd9$14e84334-71df-4e69-8c85-dc2707a390c6\x00\x96\x81\x0c\xcc\x8a\xcfI\x10 \x84\xb0h\xbbw\xc4\x04some\x81\xa9TMP_1302_\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";

        let l = read_array_len(&mut data).unwrap();
        assert_eq!(l, 3);
        let str_len = read_str_len(&mut data).unwrap();
        let (request_id, new_data) = data.split_at(str_len as usize);
        let request_id = from_utf8(request_id).unwrap();
        assert_eq!(request_id, "14e84334-71df-4e69-8c85-dc2707a390c6");
        data = new_data;
        let msg_type = rmp::decode::read_pfix(&mut data).unwrap();
        assert_eq!(msg_type, MessageType::DQL as u8);

        let package = DQLPackageIterator::new(data).unwrap();

        for elem in package {
            match elem.unwrap() {
                DQLResult::SchemaInfo(schema_info) => {
                    assert_eq!(schema_info.len(), 1);
                    for res in schema_info {
                        let (t_id, version) = res.unwrap();
                        assert_eq!(t_id, 12);
                        assert_eq!(version, 138);
                    }
                }
                DQLResult::PlanId(plan_id) => {
                    assert_eq!(plan_id, 5264743718663535479);
                }
                DQLResult::SenderId(sender_id) => {
                    assert_eq!(sender_id, "some");
                }
                DQLResult::Vtables(vtables) => {
                    for result in vtables {
                        let (name, tuples) = result.unwrap();
                        assert_eq!(name, "TMP_1302_");
                        assert_eq!(tuples.len(), 2);
                        let mut actual = Vec::with_capacity(2);
                        for tuple in tuples {
                            let tuple = tuple.unwrap();
                            actual.push(tuple);
                        }
                        let expected = vec![[148, 1, 2, 3, 0], [148, 3, 2, 1, 1]];
                        assert_eq!(actual, expected);
                    }
                }
                DQLResult::Options(sql_motion_row_max, sql_vdbe_opcode_max) => {
                    assert_eq!(sql_motion_row_max, 123);
                    assert_eq!(sql_vdbe_opcode_max, 456);
                }
                DQLResult::Params(params) => {
                    let expected = vec![147, 204, 138, 123, 205, 1, 176];
                    assert_eq!(params, expected.as_slice());
                }
            }
        }
    }

    #[test]
    fn test_encode_dql_cache_miss() {
        let data = TestEncoderBuilder::new()
            .set_schema_info(HashMap::from([(12, 138)]))
            .set_meta(HashMap::from([(
                "TMP_1302_".to_smolstr(),
                vec![
                    ("a".to_smolstr(), ColumnType::Integer),
                    ("b".to_smolstr(), ColumnType::Integer),
                ],
            )]))
            .set_sql("select * from TMP_1302_;".to_smolstr())
            .build();

        let mut writer = Vec::new();
        write_dql_additional_data_package(&mut writer, &data).unwrap();
        let expected: &[u8] = b"\x93\x81\x0c\xcc\x8a\x81\xa9TMP_1302_\x92\x92\xa1a\x05\x92\xa1b\x05\xb8select * from TMP_1302_;";
        assert_eq!(writer, expected);
    }

    #[test]
    fn test_handle_dql_cache_miss() {
        let data: &[u8] = b"\x93\x81\x0c\xcc\x8a\x81\xa9TMP_1302_\x92\x92\xa1a\x05\x92\xa1b\x05\xb8select * from TMP_1302_;";

        let package = DQLCacheMissIterator::new(data).unwrap();

        for elem in package {
            match elem.unwrap() {
                DQLCacheMissResult::SchemaInfo(schema_info) => {
                    assert_eq!(schema_info.len(), 1);
                    for res in schema_info {
                        let (t_id, ver) = res.unwrap();
                        assert_eq!(t_id, 12);
                        assert_eq!(ver, 138);
                    }
                }
                DQLCacheMissResult::VtablesMetadata(vtables_metadata) => {
                    assert_eq!(vtables_metadata.len(), 1);
                    for res in vtables_metadata {
                        let (table_name, columns) = res.unwrap();
                        assert_eq!(table_name, "TMP_1302_");
                        let expected = vec![("a", ColumnType::Integer), ("b", ColumnType::Integer)];
                        assert_eq!(columns, expected);
                    }
                }
                DQLCacheMissResult::Sql(sql) => {
                    assert_eq!(sql, "select * from TMP_1302_;");
                }
            }
        }
    }
}
