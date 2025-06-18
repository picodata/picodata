use std::{io::Cursor, str::from_utf8};

use rmp::{decode::RmpRead, Marker};
use sbroad::errors::{Action, Entity, SbroadError};
use smol_str::format_smolstr;

#[derive(Debug)]
pub struct DqlRes<'bytes> {
    pub msgpack_slice: Cursor<&'bytes [u8]>,
}

fn read_map_data(rd: &mut Cursor<&[u8]>, mut len: u64) -> Result<(), SbroadError> {
    while len > 0 {
        read_value_inner(rd)?;
        read_value_inner(rd)?;
        len -= 1;
    }

    Ok(())
}

fn read_array_data(rd: &mut Cursor<&[u8]>, mut len: u64) -> Result<(), SbroadError> {
    while len > 0 {
        read_value_inner(rd)?;
        len -= 1;
    }

    Ok(())
}

#[inline]
fn shift_pos(rd: &mut Cursor<&[u8]>, len: u64) -> Result<(), SbroadError> {
    let pos = rd.position();
    let slice_len = rd.get_ref().len() as u64;
    if len + pos > slice_len {
        return Err(SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("invalid msgpack!"),
        ));
    }

    rd.set_position(pos + len);

    Ok(())
}

fn read_value_inner(rd: &mut Cursor<&[u8]>) -> Result<(), SbroadError> {
    match rmp::decode::read_marker(rd).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("required tuple len: {e:?}"),
        )
    })? {
        Marker::Reserved
        | Marker::Null
        | Marker::True
        | Marker::False
        | Marker::FixPos(_)
        | Marker::FixNeg(_) => {}
        Marker::U8 | Marker::I8 => shift_pos(rd, 1)?,
        Marker::U16 | Marker::I16 => shift_pos(rd, 2)?,
        Marker::U32 | Marker::I32 => shift_pos(rd, 4)?,
        Marker::U64 | Marker::I64 => shift_pos(rd, 8)?,
        Marker::F32 => shift_pos(rd, 4)?,
        Marker::F64 => shift_pos(rd, 8)?,
        Marker::FixStr(len) => shift_pos(rd, len as u64)?,
        Marker::Str8 => {
            let len = rd.read_data_u8().expect("U8 expected");
            shift_pos(rd, len as u64)?;
        }
        Marker::Str16 => {
            let len = rd.read_data_u16().expect("U16 expected");
            shift_pos(rd, len as u64)?;
        }
        Marker::Str32 => {
            let len = rd.read_data_u32().expect("U32 expected");
            shift_pos(rd, len as u64)?;
        }
        Marker::FixArray(len) => {
            read_array_data(rd, len as u64)?;
        }
        Marker::Array16 => {
            let len = rd.read_data_u16().expect("U16 expected");
            read_array_data(rd, len as u64)?;
        }
        Marker::Array32 => {
            let len = rd.read_data_u32().expect("U32 expected");
            read_array_data(rd, len as u64)?;
        }
        Marker::FixMap(len) => read_map_data(rd, len as u64)?,
        Marker::Map16 => {
            let len = rd.read_data_u16().expect("U16 expected");
            read_map_data(rd, len as u64)?;
        }
        Marker::Map32 => {
            let len = rd.read_data_u32().expect("U32 expected");
            read_map_data(rd, len as u64)?;
        }
        Marker::Bin8 => {
            let len = rd.read_data_u8().expect("U8 expected");
            read_map_data(rd, len as u64)?;
        }
        Marker::Bin16 => {
            let len = rd.read_data_u16().expect("U8 expected");
            shift_pos(rd, len as u64)?;
        }
        Marker::Bin32 => {
            let len = rd.read_data_u32().expect("U8 expected");
            shift_pos(rd, len as u64)?;
        }
        Marker::FixExt1 => shift_pos(rd, 2)?,
        Marker::FixExt2 => shift_pos(rd, 3)?,
        Marker::FixExt4 => shift_pos(rd, 5)?,
        Marker::FixExt8 => shift_pos(rd, 9)?,
        Marker::FixExt16 => shift_pos(rd, 17)?,
        Marker::Ext8 => {
            let len = rd.read_data_u8().expect("U8 expected") as u64;
            shift_pos(rd, len + 1)?;
        }
        Marker::Ext16 => {
            let len = rd.read_data_u16().expect("U16 expected") as u64;
            shift_pos(rd, len + 1)?;
        }
        Marker::Ext32 => {
            let len = rd.read_data_u32().expect("U32 expected") as u64;
            shift_pos(rd, len + 1)?;
        }
    };

    Ok(())
}

impl<'a> Iterator for DqlRes<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let initial_pos = self.msgpack_slice.position() as usize;

        let tuple_len = rmp::decode::read_array_len(&mut self.msgpack_slice).map_err(|e| {
            SbroadError::FailedTo(
                Action::Decode,
                Some(Entity::MsgPack),
                format_smolstr!("required tuple len: {e:?}"),
            )
        });

        let Ok(tuple_len) = tuple_len else {
            return None;
        };

        for _ in 0..tuple_len {
            read_value_inner(&mut self.msgpack_slice).expect("failed to decode type");
        }

        let current_pos = self.msgpack_slice.position() as usize;
        let output = &self.msgpack_slice.get_ref()[initial_pos..current_pos];

        Some(output)
    }
}

#[derive(Debug)]
pub enum DecodeResult<'a> {
    RowCnt(u64),
    Dql(DqlRes<'a>),
    Miss,
}

/// Decode the result tuples from msgpack
///
/// # Errors
/// - Failed to decode the execution plan.
pub fn decode_msgpack_res(buf: &[u8]) -> Result<DecodeResult, SbroadError> {
    let mut stream = Cursor::new(buf);
    let map_len = rmp::decode::read_map_len(&mut stream).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("array length: {e:?}"),
        )
    })? as usize;
    if map_len != 1 {
        return Err(SbroadError::Invalid(
            Entity::Tuple,
            Some(format_smolstr!("expected map of 1 element, got {map_len}")),
        ));
    }

    // Decode required key.
    let key_len = rmp::decode::read_str_len(&mut stream).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("required key length: {e:?}"),
        )
    })? as usize;

    let mut data = [0; 4];
    stream.read_exact_buf(&mut data[..key_len]).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("read key: {e:?}"),
        )
    })?;
    let response_type = from_utf8(&data[..key_len]).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("failed to decode string: {e:?}"),
        )
    })?;

    if response_type == "dql" {
        let _ = rmp::decode::read_array_len(&mut stream).map_err(|e| {
            SbroadError::FailedTo(
                Action::Decode,
                Some(Entity::MsgPack),
                format_smolstr!("required array len: {e:?}"),
            )
        })? as usize;

        Ok(DecodeResult::Dql(DqlRes {
            msgpack_slice: stream,
        }))
    } else if response_type == "miss" {
        Ok(DecodeResult::Miss)
    } else if response_type == "dml" {
        let _ = rmp::decode::read_array_len(&mut stream).map_err(|e| {
            SbroadError::FailedTo(
                Action::Decode,
                Some(Entity::MsgPack),
                format_smolstr!("optional array length: {e:?}"),
            )
        })?;

        let row_cnt = rmp::decode::read_int(&mut stream).map_err(|e| {
            SbroadError::FailedTo(
                Action::Decode,
                Some(Entity::MsgPack),
                format_smolstr!("optional array length: {e:?}"),
            )
        })?;
        Ok(DecodeResult::RowCnt(row_cnt))
    } else {
        Err(SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("key must be \"dql\", \"dml\", or \"miss\""),
        ))
    }
}

mod tests {
    use tarantool::datetime::Datetime;
    use tarantool::decimal::Decimal;
    use tarantool::uuid::Uuid;
    use time::macros::datetime;

    use crate::sql::cluster_proto::{decode_msgpack_res, DecodeResult};

    #[tarantool::test]
    fn test_deserialize_dql() {
        // {
        //     "dql": [
        //         [5, "foo", true, dt, uuid, dec, 7],
        //         [8, "bar", false, dt, uuid, dec, 9]
        //     ]
        // }
        let msgpack_bytes = b"\x81\xA3dql\x92\x97\x05\xA3foo\xC3";
        let msgpack_bytes_2 = b"\x07\x97\x08\xA3bar\xC2";
        let msgpack_bytes_3 = b"\x09";

        let dt_slice =
            rmp_serde::to_vec::<Datetime>(&datetime!(2023-11-11 2:03:19.35421 -3).into()).unwrap();
        let uuid_slice = rmp_serde::to_vec(&Uuid::nil()).unwrap();
        let dec_slice = rmp_serde::to_vec(&Decimal::from(666)).unwrap();

        let mut data = Vec::new();
        data.extend_from_slice(msgpack_bytes);
        data.extend_from_slice(&dt_slice);
        data.extend_from_slice(&uuid_slice);
        data.extend_from_slice(&dec_slice);
        data.extend_from_slice(msgpack_bytes_2);
        data.extend_from_slice(&dt_slice);
        data.extend_from_slice(&uuid_slice);
        data.extend_from_slice(&dec_slice);
        data.extend_from_slice(msgpack_bytes_3);

        let query_res = decode_msgpack_res(&data).unwrap();
        match query_res {
            DecodeResult::Dql(mut res) => {
                let tuple_slice = res.next().unwrap();
                let expected_tuple_slice = b"\x97\x05\xA3foo\xC3\xD8\x04\x17\x0B\x4F\x65\x00\x00\x00\x00\xD0\xD0\x1C\x15\x4C\xFF\x00\x00\xD8\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xC7\x03\x01\x00\x66\x6C\x07";
                assert_eq!(tuple_slice, expected_tuple_slice);
                let tuple_slice = res.next().unwrap();
                let expected_tuple_slice = b"\x97\x08\xA3bar\xC2\xD8\x04\x17\x0B\x4F\x65\x00\x00\x00\x00\xD0\xD0\x1C\x15\x4C\xFF\x00\x00\xD8\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xC7\x03\x01\x00\x66\x6C\x09";
                assert_eq!(tuple_slice, expected_tuple_slice);
            }
            _ => {
                panic!("Expected DQL result!");
            }
        }
    }

    #[tarantool::test]
    fn test_deserialize_dql2() {
        // {
        //     "dql" : [
        //         [5, ["Online", 10], "buz"],
        //         [56, ["Offline", 0], "bar"]
        //     ]
        // }
        let msgpack_bytes =
            b"\x81\xA3dql\x92\x93\x05\x92\xA6Online\x0A\xA3buz\x93\x38\x92\xA7Offline\x00\xA3bar";

        let query_res = decode_msgpack_res(msgpack_bytes).unwrap();
        match query_res {
            DecodeResult::Dql(mut res) => {
                let tuple_slice = res.next().unwrap();
                let expected_tuple_slice = b"\x93\x05\x92\xA6Online\x0A\xA3buz";
                assert_eq!(tuple_slice, expected_tuple_slice);
                let tuple_slice = res.next().unwrap();
                let expected_tuple_slice = b"\x93\x38\x92\xA7Offline\x00\xA3bar";
                assert_eq!(tuple_slice, expected_tuple_slice);
            }
            _ => {
                panic!("Expected DQL result!");
            }
        }
    }

    #[tarantool::test]
    fn test_deserialize_dql3() {
        // {
        //     "dql" : [
        //         [5, {"Online" : 567}, "buz"],
        //         [56, {"Offline" : 800}, "bar"],
        //         [56000, null, "bar"],
        //         [-450, {"Online" : 4500}, null]
        //     ]
        // }
        let msgpack_bytes = b"\x81\xA3dql\x92\x93\x05\x81\xA6Online\xCD\x02\x37\xA3buz\x93\x38\x81\xA7Offline\xCD\x03\x20\xA3bar\x93\xCD\xDA\xC0\xC0\xA3bar\x93\xD1\xFE\x3E\x81\xA6Online\xCD\x11\x94\xC0";

        let query_res = decode_msgpack_res(msgpack_bytes).unwrap();
        match query_res {
            DecodeResult::Dql(mut res) => {
                let tuple_slice = res.next().unwrap();
                let expected_tuple_slice = b"\x93\x05\x81\xA6Online\xCD\x02\x37\xA3buz";
                assert_eq!(tuple_slice, expected_tuple_slice);
                let tuple_slice = res.next().unwrap();
                let expected_tuple_slice = b"\x93\x38\x81\xA7Offline\xCD\x03\x20\xA3bar";
                assert_eq!(tuple_slice, expected_tuple_slice);
                let tuple_slice = res.next().unwrap();
                let expected_tuple_slice = b"\x93\xCD\xDA\xC0\xC0\xA3bar";
                assert_eq!(tuple_slice, expected_tuple_slice);
                let tuple_slice = res.next().unwrap();
                let expected_tuple_slice = b"\x93\xD1\xFE\x3E\x81\xA6Online\xCD\x11\x94\xC0";
                assert_eq!(tuple_slice, expected_tuple_slice);
            }
            _ => {
                panic!("Expected DQL result!");
            }
        }
    }

    #[tarantool::test]
    fn test_deserialize_dml() {
        // {
        //     "dml": [567]
        // }
        let msgpack_bytes = b"\x81\xA3dml\x91\xCD\x02\x37";
        let res = decode_msgpack_res(msgpack_bytes).unwrap();
        match res {
            DecodeResult::RowCnt(row_count) => {
                assert_eq!(567, row_count);
            }
            _ => {
                panic!("Expected DML result!");
            }
        }
    }

    #[tarantool::test]
    fn test_deserialize_miss() {
        // {
        //     "miss": []
        // }
        let msgpack_bytes = b"\x81\xA4miss\x91\x05";
        let res = decode_msgpack_res(msgpack_bytes).unwrap();
        match res {
            DecodeResult::Miss => {}
            _ => {
                panic!("Expected MISS result!");
            }
        }
    }
}
