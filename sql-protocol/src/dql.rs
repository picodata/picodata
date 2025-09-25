use crate::error::ProtocolError;
use crate::message_type::MessageType;
use crate::msgpack::ByteCounter;
use crate::protocol_encoder::{MsgpackWriter, ProtocolEncoder};
use crate::tuple_iterator::TupleIterator;
use smol_str::SmolStr;
use std::io::Write;
use std::str::from_utf8;

pub fn write_dql_package(
    mut w: impl Write,
    data: &impl ProtocolEncoder,
) -> Result<(), std::io::Error> {
    rmp::encode::write_array_len(&mut w, 3)?;
    let request_id = data.get_request_id();
    rmp::encode::write_str(&mut w, request_id.as_str())?;
    rmp::encode::write_pfix(&mut w, MessageType::DQL as u8)?;

    rmp::encode::write_array_len(&mut w, 6)?;
    // Write schema info as map
    let schema_info = data.get_schema_info();
    rmp::encode::write_map_len(&mut w, schema_info.len() as u32)?;
    for (key, value) in schema_info {
        rmp::encode::write_uint(&mut w, *key as u64)?;
        rmp::encode::write_uint(&mut w, *value)?;
    }

    let plan_id = data.get_plan_id();
    rmp::encode::write_u64(&mut w, plan_id)?;

    let sender_id = data.get_sender_id();
    rmp::encode::write_bin(&mut w, sender_id.as_bytes())?;

    write_vtables(&mut w, data.get_vtables(plan_id))?;

    let options = data.get_options();
    rmp::encode::write_array_len(&mut w, options.len() as u32)?;
    for option in options {
        rmp::encode::write_uint(&mut w, option)?;
    }

    let mut params = data.get_params();
    rmp::encode::write_array_len(&mut w, params.len() as u32)?;
    while params.next().is_some() {
        params.write_current(&mut w)?;
    }

    Ok(())
}

fn write_vtables(
    mut w: impl Write,
    vtables: impl ExactSizeIterator<Item = (SmolStr, impl MsgpackWriter)>,
) -> Result<(), std::io::Error> {
    rmp::encode::write_map_len(&mut w, vtables.len() as u32)?;

    for (key, mut tuples) in vtables {
        rmp::encode::write_str(&mut w, key.as_str())?;
        rmp::encode::write_array_len(&mut w, 2)?;
        rmp::encode::write_uint(&mut w, 0)?; // usual vtable
        rmp::encode::write_array_len(&mut w, tuples.len() as u32)?;

        while tuples.next().is_some() {
            let mut tuple_counter = ByteCounter::default();
            tuples.write_current(&mut tuple_counter)?;
            rmp::encode::write_bin_len(&mut w, tuple_counter.bytes() as u32)?;
            tuples.write_current(&mut w)?;
        }
    }

    Ok(())
}

pub fn execute_dql<VersionCheck, TupleInserter, Execute>(
    mut raw_payload: &[u8],
    version_check_f: VersionCheck,
    tuple_f: TupleInserter,
    execute_f: Execute,
) -> Result<(), ProtocolError>
where
    VersionCheck: Fn(u32, u64) -> Result<(), ProtocolError>, // table_id, version
    TupleInserter: Fn(&str, &mut TupleIterator) -> Result<(), ProtocolError>,
    Execute: Fn(u64, &[u8], u64, u64) -> Result<(), ProtocolError>, // plan_id, params, motion_row_max, vdbe_opcode_max
{
    let l = rmp::decode::read_array_len(&mut raw_payload)?;
    if l != 6 {
        return Err(ProtocolError::DecodeError(format!(
            "DQL package is invalid: expected to have package array length 6, got {l}"
        )));
    }
    let l = rmp::decode::read_map_len(&mut raw_payload)?;
    for _ in 0..l {
        let id = rmp::decode::read_int(&mut raw_payload)?;
        let version: u64 = rmp::decode::read_int(&mut raw_payload)?;

        version_check_f(id, version)?;
    }

    let plan_id = rmp::decode::read_u64(&mut raw_payload)?;

    // TODO: check plan_id in cache
    // if not in cache, unlock cache and make request to the router
    // if in cache, just continue

    let sender_id_len = rmp::decode::read_bin_len(&mut raw_payload)?;
    let _sender_id = from_utf8(&raw_payload[..sender_id_len as usize])
        .map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
    raw_payload = &raw_payload[sender_id_len as usize..];

    // populate vtables

    let l = rmp::decode::read_map_len(&mut raw_payload)?;
    for _ in 0..l {
        let ls = rmp::decode::read_str_len(&mut raw_payload)?;
        let (vtable_name, la) = raw_payload.split_at(ls as usize);
        let vtable_name =
            from_utf8(vtable_name).map_err(|err| ProtocolError::DecodeError(err.to_string()))?;
        raw_payload = la;
        let l = rmp::decode::read_array_len(&mut raw_payload)?;
        if l != 2 {
            return Err(ProtocolError::DecodeError(format!(
                "DQL package is invalid: expected to have vtable array length 2, got {l}"
            )));
        }
        // don't use for now, only one type of vtable is supported
        let _ = rmp::decode::read_pfix(&mut raw_payload)?;
        let l = rmp::decode::read_array_len(&mut raw_payload)?;
        let mut iterator = TupleIterator::new(raw_payload, l as usize);

        tuple_f(vtable_name, &mut iterator)?;

        if iterator.next().is_some() {
            return Err(ProtocolError::DecodeError(
                "Tuple itartor was not consumed completely.".to_string(),
            ));
        }

        raw_payload = iterator.rest_bytes()?;
    }

    let options = rmp::decode::read_array_len(&mut raw_payload)?;
    if options != 2 {
        return Err(ProtocolError::DecodeError(format!(
            "DQL package is invalid: expected to have options array length 2, got {options}"
        )));
    }
    let sql_motion_row_max: u64 = rmp::decode::read_int(&mut raw_payload)?;
    let sql_vdbe_opcode_max: u64 = rmp::decode::read_int(&mut raw_payload)?;

    let encoded_params = raw_payload;

    execute_f(
        plan_id,
        encoded_params,
        sql_motion_row_max,
        sql_vdbe_opcode_max,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol_encoder::test::TestEncoder;
    use smol_str::ToSmolStr;
    use std::collections::HashMap;
    use std::str::from_utf8;

    #[test]
    fn test_encode_dql() {
        let data = TestEncoder {
            plan_id: 5264743718663535479,
            request_id: "14e84334-71df-4e69-8c85-dc2707a390c6".to_string(),
            schema_info: HashMap::from([(12, 138)]),
            sender_id: "some".to_string(),
            vtables: HashMap::from([(
                "TMP_1302_".to_smolstr(),
                vec![vec![1, 2, 3], vec![3, 2, 1]],
            )]),
            options: [123, 456],
            params: vec![138, 123, 432],
        };

        let mut writer = Vec::new();

        write_dql_package(&mut writer, &data).unwrap();
        let expected: &[u8] = b"\x93\xd9$14e84334-71df-4e69-8c85-dc2707a390c6\x00\x96\x81\x0c\xcc\x8a\xcfI\x10 \x84\xb0h\xbbw\xc4\x04some\x81\xa9TMP_1302_\x92\x00\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";

        assert_eq!(writer, expected);
    }

    #[test]
    fn test_execute_dql_cache_hit() {
        let mut data: &[u8] = b"\x93\xd9$14e84334-71df-4e69-8c85-dc2707a390c6\x00\x96\x81\x0c\xcc\x8a\xcfI\x10 \x84\xb0h\xbbw\xc4\x04some\x81\xa9TMP_1302_\x92\x00\x92\xc4\x05\x94\x01\x02\x03\x00\xc4\x05\x94\x03\x02\x01\x01\x92{\xcd\x01\xc8\x93\xcc\x8a{\xcd\x01\xb0";

        let version_f = |t_id: u32, ver: u64| -> Result<(), ProtocolError> {
            assert_eq!(t_id, 12);
            assert_eq!(ver, 138);
            Ok(())
        };

        let tuple_f = |name: &str, tuples: &mut TupleIterator| -> Result<(), ProtocolError> {
            assert_eq!(name, "TMP_1302_");
            let actual = vec![tuples.next().unwrap(), tuples.next().unwrap()];
            let expected = vec![[148, 1, 2, 3, 0], [148, 3, 2, 1, 1]];
            assert_eq!(actual, expected);
            Ok(())
        };
        let execute_f =
            |plan: u64, params: &[u8], option1: u64, option2: u64| -> Result<(), ProtocolError> {
                assert_eq!(plan, 5264743718663535479);
                assert_eq!(params, &[147, 204, 138, 123, 205, 1, 176]);
                assert_eq!(option1, 123);
                assert_eq!(option2, 456);
                Ok(())
            };

        let l = rmp::decode::read_array_len(&mut data).unwrap();
        assert_eq!(l, 3);
        let str_len = rmp::decode::read_str_len(&mut data).unwrap();
        let (request_id, new_data) = data.split_at(str_len as usize);
        let request_id = from_utf8(request_id).unwrap();
        assert_eq!(request_id, "14e84334-71df-4e69-8c85-dc2707a390c6");
        data = new_data;
        let msg_type = rmp::decode::read_pfix(&mut data).unwrap();
        assert_eq!(msg_type, MessageType::DQL as u8);

        execute_dql(data, version_f, tuple_f, execute_f).unwrap()
    }
}
