#![cfg(feature = "picodata")]

use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::ptr::NonNull;
use tarantool::error::{Error, TarantoolError};
use tarantool::ffi::lua::lua_State;
use tarantool::ffi::sql::{obuf_append, Obuf, ObufWrapper, Port, PortC, PortVTable, IPROTO_DATA};
use tarantool::index::IndexType;
use tarantool::msgpack::write_array_len;
use tarantool::space::{Field, Space};
use tarantool::sql::{sql_execute_into_port, unprepare};
use tarantool::tuple::Tuple;

fn create_sql_test_space(name: &str) -> tarantool::Result<Space> {
    let space = Space::builder(name)
        .if_not_exists(true)
        .field(Field::unsigned("ID"))
        .field(Field::string("VALUE"))
        .create()?;
    space
        .index_builder("primary")
        .if_not_exists(true)
        .index_type(IndexType::Tree)
        .part(1)
        .create()?;
    space
        .index_builder("secondary")
        .if_not_exists(true)
        .index_type(IndexType::Tree)
        .part(1)
        .part(2)
        .create()?;
    Ok(space)
}

fn drop_sql_test_space(space: Space) -> tarantool::Result<()> {
    space.drop()
}

fn decode_dql_result<OUT>(stream: &mut (impl Read + Sized)) -> OUT
where
    OUT: DeserializeOwned,
{
    let map_len = rmp::decode::read_map_len(stream).unwrap();
    let mut data = None;
    for _ in 0..map_len {
        let key = rmp::decode::read_pfix(stream).unwrap();
        if key != IPROTO_DATA {
            let _ = rmpv::decode::read_value(stream).unwrap();
            continue;
        }
        data = Some(rmpv::decode::read_value(stream).unwrap());
    }
    let data = data.unwrap();
    rmpv::ext::from_value::<OUT>(data).unwrap()
}

fn decode_port<OUT>(port: &PortC) -> Vec<OUT>
where
    OUT: DeserializeOwned,
{
    let mut result = Vec::new();
    for mp_bytes in port.iter() {
        let entry: OUT = rmp_serde::from_slice(mp_bytes).unwrap();
        result.push(entry);
    }
    result
}

pub fn prepared_invalid_query() {
    let maybe_stmt = tarantool::sql::prepare("SELECT * FROM UNKNOWN_SPACE".to_string());
    assert!(maybe_stmt.is_err());
    assert!(matches!(
        maybe_stmt.err().unwrap(),
        Error::Tarantool(TarantoolError { .. })
    ));
}

pub fn prepared_source_query() {
    let sp = create_sql_test_space("SQL_TEST").unwrap();

    let stmt = tarantool::sql::prepare("SELECT * FROM SQL_TEST".to_string()).unwrap();
    assert_eq!(stmt.source(), "SELECT * FROM SQL_TEST");

    unprepare(stmt).unwrap();
    drop_sql_test_space(sp).unwrap();
}

pub fn prepared_no_params() {
    let sp = create_sql_test_space("SQL_TEST").unwrap();

    sp.insert(&(1, "one")).unwrap();
    sp.insert(&(2, "two")).unwrap();
    sp.insert(&(3, "three")).unwrap();
    sp.insert(&(4, "four")).unwrap();

    let sql = String::from("SELECT * FROM SQL_TEST");
    let stmt = tarantool::sql::prepare(sql).unwrap();

    let mut stream = stmt.execute_raw(&(), 100).unwrap();
    let result = decode_dql_result::<Vec<(u64, String)>>(&mut stream);

    assert_eq!((1, "one".to_string()), result[0]);
    assert_eq!((2, "two".to_string()), result[1]);
    assert_eq!((3, "three".to_string()), result[2]);
    assert_eq!((4, "four".to_string()), result[3]);

    let mut port = Port::new_port_c();
    let mut port_c = unsafe { port.as_mut_port_c() };
    stmt.execute_into_port(&(), 100, &mut port_c).unwrap();
    let decoded_port: Vec<(u64, String)> = decode_port(&port_c);
    assert_eq!(decoded_port, result);

    let sql = "SELECT * FROM SQL_TEST WHERE ID = 1";
    let mut stream = tarantool::sql::prepare_and_execute_raw(sql, &(), 100).unwrap();
    let result = decode_dql_result::<Vec<(u64, String)>>(&mut stream);
    assert_eq!(1, result.len());
    assert_eq!((1, "one".to_string()), result[0]);

    let mut port = Port::new_port_c();
    let mut port_c = unsafe { port.as_mut_port_c() };
    sql_execute_into_port(sql, &(), 100, &mut port_c).unwrap();
    let decoded_port: Vec<(u64, String)> = decode_port(&port_c);
    assert_eq!(decoded_port, result);

    unprepare(stmt).unwrap();
    drop_sql_test_space(sp).unwrap();
}

pub fn prepared_large_query() {
    let sp = create_sql_test_space("SQL_TEST").unwrap();

    let mut i = 1;
    while i < 10000 {
        sp.insert(&(i, "one")).unwrap();
        sp.insert(&(i + 1, "two")).unwrap();
        sp.insert(&(i + 2, "three")).unwrap();
        sp.insert(&(i + 3, "four")).unwrap();

        i += 4;
    }
    let sql = String::from("SELECT * FROM SQL_TEST");
    let stmt = tarantool::sql::prepare(sql).unwrap();
    let mut stream = stmt.execute_raw(&(), 0).unwrap();
    let result = decode_dql_result::<Vec<(u64, String)>>(&mut stream);

    let mut i = 0;
    while i < 10000 {
        assert_eq!((i as u64 + 1, "one".to_string()), result[i]);
        assert_eq!((i as u64 + 2, "two".to_string()), result[i + 1]);
        assert_eq!((i as u64 + 3, "three".to_string()), result[i + 2]);
        assert_eq!((i as u64 + 4, "four".to_string()), result[i + 3]);
        i += 4;
    }

    let mut port = Port::new_port_c();
    let mut port_c = unsafe { port.as_mut_port_c() };
    stmt.execute_into_port(&(), 0, &mut port_c).unwrap();
    let decoded_port: Vec<(u64, String)> = decode_port(&port_c);
    assert_eq!(decoded_port, result);

    unprepare(stmt).unwrap();
    drop_sql_test_space(sp).unwrap();
}

pub fn prepared_invalid_params() {
    let sp = create_sql_test_space("SQL_TEST").unwrap();

    let stmt = tarantool::sql::prepare("SELECT * FROM SQL_TEST WHERE ID > ?".to_string()).unwrap();
    let result = stmt.execute_raw(&("not uint value"), 0);

    assert!(result.is_err());
    assert!(matches!(
        result.err().unwrap(),
        Error::Tarantool(TarantoolError { .. })
    ));

    let mut port = Port::new_port_c();
    let mut port_c = unsafe { port.as_mut_port_c() };
    let result = stmt.execute_into_port(&("not uint value",), 0, &mut port_c);
    assert!(result.is_err());
    assert!(matches!(
        result.err().unwrap(),
        Error::Tarantool(TarantoolError { .. })
    ));

    let result = tarantool::sql::prepare_and_execute_raw(
        "SELECT * FROM SQL_TEST WHERE ID = ?",
        &("not uint value"),
        0,
    );
    assert!(result.is_err());
    assert!(matches!(
        result.err().unwrap(),
        Error::Tarantool(TarantoolError { .. })
    ));

    let mut port = Port::new_port_c();
    let mut port_c = unsafe { port.as_mut_port_c() };
    let result = sql_execute_into_port(
        "SELECT * FROM SQL_TEST WHERE ID = ?",
        &("not uint value"),
        0,
        &mut port_c,
    );
    assert!(result.is_err());
    assert!(matches!(
        result.err().unwrap(),
        Error::Tarantool(TarantoolError { .. })
    ));

    unprepare(stmt).unwrap();
    drop_sql_test_space(sp).unwrap();
}

pub fn prepared_with_unnamed_params() {
    let sp = create_sql_test_space("SQL_TEST").unwrap();

    sp.insert(&(101, "one")).unwrap();
    sp.insert(&(102, "two")).unwrap();
    sp.insert(&(103, "three")).unwrap();
    sp.insert(&(104, "four")).unwrap();

    let stmt = tarantool::sql::prepare("SELECT * FROM SQL_TEST WHERE ID > ?".to_string()).unwrap();

    let mut stream = stmt.execute_raw(&(102,), 0).unwrap();
    let result = decode_dql_result::<Vec<(u8, String)>>(&mut stream);
    assert_eq!(2, result.len());
    assert_eq!((103, "three".to_string()), result[0]);
    assert_eq!((104, "four".to_string()), result[1]);

    let mut port = Port::new_port_c();
    let mut port_c = unsafe { port.as_mut_port_c() };
    stmt.execute_into_port(&(102,), 0, &mut port_c).unwrap();
    let decoded_port: Vec<(u8, String)> = decode_port(&port_c);
    assert_eq!(decoded_port, result);

    let mut stream = stmt.execute_raw(&(103,), 0).unwrap();
    let result = decode_dql_result::<Vec<(u8, String)>>(&mut stream);
    assert_eq!(1, result.len());
    assert_eq!((104, "four".to_string()), result[0]);

    let mut port = Port::new_port_c();
    let mut port_c = unsafe { port.as_mut_port_c() };
    stmt.execute_into_port(&(103,), 0, &mut port_c).unwrap();
    let decoded_port: Vec<(u8, String)> = decode_port(&port_c);
    assert_eq!(decoded_port, result);

    let stmt2 =
        tarantool::sql::prepare("SELECT * FROM SQL_TEST WHERE ID > ? AND VALUE = ?".to_string())
            .unwrap();
    let mut stream = stmt2.execute_raw(&(102, "three"), 0).unwrap();
    let result = decode_dql_result::<Vec<(u8, String)>>(&mut stream);
    assert_eq!(1, result.len());
    assert_eq!((103, "three".to_string()), result[0]);

    let mut port = Port::new_port_c();
    let mut port_c = unsafe { port.as_mut_port_c() };
    stmt2
        .execute_into_port(&(102, "three"), 0, &mut port_c)
        .unwrap();
    let decoded_port: Vec<(u8, String)> = decode_port(&port_c);
    assert_eq!(decoded_port, result);

    let mut stream = tarantool::sql::prepare_and_execute_raw(
        "SELECT * FROM SQL_TEST WHERE ID = ? AND VALUE = ?",
        &(101, "one"),
        0,
    )
    .unwrap();
    let result = decode_dql_result::<Vec<(u8, String)>>(&mut stream);
    assert_eq!(1, result.len());
    assert_eq!((101, "one".to_string()), result[0]);

    let mut port = Port::new_port_c();
    let mut port_c = unsafe { port.as_mut_port_c() };
    sql_execute_into_port(
        "SELECT * FROM SQL_TEST WHERE ID = ? AND VALUE = ?",
        &(101, "one"),
        0,
        &mut port_c,
    )
    .unwrap();
    let decoded_port: Vec<(u8, String)> = decode_port(&port_c);
    assert_eq!(decoded_port, result);

    unprepare(stmt).unwrap();
    drop_sql_test_space(sp).unwrap();
}

pub fn prepared_with_named_params() {
    let sp = create_sql_test_space("SQL_TEST").unwrap();

    sp.insert(&(1, "one")).unwrap();
    sp.insert(&(2, "two")).unwrap();
    sp.insert(&(3, "three")).unwrap();
    sp.insert(&(4, "four")).unwrap();

    fn bind_id(id: u64) -> HashMap<String, u64> {
        let mut map = HashMap::new();
        map.insert(":ID".to_string(), id);
        map
    }

    fn bind_name(name: &str) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert(":NAME".to_string(), name.to_string());
        map
    }

    let stmt =
        tarantool::sql::prepare("SELECT * FROM SQL_TEST WHERE ID > :ID".to_string()).unwrap();

    let mut stream = stmt.execute_raw(&[bind_id(2)], 0).unwrap();
    let result = decode_dql_result::<Vec<(u8, String)>>(&mut stream);
    assert_eq!(2, result.len());
    assert_eq!((3, "three".to_string()), result[0]);
    assert_eq!((4, "four".to_string()), result[1]);

    let mut port = Port::new_port_c();
    let mut port_c = unsafe { port.as_mut_port_c() };
    stmt.execute_into_port(&[bind_id(2)], 0, &mut port_c)
        .unwrap();
    let decoded_port: Vec<(u8, String)> = decode_port(&port_c);
    assert_eq!(decoded_port, result);

    let mut stream = stmt.execute_raw(&[bind_id(3)], 0).unwrap();
    let result = decode_dql_result::<Vec<(u8, String)>>(&mut stream);
    assert_eq!(1, result.len());
    assert_eq!((4, "four".to_string()), result[0]);

    let mut port = Port::new_port_c();
    let mut port_c = unsafe { port.as_mut_port_c() };
    stmt.execute_into_port(&[bind_id(3)], 0, &mut port_c)
        .unwrap();
    let decoded_port: Vec<(u8, String)> = decode_port(&port_c);
    assert_eq!(decoded_port, result);

    let stmt2 = tarantool::sql::prepare(
        "SELECT * FROM SQL_TEST WHERE ID > :ID AND VALUE = :NAME".to_string(),
    )
    .unwrap();
    let mut stream = stmt2
        .execute_raw(&(bind_id(2), bind_name("three")), 0)
        .unwrap();
    let result = decode_dql_result::<Vec<(u8, String)>>(&mut stream);
    assert_eq!(1, result.len());
    assert_eq!((3, "three".to_string()), result[0]);

    let mut port = Port::new_port_c();
    let mut port_c = unsafe { port.as_mut_port_c() };
    stmt2
        .execute_into_port(&(bind_id(2), bind_name("three")), 0, &mut port_c)
        .unwrap();
    let decoded_port: Vec<(u8, String)> = decode_port(&port_c);
    assert_eq!(decoded_port, result);

    let mut stream = tarantool::sql::prepare_and_execute_raw(
        "SELECT * FROM SQL_TEST WHERE ID = :ID AND VALUE = :NAME",
        &(bind_id(1), bind_name("one")),
        0,
    )
    .unwrap();
    let result = decode_dql_result::<Vec<(u8, String)>>(&mut stream);
    assert_eq!(1, result.len());
    assert_eq!((1, "one".to_string()), result[0]);

    let mut port = Port::new_port_c();
    let mut port_c = unsafe { port.as_mut_port_c() };
    sql_execute_into_port(
        "SELECT * FROM SQL_TEST WHERE ID = :ID AND VALUE = :NAME",
        &(bind_id(1), bind_name("one")),
        0,
        &mut port_c,
    )
    .unwrap();
    let decoded_port: Vec<(u8, String)> = decode_port(&port_c);
    assert_eq!(decoded_port, result);

    unprepare(stmt).unwrap();
    drop_sql_test_space(sp).unwrap();
}

pub fn port_c() {
    let tuple_refs = |tuple: &Tuple| unsafe { NonNull::new(tuple.as_ptr()).unwrap().as_ref() }.refs;
    let mut port = Port::new_port_c();
    let port_c = unsafe { port.as_mut_port_c() };

    // Check that we can iterate over an empty port.
    let mut iter = port_c.iter();
    assert_eq!(iter.next(), None);

    // Let's check that the data in the port can outlive
    // the original tuples after dropping them.
    {
        let tuple1 = Tuple::new(&("A",)).unwrap();
        port_c.add_tuple(&tuple1);
        assert_eq!(port_c.size(), 1);
        let mp1 = b"\x91\xa1B";
        unsafe { port_c.add_mp(mp1.as_slice()) };
        assert_eq!(port_c.size(), 2);
        let tuple2 = Tuple::new(&("C", "D")).unwrap();
        port_c.add_tuple(&tuple2);
        assert_eq!(port_c.size(), 3);
        let mp2 = b"\x91\xa1E";
        unsafe { port_c.add_mp(mp2.as_slice()) };
        assert_eq!(port_c.size(), 4);
    }
    let tuple3 = Tuple::new(&("F",)).unwrap();
    // The tuple has two references and it should not surprise you.
    // The first one is a long-live reference produces by the box_tuple_ref.
    // The second is temporary produced by tuple_bless when the tuple is added
    // to the output box_tuple_last pointer. The next tuple put to the
    // box_tuple_last decreases the reference count of the previous tuple.
    assert_eq!(tuple_refs(&tuple3), 2);
    let _ = Tuple::new(&("G",)).unwrap();
    assert_eq!(tuple_refs(&tuple3), 1);
    port_c.add_tuple(&tuple3);
    assert_eq!(tuple_refs(&tuple3), 2);

    let expected: Vec<Vec<String>> = vec![
        vec!["A".into()],
        vec!["B".into()],
        vec!["C".into(), "D".into()],
        vec!["E".into()],
        vec!["F".into()],
    ];
    let mut result = Vec::new();
    for mp_bytes in port_c.iter() {
        let entry: Vec<String> = rmp_serde::from_slice(mp_bytes).unwrap();
        result.push(entry);
    }
    assert_eq!(result, expected);

    // Check the last msgpack in the port.
    let last_mp = port_c.last_mp().unwrap();
    assert_eq!(last_mp, b"\x91\xa1F");

    // Check the first msgpack in the port.
    let first_mp = port_c.first_mp().unwrap();
    assert_eq!(first_mp, b"\x91\xa1A");

    // Check port destruction and the amount of references
    // in the tuples.
    drop(port);
    assert_eq!(tuple_refs(&tuple3), 1);
}

pub fn port_c_vtab() {
    #[no_mangle]
    unsafe extern "C" fn dump_msgpack_with_header(port: *mut Port, out: *mut Obuf) {
        // When we write data from the port to the out buffer we treat
        // the first msgpack as a header. All the other ones are treated
        // as an array of data. So, the algorithm:
        // 1. Write the first msgpack from the port.
        // 2. Write an array with the size of all other msgpacks.
        // 3. Write all other msgpacks to the out buffer.
        // If the port is empty, write MP_NULL.
        // If the port has only a single msgpack, write the msgpack and an empty array.

        let port_c: &PortC = NonNull::new_unchecked(port as *mut PortC).as_ref();
        if port_c.size() == 0 {
            obuf_append(out, b"\xC0").expect("Failed to append MP_NULL");
            return;
        }

        // Write the first msgpack from the port.
        let first_mp = port_c.first_mp().expect("Failed to get first msgpack");
        obuf_append(out, first_mp).expect("Failed to append first msgpack");

        // Write an array with the size of all other msgpacks.
        let size = (port_c.size() - 1) as u32;
        let mut array_len_buf = [0u8; 5];
        let mut cursor = Cursor::new(&mut array_len_buf[..]);
        write_array_len(&mut cursor, size).expect("Failed to write array length");
        let buf_len = cursor.position() as usize;
        obuf_append(out, &array_len_buf[..buf_len]).expect("Failed to append array length");

        for (idx, mp_bytes) in port_c.iter().enumerate() {
            // Skip the first msgpack.
            if idx == 0 {
                continue;
            }
            obuf_append(out, mp_bytes).expect("Failed to append msgpack");
        }
    }

    #[no_mangle]
    unsafe extern "C" fn dump_lua_with_panic(_port: *mut Port, _l: *mut lua_State, _is_flat: bool) {
        unimplemented!();
    }

    let vtab = PortVTable::new(dump_msgpack_with_header, dump_lua_with_panic);
    let mut out = ObufWrapper::new(100);

    // Check an empty port.
    let mut port = Port::new_port_c();
    let port_c = unsafe { port.as_mut_port_c() };
    port_c.vtab = &vtab as *const PortVTable;
    unsafe { dump_msgpack_with_header(port_c.as_mut_ptr(), out.obuf()) };
    let mut result = [0u8; 1];
    let len = out
        .read(&mut result)
        .expect("Failed to read from out buffer");
    assert_eq!(len, 1);
    assert_eq!(result[0], 0xC0);
    out.reset();

    // Check a port with a single msgpack.
    let header_mp = b"\xd96HEADER";
    unsafe { port_c.add_mp(header_mp) };
    unsafe { ((*port_c.vtab).dump_msgpack)(port_c.as_mut_ptr(), out.obuf()) };
    let expected = b"\xd96HEADER\x90";
    let mut result = [0u8; 9];
    let len = out
        .read(&mut result)
        .expect("Failed to read from out buffer");
    assert_eq!(len, expected.len());
    assert_eq!(&result[..], expected);
    out.reset();
    drop(port);

    // Check a port with multiple msgpacks.
    let mut port = Port::new_port_c();
    let port_c = unsafe { port.as_mut_port_c() };
    port_c.vtab = &vtab as *const PortVTable;
    let header_mp = b"\xd96HEADER";
    unsafe { port_c.add_mp(header_mp) };
    let mp1 = b"\xd95DATA1";
    unsafe { port_c.add_mp(mp1) };
    let mp2 = b"\xd95DATA2";
    unsafe { port_c.add_mp(mp2) };
    // Check that the C wrapper over the virtual `dump_msgpack` method works.
    unsafe { dump_msgpack_with_header(port_c.as_mut_ptr(), out.obuf()) };
    let expected = b"\xd96HEADER\x92\xd95DATA1\xd95DATA2";
    let mut result = [0u8; 23];
    let len = out
        .read(&mut result)
        .expect("Failed to read from out buffer");
    assert_eq!(len, expected.len());
    assert_eq!(&result[..], expected);

    // Check a manual drop of the port.
    let mut port = unsafe { Port::zeroed() };
    port.vtab = &vtab as *const PortVTable;
    let port_c = unsafe { port.as_mut_port_c() };
    unsafe { port_c.add_mp(b"\xd94DATA") };
    unsafe { ((*port.vtab).destroy)(port.as_mut()) };
    // Avoid double free.
    std::mem::forget(port);
}
