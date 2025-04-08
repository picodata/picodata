#![cfg(feature = "picodata")]

use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::io::Read;
use std::ptr::NonNull;
use tarantool::error::{Error, TarantoolError};
use tarantool::ffi::sql::{PortC, IPROTO_DATA};
use tarantool::index::IndexType;
use tarantool::space::{Field, Space};
use tarantool::sql::unprepare;
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

    let sql = "SELECT * FROM SQL_TEST WHERE ID = 1";
    let mut stream = tarantool::sql::prepare_and_execute_raw(sql, &(), 100).unwrap();
    let result = decode_dql_result::<Vec<(u64, String)>>(&mut stream);
    assert_eq!(1, result.len());
    assert_eq!((1, "one".to_string()), result[0]);

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

    let mut stream = stmt.execute_raw(&(103,), 0).unwrap();
    let result = decode_dql_result::<Vec<(u8, String)>>(&mut stream);
    assert_eq!(1, result.len());
    assert_eq!((104, "four".to_string()), result[0]);

    let stmt2 =
        tarantool::sql::prepare("SELECT * FROM SQL_TEST WHERE ID > ? AND VALUE = ?".to_string())
            .unwrap();
    let mut stream = stmt2.execute_raw(&(102, "three"), 0).unwrap();
    let result = decode_dql_result::<Vec<(u8, String)>>(&mut stream);
    assert_eq!(1, result.len());
    assert_eq!((103, "three".to_string()), result[0]);

    let mut stream = tarantool::sql::prepare_and_execute_raw(
        "SELECT * FROM SQL_TEST WHERE ID = ? AND VALUE = ?",
        &(101, "one"),
        0,
    )
    .unwrap();
    let result = decode_dql_result::<Vec<(u8, String)>>(&mut stream);
    assert_eq!(1, result.len());
    assert_eq!((101, "one".to_string()), result[0]);

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

    let mut stream = stmt.execute_raw(&[bind_id(3)], 0).unwrap();
    let result = decode_dql_result::<Vec<(u8, String)>>(&mut stream);
    assert_eq!(1, result.len());
    assert_eq!((4, "four".to_string()), result[0]);

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

    let mut stream = tarantool::sql::prepare_and_execute_raw(
        "SELECT * FROM SQL_TEST WHERE ID = :ID AND VALUE = :NAME",
        &(bind_id(1), bind_name("one")),
        0,
    )
    .unwrap();
    let result = decode_dql_result::<Vec<(u8, String)>>(&mut stream);
    assert_eq!(1, result.len());
    assert_eq!((1, "one".to_string()), result[0]);

    unprepare(stmt).unwrap();
    drop_sql_test_space(sp).unwrap();
}

pub fn port_c() {
    let tuple_refs = |tuple: &Tuple| unsafe { NonNull::new(tuple.as_ptr()).unwrap().as_ref() }.refs;
    let mut port_c = PortC::default();

    // Check that we can iterate over an empty port.
    let mut iter = port_c.iter();
    assert_eq!(iter.next(), None);

    // Let's check that the data in the port can outlive
    // the original tuples after dropping them.
    {
        let mut tuple1 = Tuple::new(&("A",)).unwrap();
        port_c.add_tuple(&mut tuple1);
        let mp1 = b"\x91\xa1B";
        unsafe { port_c.add_mp(mp1.as_slice()) };
        let mut tuple2 = Tuple::new(&("C", "D")).unwrap();
        port_c.add_tuple(&mut tuple2);
        let mp2 = b"\x91\xa1E";
        unsafe { port_c.add_mp(mp2.as_slice()) };
    }
    let mut tuple3 = Tuple::new(&("F",)).unwrap();
    // The tuple has two references and it should not surprise you.
    // The first one is a long-live reference produces by the box_tuple_ref.
    // The second is temporary produced by tuple_bless when the tuple is added
    // to the output box_tuple_last pointer. The next tuple put to the
    // box_tuple_last decreases the reference count of the previous tuple.
    assert_eq!(tuple_refs(&tuple3), 2);
    let _ = Tuple::new(&("G",)).unwrap();
    assert_eq!(tuple_refs(&tuple3), 1);
    port_c.add_tuple(&mut tuple3);
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

    // Check port destruction and the amount of references
    // in the tuples.
    drop(port_c);
    assert_eq!(tuple_refs(&tuple3), 1);
}
