#![cfg(feature = "picodata")]

use tarantool::error::{Error, TarantoolError};
use tarantool::index::IndexType;
use tarantool::space::{Field, Space};

fn create_sql_test_space(name: &str) -> tarantool::Result<Space> {
    let space = Space::builder(name)
        .if_not_exists(true)
        .field(Field::unsigned("ID"))
        .field(Field::string("VALUE"))
        .create()?;
    space.index_builder("primary")
        .if_not_exists(true)
        .index_type(IndexType::Tree)
        .part(1)
        .create()?;
    space.index_builder("secondary")
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

pub fn prepared_invalid_query() {
    let maybe_stmt = tarantool::sql::prepare("SELECT * FROM UNKNOWN_SPACE");
    assert!(maybe_stmt.is_err());
    assert!(matches!(maybe_stmt.err().unwrap(), Error::Tarantool(TarantoolError { .. })));
}

pub fn prepared_source_query() {
    let sp = create_sql_test_space("SQL_TEST").unwrap();

    let stmt = tarantool::sql::prepare("SELECT * FROM SQL_TEST").unwrap();
    assert_eq!(stmt.source(), Ok("SELECT * FROM SQL_TEST"));

    drop_sql_test_space(sp).unwrap();
}

pub fn prepared_no_params() {
    let sp = create_sql_test_space("SQL_TEST").unwrap();

    sp.insert(&(1, "one")).unwrap();
    sp.insert(&(2, "two")).unwrap();
    sp.insert(&(3, "three")).unwrap();
    sp.insert(&(4, "four")).unwrap();

    let sql = "SELECT * FROM SQL_TEST";
    let stmt = tarantool::sql::prepare(sql).unwrap();

    let vec = stmt.execute::<_, Vec<(u64, String)>>(&()).unwrap();

    assert_eq!((1, "one".to_string()), vec[0]);
    assert_eq!((2, "two".to_string()), vec[1]);
    assert_eq!((3, "three".to_string()), vec[2]);
    assert_eq!((4, "four".to_string()), vec[3]);


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
    let sql = "SELECT * FROM SQL_TEST";
    let stmt = tarantool::sql::prepare(sql).unwrap();
    let result = stmt.execute::<_, Vec<(u64, String)>>(&()).unwrap();

    let mut i = 0;
    while i < 10000 {
        assert_eq!((i as u64 + 1, "one".to_string()), result[i]);
        assert_eq!((i as u64 + 2, "two".to_string()), result[i + 1]);
        assert_eq!((i as u64 + 3, "three".to_string()), result[i + 2]);
        assert_eq!((i as u64 + 4, "four".to_string()), result[i + 3]);
        i += 4;
    }

    drop_sql_test_space(sp).unwrap();
}

pub fn prepared_invalid_params() {
    let sp = create_sql_test_space("SQL_TEST").unwrap();

    let stmt = tarantool::sql::prepare("SELECT * FROM SQL_TEST WHERE ID > ?").unwrap();
    let result = stmt.execute::<_, Vec<(u8, String)>>(&("not uint value", ));

    assert!(result.is_err());
    assert!(matches!(result.err().unwrap(), Error::Tarantool(TarantoolError { .. })));

    drop_sql_test_space(sp).unwrap();
}

pub fn prepared_with_unnamed_params() {
    let sp = create_sql_test_space("SQL_TEST").unwrap();

    sp.insert(&(101, "one")).unwrap();
    sp.insert(&(102, "two")).unwrap();
    sp.insert(&(103, "three")).unwrap();
    sp.insert(&(104, "four")).unwrap();

    let stmt = tarantool::sql::prepare("SELECT * FROM SQL_TEST WHERE ID > ?").unwrap();

    let result = stmt.execute::<_, Vec<(u8, String)>>(&(102, )).unwrap();
    assert_eq!(2, result.len());
    assert_eq!((103, "three".to_string()), result[0]);
    assert_eq!((104, "four".to_string()), result[1]);

    let result = stmt.execute::<_, Vec<(u8, String)>>(&(103, )).unwrap();
    assert_eq!(1, result.len());
    assert_eq!((104, "four".to_string()), result[0]);

    let stmt2 = tarantool::sql::prepare("SELECT * FROM SQL_TEST WHERE ID > ? AND VALUE = ?").unwrap();
    let result = stmt2.execute::<_, Vec<(u8, String)>>(&(102, "three")).unwrap();
    assert_eq!(1, result.len());
    assert_eq!((103, "three".to_string()), result[0]);

    drop_sql_test_space(sp).unwrap();
}

pub fn prepared_with_named_params() {
    let sp = create_sql_test_space("SQL_TEST").unwrap();

    sp.insert(&(1, "one")).unwrap();
    sp.insert(&(2, "two")).unwrap();
    sp.insert(&(3, "three")).unwrap();
    sp.insert(&(4, "four")).unwrap();

    #[derive(serde::Serialize)]
    struct IdBind {
        #[serde(rename = ":ID")]
        id: u64,
    }

    #[derive(serde::Serialize)]
    struct NameBind {
        #[serde(rename = ":NAME")]
        name: String,
    }

    let stmt = tarantool::sql::prepare("SELECT * FROM SQL_TEST WHERE ID > :ID").unwrap();

    let result: Vec<(u8, String)> = stmt.execute(&(IdBind { id: 2 }, )).unwrap();
    assert_eq!(2, result.len());
    assert_eq!((3, "three".to_string()), result[0]);
    assert_eq!((4, "four".to_string()), result[1]);

    let result = stmt.execute::<_, Vec<(u8, String)>>(&(IdBind { id: 3 }, )).unwrap();
    assert_eq!(1, result.len());
    assert_eq!((4, "four".to_string()), result[0]);

    let stmt2 = tarantool::sql::prepare("SELECT * FROM SQL_TEST WHERE ID > :ID AND VALUE = :NAME").unwrap();
    let result = stmt2.execute::<_, Vec<(u8, String)>>(&(
        IdBind { id: 2 },
        NameBind { name: "three".to_string() }
    )).unwrap();
    assert_eq!(1, result.len());
    assert_eq!((3, "three".to_string()), result[0]);

    drop_sql_test_space(sp).unwrap();
}
