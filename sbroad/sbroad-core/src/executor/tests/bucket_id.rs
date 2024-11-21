use pretty_assertions::assert_eq;

use crate::backend::sql::ir::PatternWithParams;
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::result::ProducerResult;
use crate::ir::value::{LuaValue, Value};

use super::*;

#[test]
fn bucket1_test() {
    let sql = r#"SELECT *, "bucket_id" FROM "t1""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    expected.rows.push(vec![
        LuaValue::String("Execute query on all buckets".to_string()),
        LuaValue::String(String::from(PatternWithParams::new(
            r#"SELECT "t1"."a", "t1"."b", "t1"."bucket_id" FROM "t1""#.to_string(),
            vec![],
        ))),
    ]);
    assert_eq!(expected, result);
}

#[test]
fn bucket2_test() {
    let sql = r#"SELECT "a", "bucket_id", "b" FROM "t1"
        WHERE "a" = 1 AND "b" = 2"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();
    let param1 = Value::from(1_u64);
    let param2 = Value::from(2_u64);
    let bucket = query
        .coordinator
        .determine_bucket_id(&[&param1, &param2])
        .unwrap();

    expected.rows.push(vec![
        LuaValue::String(format!("Execute query on a bucket [{bucket}]")),
        LuaValue::String(String::from(PatternWithParams::new(
            format!(
                "{} {}",
                r#"SELECT "t1"."a", "t1"."bucket_id", "t1"."b" FROM "t1""#,
                r#"WHERE ("t1"."a") = (?) and ("t1"."b") = (?)"#,
            ),
            vec![param1, param2],
        ))),
    ]);
    assert_eq!(expected, result);
}

#[test]
fn bucket3_test() {
    let sql = r#"SELECT *, func('111') FROM "t1""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    expected.rows.push(vec![
        LuaValue::String("Execute query on all buckets".to_string()),
        LuaValue::String(String::from(PatternWithParams::new(
            r#"SELECT "t1"."a", "t1"."b", "func" (?) as "col_1" FROM "t1""#.to_string(),
            vec![Value::from("111".to_string())],
        ))),
    ]);
    assert_eq!(expected, result);
}

#[test]
fn sharding_key_from_tuple1() {
    let coordinator = RouterRuntimeMock::new();
    let tuple = vec![Value::from("123"), Value::from(1_u64)];
    let sharding_key = coordinator
        .extract_sharding_key_from_tuple("t1".into(), &tuple)
        .unwrap();
    assert_eq!(sharding_key, vec![&Value::from("123"), &Value::from(1_u64)]);
}
