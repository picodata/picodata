use super::*;
use crate::ir::value::double::Double;
use crate::ir::value::Value;
use std::str::FromStr;
use tarantool::decimal::Decimal;

#[test]
fn cast1_test() {
    broadcast_check(
        r#"SELECT CAST('1' as string) FROM "t1""#,
        r#"SELECT CAST($1 AS string) as "col_1" FROM "t1""#,
        vec![Value::from("1")],
    );
}

#[test]
fn cast2_test() {
    broadcast_check(
        r#"SELECT CAST(true as bool) FROM "t1""#,
        r#"SELECT CAST($1 AS boolean) as "col_1" FROM "t1""#,
        vec![Value::from(true)],
    );
}

#[test]
fn cast3_test() {
    broadcast_check(
        r#"SELECT CAST(false as boolean) FROM "t1""#,
        r#"SELECT CAST($1 AS boolean) as "col_1" FROM "t1""#,
        vec![Value::from(false)],
    );
}

#[test]
fn cast4_test() {
    broadcast_check(
        r#"SELECT CAST('1.0' as decimal) FROM "t1""#,
        r#"SELECT CAST($1 AS decimal) as "col_1" FROM "t1""#,
        vec![Value::from(Decimal::from_str("1.0").unwrap())],
    );
}

#[test]
fn cast5_test() {
    broadcast_check(
        r#"SELECT CAST('1.0' as double) FROM "t1""#,
        r#"SELECT CAST($1 AS double) as "col_1" FROM "t1""#,
        vec![Value::from(Double::from(1.0))],
    );
}

#[test]
fn cast6_test() {
    broadcast_check(
        r#"SELECT CAST('1' as int) FROM "t1""#,
        r#"SELECT CAST($1 AS integer) as "col_1" FROM "t1""#,
        vec![Value::from(1)],
    );
}

#[test]
fn cast7_test() {
    broadcast_check(
        r#"SELECT CAST('1' as integer) FROM "t1""#,
        r#"SELECT CAST($1 AS integer) as "col_1" FROM "t1""#,
        vec![Value::from(1)],
    );
}

#[test]
fn cast8_test() {
    broadcast_check(
        r#"SELECT CAST(1 as string) FROM "t1""#,
        r#"SELECT CAST (CAST($1 AS unsigned) as string) as "col_1" FROM "t1""#,
        vec![Value::from(1_u64)],
    );
}

#[test]
fn cast9_test() {
    broadcast_check(
        r#"SELECT CAST(1 as text) FROM "t1""#,
        r#"SELECT CAST (CAST($1 AS unsigned) as text) as "col_1" FROM "t1""#,
        vec![Value::from(1_u64)],
    );
}

#[test]
fn cast10_test() {
    broadcast_check(
        r#"SELECT CAST('1' as unsigned) FROM "t1""#,
        r#"SELECT CAST($1 AS unsigned) as "col_1" FROM "t1""#,
        vec![Value::from(1 as u32)],
    );
}

#[test]
fn cast11_test() {
    broadcast_check(
        r#"SELECT CAST(1 as varchar(10)) FROM "t1""#,
        r#"SELECT CAST (CAST($1 AS unsigned) as varchar(10)) as "col_1" FROM "t1""#,
        vec![Value::from(1_u64)],
    );
}

#[test]
fn cast12_test() {
    broadcast_check(
        r#"SELECT CAST(trim("a") as varchar(100)) FROM "t1""#,
        r#"SELECT CAST (TRIM ("t1"."a") as varchar(100)) as "col_1" FROM "t1""#,
        vec![],
    );
}

#[test]
fn pgcast1_test() {
    broadcast_check(
        r#"SELECT true::bool FROM "t1""#,
        r#"SELECT CAST($1 AS boolean) as "col_1" FROM "t1""#,
        vec![Value::from(true)],
    );
}

#[test]
fn pgcast2_test() {
    broadcast_check(
        r#"SELECT false::bool FROM "t1""#,
        r#"SELECT CAST($1 AS boolean) as "col_1" FROM "t1""#,
        vec![Value::from(false)],
    );
}

#[test]
fn pgcast3_test() {
    broadcast_check(
        r#"SELECT '1.0'::decimal FROM "t1""#,
        r#"SELECT CAST($1 AS decimal) as "col_1" FROM "t1""#,
        vec![Value::from(Decimal::from_str("1.0").unwrap())],
    );
}

#[test]
fn pgcast4_test() {
    broadcast_check(
        r#"SELECT '1.0'::double FROM "t1""#,
        r#"SELECT CAST($1 AS double) as "col_1" FROM "t1""#,
        vec![Value::from(Double::from(1.0))],
    );
}

#[test]
fn pgcast5_test() {
    broadcast_check(
        r#"SELECT '1'::int FROM "t1""#,
        r#"SELECT CAST($1 AS integer) as "col_1" FROM "t1""#,
        vec![Value::from(1)],
    );
}

#[test]
fn pgcast6_test() {
    broadcast_check(
        r#"SELECT '1'::integer FROM "t1""#,
        r#"SELECT CAST($1 AS integer) as "col_1" FROM "t1""#,
        vec![Value::from(1)],
    );
}

#[test]
fn pgcast7_test() {
    broadcast_check(
        r#"SELECT 1::string FROM "t1""#,
        r#"SELECT CAST (CAST($1 AS unsigned) as string) as "col_1" FROM "t1""#,
        vec![Value::from(1_u64)],
    );
}

#[test]
fn pgcast8_test() {
    broadcast_check(
        r#"SELECT 1::text FROM "t1""#,
        r#"SELECT CAST (CAST($1 AS unsigned) as text) as "col_1" FROM "t1""#,
        vec![Value::from(1_u64)],
    );
}
