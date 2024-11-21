use super::*;
use crate::ir::value::Value;

#[test]
fn like1_test() {
    broadcast_check(
        r#"SELECT a || 'a' like 'ab' FROM t1"#,
        r#"SELECT (("t1"."a") || (?)) LIKE (?) ESCAPE (?) as "col_1" FROM "t1""#,
        vec![Value::from("a"), Value::from("ab"), Value::from("\\")],
    );
}

#[test]
fn like2_test() {
    broadcast_check(
        r#"SELECT a like a escape 'x' FROM t1"#,
        r#"SELECT ("t1"."a") LIKE ("t1"."a") ESCAPE (?) as "col_1" FROM "t1""#,
        vec![Value::from("x")],
    );
}

#[test]
fn ilike_test() {
    broadcast_check(
        r#"SELECT a ilike a escape 'x' FROM t1"#,
        r#"SELECT (lower ("t1"."a")) LIKE (lower ("t1"."a")) ESCAPE (?) as "col_1" FROM "t1""#,
        vec![Value::from("x")],
    );
}
