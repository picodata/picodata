use super::*;
use crate::ir::value::Value;
#[test]
fn concat1_test() {
    broadcast_check(
        r#"SELECT CAST('1' as string) || 'hello' FROM "t1""#,
        r#"SELECT CAST($1 AS string) || CAST($2 AS string) as "col_1" FROM "t1""#,
        vec![Value::from("1"), Value::from("hello")],
    );
}

#[test]
fn concat2_test() {
    broadcast_check(
        r#"SELECT trim('hello') || CAST(42 as string) FROM "t1""#,
        r#"SELECT TRIM (CAST($1 AS string)) || CAST (CAST($2 AS int) as string) as "col_1" FROM "t1""#,
        vec![Value::from("hello"), Value::from(42)],
    );
}

#[test]
fn concat3_test() {
    broadcast_check(
        r#"SELECT 'a' || 'b' FROM "t1""#,
        r#"SELECT CAST($1 AS string) || CAST($2 AS string) as "col_1" FROM "t1""#,
        vec![Value::from("a"), Value::from("b")],
    );
}

#[test]
fn concat4_test() {
    broadcast_check(
        r#"SELECT "a" FROM "t1" WHERE "a" || 'a' = CAST(42 as string) || trim('b') || 'a'"#,
        r#"SELECT "t1"."a" FROM "t1" WHERE ("t1"."a" || CAST($1 AS string)) = ((CAST (CAST($2 AS int) as string) || TRIM (CAST($3 AS string))) || CAST($4 AS string))"#,
        vec![
            Value::from("a"),
            Value::from(42),
            Value::from("b"),
            Value::from("a"),
        ],
    );
}
