use super::*;
use insta::assert_yaml_snapshot;

#[test]
fn concat1_test() {
    let info = get_broadcast(r#"SELECT CAST('1' as string) || 'hello' FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS string) || CAST($2 AS string) as \"col_1\" FROM \"t1\""
      - - String: "1"
        - String: hello
    "#);
}

#[test]
fn concat2_test() {
    let info = get_broadcast(r#"SELECT trim('hello') || CAST(42 as string) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT TRIM (CAST($1 AS string)) || CAST (CAST($2 AS int) as string) as \"col_1\" FROM \"t1\""
      - - String: hello
        - Integer: 42
    "#);
}

#[test]
fn concat3_test() {
    let info = get_broadcast(r#"SELECT 'a' || 'b' FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS string) || CAST($2 AS string) as \"col_1\" FROM \"t1\""
      - - String: a
        - String: b
    "#);
}

#[test]
fn concat4_test() {
    let info = get_broadcast(
        r#"SELECT "a" FROM "t1" WHERE "a" || 'a' = CAST(42 as string) || trim('b') || 'a'"#,
    );
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT \"t1\".\"a\" FROM \"t1\" WHERE \"t1\".\"a\" || CAST($1 AS string) = (CAST (CAST($2 AS int) as string) || TRIM (CAST($3 AS string))) || CAST($4 AS string)"
      - - String: a
        - Integer: 42
        - String: b
        - String: a
    "#);
}
