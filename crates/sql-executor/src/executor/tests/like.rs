use super::*;
use insta::assert_yaml_snapshot;

#[test]
fn like1_test() {
    let info = get_broadcast(r#"SELECT a || 'a' like 'ab' FROM t1"#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT (\"t1\".\"a\"::text || $1::text)::text LIKE $2::text ESCAPE $3::text as \"col_1\" FROM \"t1\""
      - - String: a
        - String: ab
        - String: "\\"
    "#);
}

#[test]
fn like2_test() {
    let info = get_broadcast(r#"SELECT a like a escape 'x' FROM t1"#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT \"t1\".\"a\"::text LIKE \"t1\".\"a\"::text ESCAPE $1::text as \"col_1\" FROM \"t1\""
      - - String: x
    "#);
}

#[test]
fn ilike_test() {
    let info = get_broadcast(r#"SELECT a ilike a escape 'x' FROM t1"#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT lower (\"t1\".\"a\"::text)::text LIKE lower (\"t1\".\"a\"::text)::text ESCAPE $1::text as \"col_1\" FROM \"t1\""
      - - String: x
    "#);
}
