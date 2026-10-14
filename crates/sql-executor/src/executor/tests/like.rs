use super::*;
use insta::assert_yaml_snapshot;

#[test]
fn like1_test() {
    let info = get_broadcast(r#"SELECT a || 'a' like 'ab' FROM t1"#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST ((CAST (\"t1\".\"a\" as text) || CAST($1 AS text)) as text) LIKE CAST($2 AS text) ESCAPE CAST($3 AS text) as \"col_1\" FROM \"t1\""
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
      - "SELECT CAST (\"t1\".\"a\" as text) LIKE CAST (\"t1\".\"a\" as text) ESCAPE CAST($1 AS text) as \"col_1\" FROM \"t1\""
      - - String: x
    "#);
}

#[test]
fn ilike_test() {
    let info = get_broadcast(r#"SELECT a ilike a escape 'x' FROM t1"#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST (lower (CAST (\"t1\".\"a\" as text)) as text) LIKE CAST (lower (CAST (\"t1\".\"a\" as text)) as text) ESCAPE CAST($1 AS text) as \"col_1\" FROM \"t1\""
      - - String: x
    "#);
}
