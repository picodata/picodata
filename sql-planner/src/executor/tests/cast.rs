use super::*;
use insta::assert_yaml_snapshot;

#[test]
fn cast1_test() {
    let info = get_broadcast(r#"SELECT CAST('1' as string) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS string) as \"col_1\" FROM \"t1\""
      - - String: "1"
    "#);
}

#[test]
fn cast2_test() {
    let info = get_broadcast(r#"SELECT CAST(true as bool) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS bool) as \"col_1\" FROM \"t1\""
      - - Boolean: true
    "#);
}

#[test]
fn cast3_test() {
    let info = get_broadcast(r#"SELECT CAST(false as bool) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS bool) as \"col_1\" FROM \"t1\""
      - - Boolean: false
    "#);
}

#[test]
fn cast4_test() {
    let info = get_broadcast(r#"SELECT CAST('1.0' as decimal) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS decimal) as \"col_1\" FROM \"t1\""
      - - Decimal:
            - 1
            - - 1
              - 1
              - 12
    "#);
}

#[test]
fn cast5_test() {
    let info = get_broadcast(r#"SELECT CAST('1.0' as double) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS double) as \"col_1\" FROM \"t1\""
      - - Double: 1
    "#);
}

#[test]
fn cast6_test() {
    let info = get_broadcast(r#"SELECT CAST('1' as int) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS int) as \"col_1\" FROM \"t1\""
      - - Integer: 1
    "#);
}

#[test]
fn cast7_test() {
    let info = get_broadcast(r#"SELECT CAST('1' as int) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS int) as \"col_1\" FROM \"t1\""
      - - Integer: 1
    "#);
}

#[test]
fn cast8_test() {
    let info = get_broadcast(r#"SELECT CAST(1 as string) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST (CAST($1 AS int) as string) as \"col_1\" FROM \"t1\""
      - - Integer: 1
    "#);
}

#[test]
fn cast9_test() {
    let info = get_broadcast(r#"SELECT CAST(1 as text) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST (CAST($1 AS int) as string) as \"col_1\" FROM \"t1\""
      - - Integer: 1
    "#);
}

#[test]
fn cast10_test() {
    let info = get_broadcast(r#"SELECT CAST('1' as int) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS int) as \"col_1\" FROM \"t1\""
      - - Integer: 1
    "#);
}

#[test]
fn cast11_test() {
    let info = get_broadcast(r#"SELECT CAST(1 as varchar(10)) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST (CAST($1 AS int) as string) as \"col_1\" FROM \"t1\""
      - - Integer: 1
    "#);
}

#[test]
fn cast12_test() {
    let info = get_broadcast(r#"SELECT CAST(trim("a") as varchar(100)) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST (TRIM (\"t1\".\"a\") as string) as \"col_1\" FROM \"t1\""
      - []
    "#);
}

#[test]
fn pgcast1_test() {
    let info = get_broadcast(r#"SELECT true::bool FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS bool) as \"col_1\" FROM \"t1\""
      - - Boolean: true
    "#);
}

#[test]
fn pgcast2_test() {
    let info = get_broadcast(r#"SELECT false::bool FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS bool) as \"col_1\" FROM \"t1\""
      - - Boolean: false
    "#);
}

#[test]
fn pgcast3_test() {
    let info = get_broadcast(r#"SELECT '1.0'::decimal FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS decimal) as \"col_1\" FROM \"t1\""
      - - Decimal:
            - 1
            - - 1
              - 1
              - 12
    "#);
}

#[test]
fn pgcast4_test() {
    let info = get_broadcast(r#"SELECT '1.0'::double FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS double) as \"col_1\" FROM \"t1\""
      - - Double: 1
    "#);
}

#[test]
fn pgcast5_test() {
    let info = get_broadcast(r#"SELECT '1'::int FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS int) as \"col_1\" FROM \"t1\""
      - - Integer: 1
    "#);
}

#[test]
fn pgcast6_test() {
    let info = get_broadcast(r#"SELECT '1'::int FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS int) as \"col_1\" FROM \"t1\""
      - - Integer: 1
    "#);
}

#[test]
fn pgcast7_test() {
    let info = get_broadcast(r#"SELECT 1::string FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST (CAST($1 AS int) as string) as \"col_1\" FROM \"t1\""
      - - Integer: 1
    "#);
}

#[test]
fn pgcast8_test() {
    let info = get_broadcast(r#"SELECT 1::text FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST (CAST($1 AS int) as string) as \"col_1\" FROM \"t1\""
      - - Integer: 1
    "#);
}
