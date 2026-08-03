use super::*;
use insta::assert_yaml_snapshot;

#[test]
fn explicit_cast_and_literal_operands_test() {
    let info = get_broadcast(r#"SELECT CAST('1' as string) || 'hello' FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS string) || CAST($2 AS string) as \"col_1\" FROM \"t1\""
      - - String: "1"
        - String: hello
    "#);
}

#[test]
fn scalar_function_and_explicit_cast_operands_test() {
    let info = get_broadcast(r#"SELECT trim('hello') || CAST(42 as string) FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST (TRIM (CAST($1 AS string)) as string) || CAST (CAST (CAST($2 AS int) as string) as string) as \"col_1\" FROM \"t1\""
      - - String: hello
        - Integer: 42
    "#);
}

#[test]
fn two_string_literal_operands_test() {
    let info = get_broadcast(r#"SELECT 'a' || 'b' FROM "t1""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS string) || CAST($2 AS string) as \"col_1\" FROM \"t1\""
      - - String: a
        - String: b
    "#);
}

#[test]
fn column_operand_and_chained_concat_without_parens_test() {
    let info = get_broadcast(
        r#"SELECT "a" FROM "t1" WHERE "a" || 'a' = CAST(42 as string) || trim('b') || 'a'"#,
    );
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT \"t1\".\"a\" FROM \"t1\" WHERE CAST (\"t1\".\"a\" as string) || CAST($1 AS string) = CAST ((CAST (CAST (CAST($2 AS int) as string) as string) || CAST (TRIM (CAST($3 AS string)) as string)) as string) || CAST($4 AS string)"
      - - String: a
        - Integer: 42
        - String: b
        - String: a
    "#);
}

/// Regression test for a panic previously raised by `Plan::set_target_in_subtree`
/// when a `GROUP BY` grouping expression wraps a projection alias placeholder
/// directly under `||` (e.g. `GROUP BY 'x' || alias`). The concat cast pass
/// used to unwrap the placeholder `Alias` node in place, dropping it from the
/// tree before `fix_groupby_aliases` could resolve it.
#[test]
fn group_by_alias_placeholder_under_concat_reports_grouping_error_test() {
    let coordinator = RouterRuntimeMock::new();
    let err = ExecutingQuery::from_text_and_params(
        &coordinator,
        r#"SELECT "a" AS a_alias FROM "t1" GROUP BY 'x' || a_alias"#,
        vec![],
    )
    .unwrap_err();
    assert_snapshot!(err.to_string(), @r#"invalid query: column "a" is not found in grouping expressions!"#);
}

#[test]
fn integer_operands_are_rejected_test() {
    let coordinator = RouterRuntimeMock::new();
    let err =
        ExecutingQuery::from_text_and_params(&coordinator, "SELECT 1 || 2", vec![]).unwrap_err();
    assert_snapshot!(err.to_string(), @"could not resolve operator overload for ||(int, int)");
}

#[test]
fn array_operand_is_rejected_test() {
    let coordinator = RouterRuntimeMock::new();
    let err = ExecutingQuery::from_text_and_params(
        &coordinator,
        r#"SELECT 'x' || "b" FROM "arr_t""#,
        vec![],
    )
    .unwrap_err();
    assert_snapshot!(err.to_string(), @"could not resolve operator overload for ||(text, int[])");
}

#[test]
fn boolean_column_operand_is_cast_to_string_test() {
    let info = get_broadcast(r#"SELECT 'x' || "b" FROM "bool_sharded""#);
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT CAST($1 AS string) || CAST (\"bool_sharded\".\"b\" as string) as \"col_1\" FROM \"bool_sharded\""
      - - String: x
    "#);
}

/// Regression test pinning `||` overload resolution for a parameter with no
/// statically known type (e.g. a `Parse`-phase `$1` with no type hint). All
/// same-cost `(Text, S)` overloads tie on such an argument. Resolution should always
/// settle on `(Text, Text)`, and the parameter should not be wrapped in a cast.
#[test]
fn untyped_null_parameter_resolves_concat_as_text_test() {
    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(
        &coordinator,
        r#"SELECT 'x' || $1 FROM "t1""#,
        vec![Value::Null],
    )
    .unwrap();
    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();
    let info = port.decode();
    assert_yaml_snapshot!(info, @r#"
    - All:
        - "SELECT CAST($1 AS string) || $2 as \"col_1\" FROM \"t1\""
        - - String: x
          - "Null"
    "#);
}
