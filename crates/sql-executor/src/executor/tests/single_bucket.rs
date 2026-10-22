use super::*;
use crate::executor::engine::mock::{PortMocked, RouterRuntimeMock};
use crate::ir::value::Value;
use crate::test_helpers::ExecutingQueryExt;
use insta::assert_yaml_snapshot;

fn dispatch(sql: &str, params: Vec<Value>) -> Vec<DispatchInfo> {
    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, params).unwrap();
    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();
    port.decode()
}

#[test]
fn colocated_subquery() {
    let info = dispatch(
        "SELECT * FROM t5 WHERE a = $1 AND b IN (SELECT b FROM t3_2 WHERE a = $1)",
        vec![Value::from(7)],
    );
    assert_yaml_snapshot!(info, @r#"
    - Filtered:
        - - "SELECT \"t5\".\"a\", \"t5\".\"b\" FROM \"t5\" WHERE \"t5\".\"a\" = $1::int and \"t5\".\"b\" in (SELECT \"t3_2\".\"b\" FROM \"t3_2\" WHERE \"t3_2\".\"a\" = $2::int)"
          - - Integer: 7
            - Integer: 7
          - replicaset_0
          - - 2329
    "#);
}

#[test]
fn colocated_subquery_used_twice() {
    let info = dispatch(
        "SELECT * FROM t5 WHERE a = $1 AND (SELECT b FROM t3_2 WHERE a = $1) BETWEEN 1 AND 2",
        vec![Value::from(7)],
    );
    assert_yaml_snapshot!(info, @r#"
    - Filtered:
        - - "SELECT \"t5\".\"a\", \"t5\".\"b\" FROM \"t5\" WHERE \"t5\".\"a\" = $1::int and (SELECT \"t3_2\".\"b\" FROM \"t3_2\" WHERE \"t3_2\".\"a\" = $2::int) >= $3::int and (SELECT \"t3_2\".\"b\" FROM \"t3_2\" WHERE \"t3_2\".\"a\" = $2::int) <= $4::int"
          - - Integer: 7
            - Integer: 7
            - Integer: 1
            - Integer: 2
          - replicaset_0
          - - 2329
    "#);
}

#[test]
fn pinned_select_without_scan_at_the_root() {
    let info = dispatch(
        "SELECT EXISTS (SELECT 1 FROM t5 WHERE a = $1) AND (SELECT b FROM t3_2 WHERE a = $1) > 0",
        vec![Value::from(7)],
    );
    assert_yaml_snapshot!(info, @r#"
    - Filtered:
        - - "SELECT exists (SELECT $1::int as \"col_1\" FROM \"t5\" WHERE \"t5\".\"a\" = $2::int) and (SELECT \"t3_2\".\"b\" FROM \"t3_2\" WHERE \"t3_2\".\"a\" = $3::int) > $4::int as \"col_1\""
          - - Integer: 1
            - Integer: 7
            - Integer: 7
            - Integer: 0
          - replicaset_0
          - - 2329
    "#);
}

#[test]
fn pinned_select_without_scan_under_one_row_operators() {
    let info = dispatch(
        "SELECT c + 1 FROM (SELECT (SELECT b FROM t5 WHERE a = $1) AS c) WHERE c > 0 ORDER BY 1",
        vec![Value::from(7)],
    );
    assert_yaml_snapshot!(info, @r#"
    - Filtered:
        - - "SELECT \"col_1\" FROM (SELECT \"unnamed_subquery\".\"c\" + $1::int as \"col_1\" FROM (SELECT (SELECT \"t5\".\"b\" FROM \"t5\" WHERE \"t5\".\"a\" = $2::int) as \"c\") as \"unnamed_subquery\" WHERE \"unnamed_subquery\".\"c\" > $3::int) ORDER BY 1"
          - - Integer: 1
            - Integer: 7
            - Integer: 0
          - replicaset_0
          - - 2329
    "#);
    let info = dispatch(
        "SELECT DISTINCT c FROM (SELECT (SELECT b FROM t5 WHERE a = $1) AS c)",
        vec![Value::from(7)],
    );
    assert_yaml_snapshot!(info, @r#"
    - Filtered:
        - - "SELECT DISTINCT \"unnamed_subquery\".\"c\" FROM (SELECT (SELECT \"t5\".\"b\" FROM \"t5\" WHERE \"t5\".\"a\" = $1::int) as \"c\") as \"unnamed_subquery\""
          - - Integer: 7
          - replicaset_0
          - - 2329
    "#);
}

#[test]
fn pinned_select_without_scan_in_a_colocated_subquery() {
    let info = dispatch(
        "SELECT * FROM t5 WHERE a = $1 AND b = (SELECT (SELECT b FROM t3_2 WHERE a = $1) + 1)",
        vec![Value::from(7)],
    );
    assert_yaml_snapshot!(info, @r#"
    - Filtered:
        - - "SELECT \"t5\".\"a\", \"t5\".\"b\" FROM \"t5\" WHERE \"t5\".\"a\" = $1::int and \"t5\".\"b\" = (SELECT (SELECT \"t3_2\".\"b\" FROM \"t3_2\" WHERE \"t3_2\".\"a\" = $2::int) + $3::int as \"col_1\")"
          - - Integer: 7
            - Integer: 7
            - Integer: 1
          - replicaset_0
          - - 2329
    "#);
}

#[test]
fn pinned_select_without_scan_without_buckets() {
    let info = dispatch(
        "SELECT (SELECT b FROM t5 WHERE a = $1 AND a = $2)",
        vec![Value::from(1), Value::from(2)],
    );
    assert_yaml_snapshot!(info, @r#"
    - Any:
        - "SELECT (SELECT \"t5\".\"b\" FROM \"t5\" WHERE \"t5\".\"a\" = $1::int and \"t5\".\"a\" = $2::int) as \"col_1\""
        - - Integer: 1
          - Integer: 2
    "#);
}

#[test]
fn window_union_and_except() {
    let info = dispatch(
        "SELECT row_number() OVER (ORDER BY b) FROM t5 WHERE a = $1",
        vec![Value::from(7)],
    );
    assert_yaml_snapshot!(info, @r#"
    - Filtered:
        - - "SELECT row_number () OVER (ORDER BY \"t5\".\"b\" ASC) as \"col_1\" FROM \"t5\" WHERE \"t5\".\"a\" = $1::int"
          - - Integer: 7
          - replicaset_0
          - - 2329
    "#);
    let info = dispatch(
        "SELECT b FROM t5 WHERE a = $1 UNION SELECT b FROM t3_2 WHERE a = $1",
        vec![Value::from(7)],
    );
    assert_yaml_snapshot!(info, @r#"
    - Filtered:
        - - "SELECT \"t5\".\"b\" FROM \"t5\" WHERE \"t5\".\"a\" = $1::int UNION SELECT \"t3_2\".\"b\" FROM \"t3_2\" WHERE \"t3_2\".\"a\" = $2::int"
          - - Integer: 7
            - Integer: 7
          - replicaset_0
          - - 2329
    "#);
    let info = dispatch(
        "SELECT b FROM t5 WHERE a = $1 EXCEPT SELECT b FROM t3_2 WHERE a = $1",
        vec![Value::from(7)],
    );
    assert_yaml_snapshot!(info, @r#"
    - Filtered:
        - - "SELECT \"t5\".\"b\" FROM \"t5\" WHERE \"t5\".\"a\" = $1::int EXCEPT SELECT \"t3_2\".\"b\" FROM \"t3_2\" WHERE \"t3_2\".\"a\" = $2::int"
          - - Integer: 7
            - Integer: 7
          - replicaset_0
          - - 2329
    "#);
}

#[test]
fn union_all_of_scalar_aggregates_on_one_parameter() {
    let info = dispatch(
        "SELECT count(*) FROM t5 WHERE a = $1 UNION ALL SELECT count(*) FROM t3_2 WHERE a = $1",
        vec![Value::from(7)],
    );
    assert_yaml_snapshot!(info, @r#"
    - Filtered:
        - - "SELECT count (*) as \"col_1\" FROM \"t5\" WHERE \"t5\".\"a\" = $1::int UNION ALL SELECT count (*) as \"col_1\" FROM \"t3_2\" WHERE \"t3_2\".\"a\" = $2::int"
          - - Integer: 7
            - Integer: 7
          - replicaset_0
          - - 2329
    "#);
}

/// No buckets and no aggregate: nothing is dispatched.
#[test]
fn colocated_subquery_with_empty_input() {
    let info = dispatch(
        "SELECT * FROM t5 WHERE a = $1 AND a = $2 AND b IN (SELECT b FROM t3_2 WHERE a = $1)",
        vec![Value::from(1), Value::from(2)],
    );
    assert_yaml_snapshot!(info, @"- Filtered: []");
}
