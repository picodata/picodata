use super::*;
use insta::assert_yaml_snapshot;

#[test]
fn repeated_alias_inside_expression_test() {
    let info = get_broadcast(r#"SELECT 1 c2 FROM (SELECT 1) GROUP BY c2 + c2"#);
    assert_yaml_snapshot!(info, @r#"
    Any:
      - "SELECT $1::int as \"c2\" FROM (SELECT $2::int as \"col_1\") as \"unnamed_subquery\" GROUP BY $3::int + $4::int"
      - - Integer: 1
        - Integer: 1
        - Integer: 1
        - Integer: 1
    "#);
}

#[test]
fn repeated_alias_over_cte_test() {
    let info = get_broadcast(
        r#"WITH cte(a) AS (SELECT 1) SELECT DISTINCT a AS b FROM cte GROUP BY a, b, b, a"#,
    );
    assert_yaml_snapshot!(info, @r#"
    Any:
      - "SELECT DISTINCT \"cte\".\"a\" as \"b\" FROM (SELECT \"cte\".\"col_1\" as \"a\" FROM (SELECT $1::int as \"col_1\") as \"cte\") as \"cte\" GROUP BY \"cte\".\"a\", \"cte\".\"a\", \"cte\".\"a\", \"cte\".\"a\""
      - - Integer: 1
    "#);
}

/// `BETWEEN` copies its left operand, so the alias occurs twice.
#[test]
fn alias_in_between_test() {
    let info = get_broadcast(r#"SELECT 1 x FROM (SELECT 1) GROUP BY x BETWEEN 0 AND 2"#);
    assert_yaml_snapshot!(info, @r#"
    Any:
      - "SELECT $1::int as \"x\" FROM (SELECT $2::int as \"col_1\") as \"unnamed_subquery\" GROUP BY $3::int >= $4::int and $5::int <= $6::int"
      - - Integer: 1
        - Integer: 1
        - Integer: 1
        - Integer: 0
        - Integer: 1
        - Integer: 2
    "#);
}
