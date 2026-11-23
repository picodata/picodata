use pretty_assertions::assert_eq;
use sql::explain::utils::format_query;
use sql::helpers::check_transformation;
use sql::ir::value::Value;
use sql::ir::Plan;

fn merge_tuples(plan: &mut Plan) {
    plan.merge_tuples().unwrap();
}

#[test]
fn merge_tuples1() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" = 1 and "b" = 2 and "c" < 3 and 4 < "a""#;
    let actual_pattern_params = check_transformation(input, vec![], &merge_tuples);

    assert_eq!(
        actual_pattern_params.params,
        vec![
            Value::from(1),
            Value::from(2),
            Value::from(3),
            Value::from(4),
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ("t"."a", "t"."b") = ($1::int, $2::int) and "t"."c" < $3::int and $4::int < "t"."a""#
    );
}

#[test]
fn merge_tuples2() {
    let input = r#"SELECT "a" FROM "t"
        WHERE "a" = 1 and null and "b" = 2
        or true and "c" >= 3 and 4 <= "a""#;
    let actual_pattern_params = check_transformation(input, vec![], &merge_tuples);

    assert_eq!(
        actual_pattern_params.params,
        vec![
            Value::from(2),
            Value::from(1),
            Value::Null,
            Value::from(3),
            Value::Boolean(true),
            Value::from(4),
        ]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ("t"."b", "t"."a") = ($1::int, $2::int) and $3 or "t"."c" >= $4::int and $5::bool and $6::int <= "t"."a""#
    );
}

#[test]
fn merge_tuples3() {
    let input = r#"SELECT "a" FROM "t" WHERE true"#;
    let actual_pattern_params = check_transformation(input, vec![], &merge_tuples);

    assert_eq!(actual_pattern_params.params, vec![Value::Boolean(true)]);
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE $1::bool"#
    );
}

#[test]
fn merge_tuples4() {
    let input = r#"SELECT "a" FROM "t" WHERE ("a", "b") = (1, 2) and 3 = "c""#;
    let actual_pattern_params = check_transformation(input, vec![], &merge_tuples);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::from(2), Value::from(3)]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ("t"."a", "t"."b", "t"."c") = ($1::int, $2::int, $3::int)"#
    );
}

#[test]
fn merge_tuples5() {
    let input = r#"SELECT "a" FROM "t" WHERE 3 < "c" and ("a", "b") > (1, 2)"#;
    let actual_pattern_params = check_transformation(input, vec![], &merge_tuples);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(1), Value::from(2), Value::from(3)]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE ("t"."a", "t"."b") > ($1::int, $2::int) and $3::int < "t"."c""#
    );
}

#[test]
fn merge_tuples6() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" <> 1 and "b" <> 2"#;
    let actual_pattern_params = check_transformation(input, vec![], &merge_tuples);

    assert_eq!(
        actual_pattern_params.params,
        vec![Value::from(2), Value::from(1)]
    );
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a" FROM "t" WHERE "t"."b" <> $1::int and "t"."a" <> $2::int"#
    );
}

#[test]
fn merge_tuples7() {
    let input = r#"
    select "a", "f" from "t" inner join "t2"
    on "t"."a" = "t2"."e" and "t2"."f" = "t"."b"
"#;
    // merge_tuples must group rows of the same table on the same
    // side of the equality for join conflict resultion to work
    // correctly, otherwise we will get Motion(Full) instead
    // local join here
    let actual_pattern_params = check_transformation(input, vec![], &merge_tuples);

    assert_eq!(actual_pattern_params.params, vec![]);
    insta::assert_snapshot!(
        actual_pattern_params.pattern,
        @r#"SELECT "t"."a", "t2"."f" FROM "t" INNER JOIN "t2" ON ("t"."a", "t"."b") = ("t2"."e", "t2"."f")"#
    );
}

#[test]
fn merge_tuples8() {
    let input = r#"
    select "a", "f" from "t" inner join "t2"
    on "t"."a" = "t"."b" and "t"."a" = "t2"."e" and "t2"."f" = "t"."b" and "t2"."f" = "t2"."e"
"#;
    // check merge tuple will create two groupes:
    // one with grouped columns and other group with all other equalities
    let actual_pattern_params = check_transformation(input, vec![], &merge_tuples);

    assert_eq!(actual_pattern_params.params, vec![]);
    insta::assert_snapshot!(actual_pattern_params.pattern, @r#"SELECT "t"."a", "t2"."f" FROM "t" INNER JOIN "t2" ON ("t"."b", "t"."a") = ("t2"."f", "t2"."e") and ("t2"."f", "t"."a") = ("t2"."e", "t"."b")"#);
}

fn merged_sql(query: &str) -> String {
    let pattern = check_transformation(query, vec![], &merge_tuples).pattern;
    format_query(&pattern, &[] as &[&str], true)
}

/// DNF leaves an operand of a comparison as it is, so the chains meet an `OR`
/// under `AND` there. The conjunction must stay whole.
#[test]
fn merge_tuples_keep_or_in_comparison() {
    let sql = merged_sql(
        r#"SELECT "a" FROM "t"
        WHERE (("a" = 1 OR "a" = 2) AND "b" = 3 AND "c" < 4) = false"#,
    );
    insta::assert_snapshot!(sql, @r#"
    SELECT
      "t"."a"
    FROM
      "t"
    WHERE
      (
        (
          "t"."a" = $1::int
          or "t"."a" = $2::int
        )
        and "t"."b" = $3::int
        and "t"."c" < $4::int
      ) = $5::bool
    "#);
}

/// DNF also leaves a condition with too many disjunctions as it is; the shape
/// is the same with fewer of them when only the tuples are merged. Under an
/// `OR` the local SQL has no other copy of the conjunction.
#[test]
fn merge_tuples_keep_or_under_or() {
    let sql = merged_sql(
        r#"SELECT "a" FROM "t"
        WHERE ("a" = 1 OR "b" = 2) AND "c" = 3 AND "d" = 4 OR "a" = 5"#,
    );
    insta::assert_snapshot!(sql, @r#"
    SELECT
      "t"."a"
    FROM
      "t"
    WHERE
      (
        "t"."a" = $1::int
        or "t"."b" = $2::int
      )
      and "t"."c" = $3::int
      and "t"."d" = $4::int
      or "t"."a" = $5::int
    "#);
}

#[test]
fn merge_tuples_keep_or_last() {
    let sql = merged_sql(
        r#"SELECT "a" FROM "t"
        WHERE ("a" > 0 AND "a" < 4 AND ("a" = 1 OR "a" = 2)) = false"#,
    );
    insta::assert_snapshot!(sql, @r#"
    SELECT
      "t"."a"
    FROM
      "t"
    WHERE
      (
        "t"."a" > $1::int
        and "t"."a" < $2::int
        and (
          "t"."a" = $3::int
          or "t"."a" = $4::int
        )
      ) = $5::bool
    "#);
}

/// The conjunction inside the `OR` is whole, so its tuple is merged.
#[test]
fn merge_tuples_inside_or_of_conjunction() {
    let sql = merged_sql(
        r#"SELECT "a" FROM "t"
        WHERE (("a" = 1 OR ("b" = 2 AND "c" = 3)) AND "b" = 4 AND "c" = 5) = false"#,
    );
    insta::assert_snapshot!(sql, @r#"
    SELECT
      "t"."a"
    FROM
      "t"
    WHERE
      (
        (
          "t"."a" = $1::int
          or ("t"."b", "t"."c") = ($2::int, $3::int)
        )
        and "t"."b" = $4::int
        and "t"."c" = $5::int
      ) = $6::bool
    "#);
}
