use crate::explain::utils::{format_query, format_sql};

#[test]
fn negative_value_before_cast() {
    let sql = r#"SELECT "t"."a" FROM "t" WHERE "t"."a" = $1::int"#;
    insta::assert_snapshot!(format_sql(sql, &["-1"], false), @r#"SELECT "t"."a" FROM "t" WHERE "t"."a" = (-1)::int"#);
}

#[test]
fn negative_value_without_cast() {
    let sql = r#"SELECT "t"."a" - $1 FROM "t" WHERE "t"."a" in ($2, $3)"#;
    insta::assert_snapshot!(format_sql(sql, &["-1", "-2", "3"], false), @r#"SELECT "t"."a" - -1 FROM "t" WHERE "t"."a" in (-2, 3)"#);
}

#[test]
fn non_negative_values_before_cast() {
    let sql = r#"SELECT $1::int, $2::text, $3::text"#;
    insta::assert_snapshot!(format_sql(sql, &["1", "'-1'", "'a'"], false), @"SELECT 1::int, '-1'::text, 'a'::text");
}

#[test]
fn placeholder_text_in_quotes_is_not_a_cast() {
    let sql = r#"SELECT "$1::int", '$2::int', $1, $2 FROM "t""#;
    insta::assert_snapshot!(format_sql(sql, &["-1", "-2"], false), @r#"SELECT "$1::int", '$2::int', -1, -2 FROM "t""#);
}

/// Without parameters, a short query is on one line and the placeholders
/// stay in it. This is the `pico_sql_pretty` SQL function.
#[test]
fn short_query_without_params() {
    let sql = "select   a,b from t   where a=$1 and b = ?";
    insta::assert_snapshot!(format_query(sql, &[] as &[String], true), @"select a, b from t where a = $1 and b = ?");
}

/// A query of at least `LINE_WIDTH` characters gets a line for each clause.
#[test]
fn long_query_without_params() {
    let sql = "select t.a, t.b, count(*) from some_long_table_name t join other_table o on t.a = o.a where t.b > 10 group by t.a, t.b order by 3 desc";
    insta::assert_snapshot!(format_query(sql, &[] as &[String], true), @"
    select
      t.a,
      t.b,
      count(*)
    from
      some_long_table_name t
      join other_table o on t.a = o.a
    where
      t.b > 10
    group by
      t.a,
      t.b
    order by
      3 desc
    ");
}

/// The formatter takes quadratic time or memory on some text, so such a query
/// stays as it is.
#[test]
fn query_too_hard_to_format_is_not_changed() {
    let format = |sql: &str| format_query(sql, &[] as &[String], true);
    let is_formatted = |sql: &str| format(sql).contains('\n');
    let is_unchanged = |sql: &str| format(sql) == sql;

    // The extra spaces show that the formatter did not touch the text.
    let long_name = "a".repeat(100);
    let parens = |n: usize| format!("select  {}{long_name}{}", "(".repeat(n), ")".repeat(n));
    assert!(is_formatted(&parens(64)));
    assert!(is_unchanged(&parens(65)));

    let cases = |n: usize| {
        format!(
            "select  {}{long_name}{}",
            "case ".repeat(n),
            " end".repeat(n)
        )
    };
    assert!(is_formatted(&cases(64)));
    assert!(is_unchanged(&cases(65)));

    // Closed blocks do not count, and a parenthesis in a literal is no block.
    let siblings = format!("select  {}", "(((a))), ".repeat(100));
    assert!(is_formatted(siblings.trim_end_matches(", ")));
    let literal = format!("select  '{}', {long_name}", "(".repeat(100));
    assert!(is_formatted(&literal));

    // A word without spaces.
    let word = |n: usize| format!("select  {}", "a,".repeat(n / 2));
    assert!(is_formatted(&word(1024)));
    assert!(is_unchanged(&word(1026)));

    // A long query, whatever its shape.
    let long = |n: usize| format!("select  {}", "a ".repeat((n - 8) / 2));
    assert!(is_formatted(&long(64 * 1024)));
    assert!(is_unchanged(&long(64 * 1024 + 2)));
}

/// On one line, a line comment would take the rest of the query.
#[test]
fn query_with_line_comment_is_not_changed() {
    let sql = "select a -- the key\nfrom t";
    insta::assert_snapshot!(format_query(sql, &[] as &[String], false), @r"
    select a -- the key
    from t
    ");
}

/// A line comment at the end of the query takes nothing.
#[test]
fn query_with_line_comment_at_the_end_is_formatted() {
    let sql = "select   a,b from t -- all rows";
    insta::assert_snapshot!(format_query(sql, &[] as &[String], false), @"select a, b from t -- all rows");
}

/// The formatter reads a backslash as an escape, and SQL does not.
#[test]
fn query_with_backslash_in_string_is_not_changed() {
    let sql = r"select a from t where a like '%\' or b='x'";
    insta::assert_snapshot!(format_query(sql, &[] as &[String], false), @r"select a from t where a like '%\' or b='x'");
}

/// A query that is not changed gets no values for its placeholders.
#[test]
fn query_with_params_is_not_changed() {
    let sql = "select a -- the key\nfrom t where a = $1";
    insta::assert_snapshot!(format_query(sql, &["1"], false), @r"
    select a -- the key
    from t where a = $1
    ");
}

/// The parts of operators, numbers and quoted text stay together.
#[test]
fn query_with_operators_and_literals_is_formatted() {
    let sql =
        r#"select a::int,b->>'x',"A  b",'it''s  y',1.5e-3 from t where a<>1 and b>=2 and c||d='x'"#;
    insta::assert_snapshot!(format_query(sql, &[] as &[String], false), @r#"select a::int, b ->> 'x', "A  b", 'it''s  y', 1.5e-3 from t where a <> 1 and b >= 2 and c || d = 'x'"#);
}
