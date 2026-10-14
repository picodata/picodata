use crate::explain::utils::format_sql;

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
