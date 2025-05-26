use crate::{
    errors::SbroadError,
    executor::engine::mock::RouterConfigurationMock,
    frontend::{sql::ast::AbstractSyntaxTree, Ast},
    ir::Plan,
};

fn parse(query: &str) -> Result<Plan, SbroadError> {
    let metadata = &RouterConfigurationMock::new();
    AbstractSyntaxTree::transform_into_plan(query, &[], metadata)
}

#[track_caller]
fn assert_fails_with_error(sql: &str, err_msg: &str) {
    let err = parse(sql).unwrap_err();
    assert_eq!(err.to_string(), err_msg)
}

#[track_caller]
fn assert_ok(sql: &str) {
    assert!(parse(sql).is_ok());
}

#[test]
fn arithmetic() {
    assert_ok("select 1 + 1");
    assert_ok("select 1 - 1.5");
    assert_ok("select 1.5 * 1.5");
    assert_ok("select 1.5 / -1");
    assert_ok("select 1.5 * -1.5");
    assert_ok("select 1.5 + $1");

    assert_ok("select 1 - $1");
    assert_ok("select 1.5 + ($1 + $2)");
    assert_ok("select $1 + $2 + $3 + 4");
    assert_ok("select ($1 + $2 + $3) + 4");

    // TODO: support default parameter types
    //assert_ok("with t(a) as (select $1) select a + 4 from t");
    assert_ok("with t(a) as (select $1 + 1) select a + 4 from t");
}

#[test]
fn comparison() {
    assert_ok("select 1 = 2");
    assert_ok("select 1 < 2.5");
    assert_ok("select $1 <= 2.5");

    assert_ok("select ($1, $2) > (1, 2.5)");
    assert_ok("select ($1, 'kek') >= (1, $2)");
    assert_ok("select (1, 2) <> (select 1, 2)");
    assert_ok("select (select 1, 2) = (values (1, 2))");

    assert_ok("select 1 in (1, 2, 3)");
    assert_ok("select (select 1, 2) in (values (1, 2), (2, 3))");
    assert_ok("select 1 in (values (1), (2), (3))");
    assert_ok("select (select 1, 2) in (values (1, 2))");

    assert_ok("select LOCALTIMESTAMP = LOCALTIMESTAMP");
    assert_ok("select LOCALTIMESTAMP in (LOCALTIMESTAMP, LOCALTIMESTAMP)");

    assert_ok("select (1, 2) = (1, '2')");
    assert_ok("select 1 in (1, '2')");
    assert_ok("select LOCALTIMESTAMP = '2023-07-07T12:34:56Z'");

    assert_fails_with_error(
        "select 1 in (1, false)",
        "IN types int, int and bool cannot be matched",
    );

    assert_fails_with_error(
        "select 1 in (1 * 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid, false)",
        "could not resolve operator overload for *(int, uuid)",
    );

    assert_fails_with_error(
        "select (1, 2) <> (select 1, 'kek')",
        "could not resolve operator overload for <>(int, text)",
    );
}

#[test]
fn arithmetic_errors() {
    assert_fails_with_error(
        "select 1 + false",
        "could not resolve operator overload for +(int, bool)",
    );

    assert_fails_with_error(
        "select 1 + (1 = 2)",
        "could not resolve operator overload for +(int, bool)",
    );

    assert_fails_with_error(
        "select count(true + true) from t;",
        "could not resolve operator overload for +(bool, bool)",
    );

    assert_fails_with_error(
        "select (1.5 + 1::double) + LOCALTIMESTAMP from t;",
        "could not resolve operator overload for +(double, datetime)",
    );

    assert_fails_with_error(
        "select 1 + 'kek'",
        "failed to parse 'kek' as a value of type int, consider using explicit type casts",
    );
}

#[test]
fn comparison_errors() {
    assert_fails_with_error(
        "select (select LOCALTIMESTAMP, 2) = (select 1, 2)",
        "could not resolve operator overload for =(datetime, int)",
    );

    assert_fails_with_error(
        "select (1, 2) = (select 1, '2')",
        "could not resolve operator overload for =(int, text)",
    );

    assert_fails_with_error(
        "select (1, 2) = (1, 2, 3)",
        "unequal number of entries in row expression: 2 and 3",
    );

    assert_fails_with_error(
        "select (1, 2) = (select 1, 2, 3)",
        "subquery returns 3 columns, expected 2",
    );

    assert_fails_with_error("select 1 = (1, 2, 3)", "row value misused");

    assert_fails_with_error(
        "select (1, 2) in (1, 2, 3)",
        "IN operator for rows is not supported",
    );

    assert_fails_with_error(
        "select (select 1, 2) in (1, 2, 3)",
        "subquery must return only one column",
    );

    assert_fails_with_error(
        "select (select 1, 2) in (select 1, 2, 3)",
        "subquery returns 2 columns, expected 3",
    );
}

#[test]
fn functions() {
    assert_ok("select null like '2'");
    assert_ok("select null like null");
    assert_ok("select null like null escape null");
    assert_ok("select '1' like '2'");
    assert_ok("select '1' like '2' escape '3'");
    assert_ok("select '1' || '2' like '2' escape '3'");
    assert_ok("select '1' || '2' like $2 escape '3'");
    assert_ok("select '1' || '2' like '2' escape $3");
    assert_ok("select $1 like $2 escape $3");
    assert_ok("select $1 || $2 like $3 escape $4");

    assert_ok("select trim(both null from null)");
    assert_ok("select trim(both from 'yxTomxx')");
    assert_ok("select trim(both 'kek' from 'yxTomxx')");
    assert_ok("select trim(both $1 from $2)");
    assert_ok("select trim(both $1 from $2) || trim(both $3 from $4)");

    assert_ok("with t as (select 1 as a) select max(a) from t;");
    assert_ok("with t as (select 1 as a) select sum(a + 1) from t;");
    assert_ok("with t as (select 1 as a) select min(a - $1) from t;");
    assert_ok("with t as (select 1 as a) select avg((a / a)) from t;");
    assert_ok("with t as (select 1::text as a) select max(a) from t;");
    assert_ok("with t as (select 1 as a) select max(a) = max(a + 1) from t;");
}

#[test]
fn functions_errors() {
    assert_fails_with_error(
        "select 1 like 2",
        "could not resolve function overload for like(int, int, text)",
    );
    assert_fails_with_error(
        "select '1' like 2",
        "could not resolve function overload for like(text, int, text)",
    );
    assert_fails_with_error(
        "select '1' like '2' escape 3",
        "could not resolve function overload for like(text, text, int)",
    );

    assert_fails_with_error(
        "select trim(both null from 1 + 1)",
        "could not resolve function overload for trim(unknown, int)",
    );
    assert_fails_with_error(
        "select trim(both 'kek' from  $1 + $2)",
        "could not resolve function overload for trim(text, int)",
    );
}

#[test]
fn case() {
    assert_ok(
        r#"
        WITH params(input) AS (SELECT 'kiwi' AS input)
        SELECT 
          CASE (SELECT input FROM params)
            WHEN 'apple' THEN 'fruit'
            WHEN 'carrot' THEN 'vegetable'
            WHEN 'kiwi' THEN 'exotic fruit'
            ELSE 'unknown'
          END AS food_type;
        "#,
    );

    assert_ok(
        r#"
        WITH numbers(num) AS (
          SELECT 5.0 UNION ALL
          SELECT 7.5 UNION ALL
          SELECT 10.0
        )
        SELECT num,
          CASE
            WHEN num = 5 THEN 'exact_int'
            WHEN num = 5.0 THEN 'exact_float'
            WHEN num > 6 THEN 'gt_six'
            ELSE 'other'
          END AS category
        FROM numbers;
        "#,
    );

    assert_ok(
        r#"
        WITH transactions(amt) AS (
          SELECT CAST(10 AS DECIMAL) UNION ALL
          SELECT 25.99 UNION ALL
          SELECT CAST(50 AS DECIMAL)
        )
        SELECT amt,
          CASE
            WHEN amt < 20 THEN 0
            WHEN amt < 40 THEN 1.5
            ELSE CAST(2.0 AS DOUBLE)
          END AS tax_rate
        FROM transactions;
        "#,
    );

    assert_ok(
        r#"
        WITH transactions(amt) AS (
          SELECT CAST(10 AS DECIMAL) UNION ALL
          SELECT 25.99 UNION ALL
          SELECT CAST(50 AS DECIMAL)
        )
        SELECT amt,
          CASE
            WHEN amt < 20 THEN 0
            WHEN amt < 40 THEN 1.5
            ELSE CAST(2.0 AS DOUBLE)
          END AS tax_rate
        FROM transactions;
        "#,
    );

    assert_ok(
        r#"
        WITH items(value) AS (
          SELECT CAST(NULL AS DECIMAL) UNION ALL
          SELECT 5.0 UNION ALL
          SELECT 15.5
        )
        SELECT value,
          CASE
            WHEN value IS NULL THEN -1
            WHEN value < 10 THEN 0.0
            ELSE CAST(10 AS DOUBLE)
          END AS result
        FROM items;
        "#,
    );

    assert_ok(
        r#"
        WITH items(value) AS (
          SELECT CAST(NULL AS DECIMAL) UNION ALL
          SELECT 5.0 UNION ALL
          SELECT 15.5
        )
        SELECT value,
          CASE
            WHEN value IS NULL THEN -1
            WHEN value < 10 THEN 0.0
            ELSE CAST(10 AS DOUBLE)
          END AS result
        FROM items;
        "#,
    );

    assert_ok(
        r#"
        WITH users(email) AS (
          SELECT 'user@example.com' UNION ALL
          SELECT 'invalid-email' UNION ALL
          SELECT NULL)
        SELECT email,
          CASE
            WHEN email LIKE '%@%' THEN 'valid'
            WHEN email IS NULL THEN 'missing'
            ELSE 'invalid'
          END AS status
        FROM users;
        "#,
    );

    assert_fails_with_error(
        r#"
        WITH users(email) AS (
          SELECT 'user@example.com' UNION ALL
          SELECT 'invalid-email' UNION ALL
          SELECT NULL)
        SELECT email,
          CASE
            WHEN email LIKE '%@%' THEN 'valid'
            WHEN email IS NULL THEN 42
            ELSE 'invalid'
          END AS status
        FROM users;
        "#,
        "failed to parse 'valid' as a value of type int, consider using explicit type casts",
    );

    assert_fails_with_error(
        r#"
        WITH params(input) AS (SELECT 'kiwi' AS input)
        SELECT 
          CASE (SELECT input FROM params)
            WHEN 'apple' THEN 'fruit'
            WHEN 'carrot' THEN 'vegetable'
            WHEN 1.5 THEN 'exotic fruit'
            ELSE 'unknown'
          END AS food_type;
        "#,
        "CASE/WHEN types text, text, text and numeric cannot be matched",
    );

    assert_fails_with_error(
        r#"
        WITH params(a) AS (SELECT 42)
        SELECT 
          CASE a
            WHEN 'apple' THEN 'fruit'
            WHEN 'carrot' THEN 'vegetable'
            WHEN 'kiwi' THEN 'exotic fruit'
            ELSE 'unknown'
          END AS food_type;
        FROM params
        "#,
        "failed to parse 'apple' as a value of type int, consider using explicit type casts",
    );
}

#[test]
fn unary() {
    assert_ok("SELECT NULL IS NULL");
    assert_ok("SELECT 1 IS NULL");
    assert_ok("SELECT (1 = 2) IS NULL");
    assert_ok("SELECT NOT 1 = 2");
    assert_ok("SELECT NOT 1 = $1");
    assert_ok("SELECT NOT NULL");
    assert_ok("SELECT EXISTS (SELECT 1)");
    assert_ok("SELECT EXISTS (SELECT 1, 2, 3)");
    assert_ok("SELECT NOT EXISTS (SELECT 1, 2, 3)");

    assert_fails_with_error(
        "SELECT NOT 1",
        "argument of NOT must be type boolean, not type int",
    );
}

#[test]
fn coalesce() {
    assert_ok("SELECT COALESCE(1, -2)");
    assert_ok("SELECT COALESCE(1::int, -2::int)");
    assert_ok("SELECT COALESCE(1.5, -2)");
    assert_ok("SELECT COALESCE(1.5, -2, 3.5)");
    assert_ok("SELECT COALESCE(1.5, -2, 3.5)");
    assert_ok("SELECT COALESCE(1.5, -2, $1)");
    assert_ok("SELECT COALESCE(1.5, -2, $1) + COALESCE(1, 2, $2)");

    assert_fails_with_error(
        "SELECT COALESCE(1.5, false, $1)",
        "COALESCE types numeric, bool and unknown cannot be matched",
    );

    assert_fails_with_error(
        "SELECT COALESCE(1.5, 'kek', $1)",
        "failed to parse 'kek' as a value of type numeric, consider using explicit type casts",
    );
}

#[test]
fn values() {
    assert_ok("VALUES (1, 2)");
    assert_ok("VALUES (1, '2')");
    assert_ok("VALUES (1, 2), (2, 3)");
    assert_ok("VALUES (1, '2'), (2, '3')");
    assert_ok("VALUES (1::text, 2), (2::text, 3)");
    assert_ok("VALUES (1::text, 2 + 1), (2::text, 3 + 2)");
    assert_ok("VALUES (1::text, 2 + 1), (2::text, 3.5 + 2.5)");
    assert_ok("VALUES (1::text, 2 + 1), ($1, $2)");
    assert_ok("VALUES (false, LOCALTIMESTAMP), ($1, $2)");
    assert_ok("VALUES (1, 2), ((select 2), 3)");
    assert_ok("SELECT * FROM (SELECT 1 AS a) WHERE (a, a) in (VALUES (1, 2), (1, 2))");
    assert_ok("SELECT * FROM (SELECT 1 AS a) WHERE EXISTS (VALUES (1, 2), (1, 2))");
    assert_ok("SELECT * FROM (SELECT 1 AS a) WHERE (a, a) in (VALUES (1, 2), (1, '2'))");

    assert_fails_with_error(
        "VALUES ('kek' + false)",
        "could not resolve operator overload for +(text, bool)",
    );

    assert_fails_with_error(
        "VALUES (false, LOCALTIMESTAMP), (LOCALTIMESTAMP, 'kek')",
        "VALUES types bool and datetime cannot be matched",
    );

    assert_fails_with_error(
        "VALUES (1, 2), (1, 2, 3)",
        "VALUES lists must all be the same length",
    );

    assert_fails_with_error(
        "SELECT * FROM (SELECT 1 AS a) WHERE (a, a) in (VALUES (1, 2), (1, 2, 3))",
        "VALUES lists must all be the same length",
    );

    assert_fails_with_error(
        "SELECT * FROM (SELECT 1 AS a) WHERE EXISTS (VALUES (1, 2), (1, false), (1, 2))",
        "VALUES types int, bool and int cannot be matched",
    );
}

#[test]
fn windows() {
    assert_ok("SELECT count(*) over () from (select 1);");
    assert_ok("SELECT count(*) over (PARTITION BY 1) from (select 1);");
    assert_ok("SELECT count(*) over (PARTITION BY 1 ORDER BY 1) from (select 1);");
    assert_ok("SELECT max(a) over (PARTITION BY 1 ORDER BY 1) from (select 1 as a);");
    assert_ok("SELECT max(a + a) over (PARTITION BY 1 ORDER BY 1) from (select 1 as a);");
    assert_ok("SELECT max(a + a) filter (where a = a) over (PARTITION BY 1 ORDER BY 1) from (select 1 as a);");
    assert_ok("SELECT max(a + a) filter (where a = $1) over (PARTITION BY 1 ORDER BY 1) from (select 1 as a);");

    assert_ok(
        "WITH t AS (SELECT 'a' as a) \
        SELECT count(*) over(ROWS BETWEEN 1 + 1 PRECEDING AND CURRENT ROW) from t;",
    );
    assert_ok(
        "WITH t AS (SELECT 'a' as a) \
        SELECT count(*) over(ROWS BETWEEN 1 + 1 PRECEDING AND 2 - 1 FOLLOWING) from t;",
    );

    assert_fails_with_error(
        "SELECT count(*) over (PARTITION BY a + false) from (select 1 as a);",
        "could not resolve operator overload for +(int, bool)",
    );

    assert_fails_with_error(
        "WITH t AS (SELECT 'a' as a) \
        SELECT count(*) over(ROWS BETWEEN 1 PRECEDING AND 1 + 0.5 FOLLOWING) from t;",
        "argument of ROWS must have integer type, got numeric",
    );

    assert_fails_with_error(
        "WITH t AS (SELECT 'a' as a) \
        SELECT sum(a) over(ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) from t;",
        "could not resolve window function overload for sum(text)",
    );

    assert_fails_with_error(
        "WITH t AS (SELECT 'a' as a) \
        SELECT sum(a::int + false) over(PARTITION BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) from t;",
        "could not resolve operator overload for +(int, bool)",
    );
}

#[test]
fn order_by() {
    assert_ok("WITH t(a, b) AS (SELECT 1, 1.5::double) SELECT a, b from t order by 2, 1");
    assert_ok("WITH t(a, b) AS (SELECT 1, 1.5::double) SELECT a, b from t order by a, b + a");

    assert_fails_with_error(
        "WITH t(a, b) AS (SELECT 1, 1.5::double) SELECT a, b from t order by a, b + false",
        "could not resolve operator overload for +(double, bool)",
    );
}

#[test]
fn cast() {
    assert_ok("SELECT 1::int");
    assert_ok("SELECT 1::double");
    assert_ok("SELECT 1::numeric");
    assert_ok("SELECT '2024-03-11 12:33:14 UTC'::datetime");
    assert_ok("SELECT CAST('11111111-1111-1111-1111-111111111111' AS UUID)");

    assert_ok("SELECT $1::int");
    assert_ok("SELECT $1::double");
    assert_ok("SELECT $1::numeric");
    assert_ok("SELECT $1::datetime");
    assert_ok("SELECT $1::uuid");

    assert_ok("SELECT 1::int = $1");
    assert_ok("SELECT 1::double = $1");
    assert_ok("SELECT 1::numeric = $1");
    assert_ok("SELECT '2024-03-11 12:33:14 UTC'::datetime = $1");
    assert_ok("SELECT CAST('11111111-1111-1111-1111-111111111111' AS UUID) = $1");

    assert_ok(
        "SELECT (\
        1::int, \
        1::double, \
        1::numeric, \
        '2024-03-11 12:33:14 UTC'::datetime, \
        '11111111-1111-1111-1111-111111111111'::uuid \
        ) = ($1, $2, $3, $4, $5)",
    );
}

#[test]
fn clause_based_parameter_type_inference() {
    // WHERE
    assert_ok("SELECT * FROM (SELECT 1) WHERE $1");
    assert_ok("SELECT * FROM (SELECT 1) HAVING $1");
    assert_ok("SELECT * FROM (SELECT 1) WHERE $1 HAVING $2");
    assert_ok(
        "SELECT max(a + a) FILTER (WHERE $1) over (PARTITION BY 1 ORDER BY 1) from (SELECT 1 as a);"
    );
    assert_ok("UPDATE t2 SET e = 3 WHERE $1");
    assert_ok("DELETE FROM t2 WHERE $1");

    // WINDOW
    assert_ok(
        "SELECT sum(x) OVER win from (select 1 as x) WINDOW win as (ROWS BETWEEN $1 PRECEDING AND $2 FOLLOWING)"
    );

    assert_ok(
        "SELECT sum(x) OVER (ROWS BETWEEN $1 PRECEDING AND $2 FOLLOWING) FROM (SELECT 1 as x)",
    );

    // coerce string literal
    assert_ok(
        "WITH t AS (SELECT 'a' as a)
        SELECT count() over(ROWS BETWEEN '1' PRECEDING AND CURRENT ROW) from t;",
    );

    // JOIN
    assert_ok("WITH t AS (SELECT 1) SELECT * FROM t join t on $1")
}

#[test]
fn parameter_and_text_type_defaulting() {
    assert_ok("SELECT $1");
    assert_ok("SELECT $1, $2");
    assert_ok("SELECT $1, $1");

    assert_ok("SELECT $1 FROM (SELECT $2)");
    assert_ok("SELECT $1 FROM (SELECT $1)");

    assert_ok("SELECT $1 FROM (SELECT $1) WHERE $1 = $1");
    assert_ok("SELECT $1 FROM (SELECT $2) WHERE $3 = $4");
    assert_ok("SELECT $1 FROM (SELECT $2) WHERE $3 = 'kek'");
    assert_ok("SELECT $1 FROM (SELECT $2) WHERE $3 in ($4, $5)");
    assert_ok("SELECT $1 FROM (SELECT $2) WHERE $3 in ($4, 'kek')");
    assert_ok("SELECT $1 FROM (SELECT $2) WHERE 'kek' in ($4, $5)");
    assert_ok("SELECT $1 FROM (SELECT $2) WHERE $3 = NULL");

    assert_ok("SELECT CASE WHEN true THEN $1 WHEN false THEN $2 END");
    assert_ok("SELECT CASE WHEN true THEN $1 WHEN false THEN 'lol' END");
    assert_ok("SELECT max($1);");
    assert_ok("SELECT COALESCE($1, $2)");
    assert_ok("SELECT COALESCE($1, 'lol')");

    assert_ok("VALUES ($1, $2)");
    assert_ok("VALUES ($1, $2), ($3, $4)");
    assert_ok("VALUES ($1, 'kek'), ($2, 'lol')");
    assert_ok("VALUES ($1, 'kek'), ('lol', $2)");
}

#[test]
fn text_literal_coercion() {
    assert_ok("SELECT '1' + 1");
    assert_ok("SELECT '2' + '2'");
    assert_ok("SELECT COALESCE('1', '2') + 1");
    assert_ok("SELECT COALESCE('1', $1) + 1");
    assert_ok("SELECT COALESCE('1', $1) + 1");
    assert_ok("SELECT COALESCE('1' || '2')::int * 2;");
    assert_ok("SELECT 1.5 + max('1')");
    assert_ok("SELECT 1.5 + max('1.5')");
    assert_ok("SELECT 1.5 + COALESCE(max('1.5'), $1)");
    assert_ok("SELECT coalesce('1' || '2', 'kek')::int * 2;");
    assert_ok("SELECT coalesce('f', false);");

    assert_ok("SELECT * FROM (SELECT 1) WHERE 'false'");
    assert_ok("SELECT * FROM (SELECT 1) WHERE 'true'");
    assert_ok("SELECT * FROM (SELECT 1) WHERE 't' AND 'f'");
    assert_ok("SELECT * FROM (SELECT 1) WHERE 't' AND 'f' HAVING 't' OR 'f'");
    assert_ok("SELECT * FROM (SELECT 1) WHERE NOT 'f'");
    assert_ok("SELECT * FROM (SELECT 1) WHERE CASE WHEN 'f' THEN '1' WHEN 't' THEN '2' END = 1");
    assert_ok("SELECT * FROM (SELECT 1) WHERE 'kek' = NULL");
    assert_ok("SELECT * FROM (SELECT 1) WHERE 'kek' = $1");

    assert_ok("UPDATE t2 SET e = '3'");
    assert_ok("UPDATE t2 SET e = '3' WHERE 'f'");
    assert_ok("DELETE FROM t2 WHERE 1.5 = '1.5'");
    assert_ok("DELETE FROM t2 WHERE LOCALTIMESTAMP = '2023-07-07T12:34:56Z'");
    assert_ok("DELETE FROM t2 WHERE LOCALTIMESTAMP = '2023-07-07T12:34:56.123456Z'");

    assert_fails_with_error(
        "SELECT 1::int + '1.5'",
        "failed to parse '1.5' as a value of type int, consider using explicit type casts",
    );
    assert_fails_with_error(
        "SELECT COALESCE('kek', 1.5)",
        "failed to parse 'kek' as a value of type numeric, consider using explicit type casts",
    );
    assert_fails_with_error(
        "SELECT * FROM (SELECT 1) WHERE 'maybe'",
        "failed to parse 'maybe' as a value of type bool, consider using explicit type casts",
    );
    assert_fails_with_error(
        "WITH t AS (SELECT 'a' as a)
        SELECT count() over(ROWS BETWEEN 'start' PRECEDING AND CURRENT ROW) from t;",
        "failed to parse 'start' as a value of type int, consider using explicit type casts",
    );
}
