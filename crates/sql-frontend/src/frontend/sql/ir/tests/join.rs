use sql_executor::test_helpers::{
    expect_sql_to_ir_error, sql_to_ir_without_bind, sql_to_optimized_ir,
};
use sql_explain::explain::explain_logical;

#[test]
fn multi_join1() {
    let input = r#"explain (logical) SELECT * FROM (
            SELECT "identification_number", "product_code" FROM "hash_testing"
        ) as t1
        INNER JOIN (SELECT "id" FROM "test_space") as t2
        ON t1."identification_number" = t2."id"
        LEFT JOIN (SELECT "id" FROM "test_space") as t3
        ON t1."identification_number" = t3."id"
        WHERE t1."identification_number" = 5 and t1."product_code" = '123'"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    projection (t1.identification_number::int -> identification_number, t1.product_code::string -> product_code, t2.id::int -> id, t3.id::int -> id)
      selection ((t1.identification_number::int = 5::int and t1.product_code::string = '123'::string))
        left join on (t1.identification_number::int = t3.id::int)
          join on (t1.identification_number::int = t2.id::int)
            scan t1
              projection (hash_testing.identification_number::int -> identification_number, hash_testing.product_code::string -> product_code)
                scan hash_testing
            motion [policy: full, program: ReshardIfNeeded]
              scan t2
                projection (test_space.id::int -> id)
                  scan test_space
          motion [policy: full, program: ReshardIfNeeded]
            scan t3
              projection (test_space.id::int -> id)
                scan test_space
    ");
}

#[test]
fn multi_join2() {
    let input = r#"explain (logical) SELECT * FROM "t1_2" "t1" LEFT JOIN "t2" ON "t1"."a" = "t2"."e"
    LEFT JOIN "t4" ON true
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    projection (t1.a::int -> a, t1.b::int -> b, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t4.c::string -> c, t4.d::int -> d)
      left join on (true::bool)
        left join on (t1.a::int = t2.e::int)
          scan t1_2 -> t1
          motion [policy: full, program: ReshardIfNeeded]
            projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
              scan t2
        motion [policy: full, program: ReshardIfNeeded]
          projection (t4.bucket_id::int -> bucket_id, t4.c::string -> c, t4.d::int -> d)
            scan t4
    ");
}

#[test]
fn multi_join3() {
    let input = r#"explain (logical) SELECT * FROM "t1_2" "t1" LEFT JOIN "t2" ON "t1"."a" = "t2"."e"
    JOIN "t3_2" "t3" ON "t1"."a" = "t3"."a" JOIN "t4" ON "t2"."f" = "t4"."c"::int
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    projection (t1.a::int -> a, t1.b::int -> b, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t3.a::int -> a, t3.b::int -> b, t4.c::string -> c, t4.d::int -> d)
      join on (t2.f::int = t4.c::string::int)
        join on (t1.a::int = t3.a::int)
          left join on (t1.a::int = t2.e::int)
            scan t1_2 -> t1
            motion [policy: full, program: ReshardIfNeeded]
              projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
                scan t2
          motion [policy: full, program: ReshardIfNeeded]
            projection (t3.bucket_id::int -> bucket_id, t3.a::int -> a, t3.b::int -> b)
              scan t3_2 -> t3
        motion [policy: full, program: ReshardIfNeeded]
          projection (t4.bucket_id::int -> bucket_id, t4.c::string -> c, t4.d::int -> d)
            scan t4
    ");
}

#[test]
fn multi_join4() {
    let input = r#"explain (logical) SELECT "t1"."a" FROM "t1" JOIN "t1" as "t2" ON "t1"."a" = "t2"."a"
    JOIN "t3" ON "t1"."a" = "t3"."a"
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    projection (t1.a::string -> a)
      join on (t1.a::string = t3.a::string)
        join on (t1.a::string = t2.a::string)
          scan t1
          motion [policy: full, program: ReshardIfNeeded]
            projection (t2.a::string -> a, t2.bucket_id::int -> bucket_id, t2.b::int -> b)
              scan t1 -> t2
        motion [policy: full, program: ReshardIfNeeded]
          projection (t3.bucket_id::int -> bucket_id, t3.a::string -> a, t3.b::int -> b)
            scan t3
    ");
}

#[test]
fn join_duplicate_table_name() {
    let cases = [
        ("SELECT 1 FROM t JOIN t ON true", "t"),
        ("SELECT 1 FROM t LEFT JOIN t ON true", "t"),
        // An alias clashes with a table, a subquery or another alias.
        ("SELECT 1 FROM t JOIN t1 AS t ON true", "t"),
        ("SELECT 1 FROM t JOIN (SELECT 1) AS t ON true", "t"),
        ("SELECT 1 FROM t AS x JOIN t1 AS x ON true", "x"),
        (
            "SELECT 1 FROM (VALUES (1)) AS v JOIN (VALUES (2)) AS v ON true",
            "v",
        ),
        // Names are compared after normalization, without the schema.
        (r#"SELECT 1 FROM t JOIN t AS "t" ON true"#, "t"),
        ("SELECT 1 FROM public.t JOIN t ON true", "t"),
        // Any earlier item of the FROM clause counts, not only the adjacent one.
        ("SELECT 1 FROM t JOIN t1 ON true JOIN t ON true", "t"),
        // Rejected before the join condition is resolved.
        ("SELECT * FROM t JOIN t ON t.a = t.a", "t"),
        ("WITH q AS (SELECT 1) SELECT 1 FROM q JOIN q ON true", "q"),
        (
            "WITH t AS (SELECT 1 AS a) SELECT 1 FROM t JOIN t ON true",
            "t",
        ),
        // Nested queries are checked too.
        ("SELECT (SELECT count(*) FROM t JOIN t ON true)", "t"),
        (
            "WITH q AS (SELECT 1 FROM t JOIN t ON true) SELECT * FROM q",
            "t",
        ),
        (
            "SELECT 1 FROM t UNION ALL SELECT 1 FROM t1 JOIN t1 ON true",
            "t1",
        ),
        (
            "INSERT INTO t SELECT t.a, t.b, t.c, t.d FROM t JOIN t ON true",
            "t",
        ),
        // The first repeated name is reported, and the outer FROM clause is
        // checked before the queries nested in it.
        (
            "SELECT 1 FROM t JOIN t1 ON true JOIN t1 ON true JOIN t ON true",
            "t1",
        ),
        (
            "SELECT 1 FROM t JOIN t ON true WHERE EXISTS (SELECT 1 FROM t1 JOIN t1 ON true)",
            "t",
        ),
        // PostgreSQL reports "t1" here.
        (
            "SELECT 1 FROM (SELECT 1 FROM t1 JOIN t1 ON true) AS s JOIN t ON true JOIN t ON true",
            "t",
        ),
    ];

    for (query, name) in cases {
        let error = expect_sql_to_ir_error(query, &[]);
        assert_eq!(
            error.to_string(),
            format!(r#"table name "{name}" specified more than once"#),
            "{query}"
        );
    }
}

#[test]
fn join_distinct_table_names() {
    let queries = [
        "SELECT * FROM (SELECT * FROM t) as q JOIN t ON true",
        "SELECT 1 FROM t JOIN t AS u ON true",
        "WITH q AS (SELECT 1) SELECT 1 FROM q JOIN q AS r ON true",
        // Quoted names are case-sensitive.
        r#"SELECT 1 FROM t JOIN t AS "T" ON true"#,
        // Subqueries without an alias have no name.
        "SELECT 1 FROM (SELECT 1) JOIN (SELECT 2) ON true",
        // A nested query has a FROM clause of its own.
        "SELECT 1 FROM t AS x WHERE EXISTS (SELECT 1 FROM t1 AS x)",
    ];

    for query in queries {
        sql_to_ir_without_bind(query, &[]);
    }
}
