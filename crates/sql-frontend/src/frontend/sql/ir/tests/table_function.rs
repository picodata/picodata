use sql_executor::test_helpers::{expect_sql_to_ir_error, sql_to_optimized_ir};
use sql_explain::explain::explain_logical;
use sql_ir::ir::types::{DerivedType, UnrestrictedType};
use sql_ir::ir::value::Value;

#[track_caller]
fn explain(sql: &str, params: Vec<Value>) -> String {
    let plan = sql_to_optimized_ir(&format!("explain (logical) {sql}"), params);
    explain_logical(&plan).unwrap()
}

// A table function in FROM gives a global relation: there is no motion.
#[test]
fn generate_series_in_from() {
    insta::assert_snapshot!(explain("SELECT * FROM generate_series(1, 3)", vec![]), @"
    projection (generate_series.value::int -> value)
      scan generate_series(1::int, 3::int)
    ");
}

// The alias names the scan, and the columns keep their names.
#[test]
fn generate_series_with_alias_and_step() {
    insta::assert_snapshot!(explain("SELECT g.value FROM generate_series(1, 10, 3) AS g", vec![]), @"
    projection (g.value::int -> value)
      scan generate_series(1::int, 10::int, 3::int) -> g
    ");
}

// A decimal argument gives a decimal series, and WITH ORDINALITY adds
// the ordinality column.
#[test]
fn generate_series_decimal_with_ordinality() {
    insta::assert_snapshot!(explain("SELECT * FROM generate_series(1.5, 3) WITH ORDINALITY g", vec![]), @"
    projection (g.value::decimal -> value, g.ordinality::int -> ordinality)
      scan generate_series(1.5::decimal, 3::decimal) with ordinality -> g
    ");
}

// The types of the parameters come from the function.
#[test]
fn generate_series_parameters() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM generate_series($1, $2)",
        vec![Value::from(1), Value::from(3)],
    ), @"
    projection (generate_series.value::int -> value)
      scan generate_series(1::int, 3::int)
    ");
}

// An argument can be any expression of a suitable type.
#[test]
fn generate_series_expression_argument() {
    insta::assert_snapshot!(explain("SELECT * FROM generate_series(1, abs(3) + 1)", vec![]), @"
    projection (generate_series.value::int -> value)
      scan generate_series(1::int, (abs(3::int)::int + 1::int)::int)
    ");
}

// The type of the elements of the array is the type of the column.
#[test]
fn unnest_array_literal() {
    insta::assert_snapshot!(explain("SELECT * FROM unnest(array['a', 'b'])", vec![]), @"
    projection (unnest.value::text -> value)
      scan unnest(ARRAY['a'::text, 'b'::text])
    ");
}

// The arguments of a function on the right side of a join can reference the
// left side. The rows of the left side stay where they are.
#[test]
fn lateral_reference_to_sharded_table() {
    insta::assert_snapshot!(explain(
        r#"SELECT * FROM "arr_t" JOIN unnest("arr_t"."b") WITH ORDINALITY u ON u.value > 1"#,
        vec![],
    ), @"
    projection (arr_t.a::int -> a, arr_t.b::int[] -> b, u.value::int -> value, u.ordinality::int -> ordinality)
      join on (u.value::int > 1::int)
        scan arr_t
        scan unnest(arr_t.b::int[]) with ordinality -> u
    ");
}

// A left join keeps the rows of the left side for which the function gives
// no rows.
#[test]
fn lateral_left_join() {
    insta::assert_snapshot!(explain(
        r#"SELECT * FROM "t" LEFT JOIN generate_series("t"."a", 3) g ON g.value > "t"."b""#,
        vec![],
    ), @"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, g.value::int -> value)
      left join on (g.value::int > t.b::int)
        scan t
        scan generate_series(t.a::int::int, 3::int) -> g
    ");
}

// A function can reference all the sources to its left, including other
// functions.
#[test]
fn lateral_reference_to_several_sources() {
    insta::assert_snapshot!(explain(
        r#"SELECT * FROM "t" JOIN generate_series(1, 3) g ON g.value = "t"."a" JOIN generate_series(g.value, "t"."b") h ON true"#,
        vec![],
    ), @"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, g.value::int -> value, h.value::int -> value)
      join on (true::bool)
        join on (g.value::int = t.a::int)
          scan t
          scan generate_series(1::int, 3::int) -> g
        scan generate_series(g.value::int::int, t.b::int::int) -> h
    ");
}

// A function on the right side of CROSS JOIN can reference the sources on
// the left, as for other joins.
#[test]
fn lateral_cross_join() {
    insta::assert_snapshot!(explain(
        r#"SELECT * FROM "arr_t" CROSS JOIN unnest("arr_t"."b") u INNER JOIN "t2" ON "t2"."e" = u.value"#,
        vec![],
    ), @"
    projection (arr_t.a::int -> a, arr_t.b::int[] -> b, u.value::int -> value, t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h)
      join on (t2.e::int = u.value::int)
        join on (true::bool)
          scan arr_t
          scan unnest(arr_t.b::int[]) -> u
        motion [policy: full, program: ReshardIfNeeded]
          projection (t2.e::int -> e, t2.f::int -> f, t2.g::int -> g, t2.h::int -> h, t2.bucket_id::int -> bucket_id)
            scan t2
    ");
}

// A function can be in a subquery.
#[test]
fn function_in_subquery() {
    insta::assert_snapshot!(explain(
        r#"SELECT * FROM "t" WHERE "a" IN (SELECT value FROM generate_series(1, 3))"#,
        vec![],
    ), @"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (t.a::int in ROW($0))
        scan t
    subquery $0:
      scan
        projection (generate_series.value::int -> value)
          scan generate_series(1::int, 3::int)
    ");
}

// A subquery in an argument that reads a sharded table is moved to every
// instance, so that every instance computes the same rows.
#[test]
fn subquery_in_arguments() {
    insta::assert_snapshot!(explain(
        r#"SELECT * FROM generate_series(1, (SELECT max("a") FROM "t"))"#,
        vec![],
    ), @"
    projection (generate_series.value::int -> value)
      scan generate_series(1::int, ROW($0)::int)
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (max(max_1::int)::int -> col_1)
            motion [policy: full, program: ReshardIfNeeded]
              projection (max(t.a::int::int)::int -> max_1)
                scan t
    ");
}

// A subquery in an argument of a lateral function.
#[test]
fn subquery_in_arguments_of_lateral_function() {
    insta::assert_snapshot!(explain(
        r#"SELECT * FROM "t" JOIN generate_series("t"."a", (SELECT 3)) g ON true"#,
        vec![],
    ), @"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, g.value::int -> value)
      join on (true::bool)
        scan t
        scan generate_series(t.a::int::int, ROW($0)::int) -> g
    subquery $0:
      scan
        projection (3::int -> col_1)
    ");
}

// The errors for wrong calls.
#[test]
fn errors() {
    for (sql, err) in [
        (
            "SELECT * FROM no_such_function(1)",
            r#"table function "no_such_function" does not exist"#,
        ),
        (
            "SELECT * FROM generate_series(1)",
            "could not resolve function overload for generate_series(int)",
        ),
        // PostgreSQL has no series of doubles.
        (
            "SELECT * FROM generate_series(1.5::double, 3)",
            "could not resolve function overload for generate_series(double, numeric)",
        ),
        (
            "SELECT * FROM generate_series(1, 3, 0.5::double)",
            "could not resolve function overload for generate_series(numeric, numeric, double)",
        ),
        (
            "SELECT * FROM (SELECT 1.5::double AS d) s JOIN generate_series(s.d, 3) g ON true",
            "could not resolve function overload for generate_series(double, numeric)",
        ),
        (
            "SELECT * FROM generate_series(1, count(*))",
            r#"aggregate functions are not supported in the arguments of table function "generate_series""#,
        ),
        // A function on the left side of a join cannot reference the right
        // side.
        (
            r#"SELECT * FROM generate_series(1, "t"."a") g JOIN "t" ON true"#,
            r#"could not resolve reference "t""#,
        ),
        // Two sources cannot have the same name.
        (
            "SELECT * FROM generate_series(1, 3) JOIN generate_series(1, 3) ON true",
            r#"table name "generate_series" specified more than once"#,
        ),
    ] {
        assert_eq!(expect_sql_to_ir_error(sql, &[]).to_string(), err, "{sql}");
    }

    // A parameter has the type that a client gives it.
    let double = [DerivedType::new(UnrestrictedType::Double)];
    assert_eq!(
        expect_sql_to_ir_error("SELECT * FROM generate_series($1, 3)", &double).to_string(),
        "could not resolve function overload for generate_series(double, numeric)"
    );
}
