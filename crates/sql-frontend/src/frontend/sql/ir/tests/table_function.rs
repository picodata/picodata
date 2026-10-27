use sql_executor::test_helpers::{
    expect_sql_to_ir_error, sql_to_ir_without_bind, sql_to_optimized_ir,
};
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
    insta::assert_snapshot!(explain(
        "SELECT g.value FROM generate_series(1, 10, 3) AS g",
        vec![],
    ), @"
    projection (g.value::int -> value)
      scan generate_series(1::int, 10::int, 3::int) -> g
    ");
}

// A decimal argument gives a decimal series, and WITH ORDINALITY adds
// the ordinality column.
#[test]
fn generate_series_decimal_with_ordinality() {
    insta::assert_snapshot!(explain(
        "SELECT * FROM generate_series(1.5, 3) WITH ORDINALITY g",
        vec![],
    ), @"
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
    insta::assert_snapshot!(explain(
        "SELECT * FROM generate_series(1, abs(3) + 1)",
        vec![],
    ), @"
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
        r#"SELECT "arr_t"."a", u.value, u.ordinality
           FROM "arr_t" JOIN unnest("arr_t"."b") WITH ORDINALITY u ON u.value > 1"#,
        vec![],
    ), @"
    projection (arr_t.a::int -> a, u.value::int -> value, u.ordinality::int -> ordinality)
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
        r#"SELECT "t"."a", g.value
           FROM "t" LEFT JOIN generate_series("t"."a", 3) g ON g.value > "t"."b""#,
        vec![],
    ), @"
    projection (t.a::int -> a, g.value::int -> value)
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
        r#"SELECT "t"."a", h.value FROM "t"
           JOIN generate_series(1, 3) g ON g.value = "t"."a"
           JOIN generate_series(g.value, "t"."b") h ON true"#,
        vec![],
    ), @"
    projection (t.a::int -> a, h.value::int -> value)
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
        r#"SELECT "arr_t"."a", u.value FROM "arr_t"
           CROSS JOIN unnest("arr_t"."b") u
           INNER JOIN "t3" ON "t3"."b" = u.value"#,
        vec![],
    ), @"
    projection (arr_t.a::int -> a, u.value::int -> value)
      join on (t3.b::int = u.value::int)
        join on (true::bool)
          scan arr_t
          scan unnest(arr_t.b::int[]) -> u
        motion [policy: full, program: ReshardIfNeeded]
          projection (t3.bucket_id::int -> bucket_id, t3.a::text -> a, t3.b::int -> b)
            scan t3
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
        r#"SELECT "t"."a", g.value
           FROM "t" JOIN generate_series("t"."a", (SELECT 3)) g ON true"#,
        vec![],
    ), @"
    projection (t.a::int -> a, g.value::int -> value)
      join on (true::bool)
        scan t
        scan generate_series(t.a::int::int, ROW($0)::int) -> g
    subquery $0:
      scan
        projection (3::int -> col_1)
    ");
}

// A value without a type of its own is an array of text, as any value of
// unknown type is text: a parameter without a type from the client, NULL
// and a text literal.
#[test]
fn unnest_of_unknown_type_defaults_to_text() {
    let plan = sql_to_ir_without_bind(
        "explain (logical) SELECT * FROM unnest($1)",
        &[DerivedType::unknown()],
    );
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @"
    projection (unnest.value::text -> value)
      scan unnest($1::text[])
    ");

    insta::assert_snapshot!(explain("SELECT * FROM unnest(NULL)", vec![]), @"
    projection (unnest.value::text -> value)
      scan unnest(NULL::unknown)
    ");

    // A text literal is `text[]` too, but picodata cannot convert text to
    // an array, as `SELECT '{a,b}'::text[]` cannot either.
    let err = expect_sql_to_ir_error("SELECT * FROM unnest('{a,b}')", &[]);
    assert_eq!(
        err.to_string(),
        "failed to parse '{a,b}' as a value of type text[], consider using explicit type casts"
    );
}

// unnest of several arrays is one scan with a column for each array, with
// the element type of the array.
#[test]
fn unnest_of_several_arrays() {
    insta::assert_snapshot!(
        explain("SELECT * FROM unnest(ARRAY[1, 2], ARRAY['a'], NULL) WITH ORDINALITY", vec![]),
        @"
    projection (unnest.value_1::int -> value_1, unnest.value_2::text -> value_2, unnest.value_3::text -> value_3, unnest.ordinality::int -> ordinality)
      scan unnest(ARRAY[1::int, 2::int], ARRAY['a'::text], NULL::unknown) with ordinality
    "
    );

    // The arrays can come from the left side of a join.
    insta::assert_snapshot!(explain(
        r#"SELECT u.value_2 FROM "arr_t" JOIN unnest("arr_t"."b", "arr_t"."b") u ON u.value_1 > 1"#,
        vec![],
    ), @"
    projection (u.value_2::int -> value_2)
      join on (u.value_1::int > 1::int)
        scan arr_t
        scan unnest(arr_t.b::int[], arr_t.b::int[]) -> u
    ");
}

// An array of unnest of several arrays can come from a subquery, which is
// moved to every instance as for any argument.
#[test]
fn unnest_of_several_arrays_from_subquery() {
    insta::assert_snapshot!(explain(
        r#"SELECT u.value_1, u.value_2
           FROM unnest((SELECT "b" FROM "arr_t" WHERE "a" = 1), ARRAY['x']) u"#,
        vec![],
    ), @"
    projection (u.value_1::int -> value_1, u.value_2::text -> value_2)
      scan unnest(ROW($0), ARRAY['x'::text]) -> u
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (arr_t.b::int[] -> b)
            selection (arr_t.a::int = 1::int)
              scan arr_t
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
            "could not resolve function overload \
             for generate_series(numeric, numeric, double)",
        ),
        (
            "SELECT * FROM (SELECT 1.5::double AS d) s \
             JOIN generate_series(s.d, 3) g ON true",
            "could not resolve function overload for generate_series(double, numeric)",
        ),
        // unnest gives the element type of each argument, so every
        // argument must be an array.
        (
            "SELECT * FROM unnest(1)",
            "could not resolve function overload for unnest(int)",
        ),
        (
            "SELECT * FROM unnest(ARRAY[1], 2)",
            "could not resolve function overload for unnest(int[], int)",
        ),
        // The columns of several arrays are numbered, so there is no
        // column `value`.
        (
            "SELECT value FROM unnest(ARRAY[1], ARRAY[2])",
            r#"column with name "value" not found"#,
        ),
        (
            "SELECT * FROM generate_series(1, count(*))",
            "aggregate functions are not allowed in functions in FROM",
        ),
        // Every instance computes the same rows of a function. A volatile
        // function is found below an array, a scalar function, a cast and
        // a CASE.
        (
            "SELECT * FROM unnest(ARRAY[pico_instance_uuid()])",
            "volatile functions are not allowed in functions in FROM",
        ),
        (
            "SELECT * FROM unnest(ARRAY[func(pico_instance_uuid())])",
            "volatile functions are not allowed in functions in FROM",
        ),
        (
            "SELECT * FROM generate_series(1, pico_instance_uuid()::int)",
            "volatile functions are not allowed in functions in FROM",
        ),
        (
            "SELECT * FROM generate_series(1, CASE WHEN true THEN pico_instance_uuid()::int ELSE 2 END)",
            "volatile functions are not allowed in functions in FROM",
        ),
        // A subquery without tables is computed on every instance too.
        (
            "SELECT * FROM unnest(ARRAY[(SELECT pico_instance_uuid())])",
            "volatile functions are not allowed in functions in FROM",
        ),
        (
            r#"SELECT * FROM unnest(ARRAY[(SELECT pico_instance_uuid() FROM "t")])"#,
            "volatile functions are not allowed in functions in FROM",
        ),
        // An aggregate is reported before a volatile function.
        (
            "SELECT * FROM generate_series(count(*), pico_instance_uuid()::int)",
            "aggregate functions are not allowed in functions in FROM",
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
    let err = expect_sql_to_ir_error("SELECT * FROM generate_series($1, 3)", &double);
    assert_eq!(
        err.to_string(),
        "could not resolve function overload for generate_series(double, numeric)"
    );

    // Tarantool takes at most 127 arguments of a function.
    let sql = format!("SELECT * FROM unnest({})", vec!["ARRAY[1]"; 128].join(", "));
    let err = expect_sql_to_ir_error(&sql, &[]);
    assert_eq!(
        err.to_string(),
        "unnest takes at most 127 arguments, got 128"
    );
}
