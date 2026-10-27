use super::*;
use insta::assert_snapshot;

/// Puts each clause of the local SQL on its own line, so that the snapshots
/// stay narrow.
fn by_clauses(sql: &str) -> String {
    let mut sql = sql.to_string();
    for keyword in ["FROM", "INNER JOIN", "LEFT JOIN", "ON", "WHERE", "ORDER BY"] {
        sql = sql.replace(&format!(" {keyword} "), &format!("\n{keyword} "));
    }
    sql
}

/// The instances, the parameters and the local SQL [`by_clauses`] of
/// a query dispatched by one request.
fn dispatched(sql: &str) -> String {
    let (instances, sql, params) = match get_broadcast(sql) {
        DispatchInfo::All(sql, params) => ("All", sql, params),
        DispatchInfo::Any(sql, params) => ("Any", sql, params),
        info => panic!("unexpected dispatch {info:?}"),
    };
    format!("{instances} {params:?}\n{}", by_clauses(&sql))
}

// A table function is executed on any instance. The scan always has a name,
// so that the references to its columns can be qualified.
#[test]
fn generate_series_sql() {
    let info = dispatched(r#"SELECT g.value FROM generate_series(1, 3) AS g"#);
    assert_snapshot!(info, @r#"
    Any [Integer(1), Integer(3)]
    SELECT "g"."VALUE" as "value"
    FROM generate_series ($1::int, $2::int) as "g"
    "#);
}

// WITH ORDINALITY goes before the name of the scan. An asterisk expands to
// the columns with their names in queries, so that a materialized result
// keeps these names.
#[test]
fn with_ordinality_sql() {
    let info = dispatched(r#"SELECT * FROM generate_series(1, 3) WITH ORDINALITY g"#);
    assert_snapshot!(info, @r#"
    Any [Integer(1), Integer(3)]
    SELECT "g"."VALUE" as "value", "g"."ORDINALITY" as "ordinality"
    FROM generate_series ($1::int, $2::int) WITH ORDINALITY as "g"
    "#);
}

// The columns of a table function have the tarantool names in the local SQL.
// Tarantool does not know the type of the elements of an array column, so
// the elements are cast to the type that the planner gives them.
#[test]
fn lateral_join_sql() {
    let info = dispatched(
        r#"SELECT u.value, u.ordinality
           FROM "arr_t" JOIN unnest("arr_t"."b") WITH ORDINALITY u ON u.value > 1"#,
    );
    assert_snapshot!(info, @r#"
    All [Integer(1)]
    SELECT "u"."VALUE"::int as "value", "u"."ORDINALITY" as "ordinality"
    FROM "arr_t"
    INNER JOIN unnest ("arr_t"."b") WITH ORDINALITY as "u"
    ON "u"."VALUE"::int > $1::int
    "#);
}

// The alias of a cast column stays, though it is the local name of the column.
#[test]
fn alias_of_cast_column_sql() {
    let info = dispatched(
        r#"SELECT s."VALUE"
           FROM (SELECT u.value AS "VALUE" FROM unnest(ARRAY[1, 2]) u) s"#,
    );
    assert_snapshot!(info, @r#"
    Any [Integer(1), Integer(2)]
    SELECT "s"."VALUE"
    FROM (SELECT "u"."VALUE"::int as "VALUE"
    FROM unnest ([ $1::int, $2::int ]) as "u") as "s"
    "#);
}

// The arguments of a function can reference another function to the left.
#[test]
fn lateral_join_of_functions_sql() {
    let info = dispatched(
        r#"SELECT a.value, b.value
           FROM generate_series(1, 3) a LEFT JOIN generate_series(a.value, 3) b ON true"#,
    );
    assert_snapshot!(info, @r#"
    Any [Integer(1), Integer(3), Integer(3), Boolean(true)]
    SELECT "a"."VALUE" as "value", "b"."VALUE" as "value"
    FROM generate_series ($1::int, $2::int) as "a"
    LEFT JOIN generate_series ("a"."VALUE"::int, $3::int) as "b"
    ON $4::bool
    "#);
}

// An outer query references the columns of a subquery by their names in
// the subquery.
#[test]
fn function_in_subquery_sql() {
    let info = dispatched(
        r#"SELECT s.value
           FROM (SELECT * FROM generate_series(1, 3) g) s"#,
    );
    assert_snapshot!(info, @r#"
    Any [Integer(1), Integer(3)]
    SELECT "s"."value"
    FROM (SELECT "g"."VALUE" as "value"
    FROM generate_series ($1::int, $2::int) as "g") as "s"
    "#);
}

// A projection gives the column of a table function its name in queries, so
// that the outer query can reference it by that name.
#[test]
fn function_column_alias_sql() {
    let info = dispatched(
        r#"SELECT s.value
           FROM (SELECT g.value FROM generate_series(1, 3) g) s"#,
    );
    assert_snapshot!(info, @r#"
    Any [Integer(1), Integer(3)]
    SELECT "s"."value"
    FROM (SELECT "g"."VALUE" as "value"
    FROM generate_series ($1::int, $2::int) as "g") as "s"
    "#);
}

// A subquery in an argument is a part of the local SQL.
#[test]
fn subquery_in_arguments_sql() {
    let info = dispatched(r#"SELECT g.value FROM generate_series(1, (SELECT 3)) g"#);
    assert_snapshot!(info, @r#"
    Any [Integer(1), Integer(3)]
    SELECT "g"."VALUE" as "value"
    FROM generate_series ($1::int, (SELECT $2::int as "col_1")::int) as "g"
    "#);
}

/// The local SQL of the first motion subtree and of the stage that reads it,
/// with the motion materialized as a virtual table of `columns` integer
/// columns. The second stage is the subtree of `next_motion` (slice and
/// position), or the top of the plan.
fn get_two_stage_sql(
    sql: &str,
    columns: usize,
    next_motion: Option<(usize, usize)>,
) -> (String, String) {
    use super::exec_plan::{get_sql_from_execution_plan, reshard_vtable};
    use crate::executor::engine::mock::TEMPLATE;
    use crate::ir::tree::Snapshot;
    use std::rc::Rc;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    let mut vtable = VirtualTable::new();
    for _ in 0..columns {
        vtable.add_column(vcolumn_integer_user_non_null());
    }
    // Materialization names the virtual table after the motion.
    if let Relational::Motion(Motion {
        alias: Some(alias), ..
    }) = query
        .get_exec_plan()
        .get_ir_plan()
        .get_relation_node(motion_id)
        .unwrap()
    {
        vtable.set_alias(alias);
    }
    reshard_vtable(&query, motion_id, &mut vtable);
    let mut vtables = HashMap::new();
    vtables.insert(motion_id, Rc::new(vtable));

    let next_motion_id = next_motion.map(|(slice, pos)| query.get_motion_id(slice, pos));
    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let ir = exec_plan.get_ir_plan();
    let top_id = match next_motion_id {
        Some(motion_id) => ir.get_motion_subtree_root(motion_id).unwrap(),
        None => ir.get_top().unwrap(),
    };
    let motion_child_id = ir.get_motion_subtree_root(motion_id).unwrap();
    let first = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    let second = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    (by_clauses(&first.pattern), by_clauses(&second.pattern))
}

// An asterisk over a table function expands to its columns with their
// names in queries, so that the outer SELECT can reference them.
#[test]
fn asterisk_over_lateral_join_sql() {
    let (first, second) = get_two_stage_sql(
        r#"SELECT * FROM "arr_t"
           JOIN generate_series(1, "arr_t"."a") g ON true ORDER BY 3"#,
        3,
        None,
    );
    assert_snapshot!(first, @r#"
    SELECT "arr_t"."a", "arr_t"."b", "g"."VALUE" as "value"
    FROM "arr_t"
    INNER JOIN generate_series ($1::int, "arr_t"."a"::int) as "g"
    ON $2::bool
    "#);
    assert_snapshot!(second, @r#"
    SELECT "COL_1" as "a", "COL_2" as "b", "COL_3" as "value"
    FROM "_tmp_0_0136"
    ORDER BY 3
    "#);
}

// A motion over a table function reads it through a projection, and the
// virtual table has the name of the scan.
#[test]
fn motion_over_function_sql() {
    let (first, second) = get_two_stage_sql(
        r#"SELECT g.value FROM generate_series(1, 3) g
           LEFT JOIN "t" ON g.value = "t"."a""#,
        1,
        Some((1, 0)),
    );
    assert_snapshot!(first, @r#"
    SELECT "g"."VALUE" as "value"
    FROM generate_series ($1::int, $2::int) as "g"
    "#);
    assert_snapshot!(second, @r#"
    SELECT "g"."COL_1" as "value", "t"."a", "t"."b", "t"."c", "t"."d", "t"."bucket_id"
    FROM "_tmp_0_0136" as "g"
    INNER JOIN "t"
    ON "g"."COL_1" = "t"."a"
    "#);
}

// A CTE over a table function gives its columns the names in queries.
#[test]
fn cte_over_function_sql() {
    let (first, second) = get_two_stage_sql(
        r#"WITH c AS (SELECT * FROM generate_series(1, 3))
           SELECT * FROM c c1 JOIN c c2 ON c1.value = c2.value"#,
        1,
        None,
    );
    assert_snapshot!(first, @r#"
    SELECT "generate_series"."VALUE" as "value"
    FROM generate_series ($1::int, $2::int) as "generate_series"
    "#);
    assert_snapshot!(second, @r#"
    SELECT *
    FROM (SELECT "COL_1"
    FROM "_tmp_0_0136") as "c1"
    INNER JOIN (SELECT "COL_1"
    FROM "_tmp_0_0136") as "c2"
    ON "c1"."COL_1" = "c2"."COL_1"
    "#);
}
