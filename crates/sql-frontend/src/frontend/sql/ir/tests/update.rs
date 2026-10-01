use crate::ir::value::Value;
use sql_executor::test_helpers::{
    expect_sql_to_ir_error, sql_to_ir_without_bind, sql_to_optimized_ir,
};
use sql_explain::explain::explain_logical;

#[test]
fn update1() {
    let pattern = r#"explain (logical) UPDATE "test_space" SET "FIRST_NAME" = 'test'"#;
    let plan = sql_to_optimized_ir(pattern, vec![]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    update test_space ("FIRST_NAME" = col_0)
      motion [policy: local, program: ReshardIfNeeded]
        projection ('test'::string -> col_0, test_space.id::int -> col_1)
          scan test_space
    "#);
}

#[test]
fn update2() {
    let pattern = r#"explain (logical) UPDATE "test_space" SET "FIRST_NAME" = ?"#;
    let plan = sql_to_optimized_ir(pattern, vec![Value::from("test")]);

    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r#"
    update test_space ("FIRST_NAME" = col_0)
      motion [policy: local, program: ReshardIfNeeded]
        projection ('test'::string -> col_0, test_space.id::int -> col_1)
          scan test_space
    "#);
}

#[test]
fn update_from_duplicate_table_name() {
    // The target table is an item of the FROM clause too.
    let queries = [
        "UPDATE t SET d = 1 FROM t WHERE true",
        "UPDATE t SET d = 1 FROM t1 AS t",
        "UPDATE t SET d = 1 FROM (SELECT 1) AS t",
    ];

    for query in queries {
        let error = expect_sql_to_ir_error(query, &[]);
        assert_eq!(
            error.to_string(),
            r#"table name "t" specified more than once"#,
            "{query}"
        );
    }
}

#[test]
fn update_from_distinct_table_names() {
    let queries = [
        "UPDATE t SET d = 1 FROM t AS t2 WHERE t.a = t2.a",
        // A subquery has a FROM clause of its own.
        "UPDATE t SET d = 1 FROM (SELECT a FROM t) AS s WHERE t.a = s.a",
    ];

    for query in queries {
        sql_to_ir_without_bind(query, &[]);
    }
}
