use crate::{
    executor::engine::mock::RouterConfigurationMock, frontend::sql::ast::AbstractSyntaxTree,
    frontend::sql::Ast, ir::transformation::helpers::sql_to_optimized_ir,
};
use pretty_assertions::assert_eq;

#[test]
fn like_valid() {
    let queries = vec![
        "SELECT a like a FROM t1",
        "SELECT a like a escape 'abc' FROM t1",
        "SELECT a || 'a' like a FROM t1",
        "SELECT a || 'a' like a escape 'a' FROM t1",
        "SELECT a || 'a' like a || 'a' FROM t1",
        "SELECT a || 'a' like a || 'a' escape 'a' FROM t1",
        "SELECT a || 'a' like a || 'a' escape 'a' || 'a' FROM t1",
        "SELECT not a || 'a' like a || 'a' FROM t1",
        "SELECT not a || 'a' like a || 'a' FROM t1",
        "SELECT true or a || 'a' like a || 'a' FROM t1",
        "SELECT true or a || 'a' like a || 'a' and false FROM t1",
        "SELECT true or a || 'a' like a || 'a' and false FROM t1",
        "SELECT true between false and 'a' like 'b'  FROM t1",
    ];
    for query in queries {
        let _ = sql_to_optimized_ir(query, vec![]);
    }
}

#[test]
fn like_invalid1() {
    let input = r#"select a like a escape 'a' escape 'a' from t1"#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, &[], metadata).unwrap_err();

    assert_eq!(
        "invalid expression: escape specified twice: expr1 LIKE/SIMILAR expr2 ESCAPE expr 3 ESCAPE expr4",
        err.to_string()
    );
}

#[test]
fn like_invalid2() {
    let input = r#"select a escape 'b' from t1"#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, &[], metadata).unwrap_err();

    assert_eq!(
        "invalid expression: ESCAPE can go only after LIKE or SIMILAR expressions, got: PlanId { plan_id: NodeId { offset: 3, arena_type: Arena96 } }",
        err.to_string()
    );
}

#[test]
fn like_explain1() {
    let input = r#"select a like a from t1 where a || 'a' like 'a' || 'a'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (ROW("t1"."a"::string) LIKE ROW("t1"."a"::string) ESCAPE ROW('\'::string) -> "col_1")
        selection ROW(ROW("t1"."a"::string) || ROW('a'::string)) LIKE ROW(ROW('a'::string) || ROW('a'::string)) ESCAPE ROW('\'::string)
            scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn like_explain2() {
    let input = r#"select a like a escape '\' from t1 where a || 'a' like 'a' || 'a' escape 'x'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"projection (ROW("t1"."a"::string) LIKE ROW("t1"."a"::string) ESCAPE ROW('\'::string) -> "col_1")
    selection ROW(ROW("t1"."a"::string) || ROW('a'::string)) LIKE ROW(ROW('a'::string) || ROW('a'::string)) ESCAPE ROW('x'::string)
        scan "t1"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
"#);
}

#[test]
fn like_explain3() {
    let input = r#"select a like a from t1 group by a like a"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::boolean -> "col_1")
        group by ("gr_expr_1"::boolean) output: ("gr_expr_1"::boolean -> "gr_expr_1")
            motion [policy: segment([ref("gr_expr_1")])]
                projection (ROW("t1"."a"::string) LIKE ROW("t1"."a"::string) ESCAPE ROW('\'::string) -> "gr_expr_1")
                    group by (ROW("t1"."a"::string) LIKE ROW("t1"."a"::string) ESCAPE ROW('\'::string)) output: ("t1"."a"::string -> "a", "t1"."bucket_id"::unsigned -> "bucket_id", "t1"."b"::integer -> "b")
                        scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn like_explain4() {
    let input = r#"select * from t1 where (select 'hi' from t1) like (select 'hi' from t1) escape (select '\' from t1)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."a"::string -> "a", "t1"."b"::integer -> "b")
        selection ROW($2) LIKE ROW($1) ESCAPE ROW($0)
            scan "t1"
    subquery $0:
    motion [policy: full]
                scan
                    projection ('\'::string -> "col_1")
                        scan "t1"
    subquery $1:
    motion [policy: full]
                scan
                    projection ('hi'::string -> "col_1")
                        scan "t1"
    subquery $2:
    motion [policy: full]
                scan
                    projection ('hi'::string -> "col_1")
                        scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn ilike_explain() {
    let input = r#"select a ilike a escape 'x' from t1 group by a ilike a escape 'x'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::boolean -> "col_1")
        group by ("gr_expr_1"::boolean) output: ("gr_expr_1"::boolean -> "gr_expr_1")
            motion [policy: segment([ref("gr_expr_1")])]
                projection (ROW(lower(("t1"."a"::string))::string) LIKE ROW(lower(("t1"."a"::string))::string) ESCAPE ROW('x'::string) -> "gr_expr_1")
                    group by (ROW(lower(("t1"."a"::string))::string) LIKE ROW(lower(("t1"."a"::string))::string) ESCAPE ROW('x'::string)) output: ("t1"."a"::string -> "a", "t1"."bucket_id"::unsigned -> "bucket_id", "t1"."b"::integer -> "b")
                        scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
