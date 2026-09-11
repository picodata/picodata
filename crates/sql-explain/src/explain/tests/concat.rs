use crate::explain::explain_logical;
use sql_executor::test_helpers::sql_to_optimized_ir;

#[test]
fn concat1_test() {
    let sql = r#"explain (logical) SELECT CAST('1' as string) || 'hello' FROM "t1""#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    projection ('1'::string || 'hello'::string -> col_1)
      scan t1
    ");
}

#[test]
fn concat2_test() {
    let sql =
        r#"explain (logical) SELECT "a" FROM "t1" WHERE CAST('1' as string) || "a" || '2' = '42'"#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @"
    projection (t1.a::string -> a)
      selection (('1'::string || t1.a::string::string)::string || '2'::string = '42'::string)
        scan t1
    ");
}

/// Operands that are already casted to text don't get a second cast on top.
#[test]
fn concat_explicitly_casted_args_test() {
    let sql = r#"explain (logical) SELECT "a"::text || "b"::text FROM "t1""#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @"
    projection (t1.a::string::string || t1.b::int::string -> col_1)
      scan t1
    ");
}

/// A cast to a non-text type still has to be casted to text.
#[test]
fn concat_arg_casted_to_non_text_test() {
    let sql = r#"explain (logical) SELECT "a" || "b"::int FROM "t1""#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @"
    projection (t1.a::string::string || t1.b::int::int::string -> col_1)
      scan t1
    ");
}
