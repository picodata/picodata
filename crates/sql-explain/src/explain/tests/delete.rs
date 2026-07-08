use crate::explain::explain_logical;
use sql_executor::test_helpers::sql_to_optimized_ir;

#[test]
fn delete1_test() {
    let sql = r#"explain (logical) DELETE FROM "t1""#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @"delete from t1");
}

#[test]
fn delete2_test() {
    let sql = r#"explain (logical) DELETE FROM "t1" where "b" > 3"#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    delete from t1
      motion [policy: local, program: [PrimaryKey(0, 1), ReshardIfNeeded]]
        projection (t1.a::string -> pk_col_0, t1.b::int -> pk_col_1)
          selection (t1.b::int > 3::int)
            scan t1
    ");
}

#[test]
fn delete3_test() {
    let sql = r#"explain (logical) DELETE FROM "t1" where "a" in (SELECT "b"::text from "t1")"#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(explain_logical(&plan).unwrap(), @r"
    delete from t1
      motion [policy: local, program: [PrimaryKey(0, 1), ReshardIfNeeded]]
        projection (t1.a::string -> pk_col_0, t1.b::int -> pk_col_1)
          selection (t1.a::string in ROW($0))
            scan t1
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t1.b::int::string -> col_1)
            scan t1
    ");
}
