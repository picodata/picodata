use super::*;

#[test]
fn delete1_test() {
    let sql = r#"DELETE FROM "t1""#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    delete "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn delete2_test() {
    let sql = r#"DELETE FROM "t1" where "b" > 3"#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    delete "t1"
        motion [policy: local]
            projection ("t1"."a"::string -> "pk_col_0", "t1"."b"::integer -> "pk_col_1")
                selection "t1"."b"::integer > 3::unsigned
                    scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn delete3_test() {
    let sql = r#"DELETE FROM "t1" where "a" in (SELECT "b"::text from "t1")"#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    delete "t1"
        motion [policy: local]
            projection ("t1"."a"::string -> "pk_col_0", "t1"."b"::integer -> "pk_col_1")
                selection "t1"."a"::string in ROW($0)
                    scan "t1"
    subquery $0:
    motion [policy: full]
                        scan
                            projection ("t1"."b"::integer::text -> "col_1")
                                scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
