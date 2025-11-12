use crate::ir::transformation::helpers::sql_to_optimized_ir;

#[test]
fn front_select_chaning_1() {
    let input = r#"
    select "product_code" from "hash_testing"
    union all
    select "e" from "t2"
    union all
    select "a" from "t3"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    union all
        union all
            projection ("hash_testing"."product_code"::string -> "product_code")
                scan "hash_testing"
            projection ("t2"."e"::int -> "e")
                scan "t2"
        projection ("t3"."a"::string -> "a")
            scan "t3"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_select_chaining_2() {
    let input = r#"
    select "product_code" from "hash_testing"
    union all
    select "e" from "t2"
    union
    select "a" from "t3"
    except
    select "b" from "t3"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        motion [policy: full]
            union
                union all
                    projection ("hash_testing"."product_code"::string -> "product_code")
                        scan "hash_testing"
                    projection ("t2"."e"::int -> "e")
                        scan "t2"
                projection ("t3"."a"::string -> "a")
                    scan "t3"
        motion [policy: full]
            intersect
                projection ("t3"."b"::int -> "b")
                    scan "t3"
                motion [policy: full]
                    union
                        union all
                            projection ("hash_testing"."product_code"::string -> "product_code")
                                scan "hash_testing"
                            projection ("t2"."e"::int -> "e")
                                scan "t2"
                        projection ("t3"."a"::string -> "a")
                            scan "t3"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_select_chaining_3() {
    let input = r#"
    select "product_code" from "hash_testing"
    union all
    select "e" from "t2"
    order by 1
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("product_code"::string -> "product_code")
        order by (1)
            motion [policy: full]
                scan
                    union all
                        projection ("hash_testing"."product_code"::string -> "product_code")
                            scan "hash_testing"
                        projection ("t2"."e"::int -> "e")
                            scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn union_under_insert() {
    let input = r#"
    insert into t2
    select e, f, 1, 1 from t2
    union
    select f, e, 2, 2 from t2
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t2" on conflict: fail
        motion [policy: segment([ref("e"), ref("f")])]
            union
                projection ("t2"."e"::int -> "e", "t2"."f"::int -> "f", 1::int -> "col_1", 1::int -> "col_2")
                    scan "t2"
                projection ("t2"."f"::int -> "f", "t2"."e"::int -> "e", 2::int -> "col_1", 2::int -> "col_2")
                    scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn union_under_insert1() {
    let input = r#"
    insert into "TBL"
    select * from (values (1, 1))
    union
    select * from (values (2, 2))
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "TBL" on conflict: fail
        motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")])]
            union
                projection ("unnamed_subquery"."COLUMN_1"::int -> "COLUMN_1", "unnamed_subquery"."COLUMN_2"::int -> "COLUMN_2")
                    scan "unnamed_subquery"
                        motion [policy: full]
                            values
                                value row (data=ROW(1::int, 1::int))
                projection ("unnamed_subquery_1"."COLUMN_3"::int -> "COLUMN_3", "unnamed_subquery_1"."COLUMN_4"::int -> "COLUMN_4")
                    scan "unnamed_subquery_1"
                        motion [policy: full]
                            values
                                value row (data=ROW(2::int, 2::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
