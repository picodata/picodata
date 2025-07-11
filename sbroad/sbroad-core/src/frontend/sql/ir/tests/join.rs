use crate::ir::transformation::helpers::sql_to_optimized_ir;

#[test]
fn milti_join1() {
    let input = r#"SELECT * FROM (
            SELECT "identification_number", "product_code" FROM "hash_testing"
        ) as t1
        INNER JOIN (SELECT "id" FROM "test_space") as t2
        ON t1."identification_number" = t2."id"
        LEFT JOIN (SELECT "id" FROM "test_space") as t3
        ON t1."identification_number" = t3."id"
        WHERE t1."identification_number" = 5 and t1."product_code" = '123'"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."identification_number"::integer -> "identification_number", "t1"."product_code"::string -> "product_code", "t2"."id"::unsigned -> "id", "t3"."id"::unsigned -> "id")
        selection ("t1"."identification_number"::integer = 5::unsigned) and ("t1"."product_code"::string = '123'::string)
            left join on "t1"."identification_number"::integer = "t3"."id"::unsigned
                join on "t1"."identification_number"::integer = "t2"."id"::unsigned
                    scan "t1"
                        projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
                            scan "hash_testing"
                    motion [policy: full]
                        scan "t2"
                            projection ("test_space"."id"::unsigned -> "id")
                                scan "test_space"
                motion [policy: full]
                    scan "t3"
                        projection ("test_space"."id"::unsigned -> "id")
                            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn milti_join2() {
    let input = r#"SELECT * FROM "t1_2" "t1" LEFT JOIN "t2" ON "t1"."a" = "t2"."e"
    LEFT JOIN "t4" ON true
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."a"::integer -> "a", "t1"."b"::integer -> "b", "t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h", "t4"."c"::string -> "c", "t4"."d"::integer -> "d")
        left join on true::boolean
            left join on "t1"."a"::integer = "t2"."e"::unsigned
                scan "t1"
                    projection ("t1"."a"::integer -> "a", "t1"."b"::integer -> "b")
                        scan "t1_2" -> "t1"
                motion [policy: full]
                    scan "t2"
                        projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                            scan "t2"
            motion [policy: full]
                scan "t4"
                    projection ("t4"."c"::string -> "c", "t4"."d"::integer -> "d")
                        scan "t4"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn milti_join3() {
    let input = r#"SELECT * FROM "t1_2" "t1" LEFT JOIN "t2" ON "t1"."a" = "t2"."e"
    JOIN "t3_2" "t3" ON "t1"."a" = "t3"."a" JOIN "t4" ON "t2"."f" = "t4"."c"::int
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."a"::integer -> "a", "t1"."b"::integer -> "b", "t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h", "t3"."a"::integer -> "a", "t3"."b"::integer -> "b", "t4"."c"::string -> "c", "t4"."d"::integer -> "d")
        join on "t2"."f"::unsigned = "t4"."c"::string::int
            join on "t1"."a"::integer = "t3"."a"::integer
                left join on "t1"."a"::integer = "t2"."e"::unsigned
                    scan "t1"
                        projection ("t1"."a"::integer -> "a", "t1"."b"::integer -> "b")
                            scan "t1_2" -> "t1"
                    motion [policy: full]
                        scan "t2"
                            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                                scan "t2"
                motion [policy: full]
                    scan "t3"
                        projection ("t3"."a"::integer -> "a", "t3"."b"::integer -> "b")
                            scan "t3_2" -> "t3"
            motion [policy: full]
                scan "t4"
                    projection ("t4"."c"::string -> "c", "t4"."d"::integer -> "d")
                        scan "t4"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn milti_join4() {
    let input = r#"SELECT "t1"."a" FROM "t1" JOIN "t1" as "t2" ON "t1"."a" = "t2"."a"
    JOIN "t3" ON "t1"."a" = "t3"."a"
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."a"::string -> "a")
        join on "t1"."a"::string = "t3"."a"::string
            join on "t1"."a"::string = "t2"."a"::string
                scan "t1"
                    projection ("t1"."a"::string -> "a", "t1"."b"::integer -> "b")
                        scan "t1"
                motion [policy: full]
                    scan "t2"
                        projection ("t2"."a"::string -> "a", "t2"."b"::integer -> "b")
                            scan "t1" -> "t2"
            motion [policy: full]
                scan "t3"
                    projection ("t3"."a"::string -> "a", "t3"."b"::integer -> "b")
                        scan "t3"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
