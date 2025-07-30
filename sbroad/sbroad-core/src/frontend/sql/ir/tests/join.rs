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
    projection ("t1"."identification_number"::int -> "identification_number", "t1"."product_code"::string -> "product_code", "t2"."id"::int -> "id", "t3"."id"::int -> "id")
        selection ("t1"."identification_number"::int = 5::int) and ("t1"."product_code"::string = '123'::string)
            left join on "t1"."identification_number"::int = "t3"."id"::int
                join on "t1"."identification_number"::int = "t2"."id"::int
                    scan "t1"
                        projection ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
                            scan "hash_testing"
                    motion [policy: full]
                        scan "t2"
                            projection ("test_space"."id"::int -> "id")
                                scan "test_space"
                motion [policy: full]
                    scan "t3"
                        projection ("test_space"."id"::int -> "id")
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
    projection ("t1"."a"::int -> "a", "t1"."b"::int -> "b", "t2"."e"::int -> "e", "t2"."f"::int -> "f", "t2"."g"::int -> "g", "t2"."h"::int -> "h", "t4"."c"::string -> "c", "t4"."d"::int -> "d")
        left join on true::bool
            left join on "t1"."a"::int = "t2"."e"::int
                scan "t1_2" -> "t1"
                motion [policy: full]
                    projection ("t2"."e"::int -> "e", "t2"."f"::int -> "f", "t2"."g"::int -> "g", "t2"."h"::int -> "h", "t2"."bucket_id"::int -> "bucket_id")
                        scan "t2"
            motion [policy: full]
                projection ("t4"."bucket_id"::int -> "bucket_id", "t4"."c"::string -> "c", "t4"."d"::int -> "d")
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
    projection ("t1"."a"::int -> "a", "t1"."b"::int -> "b", "t2"."e"::int -> "e", "t2"."f"::int -> "f", "t2"."g"::int -> "g", "t2"."h"::int -> "h", "t3"."a"::int -> "a", "t3"."b"::int -> "b", "t4"."c"::string -> "c", "t4"."d"::int -> "d")
        join on "t2"."f"::int = "t4"."c"::string::int
            join on "t1"."a"::int = "t3"."a"::int
                left join on "t1"."a"::int = "t2"."e"::int
                    scan "t1_2" -> "t1"
                    motion [policy: full]
                        projection ("t2"."e"::int -> "e", "t2"."f"::int -> "f", "t2"."g"::int -> "g", "t2"."h"::int -> "h", "t2"."bucket_id"::int -> "bucket_id")
                            scan "t2"
                motion [policy: full]
                    projection ("t3"."bucket_id"::int -> "bucket_id", "t3"."a"::int -> "a", "t3"."b"::int -> "b")
                        scan "t3_2" -> "t3"
            motion [policy: full]
                projection ("t4"."bucket_id"::int -> "bucket_id", "t4"."c"::string -> "c", "t4"."d"::int -> "d")
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
                motion [policy: full]
                    projection ("t2"."a"::string -> "a", "t2"."bucket_id"::int -> "bucket_id", "t2"."b"::int -> "b")
                        scan "t1" -> "t2"
            motion [policy: full]
                projection ("t3"."bucket_id"::int -> "bucket_id", "t3"."a"::string -> "a", "t3"."b"::int -> "b")
                    scan "t3"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}
