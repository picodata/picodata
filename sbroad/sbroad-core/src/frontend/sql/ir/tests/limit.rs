use crate::ir::transformation::helpers::sql_to_optimized_ir;
use pretty_assertions::assert_eq;

#[test]
fn select() {
    let sql = r#"SELECT "id" FROM "test_space" LIMIT 100"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"limit 100
    motion [policy: full]
        limit 100
            projection ("test_space"."id"::unsigned -> "id")
                scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn union_all() {
    let sql = r#"
    select "product_code" from "hash_testing"
    union all
    select "e" from "t2"
    limit 100
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"limit 100
    motion [policy: full]
        limit 100
            union all
                projection ("hash_testing"."product_code"::string -> "product_code")
                    scan "hash_testing"
                projection ("t2"."e"::unsigned -> "e")
                    scan "t2"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn aggregate() {
    let input = r#"SELECT min("b"), min(distinct "b") FROM "t" LIMIT 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"limit 1
    projection (min(("min_696"::unsigned))::scalar -> "col_1", min(distinct ("column_796"::unsigned))::scalar -> "col_2")
        motion [policy: full]
            projection ("t"."b"::unsigned -> "column_796", min(("t"."b"::unsigned))::scalar -> "min_696")
                group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn group_by() {
    let input = r#"SELECT cOuNt(*), "b" FROM "t" group by "b" limit 555"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"limit 555
    motion [policy: full]
        limit 555
            projection (sum(("count_1196"::integer))::decimal -> "col_1", "column_596"::unsigned -> "b")
                group by ("column_596"::unsigned) output: ("column_596"::unsigned -> "column_596", "count_1196"::integer -> "count_1196")
                    motion [policy: segment([ref("column_596")])]
                        projection ("t"."b"::unsigned -> "column_596", count((*::integer))::integer -> "count_1196")
                            group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                                scan "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn single_limit() {
    let sql = r#"SELECT * FROM (SELECT "id" FROM "test_space" LIMIT 1) LIMIT 1"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"limit 1
    projection ("id"::unsigned -> "id")
        scan
            limit 1
                motion [policy: full]
                    limit 1
                        projection ("test_space"."id"::unsigned -> "id")
                            scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn join() {
    let input = r#"SELECT * FROM "t1" LEFT JOIN "t2" ON "t1"."a" = "t2"."e"
    JOIN "t3" ON "t1"."a" = "t3"."a" JOIN "t4" ON "t2"."f" = "t4"."c"
    LIMIT 128
"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"limit 128
    motion [policy: full]
        limit 128
            projection ("t1"."a"::string -> "a", "t1"."b"::integer -> "b", "t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h", "t3"."a"::string -> "a", "t3"."b"::integer -> "b", "t4"."c"::string -> "c", "t4"."d"::integer -> "d")
                join on ROW("t2"."f"::unsigned) = ROW("t4"."c"::string)
                    join on ROW("t1"."a"::string) = ROW("t3"."a"::string)
                        left join on ROW("t1"."a"::string) = ROW("t2"."e"::unsigned)
                            scan "t1"
                                projection ("t1"."a"::string -> "a", "t1"."b"::integer -> "b")
                                    scan "t1"
                            motion [policy: full]
                                scan "t2"
                                    projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                                        scan "t2"
                        motion [policy: full]
                            scan "t3"
                                projection ("t3"."a"::string -> "a", "t3"."b"::integer -> "b")
                                    scan "t3"
                    motion [policy: full]
                        scan "t4"
                            projection ("t4"."c"::string -> "c", "t4"."d"::integer -> "d")
                                scan "t4"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn limit_all() {
    let sql = r#"SELECT "id" FROM "test_space" LIMIT ALL"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn limit_null() {
    let sql = r#"SELECT "id" FROM "test_space" LIMIT NULL"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}
