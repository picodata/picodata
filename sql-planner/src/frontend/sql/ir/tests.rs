use crate::errors::SbroadError;
use crate::executor::engine::mock::RouterConfigurationMock;
use crate::frontend::sql::ast::{AbstractSyntaxTree, ParseTree, Rule};
use crate::frontend::sql::ParsingPairsMap;
use crate::frontend::Ast;
use crate::ir::node::relational::Relational;
use crate::ir::node::NodeId;
use crate::ir::options::Options;
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::tree::traversal::PostOrder;
use crate::ir::types::{DerivedType, UnrestrictedType as Type};
use crate::ir::value::Value;
use crate::ir::{Plan, Positions};
use itertools::Itertools;
use pest::Parser;
use pretty_assertions::assert_eq;
use std::collections::HashMap;

fn sql_to_optimized_ir_add_motions_err(query: &str) -> SbroadError {
    let metadata = &RouterConfigurationMock::new();
    AbstractSyntaxTree::transform_into_plan(query, &[], metadata)
        .unwrap()
        .replace_in_operator()
        .unwrap()
        .push_down_not()
        .unwrap()
        .split_columns()
        .unwrap()
        .set_dnf()
        .unwrap()
        .derive_equalities()
        .unwrap()
        .merge_tuples()
        .unwrap()
        .add_motions()
        .unwrap_err()
}

#[test]
fn front_sql1() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "identification_number" = 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
        selection "hash_testing"."identification_number"::int = 1::int
            scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql2() {
    let input = r#"SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1 AND "product_code" = '1'
        OR "identification_number" = 2 AND "product_code" = '2'"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
        selection (("hash_testing"."identification_number"::int = 1::int) and ("hash_testing"."product_code"::string = '1'::string)) or (("hash_testing"."identification_number"::int = 2::int) and ("hash_testing"."product_code"::string = '2'::string))
            scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql3() {
    let input = r#"SELECT *
        FROM
            (SELECT "identification_number", "product_code"
            FROM "hash_testing"
            WHERE "sys_op" = 1
            UNION ALL
            SELECT "identification_number", "product_code"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 1) AS "t3"
        WHERE "identification_number" = 1"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t3"."identification_number"::int -> "identification_number", "t3"."product_code"::string -> "product_code")
        selection "t3"."identification_number"::int = 1::int
            scan "t3"
                union all
                    projection ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
                        selection "hash_testing"."sys_op"::int = 1::int
                            scan "hash_testing"
                    projection ("hash_testing_hist"."identification_number"::int -> "identification_number", "hash_testing_hist"."product_code"::string -> "product_code")
                        selection "hash_testing_hist"."sys_op"::int > 1::int
                            scan "hash_testing_hist"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql4() {
    let input = r#"SELECT *
        FROM
            (SELECT "identification_number", "product_code"
            FROM "hash_testing"
            WHERE "sys_op" = 1
            UNION ALL
            SELECT "identification_number", "product_code"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 1) AS "t3"
        WHERE ("identification_number" = 1
            OR ("identification_number" = 2
            OR "identification_number" = 3))
            AND ("product_code" = '1'
            OR "product_code" = '2')"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t3"."identification_number"::int -> "identification_number", "t3"."product_code"::string -> "product_code")
        selection (("t3"."identification_number"::int = 1::int) or (("t3"."identification_number"::int = 2::int) or ("t3"."identification_number"::int = 3::int))) and (("t3"."product_code"::string = '1'::string) or ("t3"."product_code"::string = '2'::string))
            scan "t3"
                union all
                    projection ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
                        selection "hash_testing"."sys_op"::int = 1::int
                            scan "hash_testing"
                    projection ("hash_testing_hist"."identification_number"::int -> "identification_number", "hash_testing_hist"."product_code"::string -> "product_code")
                        selection "hash_testing_hist"."sys_op"::int > 1::int
                            scan "hash_testing_hist"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql5() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "identification_number" in (
        SELECT "identification_number" FROM "hash_testing_hist" WHERE "product_code" = 'a')"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
        selection "hash_testing"."identification_number"::int in ROW($0)
            scan "hash_testing"
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
                scan
                    projection ("hash_testing_hist"."identification_number"::int -> "identification_number")
                        selection "hash_testing_hist"."product_code"::string = 'a'::string
                            scan "hash_testing_hist"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql6() {
    let input = r#"SELECT "id", "product_units" FROM "hash_testing"
        INNER JOIN (SELECT "id" FROM "test_space") as t
        ON "hash_testing"."identification_number" = t."id"
        WHERE "hash_testing"."identification_number" = 5 and "hash_testing"."product_code" = '123'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."id"::int -> "id", "hash_testing"."product_units"::bool -> "product_units")
        selection ("hash_testing"."identification_number"::int = 5::int) and ("hash_testing"."product_code"::string = '123'::string)
            join on "hash_testing"."identification_number"::int = "t"."id"::int
                scan "hash_testing"
                motion [policy: full, program: ReshardIfNeeded]
                    scan "t"
                        projection ("test_space"."id"::int -> "id")
                            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql8() {
    let input = r#"SELECT t."identification_number", "product_code" FROM "hash_testing" as t
        WHERE t."identification_number" = 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."identification_number"::int -> "identification_number", "t"."product_code"::string -> "product_code")
        selection "t"."identification_number"::int = 1::int
            scan "hash_testing" -> "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql9() {
    let input = r#"SELECT *
        FROM
            (SELECT "id", "FIRST_NAME"
            FROM "test_space"
            WHERE "sys_op" < 0
                    AND "sysFrom" >= 0
            UNION ALL
            SELECT "id", "FIRST_NAME"
            FROM "test_space_hist"
            WHERE "sysFrom" <= 0) AS "t3"
        INNER JOIN
            (SELECT "identification_number", "product_code"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 0
            UNION ALL
            SELECT "identification_number", "product_code"
            FROM "hash_single_testing_hist"
            WHERE "sys_op" <= 0) AS "t8"
            ON "t3"."id" = "t8"."identification_number"
        WHERE "id" = 1 AND "t8"."identification_number" = 1 AND "product_code" = '123'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t3"."id"::int -> "id", "t3"."FIRST_NAME"::string -> "FIRST_NAME", "t8"."identification_number"::int -> "identification_number", "t8"."product_code"::string -> "product_code")
        selection (("t3"."id"::int = 1::int) and ("t8"."identification_number"::int = 1::int)) and ("t8"."product_code"::string = '123'::string)
            join on "t3"."id"::int = "t8"."identification_number"::int
                scan "t3"
                    union all
                        projection ("test_space"."id"::int -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                            selection ("test_space"."sys_op"::int < 0::int) and ("test_space"."sysFrom"::int >= 0::int)
                                scan "test_space"
                        projection ("test_space_hist"."id"::int -> "id", "test_space_hist"."FIRST_NAME"::string -> "FIRST_NAME")
                            selection "test_space_hist"."sysFrom"::int <= 0::int
                                scan "test_space_hist"
                motion [policy: segment([ref("identification_number")]), program: ReshardIfNeeded]
                    scan "t8"
                        union all
                            projection ("hash_testing_hist"."identification_number"::int -> "identification_number", "hash_testing_hist"."product_code"::string -> "product_code")
                                selection "hash_testing_hist"."sys_op"::int > 0::int
                                    scan "hash_testing_hist"
                            projection ("hash_single_testing_hist"."identification_number"::int -> "identification_number", "hash_single_testing_hist"."product_code"::string -> "product_code")
                                selection "hash_single_testing_hist"."sys_op"::int <= 0::int
                                    scan "hash_single_testing_hist"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql10() {
    let input = r#"INSERT INTO "t" VALUES(1, 2, 3, 4)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: fail
        motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")]), program: ReshardIfNeeded]
            values
                value row (data=ROW(1::int, 2::int, 3::int, 4::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql11() {
    let input = r#"INSERT INTO "t" ("b", "d") VALUES(1, 2)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: fail
        motion [policy: segment([value(NULL), ref("COLUMN_1")]), program: ReshardIfNeeded]
            values
                value row (data=ROW(1::int, 2::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql14() {
    let input = r#"INSERT INTO "t" ("b", "c") SELECT "b", "d" FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: fail
        motion [policy: segment([value(NULL), ref("b")]), program: ReshardIfNeeded]
            projection ("t"."b"::int -> "b", "t"."d"::int -> "d")
                scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

// check cyrillic strings support
#[test]
fn front_sql16() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "product_code" = 'кириллица'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
        selection "hash_testing"."product_code"::string = 'кириллица'::string
            scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql17() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        WHERE "product_code" IS NULL"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("hash_testing"."identification_number"::int -> "identification_number")
        selection "hash_testing"."product_code"::string is null
            scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql18() {
    let input = r#"SELECT "product_code" FROM "hash_testing"
        WHERE "product_code" BETWEEN '1' AND '2'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("hash_testing"."product_code"::string -> "product_code")
        selection ("hash_testing"."product_code"::string >= '1'::string) and ("hash_testing"."product_code"::string <= '2'::string)
            scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql19() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        WHERE "product_code" IS NOT NULL"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("hash_testing"."identification_number"::int -> "identification_number")
        selection not ("hash_testing"."product_code"::string is null)
            scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_is_true() {
    let input = r#"select true is true"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (true::bool = true::bool -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    let input = r#"select true is not true"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (not (true::bool = true::bool) -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_is_false() {
    let input = r#"select true is false"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (true::bool = false::bool -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    let input = r#"select true is not false"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (not (true::bool = false::bool) -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_is_null_unknown() {
    let input = r#"select true is null"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (true::bool is null -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    let input = r#"select true is unknown"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (true::bool is null -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    let input = r#"select true is not null"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (not (true::bool is null) -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    let input = r#"select true is not unknown"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (not (true::bool is null) -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_between_with_additional_and_from_left() {
    // Was previously misinterpreted as
    //                  SELECT "id" FROM "test_space" as "t" WHERE ("t"."id" > 1 AND "t"."id") BETWEEN "t"."id" AND "t"."id" + 10
    let input = r#"SELECT "id" FROM "test_space" as "t" WHERE "t"."id" > 1 AND "t"."id" BETWEEN "t"."id" AND "t"."id" + 10"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."id"::int -> "id")
        selection (("t"."id"::int > 1::int) and ("t"."id"::int >= "t"."id"::int)) and ("t"."id"::int <= ("t"."id"::int + 10::int))
            scan "test_space" -> "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_between_with_additional_not_from_left() {
    // Was previously misinterpreted as
    //                  SELECT "id" FROM "test_space" as "t" WHERE (not "t"."id") BETWEEN "t"."id" AND "t"."id" + 10 and true
    let input = r#"SELECT "id" FROM "test_space" as "t" WHERE not "t"."id" BETWEEN "t"."id" AND "t"."id" + 10 and true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."id"::int -> "id")
        selection (not (("t"."id"::int >= "t"."id"::int) and ("t"."id"::int <= ("t"."id"::int + 10::int)))) and true::bool
            scan "test_space" -> "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_between_with_additional_and_from_left_and_right() {
    // Was previously misinterpreted as
    //                  SELECT "id" FROM "test_space" as "t" WHERE ("t"."id" > 1 AND "t"."id") BETWEEN "t"."id" AND "t"."id" + 10 AND true
    let input = r#"SELECT "id" FROM "test_space" as "t" WHERE "t"."id" > 1 AND "t"."id" BETWEEN "t"."id" AND "t"."id" + 10 AND true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."id"::int -> "id")
        selection ((("t"."id"::int > 1::int) and ("t"."id"::int >= "t"."id"::int)) and ("t"."id"::int <= ("t"."id"::int + 10::int))) and true::bool
            scan "test_space" -> "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_between_with_nested_not_from_the_left() {
    // `not not false between false and true` should be interpreted as
    // `not (not (false between false and true))`
    let input =
        r#"SELECT "id" FROM "test_space" as "t" WHERE not not false between false and true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."id"::int -> "id")
        selection true::bool
            scan "test_space" -> "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_between_with_nested_and_from_the_left() {
    // `false and true and false between false and true` should be interpreted as
    // `(false and true) and (false between false and true)`
    let input = r#"SELECT "id" FROM "test_space" as "t" WHERE false and true and false between false and true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."id"::int -> "id")
        selection false::bool
            scan "test_space" -> "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_between_invalid() {
    let invalid_between_expressions = vec![
        r#"SELECT * FROM "test_space" WHERE 1 BETWEEN 2"#,
        r#"SELECT * FROM "test_space" WHERE 1 BETWEEN 2 OR 3"#,
    ];

    for invalid_expr in invalid_between_expressions {
        let metadata = &RouterConfigurationMock::new();
        let plan = AbstractSyntaxTree::transform_into_plan(invalid_expr, &[], metadata);
        let err = plan.unwrap_err();

        assert_eq!(
            true,
            err.to_string().contains(
                "BETWEEN operator should have a view of `expr_1 BETWEEN expr_2 AND expr_3`."
            )
        );
    }
}

#[test]
fn front_sql_parse_inner_join() {
    let input = r#"SELECT * FROM "hash_testing"
        left join "hash_testing" on true inner join "hash_testing" on true"#;

    // Check there are no panics
    let _ = sql_to_optimized_ir(input, vec![]);
    assert_eq!(true, true)
}

#[test]
fn front_sql_check_arbitrary_utf_in_single_quote_strings() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        WHERE "product_code" = '«123»§#*&%@/// / // \\ ƵǖḘỺʥ ͑ ͑  ͕ΆΨѮښ ۞ܤ'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("hash_testing"."identification_number"::int -> "identification_number")
        selection "hash_testing"."product_code"::string = '«123»§#*&%@/// / // \\ ƵǖḘỺʥ ͑ ͑  ͕ΆΨѮښ ۞ܤ'::string
            scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_check_single_quotes_are_escaped() {
    let input = "select '', '''', 'left''right', '''center'''";
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (''::string -> "col_1", '''::string -> "col_2", 'left'right'::string -> "col_3", ''center''::string -> "col_4")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_check_arbitraty_utf_in_identifiers() {
    let input = r#"SELECT "id" "from", "id" as "select", "id"
                               "123»&%ښ۞@Ƶǖselect.""''\\"
                                , "id" aц1&@$Ƶǖ^&«»§&ښ۞@Ƶǖ FROM "test_space" &ښ۞@Ƶǖ"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("&ښ۞@ƶǖ"."id"::int -> "from", "&ښ۞@ƶǖ"."id"::int -> "select", "&ښ۞@ƶǖ"."id"::int -> "123»&%ښ۞@Ƶǖselect.""''\\", "&ښ۞@ƶǖ"."id"::int -> "aц1&@$ƶǖ^&«»§&ښ۞@ƶǖ")
        scan "test_space" -> "&ښ۞@ƶǖ"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_check_inapplicatable_symbols() {
    let input = r#"
    SELECT "A"*"A", "B"+"B", "A"-"A"
    FROM "TBL"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("TBL"."A"::int * "TBL"."A"::int -> "col_1", "TBL"."B"::int + "TBL"."B"::int -> "col_2", "TBL"."A"::int - "TBL"."A"::int -> "col_3")
        scan "TBL"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_projection_with_scan_specification_under_scan() {
    let input = r#"SELECT "hash_testing".* FROM "hash_testing""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::bool -> "product_units", "hash_testing"."sys_op"::int -> "sys_op")
        scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_projection_with_scan_specification_under_join() {
    let input = r#"SELECT "hash_testing".* FROM "hash_testing" join "test_space" on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::bool -> "product_units", "hash_testing"."sys_op"::int -> "sys_op")
        join on true::bool
            scan "hash_testing"
            motion [policy: full, program: ReshardIfNeeded]
                projection ("test_space"."id"::int -> "id", "test_space"."sysFrom"::int -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::int -> "sys_op", "test_space"."bucket_id"::int -> "bucket_id")
                    scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_projection_with_scan_specification_under_join_of_subqueries() {
    let input = r#"SELECT "ts_sq".*, "hs".* FROM "hash_testing" as "hs"
                                join (select "ts".* from "test_space" as "ts") as "ts_sq" on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("ts_sq"."id"::int -> "id", "ts_sq"."sysFrom"::int -> "sysFrom", "ts_sq"."FIRST_NAME"::string -> "FIRST_NAME", "ts_sq"."sys_op"::int -> "sys_op", "hs"."identification_number"::int -> "identification_number", "hs"."product_code"::string -> "product_code", "hs"."product_units"::bool -> "product_units", "hs"."sys_op"::int -> "sys_op")
        join on true::bool
            scan "hash_testing" -> "hs"
            motion [policy: full, program: ReshardIfNeeded]
                scan "ts_sq"
                    projection ("ts"."id"::int -> "id", "ts"."sysFrom"::int -> "sysFrom", "ts"."FIRST_NAME"::string -> "FIRST_NAME", "ts"."sys_op"::int -> "sys_op")
                        scan "test_space" -> "ts"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_order_by_with_simple_select() {
    let input = r#"select * from "test_space" order by "id""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("id"::int -> "id", "sysFrom"::int -> "sysFrom", "FIRST_NAME"::string -> "FIRST_NAME", "sys_op"::int -> "sys_op")
        order by ("id"::int)
            motion [policy: full, program: ReshardIfNeeded]
                scan
                    projection ("test_space"."id"::int -> "id", "test_space"."sysFrom"::int -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::int -> "sys_op")
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_order_by_with_param() {
    let input = r#"select * from "test_space" order by ?"#;

    let metadata = &RouterConfigurationMock::new();
    let params_types = [DerivedType::new(Type::Integer)];
    let plan = AbstractSyntaxTree::transform_into_plan(input, &params_types, metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string().contains(
            "Using parameter as a standalone ORDER BY expression doesn't influence sorting"
        )
    );
}

#[test]
fn front_order_by_without_position_and_reference() {
    let input = r#"select * from "test_space" order by 1 + 8 asc, true and 'value'"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, &[], metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("ORDER BY element that is not position and doesn't contain reference doesn't influence ordering")
    );
}

#[test]
fn front_order_by_with_order_type_specification() {
    let input = r#"select * from "test_space" order by "id" desc, "sysFrom" asc"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("id"::int -> "id", "sysFrom"::int -> "sysFrom", "FIRST_NAME"::string -> "FIRST_NAME", "sys_op"::int -> "sys_op")
        order by ("id"::int desc, "sysFrom"::int asc)
            motion [policy: full, program: ReshardIfNeeded]
                scan
                    projection ("test_space"."id"::int -> "id", "test_space"."sysFrom"::int -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::int -> "sys_op")
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_order_by_with_indices() {
    let input = r#"select * from "test_space" order by 2, 1 desc"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("id"::int -> "id", "sysFrom"::int -> "sysFrom", "FIRST_NAME"::string -> "FIRST_NAME", "sys_op"::int -> "sys_op")
        order by (2, 1 desc)
            motion [policy: full, program: ReshardIfNeeded]
                scan
                    projection ("test_space"."id"::int -> "id", "test_space"."sysFrom"::int -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::int -> "sys_op")
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_order_by_ordering_by_expressions_from_projection() {
    let input =
        r#"select "id" as "my_col", "id" from "test_space" order by "my_col", "id", 1 desc, 2 asc"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("my_col"::int -> "my_col", "id"::int -> "id")
        order by ("my_col"::int, "id"::int, 1 desc, 2 asc)
            motion [policy: full, program: ReshardIfNeeded]
                scan
                    projection ("test_space"."id"::int -> "my_col", "test_space"."id"::int -> "id")
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_order_by_with_indices_bigger_than_projection_output_length() {
    let input = r#"select "id" from "test_space" order by 1 asc, 2 desc, 3"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, &[], metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("Ordering index (2) is bigger than child projection output length (1).")
    );
}

#[test]
fn front_order_by_over_single_distribution_must_not_add_motion() {
    let input = r#"select "id_count" from
                        (select count("id") as "id_count" from "test_space")
                        order by "id_count""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("id_count"::int -> "id_count")
        order by ("id_count"::int)
            scan
                projection ("unnamed_subquery"."id_count"::int -> "id_count")
                    scan "unnamed_subquery"
                        projection (sum(("count_1"::int))::int -> "id_count")
                            motion [policy: full, program: ReshardIfNeeded]
                                projection (count(("test_space"."id"::int))::int -> "count_1")
                                    scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_join_with_identical_columns() {
    let input = r#"select * from (select "sysFrom" from "test_space") join (select "sysFrom" from "test_space") on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("unnamed_subquery"."sysFrom"::int -> "sysFrom", "unnamed_subquery_1"."sysFrom"::int -> "sysFrom")
        join on true::bool
            scan "unnamed_subquery"
                projection ("test_space"."sysFrom"::int -> "sysFrom")
                    scan "test_space"
            motion [policy: full, program: ReshardIfNeeded]
                scan "unnamed_subquery_1"
                    projection ("test_space"."sysFrom"::int -> "sysFrom")
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_join_with_vtable_ambiguous_column_name() {
    let input = r#"select * from "test_space"
                        join (
                            select * from (select "id" from "test_space") t1
                            join (select "id" from "test_space") t2
                            on true
                        )
                        on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::int -> "id", "test_space"."sysFrom"::int -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::int -> "sys_op", "unnamed_subquery"."id"::int -> "id", "unnamed_subquery"."id"::int -> "id")
        join on true::bool
            scan "test_space"
            motion [policy: full, program: ReshardIfNeeded]
                scan "unnamed_subquery"
                    projection ("t1"."id"::int -> "id", "t2"."id"::int -> "id")
                        join on true::bool
                            scan "t1"
                                projection ("test_space"."id"::int -> "id")
                                    scan "test_space"
                            motion [policy: full, program: ReshardIfNeeded]
                                scan "t2"
                                    projection ("test_space"."id"::int -> "id")
                                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_case_search() {
    let input = r#"select
                            case "id" when 1 then true end
                        from
                        "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (case "test_space"."id"::int when 1::int then true::bool end -> "col_1")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_case_simple() {
    let input = r#"select
                            case
                                when true = true then 'Moscow'
                                when 1 != 2 and 4 < 5 then '42'
                                else 'false'
                            end as "case_result"
                        from
                        "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (case when true::bool = true::bool then 'Moscow'::string when (1::int <> 2::int) and (4::int < 5::int) then '42'::string else 'false'::string end -> "case_result")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_case_nested() {
    let input = r#"select
                            case "id"
                                when 1 then
                                    case "sysFrom"
                                        when 69 then true
                                        when 42 then false
                                    end
                                when 2 then 42 = 42
                                else false
                            end as "case_result"
                        from
                        "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (case "test_space"."id"::int when 1::int then case "test_space"."sysFrom"::int when 69::int then true::bool when 42::int then false::bool end when 2::int then 42::int = 42::int else false::bool end -> "case_result")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_subquery_column_duplicates() {
    let input = r#"SELECT "id" FROM "test_space" WHERE ("id", "id")
        IN (SELECT "id", "id" from "test_space")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::int -> "id")
        selection ROW("test_space"."id"::int, "test_space"."id"::int) in ROW($0, $0)
            scan "test_space"
    subquery $0:
    scan
                projection ("test_space"."id"::int -> "id", "test_space"."id"::int -> "id")
                    scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

impl Plan {
    fn get_positions(&self, node_id: NodeId) -> Option<Positions> {
        let mut context = self.context_mut();
        context
            .get_shard_columns_positions(node_id, self)
            .unwrap()
            .copied()
    }
}

#[test]
fn track_shard_col_pos() {
    let input = r#"
    select "e", "bucket_id", "f"
    from "t2"
    where "e" + "f" = 3
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    let dfs = PostOrder::with_capacity(|x| plan.nodes.rel_iter(x), 10);
    for level_node in dfs.into_iter(top) {
        let node_id = level_node.1;
        let node = plan.get_relation_node(node_id).unwrap();
        match node {
            Relational::ScanRelation(_) | Relational::Selection(_) => {
                assert_eq!([Some(4_usize), None], plan.get_positions(node_id).unwrap())
            }
            Relational::Projection(_) => {
                assert_eq!([Some(1_usize), None], plan.get_positions(node_id).unwrap())
            }
            _ => {}
        }
    }

    let input = r#"select t_mv."bucket_id", "t2"."bucket_id" from "t2" join (
        select "bucket_id" from "test_space" where "id" = 1
    ) as t_mv
    on t_mv."bucket_id" = "t2"."bucket_id";
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    let dfs = PostOrder::with_capacity(|x| plan.nodes.rel_iter(x), 10);
    for level_node in dfs.into_iter(top) {
        let node_id = level_node.1;
        let node = plan.get_relation_node(node_id).unwrap();
        if let Relational::Join(_) = node {
            assert_eq!(
                [Some(4_usize), Some(5_usize)],
                plan.get_positions(node_id).unwrap()
            );
        }
    }
    assert_eq!(
        [Some(0_usize), Some(1_usize)],
        plan.get_positions(top).unwrap()
    );

    let input = r#"select t_mv."bucket_id", "t2"."bucket_id" from "t2" join (
        select "bucket_id" from "test_space" where "id" = 1
    ) as t_mv
    on t_mv."bucket_id" < "t2"."bucket_id";
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    let dfs = PostOrder::with_capacity(|x| plan.nodes.rel_iter(x), 10);
    for level_node in dfs.into_iter(top) {
        let node_id = level_node.1;
        let node = plan.get_relation_node(node_id).unwrap();
        if let Relational::Join(_) = node {
            assert_eq!([Some(4_usize), None], plan.get_positions(node_id).unwrap());
        }
    }
    assert_eq!([Some(1_usize), None], plan.get_positions(top).unwrap());

    let input = r#"
    select "bucket_id", "e" from "t2"
    union all
    select "id", "bucket_id" from "test_space"
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    assert_eq!(None, plan.get_positions(top));

    let input = r#"
    select "bucket_id", "e" from "t2"
    union all
    select "bucket_id", "id" from "test_space"
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    assert_eq!([Some(0_usize), None], plan.get_positions(top).unwrap());

    let input = r#"
    select "e" from (select "bucket_id" as "e" from "t2")
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    assert_eq!([Some(0_usize), None], plan.get_positions(top).unwrap());

    let input = r#"
    select "e" as "bucket_id" from "t2"
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    assert_eq!(None, plan.get_positions(top));
}

#[test]
fn front_sql_join_on_bucket_id1() {
    let input = r#"select * from "t2" join (
        select "bucket_id" from "test_space" where "id" = 1
    ) as t_mv
    on t_mv."bucket_id" = "t2"."bucket_id";
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t2"."e"::int -> "e", "t2"."f"::int -> "f", "t2"."g"::int -> "g", "t2"."h"::int -> "h", "t_mv"."bucket_id"::int -> "bucket_id")
        join on "t_mv"."bucket_id"::int = "t2"."bucket_id"::int
            scan "t2"
            scan "t_mv"
                projection ("test_space"."bucket_id"::int -> "bucket_id")
                    selection "test_space"."id"::int = 1::int
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_join_on_bucket_id2() {
    let input = r#"select * from "t2" join (
        select "bucket_id" from "test_space" where "id" = 1
    ) as t_mv
    on t_mv."bucket_id" = "t2"."bucket_id" or "t2"."e" = "t2"."f";
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t2"."e"::int -> "e", "t2"."f"::int -> "f", "t2"."g"::int -> "g", "t2"."h"::int -> "h", "t_mv"."bucket_id"::int -> "bucket_id")
        join on ("t_mv"."bucket_id"::int = "t2"."bucket_id"::int) or ("t2"."e"::int = "t2"."f"::int)
            scan "t2"
            motion [policy: full, program: ReshardIfNeeded]
                scan "t_mv"
                    projection ("test_space"."bucket_id"::int -> "bucket_id")
                        selection "test_space"."id"::int = 1::int
                            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby_on_bucket_id() {
    let input = r#"
    select b, count(*) from (select "bucket_id" as b from "t2") as t
    group by b
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."b"::int -> "b", count((*::int))::int -> "col_1")
        group by ("t"."b"::int) output: ("t"."b"::int -> "b")
            scan "t"
                projection ("t2"."bucket_id"::int -> "b")
                    scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_sq_on_bucket_id() {
    let input = r#"
    select b, e from (select "bucket_id" as b, "e" as e from "t2") as t
    where (b, e) in (select "bucket_id", "id" from "test_space")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."b"::int -> "b", "t"."e"::int -> "e")
        selection ROW("t"."b"::int, "t"."e"::int) in ROW($0, $0)
            scan "t"
                projection ("t2"."bucket_id"::int -> "b", "t2"."e"::int -> "e")
                    scan "t2"
    subquery $0:
    scan
                projection ("test_space"."bucket_id"::int -> "bucket_id", "test_space"."id"::int -> "id")
                    scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_except_on_bucket_id() {
    let input = r#"
    select "e", "bucket_id" from "t2"
    except
    select "id", "bucket_id" from "test_space"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        projection ("t2"."e"::int -> "e", "t2"."bucket_id"::int -> "bucket_id")
            scan "t2"
        projection ("test_space"."id"::int -> "id", "test_space"."bucket_id"::int -> "bucket_id")
            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_exists_subquery_select_from_table() {
    let input = r#"SELECT "id" FROM "test_space" WHERE EXISTS (SELECT 0 FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::int -> "id")
        selection exists ROW($0)
            scan "test_space"
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
                scan
                    projection (0::int -> "col_1")
                        scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_not_exists_subquery_select_from_table() {
    let input = r#"SELECT "id" FROM "test_space" WHERE NOT EXISTS (SELECT 0 FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::int -> "id")
        selection not exists ROW($0)
            scan "test_space"
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
                scan
                    projection (0::int -> "col_1")
                        scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_exists_subquery_select_from_table_with_condition() {
    let input = r#"SELECT "id" FROM "test_space" WHERE EXISTS (SELECT 0 FROM "hash_testing" WHERE "identification_number" != 42)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::int -> "id")
        selection exists ROW($0)
            scan "test_space"
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
                scan
                    projection (0::int -> "col_1")
                        selection "hash_testing"."identification_number"::int <> 42::int
                            scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing" group by "identification_number", "product_code""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "identification_number", "gr_expr_2"::string -> "product_code")
        group by ("gr_expr_1"::int, "gr_expr_2"::string) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::string -> "gr_expr_2")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("hash_testing"."identification_number"::int -> "gr_expr_1", "hash_testing"."product_code"::string -> "gr_expr_2")
                    group by ("hash_testing"."identification_number"::int, "hash_testing"."product_code"::string) output: ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::bool -> "product_units", "hash_testing"."sys_op"::int -> "sys_op", "hash_testing"."bucket_id"::int -> "bucket_id")
                        scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby_less_cols_in_proj() {
    // check case when we specify less columns than in groupby clause
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        GROUP BY "identification_number", "product_units"
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "identification_number")
        group by ("gr_expr_1"::int, "gr_expr_2"::bool) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::bool -> "gr_expr_2")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("hash_testing"."identification_number"::int -> "gr_expr_1", "hash_testing"."product_units"::bool -> "gr_expr_2")
                    group by ("hash_testing"."identification_number"::int, "hash_testing"."product_units"::bool) output: ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::bool -> "product_units", "hash_testing"."sys_op"::int -> "sys_op", "hash_testing"."bucket_id"::int -> "bucket_id")
                        scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby_union_1() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        GROUP BY "identification_number"
        UNION ALL
        SELECT "identification_number" FROM "hash_testing""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    union all
        motion [policy: segment([ref("identification_number")]), program: ReshardIfNeeded]
            projection ("gr_expr_1"::int -> "identification_number")
                group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1")
                    motion [policy: full, program: ReshardIfNeeded]
                        projection ("hash_testing"."identification_number"::int -> "gr_expr_1")
                            group by ("hash_testing"."identification_number"::int) output: ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::bool -> "product_units", "hash_testing"."sys_op"::int -> "sys_op", "hash_testing"."bucket_id"::int -> "bucket_id")
                                scan "hash_testing"
        projection ("hash_testing"."identification_number"::int -> "identification_number")
            scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby_union_2() {
    let input = r#"SELECT "identification_number" FROM "hash_testing" UNION ALL
        SELECT * FROM (SELECT "identification_number" FROM "hash_testing"
        GROUP BY "identification_number"
        UNION ALL
        SELECT "identification_number" FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    union all
        projection ("hash_testing"."identification_number"::int -> "identification_number")
            scan "hash_testing"
        projection ("unnamed_subquery"."identification_number"::int -> "identification_number")
            scan "unnamed_subquery"
                union all
                    motion [policy: segment([ref("identification_number")]), program: ReshardIfNeeded]
                        projection ("gr_expr_1"::int -> "identification_number")
                            group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1")
                                motion [policy: full, program: ReshardIfNeeded]
                                    projection ("hash_testing"."identification_number"::int -> "gr_expr_1")
                                        group by ("hash_testing"."identification_number"::int) output: ("hash_testing"."identification_number"::int -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::bool -> "product_units", "hash_testing"."sys_op"::int -> "sys_op", "hash_testing"."bucket_id"::int -> "bucket_id")
                                            scan "hash_testing"
                    projection ("hash_testing"."identification_number"::int -> "identification_number")
                        scan "hash_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby_join_1() {
    // inner select is a kostyl because tables have the col sys_op
    let input = r#"SELECT "product_code", "product_units" FROM (SELECT "product_units", "product_code", "identification_number" FROM "hash_testing") as t2
        INNER JOIN (SELECT "id" from "test_space") as t
        ON t2."identification_number" = t."id"
        group by t2."product_code", t2."product_units"
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::string -> "product_code", "gr_expr_2"::bool -> "product_units")
        group by ("gr_expr_1"::string, "gr_expr_2"::bool) output: ("gr_expr_1"::string -> "gr_expr_1", "gr_expr_2"::bool -> "gr_expr_2")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t2"."product_code"::string -> "gr_expr_1", "t2"."product_units"::bool -> "gr_expr_2")
                    group by ("t2"."product_code"::string, "t2"."product_units"::bool) output: ("t2"."product_units"::bool -> "product_units", "t2"."product_code"::string -> "product_code", "t2"."identification_number"::int -> "identification_number", "t"."id"::int -> "id")
                        join on "t2"."identification_number"::int = "t"."id"::int
                            scan "t2"
                                projection ("hash_testing"."product_units"::bool -> "product_units", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."identification_number"::int -> "identification_number")
                                    scan "hash_testing"
                            motion [policy: full, program: ReshardIfNeeded]
                                scan "t"
                                    projection ("test_space"."id"::int -> "id")
                                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby_bucket_id() {
    let input = r#"SELECT * FROM t GROUP BY a, b, c, d, bucket_id"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
        group by ("t"."a"::int, "t"."b"::int, "t"."c"::int, "t"."d"::int, "t"."bucket_id"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
            scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_join() {
    // test we can have not null and bool kind of condition in join
    let input = r#"SELECT "product_code", "product_units" FROM (SELECT "product_units", "product_code", "identification_number" FROM "hash_testing") as t2
        INNER JOIN (SELECT "id" from "test_space") as t
        ON t2."identification_number" = t."id" and t."id" is not null
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t2"."product_code"::string -> "product_code", "t2"."product_units"::bool -> "product_units")
        join on ("t2"."identification_number"::int = "t"."id"::int) and (not ("t"."id"::int is null))
            scan "t2"
                projection ("hash_testing"."product_units"::bool -> "product_units", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."identification_number"::int -> "identification_number")
                    scan "hash_testing"
            motion [policy: full, program: ReshardIfNeeded]
                scan "t"
                    projection ("test_space"."id"::int -> "id")
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    // here hash_single_testing is sharded by "identification_number", so it is a local join
    let input = r#"SELECT "product_code", "product_units" FROM (SELECT "product_units", "product_code", "identification_number" FROM "hash_single_testing") as t1
        INNER JOIN (SELECT "id" from "test_space") as t2
        ON t1."identification_number" = t2."id" and not t2."id" is null
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."product_code"::string -> "product_code", "t1"."product_units"::bool -> "product_units")
        join on ("t1"."identification_number"::int = "t2"."id"::int) and (not ("t2"."id"::int is null))
            scan "t1"
                projection ("hash_single_testing"."product_units"::bool -> "product_units", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."identification_number"::int -> "identification_number")
                    scan "hash_single_testing"
            scan "t2"
                projection ("test_space"."id"::int -> "id")
                    scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    // check we have no error, in case one of the join children has Distribution::Single
    let input = r#"SELECT "product_code", "product_units" FROM (SELECT "product_units", "product_code", "identification_number" FROM "hash_single_testing") as t1
        INNER JOIN (SELECT sum("id") as "id" from "test_space") as t2
        ON t1."identification_number" = t2."id" and t2."id" is not null
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."product_code"::string -> "product_code", "t1"."product_units"::bool -> "product_units")
        join on ("t1"."identification_number"::int = "t2"."id"::decimal) and (not ("t2"."id"::decimal is null))
            scan "t1"
                projection ("hash_single_testing"."product_units"::bool -> "product_units", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."identification_number"::int -> "identification_number")
                    scan "hash_single_testing"
            motion [policy: full, program: ReshardIfNeeded]
                scan "t2"
                    projection (sum(("sum_1"::decimal))::decimal -> "id")
                        motion [policy: full, program: ReshardIfNeeded]
                            projection (sum(("test_space"."id"::int))::decimal -> "sum_1")
                                scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby_insert() {
    let input = r#"INSERT INTO "t" ("c", "b")
    SELECT "b", "d" FROM "t" group by "b", "d" ON CONFLICT DO FAIL"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: fail
        motion [policy: segment([value(NULL), ref("d")]), program: ReshardIfNeeded]
            projection ("gr_expr_1"::int -> "b", "gr_expr_2"::int -> "d")
                group by ("gr_expr_1"::int, "gr_expr_2"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2")
                    motion [policy: full, program: ReshardIfNeeded]
                        projection ("t"."b"::int -> "gr_expr_1", "t"."d"::int -> "gr_expr_2")
                            group by ("t"."b"::int, "t"."d"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                                scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby_invalid() {
    let input = r#"select "b", "a" from "t" group by "b""#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, &[], metadata).unwrap();
    let res = plan.optimize();

    assert_eq!(true, res.is_err());
}

#[test]
fn front_sql_distinct_invalid() {
    let input = r#"select "b", bucket_id(distinct cast("a" as string)) from "t" group by "b", bucket_id(distinct cast("a" as string))"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, &[], metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("DISTINCT modifier is allowed only for aggregate functions")
    );
}

#[test]
fn front_sql_aggregates() {
    let input = r#"SELECT "b", count("a") + count("b") FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "b", sum(("count_1"::int))::int + sum(("count_2"::int))::int -> "col_1")
        group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "count_1"::int -> "count_1", "count_2"::int -> "count_2")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."b"::int -> "gr_expr_1", count(("t"."a"::int))::int -> "count_1", count(("t"."b"::int))::int -> "count_2")
                    group by ("t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_distinct_asterisk() {
    let input = r#"select distinct * from (select "id" from "test_space_hist")
        join (select "id" from "test_space") on true"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "id", "gr_expr_2"::int -> "id")
        group by ("gr_expr_1"::int, "gr_expr_2"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("unnamed_subquery"."id"::int -> "gr_expr_1", "unnamed_subquery_1"."id"::int -> "gr_expr_2")
                    group by ("unnamed_subquery"."id"::int, "unnamed_subquery_1"."id"::int) output: ("unnamed_subquery"."id"::int -> "id", "unnamed_subquery_1"."id"::int -> "id")
                        join on true::bool
                            scan "unnamed_subquery"
                                projection ("test_space_hist"."id"::int -> "id")
                                    scan "test_space_hist"
                            motion [policy: full, program: ReshardIfNeeded]
                                scan "unnamed_subquery_1"
                                    projection ("test_space"."id"::int -> "id")
                                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_avg_aggregate() {
    let input = r#"SELECT avg("b"), avg(distinct "b"), avg("b") * avg("b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(("avg_1"::decimal::double))::decimal / sum(("avg_2"::decimal::double))::decimal -> "col_1", avg(distinct ("gr_expr_1"::decimal::double))::decimal -> "col_2", (sum(("avg_1"::decimal::double))::decimal / sum(("avg_2"::decimal::double))::decimal) * (sum(("avg_1"::decimal::double))::decimal / sum(("avg_2"::decimal::double))::decimal) -> "col_3")
        motion [policy: full, program: ReshardIfNeeded]
            projection ("t"."b"::int -> "gr_expr_1", sum(("t"."b"::int))::decimal -> "avg_1", count(("t"."b"::int))::int -> "avg_2")
                group by ("t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_total_aggregate() {
    let input = r#"SELECT total("b"), total(distinct "b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (total(("total_1"::double))::double -> "col_1", total(distinct ("gr_expr_1"::double))::double -> "col_2")
        motion [policy: full, program: ReshardIfNeeded]
            projection ("t"."b"::int -> "gr_expr_1", total(("t"."b"::int))::double -> "total_1")
                group by ("t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_min_aggregate() {
    let input = r#"SELECT min("b"), min(distinct "b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (min(("min_1"::int))::int -> "col_1", min(distinct ("gr_expr_1"::int))::int -> "col_2")
        motion [policy: full, program: ReshardIfNeeded]
            projection ("t"."b"::int -> "gr_expr_1", min(("t"."b"::int))::int -> "min_1")
                group by ("t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_max_aggregate() {
    let input = r#"SELECT max("b"), max(distinct "b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (max(("max_1"::int))::int -> "col_1", max(distinct ("gr_expr_1"::int))::int -> "col_2")
        motion [policy: full, program: ReshardIfNeeded]
            projection ("t"."b"::int -> "gr_expr_1", max(("t"."b"::int))::int -> "max_1")
                group by ("t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_group_concat_aggregate() {
    let input = r#"SELECT group_concat("FIRST_NAME"), group_concat(distinct "FIRST_NAME") FROM "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (group_concat(("group_concat_1"::string))::string -> "col_1", group_concat(distinct ("gr_expr_1"::string))::string -> "col_2")
        motion [policy: full, program: ReshardIfNeeded]
            projection ("test_space"."FIRST_NAME"::string -> "gr_expr_1", group_concat(("test_space"."FIRST_NAME"::string))::string -> "group_concat_1")
                group by ("test_space"."FIRST_NAME"::string) output: ("test_space"."id"::int -> "id", "test_space"."sysFrom"::int -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::int -> "sys_op", "test_space"."bucket_id"::int -> "bucket_id")
                    scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_group_concat_aggregate2() {
    let input = r#"SELECT group_concat("FIRST_NAME", ' '), group_concat(distinct "FIRST_NAME") FROM "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (group_concat(("group_concat_1"::string, ' '::string))::string -> "col_1", group_concat(distinct ("gr_expr_1"::string))::string -> "col_2")
        motion [policy: full, program: ReshardIfNeeded]
            projection ("test_space"."FIRST_NAME"::string -> "gr_expr_1", group_concat(("test_space"."FIRST_NAME"::string, ' '::string))::string -> "group_concat_1")
                group by ("test_space"."FIRST_NAME"::string) output: ("test_space"."id"::int -> "id", "test_space"."sysFrom"::int -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::int -> "sys_op", "test_space"."bucket_id"::int -> "bucket_id")
                    scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_string_agg_alias_to_group_concat() {
    // Test 1
    let input = r#"SELECT string_agg("FIRST_NAME", ',') FROM "test_space""#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (group_concat(("group_concat_1"::string, ','::string))::string -> "col_1")
        motion [policy: full, program: ReshardIfNeeded]
            projection (group_concat(("test_space"."FIRST_NAME"::string, ','::string))::string -> "group_concat_1")
                scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    // Test 2
    let input = r#"SELECT "id", string_agg("FIRST_NAME", ',') FROM "test_space" GROUP BY "id""#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "id", group_concat(("group_concat_1"::string, ','::string))::string -> "col_1")
        group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "group_concat_1"::string -> "group_concat_1")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("test_space"."id"::int -> "gr_expr_1", group_concat(("test_space"."FIRST_NAME"::string, ','::string))::string -> "group_concat_1")
                    group by ("test_space"."id"::int) output: ("test_space"."id"::int -> "id", "test_space"."sysFrom"::int -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::int -> "sys_op", "test_space"."bucket_id"::int -> "bucket_id")
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_count_asterisk1() {
    let input = r#"SELECT count(*), count(*) FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(("count_1"::int))::int -> "col_1", sum(("count_1"::int))::int -> "col_2")
        motion [policy: full, program: ReshardIfNeeded]
            projection (count((*::int))::int -> "count_1")
                scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_count_asterisk2() {
    let input = r#"SELECT cOuNt(*), "b" FROM "t" group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(("count_1"::int))::int -> "col_1", "gr_expr_1"::int -> "b")
        group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "count_1"::int -> "count_1")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."b"::int -> "gr_expr_1", count((*::int))::int -> "count_1")
                    group by ("t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_invalid_count_asterisk1() {
    let input = r#"SELECT sum(*) FROM "t" group by "b""#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, &[], metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("\"*\" is allowed only inside \"count\" aggregate function.")
    );
}

#[test]
fn front_sql_aggregates_with_subexpressions() {
    let input = r#"SELECT "b", count("a" * "b" + 1), count(trim("a"::text)) FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "b", sum(("count_1"::int))::int -> "col_1", sum(("count_2"::int))::int -> "col_2")
        group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "count_1"::int -> "count_1", "count_2"::int -> "count_2")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."b"::int -> "gr_expr_1", count((("t"."a"::int * "t"."b"::int) + 1::int))::int -> "count_1", count((TRIM("t"."a"::int::string)))::int -> "count_2")
                    group by ("t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_aggregates_with_distinct1() {
    let input = r#"SELECT "b", count(distinct "a"), count(distinct "b") FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "b", count(distinct ("gr_expr_2"::int))::int -> "col_1", count(distinct ("gr_expr_1"::int))::int -> "col_2")
        group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."b"::int -> "gr_expr_1", "t"."a"::int -> "gr_expr_2")
                    group by ("t"."b"::int, "t"."a"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_aggregates_with_distinct2() {
    let input = r#"SELECT "b", sum(distinct "a" + "b" + 3) FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "b", sum(distinct ("gr_expr_2"::decimal))::decimal -> "col_1")
        group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."b"::int -> "gr_expr_1", ("t"."a"::int + "t"."b"::int) + 3::int -> "gr_expr_2")
                    group by ("t"."b"::int, ("t"."a"::int + "t"."b"::int) + 3::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_aggregates_with_distinct3() {
    let input = r#"SELECT sum(distinct "a" + "b" + 3) FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(distinct ("gr_expr_1"::decimal))::decimal -> "col_1")
        motion [policy: full, program: ReshardIfNeeded]
            projection (("t"."a"::int + "t"."b"::int) + 3::int -> "gr_expr_1")
                group by (("t"."a"::int + "t"."b"::int) + 3::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_aggregate_inside_aggregate() {
    let input = r#"select "b", count(sum("a")) from "t" group by "b""#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, &[], metadata)
        .unwrap()
        .optimize()
        .unwrap_err();

    assert_eq!(
        "invalid query: aggregate functions inside aggregate function are not allowed.",
        err.to_string()
    );
}

#[test]
fn front_sql_column_outside_aggregate_no_groupby() {
    let input = r#"select "b", count("a") from "t""#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, &[], metadata)
        .unwrap()
        .optimize()
        .unwrap_err();

    assert_eq!(
        "invalid query: found column reference (\"b\") outside aggregate function",
        err.to_string()
    );
}

#[test]
fn front_sql_option_basic() {
    let input = r#"select * from "t" option(sql_vdbe_opcode_max = 1000, sql_motion_row_max = 10)"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
        scan "t"
    execution options:
        sql_vdbe_opcode_max = 1000
        sql_motion_row_max = 10
    "#);
}

#[test]
fn front_sql_option_with_param() {
    let input = r#"select * from "t" option(sql_vdbe_opcode_max = ?, sql_motion_row_max = ?)"#;

    let plan = sql_to_optimized_ir(input, vec![Value::Integer(1000), Value::Integer(10)]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
        scan "t"
    execution options:
        sql_vdbe_opcode_max = 1000
        sql_motion_row_max = 10
    "#);
}

#[test]
fn front_sql_pg_style_params1() {
    let input = r#"select $1, $2, $1 from "t""#;

    let plan = sql_to_optimized_ir(
        input,
        vec![Value::Integer(1000), Value::String("hi".into())],
    );
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (1000::int -> "col_1", 'hi'::string -> "col_2", 1000::int -> "col_3")
        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_pg_style_params2() {
    let input =
        r#"select $1, $2, $1 from "t" option(sql_vdbe_opcode_max = $1, sql_motion_row_max = $1)"#;

    let plan = sql_to_optimized_ir(
        input,
        vec![Value::Integer(1000), Value::String("hi".into())],
    );
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (1000::int -> "col_1", 'hi'::string -> "col_2", 1000::int -> "col_3")
        scan "t"
    execution options:
        sql_vdbe_opcode_max = 1000
        sql_motion_row_max = 1000
    "#);
}

#[test]
fn front_sql_pg_style_params3() {
    let input = r#"select "a" + $1 from "t"
        where "a" = $1
        group by "a" + $1
        having count("b") > $1
        option(sql_vdbe_opcode_max = $1, sql_motion_row_max = $1)"#;

    let plan = sql_to_optimized_ir(input, vec![Value::Integer(42)]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "col_1")
        having sum(("count_1"::int))::int > 42::int
            group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "count_1"::int -> "count_1")
                motion [policy: full, program: ReshardIfNeeded]
                    projection ("t"."a"::int + 42::int -> "gr_expr_1", count(("t"."b"::int))::int -> "count_1")
                        group by ("t"."a"::int + 42::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                            selection "t"."a"::int = 42::int
                                scan "t"
    execution options:
        sql_vdbe_opcode_max = 42
        sql_motion_row_max = 42
    "#);
}

#[test]
fn front_sql_pg_style_params4() {
    let input = r#"select $1, ? from "t""#;

    let metadata = &RouterConfigurationMock::new();
    let params = [DerivedType::new(Type::Integer)];
    let err = AbstractSyntaxTree::transform_into_plan(input, &params, metadata).unwrap_err();

    assert_eq!(
        "invalid parameters usage. Got $n and ? parameters in one query!",
        err.to_string()
    );
}

#[test]
fn front_sql_pg_style_params5() {
    let input = r#"select $0 from "t""#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, &[], metadata).unwrap_err();

    assert_eq!(
        "invalid query: $n parameters are indexed from 1!",
        err.to_string()
    );
}

#[test]
fn front_sql_pg_style_params6() {
    // https://git.picodata.io/core/picodata/-/issues/1220
    let input = r#"select $1 + $1"#;
    let metadata = &RouterConfigurationMock::new();
    let mut plan = AbstractSyntaxTree::transform_into_plan(input, &[], metadata).unwrap();
    let err = plan.bind_params(vec![], Options::default()).unwrap_err();
    assert_eq!(
        "invalid query: expected 1 values for parameters, got 0",
        err.to_string()
    );
}

#[test]
fn front_sql_pg_style_params7() {
    // https://git.picodata.io/core/picodata/-/issues/1220
    let input = r#"select ((values ((select ((values ($1)))))))"#;
    let metadata = &RouterConfigurationMock::new();
    let params = [DerivedType::new(Type::Integer)];
    let mut plan = AbstractSyntaxTree::transform_into_plan(input, &params, metadata).unwrap();
    let err = plan.bind_params(vec![], Options::default()).unwrap_err();
    assert_eq!(
        "invalid query: expected 1 values for parameters, got 0",
        err.to_string()
    );
}

#[test]
fn front_sql_pg_style_params8() {
    // https://git.picodata.io/core/picodata/-/issues/1220
    let input = r#"select $1 + $1 = $2"#;
    let metadata = &RouterConfigurationMock::new();
    let mut plan = AbstractSyntaxTree::transform_into_plan(input, &[], metadata).unwrap();
    let err = plan
        .bind_params(vec![Value::Integer(1)], Options::default())
        .unwrap_err();
    assert_eq!(
        "invalid query: expected 2 values for parameters, got 1",
        err.to_string()
    );
}

#[test]
fn front_sql_pg_style_params9() {
    // https://git.picodata.io/core/picodata/-/issues/1663
    let input = "select $1, $2, $3 from (select $4) where $5 = (select $6) or exists (select $7)";

    let params = (1..=7).into_iter().map(|x| Value::Integer(x)).collect();
    let plan = sql_to_optimized_ir(input, params);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (1::int -> "col_1", 2::int -> "col_2", 3::int -> "col_3")
        selection (5::int = ROW($1)) or exists ROW($0)
            scan "unnamed_subquery"
                projection (4::int -> "col_1")
    subquery $0:
    scan
                projection (7::int -> "col_1")
    subquery $1:
    scan
                projection (6::int -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_tnt_style_params1() {
    // https://git.picodata.io/core/picodata/-/issues/1663
    let input = "select ?, ?, ? from (select ?) where ? = (select ?) or exists (select ?)";

    let params = (1..=7).into_iter().map(|x| Value::Integer(x)).collect();
    let plan = sql_to_optimized_ir(input, params);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (1::int -> "col_1", 2::int -> "col_2", 3::int -> "col_3")
        selection (5::int = ROW($1)) or exists ROW($0)
            scan "unnamed_subquery"
                projection (4::int -> "col_1")
    subquery $0:
    scan
                projection (7::int -> "col_1")
    subquery $1:
    scan
                projection (6::int -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_tnt_style_params2() {
    // https://git.picodata.io/core/picodata/-/issues/1460
    let input = "select ? between 1 and 2";

    let plan = sql_to_optimized_ir(input, vec![Value::Integer(1)]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ((1::int >= 1::int) and (1::int <= 2::int) -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_option_defaults() {
    let input = r#"select * from "t" where "a" = ? and "b" = ?"#;

    let plan = sql_to_optimized_ir(input, vec![Value::Integer(1000), Value::Integer(10)]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
        selection ("t"."a"::int = 1000::int) and ("t"."b"::int = 10::int)
            scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_column_outside_aggregate() {
    let input = r#"select "b", "a", count("a") from "t" group by "b""#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, &[], metadata)
        .unwrap()
        .optimize()
        .unwrap_err();

    assert_eq!(
        "invalid query: column \"a\" is not found in grouping expressions!",
        err.to_string()
    );
}

#[test]
fn front_sql_aggregate_without_groupby() {
    let input = r#"SELECT sum("a" * "b" + 1) FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(("sum_1"::decimal))::decimal -> "col_1")
        motion [policy: full, program: ReshardIfNeeded]
            projection (sum((("t"."a"::int * "t"."b"::int) + 1::int))::decimal -> "sum_1")
                scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_aggregate_without_groupby2() {
    let input = r#"SELECT * FROM (SELECT count("id") FROM "test_space") as "t1""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."col_1"::int -> "col_1")
        scan "t1"
            projection (sum(("count_1"::int))::int -> "col_1")
                motion [policy: full, program: ReshardIfNeeded]
                    projection (count(("test_space"."id"::int))::int -> "count_1")
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_aggregate_on_aggregate() {
    let input = r#"SELECT max(c) FROM (SELECT count("id") as c FROM "test_space") as "t1""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (max(("t1"."c"::int))::int -> "col_1")
        scan "t1"
            projection (sum(("count_1"::int))::int -> "c")
                motion [policy: full, program: ReshardIfNeeded]
                    projection (count(("test_space"."id"::int))::int -> "count_1")
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_union_single_left() {
    let input = r#"
        SELECT "a" FROM "t"
        UNION ALL
        SELECT sum("a") FROM "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    union all
        projection ("t"."a"::int -> "a")
            scan "t"
        motion [policy: segment([ref("col_1")]), program: ReshardIfNeeded]
            projection (sum(("sum_1"::decimal))::decimal -> "col_1")
                motion [policy: full, program: ReshardIfNeeded]
                    projection (sum(("t"."a"::int))::decimal -> "sum_1")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_union_single_right() {
    let input = r#"
        SELECT sum("a") FROM "t"
        UNION ALL
        SELECT "a" FROM "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    union all
        motion [policy: segment([ref("col_1")]), program: ReshardIfNeeded]
            projection (sum(("sum_1"::decimal))::decimal -> "col_1")
                motion [policy: full, program: ReshardIfNeeded]
                    projection (sum(("t"."a"::int))::decimal -> "sum_1")
                        scan "t"
        projection ("t"."a"::int -> "a")
            scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_union_single_both() {
    let input = r#"
        SELECT sum("a") FROM "t"
        UNION ALL
        SELECT sum("a") FROM "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    union all
        projection (sum(("sum_1"::decimal))::decimal -> "col_1")
            motion [policy: full, program: ReshardIfNeeded]
                projection (sum(("t"."a"::int))::decimal -> "sum_1")
                    scan "t"
        projection (sum(("sum_1"::decimal))::decimal -> "col_1")
            motion [policy: full, program: ReshardIfNeeded]
                projection (sum(("t"."a"::int))::decimal -> "sum_1")
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_insert_single() {
    let input = r#"INSERT INTO "t" ("c", "b") SELECT sum("b"), count("d") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: fail
        motion [policy: segment([value(NULL), ref("col_2")]), program: ReshardIfNeeded]
            projection (sum(("sum_1"::decimal))::decimal -> "col_1", sum(("count_2"::int))::int -> "col_2")
                motion [policy: full, program: ReshardIfNeeded]
                    projection (sum(("t"."b"::int))::decimal -> "sum_1", count(("t"."d"::int))::int -> "count_2")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_except_single_right() {
    let input = r#"SELECT "a", "b" from "t"
        EXCEPT
        SELECT sum("a"), count("b") from "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        projection ("t"."a"::int -> "a", "t"."b"::int -> "b")
            scan "t"
        motion [policy: segment([ref("col_1"), ref("col_2")]), program: ReshardIfNeeded]
            projection (sum(("sum_1"::decimal))::decimal -> "col_1", sum(("count_2"::int))::int -> "col_2")
                motion [policy: full, program: ReshardIfNeeded]
                    projection (count(("t"."b"::int))::int -> "count_2", sum(("t"."a"::int))::decimal -> "sum_1")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    let input = r#"SELECT "b", "a" from "t"
        EXCEPT
        SELECT sum("a"), count("b") from "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        projection ("t"."b"::int -> "b", "t"."a"::int -> "a")
            scan "t"
        motion [policy: segment([ref("col_2"), ref("col_1")]), program: ReshardIfNeeded]
            projection (sum(("sum_1"::decimal))::decimal -> "col_1", sum(("count_2"::int))::int -> "col_2")
                motion [policy: full, program: ReshardIfNeeded]
                    projection (count(("t"."b"::int))::int -> "count_2", sum(("t"."a"::int))::decimal -> "sum_1")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_except_single_left() {
    let input = r#"SELECT sum("a"), count("b") from "t"
        EXCEPT
        SELECT "a", "b" from "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        motion [policy: segment([ref("col_1"), ref("col_2")]), program: ReshardIfNeeded]
            projection (sum(("sum_1"::decimal))::decimal -> "col_1", sum(("count_2"::int))::int -> "col_2")
                motion [policy: full, program: ReshardIfNeeded]
                    projection (count(("t"."b"::int))::int -> "count_2", sum(("t"."a"::int))::decimal -> "sum_1")
                        scan "t"
        projection ("t"."a"::int -> "a", "t"."b"::int -> "b")
            scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_except_single_both() {
    let input = r#"SELECT sum("a"), count("b") from "t"
        EXCEPT
        SELECT sum("a"), sum("b") from "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    except
        motion [policy: segment([ref("col_1")]), program: ReshardIfNeeded]
            projection (sum(("sum_1"::decimal))::decimal -> "col_1", sum(("count_2"::int))::int -> "col_2")
                motion [policy: full, program: ReshardIfNeeded]
                    projection (count(("t"."b"::int))::int -> "count_2", sum(("t"."a"::int))::decimal -> "sum_1")
                        scan "t"
        motion [policy: segment([ref("col_1")]), program: ReshardIfNeeded]
            projection (sum(("sum_1"::decimal))::decimal -> "col_1", sum(("sum_2"::decimal))::decimal -> "col_2")
                motion [policy: full, program: ReshardIfNeeded]
                    projection (sum(("t"."b"::int))::decimal -> "sum_2", sum(("t"."a"::int))::decimal -> "sum_1")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby_expression() {
    let input = r#"SELECT "a"+"b" FROM "t"
        group by "a"+"b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "col_1")
        group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."a"::int + "t"."b"::int -> "gr_expr_1")
                    group by ("t"."a"::int + "t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby_expression2() {
    let input = r#"SELECT ("a"+"b") + count("a") FROM "t"
        group by ("a"+"b")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int + sum(("count_1"::int))::int -> "col_1")
        group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "count_1"::int -> "count_1")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."a"::int + "t"."b"::int -> "gr_expr_1", count(("t"."a"::int))::int -> "count_1")
                    group by ("t"."a"::int + "t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby_expression3() {
    let input = r#"SELECT "a"+"b", ("c"*"d")*sum("c"*"d")/count("a"*"b") FROM "t"
        group by "a"+"b", "a"+"b", ("c"*"d")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "col_1", ("gr_expr_2"::int * sum(("sum_1"::decimal))::decimal) / sum(("count_2"::int))::int -> "col_2")
        group by ("gr_expr_1"::int, "gr_expr_2"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2", "sum_1"::decimal -> "sum_1", "count_2"::int -> "count_2")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."a"::int + "t"."b"::int -> "gr_expr_1", "t"."c"::int * "t"."d"::int -> "gr_expr_2", sum(("t"."c"::int * "t"."d"::int))::decimal -> "sum_1", count(("t"."a"::int * "t"."b"::int))::int -> "count_2")
                    group by ("t"."a"::int + "t"."b"::int, "t"."c"::int * "t"."d"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby_expression4() {
    let input = r#"SELECT "a"+"b", "a" FROM "t"
        group by "a"+"b", "a""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "col_1", "gr_expr_2"::int -> "a")
        group by ("gr_expr_1"::int, "gr_expr_2"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."a"::int + "t"."b"::int -> "gr_expr_1", "t"."a"::int -> "gr_expr_2")
                    group by ("t"."a"::int + "t"."b"::int, "t"."a"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_groupby_with_aggregates() {
    let input = r#"
        select * from (select "a", "b", sum("c") as "c" from "t" group by "a", "b") as t1
        join (select "g", "e", sum("f") as "f" from "t2" group by "g", "e") as t2
        on (t1."a", t2."g") = (t2."e", t1."b")"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."a"::int -> "a", "t1"."b"::int -> "b", "t1"."c"::decimal -> "c", "t2"."g"::int -> "g", "t2"."e"::int -> "e", "t2"."f"::decimal -> "f")
        join on ROW("t1"."a"::int, "t1"."b"::int) = ROW("t2"."e"::int, "t2"."g"::int)
            scan "t1"
                projection ("gr_expr_1"::int -> "a", "gr_expr_2"::int -> "b", sum(("sum_1"::decimal))::decimal -> "c")
                    group by ("gr_expr_1"::int, "gr_expr_2"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2", "sum_1"::decimal -> "sum_1")
                        motion [policy: full, program: ReshardIfNeeded]
                            projection ("t"."a"::int -> "gr_expr_1", "t"."b"::int -> "gr_expr_2", sum(("t"."c"::int))::decimal -> "sum_1")
                                group by ("t"."a"::int, "t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                                    scan "t"
            scan "t2"
                projection ("gr_expr_1"::int -> "g", "gr_expr_2"::int -> "e", sum(("sum_1"::decimal))::decimal -> "f")
                    group by ("gr_expr_1"::int, "gr_expr_2"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2", "sum_1"::decimal -> "sum_1")
                        motion [policy: full, program: ReshardIfNeeded]
                            projection ("t2"."g"::int -> "gr_expr_1", "t2"."e"::int -> "gr_expr_2", sum(("t2"."f"::int))::decimal -> "sum_1")
                                group by ("t2"."g"::int, "t2"."e"::int) output: ("t2"."e"::int -> "e", "t2"."f"::int -> "f", "t2"."g"::int -> "g", "t2"."h"::int -> "h", "t2"."bucket_id"::int -> "bucket_id")
                                    scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_left_join() {
    let input = r#"SELECT * from (select "a" as a from "t") as o
        left outer join (select "b" as c, "d" as d from "t") as i
        on o.a = i.c
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("o"."a"::int -> "a", "i"."c"::int -> "c", "i"."d"::int -> "d")
        left join on "o"."a"::int = "i"."c"::int
            scan "o"
                projection ("t"."a"::int -> "a")
                    scan "t"
            motion [policy: full, program: ReshardIfNeeded]
                scan "i"
                    projection ("t"."b"::int -> "c", "t"."d"::int -> "d")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_left_join_single_left() {
    let input = r#"
        select * from (select sum("id") / 3 as a from "test_space") as t1
        left outer join (select "id" as b from "test_space") as t2
        on t1.a = t2.b
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."a"::decimal -> "a", "t2"."b"::int -> "b")
        left join on "t1"."a"::decimal = "t2"."b"::int
            motion [policy: segment([ref("a")]), program: ReshardIfNeeded]
                scan "t1"
                    projection (sum(("sum_1"::decimal))::decimal / 3::int -> "a")
                        motion [policy: full, program: ReshardIfNeeded]
                            projection (sum(("test_space"."id"::int))::decimal -> "sum_1")
                                scan "test_space"
            motion [policy: full, program: ReshardIfNeeded]
                scan "t2"
                    projection ("test_space"."id"::int -> "b")
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_left_join_single_left2() {
    let input = r#"
        select * from (select sum("id") / 3 as a from "test_space") as t1
        left join (select "id" as b from "test_space") as t2
        on t1.a + 3 != t2.b
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // full motion should be under outer child
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."a"::decimal -> "a", "t2"."b"::int -> "b")
        left join on ("t1"."a"::decimal + 3::int) <> "t2"."b"::int
            motion [policy: segment([ref("a")]), program: ReshardIfNeeded]
                scan "t1"
                    projection (sum(("sum_1"::decimal))::decimal / 3::int -> "a")
                        motion [policy: full, program: ReshardIfNeeded]
                            projection (sum(("test_space"."id"::int))::decimal -> "sum_1")
                                scan "test_space"
            motion [policy: full, program: ReshardIfNeeded]
                scan "t2"
                    projection ("test_space"."id"::int -> "b")
                        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_left_join_single_both() {
    let input = r#"
        select * from (select sum("id") / 3 as a from "test_space") as t1
        left join (select count("id") as b from "test_space") as t2
        on t1.a != t2.b
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // full motion should be under outer child
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t1"."a"::decimal -> "a", "t2"."b"::int -> "b")
        left join on "t1"."a"::decimal <> "t2"."b"::int
            scan "t1"
                projection (sum(("sum_1"::decimal))::decimal / 3::int -> "a")
                    motion [policy: full, program: ReshardIfNeeded]
                        projection (sum(("test_space"."id"::int))::decimal -> "sum_1")
                            scan "test_space"
            scan "t2"
                projection (sum(("count_1"::int))::int -> "b")
                    motion [policy: full, program: ReshardIfNeeded]
                        projection (count(("test_space"."id"::int))::int -> "count_1")
                            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_nested_subqueries() {
    let input = r#"SELECT "a" FROM "t"
        WHERE "a" in (SELECT "a"::int FROM "t1" WHERE "a" in (SELECT "b"::text FROM "t1"))"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."a"::int -> "a")
        selection "t"."a"::int in ROW($1)
            scan "t"
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
                                scan
                                    projection ("t1"."b"::int::string -> "col_1")
                                        scan "t1"
    subquery $1:
    motion [policy: full, program: ReshardIfNeeded]
                scan
                    projection ("t1"."a"::string::int -> "col_1")
                        selection "t1"."a"::string in ROW($0)
                            scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_having1() {
    let input = r#"SELECT "a", sum("b") FROM "t"
        group by "a"
        having "a" > 1 and sum(distinct "b") > 1
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    println!("Formatted arena: {}", plan.formatted_arena().unwrap());

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "a", sum(("sum_1"::decimal))::decimal -> "col_1")
        having ("gr_expr_1"::int > 1::int) and (sum(distinct ("gr_expr_2"::decimal))::decimal > 1::int)
            group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2", "sum_1"::decimal -> "sum_1")
                motion [policy: full, program: ReshardIfNeeded]
                    projection ("t"."a"::int -> "gr_expr_1", "t"."b"::int -> "gr_expr_2", sum(("t"."b"::int))::decimal -> "sum_1")
                        group by ("t"."a"::int, "t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                            scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_having2() {
    let input = r#"SELECT sum("a") * count(distinct "b"), sum("a") FROM "t"
        having sum(distinct "b") > 1 and sum("a") > 1
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(("sum_1"::decimal))::decimal * count(distinct ("gr_expr_1"::int))::int -> "col_1", sum(("sum_1"::decimal))::decimal -> "col_2")
        having (sum(distinct ("gr_expr_1"::decimal))::decimal > 1::int) and (sum(("sum_1"::decimal))::decimal > 1::int)
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."b"::int -> "gr_expr_1", sum(("t"."a"::int))::decimal -> "sum_1")
                    group by ("t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_having3() {
    let input = r#"SELECT sum("a") FROM "t"
        having sum("a") > 1
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(("sum_1"::decimal))::decimal -> "col_1")
        having sum(("sum_1"::decimal))::decimal > 1::int
            motion [policy: full, program: ReshardIfNeeded]
                projection (sum(("t"."a"::int))::decimal -> "sum_1")
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_having4() {
    let input = r#"SELECT sum("a") FROM "t"
        having sum("a") > 1 and "b" > 1
    "#;

    let err = sql_to_optimized_ir_add_motions_err(input);

    assert_eq!(
        true,
        err.to_string()
            .contains("HAVING argument must appear in the GROUP BY clause or be used in an aggregate function")
    );
}

#[test]
fn front_sql_having_with_sq() {
    let input = r#"
        SELECT "sysFrom", sum(distinct "id") as "sum", count(distinct "id") as "count" from "test_space"
        group by "sysFrom"
        having (select "sysFrom" from "test_space" where "sysFrom" = 2) > count(distinct "id")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "sysFrom", sum(distinct ("gr_expr_2"::decimal))::decimal -> "sum", count(distinct ("gr_expr_2"::int))::int -> "count")
        having ROW($0) > count(distinct ("gr_expr_2"::int))::int
            group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2")
                motion [policy: full, program: ReshardIfNeeded]
                    projection ("test_space"."sysFrom"::int -> "gr_expr_1", "test_space"."id"::int -> "gr_expr_2")
                        group by ("test_space"."sysFrom"::int, "test_space"."id"::int) output: ("test_space"."id"::int -> "id", "test_space"."sysFrom"::int -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::int -> "sys_op", "test_space"."bucket_id"::int -> "bucket_id")
                            scan "test_space"
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
                scan
                    projection ("test_space"."sysFrom"::int -> "sysFrom")
                        selection "test_space"."sysFrom"::int = 2::int
                            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_unmatched_column_in_having() {
    let input = r#"SELECT sum("a"), "a" FROM "t"
        group by "a"
        having sum("a") > 1 and "a" > 1 or "c" = 1
    "#;

    let err = sql_to_optimized_ir_add_motions_err(input);

    assert_eq!(
        true,
        err.to_string()
            .contains("column \"c\" is not found in grouping expressions!")
    );
}

#[test]
fn front_sql_having_with_sq_segment_motion() {
    // check subquery has Full Motion on groupby columns
    let input = r#"
        SELECT "sysFrom", "sys_op", sum(distinct "id") as "sum", count(distinct "id") as "count" from "test_space"
        group by "sysFrom", "sys_op"
        having ("sysFrom", "sys_op") in (select "a", "d" from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "sysFrom", "gr_expr_2"::int -> "sys_op", sum(distinct ("gr_expr_3"::decimal))::decimal -> "sum", count(distinct ("gr_expr_3"::int))::int -> "count")
        having ROW("gr_expr_1"::int, "gr_expr_2"::int) in ROW($0, $0)
            group by ("gr_expr_1"::int, "gr_expr_2"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2", "gr_expr_3"::int -> "gr_expr_3")
                motion [policy: full, program: ReshardIfNeeded]
                    projection ("test_space"."sysFrom"::int -> "gr_expr_1", "test_space"."sys_op"::int -> "gr_expr_2", "test_space"."id"::int -> "gr_expr_3")
                        group by ("test_space"."sysFrom"::int, "test_space"."sys_op"::int, "test_space"."id"::int) output: ("test_space"."id"::int -> "id", "test_space"."sysFrom"::int -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::int -> "sys_op", "test_space"."bucket_id"::int -> "bucket_id")
                            scan "test_space"
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
                scan
                    projection ("t"."a"::int -> "a", "t"."d"::int -> "d")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_having_with_sq_segment_local_motion() {
    // Check subquery has Motion::Full
    let input = r#"
        SELECT "sysFrom", "sys_op", sum(distinct "id") as "sum", count(distinct "id") as "count" from "test_space"
        group by "sysFrom", "sys_op"
        having ("sysFrom", "sys_op") in (select "a", "b" from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "sysFrom", "gr_expr_2"::int -> "sys_op", sum(distinct ("gr_expr_3"::decimal))::decimal -> "sum", count(distinct ("gr_expr_3"::int))::int -> "count")
        having ROW("gr_expr_1"::int, "gr_expr_2"::int) in ROW($0, $0)
            group by ("gr_expr_1"::int, "gr_expr_2"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2", "gr_expr_3"::int -> "gr_expr_3")
                motion [policy: full, program: ReshardIfNeeded]
                    projection ("test_space"."sysFrom"::int -> "gr_expr_1", "test_space"."sys_op"::int -> "gr_expr_2", "test_space"."id"::int -> "gr_expr_3")
                        group by ("test_space"."sysFrom"::int, "test_space"."sys_op"::int, "test_space"."id"::int) output: ("test_space"."id"::int -> "id", "test_space"."sysFrom"::int -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::int -> "sys_op", "test_space"."bucket_id"::int -> "bucket_id")
                            scan "test_space"
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
                scan
                    projection ("t"."a"::int -> "a", "t"."b"::int -> "b")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_unique_local_aggregates() {
    // make sure we don't compute extra aggregates at local stage
    let input = r#"SELECT sum("a"), count("a"), sum("a") + count("a") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // here we must compute only two aggregates at local stage: sum(a), count(a)
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(("sum_1"::decimal))::decimal -> "col_1", sum(("count_2"::int))::int -> "col_2", sum(("sum_1"::decimal))::decimal + sum(("count_2"::int))::int -> "col_3")
        motion [policy: full, program: ReshardIfNeeded]
            projection (count(("t"."a"::int))::int -> "count_2", sum(("t"."a"::int))::decimal -> "sum_1")
                scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_unique_local_groupings() {
    // make sure we don't compute extra group by columns at local stage
    let input = r#"SELECT sum(distinct "a"), count(distinct "a"), count(distinct "b") FROM "t"
        group by "b"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // here we must compute only two groupby columns at local stage: a, b
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(distinct ("gr_expr_2"::decimal))::decimal -> "col_1", count(distinct ("gr_expr_2"::int))::int -> "col_2", count(distinct ("gr_expr_1"::int))::int -> "col_3")
        group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."b"::int -> "gr_expr_1", "t"."a"::int -> "gr_expr_2")
                    group by ("t"."b"::int, "t"."a"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_join_table_with_bucket_id_as_first_col() {
    // here we are joining t3 who has bucket_id as its first column,
    // check that we correctly handle references in join condition,
    // after inserting SQ with Projection under outer child
    let input = r#"
SELECT * FROM
    "t5" "t3"
INNER JOIN
    (SELECT * FROM "hash_single_testing" INNER JOIN (SELECT "id" FROM "test_space") as "ts"
     ON "hash_single_testing"."identification_number" = "ts"."id") as "ij"
ON "t3"."a" = "ij"."id"
"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t3"."a"::int -> "a", "t3"."b"::int -> "b", "ij"."identification_number"::int -> "identification_number", "ij"."product_code"::string -> "product_code", "ij"."product_units"::bool -> "product_units", "ij"."sys_op"::int -> "sys_op", "ij"."id"::int -> "id")
        join on "t3"."a"::int = "ij"."id"::int
            scan "t5" -> "t3"
            scan "ij"
                projection ("hash_single_testing"."identification_number"::int -> "identification_number", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."product_units"::bool -> "product_units", "hash_single_testing"."sys_op"::int -> "sys_op", "ts"."id"::int -> "id")
                    join on "hash_single_testing"."identification_number"::int = "ts"."id"::int
                        scan "hash_single_testing"
                        scan "ts"
                            projection ("test_space"."id"::int -> "id")
                                scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_select_distinct() {
    // make sure we don't compute extra group by columns at local stage
    let input = r#"SELECT distinct "a", "a" + "b" FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // here we must compute only two groupby columns at local stage: a, b
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "a", "gr_expr_2"::int -> "col_1")
        group by ("gr_expr_1"::int, "gr_expr_2"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."a"::int -> "gr_expr_1", "t"."a"::int + "t"."b"::int -> "gr_expr_2")
                    group by ("t"."a"::int, "t"."a"::int + "t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_select_distinct_asterisk() {
    let input = r#"SELECT distinct * FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("gr_expr_1"::int -> "a", "gr_expr_2"::int -> "b", "gr_expr_3"::int -> "c", "gr_expr_4"::int -> "d")
        group by ("gr_expr_1"::int, "gr_expr_2"::int, "gr_expr_3"::int, "gr_expr_4"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "gr_expr_2"::int -> "gr_expr_2", "gr_expr_3"::int -> "gr_expr_3", "gr_expr_4"::int -> "gr_expr_4")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."a"::int -> "gr_expr_1", "t"."b"::int -> "gr_expr_2", "t"."c"::int -> "gr_expr_3", "t"."d"::int -> "gr_expr_4")
                    group by ("t"."a"::int, "t"."b"::int, "t"."c"::int, "t"."d"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_invalid_select_distinct() {
    let input = r#"SELECT distinct "a" FROM "t"
        group by "b"
    "#;

    let err = sql_to_optimized_ir_add_motions_err(input);

    assert_eq!(
        true,
        err.to_string()
            .contains("column \"a\" is not found in grouping expressions!")
    );
}

#[test]
fn front_sql_select_distinct_with_aggr() {
    let input = r#"SELECT distinct sum("a"), "b" FROM "t"
    group by "b"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(("sum_1"::decimal))::decimal -> "col_1", "gr_expr_1"::int -> "b")
        group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "sum_1"::decimal -> "sum_1")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("t"."b"::int -> "gr_expr_1", sum(("t"."a"::int))::decimal -> "sum_1")
                    group by ("t"."b"::int) output: ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_select_distinct_with_aggr2() {
    let input = r#"SELECT distinct sum("a") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(("sum_1"::decimal))::decimal -> "col_1")
        motion [policy: full, program: ReshardIfNeeded]
            projection (sum(("t"."a"::int))::decimal -> "sum_1")
                scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_insert_on_conflict() {
    let mut input = r#"insert into "t" values (1, 1, 1, 1) on conflict do nothing"#;

    let mut plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: nothing
        motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")]), program: ReshardIfNeeded]
            values
                value row (data=ROW(1::int, 1::int, 1::int, 1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    input = r#"insert into "t" values (1, 1, 1, 1) on conflict do replace"#;
    plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: replace
        motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")]), program: ReshardIfNeeded]
            values
                value row (data=ROW(1::int, 1::int, 1::int, 1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_insert_1() {
    let input = r#"insert into "t" ("b") select "a" from "t"
        where "a" = 1 and "b" = 2 or "a" = 2 and "b" = 3"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: fail
        motion [policy: segment([value(NULL), ref("a")]), program: ReshardIfNeeded]
            projection ("t"."a"::int -> "a")
                selection (("t"."a"::int = 1::int) and ("t"."b"::int = 2::int)) or (("t"."a"::int = 2::int) and ("t"."b"::int = 3::int))
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_insert_2() {
    let input = r#"insert into "t" ("a", "b") select "a", "b" from "t"
        where "a" = 1 and "b" = 2"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: fail
        motion [policy: local segment([ref("a"), ref("b")]), program: ReshardIfNeeded]
            projection ("t"."a"::int -> "a", "t"."b"::int -> "b")
                selection ("t"."a"::int = 1::int) and ("t"."b"::int = 2::int)
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_insert_3() {
    // check different column order leads to Segment motion
    let input = r#"insert into "t" ("b", "a") select "a", "b" from "t"
        where "a" = 1 and "b" = 2 or "a" = 3 and "b" = 4"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: fail
        motion [policy: segment([ref("b"), ref("a")]), program: ReshardIfNeeded]
            projection ("t"."a"::int -> "a", "t"."b"::int -> "b")
                selection (("t"."a"::int = 1::int) and ("t"."b"::int = 2::int)) or (("t"."a"::int = 3::int) and ("t"."b"::int = 4::int))
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_insert_4() {
    let input = r#"insert into "t" ("b", "a") select "b", "a" from "t"
        where "a" = 1 and "b" = 2"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: fail
        motion [policy: local segment([ref("a"), ref("b")]), program: ReshardIfNeeded]
            projection ("t"."b"::int -> "b", "t"."a"::int -> "a")
                selection ("t"."a"::int = 1::int) and ("t"."b"::int = 2::int)
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_insert_5() {
    let input = r#"insert into "t" ("b", "a") select 5, 6 from "t"
        where "a" = 1 and "b" = 2"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: fail
        motion [policy: segment([ref("col_2"), ref("col_1")]), program: ReshardIfNeeded]
            projection (5::int -> "col_1", 6::int -> "col_2")
                selection ("t"."a"::int = 1::int) and ("t"."b"::int = 2::int)
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_insert_6() {
    // The values should be materialized on the router, and
    // then dispatched to storages.
    let input = r#"insert into "t" ("a", "b") values (1, 2), (1, 2), (3, 4)"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: fail
        motion [policy: segment([ref("COLUMN_5"), ref("COLUMN_6")]), program: ReshardIfNeeded]
            values
                value row (data=ROW(1::int, 2::int))
                value row (data=ROW(1::int, 2::int))
                value row (data=ROW(3::int, 4::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_insert_7() {
    // Check system column can't be inserted
    let input = r#"insert into "hash_testing" ("identification_number", "product_code", "bucket_id") values (1, '2', 3)"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, &[], metadata);
    let err = plan.unwrap_err();
    assert_eq!(
        true,
        err.to_string()
            .contains("system column \"bucket_id\" cannot be inserted")
    );
}

#[test]
fn front_sql_insert_8() {
    // Both table have the same columns, but hash_single_testing has different shard key
    let input = r#"insert into "hash_testing" select * from "hash_single_testing""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "hash_testing" on conflict: fail
        motion [policy: segment([ref("identification_number"), ref("product_code")]), program: ReshardIfNeeded]
            projection ("hash_single_testing"."identification_number"::int -> "identification_number", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."product_units"::bool -> "product_units", "hash_single_testing"."sys_op"::int -> "sys_op")
                scan "hash_single_testing"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_insert_9() {
    let input = r#"insert into "t" ("a", "b") values (?, ?)"#;

    let plan = sql_to_optimized_ir(input, vec![Value::from(1), Value::from(2)]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    insert "t" on conflict: fail
        motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")]), program: ReshardIfNeeded]
            values
                value row (data=ROW(1::int, 2::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_insert_duplicate_columns() {
    let input = r#"insert into "t3" ("a", "b", "a") values (1, 2, 3)"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, &[], metadata);
    let err = plan.unwrap_err();
    assert_eq!(
        true,
        err.to_string()
            .contains("duplicated value: column \"a\" specified more than once")
    );
}

#[test]
fn front_sql_update1() {
    let input = r#"update "t" set "a" = 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    update "t"
    "b" = "col_1"
    "d" = "col_3"
    "a" = "col_0"
    "c" = "col_2"
        motion [policy: segment([]), program: [PrimaryKey(1), RearrangeForShardedUpdate(0, 1)]]
            projection (1::int -> "col_0", "t"."b"::int -> "col_1", "t"."c"::int -> "col_2", "t"."d"::int -> "col_3", "t"."a"::int -> "col_4", "t"."b"::int -> "col_5")
                scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_update2() {
    let input = r#"update "t" set "c" = "a" + "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    update "t"
    "c" = "col_0"
        motion [policy: local, program: ReshardIfNeeded]
            projection ("t"."a"::int + "t"."b"::int -> "col_0", "t"."b"::int -> "col_1")
                scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_update3() {
    let input = r#"update "t" set "c" = "a" + "b" where "c" = 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    update "t"
    "c" = "col_0"
        motion [policy: local, program: ReshardIfNeeded]
            projection ("t"."a"::int + "t"."b"::int -> "col_0", "t"."b"::int -> "col_1")
                selection "t"."c"::int = 1::int
                    scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_update4() {
    let input = r#"update "t" set
    "d" = "b1"*2,
    "c" = "b1"*2
    from (select "a" as "a1", "b" as "b1" from "t1")
    where "c" = "b1""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    update "t"
    "d" = "col_0"
    "c" = "col_0"
        motion [policy: local, program: ReshardIfNeeded]
            projection ("unnamed_subquery"."b1"::int * 2::int -> "col_0", "t"."b"::int -> "col_1")
                join on "t"."c"::int = "unnamed_subquery"."b1"::int
                    scan "t"
                    motion [policy: full, program: ReshardIfNeeded]
                        scan "unnamed_subquery"
                            projection ("t1"."a"::string -> "a1", "t1"."b"::int -> "b1")
                                scan "t1"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_update5() {
    let input = r#"update "t3_2" set
    "b" = "id"
    from "test_space"
    where "a" = "id""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    update "t3_2"
    "b" = "col_0"
        motion [policy: local, program: ReshardIfNeeded]
            projection ("test_space"."id"::int -> "col_0", "t3_2"."a"::int -> "col_1")
                join on "t3_2"."a"::int = "test_space"."id"::int
                    scan "t3_2"
                    scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_update6() {
    let input = r#"update "t3" set
    "b" = 2
    where "b" in (select sum("b") as s from "t3")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    update "t3"
    "b" = "col_0"
        motion [policy: local, program: ReshardIfNeeded]
            projection (2::int -> "col_0", "t3"."a"::string -> "col_1")
                selection "t3"."b"::int in ROW($0)
                    scan "t3"
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
                        scan
                            projection (sum(("sum_1"::decimal))::decimal -> "s")
                                motion [policy: full, program: ReshardIfNeeded]
                                    projection (sum(("t3"."b"::int))::decimal -> "sum_1")
                                        scan "t3"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_update7() {
    let input = r#"update t3 set b = $1"#;

    let plan = sql_to_optimized_ir(input, vec![Value::from(1)]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    update "t3"
    "b" = "col_0"
        motion [policy: local, program: ReshardIfNeeded]
            projection (1::int -> "col_0", "t3"."a"::string -> "col_1")
                scan "t3"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_update8() {
    let input = r#"update t3 set b = $1 + $2"#;

    let plan = sql_to_optimized_ir(input, vec![Value::from(1), Value::from(1)]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    update "t3"
    "b" = "col_0"
        motion [policy: local, program: ReshardIfNeeded]
            projection (1::int + 1::int -> "col_0", "t3"."a"::string -> "col_1")
                scan "t3"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_update9() {
    let input = r#"update t3 set b = a"#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, &[], metadata).unwrap_err();

    insta::assert_snapshot!(err.to_string(), @r#"column "b" is of type int, but expression is of type text"#);
}

#[test]
fn front_sql_not_true() {
    let input = r#"SELECT "a" FROM "t" WHERE not true"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("t"."a"::int -> "a")
        selection not true::bool
            scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_not_equal() {
    let input = r#"SELECT * FROM (VALUES (1)) where not true = true"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("unnamed_subquery"."COLUMN_1"::int -> "COLUMN_1")
        selection false::bool
            scan "unnamed_subquery"
                motion [policy: full, program: ReshardIfNeeded]
                    values
                        value row (data=ROW(1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_not_cast() {
    let input = r#"SELECT * FROM (values (1)) where not cast('true' as bool)"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("unnamed_subquery"."COLUMN_1"::int -> "COLUMN_1")
        selection false::bool
            scan "unnamed_subquery"
                motion [policy: full, program: ReshardIfNeeded]
                    values
                        value row (data=ROW(1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn from_sql_not_column() {
    let input = r#"SELECT * FROM (values (true)) where not "COLUMN_1""#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("unnamed_subquery"."COLUMN_1"::bool -> "COLUMN_1")
        selection not "unnamed_subquery"."COLUMN_1"::bool
            scan "unnamed_subquery"
                motion [policy: full, program: ReshardIfNeeded]
                    values
                        value row (data=ROW(true::bool))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_not_or() {
    let input = r#"SELECT * FROM (values (1)) where not true or true"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("unnamed_subquery"."COLUMN_1"::int -> "COLUMN_1")
        selection true::bool
            scan "unnamed_subquery"
                motion [policy: full, program: ReshardIfNeeded]
                    values
                        value row (data=ROW(1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_not_and_with_parentheses() {
    let input = r#"SELECT not (true and false) FROM (values (1))"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (not (true::bool and false::bool) -> "col_1")
        scan "unnamed_subquery"
            motion [policy: full, program: ReshardIfNeeded]
                values
                    value row (data=ROW(1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_not_or_with_parentheses() {
    let input = r#"SELECT * FROM (values (1)) where not (true or true)"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("unnamed_subquery"."COLUMN_1"::int -> "COLUMN_1")
        selection false::bool
            scan "unnamed_subquery"
                motion [policy: full, program: ReshardIfNeeded]
                    values
                        value row (data=ROW(1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_not_exists() {
    let input = r#"select * from (values (1)) where not exists (select * from (values (1)))"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("unnamed_subquery"."COLUMN_1"::int -> "COLUMN_1")
        selection not exists ROW($0)
            scan "unnamed_subquery"
                motion [policy: full, program: ReshardIfNeeded]
                    values
                        value row (data=ROW(1::int))
    subquery $0:
    scan
                projection ("unnamed_subquery_1"."COLUMN_2"::int -> "COLUMN_2")
                    scan "unnamed_subquery_1"
                        motion [policy: full, program: ReshardIfNeeded]
                            values
                                value row (data=ROW(1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_not_in() {
    let input = r#"select * from (values (1)) where 1 not in (select * from (values (1)))"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("unnamed_subquery"."COLUMN_1"::int -> "COLUMN_1")
        selection not (1::int in ROW($0))
            scan "unnamed_subquery"
                motion [policy: full, program: ReshardIfNeeded]
                    values
                        value row (data=ROW(1::int))
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
                scan
                    projection ("unnamed_subquery_1"."COLUMN_2"::int -> "COLUMN_2")
                        scan "unnamed_subquery_1"
                            motion [policy: full, program: ReshardIfNeeded]
                                values
                                    value row (data=ROW(1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_not_complex_query() {
    let input = r#"
            select not (not (cast('true' as bool)) and 1 + (?) != 1)
            from
                (select not "id" <> 2 as "nid" from "test_space") as "ts"
                inner join
                (select not not "id" = 1 as "nnid" from "test_space") as "nts"
                on not "nid" or not false = cast((not not true) as bool)
            where not exists (select * from (values (1)) where not true = (?))
        "#;
    let plan = sql_to_optimized_ir(input, vec![Value::from(1), Value::from(true)]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (not ((not true::bool) and ((1::int + 1::int) <> 1::int)) -> "col_1")
        selection not exists ROW($0)
            join on (not "ts"."nid"::bool) or (false::bool <> (not false::bool)::bool)
                scan "ts"
                    projection (not ("test_space"."id"::int <> 2::int) -> "nid")
                        scan "test_space"
                motion [policy: full, program: ReshardIfNeeded]
                    scan "nts"
                        projection (not (not ("test_space"."id"::int = 1::int)) -> "nnid")
                            scan "test_space"
    subquery $0:
    scan
                projection ("unnamed_subquery"."COLUMN_1"::int -> "COLUMN_1")
                    selection false::bool
                        scan "unnamed_subquery"
                            motion [policy: full, program: ReshardIfNeeded]
                                values
                                    value row (data=ROW(1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_arithmetic_with_parentheses() {
    let input = r#"SELECT (1 + 2) * 3 FROM (values (1))"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ((1::int + 2::int) * 3::int -> "col_1")
        scan "unnamed_subquery"
            motion [policy: full, program: ReshardIfNeeded]
                values
                    value row (data=ROW(1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_to_date() {
    let input = r#"SELECT to_date("COLUMN_1", '%Y/%d/%m') FROM (values ('2010/10/10'))"#;
    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("to_date"(("unnamed_subquery"."COLUMN_1"::string, '%Y/%d/%m'::string))::datetime -> "col_1")
        scan "unnamed_subquery"
            motion [policy: full, program: ReshardIfNeeded]
                values
                    value row (data=ROW('2010/10/10'::string))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_current_date() {
    let input = r#"
    SELECT current_date FROM (values ('2010/10/10'))
    where to_date('2010/10/10', '%Y/%d/%m') < current_Date"#;

    let today = chrono::offset::Local::now();
    let today = today.format("%Y-%m-%d 0:00:00.0 %::z");

    // functions getting current date/time all get transformed to concrete values during the optimization
    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = format!(
        r#"projection ({today}::datetime -> "col_1")
    selection "to_date"(('2010/10/10'::string, '%Y/%d/%m'::string))::datetime < {today}::datetime
        scan "unnamed_subquery"
            motion [policy: full, program: ReshardIfNeeded]
                values
                    value row (data=ROW('2010/10/10'::string))
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
"#
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_check_non_null_columns_specified() {
    let input = r#"insert into "test_space" ("sys_op") values (1)"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, &[], metadata);
    let err = plan.unwrap_err();
    assert_eq!(
        true,
        err.to_string()
            .contains("NonNull column \"id\" must be specified")
    );
}

#[test]
fn non_existent_references_in_values_do_not_panic() {
    // scenario: somebody mixed up " with '
    let input = r#"insert into "test_space" values(1, "nonexistent_reference")"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, &[], metadata);
    let err = plan.unwrap_err();

    assert!(err
        .to_string()
        .contains("Reference \"nonexistent_reference\" met under Values that is unsupported. For string literals use single quotes."));
}

#[test]
fn front_count_no_params() {
    let input = r#"select count() from "test_space""#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, &[], metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("invalid query: Expected one argument for aggregate: \"count\"")
    );
}

#[test]
fn front_mock_set_param_transaction() {
    let queries_to_check = vec![
        r#"set session default_param = default"#,
        r#"set session stringparam = 'value'"#,
        r#"set session identparam to ident"#,
        r#"set local intparam to -3"#,
        r#"set local doubleparam = -42.5"#,
        r#"set doubleparam = -42.5"#,
        r#"set local time zone local"#,
        r#"set time zone -3"#,
        r#"set time zone 'value'"#,
        r#"SET search_path TO my_schema, public;"#,
        r#"SET datestyle TO postgres, dmy;"#,
        r#"SET TIME ZONE 'PST8PDT';"#,
        r#"SET TIME ZONE 'Europe/Rome';"#,
        r#"SET param To list, 'of', 4, valuez;"#,
        r#"set transaction snapshot 'snapshot-string'"#,
        r#"set transaction read write"#,
        r#"set transaction read only"#,
        r#"set transaction deferrable"#,
        r#"set transaction not deferrable"#,
        r#"set transaction isolation level serializable"#,
        r#"set transaction isolation level repeatable read"#,
        r#"set transaction isolation level read commited"#,
        r#"set transaction isolation level read uncommited"#,
        r#"set session characteristics as transaction read only"#,
        r#"set session characteristics as transaction isolation level read commited"#,
    ];

    let metadata = &RouterConfigurationMock::new();
    for query in queries_to_check {
        let plan = AbstractSyntaxTree::transform_into_plan(query, &[], metadata);
        assert!(plan.is_ok())
    }
}

#[test]
fn front_mock_partition_by() {
    let metadata = &RouterConfigurationMock::new();

    let queries_to_check = vec![
        r#"create table t(a int primary key) partition by list (a)"#,
        r#"create table t(a int primary key) partition by hash (a, b)"#,
        r#"create table t(a int primary key) partition by range (a, b, c)"#,
    ];
    for query in queries_to_check {
        let plan = AbstractSyntaxTree::transform_into_plan(query, &[], metadata);
        assert!(plan.is_ok())
    }

    let queries_to_check = vec![
        r#"create table tp partition of t default"#,
        r#"create table tp partition of t default partition by range (a)"#,
        r#"create table tp partition of t for values in (1)"#,
        r#"create table tp partition of t for values in (1, 2)"#,
        r#"create table tp partition of t for values from (1) to (2)"#,
        r#"create table tp partition of t for values from (1, 3) to (2, 4)"#,
        r#"create table tp partition of t for values from (1, MINVALUE) to (2, MAXVALUE)"#,
        r#"create table tp partition of t for values with (modulus 1, remainder 2)"#,
        r#"create table tp partition of t for values with (modulus 1, remainder 2) partition by range (a, b, c)"#,
    ];
    for query in queries_to_check {
        let err = AbstractSyntaxTree::transform_into_plan(query, &[], metadata).unwrap_err();
        assert!(err
            .to_string()
            .contains("PARTITION OF logic is not supported yet"))
    }
}

#[test]
fn front_create_table_with_tier_syntax() {
    let query = r#"CREATE TABLE warehouse (
        id int PRIMARY KEY,
        type TEXT NOT NULL)
        USING memtx
        DISTRIBUTED BY (id)
        IN TIER "default";"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(query, &[], metadata);
    assert!(plan.is_ok());

    let query = r#"CREATE TABLE warehouse (
        id int PRIMARY KEY,
        type TEXT NOT NULL)
        USING memtx
        DISTRIBUTED BY (id)
        IN TIER;"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(query, &[], metadata);
    assert!(plan.is_err());
}

#[test]
fn front_alter_system_check_parses_ok() {
    let queries_to_check_ok = vec![
        r#"alter system set param_name = 1"#,
        r#"alter system set "param_name" = 1"#,
        r#"alter system set param_name = 'value'"#,
        r#"alter system set param_name = true"#,
        r#"alter system set param_name to 1"#,
        r#"alter system set param_name to 2.3"#,
        r#"alter system set param_name to default"#,
        r#"alter system set param_name to 'value'"#,
        r#"alter system set param_name to null"#,
        r#"alter system reset all"#,
        r#"alter system reset param_name"#,
        r#"alter system reset "param_name""#,
        r#"alter system reset "param_name" for all tiers"#,
        r#"alter system reset "param_name" for tier tier_name"#,
        r#"alter system reset "param_name" for tier "tier_name""#,
    ];
    let metadata = &RouterConfigurationMock::new();
    for query in queries_to_check_ok {
        let plan = AbstractSyntaxTree::transform_into_plan(query, &[], metadata);
        assert!(plan.is_ok())
    }

    let queries_to_check_all_expressions_not_supported = vec![
        r#"alter system set param_name = ?"#,
        r#"alter system set param_name = 1 + 1"#,
    ];
    let metadata = &RouterConfigurationMock::new();
    for query in queries_to_check_all_expressions_not_supported {
        let params_types = [DerivedType::new(Type::Integer)];
        let err =
            AbstractSyntaxTree::transform_into_plan(query, &params_types, metadata).unwrap_err();
        assert!(err
            .to_string()
            .contains("ALTER SYSTEM currently supports only literals as values."))
    }
}

#[test]
fn front_subqueries_interpreted_as_expression() {
    let input = r#"select (values (2)) from "test_space""#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (ROW($0) -> "col_1")
        scan "test_space"
    subquery $0:
    scan
            motion [policy: full, program: ReshardIfNeeded]
                values
                    value row (data=ROW(2::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_subqueries_interpreted_as_expression_as_required_child() {
    let input = r#"select * from (select (values (1)) from "test_space")"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("unnamed_subquery"."col_1"::int -> "col_1")
        scan "unnamed_subquery"
            projection (ROW($0) -> "col_1")
                scan "test_space"
    subquery $0:
    scan
                    motion [policy: full, program: ReshardIfNeeded]
                        values
                            value row (data=ROW(1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_subqueries_interpreted_as_expression_nested() {
    let input = r#"select (values ((values (2)))) from "test_space""#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (ROW($1) -> "col_1")
        scan "test_space"
    subquery $0:
    scan
                            motion [policy: full, program: ReshardIfNeeded]
                                values
                                    value row (data=ROW(2::int))
    subquery $1:
    scan
            motion [policy: full, program: ReshardIfNeeded]
                values
                    value row (data=ROW(ROW($0)))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_subqueries_interpreted_as_expression_under_group_by() {
    let input = r#"SELECT COUNT(*) FROM "test_space" GROUP BY "id" + (VALUES (1))"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (sum(("count_1"::int))::int -> "col_1")
        group by ("gr_expr_1"::int) output: ("gr_expr_1"::int -> "gr_expr_1", "count_1"::int -> "count_1")
            motion [policy: full, program: ReshardIfNeeded]
                projection ("test_space"."id"::int + ROW($0) -> "gr_expr_1", count((*::int))::int -> "count_1")
                    group by ("test_space"."id"::int + ROW($0)) output: ("test_space"."id"::int -> "id", "test_space"."sysFrom"::int -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::int -> "sys_op", "test_space"."bucket_id"::int -> "bucket_id")
                        scan "test_space"
    subquery $0:
    scan
                            motion [policy: full, program: ReshardIfNeeded]
                                values
                                    value row (data=ROW(1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_select_without_scan() {
    let input = r#"select 1"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (1::int -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_select_without_scan_2() {
    let input = r#"select (values (1)), (select count(*) from t2)"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (ROW($1) -> "col_1", ROW($0) -> "col_2")
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
            scan
                projection (sum(("count_1"::int))::int -> "col_1")
                    motion [policy: full, program: ReshardIfNeeded]
                        projection (count((*::int))::int -> "count_1")
                            scan "t2"
    subquery $1:
    scan
            motion [policy: full, program: ReshardIfNeeded]
                values
                    value row (data=ROW(1::int))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_select_without_scan_3() {
    let input = r#"select *"#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, &[], metadata).unwrap_err();

    assert_eq!(
        "invalid type: expected a Column in SelectWithoutScan, got Asterisk.",
        err.to_string()
    );
}

#[test]
fn front_select_without_scan_4() {
    let input = r#"select distinct 1"#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, &[], metadata).unwrap_err();

    assert_eq!(
        "invalid type: expected a Column in SelectWithoutScan, got Distinct.",
        err.to_string()
    );
}

#[test]
fn front_select_without_scan_5() {
    let input = r#"select (?, ?) in (select e, f from t2) as foo"#;
    let plan = sql_to_optimized_ir(input, vec![Value::Integer(1), Value::Integer(1)]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (ROW(1::int, 1::int) in ROW($0, $0) -> "foo")
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
            scan
                projection ("t2"."e"::int -> "e", "t2"."f"::int -> "f")
                    scan "t2"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_select_without_scan_6() {
    let input = r#"select (select 1) from t2 where f in (select 3 as foo)"#;
    let plan = sql_to_optimized_ir(input, vec![Value::Integer(1), Value::Integer(1)]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (ROW($1) -> "col_1")
        selection "t2"."f"::int in ROW($0)
            scan "t2"
    subquery $0:
    scan
                projection (3::int -> "foo")
    subquery $1:
    scan
            projection (1::int -> "col_1")
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_sql_check_concat_with_parameters() {
    let input = r#"values (? || ?)"#;

    let plan = sql_to_optimized_ir(input, vec![Value::from("a"), Value::from("b")]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r"
    values
        value row (data=ROW('a'::string || 'b'::string))
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    ");
}

#[test]
fn front_different_values_row_len() {
    let input = r#"values (1), (1,2)"#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, &[], metadata).unwrap_err();

    assert_eq!("VALUES lists must all be the same length", err.to_string());
}

#[test]
fn front_sql_whitespaces_are_not_ignored() {
    // Deletion of any WHITESPACE in those query will transform
    // them into invalid.
    let correct_queries = [
        r#"create user "emir" with password 'vildanov' using md5"#,
        r#"set value to key"#,
        r#"set transaction isolation level read commited"#,
        r#"grant create on user vasya to emir option(timeout=1)"#,
        r#"alter plugin "abc" 0.1.0 remove service "svc1" from tier "tier1" option(timeout=11)"#,
        r#"create table if not exists t(a int primary key,b int) using memtx distributed by(a,b) wait applied locally option(timeout=1)"#,
        r#"create procedure if not exists name(int,int,varchar(1)) language sql as $$insert into t values(1,2)$$ wait applied globally"#,
        r#"with cte1(a,b) as(select * from t),cte2 as(select * from t) select * from t join t on true group by a having b union all select * from t order by a"#,
        r#"select cast(1 as int) or not exists (values(true)) and 1+1 and true or (a in (select * from t)) and i is not null"#,
    ];

    fn whitespace_positions(s: &str) -> Vec<usize> {
        s.char_indices()
            .filter_map(|(pos, c)| if c.is_whitespace() { Some(pos) } else { None })
            .collect()
    }

    for query in correct_queries {
        let res = ParseTree::parse(Rule::Command, query);
        assert!(res.is_ok());
    }

    for query in correct_queries {
        let whitespaces = whitespace_positions(query);
        for wp_idx in whitespaces {
            let mut fixed = String::new();
            fixed.push_str(&query[..wp_idx]);
            fixed.push_str(&query[wp_idx + 1..]);
            let res = ParseTree::parse(Rule::Command, &fixed);
            assert!(res.is_err())
        }
    }
}

#[test]
fn front_sql_check_in_statement() {
    let correct_statements = [
        r#"select 1 in (1)"#,
        r#"select * from t where a in (1)"#,
        r#"select * from t where a in (select a from t)"#,
    ];

    let metadata = &RouterConfigurationMock::new();
    for input in correct_statements {
        let _ = AbstractSyntaxTree::transform_into_plan(input, &[], metadata).unwrap();
    }

    let invalid_statements = [r#"select 1 in 1"#, r#"select * from t where a in 1"#];

    for input in invalid_statements {
        let err = AbstractSyntaxTree::transform_into_plan(input, &[], metadata).unwrap_err();
        assert_eq!(
            "invalid expression: In expression must have query or a list of values as right child",
            err.to_string()
        );
    }
}

mod multi_queries {
    use super::*;
    use std::iter;

    const GOOD_QUERIES: &[&str] = &[
        r#"select * from foobar"#,
        r#"create user "emir" with password 'vildanov' using md5"#,
        r#"set value to key"#,
        r#"set transaction isolation level read commited"#,
        r#"grant create on user vasya to emir option(timeout=1)"#,
        r#"alter plugin "abc" 0.1.0 remove service "svc1" from tier "tier1" option(timeout=11)"#,
        r#"create table if not exists t(a int primary key,b int) using memtx distributed by(a,b) wait applied locally option(timeout=1)"#,
        r#"create procedure if not exists name(int,int,varchar(1)) language sql as $$insert into t values(1,2)$$ wait applied globally"#,
        r#"with cte1(a,b) as(select * from t),cte2 as(select * from t) select * from t join t on true group by a having b union all select * from t order by a"#,
        r#"select cast(1 as int) or not exists (values(true)) and 1+1 and true or (a in (select * from t)) and i is not null"#,
    ];

    // Rustfmt would put everything on one line,
    // giving a confusing mess of whitespace and punctuation.
    #[rustfmt::skip]
    const TRAILERS: &[&str] = &[
        ";",
        "     ;",
        ";    ",
        "  ;   ",
    ];

    fn parse(query: &str) -> Result<(), SbroadError> {
        let mut map1 = ParsingPairsMap::new();
        let mut map2 = HashMap::new();
        let mut map3 = HashMap::new();
        let mut standard_parse = AbstractSyntaxTree::empty();
        standard_parse.fill(query, &mut map1, &mut map2, &mut map3, &mut Vec::new())
    }

    #[test]
    fn trailing_semicolon_parses() {
        for query in GOOD_QUERIES {
            for trailer in TRAILERS {
                let modified_query = format!("{query}{trailer}");
                parse(&modified_query).unwrap_or_else(syntax_error(&modified_query));
            }
        }
    }

    #[test]
    fn multiple_empty_statements_allowed() {
        for query in GOOD_QUERIES {
            for (t1, t2) in iter::zip(TRAILERS, TRAILERS) {
                let modified_query = format!("{query}{t1}{t2}");
                parse(&modified_query).unwrap_or_else(syntax_error(&modified_query));
            }
        }
    }

    #[test]
    fn multistatement_queries_not_allowed() {
        for (start, end) in GOOD_QUERIES.iter().tuples() {
            for trailer in TRAILERS {
                let bad_query = format!("{start}{trailer}{end}");
                assert!(parse(&bad_query).is_err());
            }
        }
    }
}

#[track_caller]
fn syntax_error(query: &str) -> impl '_ + FnOnce(SbroadError) {
    move |err| {
        eprintln!("{err}");
        eprintln!("QUERY[[{query}]]END QUERY");
        panic!("syntax error");
    }
}

mod coalesce;
mod cte;
mod ddl;
mod funcs;
mod global;
mod index;
mod insert;
mod join;
mod like;
mod limit;
mod params;
mod single;
mod subtree_cloner;
mod text_literal_parsing;
mod trim;
mod union;
mod update;
