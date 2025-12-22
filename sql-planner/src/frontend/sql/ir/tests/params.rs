use crate::ir::node::{Node32, Parameter};
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::value::Value;
use smol_str::format_smolstr;

#[test]
fn front_numeric_param_in_cast() {
    let typ = "numeric";
    let pattern = format!("SELECT CAST(? AS {}) FROM \"test_space\"", typ);
    let plan = sql_to_optimized_ir(pattern.as_str(), vec![Value::from(1_i64)]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (1::decimal -> "col_1")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_number_param_in_cast() {
    let typ = "number";
    let pattern = format!("SELECT CAST(? AS {}) FROM \"test_space\"", typ);
    let plan = sql_to_optimized_ir(pattern.as_str(), vec![Value::from(1_i64)]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (1::decimal -> "col_1")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_param_in_cast() {
    let pattern = r#"SELECT CAST(? AS int) FROM "test_space""#;
    let plan = sql_to_optimized_ir(pattern, vec![Value::from(1_i64)]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection (1::int -> "col_1")
        scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_params1() {
    let pattern = r#"SELECT "id", "FIRST_NAME" FROM "test_space"
        WHERE "sys_op" = ? AND "sysFrom" > ?"#;
    let plan = sql_to_optimized_ir(pattern, vec![Value::from(0_i64), Value::from(1_i64)]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::int -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
        selection ("test_space"."sys_op"::int = 0::int) and ("test_space"."sysFrom"::int > 1::int)
            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_params2() {
    let pattern = r#"SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? AND "FIRST_NAME" = ?"#;

    let plan = sql_to_optimized_ir(pattern, vec![Value::Null, Value::from("hello")]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::int -> "id")
        selection ("test_space"."sys_op"::int = NULL::unknown) and ("test_space"."FIRST_NAME"::string = 'hello'::string)
            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

// check cyrillic params support
#[test]
fn front_params3() {
    let pattern = r#"SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? AND "FIRST_NAME" = ?"#;

    let plan = sql_to_optimized_ir(pattern, vec![Value::Null, Value::from("кириллица")]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::int -> "id")
        selection ("test_space"."sys_op"::int = NULL::unknown) and ("test_space"."FIRST_NAME"::string = 'кириллица'::string)
            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

// check symbols in values (grammar)
#[test]
fn front_params4() {
    let pattern = r#"SELECT "id" FROM "test_space"
        WHERE "FIRST_NAME" = ?"#;

    let plan = sql_to_optimized_ir(
        pattern,
        vec![Value::from(r#"''± !@#$%^&*()_+=-\/><";:,.`~"#)],
    );

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::int -> "id")
        selection "test_space"."FIRST_NAME"::string = '''± !@#$%^&*()_+=-\/><";:,.`~'::string
            scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

// check parameter binding order, when selection has sub-queries
#[test]
fn front_params5() {
    let pattern = r#"
        SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? OR "id" IN (
            SELECT "sysFrom" FROM "test_space_hist"
            WHERE "sys_op" = ?
        )
    "#;

    let plan = sql_to_optimized_ir(pattern, vec![Value::from(0_i64), Value::from(1_i64)]);

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::int -> "id")
        selection ("test_space"."sys_op"::int = 0::int) or ("test_space"."id"::int in ROW($0))
            scan "test_space"
    subquery $0:
    motion [policy: segment([ref("sysFrom")]), program: ReshardIfNeeded]
                scan
                    projection ("test_space_hist"."sysFrom"::int -> "sysFrom")
                        selection "test_space_hist"."sys_op"::int = 1::int
                            scan "test_space_hist"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn front_params6() {
    let pattern = r#"
        SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? OR "id" NOT IN (
            SELECT "id" FROM "test_space"
            WHERE "sys_op" = ?
            UNION ALL
            SELECT "id" FROM "test_space"
            WHERE "sys_op" = ?
        )
    "#;

    let plan = sql_to_optimized_ir(
        pattern,
        vec![Value::from(0_i64), Value::from(1_i64), Value::from(2_i64)],
    );

    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("test_space"."id"::int -> "id")
        selection ("test_space"."sys_op"::int = 0::int) or (not ("test_space"."id"::int in ROW($0)))
            scan "test_space"
    subquery $0:
    motion [policy: full, program: ReshardIfNeeded]
                scan
                    union all
                        projection ("test_space"."id"::int -> "id")
                            selection "test_space"."sys_op"::int = 1::int
                                scan "test_space"
                        projection ("test_space"."id"::int -> "id")
                            selection "test_space"."sys_op"::int = 2::int
                                scan "test_space"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn mark_unique_parameters1() {
    let pattern = "SELECT $1, $1, $2";
    let is_unique = vec![false, true];
    let plan = sql_to_optimized_ir(pattern, vec![Value::from(1_i64), Value::from(1_i64)]);
    for node in plan.nodes.iter32() {
        if let Node32::Parameter(Parameter { index, unique, .. }) = node {
            assert_eq!(*unique, is_unique[*index as usize]);
        }
    }
}

#[test]
fn mark_unique_parameters2() {
    let pattern = "SELECT $1, $1, $3";
    let is_unique = vec![false, false, true];
    let plan = sql_to_optimized_ir(
        pattern,
        vec![Value::from(1_i64), Value::from(1_i64), Value::from(1_i64)],
    );
    for node in plan.nodes.iter32() {
        if let Node32::Parameter(Parameter { index, unique, .. }) = node {
            assert_eq!(*unique, is_unique[*index as usize]);
        }
    }
}

#[test]
fn mark_unique_parameters3() {
    let pattern = "SELECT a + $1 FROM t WHERE b = $2";
    let is_unique = vec![true, true];
    let plan = sql_to_optimized_ir(
        pattern,
        vec![Value::from(1_i64), Value::from(1_i64), Value::from(1_i64)],
    );
    for node in plan.nodes.iter32() {
        if let Node32::Parameter(Parameter { index, unique, .. }) = node {
            assert_eq!(*unique, is_unique[*index as usize]);
        }
    }
}

#[test]
fn mark_unique_parameters4() {
    let pattern = "INSERT INTO t VALUES ($1, $2, $3, $4)";
    let is_unique = vec![true, true, true, true];
    let plan = sql_to_optimized_ir(
        pattern,
        vec![
            Value::from(1),
            Value::from(1),
            Value::from(1),
            Value::from(1),
        ],
    );
    for node in plan.nodes.iter32() {
        if let Node32::Parameter(Parameter { index, unique, .. }) = node {
            assert_eq!(*unique, is_unique[*index as usize]);
        }
    }
}

#[test]
fn mark_unique_parameters5() {
    let pattern = "INSERT INTO t VALUES ($1, $2, $3, $4), ($1 + $2, $2 + $3, $3 + $4, $4 + $1)";
    let is_unique = vec![false, false, false, false];
    let plan = sql_to_optimized_ir(
        pattern,
        vec![
            Value::from(1),
            Value::from(1),
            Value::from(1),
            Value::from(1),
        ],
    );
    for node in plan.nodes.iter32() {
        if let Node32::Parameter(Parameter { index, unique, .. }) = node {
            assert_eq!(*unique, is_unique[*index as usize]);
        }
    }
}

#[test]
fn mark_unique_parameters6() {
    let count = u16::MAX as usize;
    let mut params = Vec::with_capacity(u16::MAX as _);
    for i in 0..count {
        params.push(format_smolstr!("${}", i + 1))
    }

    let values = format!("VALUES ({})", params.join(","));
    let plan = sql_to_optimized_ir(
        &values,
        std::iter::repeat_n(Value::from(0), count).collect(),
    );
    for node in plan.nodes.iter32() {
        if let Node32::Parameter(Parameter { unique, .. }) = node {
            assert_eq!(*unique, true);
        }
    }
}
