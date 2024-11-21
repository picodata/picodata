use crate::errors::{SbroadError, TypeError};
use crate::ir::relation::Type;
use crate::ir::transformation::helpers::{sql_to_ir_without_bind, sql_to_optimized_ir};
use crate::ir::value::Value;
use pretty_assertions::assert_eq;

fn infer_pg_parameters_types(
    query: &str,
    client_types: &[Option<Type>],
) -> Result<Vec<Type>, SbroadError> {
    let mut plan = sql_to_ir_without_bind(query);
    plan.infer_pg_parameters_types(client_types)
}

#[test]
fn param_type_inference() {
    // no params
    let types = infer_pg_parameters_types(r#"SELECT * FROM "test_space""#, &[]);
    assert_eq!(types.unwrap(), []);

    // can't infer without cast
    let types = infer_pg_parameters_types(r#"SELECT $1 FROM "test_space""#, &[]);
    assert!(matches!(
        types,
        Err(SbroadError::TypeError(
            TypeError::CouldNotDetermineParameterType(0)
        ))
    ));

    let types = infer_pg_parameters_types(r#"SELECT $1 + $2 FROM "test_space""#, &[]);
    assert!(matches!(
        types,
        Err(SbroadError::TypeError(
            TypeError::CouldNotDetermineParameterType(..)
        ))
    ));

    // infer types from cast
    let types = infer_pg_parameters_types(r#"SELECT CAST($1 AS INTEGER) FROM "test_space""#, &[]);
    assert_eq!(types.unwrap(), [Type::Integer]);

    let types = infer_pg_parameters_types(r#"SELECT $1::integer FROM "test_space""#, &[]);
    assert_eq!(types.unwrap(), [Type::Integer]);

    let types = infer_pg_parameters_types(r#"SELECT $1::integer + $1 FROM "test_space""#, &[]);
    assert_eq!(types.unwrap(), [Type::Integer]);

    let types = infer_pg_parameters_types(r#"SELECT $1 + $1::integer FROM "test_space""#, &[]);
    assert_eq!(types.unwrap(), [Type::Integer]);

    let types =
        infer_pg_parameters_types(r#"SELECT $1::integer + $1::integer FROM "test_space""#, &[]);
    assert_eq!(types.unwrap(), [Type::Integer]);

    let types = infer_pg_parameters_types(
        r#"SELECT $1::integer + $2::unsigned FROM "test_space""#,
        &[],
    );
    assert_eq!(types.unwrap(), [Type::Integer, Type::Unsigned]);

    let types = infer_pg_parameters_types(
        r#"SELECT $1::integer + $3::unsigned FROM "test_space""#,
        &[],
    );
    assert!(matches!(
        types,
        Err(SbroadError::TypeError(
            TypeError::CouldNotDetermineParameterType(1)
        ))
    ));

    // client provided a type
    let types = infer_pg_parameters_types(r#"SELECT $1 FROM "test_space""#, &[Some(Type::String)]);
    assert_eq!(types.unwrap(), [Type::String]);

    // client type has a higher priority
    let types = infer_pg_parameters_types(
        r#"SELECT $1::integer FROM "test_space""#,
        &[Some(Type::String)],
    );
    assert_eq!(types.unwrap(), [Type::String]);

    let types = infer_pg_parameters_types(
        r#"SELECT $1::integer + $1 FROM "test_space""#,
        &[Some(Type::Unsigned)],
    );
    assert_eq!(types.unwrap(), [Type::Unsigned]);

    // infer one type and get another from the client
    let types = infer_pg_parameters_types(
        r#"SELECT $1 + $2::unsigned FROM "test_space""#,
        &[Some(Type::Unsigned)],
    );
    assert_eq!(types.unwrap(), [Type::Unsigned, Type::Unsigned]);

    let types = infer_pg_parameters_types(
        r#"SELECT $1::unsigned + $2 FROM "test_space""#,
        &[None, Some(Type::Unsigned)],
    );
    assert_eq!(types.unwrap(), [Type::Unsigned, Type::Unsigned]);

    // ambiguous types
    let types = infer_pg_parameters_types(
        r#"SELECT $1::integer + $1::unsigned FROM "test_space""#,
        &[],
    );
    assert!(matches!(
        types,
        Err(SbroadError::TypeError(TypeError::AmbiguousParameterType(
            ..
        )))
    ));

    // too many client types
    let types = infer_pg_parameters_types(
        r#"SELECT $1 FROM "test_space""#,
        &[Some(Type::String), Some(Type::Unsigned)],
    );
    assert!(matches!(
        types,
        Err(SbroadError::UnexpectedNumberOfValues(..))
    ));

    let types = infer_pg_parameters_types(r#"SELECT $1::integer::text FROM "test_space""#, &[]);
    assert_eq!(types.unwrap(), [Type::Integer]);

    let types = infer_pg_parameters_types(r#"SELECT ($1 * 1.0)::integer FROM "test_space""#, &[]);
    assert!(matches!(
        types,
        Err(SbroadError::TypeError(
            TypeError::CouldNotDetermineParameterType(0)
        ))
    ));

    let types = infer_pg_parameters_types(r#"SELECT $1 * 1::integer FROM "test_space""#, &[]);
    assert!(matches!(
        types,
        Err(SbroadError::TypeError(
            TypeError::CouldNotDetermineParameterType(0)
        ))
    ));
}

#[test]
fn front_param_in_cast() {
    let pattern = r#"SELECT CAST(? AS INTEGER) FROM "test_space""#;
    let plan = sql_to_optimized_ir(pattern, vec![Value::from(1_i64)]);

    let expected_explain = String::from(
        r#"projection (1::integer::int -> "col_1")
    scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_params1() {
    let pattern = r#"SELECT "id", "FIRST_NAME" FROM "test_space"
        WHERE "sys_op" = ? AND "sysFrom" > ?"#;
    let plan = sql_to_optimized_ir(pattern, vec![Value::from(0_i64), Value::from(1_i64)]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
    selection ROW("test_space"."sys_op"::unsigned) = ROW(0::integer) and ROW("test_space"."sysFrom"::unsigned) > ROW(1::integer)
        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_params2() {
    let pattern = r#"SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? AND "FIRST_NAME" = ?"#;

    let plan = sql_to_optimized_ir(pattern, vec![Value::Null, Value::from("hello")]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection ROW("test_space"."sys_op"::unsigned) = ROW(NULL::scalar) and ROW("test_space"."FIRST_NAME"::string) = ROW('hello'::string)
        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

// check cyrillic params support
#[test]
fn front_params3() {
    let pattern = r#"SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? AND "FIRST_NAME" = ?"#;

    let plan = sql_to_optimized_ir(pattern, vec![Value::Null, Value::from("кириллица")]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection ROW("test_space"."sys_op"::unsigned) = ROW(NULL::scalar) and ROW("test_space"."FIRST_NAME"::string) = ROW('кириллица'::string)
        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
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

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection ROW("test_space"."FIRST_NAME"::string) = ROW('''± !@#$%^&*()_+=-\/><";:,.`~'::string)
        scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
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

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection ROW("test_space"."sys_op"::unsigned) = ROW(0::integer) or ROW("test_space"."id"::unsigned) in ROW($0)
        scan "test_space"
subquery $0:
motion [policy: segment([ref("sysFrom")])]
            scan
                projection ("test_space_hist"."sysFrom"::unsigned -> "sysFrom")
                    selection ROW("test_space_hist"."sys_op"::unsigned) = ROW(1::integer)
                        scan "test_space_hist"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
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

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection ROW("test_space"."sys_op"::unsigned) = ROW(0::integer) or not ROW("test_space"."id"::unsigned) in ROW($0)
        scan "test_space"
subquery $0:
motion [policy: full]
            scan
                union all
                    projection ("test_space"."id"::unsigned -> "id")
                        selection ROW("test_space"."sys_op"::unsigned) = ROW(1::integer)
                            scan "test_space"
                    projection ("test_space"."id"::unsigned -> "id")
                        selection ROW("test_space"."sys_op"::unsigned) = ROW(2::integer)
                            scan "test_space"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}
