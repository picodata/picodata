use pretty_assertions::assert_eq;

use crate::backend::sql::ir::PatternWithParams;
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::result::ProducerResult;
use crate::executor::vtable::VirtualTable;
use crate::ir::tests::vcolumn_integer_user_non_null;
use crate::ir::transformation::redistribution::tests::get_motion_id;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::value::{LuaValue, Value};

use super::*;

#[test]
fn between1_test() {
    let sql = r#"
        SELECT "identification_number" FROM "hash_testing" AS "t"
        WHERE "identification_number" BETWEEN 1 AND (
            SELECT "identification_number" as "id" FROM "hash_testing_hist"
            WHERE "identification_number" = 2
        )
        "#;

    // Initialize the query.
    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();

    // Validate the motion type.
    let motion_id = *get_motion_id(plan, 0, 0).unwrap();
    assert_eq!(&MotionPolicy::Full, get_motion_policy(plan, motion_id));

    // Mock a virtual table.
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_tuple(vec![Value::from(2_u64)]);
    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);

    // Execute the query.
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    // Validate the result.
    let mut expected = ProducerResult::new();
    expected.rows.extend(vec![vec![
        LuaValue::String("Execute query on all buckets".to_string()),
        LuaValue::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT "t"."identification_number" FROM "hash_testing" as "t""#,
                r#"WHERE ("t"."identification_number") >= (?)"#,
                r#"and ("t"."identification_number") <= (SELECT "COL_1" FROM "TMP_test_0136")"#,
            ),
            vec![Value::from(1_u64)],
        ))),
    ]]);
    assert_eq!(expected, result);
}

#[test]
fn between2_test() {
    let sql = r#"
        SELECT "identification_number" FROM "hash_testing" AS "t"
        WHERE (
            SELECT "identification_number" as "id" FROM "hash_testing_hist"
            WHERE "identification_number" = 2
        ) BETWEEN 1 and 3
        "#;

    // Initialize the query.
    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();

    // Validate the motion type.
    let motion1_id = *get_motion_id(plan, 0, 0).unwrap();
    assert_eq!(&MotionPolicy::Full, get_motion_policy(plan, motion1_id));
    assert_eq!(true, get_motion_id(plan, 0, 2).is_none());

    // Mock a virtual table.
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_tuple(vec![Value::from(2_u64)]);

    // Bind the virtual table to both motions.
    query
        .coordinator
        .add_virtual_table(motion1_id, virtual_table.clone());

    // Execute the query.
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    // Validate the result.
    let mut expected = ProducerResult::new();
    expected.rows.extend(vec![vec![
        LuaValue::String("Execute query on all buckets".to_string()),
        LuaValue::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT "t"."identification_number" FROM "hash_testing" as "t""#,
                r#"WHERE (SELECT "COL_1" FROM "TMP_test_0136") >= (?)"#,
                r#"and (SELECT "COL_1" FROM "TMP_test_0136") <= (?)"#,
            ),
            vec![Value::from(1_u64), Value::from(3_u64)],
        ))),
    ]]);
    assert_eq!(expected, result);
}
