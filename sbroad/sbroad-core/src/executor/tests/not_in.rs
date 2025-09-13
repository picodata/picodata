use pretty_assertions::assert_eq;

use crate::executor::engine::mock::{DispatchInfo, PortMocked, RouterRuntimeMock};
use crate::executor::vtable::VirtualTable;
use crate::ir::tests::vcolumn_integer_user_non_null;
use crate::ir::transformation::redistribution::tests::get_motion_id;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::value::Value;

use super::*;

#[test]
fn not_in1_test() {
    let sql = r#"
        SELECT "identification_number" FROM "hash_testing" AS "t"
        WHERE "identification_number" NOT IN (
            SELECT "identification_number" as "id" FROM "hash_testing_hist"
            WHERE "identification_number" = 3
        )
        "#;

    // Initialize the query.
    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();

    // Validate the motion type.
    let motion_id = *get_motion_id(plan, 0, 0).unwrap();
    assert_eq!(&MotionPolicy::Full, get_motion_policy(plan, motion_id));
    assert_eq!(true, get_motion_id(plan, 0, 1).is_none());

    // Mock a virtual table.
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_tuple(vec![Value::from(3)]);
    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);

    // Execute the query.
    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();

    // Validate the result.
    let info = port.decode();
    assert_eq!(1, info.len());
    let DispatchInfo::All(sql, params) = info.get(0).unwrap() else {
        panic!("Expected a single dispatch on all replicasets");
    };
    assert_eq!(
        sql,
        &format!(
            "{} {}",
            r#"SELECT "t"."identification_number" FROM "hash_testing" as "t""#,
            r#"WHERE not ("t"."identification_number" in (SELECT "COL_1" FROM "TMP_test_0136"))"#,
        ),
    );
    assert_eq!(params, &vec![]);
}
