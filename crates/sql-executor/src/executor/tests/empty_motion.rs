use crate::test_helpers::ExecutingQueryExt;
use pretty_assertions::assert_eq;

use crate::executor::engine::mock::{DispatchInfo, PortMocked, RouterRuntimeMock};
use crate::executor::vtable::VirtualTable;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::value::Value;
use crate::test_helpers::get_motion_id;
use crate::test_helpers::vcolumn_integer_user_non_null;
use insta::assert_yaml_snapshot;

use super::*;

#[test]
fn empty_motion1_test() {
    let sql = r#"SELECT * FROM (
        SELECT "t"."a", "t"."b" FROM "t" INNER JOIN "t2" ON "t"."a" = "t2"."g" and "t"."b" = "t2"."h"
        WHERE "t"."a" = 0
        EXCEPT
        SELECT "t"."a", "t"."b" FROM "t" INNER JOIN "t2" ON "t"."a" = "t2"."g" and "t"."b" = "t2"."h"
        WHERE "t"."a" = 1
    ) as "Q""#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion1_id = query.get_motion_id(0, 0);
    let mut virtual_t1 = t2_empty();
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.get_exec_plan().get_ir_plan(), motion1_id)
    {
        virtual_t1.reshard(key, query.get_coordinator()).unwrap();
    }
    query
        .get_coordinator()
        .add_virtual_table(motion1_id, virtual_t1);
    let motion2_id = query.get_motion_id(0, 1);
    let mut virtual_t2 = t2_empty();
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.get_exec_plan().get_ir_plan(), motion2_id)
    {
        virtual_t2.reshard(key, query.get_coordinator()).unwrap();
    }
    query
        .get_coordinator()
        .add_virtual_table(motion2_id, virtual_t2);

    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();
    let info = port.decode();
    assert_eq!(1, info.len());
    let info = info.get(0).unwrap();
    assert_yaml_snapshot!(info, @r#"
    All:
      - "SELECT * FROM (SELECT \"t\".\"a\", \"t\".\"b\" FROM \"t\" INNER JOIN (SELECT \"COL_1\",\"COL_2\",\"COL_3\",\"COL_4\" FROM \"_tmp_0_0136\") as \"t2\" ON \"t\".\"a\" = \"t2\".\"COL_3\" and \"t\".\"b\" = \"t2\".\"COL_4\" WHERE \"t\".\"a\" = $1::int EXCEPT SELECT \"t\".\"a\", \"t\".\"b\" FROM \"t\" INNER JOIN (SELECT \"COL_1\",\"COL_2\",\"COL_3\",\"COL_4\" FROM \"_tmp_0_1136\") as \"t2\" ON \"t\".\"a\" = \"t2\".\"COL_3\" and \"t\".\"b\" = \"t2\".\"COL_4\" WHERE \"t\".\"a\" = $2::int) as \"Q\""
      - - Integer: 0
        - Integer: 1
    "#);
}

#[test]
fn empty_buckets_with_segment_motion_test() {
    let sql = r#"
        SELECT 1
        UNION ALL
        SELECT "a" FROM "t5" WHERE "a" = 1 AND "a" = 2 AND "a" IN (SELECT "b" FROM "t5")
        "#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();

    // The first slice also holds the `SerializeAsEmptyTable` motion of the
    // `SELECT 1` branch, so pick the segmented one explicitly.
    let motion_id = {
        let plan = query.get_exec_plan().get_ir_plan();
        (0..)
            .map_while(|pos| get_motion_id(plan, 0, pos).copied())
            .find(|id| matches!(get_motion_policy(plan, *id), MotionPolicy::Segment(_)))
            .expect("segment motion in the first slice")
    };

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_tuple(vec![Value::from(1)]);
    virtual_table.add_tuple(vec![Value::from(2)]);
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.get_exec_plan().get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, query.get_coordinator()).unwrap();
    }
    query
        .get_coordinator()
        .add_virtual_table(motion_id, virtual_table);

    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();

    let info = port.decode();
    assert_eq!(1, info.len());
    let info = info.get(0).unwrap();
    assert_yaml_snapshot!(info, @r#"
    Any:
      - "SELECT $1::int as \"col_1\" UNION ALL SELECT \"t5\".\"a\" FROM \"t5\" WHERE \"t5\".\"a\" = $2::int and \"t5\".\"a\" = $3::int and \"t5\".\"a\" in (SELECT \"COL_1\" FROM \"_tmp_0_0136\")"
      - - Integer: 1
        - Integer: 1
        - Integer: 2
    "#);
}

/// The full motion of a sharded UNION under UNION ALL is read on exactly one
/// replicaset; the others get an empty table instead.
#[test]
fn full_motion_of_union_under_union_all_test() {
    let sql = r#"SELECT count(*) AS "a" FROM "t5" WHERE "a" = 1
        UNION SELECT "a" FROM "t5" WHERE "a" = 1
        UNION ALL SELECT "a" FROM "t5""#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    assert_eq!(
        get_motion_policy(query.get_exec_plan().get_ir_plan(), motion_id),
        &MotionPolicy::Full
    );

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_tuple(vec![Value::from(0)]);
    virtual_table.add_tuple(vec![Value::from(1)]);
    query
        .get_coordinator()
        .add_virtual_table(motion_id, virtual_table);

    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();
    let info = port.decode();
    let [DispatchInfo::Filtered(per_rs)] = &info[..] else {
        panic!("expected one filtered dispatch, got {info:?}");
    };
    let sql_by_rs: Vec<_> = per_rs.iter().map(|(sql, _, rs, _)| (rs, sql)).collect();
    assert_yaml_snapshot!(sql_by_rs, @r#"
    - - replicaset_0
      - " select null::int as \"a\" where false UNION ALL SELECT \"t5\".\"a\" FROM \"t5\""
    - - replicaset_1
      - "SELECT \"COL_1\" as \"a\" FROM (SELECT \"COL_1\" FROM \"_tmp_0_0136\") UNION ALL SELECT \"t5\".\"a\" FROM \"t5\""
    "#);
}

fn t2_empty() -> VirtualTable {
    let mut virtual_table = VirtualTable::new();

    // t2 has four columns: e, f, g, h
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());

    virtual_table.set_alias("t2");

    virtual_table
}
