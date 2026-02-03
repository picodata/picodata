use super::*;
use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::executor::engine::mock::{DispatchInfo, PortMocked, RouterRuntimeMock};
use crate::ir::node::{ArenaType, Node, Over, Projection, ReferenceTarget, Window};
use crate::ir::operator::{OrderByElement, OrderByEntity};
use crate::ir::tests::{vcolumn_integer_user_non_null, vcolumn_user_non_null};
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::transformation::redistribution::{MotionOpcode, MotionPolicy, Program};
use crate::ir::tree::Snapshot;
use crate::ir::types::{CastType, DerivedType, UnrestrictedType as Type};
use crate::ir::Slice;
use engine::mock::TEMPLATE;
use pretty_assertions::assert_eq;
use smol_str::SmolStr;
use std::rc::Rc;

fn reshard_vtable(
    query: &ExecutingQuery<RouterRuntimeMock>,
    motion_id: NodeId,
    virtual_table: &mut VirtualTable,
) {
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }
}

/// Helper function to generate sql from `exec_plan` from given `top_id` node.
/// Used for testing.
fn get_sql_from_execution_plan(
    exec_plan: &mut ExecutionPlan,
    top_id: NodeId,
    snapshot: Snapshot,
    name_base: &str,
) -> PatternWithParams {
    let subplan = exec_plan.take_subtree(top_id).unwrap();
    let subplan_top_id = subplan.get_ir_plan().get_top().unwrap();
    let sp = SyntaxPlan::new(&subplan, subplan_top_id, snapshot).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let (sql, _) = subplan.to_sql(&nodes, name_base, None).unwrap();
    sql
}

#[test]
fn exec_plan_subtree_test() {
    let sql = r#"SELECT "FIRST_NAME" FROM "test_space" where "id" in
    (SELECT "identification_number" FROM "hash_testing" where "identification_number" > 1)"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    let mut virtual_table = virtual_table_23(None);
    reshard_vtable(&query, motion_id, &mut virtual_table);
    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check sub-query
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);

    assert_eq!(sql.params, vec![Value::from(1)]);
    insta::assert_snapshot!(
        sql.pattern,
        @r#"SELECT "hash_testing"."identification_number" FROM "hash_testing" WHERE "hash_testing"."identification_number" > CAST($1 AS int)"#,
    );

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);

    assert_eq!(sql.params, vec![]);
    insta::assert_snapshot!(
        sql.pattern,
        @r#"SELECT "test_space"."FIRST_NAME" FROM "test_space" WHERE "test_space"."id" in (SELECT "COL_1" FROM "TMP_test_0136")"#,
    )
}

#[test]
fn exec_plan_subtree_two_stage_groupby_test() {
    let sql = r#"SELECT "T1"."FIRST_NAME" FROM "test_space" as "T1" group by "T1"."FIRST_NAME""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(vcolumn_user_non_null(Type::String));

    reshard_vtable(&query, motion_id, &mut virtual_table);

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();
    if let MotionPolicy::Full = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Full for local aggregation stage");
    };

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "T1"."FIRST_NAME" as "gr_expr_1" FROM "test_space" as "T1" GROUP BY "T1"."FIRST_NAME""#
                .to_string(),
            vec![]
        )
    );

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "COL_1" as "FIRST_NAME" FROM (SELECT "COL_1" FROM "TMP_test_0136") GROUP BY "COL_1""#.to_string(),
            vec![]
        ));
}

#[test]
fn exec_plan_subtree_two_stage_groupby_test_2() {
    let sql = r#"SELECT "T1"."FIRST_NAME", "T1"."sys_op", "T1"."sysFrom" FROM "test_space" as "T1" GROUP BY "T1"."FIRST_NAME", "T1"."sys_op", "T1"."sysFrom""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(vcolumn_user_non_null(Type::String));
    virtual_table.add_column(vcolumn_user_non_null(Type::String));
    virtual_table.add_column(vcolumn_user_non_null(Type::String));
    reshard_vtable(&query, motion_id, &mut virtual_table);

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    if let MotionPolicy::Full = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Full for local aggregation stage");
    };
    assert_eq!(
        sql,
        PatternWithParams::new(
            f_sql(
                r#"SELECT "T1"."FIRST_NAME" as "gr_expr_1",
"T1"."sys_op" as "gr_expr_2",
"T1"."sysFrom" as "gr_expr_3"
FROM "test_space" as "T1"
GROUP BY "T1"."FIRST_NAME", "T1"."sys_op", "T1"."sysFrom""#
            ),
            vec![]
        )
    );

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            f_sql(
                r#"SELECT "COL_1" as "FIRST_NAME",
"COL_2" as "sys_op", "COL_3" as "sysFrom"
FROM (SELECT "COL_1","COL_2","COL_3" FROM "TMP_test_0136")
GROUP BY "COL_1", "COL_2", "COL_3""#
            ),
            vec![]
        )
    );
}

#[test]
fn exec_plan_subtree_aggregates() {
    let sql = r#"SELECT "T1"."sys_op" + "T1"."sys_op", "T1"."sys_op"*2 + count("T1"."sysFrom"),
                      sum("T1"."id"), sum(distinct "T1"."id"*"T1"."sys_op") / count(distinct "id"),
                      group_concat("T1"."FIRST_NAME", 'o'), avg("T1"."id"), total("T1"."id"), min("T1"."id"), max("T1"."id")
                      FROM "test_space" as "T1" group by "T1"."sys_op""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_user_non_null(Type::String));
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    reshard_vtable(&query, motion_id, &mut virtual_table);

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    if let MotionPolicy::Full = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Full for local aggregation stage");
    };
    assert_eq!(sql.params, vec![Value::from("o")]);
    insta::assert_snapshot!(sql.pattern, @r#"SELECT "T1"."sys_op" as "gr_expr_1", CAST (("T1"."id" * "T1"."sys_op") as int) as "gr_expr_2", CAST ("T1"."id" as int) as "gr_expr_3", sum (CAST ("T1"."id" as int)) as "sum_2", count (CAST ("T1"."sysFrom" as int)) as "count_1", count (CAST ("T1"."id" as int)) as "avg_4", group_concat (CAST ("T1"."FIRST_NAME" as string), CAST($1 AS string)) as "group_concat_3", max (CAST ("T1"."id" as int)) as "max_7", min (CAST ("T1"."id" as int)) as "min_6", total (CAST ("T1"."id" as int)) as "total_5" FROM "test_space" as "T1" GROUP BY "T1"."sys_op", CAST (("T1"."id" * "T1"."sys_op") as int), CAST ("T1"."id" as int)"#);

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(sql.params, vec![Value::Integer(2), Value::from("o")]);
    insta::assert_snapshot!(sql.pattern, @r#"SELECT "COL_1" + "COL_1" as "col_1", ("COL_1" * CAST($1 AS int)) + sum ("COL_5") as "col_2", sum ("COL_4") as "col_3", sum (DISTINCT "COL_2") / count (DISTINCT "COL_3") as "col_4", group_concat ("COL_7", CAST($2 AS string)) as "col_5", sum (CAST ("COL_4" as double)) / sum (CAST ("COL_6" as double)) as "col_6", total ("COL_10") as "col_7", min ("COL_9") as "col_8", max ("COL_8") as "col_9" FROM (SELECT "COL_1","COL_2","COL_3","COL_4","COL_5","COL_6","COL_7","COL_8","COL_9","COL_10" FROM "TMP_test_0136") GROUP BY "COL_1""#);
}

#[test]
fn exec_plan_subtree_aggregates_no_groupby() {
    let sql = r#"SELECT count("T1"."sysFrom"), sum(distinct "T1"."id" + "T1"."sysFrom") FROM "test_space" as "T1""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    reshard_vtable(&query, motion_id, &mut virtual_table);

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    if let MotionPolicy::Full = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Full for local aggregation stage");
    };
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT CAST (("T1"."id" + "T1"."sysFrom") as int) as "gr_expr_1", count (CAST ("T1"."sysFrom" as int)) as "count_1" FROM "test_space" as "T1" GROUP BY CAST (("T1"."id" + "T1"."sysFrom") as int)"#.to_string(),
            vec![]
        ));

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT sum ("COL_2") as "col_1", sum (DISTINCT "COL_1") as "col_2" FROM (SELECT "COL_1","COL_2" FROM "TMP_test_0136")"#.to_string(),
            vec![]
        ));
}

#[test]
fn exec_plan_subquery_under_motion_without_alias() {
    let sql = r#"
    SELECT * FROM
            (SELECT "id" as "tid" FROM "test_space")
    INNER JOIN
            (SELECT "identification_number" as "sid" FROM "hash_testing")
    ON true
    "#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    // name would be added during materialization from subquery
    let mut virtual_table = virtual_table_23(Some("unnamed_subquery_1"));
    reshard_vtable(&query, motion_id, &mut virtual_table);
    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT * FROM (SELECT "test_space"."id" as "tid" FROM "test_space") as "unnamed_subquery" INNER JOIN (SELECT "COL_1" FROM "TMP_test_0136") as "unnamed_subquery_1" ON CAST($1 AS bool)"#.to_string(),
            vec![Value::Boolean(true)]
        ));
}

#[test]
fn exec_plan_subquery_under_motion_with_alias() {
    let sql = r#"
    SELECT * FROM
            (SELECT "id" as "tid" FROM "test_space")
    INNER JOIN
            (SELECT "identification_number" as "sid" FROM "hash_testing") AS "hti"
    ON true
    "#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    let mut virtual_table = virtual_table_23(Some("hti"));
    reshard_vtable(&query, motion_id, &mut virtual_table);
    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT * FROM (SELECT "test_space"."id" as "tid" FROM "test_space") as "unnamed_subquery" INNER JOIN (SELECT "COL_1" FROM "TMP_test_0136") as "hti" ON CAST($1 AS bool)"#.to_string(),
            vec![Value::Boolean(true)]
        ));
}

#[test]
fn exec_plan_motion_under_in_operator() {
    let sql = r#"SELECT "id" FROM "test_space" WHERE "id" in (SELECT "identification_number" FROM "hash_testing")"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    let mut virtual_table = virtual_table_23(None);
    reshard_vtable(&query, motion_id, &mut virtual_table);
    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "test_space"."id" FROM "test_space" WHERE "test_space"."id" in (SELECT "COL_1" FROM "TMP_test_0136")"#.to_string(),
            vec![]
        ));
}

#[test]
fn exec_plan_motion_under_except() {
    let sql = r#"
    SELECT "id" FROM "test_space"
    EXCEPT
    SELECT "identification_number" FROM "hash_testing"
    "#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    let mut virtual_table = virtual_table_23(None);
    reshard_vtable(&query, motion_id, &mut virtual_table);
    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "test_space"."id" FROM "test_space" EXCEPT SELECT "COL_1" FROM "TMP_test_0136""#.to_string(),
            vec![]
        ));
}

#[test]
fn exec_plan_subtree_count_asterisk() {
    let sql = r#"SELECT count(*) FROM "test_space""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(vcolumn_integer_user_non_null());
    reshard_vtable(&query, motion_id, &mut virtual_table);

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    if let MotionPolicy::Full = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Full for local aggregation stage");
    };

    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT count (*) as "count_1" FROM "test_space""#.to_string(),
            vec![]
        )
    );

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT sum ("COL_1") as "col_1" FROM (SELECT "COL_1" FROM "TMP_test_0136")"#
                .to_string(),
            vec![]
        )
    );
}

#[test]
fn exec_plan_subtree_having() {
    let sql = format!(
        "{} {} {}",
        r#"SELECT "T1"."sys_op" + "T1"."sys_op", count("T1"."sys_op"*2) + count(distinct "T1"."sys_op"*2)"#,
        r#"FROM "test_space" as "T1" group by "T1"."sys_op""#,
        r#"HAVING sum(distinct "T1"."sys_op"*2) > 1"#
    );
    let coordinator = RouterRuntimeMock::new();

    let mut query =
        ExecutingQuery::from_text_and_params(&coordinator, sql.as_str(), vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    reshard_vtable(&query, motion_id, &mut virtual_table);

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    if let MotionPolicy::Full = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Full for local aggregation stage");
    };
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT "T1"."sys_op" as "gr_expr_1", CAST (("T1"."sys_op" * CAST($1 AS int)) as int) as "gr_expr_2","#,
                r#"count (CAST (("T1"."sys_op" * CAST($2 AS int)) as int)) as "count_1" FROM "test_space" as "T1""#,
                r#"GROUP BY "T1"."sys_op", CAST (("T1"."sys_op" * CAST($3 AS int)) as int)"#,
            ),
            vec![Value::Integer(2), Value::Integer(2), Value::Integer(2)]
        )
    );

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {} {}",
                r#"SELECT "COL_1" + "COL_1" as "col_1","#,
                r#"sum ("COL_3") + count (DISTINCT "COL_2") as "col_2" FROM"#,
                r#"(SELECT "COL_1","COL_2","COL_3" FROM "TMP_test_0136")"#,
                r#"GROUP BY "COL_1" HAVING sum (DISTINCT "COL_2") > CAST($1 AS int)"#
            ),
            vec![Value::Integer(1)]
        )
    );
}

#[test]
fn exec_plan_subtree_having_without_groupby() {
    let sql = format!(
        "{} {} {}",
        r#"SELECT count("T1"."sys_op"*2) + count(distinct "T1"."sys_op"*2)"#,
        r#"FROM "test_space" as "T1""#,
        r#"HAVING sum(distinct "T1"."sys_op"*2) > 1"#
    );
    let coordinator = RouterRuntimeMock::new();

    let mut query =
        ExecutingQuery::from_text_and_params(&coordinator, sql.as_str(), vec![]).unwrap();
    let motion_id = query.get_motion_id(0, 0);
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_column(vcolumn_integer_user_non_null());
    reshard_vtable(&query, motion_id, &mut virtual_table);

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    if let MotionPolicy::Full = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Full after local stage");
    };

    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT CAST (("T1"."sys_op" * CAST($1 AS int)) as int) as "gr_expr_1","#,
                r#"count (CAST (("T1"."sys_op" * CAST($2 AS int)) as int)) as "count_1" FROM "test_space" as "T1""#,
                r#"GROUP BY CAST (("T1"."sys_op" * CAST($3 AS int)) as int)"#,
            ),
            vec![Value::Integer(2), Value::Integer(2), Value::Integer(2)]
        )
    );

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT sum ("COL_2") + count (DISTINCT "COL_1") as "col_1""#,
                r#"FROM (SELECT "COL_1","COL_2","COL_3" FROM "TMP_test_0136")"#,
                r#"HAVING sum (DISTINCT "COL_1") > CAST($1 AS int)"#,
            ),
            vec![Value::Integer(1)]
        )
    );
}

#[test]
fn global_table_scan() {
    let sql = r#"SELECT * from "global_t""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let exec_plan = query.get_mut_exec_plan();
    assert_eq!(Vec::<Slice>::new(), exec_plan.get_ir_plan().slices.slices);

    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let buckets = query.bucket_discovery(top_id).unwrap();
    assert_eq!(Buckets::Any, buckets);
    let exec_plan = query.get_mut_exec_plan();
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(r#"SELECT * FROM "global_t""#.to_string(), vec![])
    );
}

#[test]
fn global_union_all() {
    let sql = r#"SELECT "a", "b" from "global_t" union all select "e", "f" from "t2""#;
    let mut coordinator = RouterRuntimeMock::new();
    coordinator.set_vshard_mock(2);

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();
    let info = port.decode();
    assert_eq!(1, info.len());
    let DispatchInfo::Filtered(filtered) = info.get(0).unwrap() else {
        panic!("Expected a single dispatch with custom plan");
    };
    assert_eq!(2, filtered.len());

    let (sql, _, rs, _) = filtered.get(0).unwrap();
    assert_eq!(
        sql,
        r#" select cast(null as int),cast(null as int) where false UNION ALL SELECT "t2"."e", "t2"."f" FROM "t2""#
    );
    assert_eq!(rs, "replicaset_0");

    let (sql, _, rs, _) = filtered.get(1).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "global_t"."a", "global_t"."b" FROM "global_t" UNION ALL SELECT "t2"."e", "t2"."f" FROM "t2""#
    );
    assert_eq!(rs, "replicaset_1");
}

#[test]
fn global_union_all2() {
    // check that we don't send virtual table to replicasets, where
    // global child is not materialized.
    let sql = r#"SELECT "a", "b" from "global_t" where "b"
    in (select "e" from "t2")
    union all select "e", "f" from "t2""#;
    let mut coordinator = RouterRuntimeMock::new();
    coordinator.set_vshard_mock(3);

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();

    let motion_id = query.get_motion_id(0, 0);
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(vcolumn_integer_user_non_null());
    virtual_table.add_tuple(vec![Value::Integer(1)]);
    let exec_plan = query.get_mut_exec_plan();
    exec_plan
        .set_motion_vtable(&motion_id, virtual_table, &coordinator)
        .unwrap();

    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();

    let info = port.decode();
    assert_eq!(1, info.len());
    let DispatchInfo::Filtered(filtered) = info.get(0).unwrap() else {
        panic!("Expected a single dispatch with custom plan");
    };
    assert_eq!(3, filtered.len());

    let (sql, _, rs, _) = filtered.get(0).unwrap();
    assert_eq!(
        sql,
        r#" select cast(null as int),cast(null as int) where false UNION ALL SELECT "t2"."e", "t2"."f" FROM "t2""#,
    );
    assert_eq!(rs, "replicaset_0");

    let (sql, _, rs, _) = filtered.get(1).unwrap();
    assert_eq!(
        sql,
        r#" select cast(null as int),cast(null as int) where false UNION ALL SELECT "t2"."e", "t2"."f" FROM "t2""#,
    );
    assert_eq!(rs, "replicaset_1");

    let (sql, _, rs, _) = filtered.get(2).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "global_t"."a", "global_t"."b" FROM "global_t" WHERE "global_t"."b" in (SELECT "COL_1" FROM "TMP_test_0136") UNION ALL SELECT "t2"."e", "t2"."f" FROM "t2""#,
    );
    assert_eq!(rs, "replicaset_2");
}

#[test]
fn global_union_all3() {
    // check correct distribution on union all with group by
    let sql = r#"
    select "a" from "global_t" where "b"
    in (select "e" from "t2")
    union all
    select "f" from "t2"
    group by "f""#;
    let mut coordinator = RouterRuntimeMock::new();
    coordinator.set_vshard_mock(2);

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();

    let slices = query.exec_plan.get_ir_plan().clone_slices();
    let sq_motion_id = *slices.slice(0).unwrap().position(0).unwrap();
    let sq_motion_child = query
        .exec_plan
        .get_motion_subtree_root(sq_motion_id)
        .unwrap();

    // imitate plan execution
    query.exec_plan.take_subtree(sq_motion_child).unwrap();

    let mut sq_vtable = VirtualTable::new();
    sq_vtable.add_column(vcolumn_integer_user_non_null());
    sq_vtable.add_tuple(vec![Value::Integer(1)]);
    query
        .exec_plan
        .set_motion_vtable(&sq_motion_id, sq_vtable.clone(), &coordinator)
        .unwrap();

    let groupby_motion_id = *slices.slice(0).unwrap().position(1).unwrap();
    let groupby_motion_child = query
        .exec_plan
        .get_motion_subtree_root(groupby_motion_id)
        .unwrap();
    query.exec_plan.take_subtree(groupby_motion_child).unwrap();

    let mut groupby_vtable = VirtualTable::new();
    // these tuples must belong to different replicasets
    let tuple1 = vec![Value::Integer(3)];
    let tuple2 = vec![Value::Integer(2929)];
    groupby_vtable.add_column(vcolumn_integer_user_non_null());
    groupby_vtable.add_tuple(tuple1.clone());
    groupby_vtable.add_tuple(tuple2.clone());
    reshard_vtable(&query, groupby_motion_id, &mut groupby_vtable);
    query
        .exec_plan
        .set_motion_vtable(&groupby_motion_id, groupby_vtable.clone(), &coordinator)
        .unwrap();

    let top_id = query.exec_plan.get_ir_plan().get_top().unwrap();
    let buckets = query.bucket_discovery(top_id).unwrap();
    assert_eq!(Buckets::Any, buckets);
}

#[test]
fn global_union_all4() {
    let sql = r#"
    select "b" from "global_t"
    union all
    select * from (
        select "a" from "global_t"
        union all
        select "f" from "t2"
    )
    "#;
    let mut coordinator = RouterRuntimeMock::new();
    coordinator.set_vshard_mock(2);

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();

    let info = port.decode();
    assert_eq!(1, info.len());
    let DispatchInfo::Filtered(filtered) = info.first().unwrap() else {
        panic!(
            "{}",
            format!("Expected a single dispatch with custom plan, port: {info:?}")
        );
    };
    assert_eq!(2, filtered.len());

    let (sql, _, rs, _) = filtered.first().unwrap();
    assert_eq!(
        sql,
        r#" select cast(null as int) where false UNION ALL SELECT * FROM (select cast(null as int) where false UNION ALL SELECT "t2"."f" FROM "t2") as "unnamed_subquery""#,
    );
    assert_eq!(rs, "replicaset_0");

    let (sql, _, rs, _) = filtered.get(1).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "global_t"."b" FROM "global_t" UNION ALL SELECT * FROM (SELECT "global_t"."a" FROM "global_t" UNION ALL SELECT "t2"."f" FROM "t2") as "unnamed_subquery""#,
    );
    assert_eq!(rs, "replicaset_1");
}

#[test]
fn global_except() {
    let sql = r#"select "a" from "global_t"
    except select "e" from "t2""#;
    let mut coordinator = RouterRuntimeMock::new();
    coordinator.set_vshard_mock(3);

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();

    let intersect_motion_id = query.get_motion_id(0, 0);

    {
        // check map stage
        let motion_child = query
            .exec_plan
            .get_motion_subtree_root(intersect_motion_id)
            .unwrap();
        let sql = get_sql_from_execution_plan(
            &mut query.exec_plan,
            motion_child,
            Snapshot::Oldest,
            TEMPLATE,
        );
        assert_eq!(
            sql,
            PatternWithParams::new(
                r#"SELECT "t2"."e" FROM "t2" INTERSECT SELECT "global_t"."a" FROM "global_t""#
                    .to_string(),
                vec![]
            )
        );

        let mut virtual_table = VirtualTable::new();
        virtual_table.add_column(vcolumn_integer_user_non_null());
        virtual_table.add_tuple(vec![Value::Integer(1)]);
        query
            .get_mut_exec_plan()
            .set_motion_vtable(&intersect_motion_id, virtual_table.clone(), &coordinator)
            .unwrap();
    }

    // check reduce stage
    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();
    let info = port.decode();
    assert_eq!(1, info.len());
    let DispatchInfo::Any(sql, params) = info.get(0).unwrap() else {
        panic!("Expected a single dispatch to any node");
    };
    assert_eq!(
        sql,
        r#"SELECT "global_t"."a" FROM "global_t" EXCEPT SELECT "COL_1" FROM "TMP_test_0136""#,
    );
    assert_eq!(params, &vec![]);
}

#[test]
fn local_translation_asterisk_single() {
    let sql = r#"SELECT * from "t3""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let exec_plan = query.get_mut_exec_plan();
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(r#"SELECT "t3"."a", "t3"."b" FROM "t3""#.to_string(), vec![])
    );
}

#[test]
fn local_translation_asterisk_several() {
    let sql = r#"SELECT *, * from "t3""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let exec_plan = query.get_mut_exec_plan();
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "t3"."a", "t3"."b", "t3"."a", "t3"."b" FROM "t3""#.to_string(),
            vec![]
        )
    );
}

#[test]
fn local_translation_asterisk_named() {
    let sql = r#"SELECT *, "t3".*, * from "t3""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let exec_plan = query.get_mut_exec_plan();
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "t3"."a", "t3"."b", "t3"."a", "t3"."b", "t3"."a", "t3"."b" FROM "t3""#
                .to_string(),
            vec![]
        )
    );
}

#[test]
fn local_translation_asterisk_with_additional_columns() {
    let sql = r#"SELECT "a", *, "t3"."b", "t3".*, * from "t3""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let exec_plan = query.get_mut_exec_plan();
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "t3"."a", "t3"."a", "t3"."b", "t3"."b", "t3"."a", "t3"."b", "t3"."a", "t3"."b" FROM "t3""#.to_string(),
            vec![]
        )
    );
}

#[test]
fn exec_plan_order_by() {
    let sql = r#"SELECT "identification_number" from "hash_testing"
                      ORDER BY "identification_number""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = virtual_table_23(Some("hash_testing"));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }
    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check sub-query
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "hash_testing"."identification_number" FROM "hash_testing""#.to_string(),
            vec![]
        )
    );

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "COL_1" as "identification_number" FROM (SELECT "COL_1" FROM "TMP_test_0136") as "hash_testing" ORDER BY "COL_1""#.to_string(),
            vec![]
        ));
}

#[test]
fn exec_plan_order_by_with_subquery() {
    let sql = r#"SELECT "identification_number"
                      FROM (select "identification_number" from "hash_testing")
                      ORDER BY "identification_number""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = virtual_table_23(Some("hash_testing"));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }
    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check sub-query
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "unnamed_subquery"."identification_number" FROM (SELECT "hash_testing"."identification_number" FROM "hash_testing") as "unnamed_subquery""#.to_string(),
            vec![]
        )
    );

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "COL_1" as "identification_number" FROM (SELECT "COL_1" FROM "TMP_test_0136") as "hash_testing" ORDER BY "COL_1""#.to_string(),
            vec![]
        ));
}

#[test]
fn exec_plan_order_by_with_join() {
    let sql = r#"SELECT * FROM
                      (SELECT "a" FROM "t") AS "f"
                      JOIN
                      (SELECT "a" FROM "t") AS "s"
                      ON true
                      ORDER BY 1"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]).unwrap();
    let slices = query.exec_plan.get_ir_plan().clone_slices();
    let mut sq_vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();

    let sq_motion_id = *slices.slice(0).unwrap().position(0).unwrap();
    let mut sq_vtable = VirtualTable::new();
    sq_vtable.add_column(vcolumn_integer_user_non_null());
    sq_vtable.add_tuple(vec![Value::Integer(1)]);
    sq_vtable.set_alias("s");
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.exec_plan.get_ir_plan(), sq_motion_id)
    {
        sq_vtable.reshard(key, &query.coordinator).unwrap();
    }
    sq_vtables.insert(sq_motion_id, Rc::new(sq_vtable));

    let order_by_motion_id = *slices.slice(1).unwrap().position(0).unwrap();
    let mut order_by_vtable = VirtualTable::new();
    order_by_vtable.add_column(vcolumn_integer_user_non_null());
    order_by_vtable.add_column(vcolumn_integer_user_non_null());
    order_by_vtable.add_tuple(vec![Value::Integer(1), Value::Integer(2)]);
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.exec_plan.get_ir_plan(), sq_motion_id)
    {
        order_by_vtable.reshard(key, &query.coordinator).unwrap();
    }
    sq_vtables.insert(order_by_motion_id, Rc::new(order_by_vtable));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(sq_vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    // Check sub-query.
    let sq_motion_child_id = exec_plan.get_motion_subtree_root(sq_motion_id).unwrap();
    let sql =
        get_sql_from_execution_plan(exec_plan, sq_motion_child_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(r#"SELECT "t"."a" FROM "t""#.to_string(), vec![])
    );

    // Check order by subtree.
    let order_by_motion_child_id = exec_plan
        .get_motion_subtree_root(order_by_motion_id)
        .unwrap();
    let sql = get_sql_from_execution_plan(
        exec_plan,
        order_by_motion_child_id,
        Snapshot::Oldest,
        TEMPLATE,
    );
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT * FROM (SELECT "t"."a" FROM "t") as "f" INNER JOIN (SELECT "COL_1" FROM "TMP_test_0136") as "s" ON CAST($1 AS bool)"#.to_string(),
            vec![Value::Boolean(true)]
        )
    );

    // Check main query.
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "COL_1" as "a", "COL_2" as "a" FROM (SELECT "COL_1","COL_2" FROM "TMP_test_0136") ORDER BY 1"#.to_string(),
            vec![]
        ));
}

fn check_subtree_hashes_are_equal(
    sql1: &str,
    values1: Vec<Value>,
    sql2: &str,
    values2: Vec<Value>,
) {
    let coordinator = RouterRuntimeMock::new();
    let get_hash = |sql: &str, values: Vec<Value>| -> SmolStr {
        let mut query = ExecutingQuery::from_text_and_params(&coordinator, sql, values).unwrap();
        query
            .get_mut_exec_plan()
            .get_mut_ir_plan()
            .stash_constants(Snapshot::Oldest)
            .unwrap();
        let ir = query.get_exec_plan().get_ir_plan();
        let top = ir.get_top().unwrap();
        ir.pattern_id(top).unwrap()
    };

    assert_eq!(get_hash(sql1, values1), get_hash(sql2, values2));
}

fn check_subtree_hashes_are_equal_2(
    sql1: &str,
    values1: Vec<Value>,
    sql2: &str,
    values2: Vec<Value>,
) {
    let get_hash = |sql: &str, values: Vec<Value>| -> SmolStr {
        let plan = sql_to_optimized_ir(sql, values);
        let top = plan.get_top().unwrap();
        let mut exec_plan = ExecutionPlan::from(plan.clone());
        let subplan = exec_plan.take_subtree(top).unwrap();
        subplan.get_ir_plan().pattern_id(top).unwrap()
    };

    assert_eq!(get_hash(sql1, values1), get_hash(sql2, values2));
}

#[test]
fn subtree_hash1() {
    check_subtree_hashes_are_equal(
        r#"select ?, ? from "t""#,
        vec![Value::Integer(1), Value::Integer(1)],
        r#"select $1, $2 from "t""#,
        vec![Value::Integer(1), Value::Integer(1)],
    );
}

#[test]
fn subtree_hash2() {
    check_subtree_hashes_are_equal(
        r#"select ?, ? from "t"
        option(sql_vdbe_opcode_max = ?, sql_motion_row_max = ?)"#,
        vec![
            Value::Integer(1),
            Value::Integer(11),
            Value::Integer(3),
            Value::Integer(10),
        ],
        r#"select $1, $2 from "t"
        option(sql_vdbe_opcode_max = $3, sql_motion_row_max = $4)"#,
        vec![
            Value::Integer(1),
            Value::Integer(11),
            Value::Integer(3),
            Value::Integer(10),
        ],
    );
}

#[test]
fn check_parentheses() {
    let query = r#"SELECT "id" from "test_space" WHERE "sysFrom" = ((1) + (3)) + 2"#;

    let rt = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&rt, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top_id = plan.get_top().unwrap();

    let expected = PatternWithParams::new(
        r#"SELECT "test_space"."id" FROM "test_space" WHERE "test_space"."sysFrom" = ((CAST($1 AS int) + CAST($2 AS int)) + CAST($3 AS int))"#.to_string(),
        vec![Value::from(1), Value::from(3), Value::from(2)],
    );

    assert_eq!(
        expected,
        get_sql_from_execution_plan(
            query.get_mut_exec_plan(),
            top_id,
            Snapshot::Oldest,
            TEMPLATE
        )
    );
}

/* FIXME: https://git.picodata.io/picodata/picodata/sbroad/-/issues/583
#[test]
fn subtree_hash3() {
    check_subtree_hashes_are_equal(
        r#"select ?, ? from "t"
        option(sql_vdbe_opcode_max = ?)"#,
        vec![Value::Integer(1), Value::Integer(11), Value::Integer(10)],
        r#"select ?, ? from "t""#,
        vec![Value::Integer(1), Value::Integer(1)],
    );
}
*/

#[test]
fn subtree_hash4() {
    check_subtree_hashes_are_equal_2(
        r#"VALUES (1)"#,
        vec![],
        r#"VALUES (?)"#,
        vec![Value::Integer(1)],
    );
}

#[test]
fn subtree_hash5() {
    check_subtree_hashes_are_equal_2(
        r#"VALUES (-1)"#,
        vec![],
        r#"VALUES (?)"#,
        vec![Value::Integer(-1)],
    );
}

#[test]
fn subtree_hash6() {
    check_subtree_hashes_are_equal_2(
        r#"VALUES ('abc')"#,
        vec![],
        r#"VALUES (?)"#,
        vec![Value::String("abc".to_string())],
    );
}

#[test]
fn subtree_hash7() {
    check_subtree_hashes_are_equal_2(
        r#"VALUES (0, True, 'abc')"#,
        vec![],
        r#"VALUES ($1, $2, $3)"#,
        vec![
            Value::Integer(0),
            Value::Boolean(true),
            Value::String("abc".to_string()),
        ],
    );
}

#[test]
fn take_subtree_projection_windows_transfer() {
    // Description: When take_tree was called, window references in the projection were not
    // transferred to the new tree.
    //
    // If the projection's window field pointed to a window with index 0, and we added a motion
    // with index 1, then when we took the tree, the tree was traversed in post-order. This
    // caused the motion to receive index 0 and the window to receive index 1. The test
    // verifies that the window reference is now properly transferred.
    let mut plan = Plan::default();

    let a_value = plan.nodes.add_const(1.into());
    let a = plan.nodes.add_alias("a", a_value).unwrap();
    let b_value = plan.nodes.add_const(2.into());
    let b = plan.nodes.add_alias("b", b_value).unwrap();
    let scan_relation = plan.add_select_without_scan(&[a, b]).unwrap();
    let window = {
        let ordering = [0, 1]
            .iter()
            .map(|n| {
                let new_ref = plan.nodes.add_ref(
                    ReferenceTarget::Single(scan_relation),
                    *n,
                    DerivedType::new(Type::Integer),
                    None,
                    false,
                );
                OrderByElement {
                    entity: OrderByEntity::Expression { expr_id: new_ref },
                    order_type: None,
                }
            })
            .collect();
        let window = Window {
            partition: None,
            ordering: Some(ordering),
            frame: None,
        };
        plan.nodes.push(window.into())
    };
    let stable_func_id = {
        let r#ref = plan.nodes.add_ref(
            ReferenceTarget::Single(scan_relation),
            1,
            DerivedType::new(Type::Integer),
            None,
            false,
        );
        let cast = plan.add_cast(r#ref, CastType::String).unwrap();
        let r#const = plan.nodes.add_const(".".into());
        plan.add_builtin_window_function("group_concat".into(), vec![cast, r#const])
            .unwrap()
    };
    let over = {
        let over = Over {
            stable_func: stable_func_id,
            window,
            filter: None,
        };

        plan.nodes.push(over.into())
    };
    let col_1 = plan.nodes.add_alias("col_1", over).unwrap();

    let projection = {
        let output = plan.nodes.add_row(vec![col_1], None);

        let projection = Projection {
            children: vec![scan_relation],
            windows: vec![window],
            output,
            is_distinct: false,
            group_by: None,
            having: None,
        };

        plan.nodes.push(projection.into())
    };

    plan.set_top(projection).unwrap();

    // apply motion

    let motion = {
        let motion_refs = [0, 1]
            .iter()
            .map(|n| {
                let new_ref = plan.nodes.add_ref(
                    ReferenceTarget::Single(scan_relation),
                    *n,
                    DerivedType::new(Type::Integer),
                    None,
                    false,
                );
                plan.nodes
                    .add_alias(format!("col_{n}").as_str(), new_ref)
                    .unwrap()
            })
            .collect();
        let motion_output = plan.nodes.add_row(motion_refs, None);

        let motion = Motion {
            alias: None,
            child: Some(scan_relation),
            policy: MotionPolicy::Full,
            program: Program::new(vec![MotionOpcode::ReshardIfNeeded]),
            output: motion_output,
        };
        plan.add_relational(motion.into()).unwrap()
    };

    plan.change_child(projection, scan_relation, motion)
        .unwrap();
    plan.replace_target_in_relational(projection, scan_relation, motion)
        .unwrap();

    {
        let Node::Relational(Relational::Projection(Projection { windows, .. })) =
            plan.get_node(projection).unwrap()
        else {
            panic!("should be projection")
        };

        let Some(window) = windows.get(0) else {
            panic!("should be at least one window")
        };

        assert_eq!(
            *window,
            NodeId {
                offset: 0,
                arena_type: ArenaType::Arena136
            }
        );
    }

    let mut execution_plan: ExecutionPlan = plan.into();

    let new_plan = execution_plan.take_subtree(projection).unwrap();

    let new_top = new_plan.get_ir_plan().get_top().unwrap();

    {
        let Node::Relational(Relational::Projection(Projection { windows, .. })) =
            new_plan.get_ir_plan().get_node(new_top).unwrap()
        else {
            panic!("should be projection")
        };

        let Some(window) = windows.get(0) else {
            panic!("should be at least one window")
        };

        assert_eq!(
            *window,
            NodeId {
                offset: 1,
                arena_type: ArenaType::Arena136
            }
        );
    }
}
