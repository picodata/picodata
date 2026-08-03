//! Tests for block VDBE pattern keys that need the full parse -> bind
//! pipeline (`PreparedStatement`), which lives in this umbrella crate.

use sql::backend::sql::ir::block_pattern_key;
use sql::executor::engine::helpers::generate_pattern_with_params_for_block;
use sql::executor::engine::mock::RouterRuntimeMock;
use sql::executor::engine::BlockRuntimeHook;
use sql::executor::ExecutingQuery;
use sql::ir::node::block::BlockOwned;
use sql::ir::node::BlockStatement;
use sql::ir::options::Options;
use sql::ir::value::Value;
use sql::{ExecutingQueryExt as _, PreparedStatement};

fn get_block_pattern_key_from_query(query: &mut ExecutingQuery<RouterRuntimeMock>) -> u64 {
    let exec_plan = query.get_mut_exec_plan();
    let table_id = exec_plan.get_ir_plan().relations.get("t").unwrap().id;
    exec_plan
        .get_mut_ir_plan()
        .table_version_map
        .insert(table_id, 1);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let BlockOwned::Anonymous(block) = exec_plan
        .get_ir_plan()
        .get_owned_block_node(top_id)
        .unwrap()
    else {
        panic!("expected anonymous block");
    };
    block_pattern_key(exec_plan, &block.statements).unwrap()
}

#[test]
fn block_pattern_key_hashes_insert_do_update_param_rhs_by_type() {
    let sql = r#"do $$ begin
           insert into "t" values (1, 1, 1, 1)
               on conflict ("a") do update set "c" = "c" + $1;
           end $$"#;
    let coordinator = RouterRuntimeMock::new();
    let prepared =
        PreparedStatement::parse(&coordinator, sql, &[Value::Integer(1).get_type()]).unwrap();
    let mut query1 = ExecutingQuery::from_bound_statement(
        &coordinator,
        prepared
            .bind(vec![Value::Integer(1)], Options::default())
            .unwrap(),
    );
    let mut query2 = ExecutingQuery::from_bound_statement(
        &coordinator,
        prepared
            .bind(vec![Value::Integer(2)], Options::default())
            .unwrap(),
    );
    let key1 = get_block_pattern_key_from_query(&mut query1);
    let key2 = get_block_pattern_key_from_query(&mut query2);
    assert_eq!(key1, key2);
}

#[test]
fn raw_explain_block_bind_insert_do_update_param_rhs() {
    let sql = r#"explain (raw) do $$ begin
           insert into "t" values (1, 1, 1, 1)
               on conflict ("a") do update set "c" = "c" + $1;
           end $$"#;
    let iocdu_param = Value::Integer(777);
    let coordinator = RouterRuntimeMock::new();
    let prepared = PreparedStatement::parse(&coordinator, sql, &[iocdu_param.get_type()]).unwrap();
    let mut query = ExecutingQuery::from_bound_statement(
        &coordinator,
        prepared
            .bind(vec![iocdu_param.clone()], Options::default())
            .unwrap(),
    );
    let exec_plan = query.get_mut_exec_plan();
    assert!(exec_plan.get_ir_plan().is_raw_explain());
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let BlockOwned::Anonymous(block) = exec_plan
        .get_ir_plan()
        .get_owned_block_node(top_id)
        .unwrap()
    else {
        panic!("expected anonymous block");
    };
    let query_id = match block.statements.first() {
        Some(BlockStatement::Query(query_id)) => *query_id,
        other => panic!("expected block query, got {other:?}"),
    };

    let (query, params) =
        generate_pattern_with_params_for_block(exec_plan, query_id, Some(1), false).unwrap();

    assert_eq!(query.hooks.len(), 1);
    let BlockRuntimeHook::IdxInsertOnConflictDoUpdate {
        raw_explain_detail, ..
    } = &query.hooks[0];
    assert_eq!(
        raw_explain_detail.as_deref(),
        Some(r#"picodata: ON CONFLICT ("a") UPDATE "c" += 777"#)
    );
    assert!(!params.contains(&iocdu_param));
}
