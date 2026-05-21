use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};

use crate::executor::engine::helpers::table_name;
use crate::executor::ir::ExecutionPlan;

use crate::ir::node::expression::Expression;
use crate::ir::node::Node;
use crate::ir::transformation::helpers::sql_to_ir;
use crate::ir::tree::Snapshot;

use super::*;

#[allow(clippy::needless_pass_by_value)]
#[track_caller]
fn check_sql_with_snapshot(
    query: &str,
    params: Vec<Value>,
    snapshot: Snapshot,
) -> PatternWithParams {
    let plan = sql_to_ir(query, params)
        .replace_in_operator()
        .unwrap()
        .push_down_not()
        .unwrap()
        .split_columns()
        .unwrap()
        .set_dnf()
        .unwrap()
        .analyze_equality_facts()
        .unwrap()
        .derive_equalities()
        .unwrap()
        .merge_tuples()
        .unwrap();
    let ex_plan = ExecutionPlan::new(plan);
    let top_id = ex_plan.get_ir_plan().get_top().unwrap();

    let params = ex_plan.local_sql_params(top_id, snapshot).unwrap();

    let sp = SyntaxPlan::new(&ex_plan, top_id, snapshot, false).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let sql = ex_plan
        .generate_sql(&nodes, 0, table_name, Some(params.constant_ids().to_vec()))
        .unwrap();

    PatternWithParams::new(sql, params.params().to_vec())
}

#[test]
fn local_sql_params_do_not_mutate_constants() {
    let plan = sql_to_ir(
        r#"SELECT "FIRST_NAME" FROM "test_space" WHERE "id" = 1"#,
        vec![],
    );
    let ex_plan = ExecutionPlan::new(plan);
    let top_id = ex_plan.get_ir_plan().get_top().unwrap();

    let params = ex_plan
        .local_sql_params(top_id, Snapshot::Latest)
        .expect("local sql params");
    assert_eq!(params.params(), &[Value::from(1)]);

    let sp = SyntaxPlan::new(&ex_plan, top_id, Snapshot::Latest, false).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let sql = ex_plan
        .generate_sql(&nodes, 0, table_name, Some(params.constant_ids().to_vec()))
        .unwrap();

    assert!(sql.contains("CAST($1 AS int)"));
    for id in params.constant_ids() {
        assert!(matches!(
            ex_plan.get_ir_plan().get_node(*id).unwrap(),
            Node::Expression(Expression::Constant(_))
        ));
    }
}

mod except;
mod inner_join;
mod projection;
mod selection;
mod sub_query;
mod union_all;
