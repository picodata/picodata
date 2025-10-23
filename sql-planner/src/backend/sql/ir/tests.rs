use pretty_assertions::assert_eq;

use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};

use crate::executor::ir::ExecutionPlan;

use crate::ir::transformation::helpers::sql_to_ir;
use crate::ir::tree::Snapshot;

use super::*;

#[allow(clippy::needless_pass_by_value)]
#[track_caller]
fn check_sql_with_snapshot(
    query: &str,
    params: Vec<Value>,
    expected: PatternWithParams,
    snapshot: Snapshot,
) {
    let plan = sql_to_ir(query, params)
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
        .unwrap();
    let mut ex_plan = ExecutionPlan::from(plan);
    let top_id = ex_plan.get_ir_plan().get_top().unwrap();

    ex_plan.get_mut_ir_plan().stash_constants(snapshot).unwrap();

    let sp = SyntaxPlan::new(&ex_plan, top_id, snapshot).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let (sql, _) = ex_plan.to_sql(&nodes, "test", None).unwrap();

    assert_eq!(expected, sql,);
}

mod except;
mod inner_join;
mod projection;
mod selection;
mod sub_query;
mod union_all;
