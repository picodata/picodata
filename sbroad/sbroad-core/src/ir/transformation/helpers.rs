//! IR test helpers.

use crate::backend::sql::ir::PatternWithParams;
use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::executor::engine::mock::RouterConfigurationMock;
use crate::executor::ir::ExecutionPlan;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::options::Options;
use crate::ir::tree::Snapshot;
use crate::ir::types::DerivedType;
use crate::ir::value::Value;
use crate::ir::Plan;

/// Compiles an SQL query to optimized IR plan.
///
/// # Panics
///   if query is not correct
#[track_caller]
#[must_use]
#[allow(clippy::missing_panics_doc)]
pub fn sql_to_optimized_ir(query: &str, params: Vec<Value>) -> Plan {
    sql_to_ir(query, params)
        .optimize()
        .unwrap()
        .update_timestamps()
        .unwrap()
        .cast_constants()
        .unwrap()
}

/// Compiles an SQL query to IR plan.
///
/// # Panics
///   if query is not correct
#[track_caller]
#[must_use]
pub fn sql_to_ir(query: &str, params: Vec<Value>) -> Plan {
    let params_types: Vec<_> = params.iter().map(|v| v.get_type()).collect();
    let mut plan = sql_to_ir_without_bind(query, &params_types);
    plan.bind_params(&params, Options::default()).unwrap();
    plan
}

#[track_caller]
pub fn sql_to_ir_without_bind(query: &str, params_types: &[DerivedType]) -> Plan {
    let metadata = &RouterConfigurationMock::new();
    AbstractSyntaxTree::transform_into_plan(query, params_types, metadata).unwrap()
}

/// Compiles and transforms an SQL query to a new parameterized SQL.
#[allow(dead_code)]
#[track_caller]
pub fn check_transformation(
    query: &str,
    params: Vec<Value>,
    f_transform: &dyn Fn(Plan) -> Plan,
) -> PatternWithParams {
    let mut plan = sql_to_ir(query, params);
    plan = f_transform(plan);
    let mut ex_plan = ExecutionPlan::from(plan);
    let top_id = ex_plan.get_ir_plan().get_top().unwrap();

    ex_plan
        .get_mut_ir_plan()
        .stash_constants(Snapshot::Latest)
        .unwrap();

    let sp = SyntaxPlan::new(&ex_plan, top_id, Snapshot::Latest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let (sql, _) = ex_plan.to_sql(&nodes, "", None).unwrap();
    sql
}
