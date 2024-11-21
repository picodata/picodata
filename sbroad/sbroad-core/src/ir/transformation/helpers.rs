//! IR test helpers.

use crate::backend::sql::ir::PatternWithParams;
use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::executor::engine::mock::RouterConfigurationMock;
use crate::executor::ir::ExecutionPlan;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;
use crate::ir::Plan;

/// Compiles an SQL query to optimized IR plan.
///
/// # Panics
///   if query is not correct
#[must_use]
#[allow(clippy::missing_panics_doc)]
pub fn sql_to_optimized_ir(query: &str, params: Vec<Value>) -> Plan {
    let mut plan = sql_to_ir(query, params);
    plan.optimize().unwrap();
    plan
}

/// Compiles an SQL query to IR plan.
///
/// # Panics
///   if query is not correct
#[must_use]
pub fn sql_to_ir(query: &str, params: Vec<Value>) -> Plan {
    let mut plan = sql_to_ir_without_bind(query);
    plan.bind_params(params).unwrap();
    plan.apply_options().unwrap();
    plan
}

pub fn sql_to_ir_without_bind(query: &str) -> Plan {
    let metadata = &RouterConfigurationMock::new();
    AbstractSyntaxTree::transform_into_plan(query, metadata).unwrap()
}

/// Compiles and transforms an SQL query to a new parameterized SQL.
///
/// # Panics
///   if query is not correct
#[allow(dead_code)]
pub fn check_transformation(
    query: &str,
    params: Vec<Value>,
    f_transform: &dyn Fn(&mut Plan),
) -> PatternWithParams {
    let mut plan = sql_to_ir(query, params);
    f_transform(&mut plan);
    let ex_plan = ExecutionPlan::from(plan);
    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sp = SyntaxPlan::new(&ex_plan, top_id, Snapshot::Latest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let (sql, _) = ex_plan.to_sql(&nodes, "", None).unwrap();
    sql
}
