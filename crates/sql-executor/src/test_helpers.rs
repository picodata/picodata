//! IR test helpers.
//!
//! Shared SQL-parsing test machinery for the sql-ir, sql-executor and
//! sql-frontend test suites. Compiled only with the `mock` feature, which
//! pulls in `sql-frontend` as an optional dependency; production builds
//! never see this module.

use crate::backend::sql::ir::PatternWithParams;
use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::errors::SbroadError;
use crate::executor::engine::helpers::table_name;
use crate::executor::engine::mock::RouterConfigurationMock;
use crate::executor::engine::Router;
use crate::executor::ir::ExecutionPlan;
use crate::executor::ExecutingQuery;
use crate::ir::options::Options;
use crate::ir::tree::Snapshot;
use crate::ir::types::DerivedType;
use crate::ir::value::Value;
use crate::ir::Plan;
use crate::utils::MutexLike;
use sql_frontend::frontend::sql::transform_into_plan;

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
    plan.bind_params(params, Options::default()).unwrap();
    plan
}

#[track_caller]
pub fn sql_to_ir_without_bind(query: &str, params_types: &[DerivedType]) -> Plan {
    let metadata = &RouterConfigurationMock::new();
    transform_into_plan(query, params_types, metadata).unwrap()
}

#[track_caller]
pub fn expect_sql_to_ir_error(query: &str, params_types: &[DerivedType]) -> SbroadError {
    let metadata = &RouterConfigurationMock::new();
    transform_into_plan(query, params_types, metadata).unwrap_err()
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
    let ex_plan = ExecutionPlan::new(plan);
    let top_id = ex_plan.get_ir_plan().get_top().unwrap();

    let params = ex_plan
        .local_sql_params(top_id, Snapshot::Latest)
        .expect("local sql params");

    let sp = SyntaxPlan::new(&ex_plan, top_id, Snapshot::Latest, false).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let sql = ex_plan
        .generate_sql(&nodes, 0, table_name, Some(params.constant_ids()))
        .unwrap();
    PatternWithParams::new(sql, params.params().to_vec())
}

use crate::executor::vtable::VTableColumn;
use crate::ir::relation::ColumnRole;
pub use crate::ir::test_fixtures::{
    column_integer_user_non_null, column_user_non_null, get_motion_id, sharding_column,
};
use crate::ir::types::UnrestrictedType;

pub fn vcolumn_integer_user_non_null() -> VTableColumn {
    VTableColumn {
        r#type: DerivedType::new(UnrestrictedType::Integer),
        role: ColumnRole::User,
        is_nullable: false,
    }
}

pub fn vcolumn_user_non_null(r#type: UnrestrictedType) -> VTableColumn {
    VTableColumn {
        r#type: DerivedType::new(r#type),
        role: ColumnRole::User,
        is_nullable: false,
    }
}

/// Test-only constructor of [`ExecutingQuery`] from SQL text.
///
/// A mock twin of the trait of the same name in the `sql-planner` facade:
/// runs the same [`Plan::optimize_statement`] + [`Plan::bind_statement`]
/// pipeline as `PreparedStatement::parse` + `PreparedStatement::bind`,
/// minus the plan cache, audit logging and table-version maps (the mock
/// router does not provide versions).
pub trait ExecutingQueryExt<'a, C>: Sized
where
    C: Router,
{
    /// Create a query ready for execution directly from SQL text.
    fn from_text_and_params(
        coordinator: &'a C,
        query_text: &str,
        params: Vec<Value>,
    ) -> Result<Self, SbroadError>;
}

impl<'a, C> ExecutingQueryExt<'a, C> for ExecutingQuery<'a, C>
where
    C: Router,
{
    fn from_text_and_params(
        coordinator: &'a C,
        query_text: &str,
        params: Vec<Value>,
    ) -> Result<Self, SbroadError> {
        let param_types: Vec<_> = params.iter().map(|v| v.get_type()).collect();

        let plan = {
            let metadata = coordinator.metadata().lock();
            transform_into_plan(query_text, &param_types, &*metadata)?
        };

        let plan = plan
            .optimize_statement()?
            .bind_statement(params, Options::default())?;

        Ok(ExecutingQuery::from_plan(coordinator, plan))
    }
}
