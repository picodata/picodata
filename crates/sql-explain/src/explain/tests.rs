//! Tests of the EXPLAIN rendering produced by this crate.
//!
//! Suites are named after the facet they test:
//! - `logical`: the LOGICAL facet;
//! - `buckets`: the BUCKETS facet, both estimation and rendering;
//! - `explain`: facet dispatch and tests combining several facets.
//!
//! EXPLAIN options such as `FMT` are tested in the suite of the facet they
//! apply to. SQL queries are explained through the mock dispatcher, so both
//! the query and the mock router are taken from the `sql_executor`
//! dev-dependency instance. Rendering tests build bucket sets directly.
use crate::explain::ExplainExecutingQuery;
use sql_executor::executor::engine::mock::RouterRuntimeMock;
use sql_executor::executor::ExecutingQuery;
use sql_executor::test_helpers::ExecutingQueryExt;
use sql_ir::ir::value::Value;

mod buckets;
mod explain;
mod logical;

fn explain(sql: &str) -> String {
    explain_with_params(sql, vec![])
}

fn explain_with_params(sql: &str, params: Vec<Value>) -> String {
    let metadata = &RouterRuntimeMock::new();
    let query = ExecutingQuery::from_text_and_params(metadata, sql, params).unwrap();
    ExplainExecutingQuery::from(query).explain().unwrap()
}
