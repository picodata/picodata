//! Tests of the EXPLAIN rendering produced by this crate.
//!
//! End-to-end suites (`query_explain`, `cast_constants`) explain queries
//! through the mock dispatcher, so both the query and the mock router are
//! taken from the `sql_executor` dev-dependency instance. The remaining
//! suites build plans with `sql_executor::test_helpers` and render them
//! with the context-free entry points of this crate.
mod cast_constants;
mod concat;
mod delete;
mod explain;
mod query_explain;
