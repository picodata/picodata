//! IR test helpers.
//!
//! The actual implementations live in `sql_executor::test_helpers` so that
//! the sub-crates' test suites can use them without depending on this
//! facade crate. This module re-exports them under the historical
//! `sql::helpers` paths.

pub use sql_executor::test_helpers::{
    check_transformation, column_integer_user_non_null, column_user_non_null,
    expect_sql_to_ir_error, get_motion_id, sharding_column, sql_to_ir, sql_to_ir_without_bind,
    sql_to_optimized_ir, vcolumn_integer_user_non_null, vcolumn_user_non_null,
};
