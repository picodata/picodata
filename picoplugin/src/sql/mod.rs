//! Picodata SQL API.

use crate::internal::ffi;
use crate::sql::types::SqlValue;
use abi_stable::derive_macro_reexports::RResult;
use abi_stable::std_types::{ROk, RVec};
use tarantool::error::BoxError;
use tarantool::tuple::Tuple;

pub mod types;

// TODO should sql query accept a timeout?
/// Execute SQL Query.
///
/// # Arguments
///
/// * `query`: query string
/// * `params`: query params - list of SQL values
///
/// # Examples
///
/// ```no_run
/// # use picoplugin::sql::query;
/// # use picoplugin::sql::types::SqlValue;
///  query(
///     "INSERT INTO book (id, name) VALUES (?, ?)",
///     vec![
///         SqlValue::unsigned(1),
///         SqlValue::string("Ruslan and Ludmila"),
///     ],
///  )
///  .unwrap();
/// ```
pub fn query(query: &str, params: Vec<SqlValue>) -> Result<Tuple, BoxError> {
    let query_len = query.len();
    let query_ptr = query.as_ptr();

    match unsafe { ffi::pico_ffi_sql_query(query_ptr, query_len, RVec::from(params)) } {
        ROk(ptr) => Ok(Tuple::try_from_ptr(ptr).expect("should not be null")),
        RResult::RErr(_) => Err(BoxError::last()),
    }
}
