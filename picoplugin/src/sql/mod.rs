//! Picodata SQL API.

use crate::internal::ffi;
use crate::sql::types::SqlValue;
use abi_stable::derive_macro_reexports::RResult;
use abi_stable::std_types::{ROk, RVec};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::HashMap;
use tarantool::error::{BoxError, IntoBoxError, TarantoolErrorCode};
use tarantool::tuple::Tuple;

pub mod types;

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
/// # use picoplugin::sql::query_raw;
/// # use picoplugin::sql::types::SqlValue;
///  query_raw(
///     "INSERT INTO book (id, name) VALUES (?, ?)",
///     vec![
///         SqlValue::unsigned(1),
///         SqlValue::string("Ruslan and Ludmila"),
///     ],
///  )
///  .unwrap();
/// ```
pub fn query_raw(query: &str, params: Vec<SqlValue>) -> Result<Tuple, BoxError> {
    let query_len = query.len();
    let query_ptr = query.as_ptr();

    match unsafe { ffi::pico_ffi_sql_query(query_ptr, query_len, RVec::from(params)) } {
        ROk(ptr) => Ok(Tuple::try_from_ptr(ptr).expect("should not be null")),
        RResult::RErr(_) => Err(BoxError::last()),
    }
}

pub struct Query<'a> {
    query: &'a str,
    params: Vec<SqlValue>,
}

impl<'a> Query<'a> {
    /// Bind a value for use with this SQL query.
    #[inline(always)]
    pub fn bind<T: Into<SqlValue>>(mut self, value: T) -> Self {
        self.params.push(value.into());
        self
    }

    /// Execute the query and return the total number of rows affected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use picoplugin::sql::query;
    ///  let inserted: u64 = query("INSERT INTO book (id, name) VALUES (?, ?)")
    ///     .bind(1)
    ///     .bind("Ruslan and Ludmila")
    ///     .execute()
    ///     .unwrap();
    /// assert_eq!(inserted, 1);
    /// ```
    pub fn execute(self) -> Result<u64, BoxError> {
        let tuple = query_raw(self.query, self.params)?;
        #[derive(Deserialize)]
        struct Output {
            row_count: u64,
        }

        let result = tuple
            .decode::<Vec<Output>>()
            .map_err(|tt| tt.into_box_error())?;

        let result = result.first().ok_or_else(|| {
            BoxError::new(
                TarantoolErrorCode::InvalidMsgpack,
                "sql result should contains at least one row",
            )
        })?;

        Ok(result.row_count)
    }

    /// Execute the query and return list of selected values.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use picoplugin::sql::query;
    /// # use serde::Deserialize;
    /// #[derive(Deserialize, Debug, PartialEq)]
    /// struct Book {
    ///     id: u64,
    ///     name: String,
    /// }
    /// let books = query("SELECT * from book").fetch::<Book>().unwrap();
    /// assert_eq!(&books, &[Book { id: 1, name: "Ruslan and Ludmila".to_string()}]);
    /// ```
    pub fn fetch<T: DeserializeOwned>(self) -> Result<Vec<T>, BoxError> {
        let tuple = query_raw(self.query, self.params)?;

        let mut res = tuple
            .decode::<Vec<HashMap<String, rmpv::Value>>>()
            .map_err(|tt| tt.into_box_error())?;

        let Some(mut map) = res.pop() else {
            return Err(BoxError::new(
                TarantoolErrorCode::InvalidMsgpack,
                "fetch result array should contains at least one element",
            ));
        };
        let Some(rows) = map.remove("rows") else {
            return Err(BoxError::new(
                TarantoolErrorCode::InvalidMsgpack,
                "fetch result map should contains `rows` key",
            ));
        };

        let data: Vec<T> = rmpv::ext::from_value(rows)
            .map_err(|e| BoxError::new(TarantoolErrorCode::InvalidMsgpack, e.to_string()))?;

        Ok(data)
    }
}

/// Execute a single SQL query as a prepared statement (transparently cached).
pub fn query(query: &str) -> Query {
    Query {
        query,
        params: vec![],
    }
}
