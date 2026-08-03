//! Cluster metadata trait.

use smol_str::SmolStr;
use std::time::Duration;

use crate::errors::SbroadError;
use crate::ir::function::Function;
use crate::ir::relation::Table;

/// A metadata trait of the cluster (getters for tables, functions, etc.).
pub trait Metadata: Sized {
    /// Get a table by normalized name that contains:
    /// * list of the columns,
    /// * distribution key of the output tuples (column positions),
    /// * table name.
    ///
    /// # Errors
    /// - Failed to get table by name from the metadata.
    fn table(&self, table_name: &str) -> Result<Table, SbroadError>;

    /// Get index id with given name for given table.
    ///
    /// # Errors
    /// - Failed to get table by name.
    /// - Failed to get index by name.
    /// - Failed to find index for specified table.
    fn get_index_id(&self, index_name: &str, table_name: &str) -> Result<u32, SbroadError>;

    /// Lookup for a function in the metadata cache.
    ///
    /// # Errors
    /// - Failed to get function by name from the metadata.
    fn function(&self, fn_name: &str) -> Result<&Function, SbroadError>;

    /// Get the wait timeout for the query execution.
    fn waiting_timeout(&self) -> Duration;

    /// Get the name of the sharding column (usually it is `bucket_id`).
    fn sharding_column(&self) -> &str;

    /// Provides vector of the sharding key column names or an error
    ///
    /// # Errors
    /// - Metadata does not contain space
    /// - Metadata contains incorrect sharding key format
    fn sharding_key_by_space(&self, space: &str) -> Result<Vec<SmolStr>, SbroadError>;

    /// Provides vector of the sharding key column positions in a tuple or an error
    ///
    /// # Errors
    /// - Metadata does not contain space
    fn sharding_positions_by_space(&self, space: &str) -> Result<Vec<usize>, SbroadError>;
}
