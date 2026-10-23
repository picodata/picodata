//! Table functions: functions that give rows and can be used in `FROM`.
//!
//! Tarantool executes these functions (see `sql_builtin_table_funcs` in
//! `src/box/sql/func.c`). This registry tells the planner which table
//! functions exist and which columns they give. The types of the arguments
//! are checked by the type system, like for scalar functions.

use crate::ir::types::UnrestrictedType;

/// The type of a column of a table function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableFunctionColumnType {
    /// The column always has this type.
    Fixed(UnrestrictedType),
    /// The column has the return type of the function call, which the type
    /// system gets from the types of the arguments.
    Call,
}

/// A column of a table function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableFunctionColumn {
    /// The column name in queries.
    pub name: &'static str,
    /// The column name in the local SQL that tarantool executes.
    pub local_name: &'static str,
    pub r#type: TableFunctionColumnType,
}

/// A table function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableFunction {
    /// The function name in queries and in the local SQL.
    pub name: &'static str,
    /// The columns of the result. `WITH ORDINALITY` adds
    /// [`ORDINALITY_COLUMN`] after them.
    pub columns: &'static [TableFunctionColumn],
}

/// The column that `WITH ORDINALITY` adds. It numbers the rows from 1.
pub const ORDINALITY_COLUMN: TableFunctionColumn = TableFunctionColumn {
    name: "ordinality",
    local_name: "ORDINALITY",
    r#type: TableFunctionColumnType::Fixed(UnrestrictedType::Integer),
};

const VALUE_COLUMN: TableFunctionColumn = TableFunctionColumn {
    name: "value",
    local_name: "VALUE",
    r#type: TableFunctionColumnType::Call,
};

const TABLE_FUNCTIONS: &[TableFunction] = &[
    TableFunction {
        name: "generate_series",
        columns: &[VALUE_COLUMN],
    },
    TableFunction {
        name: "unnest",
        columns: &[VALUE_COLUMN],
    },
    // Tarantool also has JSON_EACH. Its values have the tarantool JSON type,
    // which picodata cannot send to clients yet.
];

/// Finds a table function by its normalized name.
#[must_use]
pub fn find_table_function(name: &str) -> Option<&'static TableFunction> {
    TABLE_FUNCTIONS.iter().find(|func| func.name == name)
}

impl TableFunction {
    /// The number of result columns.
    #[must_use]
    pub fn result_len(&self, with_ordinality: bool) -> usize {
        self.columns.len() + usize::from(with_ordinality)
    }

    /// The result column at `position`, with the ordinality column last if
    /// it is requested.
    #[must_use]
    pub fn result_column(
        &self,
        with_ordinality: bool,
        position: usize,
    ) -> Option<&'static TableFunctionColumn> {
        let columns: &'static [TableFunctionColumn] = self.columns;
        match columns.get(position) {
            Some(column) => Some(column),
            None if with_ordinality && position == columns.len() => Some(&ORDINALITY_COLUMN),
            None => None,
        }
    }
}
