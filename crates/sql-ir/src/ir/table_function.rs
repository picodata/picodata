//! Table functions: functions that give rows and can be used in `FROM`.
//!
//! Tarantool executes these functions (see `sql_builtin_table_funcs` in
//! `src/box/sql/func.c`). This registry tells the planner which table
//! functions exist and which columns they give. The types of the arguments
//! are checked by the type system, like for scalar functions, and the
//! frontend records the types of the columns in the scan node.

use std::borrow::Cow;

use sql_type_system::expr::Type as TypeSystemType;

use crate::ir::types::{DerivedType, UnrestrictedType};

/// A column of a table function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableFunctionColumn {
    /// The column name in queries.
    pub name: &'static str,
    /// The column name in the local SQL that tarantool executes.
    pub local_name: &'static str,
    /// Tarantool may not know the type of the column, so the local SQL casts
    /// it to the type that the planner gives it.
    pub cast_in_local_sql: bool,
}

/// The columns of a table function, without the ordinality column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableFunctionColumns {
    /// The same columns for every call. Each has the return type of the call.
    Fixed(&'static [TableFunctionColumn]),
    /// A column for each argument, with the element type of the argument.
    /// The template gives the names: a call with one argument has them as
    /// they are, and a call with several arguments numbers them from 1, as
    /// `value_1`.
    PerArgument(TableFunctionColumn),
}

/// A table function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableFunction {
    /// The function name in queries and in the local SQL.
    pub name: &'static str,
    /// The columns of the result. `WITH ORDINALITY` adds
    /// [`ORDINALITY_COLUMN`] after them.
    pub columns: TableFunctionColumns,
}

/// The column that `WITH ORDINALITY` adds. It numbers the rows from 1.
pub const ORDINALITY_COLUMN: TableFunctionColumn = TableFunctionColumn {
    name: "ordinality",
    local_name: "ORDINALITY",
    cast_in_local_sql: false,
};

/// The type of [`ORDINALITY_COLUMN`].
pub const ORDINALITY_TYPE: UnrestrictedType = UnrestrictedType::Integer;

const SERIES_VALUE_COLUMN: TableFunctionColumn = TableFunctionColumn {
    name: "value",
    local_name: "VALUE",
    cast_in_local_sql: false,
};

// Tarantool knows the type of the elements only for an array literal. For
// a column or a parameter the type is ANY, and arithmetic rejects ANY.
const UNNEST_VALUE_COLUMN: TableFunctionColumn = TableFunctionColumn {
    cast_in_local_sql: true,
    ..SERIES_VALUE_COLUMN
};

const TABLE_FUNCTIONS: &[TableFunction] = &[
    TableFunction {
        name: "generate_series",
        columns: TableFunctionColumns::Fixed(&[SERIES_VALUE_COLUMN]),
    },
    // unnest(a, b) gives the elements of each array by position, as in
    // PostgreSQL. The rows are as many as the elements of the longest
    // array, and a shorter array gives NULL after its last element.
    TableFunction {
        name: "unnest",
        columns: TableFunctionColumns::PerArgument(UNNEST_VALUE_COLUMN),
    },
    // Tarantool also has JSON_EACH. Its values have the tarantool JSON type,
    // which picodata cannot send to clients yet.
];

/// Finds a table function by its normalized name.
#[must_use]
pub fn find_table_function(name: &str) -> Option<&'static TableFunction> {
    TABLE_FUNCTIONS.iter().find(|func| func.name == name)
}

/// A column of the result of a call of a table function.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultColumn {
    /// The column name in queries.
    pub name: Cow<'static, str>,
    /// The column name in the local SQL that tarantool executes.
    pub local_name: Cow<'static, str>,
    /// See [`TableFunctionColumn::cast_in_local_sql`].
    pub cast_in_local_sql: bool,
}

impl ResultColumn {
    /// The column with the names of `column`, numbered with `number`.
    fn new(column: &TableFunctionColumn, number: Option<usize>) -> Self {
        let (name, local_name) = match number {
            Some(number) => (
                Cow::Owned(format!("{}_{number}", column.name)),
                Cow::Owned(format!("{}_{number}", column.local_name)),
            ),
            None => (Cow::Borrowed(column.name), Cow::Borrowed(column.local_name)),
        };
        ResultColumn {
            name,
            local_name,
            cast_in_local_sql: column.cast_in_local_sql,
        }
    }
}

impl TableFunction {
    /// The number of columns of a call with `arg_count` arguments, without
    /// the ordinality column.
    #[must_use]
    pub fn column_count(&self, arg_count: usize) -> usize {
        match self.columns {
            TableFunctionColumns::Fixed(columns) => columns.len(),
            TableFunctionColumns::PerArgument(_) => arg_count,
        }
    }

    /// The types of the columns of a call, without the ordinality column:
    /// `call_type`, the return type of the call, for fixed columns, and the
    /// element type of each of `arg_types`, the types of the arguments, for
    /// a column per argument.
    pub fn column_types(
        &self,
        call_type: DerivedType,
        arg_types: impl IntoIterator<Item = TypeSystemType>,
    ) -> Vec<DerivedType> {
        match self.columns {
            TableFunctionColumns::Fixed(columns) => vec![call_type; columns.len()],
            // The type analysis has checked that every argument is an array.
            TableFunctionColumns::PerArgument(_) => arg_types
                .into_iter()
                .map(|r#type| match r#type {
                    TypeSystemType::Array(nested) => {
                        DerivedType::from(TypeSystemType::from(nested))
                    }
                    _ => DerivedType::unknown(),
                })
                .collect(),
        }
    }
}

/// A call of a table function: the registry entry, the types of the
/// columns of the call and the ordinality column. These give the names and
/// the types of the result columns.
#[derive(Clone, Copy, Debug)]
pub struct TableFunctionCall<'a> {
    pub function: &'static TableFunction,
    /// The types of the columns, without the ordinality column. See
    /// [`TableFunction::column_types`].
    pub column_types: &'a [DerivedType],
    pub with_ordinality: bool,
}

impl TableFunctionCall<'_> {
    /// The number of result columns.
    #[must_use]
    pub fn result_len(&self) -> usize {
        self.column_types.len() + usize::from(self.with_ordinality)
    }

    /// The result column at `position`, with the ordinality column last if
    /// it is requested.
    #[must_use]
    pub fn result_column(&self, position: usize) -> Option<ResultColumn> {
        let count = self.column_types.len();
        if position < count {
            return Some(match self.function.columns {
                TableFunctionColumns::Fixed(columns) => ResultColumn::new(&columns[position], None),
                TableFunctionColumns::PerArgument(template) => {
                    ResultColumn::new(&template, (count > 1).then_some(position + 1))
                }
            });
        }
        (self.with_ordinality && position == count)
            .then(|| ResultColumn::new(&ORDINALITY_COLUMN, None))
    }

    /// The type of the result column at `position`.
    #[must_use]
    pub fn column_type(&self, position: usize) -> DerivedType {
        match self.column_types.get(position) {
            Some(r#type) => *r#type,
            None => DerivedType::new(ORDINALITY_TYPE),
        }
    }
}
