//! Table functions: functions that give rows and can be used in `FROM`.
//!
//! Tarantool executes these functions (see `sql_builtin_table_funcs` in
//! `src/box/sql/func.c`). This registry tells the planner which table
//! functions exist and which columns they give. The types of the arguments
//! are checked by the type system, like for scalar functions, and the
//! frontend records the types of the columns in the scan node.

use std::borrow::Cow;

use smol_str::{format_smolstr, SmolStr};
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

    /// The registry column at `position`, with the ordinality column last
    /// if it is requested, and the number that the names of the column get.
    /// A call with several arguments numbers the columns of the template
    /// from 1, as `value_1`.
    fn column_at(&self, position: usize) -> Option<(&'static TableFunctionColumn, Option<usize>)> {
        let count = self.column_types.len();
        let function: &'static TableFunction = self.function;
        if position < count {
            return Some(match &function.columns {
                TableFunctionColumns::Fixed(columns) => (&columns[position], None),
                TableFunctionColumns::PerArgument(template) => {
                    (template, (count > 1).then_some(position + 1))
                }
            });
        }
        (self.with_ordinality && position == count).then_some((&ORDINALITY_COLUMN, None))
    }

    /// The name in queries of the result column at `position`. Only a
    /// numbered name is allocated.
    #[must_use]
    pub fn column_name(&self, position: usize) -> Option<Cow<'static, str>> {
        let (column, number) = self.column_at(position)?;
        Some(match number {
            Some(number) => Cow::Owned(format!("{}_{number}", column.name)),
            None => Cow::Borrowed(column.name),
        })
    }

    /// The name in the local SQL that tarantool executes of the result
    /// column at `position`. A numbered name is short and stays inline in
    /// the `SmolStr`.
    #[must_use]
    pub fn local_name(&self, position: usize) -> Option<SmolStr> {
        let (column, number) = self.column_at(position)?;
        Some(match number {
            Some(number) => format_smolstr!("{}_{number}", column.local_name),
            None => SmolStr::new_static(column.local_name),
        })
    }

    /// Whether the local SQL casts the result column at `position`. See
    /// [`TableFunctionColumn::cast_in_local_sql`].
    #[must_use]
    pub fn cast_in_local_sql(&self, position: usize) -> bool {
        self.column_at(position)
            .is_some_and(|(column, _)| column.cast_in_local_sql)
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
