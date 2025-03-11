use crate::{
    expr::{FrameKind, Type},
    type_system::FunctionKind,
};
use thiserror::Error;

// [Unsigned, Text, Boolean] -> "unsigned, text, bool"
fn format_types(types: &[Type]) -> String {
    let types: Vec<_> = types.iter().map(|t| t.as_str()).collect();
    types.join(", ")
}

// [Unsigned, Text, Boolean] -> "unsigned, text and bool"
fn format_types_with_and(types: &[Type]) -> String {
    match types.len() {
        0 => String::new(),
        1 => types[0].as_str().to_string(),
        _ => {
            let (last, all_but_last) = types.split_last().unwrap();
            let all_but_last_fmt = format_types(all_but_last);
            format!("{} and {}", all_but_last_fmt, last.as_str())
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error(
        "could not resolve {} overload for {}({})",
        kind,
        name,
        format_types(argtypes)
    )]
    CouldNotResolveOverload {
        kind: FunctionKind,
        name: String,
        argtypes: Vec<Type>,
    },

    #[error("{} types {} cannot be matched", context, format_types_with_and(types))]
    TypesCannotBeMatched { context: String, types: Vec<Type> },

    #[error("{} {} does not exist", kind, name)]
    FunctionDoesNotExist { kind: FunctionKind, name: String },

    #[error("could not determine data type of parameter {0}")]
    CouldNotDetermineParameterType(String),

    #[error("unequal number of entries in row expression: {0} and {1}")]
    UnequalNumberOfEntriesInRowExpression(usize, usize),

    #[error("subquery returns {0} columns, expected {1}")]
    SubqueryReturnsUnexpectedNumberOfColumns(usize, usize),

    #[error("argument of NOT must be type boolean, not type {0}")]
    UnexpectedNotArgumentType(Type),

    #[error("argument of {0} must have integer type, got {1}")]
    IncorrectFrameArgumentType(FrameKind, Type),

    #[error("{0} lists must all be the same length")]
    ListsMustAllBeTheSameLentgh(&'static str),

    #[error("subquery must return only one column")]
    SubqueryMustReturnOnlyOneColumn,

    #[error("row value misused")]
    RowValueMisused,

    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn could_not_resolve_overload(
        kind: FunctionKind,
        name: impl Into<String>,
        argtypes: impl Into<Vec<Type>>,
    ) -> Self {
        Self::CouldNotResolveOverload {
            kind,
            name: name.into(),
            argtypes: argtypes.into(),
        }
    }
}
