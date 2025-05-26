use crate::{
    expr::{FrameKind, Type},
    type_system::FunctionKind,
};
use thiserror::Error;

fn format_type(t: Option<Type>) -> &'static str {
    t.map(|t| t.as_str()).unwrap_or("unknown")
}

// [Some(Integer), Some(Text), Some(Boolean), None] -> "int, text, bool, unknown"
fn format_types(types: &[Option<Type>]) -> String {
    let types: Vec<_> = types.iter().map(|t| format_type(*t)).collect();
    types.join(", ")
}

// [Some(Integer), Some(Text), Some(Boolean), None] -> "int, text, bool and unknown"
fn format_types_with_and(types: &[Option<Type>]) -> String {
    match types.len() {
        0 => String::new(),
        1 => format_type(types[0]).to_string(),
        _ => {
            let (last, all_but_last) = types.split_last().unwrap();
            let all_but_last_fmt = format_types(all_but_last);
            format!("{} and {}", all_but_last_fmt, format_type(*last))
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
        argtypes: Vec<Option<Type>>,
    },

    #[error("{} types {} cannot be matched", context, format_types_with_and(types))]
    TypesCannotBeMatched {
        context: String,
        types: Vec<Option<Type>>,
    },

    #[error("{} {} does not exist", kind, name)]
    FunctionDoesNotExist { kind: FunctionKind, name: String },

    #[error("could not determine data type of parameter ${}", .0 + 1)]
    CouldNotDetermineParameterType(u16),

    #[error(
        "inconsistent types {preferred} and {another} deduced for parameter ${idx}, \
        consider using transitive type casts through a common type, \
        e.g. ${idx}::{preferred}::{another} and ${idx}::{preferred}",
        idx = idx + 1,
    )]
    InconsistentParameterTypesDeduced {
        idx: u16,
        preferred: Type,
        another: Type,
    },

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

    #[error("{0} desired types cannot be matched with {1} expressions")]
    DesiredTypesCannotBeMatchedWithExprs(usize, usize),

    #[error("subquery must return only one column")]
    SubqueryMustReturnOnlyOneColumn,

    #[error("row value misused")]
    RowValueMisused,

    #[error("unexpected expression of type any, explicit cast to the actual type is required")]
    UnexpectedExpressionOfTypeAny,

    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn could_not_resolve_overload(
        kind: FunctionKind,
        name: impl Into<String>,
        argtypes: impl Into<Vec<Option<Type>>>,
    ) -> Self {
        Self::CouldNotResolveOverload {
            kind,
            name: name.into(),
            argtypes: argtypes.into(),
        }
    }
}
