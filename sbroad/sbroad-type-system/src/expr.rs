use std::fmt::Display;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum Type {
    Unsigned,
    Integer,
    Double,
    Numeric,
    Text,
    Boolean,
    Datetime,
    Uuid,
    Array,
    Map,
    // Type of NULL literal.
    Unknown,
}

impl Type {
    pub fn as_str(&self) -> &'static str {
        match self {
            Type::Unsigned => "unsigned",
            Type::Integer => "int",
            Type::Double => "double",
            Type::Numeric => "numeric",
            Type::Text => "text",
            Type::Boolean => "bool",
            Type::Datetime => "datetime",
            Type::Uuid => "uuid",
            Type::Array => "array",
            Type::Map => "map",
            Type::Unknown => "unknown",
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Comparison operators, such as `=`, `<`, `in` and etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOperator {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    In,
}

impl ComparisonOperator {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::In => "in",
            Self::Gt => ">",
            Self::GtEq => ">=",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::NotEq => "<>",
        }
    }
}

/// Comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    IsNull,
    Exists,
}

impl UnaryOperator {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Not => "not",
            Self::IsNull => "is null",
            Self::Exists => "exists",
        }
    }
}

impl Display for ComparisonOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub const MAX_PARAMETER_INDEX: usize = 1 << 15 - 1;

/// ExprKind represents expressions that define types.
/// There are no parenthesis or aliaes, as they do not influence typing.
#[derive(Debug)]
pub enum ExprKind<Id> {
    /// NULL literal.
    /// Examples: `NULL`.
    Null,
    /// Literal value with known type. This type can be coerced to more suitable.
    /// Examples: `1.5`, `'kek`.
    Literal(Type),
    /// Column reference.
    /// Examples: `a`, `col`.
    Reference(Type),
    /// Indexed parameter value starting with 0.
    /// Parameters don't have a fixed type, but it can be inferred from the context.
    /// For examples, in expression `1 + $1` it's likely that parameter has integer type.
    /// Examples: `$1`, `$2`.
    Parameter(u16),
    /// Function or expression (scalar or aggregate).
    /// Examples: `max(a)`, `substring('abc', 1, 1)`.
    Function(String, Vec<Expr<Id>>),
    /// Operator expression.
    /// Examples: `1 + 2.5`, `'a' || 'b'`, `a and b`.
    Operator(String, Vec<Expr<Id>>),
    /// Cast expression.
    /// Note that parameter type can be set using cast: `$1::int` sets parameter type to int.
    /// Examples: `1::int`, `$1::text`.
    Cast(Box<Expr<Id>>, Type),
    /// Coalesce expression.
    /// Examples: `coalesce(1, 2)`, `coalesce(1, $1)`.
    Coalesce(Vec<Expr<Id>>),
    /// Comparison expression.
    /// Examples: `1 = b`, (1,2) in (values (1,2))`, `(1,2) = (1,2)`
    Comparison(ComparisonOperator, Box<Expr<Id>>, Box<Expr<Id>>),
    /// Row expressions are allowed only in comparison operations because there is no tuple type.
    /// Examples: `(1,2) = (1,2)`
    Row(Vec<Expr<Id>>),
    /// Subquery expression.
    /// At this point, subquery types are inferred separately, but this should be fixed later.
    /// Examples: `(select 1)`, `(1,2) = (values (1,2))`.
    Subquery(Vec<Type>),
    /// Case expression consist of when expressions and result expressions. Within each group
    /// types must be the same. In case of a "simple" form, an expression following "CASE" keyword
    /// must be included in when expressions group.
    /// is considered as a condition.
    /// Examples: `CASE WHEN a=1 THEN 'a = 1' ELSE 'a <> 1' END`,
    ///           `CASE a WHEN 1 THEN 'a = 1' ELSE 'a <> 1' END` (simple form)
    Case {
        when_exprs: Vec<Expr<Id>>,
        result_exprs: Vec<Expr<Id>>,
    },
    /// Unary expressions, such as `NOT`, `IS NULL` or `EXISTS`.
    /// Examples: `not 1 = 2`, `a is null`, `exists (select 1, 2, 3)`
    Unary(UnaryOperator, Box<Expr<Id>>),
    /// Window function invocation.
    /// Examples: `row_count() over ()`, `count(a) over (PARTITION BY b ORDER BY c)`
    WindowFunction {
        name: String,
        args: Vec<Expr<Id>>,
        filter: Option<Box<Expr<Id>>>,
        over: Box<Expr<Id>>,
    },
    /// Named window or OVER part of window function invocation.
    /// Examples: `row_count() over ()`, `count(a) over (PARTITION BY b ORDER BY c)`
    Window {
        order_by: Vec<Expr<Id>>,
        partition_by: Vec<Expr<Id>>,
        frame: Option<WindowFrame<Id>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FrameKind {
    Rows,
    Range,
}

impl FrameKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Rows => "ROWS",
            Self::Range => "RANGE",
        }
    }
}

impl Display for FrameKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// TODO: Support more precise representation, so we can catch more errors.
//  - RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column
//  - something else?
#[derive(Debug)]
pub struct WindowFrame<Id> {
    /// ROWS or RANGE
    pub kind: FrameKind,
    /// Specific offset expression of preceding or following bound (PRECEDING 2).
    pub bound_offsets: Vec<Expr<Id>>,
}

/// Type expression consists of expression kind and id.
/// ExprKind is used for type analysis.
/// Id is used to report information about types and coercions in `TypeReport`.
#[derive(Debug)]
pub struct Expr<Id> {
    /// Unique expression identifier.
    pub(crate) id: Id,
    /// Expression kind.
    pub(crate) kind: ExprKind<Id>,
}

impl<Id> Expr<Id> {
    pub const fn new(id: Id, kind: ExprKind<Id>) -> Self {
        Self { id, kind }
    }
}
