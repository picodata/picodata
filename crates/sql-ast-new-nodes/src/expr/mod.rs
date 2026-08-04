//! SQL expressions.
//!
//! # `Expr<'q, State: AstState<'q>>`
//! One [`ExprInner`] variant plus the per-state metadata that analysis will fill in.
//!
//!
//! # Tree structure
//! The variants span the whole expression language
//! * literals, parameters and column references at the leaves,
//! * unary and binary operations, `CAST`, `CASE` and the predicate forms
//!   (`LIKE`, `BETWEEN`, `IN`, `IS`, `EXISTS`) above them,
//! * the call forms, from ordinary functions to window functions.
//!
//! An expression may also contain a whole statement. Subquery is a [`MultisetStmt`].
//!
//!
//! # Rendering
//! Operator binding strength is described once, as a precedence ladder.
//! The Pratt parser in `parsing/expr/mod.rs` consumes it to build trees, and the
//! [`Display`](std::fmt::Display) impls consume it to decide where parentheses have to be
//! re-inserted. A rendered expression therefore re-parses into the tree it came
//! from.
//!
//!
//! # Operator precedence table
//!
//! | # | Operators | Assoc |
//! |---|---|---|
//! | 1 | `OR` | left |
//! | 2 | `AND` | left |
//! | 3 | `NOT` | right |
//! | 4 | `IS` (IS TRUE/FALSE/NULL/UNKNOWN/DISTINCT FROM), `ISNULL`, `NOTNULL` | non-assoc |
//! | 5 | `< > = <= >= <>` | non-assoc |
//! | 6 | `BETWEEN` `IN` `LIKE` `ILIKE` `SIMILAR TO` (incl. `NOT`-variants) | non-assoc |
//! | 7 | `ESCAPE` | non-assoc |
//! | 8 | any other operator, incl. `\|\|` | left |
//! | 9 | `+ -` (binary) | left |
//! | 10 | `* / %` | left |
//! | 11 | `^` | left |
//! | 12 | `AT TIME ZONE` / `AT LOCAL` | left |
//! | 13 | `COLLATE` | left |
//! | 14 | `+ -` (unary) | right |
//! | 15 | `[ ]` subscript | left |
//! | 16 | `::` typecast | left |
//! | 17 | `.` name separator | left |

pub(super) mod render;
mod structural_eq;

use smol_str::format_smolstr;

use crate::error::{ast_arbitrary_err, AstResult};
use crate::multiset::MultisetStmt;
use crate::window::WindowFunction;
use crate::{AstState, Ident, Raw};
use sql_ir::ir::types::CastType;

pub type RawExpr<'q> = Expr<'q, Raw>;

/// An expression node: the syntactic payload plus per-state metadata —
/// nothing in [`Raw`], the inferred type and type-system mirror id in [`Analyzed`].
#[derive(Default)]
pub struct Expr<'q, State: AstState<'q>> {
    pub inner: ExprInner<'q, State>,
    meta: State::ExprMeta,
}

impl<'q, State: AstState<'q>> Expr<'q, State> {
    pub fn into(self) -> ExprInner<'q, State> {
        self.inner
    }

    pub fn from_parts(inner: ExprInner<'q, State>, meta: State::ExprMeta) -> Self {
        Self { inner, meta }
    }

    pub fn inner_ref(&self) -> &ExprInner<'q, State> {
        &self.inner
    }

    pub fn meta_ref(&self) -> &State::ExprMeta {
        &self.meta
    }

    pub(crate) fn is_cast(&self) -> bool {
        matches!(self.inner, ExprInner::Cast(_))
    }
}

impl<'q, State: AstState<'q>> Expr<'q, State> {
    pub(crate) fn new(inner: ExprInner<'q, State>) -> Self {
        Self {
            inner,
            meta: State::ExprMeta::default(),
        }
    }
}

#[derive(Default)]
pub enum ExprInner<'q, State: AstState<'q>> {
    /// Placeholder that exists only for in-place transforms ([`std::mem::replace`]).
    /// Parsers must never build one. Rendering it means an analyzer bug.
    #[default]
    Nil,
    BinaryOperation(BinaryOperation<'q, State>),
    UnaryOperation(UnaryOperation<'q, State>),
    ColumnRef(State::ColumnRefT),
    Literal(Literal<'q>),
    SubQuery(Box<MultisetStmt<'q, State>>),
    Row(ValuesRow<'q, State>),
    Array(ArrayLiteral<'q, State>),
    WindowFunction(Box<WindowFunction<'q, State>>),
    FunctionCall(FunctionCall<'q, State>),
    Parameter(Parameter),
    Cast(Cast<'q, State>),
    Like(Like<'q, State>),
    Similar(Similar<'q, State>),
    Between(Between<'q, State>),
    In(InExpr<'q, State>),
    Is(IsExpr<'q, State>),
    Index(IndexExpr<'q, State>),
    Trim(Trim<'q, State>),
    Substring(Substring<'q, State>),
    Case(Case<'q, State>),
    Exists(Exists<'q, State>),
    TimeFunction(TimeFunction),
}

impl<'q, State: AstState<'q>> ExprInner<'q, State> {
    /// The node's variant name, for diagnostics that must not render the
    /// expression itself — an error path names the kind rather than echoing SQL
    /// that may itself be what the user got wrong.
    pub fn kind_name(&self) -> &'static str {
        match self {
            ExprInner::Nil => "Nil",
            ExprInner::BinaryOperation(_) => "BinaryOperation",
            ExprInner::UnaryOperation(_) => "UnaryOperation",
            ExprInner::ColumnRef(_) => "ColumnRef",
            ExprInner::Literal(_) => "Literal",
            ExprInner::SubQuery(_) => "SubQuery",
            ExprInner::Row(_) => "Row",
            ExprInner::Array(_) => "Array",
            ExprInner::WindowFunction(_) => "WindowFunction",
            ExprInner::FunctionCall(_) => "FunctionCall",
            ExprInner::Parameter(_) => "Parameter",
            ExprInner::Cast(_) => "Cast",
            ExprInner::Like(_) => "Like",
            ExprInner::Similar(_) => "Similar",
            ExprInner::Between(_) => "Between",
            ExprInner::In(_) => "In",
            ExprInner::Is(_) => "Is",
            ExprInner::Index(_) => "Index",
            ExprInner::Trim(_) => "Trim",
            ExprInner::Substring(_) => "Substring",
            ExprInner::Case(_) => "Case",
            ExprInner::Exists(_) => "Exists",
            ExprInner::TimeFunction(_) => "TimeFunction",
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum BinaryOp {
    Arithmetic(ArithmeticOp),
    Comparison(CmpOp),
    Boolean(BooleanOp),
    Concat,
}

#[derive(Clone, Copy, PartialEq)]
pub enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Clone, Copy, PartialEq)]
pub enum CmpOp {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
}

#[derive(Clone, Copy, PartialEq)]
pub enum BooleanOp {
    And,
    Or,
}

pub struct BinaryOperation<'q, State: AstState<'q>> {
    left: Box<Expr<'q, State>>,
    right: Box<Expr<'q, State>>,
    pub op: BinaryOp,
}

#[derive(PartialEq)]
pub enum UnaryOp {
    Plus,
    Minus,
    Not,
}

pub struct UnaryOperation<'q, State: AstState<'q>> {
    operand: Box<Expr<'q, State>>,
    operator: UnaryOp,
}

#[derive(PartialEq)]
pub struct RawColumnRef {
    table_name: Option<Ident>,
    column_name: Ident,
}

pub struct Literal<'q> {
    pub value: &'q str,
    pub quotes: QuotesType,
    pub kind: LiteralKind,
}

#[derive(Default)]
pub struct ValuesRow<'q, State: AstState<'q>> {
    values: Vec<Expr<'q, State>>,
}

/// `ARRAY[a, b, c]` literal; the element list may be empty (`ARRAY[]`).
pub struct ArrayLiteral<'q, State: AstState<'q>> {
    elems: Vec<Expr<'q, State>>,
}

/// Quotes surrounding a literal in the query source. The quotes are not part
/// of [`Literal::value`]. The value is stored bare and re-quoted on rendering.
#[derive(PartialEq)]
pub enum QuotesType {
    /// Numeric, boolean and NULL literals: `42`, `1.5`, `true`, `null`
    None,
    /// String literals: `'abc'`
    Single,
}

impl QuotesType {
    pub(crate) fn quote(&self) -> &'static str {
        match self {
            QuotesType::None => "",
            QuotesType::Single => "'",
        }
    }
}

/// The concrete kind of a literal, captured at parse time. The grammar distinguishes the kinds
/// ([`Integer`](sql_ast_new_grammar::Rule::Integer),
/// [`Decimal`](sql_ast_new_grammar::Rule::Decimal),
/// [`True`](sql_ast_new_grammar::Rule::True),
/// [`Null`](sql_ast_new_grammar::Rule::Null), ...),
/// but [`Literal::value`] keeps only the raw text,
/// so we remember the kind here to let the analyzer assign a type.
/// [`Null`](LiteralKind::Null) carries no type.
#[derive(Clone, Copy, PartialEq)]
pub enum LiteralKind {
    Integer,
    Numeric,
    Double,
    Boolean,
    Text,
    Null,
}

/// Scalar or aggregate function invocation: `name([DISTINCT] args)` / `count(*)`.
pub struct FunctionCall<'q, State: AstState<'q>> {
    name: Ident,
    args: FunctionCallArgs<'q, State>,
}

pub enum FunctionCallArgs<'q, State: AstState<'q>> {
    /// `count(*)`
    CountAsterisk,
    /// Regular argument list (empty for `f()`)
    Exprs {
        distinct: bool,
        exprs: Vec<Expr<'q, State>>,
    },
}

/// Parameter index range is $1..$65535
#[derive(PartialEq)]
pub struct Parameter(pub u16);

/// Tnt style parameters are never materialized in AST.
/// All parameters are converted to Pg style in-flight.
/// Notheless, parameter styles must not be mixed within one query.
#[derive(Copy, Clone)]
pub enum ParameterStyle {
    /// Tarantool-style positional parameter: `?`
    Tnt,
    /// PostgreSQL-style numbered parameter: `$1`
    Pg,
}

/// `CAST(child AS type)` and `child::type` build the same node;
/// the syntax is kept only to render the expression back faithfully.
#[derive(PartialEq)]
pub enum CastSyntax {
    Call,
    Postfix,
}

pub struct Cast<'q, State: AstState<'q>> {
    child: Box<Expr<'q, State>>,
    /// Read by the analyzer to build the type-system mirror node.
    pub ty: CastType,
    syntax: CastSyntax,
}

pub struct Like<'q, State: AstState<'q>> {
    is_not: bool,
    left: Box<Expr<'q, State>>,
    right: Box<Expr<'q, State>>,
    /// Attached by the parser after the fact: `ESCAPE` parses as an ordinary
    /// infix operator and is folded into the LIKE node it follows.
    pub escape: Option<Box<Expr<'q, State>>>,
    is_ilike: bool,
}

pub struct Similar<'q, State: AstState<'q>> {
    is_not: bool,
    left: Box<Expr<'q, State>>,
    right: Box<Expr<'q, State>>,
    /// Attached by the parser after the fact, like [`Like::escape`].
    pub escape: Option<Box<Expr<'q, State>>>,
}

pub struct Between<'q, State: AstState<'q>> {
    is_not: bool,
    left: Box<Expr<'q, State>>,
    center: Box<Expr<'q, State>>,
    right: Box<Expr<'q, State>>,
}

pub struct InExpr<'q, State: AstState<'q>> {
    is_not: bool,
    left: Box<Expr<'q, State>>,
    /// Always a [`Row`](ExprInner::Row) or a [`SubQuery`](ExprInner::SubQuery) (validated on construction).
    rhs: Box<Expr<'q, State>>,
}

pub struct IsExpr<'q, State: AstState<'q>> {
    is_not: bool,
    child: Box<Expr<'q, State>>,
    /// `Some(true)` for TRUE, `Some(false)` for FALSE, `None` for NULL and UNKNOWN (synonyms).
    value: Option<bool>,
}

pub struct IndexExpr<'q, State: AstState<'q>> {
    child: Box<Expr<'q, State>>,
    which: Box<Expr<'q, State>>,
}

#[derive(PartialEq)]
pub enum TrimKind {
    Leading,
    Trailing,
    Both,
}

pub struct Trim<'q, State: AstState<'q>> {
    kind: Option<TrimKind>,
    pattern: Option<Box<Expr<'q, State>>>,
    target: Box<Expr<'q, State>>,
}

pub enum Substring<'q, State: AstState<'q>> {
    /// `SUBSTRING(s FROM start FOR len)`
    FromFor(
        Box<Expr<'q, State>>,
        Box<Expr<'q, State>>,
        Box<Expr<'q, State>>,
    ),
    /// `SUBSTRING(s, start, len)`
    Regular(
        Box<Expr<'q, State>>,
        Box<Expr<'q, State>>,
        Box<Expr<'q, State>>,
    ),
    /// `SUBSTRING(s FOR len)`
    For(Box<Expr<'q, State>>, Box<Expr<'q, State>>),
    /// `SUBSTRING(s FROM start)` (also covers the `(s, start)` comma form)
    From(Box<Expr<'q, State>>, Box<Expr<'q, State>>),
    /// The regex form `SUBSTRING(s SIMILAR pat ESCAPE esc)`: the whole
    /// SIMILAR/ESCAPE chain parses as one infix expression. Grammar-wise this
    /// is the single-expression fallback variant, so it also captures any
    /// other one-argument call (e.g. `SUBSTRING(s)`); validating that the
    /// expression really is a SIMILAR with an escape is an analysis concern
    /// (the old pipeline rejects at IR build).
    Similar(Box<Expr<'q, State>>),
}

pub struct Case<'q, State: AstState<'q>> {
    pub search: Option<Box<Expr<'q, State>>>,
    pub when_blocks: Vec<(Expr<'q, State>, Expr<'q, State>)>,
    pub else_expr: Option<Box<Expr<'q, State>>>,
}

pub struct Exists<'q, State: AstState<'q>> {
    is_not: bool,
    subquery: Box<MultisetStmt<'q, State>>,
}

#[derive(PartialEq)]
pub enum TimeFunction {
    CurrentDate,
    CurrentTime(Option<u32>),
    CurrentTimestamp(Option<u32>),
    LocalTime(Option<u32>),
    LocalTimestamp(Option<u32>),
}

impl<'q> ValuesRow<'q, Raw> {
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn add_value(&mut self, value: RawExpr<'q>) {
        self.values.push(value)
    }
}
impl<'q> RawExpr<'q> {
    pub fn literal(value: &'q str, quotes: QuotesType, kind: LiteralKind) -> Self {
        Self::new(ExprInner::Literal(Literal {
            value,
            quotes,
            kind,
        }))
    }

    pub fn column_ref(column_name: Ident, table_name: Option<Ident>) -> Self {
        Self::new(ExprInner::ColumnRef(RawColumnRef::new(
            table_name,
            column_name,
        )))
    }

    pub fn unary(operator: UnaryOp, operand: Self) -> Self {
        Self::new(ExprInner::UnaryOperation(UnaryOperation {
            operand: Box::new(operand),
            operator,
        }))
    }

    pub fn binary(left: Self, op: BinaryOp, right: Self) -> Self {
        Self::new(ExprInner::BinaryOperation(BinaryOperation {
            left: Box::new(left),
            right: Box::new(right),
            op,
        }))
    }

    pub fn subquery(subquery: MultisetStmt<'q, Raw>) -> Self {
        Self::new(ExprInner::SubQuery(Box::new(subquery)))
    }

    pub fn row(row: ValuesRow<'q, Raw>) -> Self {
        Self::new(ExprInner::Row(row))
    }

    pub fn array(elems: Vec<Self>) -> Self {
        Self::new(ExprInner::Array(ArrayLiteral { elems }))
    }

    pub fn window_function(window_function: WindowFunction<'q, Raw>) -> Self {
        Self::new(ExprInner::WindowFunction(Box::new(window_function)))
    }

    pub fn function_call(name: Ident, args: FunctionCallArgs<'q, Raw>) -> Self {
        Self::new(ExprInner::FunctionCall(FunctionCall { name, args }))
    }

    pub fn parameter(parameter: Parameter) -> Self {
        Self::new(ExprInner::Parameter(parameter))
    }

    pub fn cast(child: Self, ty: CastType, syntax: CastSyntax) -> Self {
        Self::new(ExprInner::Cast(Cast {
            child: Box::new(child),
            ty,
            syntax,
        }))
    }

    pub fn like(is_not: bool, left: Self, right: Self, is_ilike: bool) -> Self {
        Self::new(ExprInner::Like(Like {
            is_not,
            left: Box::new(left),
            right: Box::new(right),
            escape: None,
            is_ilike,
        }))
    }

    pub fn similar(is_not: bool, left: Self, right: Self) -> Self {
        Self::new(ExprInner::Similar(Similar {
            is_not,
            left: Box::new(left),
            right: Box::new(right),
            escape: None,
        }))
    }

    pub fn between(is_not: bool, left: Self, center: Self, right: Self) -> Self {
        Self::new(ExprInner::Between(Between {
            is_not,
            left: Box::new(left),
            center: Box::new(center),
            right: Box::new(right),
        }))
    }

    /// Validates on construction that the right side is a value list or a
    /// subquery: the Pratt parser lets any expression follow `IN`, and
    /// establishing the invariant here spares every consumer the check.
    pub fn in_expr(is_not: bool, left: Self, rhs: Self) -> AstResult<Self> {
        if !matches!(rhs.inner_ref(), ExprInner::Row(_) | ExprInner::SubQuery(_)) {
            return Err(ast_arbitrary_err(format_smolstr!(
                "In expression must have query or a list of values as right child"
            )));
        }
        Ok(Self::new(ExprInner::In(InExpr {
            is_not,
            left: Box::new(left),
            rhs: Box::new(rhs),
        })))
    }

    pub fn is(child: Self, is_not: bool, value: Option<bool>) -> Self {
        Self::new(ExprInner::Is(IsExpr {
            is_not,
            child: Box::new(child),
            value,
        }))
    }

    pub fn index(child: Self, which: Self) -> Self {
        Self::new(ExprInner::Index(IndexExpr {
            child: Box::new(child),
            which: Box::new(which),
        }))
    }

    pub fn trim(kind: Option<TrimKind>, pattern: Option<Box<Self>>, target: Self) -> Self {
        Self::new(ExprInner::Trim(Trim {
            kind,
            pattern,
            target: Box::new(target),
        }))
    }

    pub fn substring(substring: Substring<'q, Raw>) -> Self {
        Self::new(ExprInner::Substring(substring))
    }

    pub fn case(case: Case<'q, Raw>) -> Self {
        Self::new(ExprInner::Case(case))
    }

    pub fn exists(is_not: bool, subquery: MultisetStmt<'q, Raw>) -> Self {
        Self::new(ExprInner::Exists(Exists {
            is_not,
            subquery: Box::new(subquery),
        }))
    }

    pub fn time_function(time_function: TimeFunction) -> Self {
        Self::new(ExprInner::TimeFunction(time_function))
    }
}

// State-conversion accessors. The analyzer takes a `Raw` node apart and rebuilds
// its `Analyzed` counterpart through these. They are inherent impls, so they
// belong with the declarations rather than with the analyzer that calls them.

use crate::{Analyzed, AnalyzedExprMeta, ExprMetaT};
use sql_ir::ir::types::DerivedType;

impl<'q> Expr<'q, Analyzed> {
    pub fn into_inner(self) -> ExprInner<'q, Analyzed> {
        self.inner
    }

    pub fn parts_mut(&mut self) -> (&mut ExprInner<'q, Analyzed>, &mut AnalyzedExprMeta) {
        (&mut self.inner, &mut self.meta)
    }

    pub fn data_type(&self) -> DerivedType {
        self.meta.data_type()
    }
}

impl<'q, State: AstState<'q>> BinaryOperation<'q, State> {
    pub fn into_parts(self) -> (Box<Expr<'q, State>>, Box<Expr<'q, State>>, BinaryOp) {
        (self.left, self.right, self.op)
    }

    pub fn parts_mut(
        &mut self,
    ) -> (
        &mut Box<Expr<'q, State>>,
        &mut Box<Expr<'q, State>>,
        &mut BinaryOp,
    ) {
        (&mut self.left, &mut self.right, &mut self.op)
    }
}

impl<'q> BinaryOperation<'q, Analyzed> {
    pub fn from_parts(
        left: Box<Expr<'q, Analyzed>>,
        right: Box<Expr<'q, Analyzed>>,
        op: BinaryOp,
    ) -> Self {
        Self { left, right, op }
    }
}

impl RawColumnRef {
    pub fn new(table_name: Option<Ident>, column_name: Ident) -> Self {
        Self {
            table_name,
            column_name,
        }
    }

    pub(crate) fn table_name(&self) -> Option<&str> {
        self.table_name.as_ref().map(Ident::as_str)
    }

    pub(crate) fn column_name(&self) -> &str {
        self.column_name.as_str()
    }
}

impl<'q, State: AstState<'q>> ArrayLiteral<'q, State> {
    pub fn into_parts(self) -> Vec<Expr<'q, State>> {
        self.elems
    }

    pub fn parts_mut(&mut self) -> &mut Vec<Expr<'q, State>> {
        &mut self.elems
    }
}

impl<'q> ArrayLiteral<'q, Analyzed> {
    pub fn from_parts(elems: Vec<Expr<'q, Analyzed>>) -> Self {
        Self { elems }
    }
}

impl<'q> Cast<'q, Raw> {
    pub fn into_parts(self) -> (Box<Expr<'q, Raw>>, CastType, CastSyntax) {
        (self.child, self.ty, self.syntax)
    }
}

impl<'q> Cast<'q, Analyzed> {
    pub fn from_parts(child: Box<Expr<'q, Analyzed>>, ty: CastType, syntax: CastSyntax) -> Self {
        Self { child, ty, syntax }
    }

    pub fn parts_mut(&mut self) -> (&mut Box<Expr<'q, Analyzed>>, &mut CastType, &mut CastSyntax) {
        (&mut self.child, &mut self.ty, &mut self.syntax)
    }
}
