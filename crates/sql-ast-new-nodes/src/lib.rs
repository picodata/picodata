//! Node declarations for the new SQL AST: the tree itself.
//!
//! # `AbstractSyntaxTree<'q, State: AstState<'q>>`
//! A statically typed replacement for the untyped node arena of the old SQL
//! frontend. Every syntactic form is its own Rust type, so the shape of a query
//! is checked by the compiler instead of at runtime.
//!
//! The tree is a typestate over the two stages ([Trees That Grow](https://arxiv.org/abs/1610.04799)).
//! [`AstState`] names the stage:
//! * [`Raw`] is what the parser produces.
//! * [`Analyzed`] is the same tree with analysis results included.
//!
//! One set of node structs serves both. The associated types swap the per-state payloads.
//!
//!
//! # What belongs here
//! Declarations, `Display`, and any query over a node that needs nothing but the node.
//! The dividing line against `sql-ast-new-analyzer` is whether a routine
//! needs the type system, the scope stacks or `Metadata`.
//! For example, column *routing* ([`table_expression::From::column_route`]) does not
//! and lives here. Asterisk *expansion* mints type-system ids and does not.
//!
//! This crate knows nothing about pest beyond one rule: [`Ident`]'s rendering
//! probes [`Rule::RegularIdentifier`] to decide whether an identifier can be
//! written bare.
//!
//!
//! # Rendering and comparison
//! The [`Display`] cluster writes a tree back as SQL, and it is built to
//! round-trip: it re-inserts the parentheses the parser dropped, driven by the
//! same precedence ladder, so pinning a rendering pins the *shape* of the tree
//! rather than the mere fact that a query was accepted. That is what every
//! stage's snapshot tests assert through. Two leaves have production readers as
//! well — [`write_sql_ident`] (with [`Ident`]'s [`Display`]) and
//! `Display for RawColumnRef` — because the analyzer's column-resolution errors
//! print the offending reference.
//!
//! Alongside it, the structural [`PartialEq`] over raw expressions answers what
//! rendering cannot: do two different spellings produce the same tree?
//! Statement sub-trees (subquery bodies, window functions) have no structural
//! comparison yet and fall back to their [`Display`].
//!
//! Both are ordinary crate code rather than `#[cfg(test)]`: that gate does not
//! cross a crate boundary, and the parser's and analyzer's tests are in other
//! crates.
//!
//!
//! # Panics
//! This crate must never panic: the frontend falls back to the old pipeline on
//! [`Err`], but a panic aborts the whole query.

#![cfg_attr(
    not(test),
    deny(
        clippy::todo,
        clippy::unimplemented,
        clippy::panic,
        clippy::unreachable,
        clippy::expect_used,
        clippy::unwrap_used
    )
)]
#![deny(
    rustdoc::broken_intra_doc_links,
    unreachable_pub,
    unused_lifetimes,
    single_use_lifetimes
)]
#![allow(rustdoc::private_intra_doc_links)]

pub mod error;
pub mod expr;
pub mod multiset;
pub mod rendering;
pub mod select;
pub mod table_expression;
pub mod window;

use std::fmt::{self, Display, Error, Formatter};
use std::rc::Rc;

use pest::Parser;
use smol_str::SmolStr;

use sql_ast_new_grammar::{PairParser, Rule};
use sql_ir::ir::types::{DerivedType, NestedType, UnrestrictedType};

/// Identifies an expression node to the type system, which keys its report by a
/// plain integer rather than by a reference into the tree.
///
/// Declared here rather than alongside the type-system bridge because it is
/// `nodes` that stores it, in [`AnalyzedExprMeta`].
pub type AstNodeId = u32;

use crate::table_expression::{JoinUsingColumn, ResolvedJoinUsingColumn};

use self::expr::render::Precedence;
use self::expr::{Expr, Parameter, RawColumnRef};
use self::multiset::{Cte, MultisetStmt};
use self::select::{ProjectionExpr, SelectListElem};
use self::table_expression::{AnalyzedCteOrTable, ResolvedColumnRef, TableFactor};

/// A normalized SQL identifier — the single identifier representation used
/// throughout `ast_new` (table, column, alias, CTE and window names).
///
/// The backing string type is an implementation detail. Every user builds and
/// reads identifiers through [`Ident::from_sql`], [`Ident::as_str`], `Display`
/// and the `PartialEq` impls — never the inner field — so the representation
/// (currently [`SmolStr`]) can be swapped.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident(SmolStr);

impl Ident {
    /// Normalize a grammar-produced identifier spelling: a delimited identifier loses its quotes
    /// and collapses the `""` escape (reads `"a""b"` as `a"b`),
    /// anything else folds to lowercase (Unicode-aware).
    pub fn from_sql(raw: &str) -> Self {
        let normalized = match raw.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
            Some(inner) if inner.contains('"') => SmolStr::from(inner.replace("\"\"", "\"")),
            Some(inner) => SmolStr::from(inner),
            None => SmolStr::new(raw.to_lowercase()),
        };
        Self(normalized)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl NamedEntity for Ident {
    fn name(&self) -> Option<&str> {
        Some(self.as_str())
    }
}

/// Write an already-normalized identifier as re-parseable SQL.
///
/// This probe is the whole of this crate's dependency on the grammar.
///
/// Unlike most of the [`Display`] cluster, this leaf has a production reader: the
/// analyzer's column-resolution errors print the offending reference through it
/// (see `Display for RawColumnRef`).
fn write_sql_ident(f: &mut Formatter<'_>, name: &str) -> fmt::Result {
    let renders_bare = name == name.to_lowercase()
        && PairParser::parse(Rule::RegularIdentifier, name)
            .ok()
            .and_then(|mut pairs| pairs.next())
            .is_some_and(|pair| pair.as_str().len() == name.len());
    if renders_bare {
        f.write_str(name)
    } else {
        write!(f, "\"{}\"", name.replace('"', "\"\""))
    }
}

/// Renders the identifier as re-parseable SQL (see [`write_sql_ident`]), which
/// keeps every node `Display` round-trippable: `"C2"` prints as `"C2"`,
/// not as the bare `C2` that would fold back to `c2`.
impl Display for Ident {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write_sql_ident(f, self.as_str())
    }
}

pub trait NamedEntity {
    fn name(&self) -> Option<&str>;
}

/// Sharing a node must not hide its name: forwarded so that a state whose
/// payload is an `Rc` (see [`Analyzed`]) still satisfies the [`NamedEntity`]
/// bounds on [`AstState`]. Mirrors the way `std` forwards `Display` through `Rc`.
impl<T: NamedEntity + ?Sized> NamedEntity for Rc<T> {
    fn name(&self) -> Option<&str> {
        (**self).name()
    }
}

/// Typestate of the AST:
/// * [`Raw`] (fresh from the parser)
/// * [`Analyzed`] (after name resolution and type inference).
///
/// One set of node structs serves both states.
/// The associated types swap the per-state payloads.
/// For example, a column reference is a bare name in [`Raw`] and a resolved pointer in [`Analyzed`]).
/// Therefore, tree cannot carry data its stage has no right to hold.
///
/// See [Trees That Grow](https://arxiv.org/abs/1610.04799) for theoretical explanation.
///
/// Every payload renders, so each associated type carries a [`Display`] bound.
pub trait AstState<'q>: Sized {
    type CteT: Display;

    type SelectListElemT: Display;

    type TableFactorT: NamedEntity + Display;
    type JoinUsingColumnT: Display;

    type CteOrTableT: NamedEntity + Display;

    type ExprMeta: ExprMetaT;

    type ColumnRefT: Display;

    /// State-specific expression rendering:
    /// * [`Raw`] renders the bare expression.
    /// * [`Analyzed`] decorates it with the inferred type.
    fn fmt_expr(expr: &Expr<'q, Self>, f: &mut Formatter<'_>) -> fmt::Result;

    /// Binding tier of the *rendered* form of `expr` in this state, used by
    /// operand rendering to decide parenthesization. [`Raw`] renders the bare
    /// expression, so the default is the expression's own precedence.
    /// A state whose [`fmt_expr`](AstState::fmt_expr) appends a suffix to the expression
    /// (see [`Analyzed`]) must report the suffix's tier instead.
    fn display_precedence(expr: &Expr<'q, Self>) -> Precedence {
        expr.precedence()
    }
}

/// Parser output: bare source slices, no resolution, no types.
#[derive(Default)]
pub struct Raw;

impl<'q> AstState<'q> for Raw {
    type CteT = Cte<'q, Raw>;

    type SelectListElemT = SelectListElem<'q>;
    type TableFactorT = TableFactor<'q, Raw>;
    type JoinUsingColumnT = JoinUsingColumn;
    type CteOrTableT = Ident;

    type ExprMeta = RawExprMeta;
    type ColumnRefT = RawColumnRef;

    fn fmt_expr(expr: &Expr<'q, Self>, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", expr.inner_ref())
    }
}

pub trait ExprMetaT: Default {
    fn data_type(&self) -> DerivedType {
        DerivedType::unknown()
    }
}

#[derive(Default)]
pub struct RawExprMeta {}

impl ExprMetaT for RawExprMeta {}

/// Analyzer output: relations and columns resolved, expression types
/// inferred, implicit casts materialized.
#[derive(Default)]
pub struct Analyzed;

impl<'q> AstState<'q> for Analyzed {
    type CteT = Rc<Cte<'q, Analyzed>>;

    type SelectListElemT = ProjectionExpr<'q, Analyzed>;
    /// Original From clause is shared by resolved column references.
    /// See [`ResolvedColumnRef`].
    type TableFactorT = Rc<TableFactor<'q, Analyzed>>;
    type JoinUsingColumnT = ResolvedJoinUsingColumn<'q>;
    type CteOrTableT = AnalyzedCteOrTable<'q>;

    type ExprMeta = AnalyzedExprMeta;
    type ColumnRefT = ResolvedColumnRef<'q>;
    /// Render the expression followed by its inferred type as a `::type` suffix (when known).
    /// `::type` is postfix-cast syntax, so compound expressions are wrapped in parentheses exactly
    /// like an explicit postfix cast (`(1 + 2)::int`). A cast root is left as-is to avoid a
    /// redundant `x::t::t`.
    /// For the same reason a cast's immediate child carries no type, so only the cast itself states the type.
    fn fmt_expr(expr: &Expr<'q, Self>, f: &mut Formatter<'_>) -> fmt::Result {
        let body = expr.inner_ref();
        let d_type = expr.data_type();

        if expr.is_cast() {
            return write!(f, "{body}");
        }

        if expr.precedence() < Precedence::Postfix && d_type.get().is_some() {
            write!(f, "({body})")
        } else {
            write!(f, "{body}")
        }?;

        match d_type.get() {
            None | Some(UnrestrictedType::Any) => Ok(()),
            Some(UnrestrictedType::Map) => write!(f, "::json"),
            Some(UnrestrictedType::Array(NestedType::Map)) => write!(f, "::json[]"),
            Some(UnrestrictedType::Array(NestedType::Any)) => write!(f, "::string[]"),
            Some(ty) => write!(f, "::{ty}"),
        }
    }

    /// A typed non-cast expression is rendered by `fmt_expr` with a `::type`
    /// suffix, so its rendered form binds at the postfix tier regardless of
    /// the expression underneath: e.g. as an operand of `*` it needs no
    /// extra parentheses (`(a + b)::int * c`). The condition mirrors the
    /// branch in `fmt_expr` that injects the suffix and must stay in sync
    /// with it.
    fn display_precedence(expr: &Expr<'q, Self>) -> Precedence {
        if expr.data_type().get().is_some() && !expr.is_cast() {
            Precedence::Postfix
        } else {
            expr.precedence()
        }
    }
}

#[derive(Default)]
pub struct AbstractSyntaxTree<'q, State: AstState<'q>> {
    pub root: Node<'q, State>,
}

pub type RawAst<'q> = AbstractSyntaxTree<'q, Raw>;

pub type AnalyzedAst<'q> = AbstractSyntaxTree<'q, Analyzed>;

impl<'q> AnalyzedAst<'q> {
    pub fn from_root(root: Node<'q, Analyzed>) -> Self {
        Self { root }
    }
}

impl<'q, State: AstState<'q>> Display for AbstractSyntaxTree<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.root)
    }
}

#[derive(Default)]
pub enum Node<'q, State: AstState<'q>> {
    /// An empty query (empty input, or whitespace/comments only).
    #[default]
    Empty,
    DqlStmt(DqlStmt<'q, State>),
}

impl<'q, State: AstState<'q>> Display for Node<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Node::<'q, State>::Empty => Ok(()),
            Node::DqlStmt(stmt) => write!(f, "{}", stmt),
        }
    }
}

/// A DQL statement: the multiset body plus the trailing `OPTION(...)`.
pub struct DqlStmt<'q, State: AstState<'q>> {
    pub stmt: Box<MultisetStmt<'q, State>>,
    pub option: Option<SqlOption>,
}

impl<'q, State: AstState<'q>> DqlStmt<'q, State> {
    pub fn into_parts(self) -> (Box<MultisetStmt<'q, State>>, Option<SqlOption>) {
        (self.stmt, self.option)
    }

    pub fn from_parts(stmt: Box<MultisetStmt<'q, State>>, option: Option<SqlOption>) -> Self {
        Self { stmt, option }
    }
}

impl<'q, State: AstState<'q>> Display for DqlStmt<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.stmt)?;
        if let Some(option) = &self.option {
            write!(f, " {}", option)?;
        }
        Ok(())
    }
}

/// The trailing `OPTION(...)` clause.
///
/// The head parameter is stored apart from the tail because
/// an empty list is a state when [`DqlStmt::option`] is [`None`]. Splitting it
/// out spares every reader an emptiness check the parser could not have produced.
pub struct SqlOption {
    first: SqlOptionParameter,
    rest: Vec<SqlOptionParameter>,
}

const SQL_OPTIONS_COUNT: usize = 4;

impl SqlOption {
    pub fn new(first: SqlOptionParameter) -> Self {
        Self {
            first,
            rest: Vec::with_capacity(SQL_OPTIONS_COUNT - 1),
        }
    }

    pub fn add_param(&mut self, param: SqlOptionParameter) {
        self.rest.push(param);
    }
}

impl Display for SqlOption {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "OPTION({}", self.first)?;
        for param in &self.rest {
            write!(f, ", {param}")?;
        }
        write!(f, ")")
    }
}

pub enum SqlOptionParameter {
    VdbeOpcodeMax(SqlOptionValue<u64>),
    MotionRowMax(SqlOptionValue<u64>),
    ReadPreference(SqlOptionValue<ReadPreference>),
    Forward(SqlOptionValue<Forward>),
}

impl Display for SqlOptionParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            SqlOptionParameter::VdbeOpcodeMax(value) => {
                write!(f, "sql_vdbe_opcode_max = {value}")
            }
            SqlOptionParameter::MotionRowMax(value) => {
                write!(f, "sql_motion_row_max = {value}")
            }
            SqlOptionParameter::ReadPreference(value) => write!(f, "read_preference = {value}"),
            SqlOptionParameter::Forward(value) => write!(f, "forward = {value}"),
        }
    }
}

pub enum SqlOptionValue<T> {
    Literal(T),
    Parameter(Parameter),
}

impl<T: Display> Display for SqlOptionValue<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            SqlOptionValue::Literal(value) => write!(f, "{value}"),
            SqlOptionValue::Parameter(parameter) => write!(f, "{parameter}"),
        }
    }
}
pub enum ReadPreference {
    Leader,
    Replica,
    Any,
}

impl Display for ReadPreference {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            ReadPreference::Leader => write!(f, "leader"),
            ReadPreference::Replica => write!(f, "replica"),
            ReadPreference::Any => write!(f, "any"),
        }
    }
}

pub enum Forward {
    On,
    Off,
    RoToRw,
}

impl Display for Forward {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Forward::On => write!(f, "on"),
            Forward::Off => write!(f, "off"),
            Forward::RoToRw => write!(f, "ro_to_rw"),
        }
    }
}

const NONAME_COLUMN: &str = "<noname_column>";

#[derive(Default)]
pub struct AnalyzedExprMeta {
    pub data_type: DerivedType,
    pub expr_id: Option<AstNodeId>,
}

impl ExprMetaT for AnalyzedExprMeta {
    fn data_type(&self) -> DerivedType {
        self.data_type
    }
}

impl AnalyzedExprMeta {
    pub fn new(data_type: DerivedType, id: AstNodeId) -> Self {
        Self {
            data_type,
            expr_id: Some(id),
        }
    }

    pub fn new_with_id(id: AstNodeId) -> Self {
        Self {
            data_type: DerivedType::unknown(),
            expr_id: Some(id),
        }
    }
}
