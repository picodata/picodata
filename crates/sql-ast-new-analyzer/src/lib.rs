//! The `Raw` → `Analyzed` analysis pass.
//!
//! # `Analyzer`
//! One trait, implemented per node kind: each impl consumes its `Raw` node by
//! value and returns the `Analyzed` counterpart. Analysis is a conversion
//! between two tree types, never in-place mutation, so a half-analyzed tree
//! cannot exist.
//!
//! [`AnalyzerCtx`] is threaded through the whole walk: catalog access through
//! the [`Metadata`] trait, the type-system bridge and the scope stacks.
//!
//!
//! # Module layout
//! This file holds only what every node kind shares: the traits above, the
//! context threaded through them, and the entry point. The analysis of a node
//! kind lives in the module named after the `sql-ast-new-nodes` module the node
//! is declared in, so the two crates line up one to one — as does
//! `sql-ast-new-parser`, which is split the same way.
//!
//! | module | analyzes |
//! |---|---|
//! | [`multiset`] | WITH clause, and the `UNION`/`EXCEPT`/`INTERSECT` tree |
//! | [`select`] | SELECT statement, its select list, WHERE |
//! | [`table_expression`] | FROM clause, table factors, joins |
//! | [`expr`] | expressions, and the type-system mirror built alongside |
//! | [`asterisk`] | `*` and `t.*` expansion |
//!
//! The one node analyzed here is the root [`Node`], whose whole job is to
//! dispatch.
//!
//! What the modules share is marked `pub(crate)`; what stays private stays
//! private on purpose — [`AnalyzerCtx::metadata`] is reachable only through
//! [`AnalyzerCtx::table`], for one — so the visibility marks *are* the
//! contract between this file and the modules.
//!
//!
//! # Name resolution
//! Resolution is driven by the two scope stacks in [`BinderCtx`]. A statement pushes
//! its frames on entry (WITH entries onto the CTE stack, the analyzed FROM
//! onto the FROM stack) and pops them on exit, and lookups scan
//! innermost-outward. That one rule yields the scoping semantics: a CTE
//! shadows a same-named table, and a subquery may reference columns of any
//! enclosing query (correlation).
//!
//! All three source kinds — tables, CTEs and FROM-subqueries — answer column
//! lookups through the same [`AttributeView`] interface, so the consumers
//! (column references, asterisk expansion) never care which kind they hit.
//! What a reference resolves to is a
//! [`BoundVar`](sql_ast_new_nodes::table_expression::BoundVar):
//! the `Rc`-shared
//! analyzed FROM plus (relation, column) positions, reading name and type
//! through the source on demand.
//!
//!
//! # Type inference
//! Delegated to the generic `sql_type_system` crate; the analyzer does no
//! type reasoning of its own. It builds a parallel "mirror" tree of type
//! expressions (constructors in [`expr`])
//! keyed by ids minted monotonically per statement, runs the type analyzer
//! once per expression root, and reads types back by stored id. Implicit
//! coercions the type system decides on are materialized as CAST nodes, so
//! they show up in the tree and its rendering.
//!
//!
//! # Unsupported inputs
//! The analyzer covers a much smaller subset than the parser. Anything it
//! does not handle is rejected with an "... is not supported yet" error —
//! never silently dropped. Broken internal invariants go through
//! [`analyze_invariant_error`] instead, like the parser's
//! `parse_invariant_error`.
//!
//!
//! # Relation to the parser
//! This crate and `sql-ast-new-parser` never reference each other; they are two
//! independent consumers of `sql-ast-new-nodes`.
//!
//! The `TypeSystem` registry stays in `sql-frontend`, which shares one instance
//! between this analyzer and the old pipeline. [`analyze`] therefore takes a
//! ready [`AstTypeAnalyzer`] rather than the parameter types it would be built
//! from. That is also why the tests here drive `sql-frontend`'s `Ast` trait
//! (parse, then analyze) instead of calling the parser directly: it is the only
//! place both halves are available. A dev-dependency, not a layering violation.
//!
//!
//! # Panics
//! This crate must never panic: the frontend falls back to the old pipeline on
//! [`Err`], but a panic aborts the whole query. Broken invariants return
//! [`analyze_invariant_error`].

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

mod asterisk;
mod cast;
mod expr;
mod frame;
mod multiset;
mod select;
mod table_expression;

use std::collections::HashSet;

use smol_str::{format_smolstr, SmolStr};

use sql_ast_new_nodes::error::{AstErr, AstResult, REPORT_TO_SUFFIX};
use sql_ast_new_nodes::table_expression::{AttributeView, From};
use sql_ast_new_nodes::{
    Analyzed, AnalyzedAst, AstNodeId, DqlStmt, NamedEntity, Node, Raw, RawAst,
};

use frame::Stmt;

use sql_ir::errors::{Action, Entity, SbroadError};
use sql_ir::ir::metadata::Metadata;
use sql_ir::ir::relation::Table;
use sql_ir::ir::types::DerivedType;
use sql_type_system::expr::Type as TypeSystemType;

/// The generic `sql_type_system` machinery, instantiated for the new AST: node
/// ids are the plain [`AstNodeId`] counter minted while walking the expression
/// tree, rather than references into an arena.
///
/// The aliases live here because the analyzer is the only consumer; the
/// `TypeSystem` registry they are parameterized over stays in `sql-frontend`,
/// which shares it with the old pipeline.
pub(crate) type AstTypeExpr = sql_type_system::expr::Expr<AstNodeId>;
pub(crate) type AstTypeExprKind = sql_type_system::expr::ExprKind<AstNodeId>;
pub type AstTypeAnalyzer = sql_type_system::type_system::TypeAnalyzer<'static, AstNodeId>;
pub(crate) type AstTypeReport = sql_type_system::TypeReport<AstNodeId>;

/// Analyze-stage error - user mistake.
pub(crate) fn analyze_error(msg: SmolStr) -> AstErr {
    SbroadError::FailedTo(Action::Analyze, Some(Entity::AST), msg).into()
}

fn upper_first(s: &SmolStr) -> SmolStr {
    let mut chars = s.chars();
    match chars.next() {
        None => s.clone(),
        Some(c) => c.to_uppercase().chain(chars).collect(),
    }
}

/// Analyze-stage internal-invariant violation (a bug in the analyzer, not a user mistake).
pub(crate) fn analyze_invariant_error(msg: SmolStr) -> AstErr {
    let msg = upper_first(&msg);
    analyze_error(format_smolstr!("{msg}. {REPORT_TO_SUFFIX}"))
}

/// Entry point: resolve names against `metadata` and infer expression types,
/// turning the raw tree into the analyzed one.
///
/// `type_analyzer` arrives ready-built because the `TypeSystem` registry it
/// borrows is shared with the old pipeline and lives in `sql-frontend`; see the
/// crate docs.
pub fn analyze<'q, M: Metadata>(
    ast: RawAst<'q>,
    metadata: &'q M,
    type_analyzer: AstTypeAnalyzer,
) -> AstResult<AnalyzedAst<'q>> {
    let mut meta = AnalyzerCtx::new(metadata, type_analyzer);
    let root = ast.root.analyze(&mut meta)?;
    Ok(AnalyzedAst::from_root(root))
}

/// Everything analysis threads through the tree walk: catalog access, the
/// type-system bridge and the scope stacks.
pub(crate) struct AnalyzerCtx<'q, M> {
    metadata: &'q M,
    pub(crate) type_system: TypeSystem,
    pub(crate) binder: BinderCtx<'q>,
}

pub(crate) struct TypeSystem {
    /// Type analyzer.
    pub(crate) analyzer: AstTypeAnalyzer,
    /// Monotonic id minted for every expression node analyzed in this statement. Must be unique
    /// across all expressions (the type analyzer caches/reports by id), so it is never reset.
    pub(crate) next_expr_id: AstNodeId,
    /// Type system context.
    pub(crate) ctx: TypeSystemCtx,
}

impl<'q, M: Metadata> AnalyzerCtx<'q, M> {
    /// Initializer analyzer context.
    fn new(metadata: &'q M, type_analyzer: AstTypeAnalyzer) -> Self {
        Self {
            metadata,
            type_system: TypeSystem {
                analyzer: type_analyzer,
                next_expr_id: 0,
                ctx: TypeSystemCtx {
                    proj: Vec::new(),
                    curr_proj: Vec::new(),
                    filters: Vec::new(),
                    post_t_derivation: false,
                },
            },
            binder: BinderCtx::default(),
        }
    }

    pub(crate) fn table(&self, name: &str) -> AstResult<Table> {
        self.metadata.table(name).map_err(AstErr::from)
    }
}

/// Scope stacks carried through the walk. Each nested statement pushes its
/// frame on entry and pops it on exit, and lookups scan innermost-outward —
/// which is what gives CTE shadowing and correlated subqueries (a subquery
/// referencing an enclosing FROM) their semantics.
#[derive(Default)]
pub(crate) struct BinderCtx<'q> {
    /// One frame per statement under analysis, innermost last.
    pub(crate) frames: Vec<Stmt<'q>>,

    /// Current mutable [`From`] frame accumulating entries.
    ///
    /// This is the mainstream way to analyze
    /// [`JoinedTable::using_cols`](sql_ast_new_nodes::table_expression::JoinedTable::using_cols).
    /// In [`From`] binding we add bound
    /// [`FromEntry::TableFactor`](sql_ast_new_nodes::table_expression::FromEntry::TableFactor)
    /// of [`joined table`](`sql_ast_new_nodes::table_expression::FromEntry::JoinedTable`)
    /// in order to resolve column references in join condition against this table factor.
    ///
    /// After join condition is bound this entry should be changed for bound
    /// [`joined table`](sql_ast_new_nodes::table_expression::FromEntry::JoinedTable).
    pub(crate) curr_from: From<'q, Analyzed>,
}

const EMPTY_FRAME_STACK_ERR: &str = "expected non-empty stack of statement frames";

impl<'q> BinderCtx<'q> {
    pub(crate) fn top_frame_ref(&self) -> AstResult<&Stmt<'q>> {
        self.frames
            .last()
            .ok_or_else(|| analyze_invariant_error(format_smolstr!("{EMPTY_FRAME_STACK_ERR}")))
    }

    pub(crate) fn top_frame_mut(&mut self) -> AstResult<&mut Stmt<'q>> {
        self.frames
            .last_mut()
            .ok_or_else(|| analyze_invariant_error(format_smolstr!("{EMPTY_FRAME_STACK_ERR}")))
    }

    pub(crate) fn push_frame(&mut self, frame: Stmt<'q>) {
        self.frames.push(frame);
    }

    pub(crate) fn pop_frame(&mut self, msg: Option<&'static str>) -> AstResult<Stmt<'q>> {
        self.frames.pop().ok_or_else(|| {
            analyze_invariant_error(format_smolstr!("{}", msg.unwrap_or(EMPTY_FRAME_STACK_ERR)))
        })
    }
}

/// Collection of type expressions to run type analysis on.
/// The main purpose is postponing type analysis for set operations (e.g. `UNION`).
/// For example, in pseudoquery `SELECT <e1> UNION (SELECT <e2> WHERE <e3>)`
/// `e1`, `e2` and `e3` roots are opaque type expressions to run analysis on.
/// Furthermore, `e1` and `e2` should be analyzed
/// as homogeneous expressions with intention to coerce their types.
#[derive(Default)]
pub(crate) struct TypeSystemCtx {
    /// Output types for each query frame in set operation tree
    /// or result types of values row in values statement.
    pub(crate) proj: Vec<Vec<AstTypeExpr>>,
    pub(crate) curr_proj: Vec<AstTypeExpr>,
    /// Accumulate expressions of `JOIN/ON`, `WHERE`.
    pub(crate) filters: Vec<AstTypeExpr>,
    /// Whether to postpone type derivation in statement.
    ///
    /// We derive types once per [`multiset statement`](sql_ast_new_nodes::multiset::MultisetStmt).
    /// When we are traversing set operation tree we should postpone type derivation until it is traversed.
    pub(crate) post_t_derivation: bool,
}

/// Type system mirror node for a source attribute.
pub(crate) fn texpr_from_attribute(attr: AttributeView, id: AstNodeId) -> AstTypeExpr {
    texpr_from_derived_type(attr.data_type(), id)
}

/// Type system mirror node for an already-derived type — see [`texpr_from_attribute`].
pub(crate) fn texpr_from_derived_type(d_type: DerivedType, id: AstNodeId) -> AstTypeExpr {
    let kind = d_type.get().map_or(AstTypeExprKind::Null, |ty| {
        AstTypeExprKind::Reference(TypeSystemType::from(ty))
    });
    AstTypeExpr::new(id, kind)
}

/// `Raw` → `Analyzed` conversion of one node kind. By value: analysis is a
/// conversion between two tree types, never in-place mutation.
pub(crate) trait Analyzer<'q> {
    type Node;
    /// Analysis of any AST node.
    ///
    /// This routine includes:
    /// - Resolve scopes
    /// - Resolve table schemas
    /// - Bind column references
    /// - Bind parameters
    /// - Derive expression types
    fn analyze<M>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::Node>
    where
        M: Metadata,
        Self: Sized;
}

/// Bind statement to SQL semantics consistent with.
pub(crate) trait Binder<'q> {
    type BoundNode;
    /// Do the semantic for the expression.
    ///
    /// This routine includes:
    /// - Resolve scopes (table refs, column refs).
    /// - Resolve table schemas.
    /// - Resolve parameters.
    /// - Analyze subqueries.
    /// - Enrich [type system context](`AnalyzerCtx::type_system`) with [`AstTypeExpr`]s.
    fn bind<M>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode>
    where
        M: Metadata,
        Self: Sized;
}

pub(crate) trait TypeDeriver {
    /// Derive types in statement.
    ///
    /// - Run type system analyzer on type expressions.
    /// - Adjust data types for expressions in statement (UNION, VALUES etc.).
    fn derive_types(&mut self, type_system: &mut TypeSystem) -> AstResult<()>;
}

/// Reading analysis results back into the tree.
pub(crate) trait ExprTypeDeriver {
    /// Derive types in expressions.
    ///
    /// - Retrieve types from type system report
    /// - Apply coercions if needed and validate their correctness.
    fn derive_types(&mut self, type_report: &AstTypeReport) -> AstResult<()>;
}

impl<'q> Analyzer<'q> for Node<'q, Raw> {
    type Node = Node<'q, Analyzed>;

    fn analyze<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::Node> {
        match self {
            Node::Empty => Err(analyze_error(format_smolstr!("expected non-empty tree"))),
            Node::DqlStmt(dql_stmt) => {
                let (stmt, option) = dql_stmt.into_parts();
                let stmt = stmt.analyze(meta)?;
                Ok(Node::DqlStmt(DqlStmt::from_parts(Box::new(stmt), option)))
            }
        }
    }
}

pub(crate) fn duplicated_name(named_entities: &[impl NamedEntity]) -> Option<&str> {
    let mut seen = HashSet::new();
    named_entities
        .iter()
        .filter_map(NamedEntity::name)
        .find(|name| !seen.insert(*name))
}

#[cfg(test)]
mod tests;
