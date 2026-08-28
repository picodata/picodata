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
//! | [`select`] | SELECT statement, its select list, WHERE / HAVING |
//! | [`table_expression`] | FROM clause, table factors, joins, GROUP BY |
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
//! What a reference resolves to is a [`BoundVar`]:
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
use sql_ast_new_nodes::expr::Expr;
use sql_ast_new_nodes::multiset::MultisetInner;
use sql_ast_new_nodes::table_expression::{AttributeView, BoundVar, From};
use sql_ast_new_nodes::{
    Analyzed, AnalyzedAst, AstNodeId, DqlStmt, NamedEntity, Node, Raw, RawAst,
};

use frame::{GroupingKeyMark, Stage, Stmt};

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
                    rest: Vec::new(),
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

    /// Aggregate calls whose arguments are under analysis, innermost last.
    pub(crate) aggrs: Vec<AggrCtx<'q>>,

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

    /// Target tuple source for ORDER BY
    stmt_body: Option<MultisetInner<'q, Analyzed>>,
}

const EMPTY_FRAME_STACK_ERR: &str = "expected non-empty stack of statement frames";
const MISSING_STMT_BODY_ERR: &str = "missing multiset statement body in analyzer context";

impl<'q> BinderCtx<'q> {
    pub(crate) fn top_frame_ref(&self) -> AstResult<&Stmt<'q>> {
        self.frames
            .last()
            .ok_or_else(|| analyze_invariant_error(format_smolstr!("{EMPTY_FRAME_STACK_ERR}")))
    }

    pub(crate) fn get_frame_mut(&mut self, idx: usize) -> AstResult<&mut Stmt<'q>> {
        self.frames.get_mut(idx).ok_or_else(|| {
            analyze_invariant_error(format_smolstr!(
                "expected to find statement frame with index '{idx}'"
            ))
        })
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

    pub(crate) fn depth(&self) -> AstResult<usize> {
        self.frames.len().checked_sub(1).ok_or_else(|| {
            analyze_invariant_error(format_smolstr!("aggregate call outside of any query level"))
        })
    }

    /// The aggregate calls a reference resolving to `lvl` is visible to.
    ///
    /// A level deeper than a call's own level belongs to a subquery of that call.
    fn aggrs_seeing(&mut self, lvl: usize) -> impl Iterator<Item = &mut AggrCtx<'q>> {
        self.aggrs.iter_mut().filter(move |aggr| lvl <= aggr.depth)
    }

    /// Register the level a column reference resolved to with every aggregate
    /// call whose arguments enclose it — not just the innermost one.
    pub(crate) fn update_col_sem_lvl(&mut self, lvl: usize) {
        for aggr in self.aggrs_seeing(lvl) {
            aggr.col_sem_lvl = aggr.col_sem_lvl.max(Some(lvl));
        }
    }

    /// Register the level a finished aggregate call belongs to with every call still enclosing it.
    fn update_aggr_sem_lvl(&mut self, lvl: usize) {
        for aggr in self.aggrs_seeing(lvl) {
            aggr.aggr_sem_lvl = aggr.aggr_sem_lvl.max(Some(lvl));
        }
    }

    /// Register the level a CTE referenced in the arguments is defined at with
    /// every aggregate call whose arguments enclose the reference. A CTE local
    /// to a subquery of the call is not the call's business and is skipped.
    pub(crate) fn update_cte_lvl(&mut self, lvl: usize) {
        for aggr in self.aggrs_seeing(lvl) {
            aggr.cte_lvl = aggr.cte_lvl.max(Some(lvl));
        }
    }

    pub(crate) fn init_aggr(&mut self) -> AstResult<()> {
        // The innermost frame is the query level this call is written at.
        let depth = self.depth()?;

        self.aggrs.push(AggrCtx::new(depth));

        Ok(())
    }

    /// Pop just bound aggregate function from aggregate context stack.
    /// Validate its usage. Check for nested aggregates, aggregates in clauses
    /// and enrich [`target column references of frame`](`Stmt::target_vars`).
    pub(crate) fn finalize_aggr(&mut self) -> AstResult<()> {
        let aggr = self.aggrs.pop().ok_or_else(|| {
            analyze_invariant_error(format_smolstr!(
                "expected to find context for current aggregate function"
            ))
        })?;

        let sem_lvl = aggr.sem_lvl();

        // Get frame which is aggregated. Mutability for setting aggregated flag.
        let frame = self.get_frame_mut(sem_lvl)?;

        // Check that aggregate function usage is allowed in current scope.
        if let Some(clause) = frame.stage.forbid_aggr_clause() {
            return Err(analyze_error(format_smolstr!(
                "aggregate functions are not allowed in {clause}"
            )));
        }

        // All the arguments are bound by now.
        aggr.validate_nested_aggrs()?;
        aggr.validate_nested_ctes()?;

        // Set aggregated flag.
        frame.is_aggr = true;

        // Enrich target and aggregated column references of each outer frame.
        aggr.vars.into_iter().enumerate().for_each(|(lvl, vars)| {
            let Some(frame) = self.frames.get_mut(lvl) else {
                return;
            };
            if lvl == sem_lvl {
                return;
            }
            vars.into_iter()
                .for_each(|var| match self.aggrs.last_mut() {
                    // This is the case when we cannot decide whether this column reference
                    // is aggregated anywhere or not. For instance, in this query
                    // `SELECT (SELECT sum((SELECT max(t3.a + t2.c) FROM t3) + count(t1.a)) FROM t2) FROM t1`
                    // column `t2.c` should be delegated from `max` aggregate to its parent `sum`.
                    Some(parent) if lvl <= parent.depth => parent.add_var(lvl, var),
                    // Deeper than the enclosing call - it can never be that call's
                    // semantic level, so it is a plain reference of its own frame,
                    // exactly as `reg_var` treats one met outside any call.
                    // Handing it to the parent would drop it: the parent's
                    // `AggrCtx::vars` only extends through its own depth.
                    // E.g. in
                    // `SELECT sum((SELECT (SELECT max(t2.c + t3.a) FROM t1 t3) FROM t2 HAVING true)) FROM t1`
                    // `max` belongs to `t3` and `t2.c` must still be reported as
                    // ungrouped in the `t2` level, past the `sum` written at `t1`.
                    _ => frame.add_target_var(var),
                })
        });

        // Encounter just bound aggregate function in every enclosing one.
        self.update_aggr_sem_lvl(sem_lvl);

        Ok(())
    }

    /// Register usage of bound column reference.
    /// This can be [`target column reference`](`Stmt::target_vars`).
    ///
    /// If we are inside aggregate postpone its registration until
    /// aggregate semantic level becomes known.
    /// Otherwise, just add it into targets of its frame.
    ///
    /// This routine enrich context for
    /// 1. Validation of correct aggregate usage - nesting rules and improper
    ///    places whose enclosing frames are still under analysis.
    ///    E.g. invalid `SELECT (SELECT t2.a FROM t1 t2 WHERE sum(t2.a) > 1) FROM t1`
    ///    returns error "aggregate functions are not allowed in WHERE" while valid
    ///    `SELECT (SELECT t2.a FROM t1 t2 WHERE sum(t1.a) > 1) FROM t1` is OK.
    /// 2. Accounting aggregated and target column references.
    pub(crate) fn reg_var(&mut self, lvl: usize, var: BoundVar<'q>) -> AstResult<()> {
        self.update_col_sem_lvl(lvl);

        if let Some(aggr) = self.aggrs.last_mut() {
            if lvl > aggr.depth {
                // Inner subquery column is reference. We can
                // immediately register it into corresponding frame.
                // For instance in query
                // `SELECT max(t1.a + (SELECT t2.a FROM t1 t2)) FROM t1;`
                // column `t2.a` is plain reference for `t2` level.
                if let Some(frame) = self.frames.get_mut(lvl) {
                    // Add target column reference.
                    frame.add_target_var(var);
                }
            } else {
                aggr.add_var(lvl, var);
            }
        } else if let Some(frame) = self.frames.get_mut(lvl) {
            // Add target column reference.
            frame.add_target_var(var);
        }
        Ok(())
    }

    /// Where the column references of each level would have to be rewound to if the
    /// expression about to be bound turns out to be a grouping key. Indexed by level,
    /// [`None`] for a level whose current clause has no grouping keys to match against -
    /// only a projection or a HAVING does, which is what keeps the rewind exactly this narrow.
    pub(crate) fn grouping_key_marks(&self) -> Vec<Option<GroupingKeyMark>> {
        self.frames
            .iter()
            .enumerate()
            .map(|(lvl, frame)| {
                if frame.stage.forbid_ungrouped() {
                    Some(GroupingKeyMark {
                        frame: frame.target_vars.len(),
                        aggr: self
                            .aggrs
                            .last()
                            .and_then(|aggr| aggr.vars.get(lvl))
                            .map(Vec::len),
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<Option<GroupingKeyMark>>>()
    }

    /// Drop the column references a just-bound grouping key accounts for, in every level
    /// the key belongs to. The marks span every open level, so a key spelled inside a
    /// subquery is matched against the keys of the outer level it belongs to and covers
    /// its columns there, the way a grouped column does.
    /// Both places a reference can be waiting in have to be rewound:
    /// the level's own [`Stmt::target_vars`], and - when the key sits inside aggregate
    /// arguments - the [`parked columns`](`AggrCtx::vars`) of the enclosing call, which only
    /// reach the frame in [`Self::finalize_aggr`], long after this runs.
    ///
    /// Reading the marks against the innermost open call is enough: aggregate contexts are
    /// pushed and popped within the bound subtree, and a call nested in it hands its columns
    /// to that same enclosing call when it finalizes.
    pub(crate) fn drop_grouping_key_vars(
        &mut self,
        marks: &[Option<GroupingKeyMark>],
        expr: &Expr<'q, Analyzed>,
    ) {
        for (lvl, mark) in marks
            .iter()
            .enumerate()
            .filter_map(|(lvl, mark)| mark.as_ref().map(|mark| (lvl, mark)))
        {
            let Some(frame) = self.frames.get_mut(lvl) else {
                continue;
            };
            if !frame.grouping_covers(expr) {
                continue;
            }
            frame.target_vars.truncate(mark.frame);

            // The key's columns must not reach the frame through the enclosing call either.
            // Its semantic level is read off `col_sem_lvl`, untouched here, so the call still
            // belongs to the level it did and only loses these columns' grouping duty.
            if let (Some(aggr_mark), Some(aggr)) = (mark.aggr, self.aggrs.last_mut()) {
                if let Some(vars) = aggr.vars.get_mut(lvl) {
                    vars.truncate(aggr_mark);
                }
            }
        }
    }

    pub(crate) fn set_stmt_body(&mut self, body: MultisetInner<'q, Analyzed>) {
        self.stmt_body = Some(body);
    }

    pub(crate) fn stmt_body_mut(&mut self) -> AstResult<&mut MultisetInner<'q, Analyzed>> {
        self.stmt_body
            .as_mut()
            .ok_or_else(|| analyze_invariant_error(format_smolstr!("{MISSING_STMT_BODY_ERR}")))
    }

    pub(crate) fn take_stmt_body(&mut self) -> AstResult<MultisetInner<'q, Analyzed>> {
        std::mem::take(&mut self.stmt_body)
            .ok_or_else(|| analyze_invariant_error(format_smolstr!("{MISSING_STMT_BODY_ERR}")))
    }
}

/// One entry per aggregate call whose arguments are under analysis.
///
/// Nesting is a semantic property, not a syntactic one.
/// A call belongs to the innermost query level it reads, and is rejected
/// when a call nested in its arguments belongs to that same level.
/// So the very same pair of calls is legal or illegal depending on
/// what the two of them read.
///
/// Levels are held here as absolute *levels* — the index in `from_frames` of
/// the query level a reference resolves to, not hops up from the call that
/// observed it. An index means the same thing to every call that can see it,
/// which is what lets each of the two fields below be a single number.
#[derive(Default)]
pub(crate) struct AggrCtx<'q> {
    /// The index in `from_frames` of the level the call itself is written at.
    pub(crate) depth: usize,
    /// The deepest level, no deeper than [`Self::depth`], that a column read in
    /// the arguments resolves to. [`None`] until such a column is met.
    col_sem_lvl: Option<usize>,
    /// The deepest level, no deeper than [`Self::depth`], that an aggregate
    /// nested in the arguments belongs to. [`None`] when there is none.
    aggr_sem_lvl: Option<usize>,
    /// The deepest level, no deeper than [`Self::depth`], at which a CTE
    /// referenced in the arguments is defined. [`None`] when there is none.
    cte_lvl: Option<usize>,
    /// Referenced columns. Indexed by frame level.
    ///
    /// Ordered rather than a set, because a grouping key bound inside these
    /// arguments rewinds the columns it accounts for by position - see
    /// [`BinderCtx::drop_grouping_key_vars`].
    vars: Vec<Vec<BoundVar<'q>>>,
}

impl<'q> AggrCtx<'q> {
    pub(crate) fn new(depth: usize) -> Self {
        Self {
            depth,
            vars: vec![Vec::new(); depth + 1],
            ..Default::default()
        }
    }

    /// The query level this call belongs to: the deepest level it reads, or the
    /// one it is written at when it reads nothing at all (`count(*)`, `sum(1)`).
    pub(crate) fn sem_lvl(&self) -> usize {
        self.col_sem_lvl
            .max(self.aggr_sem_lvl)
            .unwrap_or(self.depth)
    }

    /// Reject when the level this call belongs to is one a nested call brought in.
    /// A tie counts as nested, which is the plain `sum(max(a))` case.
    pub(crate) fn validate_nested_aggrs(&self) -> AstResult<()> {
        if self.aggr_sem_lvl == Some(self.sem_lvl()) {
            return Err(analyze_error(format_smolstr!(
                "aggregate function calls cannot be nested"
            )));
        }
        Ok(())
    }

    /// Reject when a CTE referenced in the arguments is defined below the level
    /// the call belongs to, as PostgreSQL does: the call would have to be
    /// evaluated at a level where the CTE is not in scope. E.g. in
    ///
    /// ```sql
    /// SELECT a, (WITH cte AS (SELECT 1) SELECT max((SELECT t1.a FROM cte))) FROM t1 GROUP BY a
    /// ```
    ///
    /// `max` belongs to the `t1` level, but `cte` is defined in the subquery.
    pub(crate) fn validate_nested_ctes(&self) -> AstResult<()> {
        if self.cte_lvl.is_some_and(|lvl| lvl > self.sem_lvl()) {
            return Err(analyze_error(format_smolstr!(
                "outer-level aggregate cannot use a nested CTE"
            )));
        }
        Ok(())
    }

    /// Record usage of bound column reference.
    ///
    /// We can say whether this column reference is aggregated or not only
    /// when current aggregate arguments are bound and semantic level is known.
    ///
    /// For instance, in this query
    ///
    /// ```sql
    /// SELECT (SELECT max(t1.a + 1) FROM t1 t2) FROM t1
    /// ```
    ///
    /// semantic level of `max` is 0 (`t1`) and so `t1.b` (if used) must be grouped
    /// or used within aggregate.
    ///
    /// While in query
    ///
    /// ```sql
    /// SELECT (SELECT max(t1.a + t2.b) FROM t1 t2), t1.b FROM t1
    /// ```
    ///
    /// semantic level of `max` is 1 (`t2`) which implies level `t1` is not grouped.
    /// Therefore, using ungrouped and unaggregated `t1.b` is valid.
    pub(crate) fn add_var(&mut self, lvl: usize, var: BoundVar<'q>) {
        if let Some(frame_vars) = self.vars.get_mut(lvl) {
            if !frame_vars.contains(&var) {
                frame_vars.push(var);
            }
        }
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
    /// Accumulate expressions of `JOIN/ON`, `WHERE`, `HAVING`.
    pub(crate) filters: Vec<AstTypeExpr>,
    /// Accumulate expressions of `GROUP BY`, `ORDER BY`, etc.
    pub(crate) rest: Vec<AstTypeExpr>,
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

/// Binding of some nodes sharing one frame is dependent on other nodes context.
///
/// That is why we need run discovery of raw parsed nodes.
trait Discoverer<'q, M: Metadata> {
    /// Enrich [`context`](`AnalyzerCtx`) with some helping info.
    ///
    /// For instance, discovery of GROUP BY enrich potential aliases and ordinal positions.
    fn discover(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self>
    where
        Self: Sized;
}

/// Bind statement to SQL semantics consistent with.
pub(crate) trait Binder<'q> {
    type BoundNode;
    /// Do the semantic for the expression.
    ///
    /// This routine includes:
    /// - Resolve scopes (table refs, column refs, named window refs).
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
