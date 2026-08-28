//! Analysis of the multiset layer: the WITH clause, and the set-operation tree
//! (`UNION`/`EXCEPT`/`INTERSECT`) whose leaves are the query's SELECTs.
//!
//! This is the layer the [`Binder`]/[`TypeDeriver`] split exists for. A set
//! operation types its branches *together* — one output column takes a single
//! type across every operand — so the whole tree is bound first and types are
//! derived once, at its root. The walk flips
//! [`post_t_derivation`](crate::TypeSystemCtx::post_t_derivation) on the way in
//! to hold derivation back until the tree is complete; a statement that is not
//! an operation derives its types right away.
//!
//! CTEs are analyzed here too. Each one joins the enclosing
//! [`ctes`](Stmt::ctes) frame as soon as it is analyzed, so the
//! bodies of later CTEs in the same WITH clause can reference it.

use std::rc::Rc;

use smol_str::format_smolstr;

use sql_ast_new_nodes::error::AstResult;
use sql_ast_new_nodes::expr::ValuesRow;
use sql_ast_new_nodes::multiset::{Cte, Ctes, MultisetInner, MultisetStmt, Operation, ValuesStmt};
use sql_ast_new_nodes::table_expression::From;
use sql_ast_new_nodes::{Analyzed, Raw};
use sql_ir::ir::metadata::Metadata;
use sql_ir::ir::relation::{Column, ColumnRole};
use sql_ir::ir::types::DerivedType;
use sql_type_system::expr::Type as TypeSystemType;

use crate::{
    analyze_error, analyze_invariant_error, duplicated_name, frame::Stmt, Analyzer, AnalyzerCtx,
    AstTypeReport, Binder, ExprTypeDeriver, TypeDeriver, TypeSystem,
};

impl<'q> Analyzer<'q> for MultisetStmt<'q, Raw> {
    type Node = MultisetStmt<'q, Analyzed>;

    fn analyze<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::Node> {
        // Decide whether we should postpone type derivation until set operation tree root.
        let parent_post_t_derivation = std::mem::replace(
            &mut meta.type_system.ctx.post_t_derivation,
            matches!(self.inner, MultisetInner::Operation(_)),
        );

        // Bind statement.
        let mut bound_stmt = self.bind(meta)?;

        // If we are allowed to run type derivation do it.
        if !parent_post_t_derivation {
            bound_stmt.derive_types(&mut meta.type_system)?;
        }

        // Restore type derivation postpone mode for outer statement.
        meta.type_system.ctx.post_t_derivation = parent_post_t_derivation;

        Ok(bound_stmt)
    }
}

impl<'q> Binder<'q> for MultisetStmt<'q, Raw> {
    type BoundNode = MultisetStmt<'q, Analyzed>;

    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        let (ctes, inner, order_by, limit) = self.into_parts();

        if order_by.is_some() {
            return Err(analyze_error(format_smolstr!(
                "ORDER BY is not supported yet"
            )));
        }

        // Push new statement frame into context stack.
        meta.binder.push_frame(Stmt::new(From::empty()));

        // WITH clause is opaque boundary for analysis (binding + type derivation).
        let ctes = ctes.analyze(meta)?;

        let mut inner = match inner {
            MultisetInner::Select(stmt) => MultisetInner::Select(stmt.bind(meta)?),
            MultisetInner::Values(values) => MultisetInner::Values(values.bind(meta)?),
            MultisetInner::Operation(operation) => MultisetInner::Operation(operation.bind(meta)?),
        };

        // Pop bound statement frame from context stack.
        let mut frame = meta.binder.pop_frame(None)?;

        // A SELECT parks its bound select list and FROM clause on the frame;
        // put both back into the statement.
        if let MultisetInner::Select(stmt) = &mut inner {
            stmt.set_sel_lst(frame.take_bound_sel_lst()?);
            stmt.set_from(std::mem::take(&mut frame.from));
        }

        Ok(Self::BoundNode::from_parts(ctes, inner, None, limit))
    }
}

/// Bind a `VALUES` table constructor.
///
/// Row expressions are bound against the enclosing scopes only — a `VALUES`
/// has no FROM of its own, so plain references fail,
/// while outer references (a correlated `(VALUES (t1.a))` subquery) resolve as usual.
///
/// Every row lands in [`TypeSystemCtx::proj`](crate::TypeSystemCtx::proj) as
/// one projection frame, so the statement-root derivation unifies the rows
/// column by column — the same machinery, and the same common-type rule, a set
/// operation uses for its branches.
///
/// The derived output columns are named `column1..columnN`. Their types stay
/// unknown until [`ExprTypeDeriver`] fills them after the root derivation.
///
/// Rows are bound before their length is compared to the first row's, so a row
/// that is both malformed and mis-sized reports the expression error.
impl<'q> Binder<'q> for ValuesStmt<'q, Raw> {
    type BoundNode = ValuesStmt<'q, Analyzed>;

    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        let rows = self.into_parts();

        let mut width: Option<usize> = None;
        let mut bound_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let exprs = row.into_parts();

            let mut bound_exprs = Vec::with_capacity(exprs.len());
            let mut t_exprs = Vec::with_capacity(exprs.len());
            for expr in exprs {
                let (bound_expr, t_expr) = expr.bind(meta)?;
                bound_exprs.push(bound_expr);
                t_exprs.push(t_expr);
            }

            match width {
                None => width = Some(bound_exprs.len()),
                Some(width) if width != bound_exprs.len() => {
                    return Err(analyze_error(format_smolstr!(
                        "VALUES lists must all be the same length"
                    )))
                }
                Some(_) => {}
            }

            meta.type_system.ctx.proj.push(t_exprs);
            bound_rows.push(ValuesRow::from_parts(bound_exprs));
        }

        let width = width.ok_or_else(|| {
            analyze_invariant_error(format_smolstr!(
                "parsing guarantees at least one VALUES row"
            ))
        })?;
        let columns = (1..=width)
            .map(|pos| {
                Column::new(
                    &format_smolstr!("column{pos}"),
                    DerivedType::unknown(),
                    ColumnRole::User,
                    true,
                )
            })
            .collect();

        Ok(ValuesStmt::from_parts(bound_rows, columns))
    }
}

impl ExprTypeDeriver for ValuesStmt<'_, Analyzed> {
    fn derive_types(&mut self, type_report: &AstTypeReport) -> AstResult<()> {
        let (rows, columns) = self.parts_mut();

        for row in rows.iter_mut() {
            for expr in row.parts_mut() {
                expr.derive_types(type_report)?;
            }
        }

        // Column `k` takes the type of the first row's `k`-th expression:
        // after the column-wise homogeneous analysis every row's expression
        // carries the common type, materialized casts included.
        if let Some(first_row) = rows.first() {
            for (column, expr) in columns.iter_mut().zip(first_row.values_ref()) {
                column.r#type = expr.data_type();
            }
        }

        Ok(())
    }
}

fn transpose<T>(matrix: Vec<Vec<T>>) -> Vec<Vec<T>> {
    if matrix.is_empty() {
        return vec![];
    }

    let cols = matrix[0].len();
    let mut result: Vec<Vec<T>> = (0..cols)
        .map(|_| Vec::with_capacity(matrix.len()))
        .collect();

    for row in matrix {
        for (col, value) in row.into_iter().enumerate() {
            result[col].push(value);
        }
    }

    result
}

impl TypeDeriver for MultisetStmt<'_, Analyzed> {
    /// Derive types in left to right manner.
    fn derive_types(&mut self, type_system: &mut TypeSystem) -> AstResult<()> {
        let inner = self.inner_mut();

        // Derive types for the filtering expressions (`JOIN/ON`, `WHERE`, `HAVING`)
        for expr in std::mem::take(&mut type_system.ctx.filters).into_iter() {
            type_system
                .analyzer
                .analyze(&expr, Some(TypeSystemType::Boolean))?;
        }

        // We derive types of filters before projections
        // because usually former ones represent more complex type requirements.

        // Transpose statements projection elements in order to
        // derive them as homogeneous expressions.
        // For instance in query `SELECT 1, 2 UNION SELECT '1', '2'`
        // Original `proj_exprs` is `[[1, 2], ['1', '2']]`
        // we transform them into `[[1, '1'], [2, '2']]`
        // A VALUES statement lands here the same way, one frame per row,
        // so its rows unify column by column under the same rule.
        let proj = transpose(std::mem::take(&mut type_system.ctx.proj));

        // How the failed unification names its clause. A set operation over
        // VALUES flattens the rows into the branch groups, so a mismatch
        // between two rows of one VALUES branch is reported as the operation's
        // — a divergence from PostgreSQL, which unifies each VALUES first.
        let homogeneous_ctx = match inner {
            MultisetInner::Values(_) => "VALUES",
            _ => "UNION/EXCEPT/INTERSECT",
        };

        for attrs in proj {
            type_system
                .analyzer
                .analyze_homogeneous_exprs(homogeneous_ctx, &attrs, None)?;
        }

        // Adjust expressions inside statement types according to type system report.
        match inner {
            MultisetInner::Select(stmt) => stmt.derive_types(type_system.analyzer.get_report())?,
            MultisetInner::Values(values) => {
                values.derive_types(type_system.analyzer.get_report())?
            }
            MultisetInner::Operation(operation) => {
                operation.left.derive_types(type_system)?;
                operation.right.derive_types(type_system)?;
            }
        }

        Ok(())
    }
}

impl<'q> Analyzer<'q> for Ctes<'q, Raw> {
    type Node = Ctes<'q, Analyzed>;

    fn analyze<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::Node> {
        let raw_ctes = self.into();

        // Detect duplicating CTE name definition if any.
        if let Some(name) = duplicated_name(&raw_ctes) {
            return Err(analyze_error(format_smolstr!(
                r#"WITH query name "{name}" specified more than once"#
            )));
        }

        let mut analyzed_ctes = Vec::with_capacity(raw_ctes.len());
        for cte in raw_ctes {
            let analyzed_cte = Rc::new(cte.analyze(meta)?);
            // Every analyzed CTE joins the current scope right away,
            // so bodies of subsequent CTEs in the same WITH clause can reference it.
            meta.binder
                .top_frame_mut()?
                .ctes
                .add(Rc::clone(&analyzed_cte));
            analyzed_ctes.push(analyzed_cte);
        }
        Ok(Ctes::from(analyzed_ctes))
    }
}

impl<'q> Analyzer<'q> for Cte<'q, Raw> {
    type Node = Cte<'q, Analyzed>;

    /// An explicit column list must match the body's column count:
    /// a shorter or longer list would silently misalign every position-based lookup made through it later.
    fn analyze<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::Node> {
        let (name, columns, raw_body) = self.into_parts();
        let analyzed_body = analyze_subquery(raw_body, meta)?;

        let actual_col_cnt = analyzed_body.result_columns_cnt()?;
        let cte_def_col_cnt = columns.len();
        if !columns.is_empty() && cte_def_col_cnt != actual_col_cnt {
            return Err(analyze_error(format_smolstr!(
                r#"WITH query "{}" has {actual_col_cnt} columns available but {cte_def_col_cnt} columns specified"#,
                name.as_str()
            )));
        }
        Ok(Cte::from_parts(name, columns, analyzed_body))
    }
}

impl<'q> Binder<'q> for Operation<'q, Raw> {
    type BoundNode = Operation<'q, Analyzed>;

    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        let (left, op, dup, right) = self.into_parts();

        let left_bound = left.bind(meta)?;
        let right_bound = right.bind(meta)?;

        if left_bound.result_columns_cnt()? != right_bound.result_columns_cnt()? {
            return Err(analyze_error(format_smolstr!(
                "operands of {op} operation have different number of columns"
            )));
        }

        Ok(Operation::from_parts(
            Box::new(left_bound),
            op,
            dup,
            Box::new(right_bound),
        ))
    }
}

/// Analyze a subquery: a nested statement, and therefore a boundary for
/// binding — the type expressions collected for the enclosing statement are
/// parked while the inner one runs and restored afterwards, so its own roots
/// never leak into the outer statement's derivation.
pub(crate) fn analyze_subquery<'q, M: Metadata>(
    subquery: MultisetStmt<'q, Raw>,
    meta: &mut AnalyzerCtx<'q, M>,
) -> AstResult<MultisetStmt<'q, Analyzed>> {
    // Preserve context about expressions to analyze in current
    // multiset statement tree. Subquery is boundary for binding.
    let saved_ts_ctx = std::mem::take(&mut meta.type_system.ctx);
    let res = subquery.analyze(meta)?;
    meta.type_system.ctx = saved_ts_ctx;
    Ok(res)
}
