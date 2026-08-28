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
use sql_ast_new_nodes::expr::{ExprInner, ValuesRow};
use sql_ast_new_nodes::multiset::{
    Cte, Ctes, MultisetInner, MultisetStmt, Operation, OrderBy, OrderByElement, ValuesStmt,
};
use sql_ast_new_nodes::table_expression::{From, OrdrByGrpByElem};
use sql_ast_new_nodes::{Analyzed, NamedEntity, Raw};
use sql_ir::ir::metadata::Metadata;
use sql_ir::ir::relation::{Column, ColumnRole};
use sql_ir::ir::types::DerivedType;
use sql_type_system::expr::Type as TypeSystemType;

use crate::{
    analyze_error, analyze_invariant_error, duplicated_name, frame::Stage, frame::Stmt, Analyzer,
    AnalyzerCtx, AstTypeReport, Binder, ExprTypeDeriver, TypeDeriver, TypeSystem,
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

        // Push new statement frame into context stack.
        meta.binder
            .push_frame(Stmt::new(From::empty(), Stage::Projection));

        // WITH clause is opaque boundary for analysis (binding + type derivation).
        let ctes = ctes.analyze(meta)?;

        meta.binder.stmt_body = match inner {
            MultisetInner::Select(stmt) => Some(MultisetInner::Select(stmt.bind(meta)?)),
            MultisetInner::Values(values) => Some(MultisetInner::Values(values.bind(meta)?)),
            MultisetInner::Operation(operation) => {
                Some(MultisetInner::Operation(operation.bind(meta)?))
            }
        };

        let order_by = if let Some(order_by) = order_by {
            Some(order_by.bind(meta)?)
        } else {
            None
        };

        // Pop bound statement frame from context stack.
        let mut frame = meta.binder.pop_frame(None)?;

        // If inner statement is SELECT do the following:
        // 1. Validate grouping expressions
        // 2. Push back FROM clause out of frame context
        if let MultisetInner::Select(stmt) = meta.binder.stmt_body_mut()? {
            // Put back select list into SELECT statement.
            stmt.set_sel_lst(frame.take_bound_sel_lst()?);

            // Put back FROM clause into SELECT statement.
            stmt.set_from(std::mem::take(&mut frame.from));

            // If GROUP BY is present put it back into SELECT statement.
            if let Ok(grby) = frame.take_bound_grby() {
                stmt.set_grby(grby);
            }

            frame.check_grouping(stmt)?;
        }

        Ok(Self::BoundNode::from_parts(
            ctes,
            meta.binder.take_stmt_body()?,
            order_by,
            limit,
        ))
    }
}

/// Bind a `VALUES` table constructor.
///
/// Row expressions are bound against the enclosing scopes only — a `VALUES`
/// has no FROM of its own, so plain references fail,
/// while outer references (a correlated `(VALUES (t1.a))` subquery) resolve as usual.
/// The frame stage names the clause for the aggregate rejection: an aggregate call
/// *belonging* to the `VALUES` level is `aggregate functions are not allowed in VALUES`,
/// but one whose columns anchor it to an enclosing query stays legal there.
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

        meta.binder.top_frame_mut()?.set_stage(Stage::Values);

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

        // Derive types for the filtering expressions (`GROUP BY`, `ORDER BY`, etc.)
        for expr in std::mem::take(&mut type_system.ctx.rest).into_iter() {
            type_system.analyzer.analyze(&expr, None)?;
        }

        // We derive types of filters, grouping, ordering expressions before projections
        // because usually former ones represont more complex type requirements.

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

        if let Some(order_by) = &mut self.order_by {
            order_by.derive_types(type_system.analyzer.get_report())?;
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

/// Bind ORDER BY against the body it sorts.
///
/// Resolves an ORDER BY element in three steps
///
/// 1. a bare, unqualified name is matched against the *output column names* -
///    an element's explicit alias, or the name its expression exposes on its own.
///    Several matches are an error only when they are not the same expression.
/// 2. an integer constant is a 1-based position over those same output
///    columns, and out of range is an error;
/// 3. anything else is an ordinary expression over the input scope, and sorting
///    by it adds a hidden output column.
///
/// Which of the three a body admits differs. A set operation has output columns
/// but no input scope of its own, so only steps 1 and 2 apply there and step 3
/// is the `invalid UNION/INTERSECT/EXCEPT ORDER BY clause` rejection. And under
/// `SELECT DISTINCT` step 3 is rejected as well: the sort runs on the distinct
/// groups, so a sort key that is not itself projected is not well defined.
///
/// A `VALUES` body sorts by its derived output columns: steps 1 and 2 match
/// names and ordinals against `column1..columnN`. In step 3 a column reference
/// still binds normally — an outer reference of a correlated subquery resolves
/// there, an unknown name fails like anywhere else — but any other expression
/// would have to resolve against the VALUES columns, which are not exposed as
/// a scope yet, so it is rejected as unsupported (PostgreSQL accepts it).
impl<'q> Binder<'q> for OrderBy<'q, Raw> {
    type BoundNode = OrderBy<'q, Analyzed>;

    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        meta.binder.top_frame_mut()?.set_stage(Stage::OrderBy);

        let body = meta.binder.take_stmt_body()?;

        // The bodies exposing output names of their own rather than a select
        // list; what each admits past those names differs below.
        let body_names = match &body {
            MultisetInner::Operation(operation) => Some(operation.left.output_names()?),
            MultisetInner::Values(values) => Some(values.output_names()),
            MultisetInner::Select(_) => None,
        };
        let is_values = matches!(&body, MultisetInner::Values(_));

        let out_cnt = match body_names.as_ref() {
            Some(names) => names.len(),
            None => meta.binder.top_frame_ref()?.bound_sel_lst_ref()?.len(),
        };

        let mut bound_elems = Vec::<OrderByElement<'q, Analyzed>>::with_capacity(self.len());
        for elem in self.into_elements() {
            let bound_elem = match elem.expr {
                OrdrByGrpByElem::Ordinal(pos) => {
                    if pos >= out_cnt {
                        return Err(analyze_error(format_smolstr!(
                            "ORDER BY position {} is not in select list",
                            pos + 1
                        )));
                    }
                    OrdrByGrpByElem::Ordinal(pos)
                }
                OrdrByGrpByElem::Expr(expr) => {
                    // A bare name naming an output column.
                    let name_pos = match expr.inner_ref() {
                        ExprInner::Var(var) if var.table_name().is_none() => {
                            let name = var.name().ok_or_else(|| {
                                analyze_invariant_error(format_smolstr!("expected column name"))
                            })?;
                            let found = match body_names.as_ref() {
                                Some(names) => output_name_pos(names, name),
                                None => meta
                                    .binder
                                    .top_frame_ref()?
                                    .bound_sel_lst_ref()?
                                    .output_name_pos(name),
                            };
                            match found {
                                // The name reaches more than one output column, and
                                // they are not the same expression.
                                Some(None) => {
                                    return Err(analyze_error(format_smolstr!(
                                        r#"ORDER BY "{name}" is ambiguous"#
                                    )))
                                }
                                Some(Some(pos)) => Some(pos),
                                None => None,
                            }
                        }
                        _ => None,
                    };

                    match name_pos {
                        Some(pos) => OrdrByGrpByElem::Ordinal(pos),
                        // Set operation has no input scope.
                        None if body_names.is_some() && !is_values => {
                            return Err(analyze_error(format_smolstr!(
                                "invalid UNION/INTERSECT/EXCEPT ORDER BY clause"
                            )))
                        }
                        // A column reference still binds below (an outer
                        // reference resolves, an unknown name fails), but any
                        // other expression would have to resolve against the
                        // VALUES columns, which are not exposed as a scope yet.
                        None if is_values && !matches!(expr.inner_ref(), ExprInner::Var(_)) => {
                            return Err(analyze_error(format_smolstr!(
                                "ORDER BY expressions over VALUES are not supported yet"
                            )))
                        }
                        None => {
                            let (bound_expr, t_expr) = expr.bind(meta)?;

                            meta.type_system.ctx.rest.push(t_expr);

                            OrdrByGrpByElem::Expr(bound_expr)
                        }
                    }
                }
            };
            bound_elems.push(OrderByElement::<'q, Analyzed>::from_parts(
                bound_elem,
                elem.direction,
                elem.nulls,
            ));
        }

        // Under DISTINCT every sort key has to be one of the projected expressions.
        if body_names.is_none() {
            let sel_lst = meta.binder.top_frame_ref()?.bound_sel_lst_ref()?;
            if sel_lst.is_distinct()
                && bound_elems.iter().any(|elem| match elem.elem_ref() {
                    // An ordinal already names an output column.
                    OrdrByGrpByElem::Ordinal(_) => false,
                    OrdrByGrpByElem::Expr(expr) => !sel_lst.projects(expr),
                })
            {
                return Err(analyze_error(format_smolstr!(
                    "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
                )));
            }
        }

        meta.binder.set_stmt_body(body);

        Ok(OrderBy::from(bound_elems))
    }
}

impl ExprTypeDeriver for OrderBy<'_, Analyzed> {
    fn derive_types(&mut self, type_report: &AstTypeReport) -> AstResult<()> {
        for elem in self.elements_mut() {
            if let Some(expr) = elem.expr_mut() {
                expr.derive_types(type_report)?;
            }
        }
        Ok(())
    }
}

/// Position of the output column named `name` among a body's output names:
/// [`None`] when nothing is called that, `Some(None)` when more than one
/// column is.
///
/// Unlike [`SelectList::output_name_pos`](sql_ast_new_nodes::select::SelectList::output_name_pos),
/// this has no "same expression" exemption.
fn output_name_pos(names: &[Option<&str>], name: &str) -> Option<Option<usize>> {
    let mut matches = names
        .iter()
        .enumerate()
        .filter(|(_, out)| out.as_deref() == Some(name))
        .map(|(pos, _)| pos);
    let first = matches.next()?;
    Some(matches.next().is_none().then_some(first))
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
