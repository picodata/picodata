//! Analysis of the SELECT layer: the statement, its select list and the
//! projection expressions in it.
//!
//! WHERE lives in this module even though it is a
//! [`table_expression`](sql_ast_new_nodes::table_expression) node, because its
//! binding is interleaved with the select list; what the FROM clause itself
//! expands into is in [`crate::table_expression`].

use smol_str::format_smolstr;

use sql_ast_new_nodes::error::AstResult;
use sql_ast_new_nodes::select::{
    ProjectionExpr, SelectList, SelectListElem, SelectListExprs, SelectStmt,
};
use sql_ast_new_nodes::table_expression::{From, TableExpression};
use sql_ast_new_nodes::{Analyzed, Ident, Raw};
use sql_ir::ir::metadata::Metadata;
use sql_ir::ir::types::UnrestrictedType;

use crate::asterisk::{missing_from_entry_error, ResolveFromAsterisk};
use crate::{
    analyze_error, analyze_invariant_error, AnalyzerCtx, AstTypeReport, Binder, ExprTypeDeriver,
    Stmt,
};

/// Bind SELECT statement.
///
/// Components of SELECT statement are analyzed in the following order:
/// 1. FROM
/// 2. WHERE
/// 3. SELECT list expressions
///
/// Currently, Picodata SQL does not support WHERE without FROM clause.
impl<'q> Binder<'q> for SelectStmt<'q, Raw> {
    type BoundNode = SelectStmt<'q, Analyzed>;

    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        let (select_list, table_expr) = self.into_parts();

        let Some(table_expr) = table_expr else {
            select_list.bind(meta)?;
            return Ok(Self::BoundNode::from_parts(SelectList::default(), None));
        };

        let (from, selection, group_by, having, named_windows) = table_expr.into_parts();

        if !group_by.is_empty() {
            return Err(analyze_error(format_smolstr!(
                "GROUP BY is not supported yet"
            )));
        }
        if having.is_some() {
            return Err(analyze_error(format_smolstr!(
                "HAVING is not supported yet"
            )));
        }
        if !named_windows.is_empty() {
            return Err(analyze_error(format_smolstr!(
                "WINDOW clause is not supported yet"
            )));
        }

        // 1. FROM
        let from = from.bind(meta)?;

        // Analyzed FROM clause can be used in WHERE clause,
        // so set it as top frame on stack.
        meta.binder.top_frame_mut()?.from = from;

        // 2. WHERE
        let selection = match selection {
            None => None,
            Some(selection) => {
                let (expr, t_expr) = selection.bind(meta)?;

                meta.type_system.ctx.filters.push(t_expr);

                Some(expr)
            }
        };

        // 3. SELECT list expressions
        select_list.bind(meta)?;

        let t_expr = TableExpression::from_parts(
            // Keep FROM clause in the statement frame until the statement takes it back.
            From::default(),
            selection,
            Vec::new(),
            None,
            Vec::new(),
        );

        Ok(Self::BoundNode::from_parts(
            SelectList::default(),
            Some(t_expr),
        ))
    }
}

impl ExprTypeDeriver for SelectStmt<'_, Analyzed> {
    fn derive_types(&mut self, type_report: &AstTypeReport) -> AstResult<()> {
        let (select_list, tbl_expr) = self.parts_mut();

        select_list.derive_types(type_report)?;

        if let Some(tbl_expr) = tbl_expr {
            let (from, selection, _group_by, _having, _named_windows) = tbl_expr.parts_mut();
            for entry in from.entries_mut() {
                entry.derive_types(type_report)?;
            }

            if let Some(expr) = selection {
                expr.derive_types(type_report)?;

                if !expr
                    .data_type()
                    .get()
                    .is_some_and(|ty| matches!(ty, UnrestrictedType::Boolean))
                {
                    return Err(analyze_error(format_smolstr!(
                        "argument of WHERE must be type boolean, not type {}",
                        expr.data_type(),
                    )));
                }
            }
        }
        Ok(())
    }
}

impl<'q> Binder<'q> for SelectList<'q, Raw> {
    /// SELECT is stored in statement frame context after its binding.
    type BoundNode = ();

    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        let (expr_list, is_distinct) = self.into_parts();

        meta.binder
            .top_frame_mut()?
            .set_bound_sel_lst(SelectList::from_parts(
                SelectListExprs::from(Vec::with_capacity(expr_list.0.len())),
                is_distinct,
            ));

        // Analyze the select list against the innermost FROM scope. Elements do
        // not map 1:1 onto projections — an asterisk expands into one
        // projection per source column.
        // let mut elems = Vec::<ProjectionExpr<'q, Analyzed>>::with_capacity(expr_list.0.len());

        for select_expr in expr_list.0.into_iter() {
            match select_expr {
                SelectListElem::Asterisk(t_name) => {
                    let resolved = meta.binder
                        .frames
                        .iter()
                        .rev()
                        .find_map(|Stmt { from, .. }| {
                            from.resolve_asterisk(
                                t_name.as_ref().map(Ident::as_str),
                                &mut meta.type_system,
                            )
                        })
                        .ok_or_else(|| {
                            t_name.map_or_else(
                                // This should never happen.
                                // Unqualified asterisk should always be resolved into list of expressions.
                                || {
                                    analyze_invariant_error(format_smolstr!(
                                        "expected to resolve unqualified asterisk into list of expressions"
                                    ))
                                },
                                |t_name| missing_from_entry_error(t_name.as_str()),
                            )
                        })??;
                    meta.binder
                        .top_frame_mut()?
                        .bound_sel_lst_mut()?
                        .exprs_mut()
                        .0
                        .extend(resolved);
                }
                SelectListElem::Expr(proj_expr) => {
                    let (expr, alias) = proj_expr.into_parts();

                    let (bound_expr, t_expr) = expr.bind(meta)?;

                    meta.type_system.ctx.curr_proj.push(t_expr);

                    let bound_proj_expr = ProjectionExpr::new(bound_expr, alias);

                    meta.binder
                        .top_frame_mut()?
                        .bound_sel_lst_mut()?
                        .exprs_mut()
                        .0
                        .push(bound_proj_expr);
                }
            }
        }

        // Push all projection expressions onto stack.
        meta.type_system
            .ctx
            .proj
            .push(std::mem::take(&mut meta.type_system.ctx.curr_proj));

        Ok(())
    }
}

impl ExprTypeDeriver for SelectList<'_, Analyzed> {
    fn derive_types(&mut self, type_report: &AstTypeReport) -> AstResult<()> {
        let exprs = self.exprs_mut();
        for expr in exprs.0.iter_mut().map(ProjectionExpr::expr_mut) {
            expr.derive_types(type_report)?;
        }
        Ok(())
    }
}
