//! Analysis of the SELECT layer: the statement, its select list and the
//! projection expressions in it.
//!
//! # Clause order
//! [`SelectStmt`] is bound clause by clause in a *semantic* order rather than
//! the written one — FROM, WHERE, select list, GROUP BY, HAVING —
//! because GROUP BY may reference a select-list alias and so has to run after
//! the list it reads. That is also why the table expression is not opaque here:
//! its clauses are interleaved with the select list instead of being handed to
//! one [`Binder`] impl of their own.
//!
//! WHERE and HAVING therefore live in this module even though they are
//! [`table_expression`](sql_ast_new_nodes::table_expression)
//! nodes; what the FROM clause itself expands into is in
//! [`crate::table_expression`].

use smol_str::format_smolstr;

use sql_ast_new_nodes::error::AstResult;
use sql_ast_new_nodes::select::{
    ProjectionExpr, SelectList, SelectListElem, SelectListExprs, SelectStmt,
};
use sql_ast_new_nodes::table_expression::{From, GroupBy, OrdrByGrpByElem, TableExpression};
use sql_ast_new_nodes::{Analyzed, Ident, Raw};
use sql_ir::ir::metadata::Metadata;
use sql_ir::ir::types::UnrestrictedType;

use crate::asterisk::{missing_from_entry_error, ResolveFromAsterisk};
use crate::{
    analyze_error, analyze_invariant_error, AnalyzerCtx, AstTypeReport, Binder, Discoverer,
    ExprTypeDeriver, Stage, Stmt,
};

/// Bind SELECT statement.
///
/// Components of SELECT statement are analyzed in the following order:
/// 1. FROM
/// 2. WHERE
/// 3. GROUP BY discovery
/// 4. SELECT list expressions
/// 5. GROUP BY
/// 6. HAVING
///
/// Using select list aliases in GROUP BY is the main reason why [table expression](`TableExpression`)
/// is not opaque component of [SELECT statement](`SelectStmt`).
///
/// GROUP BY discovery runs before the select list because
/// it may repeat an expression grouping key, and discovery is what binds the
/// keys it is matched against.
///
/// The order also decides what HAVING can be checked against. GROUP BY is bound after the select
/// list because it may name an alias, and HAVING after GROUP BY because an expression grouping key
/// covers HAVING as well as the projection. Both bound halves are therefore parked on the frame
/// across HAVING step and taken back afterwards.
///
/// Currently, Picodata SQL does not support WHERE, GROUP BY, HAVING without FROM clause.
impl<'q> Binder<'q> for SelectStmt<'q, Raw> {
    type BoundNode = SelectStmt<'q, Analyzed>;

    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        let (select_list, table_expr) = self.into_parts();

        // TODO: support separate WHERE, GROUP BY, HAVING without FROM clause.
        let Some(table_expr) = table_expr else {
            select_list.bind(meta)?;
            return Ok(Self::BoundNode::from_parts(SelectList::default(), None));
        };

        let (from, selection, group_by, having, named_windows) = table_expr.into_parts();

        if !named_windows.is_empty() {
            return Err(analyze_error(format_smolstr!(
                "WINDOW clause is not supported yet"
            )));
        }

        // 1. FROM
        let from = from.bind(meta)?;

        // Analyzed FROM clause can be used in WHERE, GROUP BY, HAVING, WINDOW clauses.
        // So ste it as top frame on stack.
        meta.binder.top_frame_mut()?.from = from;

        // 2. WHERE
        let selection = match selection {
            None => None,
            Some(selection) => {
                meta.binder.top_frame_mut()?.set_stage(Stage::Where);

                let (expr, t_expr) = selection.bind(meta)?;

                meta.type_system.ctx.filters.push(t_expr);

                Some(expr)
            }
        };

        // 3. GROUP BY discovery: resolves ordinal elements and potential
        // aliases, and binds expression grouping keys — which the
        // select list is matched against.
        meta.binder.top_frame_mut()?.set_raw_sel_lst(select_list);
        let group_by = group_by.discover(meta)?;

        // 4. SELECT list expressions
        meta.binder.top_frame_mut()?.set_stage(Stage::Projection);
        meta.binder
            .top_frame_mut()?
            .take_raw_sel_lst()?
            .bind(meta)?;

        // 5. GROUP BY
        meta.binder.top_frame_mut()?.set_stage(Stage::GroupBy);
        group_by.bind(meta)?;

        // 6. HAVING
        meta.binder.top_frame_mut()?.set_stage(Stage::Having);
        let having = match having {
            None => None,
            Some(having) => {
                let (expr, t_expr) = having.bind(meta)?;

                meta.type_system.ctx.filters.push(t_expr);

                Some(expr)
            }
        };

        let t_expr = TableExpression::from_parts(
            // Keep FROM clause and GROUP BY clause in the statement frame until
            // ORDER BY is bound
            From::default(),
            selection,
            GroupBy::default(),
            having,
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
            let (from, selection, group_by, having, _named_windows) = tbl_expr.parts_mut();
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

            group_by.derive_types(type_report)?;

            if let Some(expr) = having {
                expr.derive_types(type_report)?;

                if !expr
                    .data_type()
                    .get()
                    .is_some_and(|ty| matches!(ty, UnrestrictedType::Boolean))
                {
                    return Err(analyze_error(format_smolstr!(
                        "argument of HAVING must be type boolean, not type {}",
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

        let depth = meta.binder.depth()?;

        // The raw element index and the expanded position count different
        // bases: one element can expand into more than one projection
        // (asterisk). GROUP BY references arrive in both — ordinals in
        // expanded positions, the parser's select-item references in raw
        // indexes — so both are tracked, and the mapping between them is
        // recorded for the GROUP BY binding that runs next.
        let mut expanded_pos = 0;

        for (raw_pos, select_expr) in expr_list.0.into_iter().enumerate() {
            meta.binder
                .top_frame_mut()?
                .grby_ctx
                .raw_to_expanded
                .push(expanded_pos);
            match select_expr {
                SelectListElem::Asterisk(t_name) => {
                    // A qualified asterisk may name a relation of an enclosing
                    // query (`SELECT (SELECT t.* FROM t2) FROM (SELECT 1 AS a) t`),
                    // so the search walks outwards and keeps the level it stopped at.
                    // An unqualified `*` always resolves at the innermost level.
                    let (lvl, resolved) = meta.binder
                        .frames
                        .iter()
                        .enumerate()
                        .rev()
                        .find_map(|(lvl, Stmt { from, .. })| {
                            from.resolve_asterisk(
                                t_name.as_ref().map(Ident::as_str),
                                &mut meta.type_system,
                            )
                            .map(|resolved| (lvl, resolved))
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
                        })?;

                    let resolved = resolved?;

                    for (offset, proj) in resolved.iter().enumerate() {
                        // In case of JOIN/USING column deriving common type.
                        let Some(bound_var) = proj.transient_var_ref() else {
                            continue;
                        };

                        // Check whether expr is referenced by GROUP BY element.
                        // A position in this select list means something only
                        // to this level's GROUP BY ordinals; an expression key
                        // is matched at whichever level the column belongs to.
                        let grouped = meta.binder.frames.get(lvl).is_some_and(|frame| {
                            (lvl == depth
                                && frame
                                    .bound_grby_ref()
                                    .is_some_and(|grby| grby.contains_pos(expanded_pos + offset)))
                                || frame.grouping_covers(proj.expr_ref())
                        });
                        if grouped {
                            // Every aggregate call enclosing this asterisk belongs to
                            // the deepest level it reads, and that decides
                            // whether the call nests another one.
                            meta.binder.update_col_sem_lvl(lvl);
                        } else {
                            // Not under GROUP BY, account it in target exprs.
                            meta.binder.reg_var(lvl, bound_var.clone())?;
                        }
                    }

                    expanded_pos += resolved.len();

                    meta.binder
                        .top_frame_mut()?
                        .bound_sel_lst_mut()?
                        .exprs_mut()
                        .0
                        .extend(resolved);
                }
                SelectListElem::Expr(proj_expr) => {
                    let (expr, alias) = proj_expr.into_parts();

                    // Whether this element is referenced by GROUP BY element (ordinal or alias)
                    let (under_ordinal, under_alias) = {
                        let frame = &meta.binder.top_frame_ref()?;
                        (
                            // Ordinal position stated by user as GROUP BY element.
                            frame
                                .bound_grby_ref()
                                .is_some_and(|grby| grby.contains_pos(expanded_pos)),
                            // Raw (before asterisk flattening) ordinal position stated by analyzer while GROUP BY discovery.
                            frame
                                .grby_ctx
                                .raw_pos
                                .iter()
                                .any(|(_, pos)| *pos == raw_pos),
                        )
                    };
                    let under_gr_by = under_ordinal || under_alias;

                    if under_gr_by {
                        meta.binder.top_frame_mut()?.set_stage(Stage::GroupBy);
                    }

                    let (bound_expr, t_expr) = expr.bind(meta)?;

                    if under_gr_by {
                        meta.binder.top_frame_mut()?.set_stage(Stage::Projection);
                    }

                    meta.type_system.ctx.curr_proj.push(t_expr);

                    let bound_proj_expr = ProjectionExpr::new(bound_expr, alias);

                    meta.binder
                        .top_frame_mut()?
                        .bound_sel_lst_mut()?
                        .exprs_mut()
                        .0
                        .push(bound_proj_expr);

                    // A key named by its alias is a grouping key like one named by its ordinal.
                    // Discovery could not publish it. It knew only the raw position,
                    // and the key is looked up by its expanded one.
                    if under_alias && !under_ordinal {
                        meta.binder
                            .top_frame_mut()?
                            .bound_grby_mut()?
                            .0
                            .push(OrdrByGrpByElem::Ordinal(expanded_pos));
                    }

                    expanded_pos += 1;
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
