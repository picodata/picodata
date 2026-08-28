//! Analysis of the table expression's relational half: the FROM clause and
//! everything reachable from it — table factors, joins and their ON/USING
//! conditions — plus the GROUP BY elements read against it.
//!
//! The clauses that filter what FROM produces (WHERE, HAVING, WINDOW) are bound
//! in [`crate::select`] instead, because their order is interleaved with the
//! select list; see that module's docs.
//!
//! # Where a name is looked up
//! A FROM clause under analysis lives in
//! [`curr_from`](crate::BinderCtx::curr_from) rather than on the frame stack: it
//! is being built, so it is not yet a scope anything can resolve against. Join
//! conditions are the exception — they *must* see the entries bound before them,
//! so ON and USING push the partially built clause onto
//! [`from_frames`](crate::BinderCtx::frames) for the duration and take it
//! back afterwards.
//!
//! Picodata has no LATERAL, so a joined table itself is bound with `curr_from`
//! taken away: a subquery in it cannot reference its siblings.

use std::rc::Rc;

use smol_str::format_smolstr;

use sql_ast_new_nodes::error::AstResult;
use sql_ast_new_nodes::expr::{Expr, ExprInner, RawVar};
use sql_ast_new_nodes::table_expression::{
    AnalyzedCteOrTable, BoundJoinUsingVar, ColumnRoute, From, FromEntry, GroupBy, JoinUsingColumn,
    JoinedTable, OrdrByGrpByElem, TableFactor, TableFactorInner,
};
use sql_ast_new_nodes::{Analyzed, NamedEntity, Raw};
use sql_ir::ir::metadata::Metadata;
use sql_ir::ir::types::UnrestrictedType;

use crate::expr::bind_var;
use crate::multiset::analyze_subquery;
use crate::{
    analyze_error, analyze_invariant_error, duplicated_name, texpr_from_derived_type, AnalyzerCtx,
    AstTypeReport, Binder, Discoverer, ExprTypeDeriver, Stage, Stmt,
};

/// Resolve relation references. Recursively analyze subqueries if they are presented.
impl<'q> Binder<'q> for From<'q, Raw> {
    type BoundNode = From<'q, Analyzed>;

    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        let from_entries = self.into_tbl_factors();

        // Detect duplicating FROM entry name if any.
        if let Some(name) = duplicated_name(&from_entries) {
            return Err(analyze_error(format_smolstr!(
                r#"table "{name}" specified more than once"#
            )));
        }

        debug_assert!(meta.binder.curr_from.is_empty());

        meta.binder.curr_from = From::from_tbl_factors(Vec::with_capacity(from_entries.len()));

        for from_entry in from_entries.into_iter() {
            let analyzed_entry = from_entry.bind(meta)?;
            meta.binder.curr_from.add_entry(analyzed_entry);
        }

        Ok(std::mem::take(&mut meta.binder.curr_from))
    }
}

impl<'q> Binder<'q> for FromEntry<'q, Raw> {
    type BoundNode = FromEntry<'q, Analyzed>;

    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        match self {
            Self::TableFactor(tbl_factor) => Ok(Self::BoundNode::TableFactor(Rc::new(
                tbl_factor.bind(meta)?,
            ))),
            Self::JoinedTable(joined_tbl) => {
                Ok(Self::BoundNode::JoinedTable(joined_tbl.bind(meta)?))
            }
        }
    }
}

impl ExprTypeDeriver for FromEntry<'_, Analyzed> {
    fn derive_types(&mut self, type_report: &AstTypeReport) -> AstResult<()> {
        if let FromEntry::JoinedTable(tbl) = self {
            if let Some(expr) = &mut tbl.condition {
                expr.derive_types(type_report)?;

                if !expr
                    .data_type()
                    .get()
                    .is_some_and(|ty| matches!(ty, UnrestrictedType::Boolean))
                {
                    return Err(analyze_error(format_smolstr!(
                        "argument of JOIN/ON must be type boolean, not type {}",
                        expr.data_type(),
                    )));
                }
            }
        }
        Ok(())
    }
}

impl<'q> Binder<'q> for TableFactor<'q, Raw> {
    type BoundNode = TableFactor<'q, Analyzed>;

    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        let (inner, alias, indexed_by) = self.into_parts();
        let inner = match inner {
            TableFactorInner::CteOrTable(name) => {
                let found_cte =
                    meta.binder
                        .frames
                        .iter()
                        .enumerate()
                        .rev()
                        .find_map(|(lvl, frame)| {
                            frame
                                .ctes
                                .ctes_ref()
                                .iter()
                                .find(|cte| {
                                    cte.name().is_some_and(|cte_name| cte_name == name.as_str())
                                })
                                .map(|cte| (lvl, Rc::clone(cte)))
                        });

                if let Some((lvl, cte)) = found_cte {
                    // An enclosing aggregate call must not belong to a level
                    // above the one the CTE is defined at.
                    meta.binder.update_cte_lvl(lvl);
                    TableFactorInner::CteOrTable(AnalyzedCteOrTable::Cte(cte))
                } else if let Ok(table) = meta.table(name.as_str()) {
                    TableFactorInner::CteOrTable(AnalyzedCteOrTable::Table(table))
                } else {
                    return Err(analyze_error(format_smolstr!(
                        r#"relation "{}" does not exist"#,
                        name.as_str()
                    )));
                }
            }
            TableFactorInner::SubQuery(stmt) => {
                let analyzed_stmt = analyze_subquery(*stmt, meta)?;
                TableFactorInner::<'q, Analyzed>::SubQuery(Box::new(analyzed_stmt))
            }
        };
        Ok(Self::BoundNode::from_parts(inner, alias, indexed_by))
    }
}

impl<'q> Binder<'q> for JoinedTable<'q, Raw> {
    type BoundNode = JoinedTable<'q, Analyzed>;

    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        let (tbl, kind, condition, using_cols) = self.into_parts();

        if let Some(duplicated_name) = duplicated_name(&using_cols) {
            return Err(analyze_error(format_smolstr!(
                r#"column name "{duplicated_name}" appears more than once in USING clause"#
            )));
        }

        // Picodata does not support LATERAL yet.
        // It means column references in joined table cannot be resolved against previous FROM clauses with on same level.
        let preserved_from = std::mem::take(&mut meta.binder.curr_from);
        let analyzed_tbl = Rc::new(tbl.bind(meta)?);
        meta.binder.curr_from = preserved_from;

        // Add analyzed table factor of joined table in order to resolve join condition against it.
        meta.binder
            .curr_from
            .add_entry(FromEntry::TableFactor(analyzed_tbl));

        // Rely on grammar allowing exactly one of JOIN/ON and JOIN/USING.
        // CROSS JOIN is not supported yet.
        debug_assert!(
            (condition.is_some() && using_cols.is_empty())
                || (condition.is_none() && !using_cols.is_empty())
        );

        let analyzed_cond = match condition {
            None => None,
            Some(cond) => {
                // Previously analyzed entries in current FROM clause should be visible to JOIN/ON clause.
                // For example, in this query
                // `SELECT * FROM (SELECT 1 a) t1 INNER JOIN (SELECT 2 a) t2 ON (SELECT t1.a)::BOOL`
                // `t1.a` should be resolved subquery `t1` output attribute.
                let frame = Stmt::new(std::mem::take(&mut meta.binder.curr_from), Stage::JoinOn);
                meta.binder.push_frame(frame);
                let (bound_expr, t_expr) = cond.bind(meta)?;
                meta.binder.curr_from = meta
                    .binder
                    .pop_frame(Some(
                        "Expected the same statement frame after JOIN/ON analysis",
                    ))?
                    .from;

                meta.type_system.ctx.filters.push(t_expr);

                Some(bound_expr)
            }
        };

        let analyzed_using_columns = using_cols
            .into_iter()
            .map(|col| col.bind(meta))
            .collect::<AstResult<Vec<_>>>()?;

        // We expect to find inner analyzed table factor of joined table.
        let inner_tbl_factor = match meta.binder.curr_from.pop_entry()? {
            FromEntry::TableFactor(tbl_factor) => Ok(tbl_factor),
            FromEntry::JoinedTable(_) => Err(analyze_invariant_error(format_smolstr!(
                "expected to find only table factor, not joined table"
            ))),
        }?;

        Ok(Self::BoundNode::from_parts(
            inner_tbl_factor,
            kind,
            analyzed_cond,
            analyzed_using_columns,
        ))
    }
}

impl<'q> Binder<'q> for JoinUsingColumn {
    type BoundNode = BoundJoinUsingVar<'q>;

    /// Bind column references in JOIN/USING.
    ///
    /// The idea is to firstly bind column reference against
    /// FROM entries preceding joined table and secondly bind it against
    /// only joined table which is last entry in current FROM clause.
    ///
    /// The merged output column's type is the common type of the two inputs,
    /// derived exactly like one output column of a set operation.
    /// Derivation runs right here rather than at the statement root.
    /// Type of each bound var is known by this moment because the are
    /// either table column or output attribute of a from entry subquery.
    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        // Retrieve joined table. Keep only preceding it FROM entries.
        let last_joined_tbl = meta.binder.curr_from.pop_entry()?;

        // Use previously bound FROM entries as statement frame.
        let frame = Stmt::new(std::mem::take(&mut meta.binder.curr_from), Stage::JoinUsing);
        meta.binder.push_frame(frame);

        let resolve_var = |var: &RawVar, meta: &mut AnalyzerCtx<'q, M>| bind_var(var, meta, true);

        // Resolve against entries preceding joined table.
        let first_resolved_column = resolve_var(&self.0, meta)?.1;

        // The left input of this merge is the join output built so far.
        // A column an earlier USING merge produced contributes its merged type,
        // so a chain of merges unifies left to right.
        let left_type = meta
            .binder
            .top_frame_ref()?
            .from
            .join_using_var(&first_resolved_column)
            .map_or_else(
                || first_resolved_column.data_type(),
                BoundJoinUsingVar::data_type,
            );

        let mut saved_from = std::mem::replace(
            &mut meta.binder.top_frame_mut()?.from,
            From::from_tbl_factors(vec![last_joined_tbl]),
        );

        // Resolve against only joined table entry.
        let second_resolved_column = resolve_var(&self.0, meta)?.1;

        let left_id = meta.type_system.next_expr_id;
        let joined_id = left_id + 1;
        meta.type_system.next_expr_id += 2;

        let left_t_expr = texpr_from_derived_type(left_type, left_id);
        let joined_t_expr = texpr_from_derived_type(second_resolved_column.data_type(), joined_id);
        let merged_type = meta.type_system.analyzer.analyze_homogeneous_exprs(
            "JOIN/USING",
            &[left_t_expr, joined_t_expr],
            None,
        )?;

        let last_joined_tbl = meta
            .binder
            .pop_frame(Some("Expected statement frame in JOIN/USING analysis"))?
            .from
            .pop_entry()?;

        // Adjust current FROM entries.
        saved_from.add_entry(last_joined_tbl);
        meta.binder.curr_from = saved_from;

        Ok(Self::BoundNode::new(
            first_resolved_column,
            second_resolved_column,
            merged_type.into(),
        ))
    }
}

impl<'q, M: Metadata> Discoverer<'q, M> for GroupBy<'q, Raw> {
    fn discover(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self> {
        let mut raw_elems = Vec::<OrdrByGrpByElem<'q, Raw>>::with_capacity(self.0.len());

        let frame = meta.binder.top_frame_mut()?;
        frame.set_stage(Stage::GroupBy);

        let mut bound_elems = Vec::with_capacity(self.0.len());

        // Set GROUP BY frame stage for potential binding procedures.
        for (idx, gr_by_elem) in self.0.into_iter().enumerate() {
            match gr_by_elem {
                OrdrByGrpByElem::Ordinal(pos) => {
                    raw_elems.push(OrdrByGrpByElem::Ordinal(pos));
                    bound_elems.push(OrdrByGrpByElem::Ordinal(pos));
                }
                OrdrByGrpByElem::Expr(expr) => {
                    // Bare name should be resolved in three steps:
                    // 1. the *local* FROM
                    // 2. select list output names - an explicit alias or the
                    //    name an unaliased expression suggests
                    // 3. ordinary expression - the one step that may reach an outer query level.
                    //
                    // Step 1 has two ways to fail and only one of them may go on
                    // to step 2: a name no relation here exposes is simply not
                    // in the FROM clause, while a name several of them expose is
                    // ambiguous, and no alias can answer which relation was meant.
                    let proj_alias_pos = if let ExprInner::Var(var) = expr.inner_ref() {
                        let local_miss = var.table_name().is_none()
                            && match meta.binder.top_frame_ref()?.from.column_route(var) {
                                ColumnRoute::Ambigious => {
                                    return Err(analyze_error(format_smolstr!(
                                        "column reference '{var}' is ambigious"
                                    )))
                                }
                                ColumnRoute::Resolved(_) => false,
                                ColumnRoute::NoMatch | ColumnRoute::ColumnMissing => true,
                            };

                        if local_miss {
                            let col_name = var.name().ok_or_else(|| {
                                analyze_invariant_error(format_smolstr!("expected column name"))
                            })?;

                            // It can be an output name referencing select list.
                            match meta
                                .binder
                                .top_frame_mut()?
                                .raw_sel_lst_ref()?
                                .output_name_pos(col_name)
                            {
                                // The name belongs to more than one distinct
                                // projection, so it cannot be resolved at all.
                                Some(None) => {
                                    return Err(analyze_error(format_smolstr!(
                                        r#"GROUP BY "{}" is ambiguous"#,
                                        col_name
                                    )))
                                }
                                Some(Some(pos)) => Some(pos),
                                None => None,
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if let Some(pos) = proj_alias_pos {
                        meta.binder
                            .top_frame_mut()?
                            .grby_ctx
                            .raw_pos
                            .push((idx, pos));

                        raw_elems.push(OrdrByGrpByElem::Ordinal(pos));
                    } else {
                        let (bound, t_expr) = expr.bind(meta)?;
                        meta.type_system.ctx.rest.push(t_expr);

                        raw_elems.push(OrdrByGrpByElem::Expr(Expr::default()));
                        bound_elems.push(OrdrByGrpByElem::Expr(bound));
                    }
                }
            };
        }

        meta.binder
            .top_frame_mut()?
            .set_bound_grby(GroupBy(bound_elems));

        Ok(Self(raw_elems))
    }
}

impl<'q> Binder<'q> for GroupBy<'q, Raw> {
    /// Bound GROUP BY is stored in statement frame until its finalization.
    type BoundNode = ();

    fn bind<M: Metadata>(self, meta: &mut AnalyzerCtx<'q, M>) -> AstResult<Self::BoundNode> {
        // Discovery parked the keys it bound in GROUP BY order. Select-list
        // binding then appended the ordinal of every key stated as an alias.
        // Those trail the discovered ones, are rebuilt below from `raw_pos`,
        // and are simply left over.
        let mut interim_bound_elems = meta.binder.top_frame_mut()?.take_bound_grby()?.0;

        interim_bound_elems.reverse();

        let mut bound_elems = Vec::with_capacity(self.0.len());

        let select_list = meta.binder.top_frame_ref()?.bound_sel_lst_ref()?;
        for (idx, gr_by_elem) in self.0.into_iter().enumerate() {
            match gr_by_elem {
                OrdrByGrpByElem::Ordinal(pos) => {
                    if pos >= select_list.len() {
                        return Err(analyze_error(format_smolstr!(
                            "GROUP BY position {} is not in select list",
                            pos + 1
                        )));
                    }
                    if let Some(raw_pos) = meta
                        .binder
                        .top_frame_ref()?
                        .grby_ctx
                        .raw_pos
                        .iter()
                        .find_map(|(i, pos)| (*i == idx).then_some(pos))
                    {
                        let pos = *meta.binder
                            .top_frame_ref()?
                            .grby_ctx
                            .raw_to_expanded
                            .get(*raw_pos)
                            .ok_or_else(|| analyze_invariant_error(format_smolstr!(
                                "expected to find mapping of GROUP BY raw position into final ordinal position"
                            )))?;
                        bound_elems.push(OrdrByGrpByElem::Ordinal(pos));
                    } else {
                        let bound = interim_bound_elems.pop().ok_or_else(|| analyze_invariant_error(format_smolstr!(
                            "expected to bind all expressions in GROUP BY elements before SELECT binding"
                        )))?;
                        bound_elems.push(bound);
                    }
                }
                OrdrByGrpByElem::Expr(_) => {
                    let bound = interim_bound_elems.pop().ok_or_else(|| analyze_invariant_error(format_smolstr!(
                        "expected to bind all expressions in GROUP BY elements before SELECT binding"
                    )))?;
                    bound_elems.push(bound);
                }
            }
        }
        meta.binder
            .top_frame_mut()?
            .set_bound_grby(GroupBy(bound_elems));
        Ok(())
    }
}

impl ExprTypeDeriver for GroupBy<'_, Analyzed> {
    fn derive_types(&mut self, type_report: &AstTypeReport) -> AstResult<()> {
        for elem in self.0.iter_mut() {
            match elem {
                OrdrByGrpByElem::Ordinal(_) => {}
                OrdrByGrpByElem::Expr(expr) => expr.derive_types(type_report)?,
            }
        }
        Ok(())
    }
}
