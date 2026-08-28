//! Asterisk expansion: `*` and `t.*` into one projection per source column.
//!
//! Expansion belongs to the analyzer rather than to the nodes because it
//! *manufactures* column references that were never written, and each one needs
//! a fresh type-system mirror id so that it ends up typed exactly like an
//! explicitly written reference.
//!
//! The methods hang off node types through locally declared traits: an inherent
//! `impl` on a `sql-ast-new-nodes` type is illegal outside the crate that
//! declares it.

use std::collections::HashSet;
use std::rc::Rc;

use smol_str::format_smolstr;

use crate::expr::using_merged_expr;
use crate::{analyze_error, texpr_from_attribute, texpr_from_derived_type};
use sql_ast_new_nodes::error::{AstErr, AstResult};
use sql_ast_new_nodes::expr::{Expr, ExprInner};
use sql_ast_new_nodes::multiset::{Cte, MultisetStmt};
use sql_ast_new_nodes::select::ProjectionExpr;
use sql_ast_new_nodes::table_expression::{
    AnalyzedCteOrTable, AttributeView, BoundVar, From, FromEntry, TableFactor, TableFactorInner,
};
use sql_ast_new_nodes::{Analyzed, AnalyzedExprMeta, NamedEntity};
use sql_ir::ir::relation::{ColumnRole, Table};

use super::TypeSystem;

pub(crate) fn missing_from_entry_error(t_name: &str) -> AstErr {
    analyze_error(format_smolstr!(
        r#"missing FROM-clause entry for table "{t_name}""#
    ))
}

/// Asterisk resolver.
pub(crate) trait ResolveFromAsterisk<'q> {
    fn resolve_asterisk(
        &self,
        qualifier: Option<&str>,
        type_system: &mut TypeSystem,
    ) -> Option<AstResult<Vec<ProjectionExpr<'q, Analyzed>>>>;
}

impl<'q> ResolveFromAsterisk<'q> for From<'q, Analyzed> {
    /// Resolve qualified and unqualified asterisk against FROM clause.
    ///
    /// In case of qualified asterisk we rely on uniqueness of FROM clause entries
    /// and take the first table factor matching qualifier.
    ///
    /// # Examples
    /// 1. Query
    ///
    /// ```sql
    /// SELECT t1.* FROM t1 INNER JOIN t1 USING (a);
    /// ```
    ///
    /// is unreachable in this function.
    ///
    /// 2. Query
    ///
    /// ```sql
    /// SELECT t1.* FROM t1 INNER JOIN t1 USING (non_existent_column);
    /// ```
    ///
    /// is unreachable in this function.
    ///
    /// 3. Query
    ///
    /// ```sql
    /// SELECT t1.*, t2.* FROM t1 INNER JOIN t2 USING (a);
    /// ```
    ///
    /// expected to resolve select list into all the columns of `t1`
    /// concatenated with all the columns of `t2`.
    fn resolve_asterisk(
        &self,
        qualifier: Option<&str>,
        type_system: &mut TypeSystem,
    ) -> Option<AstResult<Vec<ProjectionExpr<'q, Analyzed>>>> {
        // TODO: design HINTs for queries like `SELECT t1.* FROM t1 AS x`
        let Some(t_name) = qualifier else {
            return Some(resolve_unqualified_asterisk(self, type_system));
        };
        // A qualified asterisk names one relation and expands to *its* columns,
        // so a USING merge does not hide anything from it.
        self.entries()
            .iter()
            .find(|entry| entry.name().is_some_and(|name| name == t_name))
            .map(|entry| {
                entry.resolve_asterisk(AsteriskResolver::new(
                    entry.tbl_factor(),
                    type_system,
                    &HashSet::new(),
                ))
            })
    }
}

/// Expand a bare `*` over the whole FROM clause.
///
/// A `USING` merge turns two columns into one, and that one is *not* left in
/// place: SQL puts the merged columns of a join first, ahead of the remaining
/// columns of its left input and then of its right one. Joins nest to the left,
/// so the outermost join's merged columns come first overall - hence the reverse
/// walk over the entries - and within one join they follow the order the `USING`
/// list wrote them in.
///
/// The merged column is represented by the *left* input's column, matching what
/// an unqualified reference to that name resolves to. That is the only choice
/// that stays correct under an outer join: in `t3 LEFT JOIN t4 USING (a)` the
/// merged `a` must keep `t3.a` for a row `t4` did not match, where `t4.a` is
/// null.
///
/// Both merged columns are then withheld from the per-entry expansion, so a
/// name merged along a chain of joins is emitted exactly once.
fn resolve_unqualified_asterisk<'q>(
    from: &From<'q, Analyzed>,
    type_system: &mut TypeSystem,
) -> AstResult<Vec<ProjectionExpr<'q, Analyzed>>> {
    let mut total_proj = Vec::<ProjectionExpr<'q, Analyzed>>::new();
    let mut reduced_columns = HashSet::<&BoundVar<'q>>::new();
    for using in from
        .entries()
        .iter()
        .rev()
        .filter_map(FromEntry::joined)
        .flat_map(|entry| &entry.using_cols)
    {
        let (left, joined) = using.parts_ref();
        reduced_columns.insert(joined);
        if reduced_columns.insert(left) {
            let id = type_system.next_expr_id;
            type_system.next_expr_id += 1;

            // The merged column an unqualified `*` emits is the same
            // expression an unqualified reference to it analyzes to - the left
            // input's column read at the merged type.
            let (expr, t_expr) = match using_merged_expr(type_system, left, using.data_type(), id) {
                Some(merged) => merged,
                None => {
                    let t_expr = texpr_from_derived_type(using.data_type(), id);
                    (
                        Expr::from_parts(
                            ExprInner::Var(left.clone()),
                            AnalyzedExprMeta::new_with_id(id),
                        ),
                        t_expr,
                    )
                }
            };
            type_system.ctx.curr_proj.push(t_expr);
            total_proj.push(ProjectionExpr::from_parts(expr, None));
        }
    }

    for entry in from.entries().iter() {
        let resolved = entry.resolve_asterisk(AsteriskResolver::new(
            entry.tbl_factor(),
            type_system,
            &reduced_columns,
        ))?;
        total_proj.extend(resolved);
    }
    Ok(total_proj)
}

/// Expansion against one FROM entry. A local trait, so that the analyzer can
/// still write `factor.resolve_asterisk(..)` on a node type it does not own.
trait ResolveAsterisk<'q> {
    fn resolve_asterisk(
        &self,
        asterisk_resolver: AsteriskResolver<'q, '_>,
    ) -> AstResult<Vec<ProjectionExpr<'q, Analyzed>>>;
}

impl<'q> ResolveAsterisk<'q> for FromEntry<'q, Analyzed> {
    /// Resolve asterisk for [`FromEntry`].
    fn resolve_asterisk(
        &self,
        asterisk_resolver: AsteriskResolver<'q, '_>,
    ) -> AstResult<Vec<ProjectionExpr<'q, Analyzed>>> {
        match self {
            Self::TableFactor(tbl_factor) => tbl_factor.resolve_asterisk(asterisk_resolver),
            Self::JoinedTable(joined_tbl) => joined_tbl.table.resolve_asterisk(asterisk_resolver),
        }
    }
}

impl<'q> ResolveAsterisk<'q> for TableFactor<'q, Analyzed> {
    /// Expand `*` / `t.*` against this factor.
    /// Qualifier mismatches are reported distinguishing
    /// the one recoverable-by-the-user case: naming the original table
    /// instead of its alias gets the dedicated "perhaps you meant the alias" error
    /// and anything else the generic missing-FROM-entry error.
    fn resolve_asterisk(
        &self,
        asterisk_resolver: AsteriskResolver<'q, '_>,
    ) -> AstResult<Vec<ProjectionExpr<'q, Analyzed>>> {
        self.inner.resolve_asterisk(asterisk_resolver)
    }
}

impl<'q> ResolveAsterisk<'q> for TableFactorInner<'q, Analyzed> {
    fn resolve_asterisk(
        &self,
        asterisk_resolver: AsteriskResolver<'q, '_>,
    ) -> AstResult<Vec<ProjectionExpr<'q, Analyzed>>> {
        match self {
            Self::CteOrTable(cte_or_table) => cte_or_table.resolve_asterisk(asterisk_resolver),
            Self::SubQuery(stmt) => asterisk_resolver.resolve_from_subquery(stmt),
        }
    }
}

impl<'q> ResolveAsterisk<'q> for AnalyzedCteOrTable<'q> {
    fn resolve_asterisk(
        &self,
        asterisk_resolver: AsteriskResolver<'q, '_>,
    ) -> AstResult<Vec<ProjectionExpr<'q, Analyzed>>> {
        match self {
            Self::Cte(cte) => asterisk_resolver.resolve_from_cte(cte),
            Self::Table(table) => asterisk_resolver.resolve_from_table(table),
        }
    }
}

/// Context for expanding one `*` / `t.*`. Expansion manufactures resolved
/// column references that never existed in the source; each needs the
/// `Rc`-shared FROM to point into, the position of the relation being
/// expanded, and fresh type-system mirror ids, so the synthesized references
/// end up typed exactly like explicitly written ones.
///
/// `reduced_vars` names the columns a `USING` merge has already emitted once,
/// up front. It is empty for a qualified asterisk.
struct AsteriskResolver<'q, 'ast> {
    tbl_factor: &'ast Rc<TableFactor<'q, Analyzed>>,
    type_system: &'ast mut TypeSystem,
    reduced_vars: &'ast HashSet<&'ast BoundVar<'q>>,
}

const SUBQUERY_DISPLAY_PREFIX: usize = 32;

fn proj_expr<'q>(
    ts: &mut TypeSystem,
    attr: AttributeView<'q, '_>,
    var: BoundVar<'q>,
) -> ProjectionExpr<'q, Analyzed> {
    let curr_id = ts.next_expr_id;
    ts.next_expr_id += 1;

    let t_expr = texpr_from_attribute(attr, curr_id);
    let proj_expr = ProjectionExpr::from_bound_var(var, t_expr.id());
    ts.ctx.curr_proj.push(t_expr);
    proj_expr
}

impl<'q, 'ast> AsteriskResolver<'q, 'ast> {
    fn new(
        tbl_factor: &'ast Rc<TableFactor<'q, Analyzed>>,
        type_system: &'ast mut TypeSystem,
        reduced_vars: &'ast HashSet<&'ast BoundVar<'q>>,
    ) -> Self {
        Self {
            tbl_factor,
            type_system,
            reduced_vars,
        }
    }

    fn bound_var(&self, column_pos: usize) -> BoundVar<'q> {
        BoundVar::from_parts(Rc::clone(self.tbl_factor), column_pos)
    }

    /// A column a `USING` merge already emitted up front.
    fn reduced(&self, var: &BoundVar<'q>) -> bool {
        self.reduced_vars.contains(var)
    }

    fn resolve_from_cte(
        self,
        cte: &'ast Cte<'q, Analyzed>,
    ) -> AstResult<Vec<ProjectionExpr<'q, Analyzed>>> {
        let mut proj_exprs = vec![];
        for column_pos in 0..cte.body_ref().result_columns_cnt()? {
            let var = self.bound_var(column_pos);
            if self.reduced(&var) {
                continue;
            }
            let attr = cte.attribute(column_pos).ok_or_else(|| {
                analyze_error(format_smolstr!(
                    "cannot get attribute from '{}' CTE result",
                    cte.name.as_str()
                ))
            })?;

            proj_exprs.push(proj_expr(self.type_system, attr, var));
        }
        Ok(proj_exprs)
    }

    fn resolve_from_table(
        self,
        table: &'ast Table,
    ) -> AstResult<Vec<ProjectionExpr<'q, Analyzed>>> {
        let mut proj_exprs = vec![];
        for (column_pos, column) in table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, col)| matches!(col.role, ColumnRole::User))
        {
            let var = self.bound_var(column_pos);
            if self.reduced(&var) {
                continue;
            }
            proj_exprs.push(proj_expr(
                self.type_system,
                AttributeView::Column(column, None),
                var,
            ));
        }
        Ok(proj_exprs)
    }

    fn resolve_from_subquery(
        self,
        stmt: &'ast MultisetStmt<'q, Analyzed>,
    ) -> AstResult<Vec<ProjectionExpr<'q, Analyzed>>> {
        let mut proj_exprs = vec![];
        for column_pos in 0..stmt.result_columns_cnt()? {
            let var = self.bound_var(column_pos);
            if self.reduced(&var) {
                continue;
            }
            let attr = stmt.attribute(column_pos).ok_or_else(|| {
                analyze_error(format_smolstr!(
                    "cannot get attribute from subquery '({})' result",
                    stmt.to_string()
                        .chars()
                        .take(SUBQUERY_DISPLAY_PREFIX)
                        .collect::<String>()
                ))
            })?;

            proj_exprs.push(proj_expr(self.type_system, attr, var));
        }
        Ok(proj_exprs)
    }
}
