//! Everything a `SELECT` says after its select list.
//!
//! # `TableExpression<'q, State: AstState<'q>>`
//! The clauses that build and narrow the row source: the mandatory
//! [`From`](struct@From), then `WHERE`, `GROUP BY`, `HAVING` and the `WINDOW`
//! clause.
//!
//! They are grouped into one node because SQL evaluates them as a unit, before
//! projection. This is also why `HAVING` may reference grouping keys that the
//! select list never mentions.
//!
//!
//! # Tree structure
//! [`From`](struct@From) is a [`TableFactor`] optionally extended by [`JoinedTable`]s.
//!
//! A factor ([`TableFactorInner`]) is one of these forms
//! * a CTE or table name,
//! * a parenthesized subquery.
//!
//! Joins nest to the left, so a chain of them is a list hanging off one factor
//! rather than a tree of pairs.

use std::collections::HashSet;
use std::fmt::{Display, Error, Formatter};

use crate::error::{ast_invariant_err, AstResult};
use crate::expr::{Expr, ExprInner, RawVar};
use crate::multiset::{Cte, MultisetStmt};
use crate::rendering::{raw_display, render_via_display, Render};
use crate::scope::{CteRef, Depth, FrameView, Scope};
use crate::select::{ProjectionExpr, SelectList, SelectListExprs};
use crate::window::NamedWindows;
use crate::{write_sql_ident, Analyzed, AstState, Ident, NamedEntity, Raw, NONAME_COLUMN};
use smol_str::format_smolstr;
use sql_ir::ir::relation::{Column, ColumnRole, Table};
use sql_ir::ir::types::DerivedType;
use std::hash::{Hash, Hasher};

pub struct TableExpression<'q, State: AstState<'q>> {
    from: From<'q, State>,
    selection: Option<Expr<'q, State>>,
    /// TODO: support instruction for  aggregating the entire set.
    /// I.e. present but empty GROUP BY clause. E.g. `SELECT SUM(a) FROM t GROUP BY ()`.
    group_by: GroupBy<'q, State>,
    having: Option<Expr<'q, State>>,
    windows: NamedWindows<'q, State>,
}

impl<'q, State: AstState<'q>> TableExpression<'q, State> {
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        From<'q, State>,
        Option<Expr<'q, State>>,
        GroupBy<'q, State>,
        Option<Expr<'q, State>>,
        NamedWindows<'q, State>,
    ) {
        (
            self.from,
            self.selection,
            self.group_by,
            self.having,
            self.windows,
        )
    }

    pub fn from_parts(
        from: From<'q, State>,
        selection: Option<Expr<'q, State>>,
        group_by: GroupBy<'q, State>,
        having: Option<Expr<'q, State>>,
        windows: NamedWindows<'q, State>,
    ) -> Self {
        Self {
            from,
            selection,
            group_by,
            having,
            windows,
        }
    }
}

impl<'q> TableExpression<'q, Analyzed> {
    #[allow(clippy::type_complexity)]
    pub fn parts_mut(
        &mut self,
    ) -> (
        &mut From<'q, Analyzed>,
        Option<&mut Expr<'q, Analyzed>>,
        &mut GroupBy<'q, Analyzed>,
        Option<&mut Expr<'q, Analyzed>>,
        &mut NamedWindows<'q, Analyzed>,
    ) {
        (
            &mut self.from,
            self.selection.as_mut(),
            &mut self.group_by,
            self.having.as_mut(),
            &mut self.windows,
        )
    }

    pub fn from_ref(&self) -> &From<'q, Analyzed> {
        &self.from
    }

    pub(crate) fn has_group_by(&self) -> bool {
        !self.group_by.0.is_empty()
    }

    pub(crate) fn has_having(&self) -> bool {
        self.having.is_some()
    }

    pub fn grouping_vars(&self, select_list: &SelectListExprs<'q, Analyzed>) -> HashSet<BoundVar> {
        self.group_by
            .0
            .iter()
            .filter_map(|elem| elem.var_ref(select_list))
            .copied()
            .collect()
    }

    pub(crate) fn set_from(&mut self, from: From<'q, Analyzed>) {
        self.from = from;
    }

    pub fn set_grby(&mut self, grby: GroupBy<'q, Analyzed>) {
        self.group_by = grby;
    }

    pub fn set_windows(&mut self, windows: NamedWindows<'q, Analyzed>) {
        self.windows = windows;
    }
}

impl<'q, State: AstState<'q>> Render<'q, State> for TableExpression<'q, State> {
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut State::RenderScope<'a>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        f.write_str("FROM ")?;
        self.from.render(f, scope)?;
        if let Some(selection) = &self.selection {
            f.write_str(" WHERE ")?;
            selection.render(f, scope)?;
        }

        self.group_by.render(f, scope)?;

        if let Some(having_expr) = &self.having {
            f.write_str(" HAVING ")?;
            having_expr.render(f, scope)?;
        }

        self.windows.render(f, scope)
    }
}

raw_display!(TableExpression);

/// List of entries. Every FROM entry is [`TableFactor`] or [`JoinedTable`].
pub struct From<'q, State: AstState<'q>> {
    /// Read by the analyzer's asterisk expansion.
    tbl_factors: Vec<FromEntry<'q, State>>,
}

impl<'q, State: AstState<'q>> From<'q, State> {
    pub fn from_tbl_factors(tbl_factors: Vec<FromEntry<'q, State>>) -> Self {
        Self { tbl_factors }
    }
}

impl<'q> From<'q, Raw> {
    pub fn into_tbl_factors(self) -> Vec<FromEntry<'q, Raw>> {
        self.tbl_factors
    }
}

impl<'q> From<'q, Analyzed> {
    pub fn empty() -> Self {
        Self::from_tbl_factors(vec![])
    }

    pub fn is_empty(&self) -> bool {
        self.tbl_factors.is_empty()
    }

    pub fn add_entry(&mut self, entry: FromEntry<'q, Analyzed>) {
        self.tbl_factors.push(entry);
    }

    pub fn entries(&self) -> &[FromEntry<'q, Analyzed>] {
        &self.tbl_factors
    }

    pub fn entries_mut(&mut self) -> &mut [FromEntry<'q, Analyzed>] {
        &mut self.tbl_factors
    }

    pub fn pop_entry(&mut self) -> AstResult<FromEntry<'q, Analyzed>> {
        self.tbl_factors
            .pop()
            .ok_or_else(|| ast_invariant_err(format_smolstr!("expected to find entry in FROM")))
    }

    /// Find corresponding `JOIN/USING` clause for bound variable.
    /// [`None`] if no `JOIN/USING` for this column.
    /// In case of multiple `JOIN/USING` for this column in FROM clause returns last one.
    pub fn join_using_var(&self, var: BoundVar) -> Option<&BoundJoinUsingVar> {
        self.entries().iter().rev().find_map(|entry| {
            entry
                .joined()?
                .using_cols
                .iter()
                .find(|using| using.left == var)
        })
    }

    /// Route a column reference against the whole clause: the entry it lands
    /// in (by position) and the column within that entry.
    pub fn column_route<S: Scope<'q> + ?Sized>(
        &self,
        var: &RawVar,
        scope: &S,
    ) -> ColumnRoute<(usize, usize)> {
        route_column(self.entries(), var, scope)
    }
}

/// Route a column reference against a run of FROM entries. The entry position
/// in the outcome counts from the start of `entries`.
///
/// [`From::column_route`] routes against the whole clause. A `JOIN ... USING`
/// column is routed twice, against the entries preceding the joined table and
/// against the joined table alone, which is why the entries come as a slice.
pub fn route_column<'q, S: Scope<'q> + ?Sized>(
    entries: &[FromEntry<'q, Analyzed>],
    var: &RawVar,
    scope: &S,
) -> ColumnRoute<(usize, usize)> {
    let tbl_qualifier = var.table_name();

    tbl_qualifier.map_or_else(
        || {
            let column_routes = entries
                .iter()
                .enumerate()
                .map(|(entry_pos, entry)| {
                    entry
                        .column_route(var, scope)
                        .map(|col_pos| (entry_pos, col_pos))
                })
                .filter(|route| !matches!(route, ColumnRoute::ColumnMissing))
                .collect::<Vec<_>>();

            if column_routes.len() > 1 {
                // Resolved into more than 1 entity
                ColumnRoute::Ambigious
            } else if column_routes
                .iter()
                .any(|route| matches!(route, ColumnRoute::Ambigious))
            {
                // Any FROM entry has ambigious resolution
                // For example, query `SELECT a FROM (SELECT 1 AS a, 2 AS a)` has ambigious `a` column.
                ColumnRoute::Ambigious
            } else {
                column_routes
                    .into_iter()
                    .find(|route| matches!(route, ColumnRoute::Resolved(_)))
                    .unwrap_or(ColumnRoute::NoMatch)
            }
        },
        |t_name| {
            entries
                .iter()
                .enumerate()
                .find(|(_, entry)| entry.name().is_some_and(|name| name == t_name))
                .map_or(
                    ColumnRoute::NoMatch,
                    // This path should not return `FromColumnRoute::NoMatch`.
                    |(entry_pos, entry)| {
                        entry
                            .column_route(var, scope)
                            .map(|col_pos| (entry_pos, col_pos))
                    },
                )
        },
    )
}

impl<'q, State: AstState<'q>> Render<'q, State> for From<'q, State> {
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut State::RenderScope<'a>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        let (initial, joined) = self.tbl_factors.split_first().ok_or(Error)?;
        initial.render(f, scope)?;
        for joined_ttbl in joined.iter() {
            f.write_str(" ")?;
            joined_ttbl.render(f, scope)?;
        }
        Ok(())
    }
}

impl Default for From<'_, Analyzed> {
    fn default() -> Self {
        Self {
            tbl_factors: vec![],
        }
    }
}

/// Relevant for analyzed [`From`] only.
pub enum FromEntry<'q, State: AstState<'q>> {
    TableFactor(TableFactor<'q, State>),
    JoinedTable(JoinedTable<'q, State>),
}

impl<'q, State: AstState<'q>> NamedEntity for FromEntry<'q, State> {
    fn name(&self) -> Option<&str> {
        match self {
            Self::TableFactor(tbl_factor) => tbl_factor.name(),
            Self::JoinedTable(joined_tbl) => joined_tbl.table.name(),
        }
    }
}

impl<'q> FromEntry<'q, Analyzed> {
    /// Route a column reference into this entry.
    ///
    /// A `USING` merge collapses the two joined columns into one, and that one
    /// belongs to the *left* input.
    ///
    /// A qualified reference names the joined table itself rather than the join
    /// output, so it still sees the hidden copy.
    pub(crate) fn column_route<S: Scope<'q> + ?Sized>(
        &self,
        var: &RawVar,
        scope: &S,
    ) -> UniqueColumnRoute<usize> {
        let Some(col_name) = var.name() else {
            return UniqueColumnRoute::ColumnMissing;
        };
        match self {
            Self::TableFactor(tbl_factor) => tbl_factor.column_route(col_name, None, scope),
            Self::JoinedTable(joined_tbl) => {
                let merged_away = var.table_name().is_none().then(|| {
                    joined_tbl
                        .using_cols
                        .iter()
                        .map(BoundJoinUsingVar::joined_tbl_column_pos)
                        .collect::<Vec<_>>()
                });
                joined_tbl
                    .table
                    .column_route(col_name, merged_away.as_deref(), scope)
            }
        }
    }

    pub fn attribute<'a, S: Scope<'q> + ?Sized>(
        &'a self,
        column_pos: usize,
        scope: &'a S,
    ) -> Option<AttributeView<'q, 'a>> {
        self.tbl_factor().attribute(column_pos, scope)
    }

    pub fn tbl_factor(&self) -> &TableFactor<'q, Analyzed> {
        match self {
            Self::TableFactor(tbl_factor) => tbl_factor,
            Self::JoinedTable(joined_tbl) => &joined_tbl.table,
        }
    }

    pub fn joined(&self) -> Option<&JoinedTable<'q, Analyzed>> {
        match self {
            Self::TableFactor(_) => None,
            Self::JoinedTable(joined_tbl) => Some(joined_tbl),
        }
    }
}

impl<'q, State: AstState<'q>> Render<'q, State> for FromEntry<'q, State> {
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut State::RenderScope<'a>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        match self {
            Self::TableFactor(tbl_factor) => tbl_factor.render(f, scope),
            Self::JoinedTable(joined_tbl) => joined_tbl.render(f, scope),
        }
    }
}

pub struct TableFactor<'q, State: AstState<'q>> {
    /// Read by the analyzer's asterisk expansion.
    pub inner: TableFactorInner<'q, State>,
    /// Read by the analyzer's asterisk expansion.
    pub alias: Option<Ident>,
    /// Written by the parser, which builds the factor field by field.
    pub indexed_by: Option<Ident>,
}

impl<'q, State: AstState<'q>> TableFactor<'q, State> {
    pub fn into_parts(self) -> (TableFactorInner<'q, State>, Option<Ident>, Option<Ident>) {
        (self.inner, self.alias, self.indexed_by)
    }

    pub fn from_parts(
        inner: TableFactorInner<'q, State>,
        alias: Option<Ident>,
        indexed_by: Option<Ident>,
    ) -> Self {
        Self {
            inner,
            alias,
            indexed_by,
        }
    }
}

impl<'q, State: AstState<'q>> NamedEntity for TableFactor<'q, State> {
    fn name(&self) -> Option<&str> {
        self.alias
            .as_ref()
            .map_or_else(|| self.inner.name(), |alias| Some(alias.as_str()))
    }
}

impl<'q> TableFactor<'q, Analyzed> {
    /// Match a possibly qualified reference against this factor.
    /// An alias replaces the table name for qualification.
    /// Qualifying by the original name of an aliased table deliberately finds nothing.
    pub(crate) fn column_route<S: Scope<'q> + ?Sized>(
        &self,
        column_name: &str,
        exclude_positions: Option<&[usize]>,
        scope: &S,
    ) -> UniqueColumnRoute<usize> {
        match &self.inner {
            TableFactorInner::CteOrTable(cte_or_table) => {
                cte_or_table.column_route(column_name, exclude_positions, scope)
            }
            // An unaliased FROM-subquery can never satisfy a qualifier because
            // qualification goes through the alias, handled by the factor.
            TableFactorInner::SubQuery(stmt) => stmt.column_route(column_name, exclude_positions),
            TableFactorInner::Function(func) => {
                columns_route(&func.columns, column_name, exclude_positions)
            }
        }
    }

    pub fn attribute<'a, S: Scope<'q> + ?Sized>(
        &'a self,
        column_pos: usize,
        scope: &'a S,
    ) -> Option<AttributeView<'q, 'a>> {
        match &self.inner {
            TableFactorInner::CteOrTable(cte_or_table) => cte_or_table.attribute(column_pos, scope),
            TableFactorInner::SubQuery(stmt) => stmt.attribute(column_pos),
            TableFactorInner::Function(func) => func
                .columns
                .get(column_pos)
                .map(|col| AttributeView::Column(col, None)),
        }
    }

    pub fn table(&self) -> Option<&Table> {
        match &self.inner {
            TableFactorInner::CteOrTable(cte_or_tbl) => cte_or_tbl.table(),
            TableFactorInner::SubQuery(_) | TableFactorInner::Function(_) => None,
        }
    }
}

impl<'q, State: AstState<'q>> Render<'q, State> for TableFactor<'q, State> {
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut State::RenderScope<'a>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        self.inner.render(f, scope)?;
        self.alias
            .as_ref()
            .map_or(Ok(()), |alias| write!(f, " AS {}", alias))?;
        self.indexed_by
            .as_ref()
            .map_or(Ok(()), |indexed_by| write!(f, " INDEXED BY {}", indexed_by))?;
        Ok(())
    }
}

pub enum TableFactorInner<'q, State: AstState<'q>> {
    /// Table name or analyzed entity
    CteOrTable(State::CteOrTableT),
    SubQuery(Box<MultisetStmt<'q, State>>),
    Function(TableFunction<'q, State>),
}

impl<'q, State: AstState<'q>> NamedEntity for TableFactorInner<'q, State> {
    fn name(&self) -> Option<&str> {
        match self {
            Self::CteOrTable(cte_or_table) => cte_or_table.name(),
            Self::SubQuery(_) => None,
            Self::Function(func) => func.name(),
        }
    }
}

impl<'q, State: AstState<'q>> Render<'q, State> for TableFactorInner<'q, State> {
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut State::RenderScope<'a>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        match self {
            TableFactorInner::CteOrTable(src) => src.render(f, scope),
            TableFactorInner::SubQuery(src) => {
                f.write_str("(")?;
                src.render(f, scope)?;
                f.write_str(")")
            }
            TableFactorInner::Function(src) => src.render(f, scope),
        }
    }
}

/// A table function in FROM, like `generate_series(1, 3) WITH ORDINALITY`.
pub struct TableFunction<'q, State: AstState<'q>> {
    /// The function call. Its type is the type of the `value` column.
    pub call: Expr<'q, State>,
    pub with_ordinality: bool,
    /// The result columns. The analyzer fills them.
    pub columns: Vec<Column>,
}

impl<'q, State: AstState<'q>> NamedEntity for TableFunction<'q, State> {
    /// A function scan without an alias has the name of the function.
    fn name(&self) -> Option<&str> {
        match &self.call.inner {
            ExprInner::FunctionCall(call) => Some(call.name()),
            _ => None,
        }
    }
}

impl<'q, State: AstState<'q>> Render<'q, State> for TableFunction<'q, State> {
    /// The call is written without its type, so that the rendering parses.
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut State::RenderScope<'a>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        self.call.inner.render(f, scope)?;
        if self.with_ordinality {
            f.write_str(" WITH ORDINALITY")?;
        }
        Ok(())
    }
}

pub struct JoinedTable<'q, State: AstState<'q>> {
    /// Body of the joined table.
    pub table: TableFactor<'q, State>,
    /// Kind of `JOIN` operation. Currently Picodata supports only some of them.
    /// TODO: support all `JOIN` kinds.
    pub kind: JoinKind,
    pub condition: Option<Expr<'q, State>>,
    pub using_cols: Vec<State::JoinUsingColumnT>,
}

impl<'q, State: AstState<'q>> JoinedTable<'q, State> {
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        TableFactor<'q, State>,
        JoinKind,
        Option<Expr<'q, State>>,
        Vec<State::JoinUsingColumnT>,
    ) {
        (self.table, self.kind, self.condition, self.using_cols)
    }
}

impl<'q> JoinedTable<'q, Analyzed> {
    pub fn from_parts(
        table: TableFactor<'q, Analyzed>,
        kind: JoinKind,
        condition: Option<Expr<'q, Analyzed>>,
        using_cols: Vec<BoundJoinUsingVar>,
    ) -> Self {
        Self {
            table,
            kind,
            condition,
            using_cols,
        }
    }
}

impl<'q, State: AstState<'q>> Render<'q, State> for JoinedTable<'q, State> {
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut State::RenderScope<'a>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        match self.kind {
            JoinKind::Inner => write!(f, "INNER JOIN")?,
            JoinKind::Left => write!(f, "LEFT OUTER JOIN")?,
            JoinKind::Cross => write!(f, "CROSS JOIN")?,
        }
        f.write_str(" ")?;
        self.table.render(f, scope)?;
        if matches!(self.kind, JoinKind::Cross) {
            return Ok(());
        }
        // Parsing rejects a non-CROSS JOIN without USING and without ON.
        if let Some((first_col, others)) = self.using_cols.split_first() {
            f.write_str(" USING (")?;
            first_col.render(f, scope)?;
            for col_name in others {
                f.write_str(", ")?;
                col_name.render(f, scope)?;
            }
            f.write_str(")")
        } else if let Some(condition) = &self.condition {
            f.write_str(" ON ")?;
            condition.render(f, scope)
        } else {
            Ok(())
        }
    }
}

#[derive(PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Cross,
}

pub struct GroupBy<'q, State: AstState<'q>>(pub Vec<OrdrByGrpByElem<'q, State>>);

impl GroupBy<'_, Analyzed> {
    pub fn contains_pos(&self, pos: usize) -> bool {
        self.0
            .iter()
            .any(|elem| elem.ordinal().is_some_and(|elem_pos| elem_pos == pos))
    }
}

impl GroupBy<'_, Raw> {
    pub fn empty() -> Self {
        Self(vec![])
    }
}

impl Default for GroupBy<'_, Analyzed> {
    fn default() -> Self {
        Self(vec![])
    }
}

impl<'q, State: AstState<'q>> Render<'q, State> for GroupBy<'q, State> {
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut State::RenderScope<'a>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        if let Some((first_gb_expr, other_gb_exprs)) = self.0.split_first() {
            f.write_str(" GROUP BY ")?;
            first_gb_expr.render(f, scope)?;
            for expr in other_gb_exprs {
                f.write_str(", ")?;
                expr.render(f, scope)?;
            }
        }
        Ok(())
    }
}

/// Elementary expresion common for [`GroupBy`] and `OrderBy`.
pub enum OrdrByGrpByElem<'q, State: AstState<'q>> {
    /// `0`-based
    Ordinal(usize),
    Expr(Expr<'q, State>),
}

impl<'q> OrdrByGrpByElem<'q, Analyzed> {
    pub fn var_ref<'ast>(
        &'ast self,
        select_list: &'ast SelectListExprs<'q, Analyzed>,
    ) -> Option<&'ast BoundVar> {
        match self {
            Self::Ordinal(pos) => select_list.var_ref(*pos),
            Self::Expr(expr) => expr.var_ref(),
        }
    }

    /// The expression a bound GROUP BY element groups by, or [`None`] for the bare
    /// column references and for an ordinal naming that `grouping_vars` already covers.
    pub fn grouping_key_expr<'ast>(
        &'ast self,
        select_list: Option<&'ast SelectList<'q, Analyzed>>,
    ) -> Option<&'ast Expr<'q, Analyzed>> {
        let key = match self {
            Self::Expr(expr) => expr,
            Self::Ordinal(pos) => select_list?.expr_at(*pos)?,
        };
        (!matches!(key.inner_ref(), ExprInner::Var(_))).then_some(key)
    }

    fn ordinal(&self) -> Option<usize> {
        match self {
            Self::Ordinal(pos) => Some(*pos),
            Self::Expr(_) => None,
        }
    }
}

impl<'q, State: AstState<'q>> Render<'q, State> for OrdrByGrpByElem<'q, State> {
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut State::RenderScope<'a>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        match self {
            Self::Ordinal(pos) => write!(f, "{}", pos + 1),
            Self::Expr(expr) => expr.render(f, scope),
        }
    }
}

/// What a FROM-clause name resolved to. The CTE arm keeps the name the query
/// wrote and points at the analyzed body by position in the `WITH` clause that
/// declared it, so every use of a CTE reads the one analyzed body through the
/// [`Scope`].
pub enum AnalyzedCteOrTable {
    Cte { name: Ident, cte: CteRef },
    Table(Table),
}

impl AnalyzedCteOrTable {
    /// Position of `column_name` in this source, honoring the qualifier.
    /// CTE lookup goes through the explicit column list when present (it
    /// overrides the body's output names). Table lookup sees user columns
    /// only, keeping other columns invisible to SQL name resolution.
    ///
    /// A CTE the scope cannot supply is an analyzer invariant violation;
    /// routing has no error channel, so it reads as a missing column.
    pub(crate) fn column_route<'q, S: Scope<'q> + ?Sized>(
        &self,
        column_name: &str,
        exclude_positions: Option<&[usize]>,
        scope: &S,
    ) -> UniqueColumnRoute<usize> {
        let lookup_cte = |cte: &Cte<'q, Analyzed>, exclude_positions: Option<&[usize]>| {
            let cte_columns_def = cte.columns_ref();
            if cte_columns_def.is_empty() {
                cte.body_ref().column_route(column_name, exclude_positions)
            } else {
                if cte_columns_def
                    .iter()
                    .filter(|name| name.as_str() == column_name)
                    .count()
                    > 1
                {
                    UniqueColumnRoute::Ambigious
                } else {
                    cte_columns_def
                        .iter()
                        .enumerate()
                        .find(|(pos, name)| {
                            !exclude_positions
                                .as_ref()
                                .is_some_and(|positions| positions.contains(pos))
                                && name.as_str() == column_name
                        })
                        .map_or(UniqueColumnRoute::ColumnMissing, |(pos, _)| {
                            UniqueColumnRoute::Resolved(pos)
                        })
                }
            }
        };

        match self {
            Self::Cte { cte, .. } => scope
                .cte(*cte)
                .map_or(UniqueColumnRoute::ColumnMissing, |cte| {
                    lookup_cte(cte, exclude_positions)
                }),
            Self::Table(table) => columns_route(&table.columns, column_name, exclude_positions),
        }
    }

    fn attribute<'a, 'q, S: Scope<'q> + ?Sized>(
        &'a self,
        column_pos: usize,
        scope: &'a S,
    ) -> Option<AttributeView<'q, 'a>> {
        match self {
            Self::Cte { cte, .. } => scope.cte(*cte)?.attribute(column_pos),
            Self::Table(tbl) => tbl
                .columns
                .get(column_pos)
                .map(|col| AttributeView::Column(col, None)),
        }
    }

    pub fn table(&self) -> Option<&Table> {
        match self {
            Self::Cte { .. } => None,
            Self::Table(tbl) => Some(tbl),
        }
    }
}

/// Position of the user column `column_name` among `columns`. Other columns
/// are invisible to SQL name resolution.
fn columns_route(
    columns: &[Column],
    column_name: &str,
    exclude_positions: Option<&[usize]>,
) -> UniqueColumnRoute<usize> {
    columns
        .iter()
        .enumerate()
        .find(|(pos, col)| {
            !exclude_positions.is_some_and(|positions| positions.contains(pos))
                && matches!(col.role, ColumnRole::User)
                && col.name == column_name
        })
        .map_or(UniqueColumnRoute::ColumnMissing, |(pos, _)| {
            UniqueColumnRoute::Resolved(pos)
        })
}

impl NamedEntity for AnalyzedCteOrTable {
    fn name(&self) -> Option<&str> {
        match self {
            Self::Cte { name, .. } => Some(name.as_str()),
            Self::Table(tbl) => Some(&tbl.name),
        }
    }
}

impl Display for AnalyzedCteOrTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Self::Cte { name, .. } => write_sql_ident(f, name.as_str()),
            Self::Table(tbl) => write_sql_ident(f, &tbl.name),
        }
    }
}

render_via_display!(AnalyzedCteOrTable, BoundJoinUsingVar, JoinUsingColumn);

/// A resolved column reference: the `column_pos`-th column of the `entry`-th
/// `FROM` entry of the statement at `depth` (see [`Scope`]). Identity is that
/// position; the column's declared type rides along so that typing never has
/// to look the column up again (PostgreSQL keeps `vartype` on a `Var` for the
/// same reason).
#[derive(Clone, Copy, Debug)]
pub struct BoundVar {
    pub depth: Depth,
    pub entry: usize,
    pub column_pos: usize,
    data_type: DerivedType,
}

impl BoundVar {
    pub fn new(depth: Depth, entry: usize, column_pos: usize, data_type: DerivedType) -> Self {
        Self {
            depth,
            entry,
            column_pos,
            data_type,
        }
    }

    pub fn data_type(&self) -> DerivedType {
        self.data_type
    }
}

impl PartialEq for BoundVar {
    fn eq(&self, other: &Self) -> bool {
        (self.depth, self.entry, self.column_pos) == (other.depth, other.entry, other.column_pos)
    }
}

impl Eq for BoundVar {}

impl Hash for BoundVar {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.depth, self.entry, self.column_pos).hash(state);
    }
}

/// `relation.column`, both names read through the scope; a column that the
/// scope cannot name (an anonymous projection) prints as `?column?`.
impl<'q> Render<'q, Analyzed> for BoundVar {
    fn render<'a>(
        &'a self,
        f: &mut Formatter<'_>,
        scope: &mut Vec<FrameView<'a, 'q>>,
    ) -> Result<(), Error>
    where
        'q: 'a,
    {
        match scope.attribute(*self).and_then(|attr| attr.name()) {
            Some(column_name) => {
                if let Some(source_name) = scope.source_name(*self) {
                    write_sql_ident(f, source_name)?;
                    write!(f, ".")?;
                }
                write_sql_ident(f, column_name)
            }
            None => write!(f, "{NONAME_COLUMN}"),
        }
    }
}

#[derive(PartialEq)]
pub struct JoinUsingColumn(pub RawVar);

impl Display for JoinUsingColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.0)
    }
}

impl NamedEntity for JoinUsingColumn {
    fn name(&self) -> Option<&str> {
        self.0.name()
    }
}

/// One `USING` column, resolved against both sides of the join.
/// The left input's column first, then the joined table's own.
///
/// The two are merged into a single output column. Picodata joins are inner or
/// left, and for both SQL says the merged column *is* the left one — but its
/// *type* is the common type of the two inputs, derived exactly like one output
/// column of a set operation. A bare (unqualified) reference to the column
/// reads that type; a qualified reference keeps the original column's own.
pub struct BoundJoinUsingVar {
    /// The column name the `USING` list wrote.
    name: Ident,
    left: BoundVar,
    joined: BoundVar,
    data_type: DerivedType,
}

impl BoundJoinUsingVar {
    pub fn new(name: Ident, left: BoundVar, joined: BoundVar, data_type: DerivedType) -> Self {
        Self {
            name,
            left,
            joined,
            data_type,
        }
    }

    /// Position, within the joined table.
    pub fn joined_tbl_column_pos(&self) -> usize {
        self.joined.column_pos
    }

    pub fn parts_ref(&self) -> (BoundVar, BoundVar) {
        (self.left, self.joined)
    }

    /// The merged output column's type: the common type of the two inputs.
    pub fn data_type(&self) -> DerivedType {
        self.data_type
    }
}

impl Display for BoundJoinUsingVar {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write_sql_ident(f, self.name.as_str())
    }
}

/// One output column of a FROM source, abstracting over its two shapes — a
/// projection expression (CTE and subquery bodies) or a real table column —
/// so resolution and asterisk expansion treat every source kind uniformly.
pub enum AttributeView<'q, 'ast> {
    Expr(&'ast ProjectionExpr<'q, Analyzed>, Option<&'ast str>),
    /// A real column, with an optional visible-name override: a CTE's explicit
    /// column list renames the columns of a column-shaped body (a `VALUES`
    /// body derives its own `column1..columnN` columns) the same way it
    /// renames a projection.
    Column(&'ast Column, Option<&'ast str>),
}

impl<'ast> AttributeView<'_, 'ast> {
    pub fn data_type(&self) -> DerivedType {
        match self {
            Self::Expr(proj_expr, _) => proj_expr.data_type(),
            Self::Column(col, _) => col.r#type,
        }
    }

    /// The name the column is visible as: the override when there is one,
    /// else the column's own; [`None`] for an anonymous projection.
    pub fn name(&self) -> Option<&'ast str> {
        match self {
            Self::Expr(_, name) => *name,
            Self::Column(col, rename) => rename.or(Some(col.name.as_str())),
        }
    }
}

/// Outcome of routing a column reference against all FROM entries.
///
/// `ColumnMissing` and `NoMatch` are deliberately distinct: PostgreSQL
/// scoping anchors a qualified reference at the innermost FROM-scope
/// relation matching the qualifier, so when that relation lacks the column
/// the lookup fails right there — it must not continue into enclosing
/// scopes where another same-named relation could satisfy it.
pub enum ColumnRoute<P> {
    /// The qualifier (if any) and the column both matched.
    Resolved(P),
    /// A relation matching the qualifier is here, but it has no such
    /// column: the reference is anchored and the scope search stops.
    /// Never produced for unqualified references — those keep searching outward.
    ColumnMissing,
    /// Nothing here matches the reference; the search continues outward.
    NoMatch,
    /// Ambigious
    Ambigious,
}

impl<P> ColumnRoute<P> {
    pub fn map<U>(self, f: impl FnOnce(P) -> U) -> ColumnRoute<U> {
        match self {
            Self::Resolved(route) => ColumnRoute::Resolved(f(route)),
            Self::ColumnMissing => ColumnRoute::ColumnMissing,
            Self::NoMatch => ColumnRoute::NoMatch,
            Self::Ambigious => ColumnRoute::Ambigious,
        }
    }
}

/// Outcome of routing a column reference against one FROM entry.
///
/// Resolution against one FROM entry is proceeded only in case
/// its name is equal to qualifier or there is no qualifier on column reference.
pub enum UniqueColumnRoute<P> {
    Resolved(P),
    ColumnMissing,
    Ambigious,
}

impl<P> UniqueColumnRoute<P> {
    pub(crate) fn map<U>(self, f: impl FnOnce(P) -> U) -> ColumnRoute<U> {
        match self {
            Self::Resolved(route) => ColumnRoute::Resolved(f(route)),
            Self::ColumnMissing => ColumnRoute::ColumnMissing,
            Self::Ambigious => ColumnRoute::Ambigious,
        }
    }
}

/// Structural comparison, one impl per node for both states.
mod structural_eq {

    use super::*;
    use crate::scope::EqDepths;
    use crate::structural_eq::StructuralEq;

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for TableExpression<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.from.eq_with(&other.from, scope)
                && self.selection.eq_with(&other.selection, scope)
                && self.group_by.eq_with(&other.group_by, scope)
                && self.having.eq_with(&other.having, scope)
                && self.windows.eq_with(&other.windows, scope)
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for From<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.tbl_factors.eq_with(&other.tbl_factors, scope)
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for FromEntry<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            match (self, other) {
                (Self::TableFactor(x), Self::TableFactor(y)) => x.eq_with(y, scope),
                (Self::JoinedTable(x), Self::JoinedTable(y)) => x.eq_with(y, scope),
                _mismatched_shapes => false,
            }
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for TableFactor<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.alias == other.alias
                && self.indexed_by == other.indexed_by
                && self.inner.eq_with(&other.inner, scope)
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for TableFactorInner<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            match (self, other) {
                (Self::CteOrTable(x), Self::CteOrTable(y)) => x.eq_with(y, scope),
                (Self::SubQuery(x), Self::SubQuery(y)) => x.eq_with(y, scope),
                (Self::Function(x), Self::Function(y)) => {
                    x.with_ordinality == y.with_ordinality && x.call.eq_with(&y.call, scope)
                }
                _mismatched_shapes => false,
            }
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for JoinedTable<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.kind == other.kind
                && self.table.eq_with(&other.table, scope)
                && self.condition.eq_with(&other.condition, scope)
                && self.using_cols.eq_with(&other.using_cols, scope)
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for GroupBy<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.0.eq_with(&other.0, scope)
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for OrdrByGrpByElem<'q, S> {
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            match (self, other) {
                (Self::Ordinal(x), Self::Ordinal(y)) => x == y,
                (Self::Expr(x), Self::Expr(y)) => x.eq_with(y, scope),
                _mismatched_shapes => false,
            }
        }
    }

    impl StructuralEq<()> for JoinUsingColumn {
        fn eq_with(&self, other: &Self, _scope: &mut ()) -> bool {
            self == other
        }
    }

    /// A resolved column reference: the same column of the same relation,
    /// regardless of the name it was written with. Relations are positions, so
    /// the same subquery written twice binds its references to equal values -
    /// at depths [`EqDepths`] relates when the two are written at different ones.
    impl StructuralEq<EqDepths> for BoundVar {
        fn eq_with(&self, other: &Self, scope: &mut EqDepths) -> bool {
            (self.entry, self.column_pos) == (other.entry, other.column_pos)
                && scope.same(self.depth, other.depth)
        }
    }

    /// One `USING` column: both resolved halves must match. The merged type is
    /// derived from them, so it carries no structural information of its own.
    impl StructuralEq<EqDepths> for BoundJoinUsingVar {
        fn eq_with(&self, other: &Self, scope: &mut EqDepths) -> bool {
            self.left.eq_with(&other.left, scope) && self.joined.eq_with(&other.joined, scope)
        }
    }

    impl StructuralEq<EqDepths> for AnalyzedCteOrTable {
        fn eq_with(&self, other: &Self, scope: &mut EqDepths) -> bool {
            match (self, other) {
                (Self::Cte { cte: x, .. }, Self::Cte { cte: y, .. }) => {
                    x.offset == y.offset && scope.same(x.depth, y.depth)
                }
                (Self::Table(x), Self::Table(y)) => x.name == y.name,
                _mismatched_shapes => false,
            }
        }
    }
}
