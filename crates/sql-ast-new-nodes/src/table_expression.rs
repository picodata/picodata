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
use std::rc::Rc;

use crate::error::{ast_invariant_err, AstResult};
use crate::expr::{Expr, ExprInner, RawVar};
use crate::multiset::{Cte, MultisetStmt};
use crate::select::{ProjectionExpr, SelectList, SelectListExprs};
use crate::window::NamedWindow;
use crate::{write_sql_ident, Analyzed, AstState, Ident, NamedEntity, Raw, NONAME_COLUMN};
use smol_str::format_smolstr;
use sql_ir::ir::relation::{Column, ColumnRole, Table};
use sql_ir::ir::types::DerivedType;
use std::hash::{Hash, Hasher};

/// Key of a bound table factor. In a nutshell it is just the address the shared
/// pointer points to.
pub(crate) type BoundTblKey = usize;

/// [`Analyzed`]'s scope for structural comparison ([`AstState::EqScope`]): the
/// FROM factors the two compared subtrees introduce, paired in FROM order and
/// keyed by [`Rc`] pointer identity like [`BoundVar::src_key`].
pub(crate) type RelationPairs = Vec<(BoundTblKey, BoundTblKey)>;

pub struct TableExpression<'q, State: AstState<'q>> {
    from: From<'q, State>,
    selection: Option<Expr<'q, State>>,
    /// TODO: support instruction for  aggregating the entire set.
    /// I.e. present but empty GROUP BY clause. E.g. `SELECT SUM(a) FROM t GROUP BY ()`.
    group_by: GroupBy<'q, State>,
    having: Option<Expr<'q, State>>,
    windows: Vec<NamedWindow<'q, State>>,
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
        Vec<NamedWindow<'q, State>>,
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
        windows: Vec<NamedWindow<'q, State>>,
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
        &mut Vec<NamedWindow<'q, Analyzed>>,
    ) {
        (
            &mut self.from,
            self.selection.as_mut(),
            &mut self.group_by,
            self.having.as_mut(),
            &mut self.windows,
        )
    }

    pub(crate) fn has_group_by(&self) -> bool {
        !self.group_by.0.is_empty()
    }

    pub(crate) fn has_having(&self) -> bool {
        self.having.is_some()
    }

    pub fn grouping_vars(
        &self,
        select_list: &SelectListExprs<'q, Analyzed>,
    ) -> HashSet<(BoundTblKey, usize)> {
        self.group_by
            .0
            .iter()
            .filter_map(|elem| elem.var_ref(select_list))
            .map(BoundVar::key)
            .collect()
    }

    pub(crate) fn set_from(&mut self, from: From<'q, Analyzed>) {
        self.from = from;
    }

    pub fn set_grby(&mut self, grby: GroupBy<'q, Analyzed>) {
        self.group_by = grby;
    }
}

impl<'q, State: AstState<'q>> Display for TableExpression<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "FROM {}", self.from)?;
        self.selection
            .as_ref()
            .map_or(Ok(()), |selection| write!(f, " WHERE {selection}"))?;

        write!(f, "{}", self.group_by)?;

        self.having
            .as_ref()
            .map_or(Ok(()), |having_expr| write!(f, " HAVING {having_expr}"))?;

        if let Some((first_window, other_windows)) = self.windows.split_first() {
            write!(f, " WINDOW {first_window}")?;
            for window in other_windows {
                write!(f, ", {window}")?;
            }
        }
        Ok(())
    }
}

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
    pub fn join_using_var(&self, var: &BoundVar<'q>) -> Option<&BoundJoinUsingVar<'q>> {
        self.entries().iter().rev().find_map(|entry| {
            entry
                .joined()?
                .using_cols
                .iter()
                .find(|using| &using.left == var)
        })
    }

    pub fn column_route(
        &self,
        var: &RawVar,
    ) -> ColumnRoute<(Rc<TableFactor<'q, Analyzed>>, usize)> {
        let tbl_qualifier = var.table_name();

        tbl_qualifier.map_or_else(
            || {
                let column_routes = self
                    .entries()
                    .iter()
                    .map(|entry| {
                        entry
                            .column_route(var)
                            .map(|col_pos| (Rc::clone(entry.tbl_factor()), col_pos))
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
                self.entries()
                    .iter()
                    .find(|entry| entry.name().is_some_and(|name| name == t_name))
                    .map_or(
                        ColumnRoute::NoMatch,
                        // This path should not return `FromColumnRoute::NoMatch`.
                        |entry| {
                            entry
                                .column_route(var)
                                .map(|col_pos| (Rc::clone(entry.tbl_factor()), col_pos))
                        },
                    )
            },
        )
    }
}

impl<'q, State: AstState<'q>> Display for From<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let (initial, joined) = self.tbl_factors.split_first().ok_or(Error)?;
        write!(f, "{initial}")?;
        for joined_ttbl in joined.iter() {
            write!(f, " {joined_ttbl}")?;
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
    TableFactor(State::TableFactorT),
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
    pub(crate) fn column_route(&self, var: &RawVar) -> UniqueColumnRoute<usize> {
        let Some(col_name) = var.name() else {
            return UniqueColumnRoute::ColumnMissing;
        };
        match self {
            Self::TableFactor(tbl_factor) => tbl_factor.column_route(col_name, None),
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
                    .column_route(col_name, merged_away.as_deref())
            }
        }
    }

    pub fn attribute(&'_ self, column_pos: usize) -> Option<AttributeView<'q, '_>> {
        match self {
            Self::TableFactor(tbl_factor) => tbl_factor.attribute(column_pos),
            Self::JoinedTable(joined_tbl) => joined_tbl.table.attribute(column_pos),
        }
    }

    pub fn tbl_factor(&self) -> &Rc<TableFactor<'q, Analyzed>> {
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

impl<'q, State: AstState<'q>> Display for FromEntry<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Self::TableFactor(tbl_factor) => write!(f, "{tbl_factor}"),
            Self::JoinedTable(joined_tbl) => write!(f, "{joined_tbl}"),
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
    pub(crate) fn column_route(
        &self,
        column_name: &str,
        exclude_positions: Option<&[usize]>,
    ) -> UniqueColumnRoute<usize> {
        self.inner.column_route(column_name, exclude_positions)
    }

    pub fn attribute(&'_ self, column_pos: usize) -> Option<AttributeView<'q, '_>> {
        self.inner.attribute(column_pos)
    }

    pub fn table(&self) -> Option<&Table> {
        self.inner.table()
    }
}

impl<'q, State: AstState<'q>> Display for TableFactor<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.inner)?;
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
}

impl<'q, State: AstState<'q>> NamedEntity for TableFactorInner<'q, State> {
    fn name(&self) -> Option<&str> {
        match self {
            Self::CteOrTable(cte_or_table) => cte_or_table.name(),
            Self::SubQuery(_) => None,
        }
    }
}

impl<'q> TableFactorInner<'q, Analyzed> {
    pub(crate) fn column_route(
        &self,
        column_name: &str,
        exclude_positions: Option<&[usize]>,
    ) -> UniqueColumnRoute<usize> {
        match self {
            Self::CteOrTable(cte_or_table) => {
                cte_or_table.column_route(column_name, exclude_positions)
            }
            // An unaliased FROM-subquery can never satisfy a qualifier because
            // qualification goes through the alias, handled by the factor.
            Self::SubQuery(stmt) => stmt.column_route(column_name, exclude_positions),
        }
    }

    fn attribute(&'_ self, column_pos: usize) -> Option<AttributeView<'q, '_>> {
        match self {
            Self::CteOrTable(cte_or_table) => cte_or_table.attribute(column_pos),
            Self::SubQuery(stmt) => stmt.attribute(column_pos),
        }
    }

    pub fn table(&self) -> Option<&Table> {
        match self {
            Self::CteOrTable(cte_or_tbl) => cte_or_tbl.table(),
            Self::SubQuery(_) => None,
        }
    }
}

impl<'q, State: AstState<'q>> Display for TableFactorInner<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            TableFactorInner::CteOrTable(src) => write!(f, "{src}"),
            TableFactorInner::SubQuery(src) => write!(f, "({src})"),
        }
    }
}

pub struct JoinedTable<'q, State: AstState<'q>> {
    /// Body of the joined table.
    pub table: State::TableFactorT,
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
        State::TableFactorT,
        JoinKind,
        Option<Expr<'q, State>>,
        Vec<State::JoinUsingColumnT>,
    ) {
        (self.table, self.kind, self.condition, self.using_cols)
    }
}

impl<'q> JoinedTable<'q, Analyzed> {
    pub fn from_parts(
        table: Rc<TableFactor<'q, Analyzed>>,
        kind: JoinKind,
        condition: Option<Expr<'q, Analyzed>>,
        using_cols: Vec<BoundJoinUsingVar<'q>>,
    ) -> Self {
        Self {
            table,
            kind,
            condition,
            using_cols,
        }
    }
}

impl<'q, State: AstState<'q>> Display for JoinedTable<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self.kind {
            JoinKind::Inner => write!(f, "INNER JOIN")?,
            JoinKind::Left => write!(f, "LEFT OUTER JOIN")?,
            JoinKind::Cross => write!(f, "CROSS JOIN")?,
        }
        write!(f, " {}", self.table)?;
        if matches!(self.kind, JoinKind::Cross) {
            return Ok(());
        }
        // Parsing rejects a non-CROSS JOIN without USING and without ON.
        if let Some((first_col, others)) = self.using_cols.split_first() {
            write!(f, " USING ({}", first_col)?;
            for col_name in others {
                write!(f, ", {}", col_name)?;
            }
            write!(f, ")")
        } else if let Some(condition) = &self.condition {
            write!(f, " ON {}", condition)
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

impl<'q, State: AstState<'q>> Display for GroupBy<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        if let Some((first_gb_expr, other_gb_exprs)) = self.0.split_first() {
            write!(f, " GROUP BY {first_gb_expr}")?;
            for expr in other_gb_exprs {
                write!(f, ", {expr}")?;
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
    ) -> Option<&'ast BoundVar<'q>> {
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

impl<'q, State: AstState<'q>> Display for OrdrByGrpByElem<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Self::Ordinal(pos) => write!(f, "{}", pos + 1),
            Self::Expr(expr) => write!(f, "{expr}"),
        }
    }
}

/// What a FROM-clause name resolved to. The CTE arm shares the `Rc` owned by
/// the WITH scope, so every use of a CTE points at the one analyzed body.
pub enum AnalyzedCteOrTable<'q> {
    Cte(Rc<Cte<'q, Analyzed>>),
    Table(Table),
}

impl<'q> AnalyzedCteOrTable<'q> {
    /// Position of `column_name` in this source, honoring the qualifier.
    /// CTE lookup goes through the explicit column list when present (it
    /// overrides the body's output names). Table lookup sees user columns
    /// only, keeping other columns invisible to SQL name resolution.
    pub(crate) fn column_route(
        &self,
        column_name: &str,
        exclude_positions: Option<&[usize]>,
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

        let lookup_table = |table: &Table, exclude_positions: Option<&[usize]>| {
            table
                .columns
                .iter()
                .enumerate()
                .find(|(pos, col)| {
                    !exclude_positions
                        .as_ref()
                        .is_some_and(|positions| positions.contains(pos))
                        && matches!(col.role, ColumnRole::User)
                        && col.name == column_name
                })
                .map(|(pos, _)| pos)
        };

        match self {
            Self::Cte(cte) => lookup_cte(cte, exclude_positions),
            Self::Table(table) => lookup_table(table, exclude_positions).map_or(
                UniqueColumnRoute::ColumnMissing,
                UniqueColumnRoute::Resolved,
            ),
        }
    }

    fn attribute(&'_ self, column_pos: usize) -> Option<AttributeView<'q, '_>> {
        match self {
            Self::Cte(cte) => cte.attribute(column_pos),
            Self::Table(tbl) => tbl
                .columns
                .get(column_pos)
                .map(|col| AttributeView::Column(col, None)),
        }
    }

    pub fn table(&self) -> Option<&Table> {
        match self {
            Self::Cte(_) => None,
            Self::Table(tbl) => Some(tbl),
        }
    }
}

impl NamedEntity for AnalyzedCteOrTable<'_> {
    fn name(&self) -> Option<&str> {
        match self {
            Self::Cte(cte) => cte.name(),
            Self::Table(tbl) => Some(&tbl.name),
        }
    }
}

impl Display for AnalyzedCteOrTable<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Self::Cte(cte) => write_sql_ident(f, cte.name.as_str()),
            Self::Table(tbl) => write_sql_ident(f, &tbl.name),
        }
    }
}

/// A resolved column reference.
#[derive(Clone)]
pub struct BoundVar<'q> {
    pub source: Rc<TableFactor<'q, Analyzed>>,
    pub column_pos: usize,
}

impl<'q> BoundVar<'q> {
    pub fn attribute<'ast>(&'ast self) -> Option<AttributeView<'q, 'ast>> {
        self.source.attribute(self.column_pos)
    }

    pub fn from_parts(source: Rc<TableFactor<'q, Analyzed>>, column_pos: usize) -> Self {
        Self { source, column_pos }
    }

    pub fn data_type(&self) -> DerivedType {
        self.source
            .attribute(self.column_pos)
            .map_or(DerivedType::unknown(), |attr| attr.data_type())
    }

    pub fn key(&self) -> (BoundTblKey, usize) {
        (self.src_key(), self.column_pos)
    }

    pub fn src_key(&self) -> BoundTblKey {
        Rc::as_ptr(&self.source) as *const () as BoundTblKey
    }
}

impl NamedEntity for BoundVar<'_> {
    fn name(&self) -> Option<&str> {
        self.source
            .attribute(self.column_pos)
            .and_then(|attr| match attr {
                AttributeView::Column(col, rename) => rename.or(Some(col.name.as_str())),
                AttributeView::Expr(_, name) => name,
            })
    }
}

impl PartialEq for BoundVar<'_> {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.source, &other.source) && self.column_pos == other.column_pos
    }
}

impl Eq for BoundVar<'_> {}

impl Hash for BoundVar<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.src_key().hash(state);
        self.column_pos.hash(state);
    }
}

impl Display for BoundVar<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self.name() {
            Some(column_name) => {
                if let Some(source_name) = self.source.name() {
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
pub struct BoundJoinUsingVar<'q> {
    left: BoundVar<'q>,
    joined: BoundVar<'q>,
    data_type: DerivedType,
}

impl<'q> BoundJoinUsingVar<'q> {
    pub fn new(left: BoundVar<'q>, joined: BoundVar<'q>, data_type: DerivedType) -> Self {
        Self {
            left,
            joined,
            data_type,
        }
    }

    /// Position, within the joined table.
    pub fn joined_tbl_column_pos(&self) -> usize {
        self.joined.column_pos
    }

    pub fn parts_ref(&self) -> (&BoundVar<'q>, &BoundVar<'q>) {
        (&self.left, &self.joined)
    }

    /// The merged output column's type: the common type of the two inputs.
    pub fn data_type(&self) -> DerivedType {
        self.data_type
    }
}

impl Display for BoundJoinUsingVar<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write_sql_ident(f, self.left.name().unwrap_or(NONAME_COLUMN))
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

impl AttributeView<'_, '_> {
    pub fn data_type(&self) -> DerivedType {
        match self {
            Self::Expr(proj_expr, _) => proj_expr.data_type(),
            Self::Column(col, _) => col.r#type,
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
    use std::iter::zip;

    use super::*;
    use crate::structural_eq::StructuralEq;

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for TableExpression<'q, S> {
        /// FROM goes first. It is what pairs this level's relations into the
        /// scope, and the other clauses read them.
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            self.from.eq_with(&other.from, scope)
                && self.selection.eq_with(&other.selection, scope)
                && self.group_by.eq_with(&other.group_by, scope)
                && self.having.eq_with(&other.having, scope)
                && self.windows.eq_with(&other.windows, scope)
        }
    }

    impl<'q, S: AstState<'q>> StructuralEq<S::EqScope> for From<'q, S> {
        /// Every entry is paired before any of them is compared: a join condition reads
        /// both sides of its join, so a half-built pairing would fail to match it.
        fn eq_with(&self, other: &Self, scope: &mut S::EqScope) -> bool {
            if self.tbl_factors.len() != other.tbl_factors.len() {
                return false;
            }
            for (entry, other_entry) in zip(&self.tbl_factors, &other.tbl_factors) {
                S::pair_relations(entry, other_entry, scope);
            }
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

    /// A resolved column reference: the same ordinal of the same relation - or
    /// of the two relations the scope pairs - regardless of the name it was
    /// written with.
    impl StructuralEq<RelationPairs> for BoundVar<'_> {
        fn eq_with(&self, other: &Self, scope: &mut RelationPairs) -> bool {
            if self.column_pos != other.column_pos {
                return false;
            }
            match scope
                .iter()
                .find(|(left, right)| *left == self.src_key() || *right == other.src_key())
            {
                Some(&(left, right)) => left == self.src_key() && right == other.src_key(),
                None => self.src_key() == other.src_key(),
            }
        }
    }

    /// One `USING` column: both resolved halves must match. The merged type is
    /// derived from them, so it carries no structural information of its own.
    impl StructuralEq<RelationPairs> for BoundJoinUsingVar<'_> {
        fn eq_with(&self, other: &Self, scope: &mut RelationPairs) -> bool {
            self.left.eq_with(&other.left, scope) && self.joined.eq_with(&other.joined, scope)
        }
    }

    impl StructuralEq<RelationPairs> for AnalyzedCteOrTable<'_> {
        fn eq_with(&self, other: &Self, _scope: &mut RelationPairs) -> bool {
            match (self, other) {
                (Self::Cte(x), Self::Cte(y)) => Rc::ptr_eq(x, y),
                (Self::Table(x), Self::Table(y)) => x.name == y.name,
                _mismatched_shapes => false,
            }
        }
    }
}
