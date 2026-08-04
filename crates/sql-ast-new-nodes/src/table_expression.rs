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

use std::fmt::{Display, Error, Formatter};
use std::rc::Rc;

use crate::error::{ast_invariant_err, AstResult};
use crate::expr::{Expr, RawColumnRef};
use crate::multiset::{Cte, MultisetStmt};
use crate::select::ProjectionExpr;
use crate::window::NamedWindow;
use crate::{write_sql_ident, Analyzed, AstState, Ident, NamedEntity, Raw, NONAME_COLUMN};
use smol_str::format_smolstr;
use sql_ir::ir::relation::{Column, ColumnRole, Table};
use sql_ir::ir::types::DerivedType;

pub struct TableExpression<'q, State: AstState<'q>> {
    from: From<'q, State>,
    selection: Option<Expr<'q, State>>,
    group_by: Vec<Expr<'q, State>>,
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
        Vec<Expr<'q, State>>,
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
        group_by: Vec<Expr<'q, State>>,
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

impl<'q, State: AstState<'q>> Display for TableExpression<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "FROM {}", self.from)?;
        self.selection
            .as_ref()
            .map_or(Ok(()), |selection| write!(f, " WHERE {selection}"))?;

        if let Some((first_gb_expr, other_gb_exprs)) = self.group_by.split_first() {
            write!(f, " GROUP BY {first_gb_expr}")?;
            for expr in other_gb_exprs {
                write!(f, ", {expr}")?;
            }
        }

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
    pub fn is_empty(&self) -> bool {
        self.tbl_factors.is_empty()
    }

    pub fn have_joined_tbls(&self) -> bool {
        self.tbl_factors.len() > 1
    }

    pub fn add_entry(&mut self, entry: FromEntry<'q, Analyzed>) {
        self.tbl_factors.push(entry);
    }

    pub fn entries(&self) -> &[FromEntry<'q, Analyzed>] {
        &self.tbl_factors
    }

    pub fn pop_entry(&mut self) -> AstResult<FromEntry<'q, Analyzed>> {
        self.tbl_factors
            .pop()
            .ok_or(ast_invariant_err(format_smolstr!(
                "expected to find entry in FROM"
            )))
    }

    /// Route a raw reference to `(relation position, column position)` within this FROM.
    /// Positions rather than pointers: the analyzed FROM sits behind an `Rc`
    /// shared by every reference into it, so a route stays valid without self-referential borrows.
    /// Joined tables are not routed yet, hence the hardcoded relation `0`.
    pub fn column_route(
        &self,
        col_ref: &RawColumnRef,
    ) -> FromColumnRoute<(Rc<TableFactor<'q, Analyzed>>, usize)> {
        let tbl_qualifier = col_ref.table_name();

        tbl_qualifier.map_or_else(
            || {
                let column_routes = self
                    .entries()
                    .iter()
                    .map(|entry| {
                        entry
                            .column_route(col_ref)
                            .map(|col_pos| (Rc::clone(entry.tbl_factor()), col_pos))
                    })
                    .filter(|route| !matches!(route, FromColumnRoute::ColumnMissing))
                    .collect::<Vec<_>>();

                if column_routes.len() > 1 {
                    // Resolved into more than 1 entity
                    FromColumnRoute::Ambigious
                } else if column_routes
                    .iter()
                    .any(|route| matches!(route, FromColumnRoute::Ambigious))
                {
                    // Any FROM entry has ambigious resolution
                    // For example, query `SELECT a FROM (SELECT 1 AS a, 2 AS a)` has ambigious `a` column.
                    FromColumnRoute::Ambigious
                } else {
                    column_routes
                        .into_iter()
                        .find(|route| matches!(route, FromColumnRoute::Resolved(_)))
                        .unwrap_or(FromColumnRoute::NoMatch)
                }
            },
            |t_name| {
                self.entries()
                    .iter()
                    .find(|entry| entry.name().is_some_and(|name| name == t_name))
                    .map_or(
                        FromColumnRoute::NoMatch,
                        // This path should not return `FromColumnRoute::NoMatch`.
                        |entry| {
                            entry
                                .column_route(col_ref)
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
    pub(crate) fn column_route(&self, col_ref: &RawColumnRef) -> SingleEntryColumnRoute<usize> {
        match self {
            Self::TableFactor(tbl_factor) => tbl_factor.column_route(col_ref.column_name(), None),
            Self::JoinedTable(joined_tbl) => {
                let exclude_positions = match col_ref.table_name() {
                    None => Some(
                        joined_tbl
                            .using_cols
                            .iter()
                            .map(|resolved_using_col| resolved_using_col.joined_tbl_column_pos())
                            .collect::<Vec<_>>(),
                    ),
                    Some(_) => None,
                };
                joined_tbl
                    .table
                    .column_route(col_ref.column_name(), exclude_positions)
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
        exclude_positions: Option<Vec<usize>>,
    ) -> SingleEntryColumnRoute<usize> {
        self.inner.column_route(column_name, exclude_positions)
    }

    pub fn attribute(&'_ self, column_pos: usize) -> Option<AttributeView<'q, '_>> {
        self.inner.attribute(column_pos)
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
        exclude_positions: Option<Vec<usize>>,
    ) -> SingleEntryColumnRoute<usize> {
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
        using_cols: Vec<ResolvedJoinUsingColumn<'q>>,
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

pub enum JoinKind {
    Inner,
    Left,
    Cross,
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
        exclude_positions: Option<Vec<usize>>,
    ) -> SingleEntryColumnRoute<usize> {
        let lookup_cte = |cte: &Cte<'q, Analyzed>, exclude_positions: Option<Vec<usize>>| {
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
                    SingleEntryColumnRoute::Ambigious
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
                        .map_or(SingleEntryColumnRoute::ColumnMissing, |(pos, _)| {
                            SingleEntryColumnRoute::Resolved(pos)
                        })
                }
            }
        };

        let lookup_table = |table: &Table, exclude_positions: Option<Vec<usize>>| {
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
                SingleEntryColumnRoute::ColumnMissing,
                SingleEntryColumnRoute::Resolved,
            ),
        }
    }

    fn attribute(&'_ self, column_pos: usize) -> Option<AttributeView<'q, '_>> {
        match self {
            Self::Cte(cte) => cte.attribute(column_pos),
            Self::Table(tbl) => tbl.columns.get(column_pos).map(AttributeView::Column),
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

/// A resolved column reference: the `Rc`-shared analyzed FROM it points into
/// plus (relation, column) positions.
pub struct ResolvedColumnRef<'q> {
    pub source: Rc<TableFactor<'q, Analyzed>>,
    pub column_pos: usize,
}

impl<'q> ResolvedColumnRef<'q> {
    pub fn from_parts(source: Rc<TableFactor<'q, Analyzed>>, column_pos: usize) -> Self {
        Self { source, column_pos }
    }

    pub fn data_type(&self) -> DerivedType {
        self.source
            .attribute(self.column_pos)
            .map_or(DerivedType::unknown(), |attr| attr.data_type())
    }

    pub(crate) fn column_name(&self) -> Option<&str> {
        self.source
            .attribute(self.column_pos)
            .and_then(|attr| match attr {
                AttributeView::Column(col) => Some(col.name.as_str()),
                AttributeView::Expr(_, name) => name,
            })
    }
}

impl Display for ResolvedColumnRef<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        if let Some(source_name) = self.source.name() {
            write_sql_ident(f, source_name)?;
            write!(f, ".")?;
        }
        // The anonymous-column placeholder is a sentinel, not an identifier —
        // quoting it would only disguise it as one.
        match self.column_name() {
            Some(column_name) => write_sql_ident(f, column_name),
            None => write!(f, "{NONAME_COLUMN}"),
        }
    }
}

pub struct JoinUsingColumn(pub RawColumnRef);

impl Display for JoinUsingColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.0)
    }
}

impl NamedEntity for JoinUsingColumn {
    fn name(&self) -> Option<&str> {
        Some(self.0.column_name())
    }
}

#[allow(unused)]
pub struct ResolvedJoinUsingColumn<'q>(ResolvedColumnRef<'q>, ResolvedColumnRef<'q>);

impl<'q> ResolvedJoinUsingColumn<'q> {
    pub fn new(first: ResolvedColumnRef<'q>, second: ResolvedColumnRef<'q>) -> Self {
        Self(first, second)
    }

    pub fn joined_tbl_column_pos(&self) -> usize {
        self.1.column_pos
    }
}

impl Display for ResolvedJoinUsingColumn<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write_sql_ident(f, self.0.column_name().unwrap_or(NONAME_COLUMN))
    }
}

/// One output column of a FROM source, abstracting over its two shapes — a
/// projection expression (CTE and subquery bodies) or a real table column —
/// so resolution and asterisk expansion treat every source kind uniformly.
pub enum AttributeView<'q, 'ast> {
    Expr(&'ast ProjectionExpr<'q, Analyzed>, Option<&'ast str>),
    Column(&'ast Column),
}

impl AttributeView<'_, '_> {
    pub fn data_type(&self) -> DerivedType {
        match self {
            Self::Expr(proj_expr, _) => proj_expr.data_type(),
            Self::Column(col) => col.r#type,
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
pub enum FromColumnRoute<P> {
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
pub enum SingleEntryColumnRoute<P> {
    Resolved(P),
    ColumnMissing,
    Ambigious,
}

impl<P> SingleEntryColumnRoute<P> {
    pub(crate) fn map<U>(self, f: impl FnOnce(P) -> U) -> FromColumnRoute<U> {
        match self {
            Self::Resolved(route) => FromColumnRoute::Resolved(f(route)),
            Self::ColumnMissing => FromColumnRoute::ColumnMissing,
            Self::Ambigious => FromColumnRoute::Ambigious,
        }
    }
}
