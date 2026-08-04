//! Query expressions: the outermost layer of DQL statement.
//!
//! # `MultisetStmt<'q, State: AstState<'q>>`
//! What SQL calls a query expression.
//! The largest form that still denotes a multiset of rows.
//! Every subquery, CTE body and top-level statement reduces to this node.
//!
//!
//! # Tree structure
//! A statement wraps its body together with the clauses that apply to the whole
//! result.
//! * [`Ctes`] is the `WITH` prelude,
//! * [`MultisetInner`] is the body,
//! * [`OrderBy`] and [`Limit`] are the trailing clauses.
//!
//! `ORDER BY` and `LIMIT` sit here rather than on the body because they bind to
//! the result of a set operation, not to either of its operands.
//!
//! The body is one of these nodes
//! * [`SelectStmt`] is a single `SELECT`,
//! * [`ValuesStmt`] is a `VALUES` table constructor,
//! * [`Operation`] combines two statements with `UNION`/`EXCEPT`/`INTERSECT`.
//!
//! [`OrderByElement`] lives here because sort items also appear inside window
//! specifications, which is where [`window`](super::window) borrows it from.
//!
//! ```text
//! MultisetStmt
//! ├── ctes: Ctes
//! │   └── ctes: Vec<Cte>
//! │       ├── name: &str
//! │       ├── columns: Vec<&str>
//! │       └── body: MultisetStmt
//! ├── inner: MultisetInner  (one of)
//! │   ├── Select(SelectStmt)
//! │   │   ├── select_list: SelectList
//! │   │   └── table_expression: Option<TableExpression>
//! │   ├── Values(ValuesStmt)
//! │   │   └── rows: Vec<ValuesRow>
//! │   └── Operation(Operation)
//! │       ├── left: MultisetStmt
//! │       ├── op: OperationKind  (UNION | EXCEPT | INTERSECT)
//! │       ├── dup_elimination: OpDupElimination  (ALL | DISTINCT)
//! │       └── right: MultisetStmt
//! ├── order_by: Option<OrderBy>
//! │   └── elems: Vec<OrderByElement>
//! │       ├── expr: Expr
//! │       ├── direction: OrderByDirection  (ASC | DESC)
//! │       └── nulls: OrderByNulls  (default | NULLS FIRST | NULLS LAST)
//! └── limit: Option<Limit>  (LIMIT ALL | LIMIT <n>)
//! ```
//!
//! Every node here belongs to this module except [`SelectStmt`], which is
//! expanded no further. The three places [`MultisetStmt`] appears inside itself
//! — a CTE body and both operands of a set operation — are what makes the tree
//! recursive.
//!
//!
//! # Parsing
//! `UNION`/`EXCEPT` and `INTERSECT` sit on two precedence tiers. The body is
//! therefore built by a small Pratt parser rather than by grammar nesting — the
//! same technique [`expr`](super::expr) uses for operators.

use std::fmt::{Display, Error, Formatter};

use smol_str::format_smolstr;

use crate::error::{ast_arbitrary_err, AstResult};
use crate::expr::{Expr, ValuesRow};
use crate::select::SelectStmt;
use crate::table_expression::{AttributeView, SingleEntryColumnRoute};
use crate::{Analyzed, AstState, Ident, NamedEntity, Raw};
use sql_ir::ir::types::DerivedType;

pub struct MultisetStmt<'q, State: AstState<'q>> {
    pub ctes: Ctes<'q, State>,
    pub inner: MultisetInner<'q, State>,
    pub order_by: Option<OrderBy<'q, State>>,
    pub limit: Option<Limit>,
}

impl<'q, State: AstState<'q>> MultisetStmt<'q, State> {
    pub fn into_parts(
        self,
    ) -> (
        Ctes<'q, State>,
        MultisetInner<'q, State>,
        Option<OrderBy<'q, State>>,
        Option<Limit>,
    ) {
        (self.ctes, self.inner, self.order_by, self.limit)
    }

    pub fn from_parts(
        ctes: Ctes<'q, State>,
        inner: MultisetInner<'q, State>,
        order_by: Option<OrderBy<'q, State>>,
        limit: Option<Limit>,
    ) -> Self {
        Self {
            ctes,
            inner,
            order_by,
            limit,
        }
    }
}

impl<'q> MultisetStmt<'q, Raw> {
    pub(crate) fn new(inner: MultisetInner<'q, Raw>) -> Self {
        Self {
            ctes: Ctes::<'q, Raw>::default(),
            inner,
            order_by: None,
            limit: None,
        }
    }

    pub fn new_select(select: SelectStmt<'q, Raw>) -> Self {
        Self::new(MultisetInner::Select(select))
    }

    pub fn new_values(values: ValuesStmt<'q, Raw>) -> Self {
        Self::new(MultisetInner::Values(values))
    }

    pub fn new_operation(
        left: Self,
        op: OperationKind,
        dup_elimination: OpDupElimination,
        right: Self,
    ) -> Self {
        Self::new(MultisetInner::Operation(Operation {
            left: Box::new(left),
            op,
            dup_elimination,
            right: Box::new(right),
        }))
    }
}

impl<'q> MultisetStmt<'q, Analyzed> {
    /// Output column types of the statement body — what this statement
    /// exposes when used as a scalar subquery or a CTE/FROM-subquery source.
    pub fn result_types(&self) -> AstResult<Vec<DerivedType>> {
        match &self.inner {
            MultisetInner::Select(stmt) => Ok(stmt.result_types()),
            MultisetInner::Values(_) => Err(ast_arbitrary_err(format_smolstr!(
                "result types of VALUES are not supported yet"
            ))),
            MultisetInner::Operation(_) => Err(ast_arbitrary_err(format_smolstr!(
                "result types of set operations are not supported yet"
            ))),
        }
    }

    pub fn result_columns_cnt(&self) -> AstResult<usize> {
        match &self.inner {
            MultisetInner::Select(stmt) => Ok(stmt.result_columns_cnt()),
            MultisetInner::Values(_) => Err(ast_arbitrary_err(format_smolstr!(
                "result columns of VALUES are not supported yet"
            ))),
            MultisetInner::Operation(_) => Err(ast_arbitrary_err(format_smolstr!(
                "result columns of set operations are not supported yet"
            ))),
        }
    }

    /// Get offset (>= 0) in column list exposed by `MultisetStmt`.
    /// If no column with `column_name` found returns `None`.
    /// The `Values`/`Operation` arms below are not supported yet
    /// and return `None` instead of failing.
    pub(crate) fn column_route(
        &self,
        column_name: &str,
        exclude_positions: Option<Vec<usize>>,
    ) -> SingleEntryColumnRoute<usize> {
        match &self.inner {
            MultisetInner::Select(stmt) => stmt.column_route(column_name, exclude_positions),
            MultisetInner::Values(_) | MultisetInner::Operation(_) => {
                SingleEntryColumnRoute::ColumnMissing
            }
        }
    }

    pub fn attribute(&self, pos: usize) -> Option<AttributeView<'q, '_>> {
        match &self.inner {
            MultisetInner::Select(stmt) => stmt.attribute(pos),
            MultisetInner::Values(_) | MultisetInner::Operation(_) => None,
        }
    }
}

impl<'q, State: AstState<'q>> Display for MultisetStmt<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        if !self.ctes.is_empty() {
            write!(f, "{} ", self.ctes)?;
        }
        write!(f, "{}", self.inner)?;
        self.order_by
            .as_ref()
            .map_or(Ok(()), |order_by| write!(f, " {order_by}"))?;
        self.limit
            .as_ref()
            .map_or(Ok(()), |limit| write!(f, " {limit}"))?;
        Ok(())
    }
}

pub enum MultisetInner<'q, State: AstState<'q>> {
    Values(ValuesStmt<'q, State>),
    Select(SelectStmt<'q, State>),
    Operation(Operation<'q, State>),
}

impl<'q, State: AstState<'q>> Display for MultisetInner<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            MultisetInner::Values(values) => write!(f, "{values}"),
            MultisetInner::Select(select) => write!(f, "{select}"),
            MultisetInner::Operation(operation) => write!(f, "{operation}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationKind {
    Union,
    Except,
    Intersect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpDupElimination {
    All,
    Distinct,
}

pub struct Operation<'q, State: AstState<'q>> {
    pub left: Box<MultisetStmt<'q, State>>,
    pub op: OperationKind,
    pub dup_elimination: OpDupElimination,
    pub right: Box<MultisetStmt<'q, State>>,
}

impl<'q, State: AstState<'q>> Display for Operation<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "({}) {}", self.left, self.op)?;
        if matches!(self.dup_elimination, OpDupElimination::All) {
            write!(f, " ALL")?;
        }
        write!(f, " ({})", self.right)
    }
}

impl Display for OperationKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let op = match self {
            OperationKind::Union => "UNION",
            OperationKind::Except => "EXCEPT",
            OperationKind::Intersect => "INTERSECT",
        };
        write!(f, "{op}")
    }
}

#[derive(Default)]
pub struct OrderBy<'q, State: AstState<'q>> {
    elems: Vec<OrderByElement<'q, State>>,
}

impl<'q, State: AstState<'q>> Display for OrderBy<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        // Parsing guarantees at least one ORDER BY element.
        let Some((first_elem, other_elems)) = self.elems.split_first() else {
            return Ok(());
        };
        write!(f, "ORDER BY {first_elem}")?;
        for elem in other_elems {
            write!(f, ", {elem}")?;
        }
        Ok(())
    }
}

impl<'q> OrderBy<'q, Raw> {
    pub fn add_elem(&mut self, elem: OrderByElement<'q, Raw>) {
        self.elems.push(elem);
    }

    pub fn is_empty(&self) -> bool {
        self.elems.is_empty()
    }
}

// Public for reuse in window ORDER BY
pub struct OrderByElement<'q, State: AstState<'q>> {
    pub expr: Expr<'q, State>,
    pub direction: OrderByDirection,
    pub nulls: OrderByNulls,
}

impl<'q, State: AstState<'q>> Display for OrderByElement<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{} {}", self.expr, self.direction)?;
        if !self.nulls.is_default() {
            write!(f, " {}", self.nulls)?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub enum OrderByDirection {
    #[default]
    Asc,
    Desc,
}

impl Display for OrderByDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            OrderByDirection::Asc => write!(f, "ASC"),
            OrderByDirection::Desc => write!(f, "DESC"),
        }
    }
}

#[derive(Default)]
pub enum OrderByNulls {
    #[default]
    Default,
    First,
    Last,
}

impl Display for OrderByNulls {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            OrderByNulls::Default => Ok(()),
            OrderByNulls::First => write!(f, "NULLS FIRST"),
            OrderByNulls::Last => write!(f, "NULLS LAST"),
        }
    }
}

impl OrderByNulls {
    pub(crate) fn is_default(&self) -> bool {
        matches!(self, OrderByNulls::Default)
    }
}

#[derive(Default)]
pub enum Limit {
    #[default]
    All,
    Value(usize),
}

impl Display for Limit {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Limit::All => write!(f, "LIMIT ALL"),
            Limit::Value(limit) => write!(f, "LIMIT {limit}"),
        }
    }
}

pub struct Ctes<'q, State: AstState<'q>> {
    ctes: Vec<State::CteT>,
}

impl<'q> Ctes<'q, Raw> {
    pub fn into(self) -> Vec<Cte<'q, Raw>> {
        self.ctes
    }
}

impl Ctes<'_, Analyzed> {
    pub fn exists(&self, name: &str) -> bool {
        self.ctes.iter().any(|cte| cte.name.as_str() == name)
    }
}

impl<'q, State: AstState<'q>> Ctes<'q, State> {
    pub fn add(&mut self, cte: State::CteT) {
        self.ctes.push(cte);
    }

    pub fn ctes_ref(&self) -> &Vec<State::CteT> {
        &self.ctes
    }

    pub fn from(ctes: Vec<State::CteT>) -> Self {
        Self { ctes }
    }

    pub fn is_empty(&self) -> bool {
        self.ctes.is_empty()
    }
}

impl<'q, State: AstState<'q>> Default for Ctes<'q, State> {
    fn default() -> Self {
        Self { ctes: vec![] }
    }
}

impl<'q, State: AstState<'q>> Display for Ctes<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let Some((first, ctes)) = self.ctes.split_first() else {
            return Ok(());
        };
        write!(f, "WITH {first}")?;
        for cte in ctes {
            write!(f, ", {cte}")?;
        }
        Ok(())
    }
}

pub struct Cte<'q, State: AstState<'q>> {
    pub name: Ident,
    columns: Vec<Ident>,
    body: MultisetStmt<'q, State>,
}

impl<'q> Cte<'q, Raw> {
    pub fn into_parts(self) -> (Ident, Vec<Ident>, MultisetStmt<'q, Raw>) {
        (self.name, self.columns, self.body)
    }
}

impl<'q> Cte<'q, Analyzed> {
    pub fn from_parts(name: Ident, columns: Vec<Ident>, body: MultisetStmt<'q, Analyzed>) -> Self {
        Self {
            name,
            columns,
            body,
        }
    }

    pub(crate) fn columns_ref(&self) -> &Vec<Ident> {
        &self.columns
    }

    pub fn body_ref(&self) -> &MultisetStmt<'q, Analyzed> {
        &self.body
    }

    /// The CTE's `column_pos`-th result attribute. An explicit column list
    /// supplies the visible name, overriding the body's output name.
    pub fn attribute(&self, column_pos: usize) -> Option<AttributeView<'q, '_>> {
        self.body.attribute(column_pos).map(|attr| {
            if self.columns.is_empty() {
                return attr;
            }
            if let AttributeView::Expr(proj_expr, _) = attr {
                let name = self.columns.get(column_pos).map(Ident::as_str);
                AttributeView::Expr(proj_expr, name)
            } else {
                attr
            }
        })
    }
}

impl<'q, State: AstState<'q>> NamedEntity for Cte<'q, State> {
    fn name(&self) -> Option<&str> {
        Some(self.name.as_str())
    }
}

impl<'q, State: AstState<'q>> Display for Cte<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.name)?;
        if let Some((first, columns)) = self.columns.split_first() {
            write!(f, " ({first}")?;
            for col in columns {
                write!(f, ", {col}")?;
            }
            write!(f, ")")?;
        }
        write!(f, " AS ({})", self.body)
    }
}

impl<'q> Cte<'q, Raw> {
    pub fn new(name: Ident, columns: Vec<Ident>, body: MultisetStmt<'q, Raw>) -> Self {
        Self {
            name,
            columns,
            body,
        }
    }
}

#[derive(Default)]
pub struct ValuesStmt<'q, State: AstState<'q>> {
    // This is [`Vec<Vec<Expr>>`] and this is bad (but how bad?), some day improve it
    rows: Vec<ValuesRow<'q, State>>,
}

impl<'q, State: AstState<'q>> Display for ValuesStmt<'q, State> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "VALUES ")?;
        // Parsing guarantees at least one row.
        let Some((first_values_row, other_values_rows)) = self.rows.split_first() else {
            return Ok(());
        };
        write!(f, "{first_values_row}")?;
        for values_row in other_values_rows {
            write!(f, ", {values_row}")?;
        }
        Ok(())
    }
}

impl<'q> ValuesStmt<'q, Raw> {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn add_row(&mut self, row: ValuesRow<'q, Raw>) {
        self.rows.push(row)
    }
}
