//! Column metadata of relational nodes.
//!
//! Every relational node produces an ordered list of columns. For `Projection`,
//! `SelectWithoutScan` and `Motion` that list is explicit (the output `Row` of
//! aliases; a motion keeps one because its child may be hidden or stubbed once
//! the motion is materialized); for every other node it is a function of the
//! node's children (or of the table for scans and inserts). This module is the
//! single place that answers "what columns does node X produce", so nobody else
//! has to walk output rows.

use std::borrow::Cow;
use std::slice;

use smol_str::{format_smolstr, SmolStr};

use crate::errors::{Entity, SbroadError};
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    Alias, Delete, Except, GroupBy, Having, Insert, Intersect, Join, Limit, Motion, NodeId,
    OrderBy, Projection, Reference, ReferenceTarget, ScanCte, ScanRelation, ScanSubQuery,
    SelectWithoutScan, Selection, Union, UnionAll, Update, Values,
};
use crate::ir::relation::{Column, ColumnRole};
use crate::ir::types::{calculate_unified_types, DerivedType};
use crate::ir::Plan;

/// A column of a relational node: the node itself and the position of the
/// column in the node output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelColumn {
    /// Relational node the column belongs to.
    pub rel_id: NodeId,
    /// Position of the column in the node output.
    pub position: usize,
}

impl RelColumn {
    #[must_use]
    pub fn new(rel_id: NodeId, position: usize) -> Self {
        RelColumn { rel_id, position }
    }
}

/// Metadata of a single column produced by a relational node.
///
/// The name is borrowed from the plan the metadata was derived from: a table
/// column name lives in `Plan::relations`, an output column name in the
/// `Alias` node of the output tuple. Only the anonymous `VALUES` names are
/// generated and therefore owned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnMeta<'p> {
    /// Column name (alias in the output tuple).
    pub name: Cow<'p, str>,
    /// Column type.
    pub r#type: DerivedType,
    /// Whether the column is a system (sharding, `bucket_id`) column.
    pub is_system: bool,
}

impl<'p> ColumnMeta<'p> {
    fn from_table_column(column: &'p Column, flag_sharding: bool) -> Self {
        ColumnMeta {
            name: Cow::Borrowed(column.name.as_str()),
            r#type: column.r#type,
            is_system: flag_sharding && column.role == ColumnRole::Sharding,
        }
    }

    /// Column of a `VALUES` node: the names are generated according to the
    /// tarantool naming of anonymous columns.
    fn values_column(pos: usize, r#type: DerivedType) -> Self {
        ColumnMeta {
            name: Cow::Owned(format!("COLUMN_{}", pos + 1)),
            r#type,
            is_system: false,
        }
    }

    /// The column name as an owned string, for the callers that keep it
    /// beyond the borrow of the plan it was derived from.
    #[must_use]
    pub fn name_owned(&self) -> SmolStr {
        SmolStr::from(&*self.name)
    }
}

/// What defines the columns of a relational node once the nodes that pass
/// their child's columns through are looked through.
enum ColumnsSource<'p> {
    /// Columns of a table: a scan flags the sharding column as a system
    /// column, an insert doesn't.
    Table {
        columns: &'p [Column],
        is_sharding: bool,
    },
    /// Aliases of an explicit output row.
    Row(&'p [NodeId]),
    /// Columns of the left child followed by the columns of the right one.
    Join { left: NodeId, right: NodeId },
    /// Columns of the left child with the types unified with the right one.
    SetOp { left: NodeId, right: NodeId },
    /// Anonymous columns of the value rows.
    Values { rows: &'p [NodeId] },
    /// No columns at all (`DELETE` without `WHERE`).
    Empty,
}

/// Iterator over the columns produced by a relational node.
///
/// Column metadata is computed lazily (types may require walking an
/// expression tree), so every item is a `Result`. See [`Plan::columns_of`].
#[derive(Debug)]
pub enum Columns<'p> {
    Table {
        columns: slice::Iter<'p, Column>,
        is_sharding: bool,
    },
    Row {
        plan: &'p Plan,
        aliases: slice::Iter<'p, NodeId>,
    },
    Values {
        types: std::vec::IntoIter<DerivedType>,
        next_pos: usize,
    },
    Chain {
        left: Box<Columns<'p>>,
        right: Box<Columns<'p>>,
    },
    Unify {
        left: Box<Columns<'p>>,
        right: Box<Columns<'p>>,
    },
    Empty,
}

impl Columns<'_> {
    fn remaining(&self) -> usize {
        match self {
            Columns::Table { columns, .. } => columns.len(),
            Columns::Row { aliases, .. } => aliases.len(),
            Columns::Values { types, .. } => types.len(),
            Columns::Chain { left, right } => left.remaining() + right.remaining(),
            Columns::Unify { left, .. } => left.remaining(),
            Columns::Empty => 0,
        }
    }
}

impl<'p> Iterator for Columns<'p> {
    type Item = Result<ColumnMeta<'p>, SbroadError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Columns::Table {
                columns,
                is_sharding: flag_sharding,
            } => columns
                .next()
                .map(|column| Ok(ColumnMeta::from_table_column(column, *flag_sharding))),
            Columns::Row { plan, aliases } => {
                let alias_id = *aliases.next()?;
                Some(plan.column_meta_of_alias(alias_id))
            }
            Columns::Values { types, next_pos } => {
                let r#type = types.next()?;
                let pos = *next_pos;
                *next_pos += 1;
                Some(Ok(ColumnMeta::values_column(pos, r#type)))
            }
            Columns::Chain { left, right } => left.next().or_else(|| right.next()),
            Columns::Unify { left, right } => {
                let left = left.next()?;
                Some(unify_columns(left, right.next()))
            }
            Columns::Empty => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining();
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for Columns<'_> {}

/// Column of a set operation: the name comes from the left child, the type
/// is unified between the children.
fn unify_columns<'p>(
    left: Result<ColumnMeta<'p>, SbroadError>,
    right: Option<Result<ColumnMeta<'p>, SbroadError>>,
) -> Result<ColumnMeta<'p>, SbroadError> {
    let left = left?;
    let Some(right) = right else {
        return Err(SbroadError::Invalid(
            Entity::Relational,
            Some("set operation children have a different number of columns".into()),
        ));
    };
    let right = right?;
    Ok(ColumnMeta {
        name: left.name,
        r#type: unify_types(left.r#type, right.r#type)?,
        is_system: left.is_system,
    })
}

fn unify_types(left: DerivedType, right: DerivedType) -> Result<DerivedType, SbroadError> {
    let unified = calculate_unified_types([left, right].into_iter().map(std::iter::once))?;
    Ok(unified
        .first()
        .expect("a single column is unified at a time")
        .1)
}

impl Plan {
    /// Looks through the nodes that pass their child's columns through and
    /// finds what defines the columns of `rel_id`.
    fn columns_source(&self, mut rel_id: NodeId) -> Result<ColumnsSource<'_>, SbroadError> {
        loop {
            let source = match self.get_relation_node(rel_id)? {
                Relational::ScanRelation(ScanRelation { relation, .. }) => ColumnsSource::Table {
                    columns: &self.get_relation_or_error(relation)?.columns,
                    is_sharding: true,
                },
                Relational::Insert(Insert { relation, .. }) => ColumnsSource::Table {
                    columns: &self.get_relation_or_error(relation)?.columns,
                    is_sharding: false,
                },
                Relational::Projection(Projection { output, .. })
                | Relational::SelectWithoutScan(SelectWithoutScan { output, .. })
                | Relational::Motion(Motion { output, .. }) => {
                    ColumnsSource::Row(self.get_row_list(*output)?)
                }
                Relational::Selection(Selection { child, .. })
                | Relational::Having(Having { child, .. })
                | Relational::GroupBy(GroupBy { child, .. })
                | Relational::OrderBy(OrderBy { child, .. })
                | Relational::Limit(Limit { child, .. })
                | Relational::ScanSubQuery(ScanSubQuery { child, .. })
                | Relational::ScanCte(ScanCte { child, .. })
                | Relational::Update(Update { child, .. })
                | Relational::Delete(Delete {
                    child: Some(child), ..
                })
                | Relational::Intersect(Intersect { right: child, .. }) => {
                    rel_id = *child;
                    continue;
                }
                Relational::Delete(Delete { child: None, .. }) => ColumnsSource::Empty,
                Relational::Join(Join { left, right, .. }) => ColumnsSource::Join {
                    left: *left,
                    right: *right,
                },
                Relational::Union(Union { left, right, .. })
                | Relational::UnionAll(UnionAll { left, right, .. })
                | Relational::Except(Except { left, right, .. }) => ColumnsSource::SetOp {
                    left: *left,
                    right: *right,
                },
                Relational::Values(Values { rows, .. }) => ColumnsSource::Values { rows },
            };
            return Ok(source);
        }
    }

    /// Ordered columns produced by the relational node `rel_id`.
    ///
    /// # Errors
    /// - `rel_id` is not a relational node
    pub fn columns_of(&self, rel_id: NodeId) -> Result<Columns<'_>, SbroadError> {
        Ok(match self.columns_source(rel_id)? {
            ColumnsSource::Table {
                columns,
                is_sharding: flag_sharding,
            } => Columns::Table {
                columns: columns.iter(),
                is_sharding: flag_sharding,
            },
            ColumnsSource::Row(aliases) => Columns::Row {
                plan: self,
                aliases: aliases.iter(),
            },
            ColumnsSource::Join { left, right } => Columns::Chain {
                left: Box::new(self.columns_of(left)?),
                right: Box::new(self.columns_of(right)?),
            },
            ColumnsSource::SetOp { left, right } => Columns::Unify {
                left: Box::new(self.columns_of(left)?),
                right: Box::new(self.columns_of(right)?),
            },
            ColumnsSource::Values { rows } => Columns::Values {
                types: self.values_column_types(rows)?.into_iter(),
                next_pos: 0,
            },
            ColumnsSource::Empty => Columns::Empty,
        })
    }

    /// Number of columns produced by the relational node `rel_id`.
    ///
    /// # Errors
    /// - see [`Plan::columns_of`]
    pub fn columns_len(&self, rel_id: NodeId) -> Result<usize, SbroadError> {
        let memo = self.nodes.columns_len_memo();
        if let Some(len) = memo.get(rel_id) {
            return Ok(len);
        }
        let len = self.derive_columns_len(rel_id)?;
        memo.insert(rel_id, len);
        Ok(len)
    }

    /// [`Plan::columns_len`] without consulting the memo.
    fn derive_columns_len(&self, rel_id: NodeId) -> Result<usize, SbroadError> {
        Ok(match self.columns_source(rel_id)? {
            ColumnsSource::Table { columns, .. } => columns.len(),
            ColumnsSource::Row(aliases) => aliases.len(),
            ColumnsSource::Join { left, right } => {
                self.columns_len(left)? + self.columns_len(right)?
            }
            ColumnsSource::SetOp { left, .. } => self.columns_len(left)?,
            ColumnsSource::Values { rows } => match rows.first() {
                Some(row_id) => self.get_row_list(*row_id)?.len(),
                None => 0,
            },
            ColumnsSource::Empty => 0,
        })
    }

    /// Metadata of the column `rel_col`.
    ///
    /// # Errors
    /// - see [`Plan::columns_of`]
    /// - the column position is out of range
    pub fn column_at(&self, rel_col: RelColumn) -> Result<ColumnMeta<'_>, SbroadError> {
        let RelColumn { rel_id, position } = rel_col;
        match self.columns_source(rel_id)? {
            ColumnsSource::Table {
                columns,
                is_sharding: flag_sharding,
            } => columns
                .get(position)
                .map(|column| ColumnMeta::from_table_column(column, flag_sharding))
                .ok_or_else(|| column_not_found(rel_col, columns.len())),
            ColumnsSource::Row(aliases) => {
                let alias_id = *aliases
                    .get(position)
                    .ok_or_else(|| column_not_found(rel_col, aliases.len()))?;
                self.column_meta_of_alias(alias_id)
            }
            ColumnsSource::Join { left, right } => {
                let left_len = self.columns_len(left)?;
                if position < left_len {
                    return self.column_at(RelColumn::new(left, position));
                }
                let right_len = self.columns_len(right)?;
                if position - left_len >= right_len {
                    return Err(column_not_found(rel_col, left_len + right_len));
                }
                self.column_at(RelColumn::new(right, position - left_len))
            }
            ColumnsSource::SetOp { left, right } => unify_columns(
                self.column_at(RelColumn::new(left, position)),
                Some(self.column_at(RelColumn::new(right, position))),
            ),
            ColumnsSource::Values { rows } => {
                let r#type = self.values_column_type(rel_col, rows)?;
                Ok(ColumnMeta::values_column(position, r#type))
            }
            ColumnsSource::Empty => Err(column_not_found(rel_col, 0)),
        }
    }

    /// Where the column `rel_col` comes from: the child relational node and
    /// the column position in that child the column is a plain copy of. For
    /// set operations that is the left child.
    ///
    /// Returns `None` when the column is not a copy of a child's column:
    /// column sources (table scans, `INSERT`, `VALUES`) and projection
    /// columns that are not a bare reference.
    ///
    /// # Errors
    /// - see [`Plan::column_at`]
    pub fn column_source(&self, rel_col: RelColumn) -> Result<Option<RelColumn>, SbroadError> {
        let RelColumn { rel_id, position } = rel_col;
        let checked = |source: RelColumn| -> Result<RelColumn, SbroadError> {
            let len = self.columns_len(source.rel_id)?;
            if source.position >= len {
                return Err(column_not_found(source, len));
            }
            Ok(source)
        };
        let source = match self.get_relation_node(rel_id)? {
            Relational::ScanRelation(_)
            | Relational::Insert(_)
            | Relational::Values(_)
            | Relational::Delete(Delete { child: None, .. }) => {
                checked(rel_col)?;
                None
            }
            Relational::Projection(Projection { output, .. })
            | Relational::SelectWithoutScan(SelectWithoutScan { output, .. })
            | Relational::Motion(Motion { output, .. }) => {
                let aliases = self.get_row_list(*output)?;
                let alias_id = *aliases
                    .get(position)
                    .ok_or_else(|| column_not_found(rel_col, aliases.len()))?;
                let expr_id = self.get_child_under_alias(alias_id)?;
                match self.get_expression_node(expr_id)? {
                    Expression::Reference(Reference {
                        target: ReferenceTarget::Single(child_id),
                        position,
                        ..
                    }) => Some(RelColumn::new(*child_id, *position)),
                    _ => None,
                }
            }
            Relational::Selection(Selection { child, .. })
            | Relational::Having(Having { child, .. })
            | Relational::GroupBy(GroupBy { child, .. })
            | Relational::OrderBy(OrderBy { child, .. })
            | Relational::Limit(Limit { child, .. })
            | Relational::ScanSubQuery(ScanSubQuery { child, .. })
            | Relational::ScanCte(ScanCte { child, .. })
            | Relational::Update(Update { child, .. })
            | Relational::Delete(Delete {
                child: Some(child), ..
            })
            | Relational::Intersect(Intersect { right: child, .. }) => {
                Some(checked(RelColumn::new(*child, position))?)
            }
            Relational::Join(Join { left, right, .. }) => {
                let left_len = self.columns_len(*left)?;
                if position < left_len {
                    Some(RelColumn::new(*left, position))
                } else {
                    Some(checked(RelColumn::new(*right, position - left_len))?)
                }
            }
            Relational::Union(Union { left, .. })
            | Relational::UnionAll(UnionAll { left, .. })
            | Relational::Except(Except { left, .. }) => {
                Some(checked(RelColumn::new(*left, position))?)
            }
        };
        Ok(source)
    }

    fn column_meta_of_alias(&self, alias_id: NodeId) -> Result<ColumnMeta<'_>, SbroadError> {
        let alias = self.get_expression_node(alias_id)?;
        let Expression::Alias(Alias { name, child }) = alias else {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format_smolstr!(
                    "expected an alias in the output tuple, got {alias:?}"
                )),
            ));
        };
        let is_system = matches!(
            self.get_expression_node(*child)?,
            Expression::Reference(Reference {
                is_system: true,
                ..
            })
        );
        Ok(ColumnMeta {
            name: Cow::Borrowed(name.as_str()),
            r#type: alias.calculate_type(self)?,
            is_system,
        })
    }

    /// Types of the `VALUES` columns: unified over all the rows.
    fn values_column_types(&self, rows: &[NodeId]) -> Result<Vec<DerivedType>, SbroadError> {
        let mut rows_types = Vec::with_capacity(rows.len());
        for row_id in rows {
            let row_types = self
                .get_row_list(*row_id)?
                .iter()
                .map(|expr_id| self.get_expression_node(*expr_id)?.calculate_type(self))
                .collect::<Result<Vec<DerivedType>, SbroadError>>()?;
            rows_types.push(row_types.into_iter());
        }
        if rows_types.is_empty() {
            return Ok(Vec::new());
        }
        Ok(calculate_unified_types(rows_types.into_iter())?
            .into_iter()
            .map(|(_, r#type)| r#type)
            .collect())
    }

    /// Type of the `VALUES` column `rel_col`: unified over all the rows.
    fn values_column_type(
        &self,
        rel_col: RelColumn,
        rows: &[NodeId],
    ) -> Result<DerivedType, SbroadError> {
        let mut types = Vec::with_capacity(rows.len());
        for row_id in rows {
            let list = self.get_row_list(*row_id)?;
            let expr_id = *list
                .get(rel_col.position)
                .ok_or_else(|| column_not_found(rel_col, list.len()))?;
            let r#type = self.get_expression_node(expr_id)?.calculate_type(self)?;
            types.push(std::iter::once(r#type));
        }
        if types.is_empty() {
            return Err(column_not_found(rel_col, 0));
        }
        Ok(calculate_unified_types(types.into_iter())?
            .first()
            .expect("a single column is unified at a time")
            .1)
    }
}

fn column_not_found(rel_col: RelColumn, len: usize) -> SbroadError {
    SbroadError::NotFound(
        Entity::Column,
        format_smolstr!(
            "at position {} among the {len} columns of relational node {}",
            rel_col.position,
            rel_col.rel_id
        ),
    )
}
