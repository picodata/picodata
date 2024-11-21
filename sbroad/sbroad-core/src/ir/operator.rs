//! Tuple operators module.
//!
//! Contains operator nodes that transform the tuples in IR tree.

use crate::executor::engine::helpers::to_user;
use crate::frontend::sql::get_unnamed_column_alias;
use crate::ir::api::children::Children;
use crate::ir::expression::PlanExpr;
use crate::ir::node::{
    Alias, Delete, Except, GroupBy, Having, Insert, Intersect, Join, Motion, MutNode, NodeId,
    OrderBy, Projection, Reference, Row, ScanCte, ScanRelation, ScanSubQuery, Selection, Union,
    UnionAll, Update, Values, ValuesRow,
};
use crate::ir::Plan;
use ahash::RandomState;

use crate::collection;
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::borrow::BorrowMut;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

use crate::errors::{Action, Entity, SbroadError};

use super::expression::{ColumnPositionMap, ExpressionId};
use super::node::expression::{Expression, MutExpression};
use super::node::relational::{MutRelational, Relational};
use super::node::{ArenaType, Limit, Node, NodeAligned, SelectWithoutScan};
use super::transformation::redistribution::{MotionPolicy, Program};
use super::tree::traversal::{LevelNode, PostOrderWithFilter, EXPR_CAPACITY};
use crate::ir::distribution::{Distribution, Key, KeySet};
use crate::ir::helpers::RepeatableState;
use crate::ir::relation::{Column, ColumnRole};
use crate::ir::transformation::redistribution::{ColumnPosition, JoinChild};

/// Binary operator returning Bool expression.
#[derive(Serialize, Deserialize, PartialEq, Debug, Eq, Hash, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Bool {
    /// `&&`
    And,
    /// `=`
    Eq,
    /// `in`
    In,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `!=`
    NotEq,
    /// `||`
    Or,
    /// `between`
    Between,
}

impl Bool {
    /// Creates `Bool` from the operator string.
    ///
    /// # Errors
    /// Returns `SbroadError` when the operator is invalid.
    pub fn from(s: &str) -> Result<Self, SbroadError> {
        match s.to_lowercase().as_str() {
            "and" => Ok(Bool::And),
            "or" => Ok(Bool::Or),
            "=" => Ok(Bool::Eq),
            "in" => Ok(Bool::In),
            ">" => Ok(Bool::Gt),
            ">=" => Ok(Bool::GtEq),
            "<" => Ok(Bool::Lt),
            "<=" => Ok(Bool::LtEq),
            "!=" | "<>" => Ok(Bool::NotEq),
            _ => Err(SbroadError::Unsupported(Entity::Operator, None)),
        }
    }
}

impl Display for Bool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op = match &self {
            Bool::And => "and",
            Bool::Or => "or",
            Bool::Eq => "=",
            Bool::In => "in",
            Bool::Gt => ">",
            Bool::GtEq => ">=",
            Bool::Lt => "<",
            Bool::LtEq => "<=",
            Bool::NotEq => "<>",
            Bool::Between => unreachable!("Between in op fmt"),
        };

        write!(f, "{op}")
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Eq, Hash, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Arithmetic {
    /// `*`
    Multiply,
    /// `/`
    Divide,
    /// `+`
    Add,
    /// `-`
    Subtract,
}

impl Arithmetic {
    /// Creates `Arithmetic` from the operator string.
    ///
    /// # Errors
    /// Returns `SbroadError` when the operator is invalid.
    pub fn from(s: &str) -> Result<Self, SbroadError> {
        match s.to_lowercase().as_str() {
            "*" => Ok(Arithmetic::Multiply),
            "/" => Ok(Arithmetic::Divide),
            "+" => Ok(Arithmetic::Add),
            "-" => Ok(Arithmetic::Subtract),
            _ => Err(SbroadError::Unsupported(Entity::Operator, None)),
        }
    }
}

impl Display for Arithmetic {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op = match &self {
            Arithmetic::Multiply => "*",
            Arithmetic::Divide => "/",
            Arithmetic::Add => "+",
            Arithmetic::Subtract => "-",
        };

        write!(f, "{op}")
    }
}

/// Unary operator returning Bool expression.
#[derive(Serialize, Deserialize, PartialEq, Debug, Eq, Hash, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Unary {
    /// `not`
    Not,
    /// `is null`
    IsNull,
    /// `exists`
    Exists,
}

impl Unary {
    /// Creates `Unary` from the operator string.
    ///
    /// # Errors
    /// Returns `SbroadError` when the operator is invalid.
    pub fn from(s: &str) -> Result<Self, SbroadError> {
        match s.to_lowercase().as_str() {
            "is null" => Ok(Unary::IsNull),
            "exists" => Ok(Unary::Exists),
            _ => Err(SbroadError::Invalid(
                Entity::Operator,
                Some(format_smolstr!(
                    "expected `is null` or `is not null`, got unary operator `{s}`"
                )),
            )),
        }
    }
}

impl Display for Unary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op = match &self {
            Unary::Not => "not",
            Unary::IsNull => "is null",
            Unary::Exists => "exists",
        };

        write!(f, "{op}")
    }
}

/// Specifies what kind of join user specified in query
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum JoinKind {
    LeftOuter,
    Inner,
}

impl Display for JoinKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            JoinKind::LeftOuter => "left",
            JoinKind::Inner => "inner",
        };
        write!(f, "{kind}")
    }
}

/// Strategy applied on INSERT execution.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize, Default)]
pub enum ConflictStrategy {
    /// Swallow the error, do not insert the conflicting tuple
    DoNothing,
    /// Replace the conflicting tuple with the new one
    DoReplace,
    /// Throw the error, no tuples will be inserted for this
    /// storage. But for other storages the insertion may be successful.
    #[default]
    DoFail,
}

impl Display for ConflictStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ConflictStrategy::DoNothing => "nothing",
            ConflictStrategy::DoReplace => "replace",
            ConflictStrategy::DoFail => "fail",
        };
        write!(f, "{s}")
    }
}

/// Execution strategy for update node.
///
/// Depending on whether some sharding column
/// is updated or not, the update will be executed
/// differently.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum UpdateStrategy {
    /// Strategy when some sharding column is updated.
    /// When some sharding column is changed, it changes
    /// the bucket_id of the tuple. When this happens
    /// tuple may be needed to relocated on the other
    /// replicaset. This works as first selecting needed
    /// tuples, redistributing them and then deletion and insertion
    /// in the same local transaction on each storage.
    ///
    /// # Details
    /// Update works as follows:
    /// 1. Projection below update selects whole table tuple
    ///    with updated columns (but without bucket_id) and
    ///    old values for sharding columns.
    /// 2. Then on the router each tuple is transformed into two
    ///    tuples (one for insertion and one for deletion):
    ///     * old values of sharding columns are popped out from original tuple
    ///       and used to calculate the bucket_id for deletion tuple. The original
    ///       tuple becomes the tuple for insertion.
    ///     * deletion tuple is created from primary keys of insertion tuple.
    ///     * bucket_id for insertion tuple is calculated using new shard key values.
    /// 3. Because pk key can't be updated and insertion tuple contains
    ///    primary key + at least 1 sharding column and deletion tuple consists only from primary key
    ///    => len of insertion tuple > len of deletion tuple.
    ///    This invariant will be used to distinguish between these tuples on the storage (tuples are
    ///    stored in the same table, because currently whole sbroad assumes that motion
    ///    produces only one table). The len of deletion tuple is saved in this struct
    ///    variant
    /// 4. On storages the table is traversed and in transaction first tuples are deleted,
    ///    then insertion tuples are inserted.
    ShardedUpdate { delete_tuple_len: Option<usize> },
    /// Strategy when no sharding column is updated.
    ///
    /// In this case because join/selection does not change
    /// the distribution of data, update can be performed
    /// locally, without any data motion. NB: this may
    /// change if join children are reordered (update table scan
    /// is not inner child) or join conflict resolution changes.
    ///
    /// Projection below update has the following structure:
    /// ```sql
    /// select upd_expr1, .., upd_expr, pk_col1, .., pk_col
    /// ```
    /// If some expressions are the same, the column is reused.
    ///
    /// # Example
    /// ```sql
    /// update t set
    /// a = c + d
    /// b = c + d
    /// c = d
    /// ```
    /// Table t has pk columns `d, e`.
    /// Then the following projection will be created:
    /// ```sql
    /// select c + d, d, e
    /// ```
    LocalUpdate,
}

#[derive(Clone, Deserialize, Debug, PartialEq, Eq, Serialize)]
pub enum OrderByEntity {
    Expression { expr_id: NodeId },
    Index { value: usize },
}

#[derive(Clone, Deserialize, Debug, PartialEq, Eq, Serialize)]
pub enum OrderByType {
    Asc,
    Desc,
}

impl Display for OrderByType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderByType::Asc => write!(f, "asc"),
            OrderByType::Desc => write!(f, "desc"),
        }
    }
}

#[derive(Clone, Deserialize, Debug, PartialEq, Eq, Serialize)]
pub struct OrderByElement {
    pub entity: OrderByEntity,
    pub order_type: Option<OrderByType>,
}

impl Plan {
    /// Add relational node to plan arena and update shard columns info.
    ///
    /// # Errors
    /// - failed to oupdate shard columns info due to invalid plan subtree
    pub fn add_relational(&mut self, node: NodeAligned) -> Result<NodeId, SbroadError> {
        let rel_id = self.nodes.push(node);
        let mut context = self.context_mut();
        context.shard_col_info.update_node(rel_id, self)?;
        Ok(rel_id)
    }

    /// Adds delete node.
    ///
    /// # Errors
    /// - child id pointes to non-existing or non-relational node.
    pub fn add_delete(
        &mut self,
        table: SmolStr,
        child_id: Option<NodeId>,
    ) -> Result<NodeId, SbroadError> {
        let (output, children) = if let Some(child_id) = child_id {
            let output = self.add_row_for_output(child_id, &[], true, None)?;
            (Some(output), vec![child_id])
        } else {
            (None, vec![])
        };
        let delete = Delete {
            relation: table,
            children,
            output,
        };
        let delete_id = self.add_relational(delete.into())?;
        if let Some(output) = output {
            self.replace_parent_in_subtree(output, None, Some(delete_id))?;
        }
        Ok(delete_id)
    }

    /// Adds except node.
    ///
    /// # Errors
    /// - children nodes are not relational
    /// - children tuples are invalid
    /// - children tuples have mismatching structure
    pub fn add_except(&mut self, left: NodeId, right: NodeId) -> Result<NodeId, SbroadError> {
        let child_row_len = |child: NodeId, plan: &Plan| -> Result<usize, SbroadError> {
            let child_output = plan.get_relation_node(child)?.output();
            Ok(plan
                .get_expression_node(child_output)?
                .get_row_list()?
                .len())
        };

        let left_row_len = child_row_len(left, self)?;
        let right_row_len = child_row_len(right, self)?;
        if left_row_len != right_row_len {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "children tuples have mismatching amount of columns in except node: left {left_row_len}, right {right_row_len}"
            )));
        }

        let output = self.add_row_for_union_except(left, right)?;
        let except = Except {
            left,
            right,
            output,
        };

        let except_id = self.add_relational(except.into())?;
        self.replace_parent_in_subtree(output, None, Some(except_id))?;
        Ok(except_id)
    }

    /// Add `Update` relational node.
    ///
    /// This function first looks whether some sharding column is
    /// updated and then creates `Projection` and `Update` nodes
    /// according to that. For details, see [`UpdateStrategy`].
    ///
    /// Returns ids of both `Projection` and `Update`.
    ///
    /// # Projection format
    /// For sharded update:
    /// ```text
    /// table_tuple, old_shard_key
    /// ```
    /// `table_tuple` consists from table columns in the same order but:
    /// 1. If column is updated, it is replaced with corresponding update expression
    /// 2. `bucket_id` column is skipped
    ///
    /// For example:
    /// ```text
    /// t: a b bucket_id c
    /// shard_key: b c
    /// pk: a b
    ///
    /// update t set c = 20
    /// projection: a, b, 20, b, c
    /// ```
    ///
    /// For local update:
    /// ```text
    /// update_exprs, pk_exprs
    /// ```
    /// All expressions are unique. Therefore `pk_exprs`
    /// contains expression not present in `update_exprs`.
    /// Example:
    /// ```text
    /// t: a b c d bucket_id
    /// shard_key: a
    /// pk: a
    /// update t set
    /// c = a,
    /// b = a + 1
    /// c = a + 1
    ///             upd_exprs pk_exprs
    /// projection: a,   a+1
    /// ```
    /// Note that here `pk_expr` are empty, because all pk columns are already
    /// present in `upd_exps`.
    ///
    ///
    /// # Arguments
    /// * `update_defs` - mapping between column position in table
    ///   and corresponding update expression.
    /// * `relation` - name of the table being updated.
    /// * `rel_child_id` - id of `Update` child
    ///
    /// # Errors
    /// - invalid update table
    /// - invalid table columns positions in `update_defs`
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::mutable_key_type)]
    pub fn add_update(
        &mut self,
        relation: &str,
        update_defs: &HashMap<ColumnPosition, ExpressionId, RepeatableState>,
        rel_child_id: NodeId,
    ) -> Result<(NodeId, NodeId), SbroadError> {
        // Create Reference node from given table column.
        fn create_ref_from_column(
            plan: &mut Plan,
            relation: &str,
            table_position_map: &HashMap<ColumnPosition, ColumnPosition>,
            col_pos: usize,
        ) -> Result<NodeId, SbroadError> {
            let table = plan.get_relation_or_error(relation)?;
            let col: &Column = table.columns.get(col_pos).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Table,
                    Some(format_smolstr!(
                        "expected to have at least {} columns",
                        col_pos + 1
                    )),
                )
            })?;
            let output_pos = *table_position_map.get(&col_pos).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "Expected {} column in update child output",
                        &col.name
                    )),
                )
            })?;
            let col_type = col.r#type;
            let node = Reference {
                // It will be updated using `replace_parent_in_subtree`
                // in the end of the function.
                parent: None,
                targets: Some(vec![0]),
                position: output_pos,
                col_type,
                asterisk_source: None,
            };
            let id = plan.nodes.push(node.into());
            Ok(id)
        }

        let table = self.get_relation_or_error(relation)?;
        // is shard key column updated
        let is_sharded_update = !table.is_global()
            && table
                .get_sk()?
                .iter()
                .any(|col| update_defs.contains_key(col));
        // Columns of Projection that will be created
        let mut projection_cols: Vec<NodeId> = Vec::with_capacity(update_defs.len());
        // Positions of columns in Projection that constitute the primary key
        let mut primary_key_positions: Vec<usize> =
            Vec::with_capacity(table.primary_key.positions.len());
        // Maps position in table of column being updated to corresponding position in Projection
        let mut update_columns_map =
            HashMap::with_capacity_and_hasher(update_defs.len(), RepeatableState);
        // Helper map between table column position and corresponding column position in child's output
        let child_map = self.table_position_map(relation, rel_child_id)?;

        let update_kind = if is_sharded_update {
            // For sharded Update Projection has the following format:
            // table_tuple , sharding_key_columns
            // table_tuple is without bucket_id column

            // Calculate primary key positions in table_tuple
            if let Some(bucket_id_pos) = table.get_bucket_id_position()? {
                table.primary_key.positions.iter().for_each(|pos| {
                    if *pos < bucket_id_pos {
                        primary_key_positions.push(*pos);
                    } else {
                        primary_key_positions.push(*pos - 1);
                    }
                });
            } else {
                primary_key_positions.extend(table.primary_key.positions.iter());
            }

            // Create projection columns for table_tuple
            let cols_len = table.columns.len();
            // current projection column position
            let mut proj_pos = 0;
            for table_col in 0..cols_len {
                let column = self.get_relation_column(relation, table_col)?;
                // skip bucket_id
                if let ColumnRole::Sharding = column.role {
                    continue;
                }
                update_columns_map.insert(table_col, proj_pos);
                let expr_id = if let Some(id) = update_defs.get(&table_col) {
                    *id
                } else {
                    create_ref_from_column(self, relation, &child_map, table_col)?
                };
                projection_cols.push(expr_id);
                proj_pos += 1;
            }

            // Create projection columns for sharding_key_columns
            // todo(ars): some sharding column maybe already present in projection_cols
            let table = self.get_relation_or_error(relation)?;
            let shard_key_len = table.get_sk()?.len();
            for i in 0..shard_key_len {
                let table = self.get_relation_or_error(relation)?;
                let col_pos = *table.get_sk()?.get(i).ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "invalid shard col position {i}"
                    ))
                })?;
                let shard_col_expr_id =
                    create_ref_from_column(self, relation, &child_map, col_pos)?;
                projection_cols.push(shard_col_expr_id);
            }
            UpdateStrategy::ShardedUpdate {
                delete_tuple_len: None,
            }
        } else {
            // For local Update, projection has the following format:
            // update_expressions, pk_expressions (not present in update_expressions)

            // Helper map between expression and its position in projection.
            // The logic is that we want to reuse expression calculation in case it's used twice
            // (e.g. `update T set a = some_expr, b = some_expr`).
            let mut expr_to_col_pos: HashMap<PlanExpr, ColumnPosition> =
                HashMap::with_capacity(update_defs.len());
            // Expressions that form primary key of updated table.
            let pk_expressions = table
                .primary_key
                .positions
                .clone()
                .iter()
                .map(|pos| create_ref_from_column(self, relation, &child_map, *pos))
                .collect::<Result<Vec<NodeId>, SbroadError>>()?;

            let mut pos = 0;
            // Add update_expressions
            for (table_col, expr_id) in update_defs {
                let expr = PlanExpr::new(*expr_id, self);
                match expr_to_col_pos.entry(expr) {
                    Entry::Occupied(o) => {
                        let column_pos = *o.get();
                        update_columns_map.insert(*table_col, column_pos);
                    }
                    Entry::Vacant(v) => {
                        projection_cols.push(*expr_id);
                        update_columns_map.insert(*table_col, pos);
                        v.insert(pos);
                        pos += 1;
                    }
                }
            }

            // Add pk_expressions
            for expr_id in pk_expressions {
                let expr = PlanExpr::new(expr_id, self);
                match expr_to_col_pos.entry(expr) {
                    Entry::Vacant(e) => {
                        projection_cols.push(expr_id);
                        primary_key_positions.push(pos);
                        e.insert(pos);
                        pos += 1;
                    }
                    Entry::Occupied(o) => {
                        let column_pos = *o.get();
                        primary_key_positions.push(column_pos);
                    }
                }
            }
            UpdateStrategy::LocalUpdate
        };

        // Generate aliases to projection expressions, because
        // it is assumed that any projection column always has an alias.
        for (pos, expr_id) in projection_cols.iter_mut().enumerate() {
            let alias = get_unnamed_column_alias(pos);
            let alias_id = self.nodes.push(
                Alias {
                    child: *expr_id,
                    name: alias,
                }
                .into(),
            );
            *expr_id = alias_id;
        }
        let proj_output = self.nodes.add_row(projection_cols, None);
        let proj_node = Projection {
            children: vec![rel_child_id],
            output: proj_output,
            is_distinct: false,
        };
        let proj_id = self.add_relational(proj_node.into())?;
        self.replace_parent_in_subtree(proj_output, None, Some(proj_id))?;
        let upd_output = self.add_row_for_output(proj_id, &[], false, None)?;
        let update_node = Update {
            relation: relation.to_smolstr(),
            pk_positions: primary_key_positions,
            children: vec![proj_id],
            update_columns_map,
            output: upd_output,
            strategy: update_kind,
        };
        let update_id = self.add_relational(update_node.into())?;
        self.replace_parent_in_subtree(upd_output, None, Some(update_id))?;

        Ok((proj_id, update_id))
    }

    /// Adds insert node.
    ///
    /// # Errors
    /// - Failed to find a target relation.
    pub fn add_insert(
        &mut self,
        relation: &str,
        child: NodeId,
        columns: &[SmolStr],
        conflict_strategy: ConflictStrategy,
    ) -> Result<NodeId, SbroadError> {
        let rel = self.relations.get(relation).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Table,
                format_smolstr!("{relation} among plan relations"),
            )
        })?;
        let columns: Vec<usize> = if columns.is_empty() {
            rel.columns
                .iter()
                .enumerate()
                .filter(|(_, c)| ColumnRole::User.eq(c.get_role()))
                .map(|(i, _)| i)
                .collect()
        } else {
            let mut names: HashMap<&str, (&ColumnRole, usize), RandomState> =
                HashMap::with_capacity_and_hasher(columns.len(), RandomState::new());
            rel.columns.iter().enumerate().for_each(|(i, c)| {
                names.insert(c.name.as_str(), (c.get_role(), i));
            });
            let mut cols: Vec<usize> = Vec::with_capacity(names.len());
            for name in columns {
                match names.get(name.as_str()) {
                    Some((&ColumnRole::User, pos)) => cols.push(*pos),
                    Some((&ColumnRole::Sharding, _)) => {
                        return Err(SbroadError::FailedTo(
                            Action::Insert,
                            Some(Entity::Column),
                            format_smolstr!("system column {} cannot be inserted", to_user(name)),
                        ))
                    }
                    None => {
                        return Err(SbroadError::NotFound(Entity::Column, (*name).to_smolstr()))
                    }
                }
            }
            cols
        };
        let child_rel = self.get_relation_node(child)?;
        let child_output = self.get_expression_node(child_rel.output())?;
        let child_output_list_len = if let Expression::Row(Row { list, .. }) = child_output {
            list.len()
        } else {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some("child output is not a Row.".into()),
            ));
        };
        if child_output_list_len != columns.len() {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "invalid number of values: {}. Table {} expects {} column(s).",
                child_output_list_len,
                relation,
                columns.len()
            )));
        }

        let mut refs: Vec<NodeId> = Vec::with_capacity(rel.columns.len());
        for (pos, col) in rel.columns.iter().enumerate() {
            let r_id = self.nodes.add_ref(None, None, pos, col.r#type, None);
            let col_alias_id = self.nodes.add_alias(&col.name, r_id)?;
            refs.push(col_alias_id);
        }
        let dist = if rel.is_global() {
            Distribution::Global
        } else {
            let keys: HashSet<_, RepeatableState> =
                collection! { Key::new(rel.get_sk()?.to_vec()) };
            Distribution::Segment {
                keys: KeySet::from(keys),
            }
        };
        let output = self.nodes.add_row(refs, Some(dist));
        let insert = Insert {
            relation: relation.into(),
            columns,
            children: vec![child],
            output,
            conflict_strategy,
        };
        let insert_id = self.nodes.push(insert.into());
        self.replace_parent_in_subtree(output, None, Some(insert_id))?;
        Ok(insert_id)
    }

    /// Adds a scan node.
    ///
    /// # Errors
    /// - relation is invalid
    pub fn add_scan(&mut self, table: &str, alias: Option<&str>) -> Result<NodeId, SbroadError> {
        let nodes = &mut self.nodes;

        if let Some(rel) = self.relations.get(table) {
            let mut refs: Vec<NodeId> = Vec::with_capacity(rel.columns.len());
            for (pos, col) in rel.columns.iter().enumerate() {
                let r_id = nodes.add_ref(None, None, pos, col.r#type, None);
                let col_alias_id = nodes.add_alias(&col.name, r_id)?;
                refs.push(col_alias_id);
            }

            let output_id = nodes.add_row(refs, None);
            let scan = ScanRelation {
                output: output_id,
                relation: SmolStr::from(table),
                alias: alias.map(SmolStr::from),
            };

            let scan_id = self.add_relational(scan.into())?;
            self.replace_parent_in_subtree(output_id, None, Some(scan_id))?;
            return Ok(scan_id);
        }
        Err(SbroadError::NotFound(
            Entity::Table,
            format_smolstr!("{table} among the plan relations"),
        ))
    }

    /// Adds inner join node.
    ///
    /// # Errors
    /// - condition is not a boolean expression
    /// - children nodes are not relational
    /// - children output tuples are invalid
    pub fn add_join(
        &mut self,
        left: NodeId,
        right: NodeId,
        condition: NodeId,
        kind: JoinKind,
    ) -> Result<NodeId, SbroadError> {
        if !self.is_trivalent(condition)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some("Condition is not a trivalent expression".into()),
            ));
        }

        // For any child in a relational scan, we need to
        // remove a sharding column from its output with a
        // projection node and wrap the result with a sub-query
        // scan.
        // As a side effect, we also need to update the references
        // to the child's output in the condition expression as
        // we have filtered out the sharding column.
        let mut children: Vec<NodeId> = Vec::with_capacity(2);
        for (child, join_child) in &[(left, JoinChild::Outer), (right, JoinChild::Inner)] {
            let child_node = self.get_relation_node(*child)?;
            if let Relational::ScanRelation(ScanRelation {
                relation, alias, ..
            }) = child_node
            {
                // We'll need it later to update the condition expression (borrow checker).
                let table = self.get_relation_or_error(relation)?;
                let sharding_column_pos = table.get_bucket_id_position()?;
                let mut needs_bucket_id_column = false;

                // Update references to the sub-query's output in the condition.
                let condition_nodes = {
                    let filter = |id| -> bool {
                        matches!(self.get_expression_node(id), Ok(Expression::Reference(_)))
                    };
                    let mut condition_tree = PostOrderWithFilter::with_capacity(
                        |node| self.nodes.expr_iter(node, false),
                        EXPR_CAPACITY,
                        Box::new(filter),
                    );
                    condition_tree.populate_nodes(condition);
                    condition_tree.take_nodes()
                };
                // We should update ONLY references that refer to current child (left, right)
                let current_target = match join_child {
                    JoinChild::Inner => Some(vec![1_usize]),
                    JoinChild::Outer => Some(vec![0_usize]),
                };
                let mut refs = Vec::with_capacity(condition_nodes.len());
                for LevelNode(_, id) in condition_nodes {
                    let expr = self.get_expression_node(id)?;
                    if let Expression::Reference(Reference {
                        position, targets, ..
                    }) = expr
                    {
                        if *targets == current_target {
                            if Some(*position) == sharding_column_pos {
                                needs_bucket_id_column = true;
                            }
                            refs.push(id);
                        }
                    }
                }

                // Wrap relation with sub-query scan.
                let scan_name = if let Some(alias_name) = alias {
                    alias_name.clone()
                } else {
                    relation.clone()
                };
                let proj_id = self.add_proj(*child, &[], false, needs_bucket_id_column)?;
                let sq_id = self.add_sub_query(proj_id, Some(&scan_name))?;
                children.push(sq_id);

                if needs_bucket_id_column {
                    continue;
                }

                if let Some(sharding_column_pos) = sharding_column_pos {
                    for ref_id in refs {
                        let expr = self.get_mut_expression_node(ref_id)?;
                        if let MutExpression::Reference(Reference {
                            position, targets, ..
                        }) = expr
                        {
                            if current_target == *targets && *position > sharding_column_pos {
                                *position -= 1;
                            }
                        }
                    }
                }
            } else {
                children.push(*child);
            }
        }
        if let (Some(left_id), Some(right_id)) = (children.first(), children.get(1)) {
            let output = self.add_row_for_join(*left_id, *right_id)?;
            let join = Join {
                children: vec![*left_id, *right_id],
                condition,
                output,
                kind,
            };

            let join_id = self.add_relational(join.into())?;
            self.replace_parent_in_subtree(condition, None, Some(join_id))?;
            self.replace_parent_in_subtree(output, None, Some(join_id))?;
            return Ok(join_id);
        }
        Err(SbroadError::Invalid(
            Entity::Expression,
            Some("invalid children for join".to_smolstr()),
        ))
    }

    /// Adds motion node.
    ///
    /// # Errors
    /// - Unable to get node.
    ///
    /// # Panics
    /// - Child node is not relational.
    /// - Child output tuple is invalid.
    pub fn add_motion(
        &mut self,
        child_id: NodeId,
        policy: &MotionPolicy,
        program: Program,
    ) -> Result<NodeId, SbroadError> {
        let child_rel_node = self.get_relation_node(child_id)?;
        let alias = match child_rel_node {
            Relational::ScanSubQuery(ScanSubQuery { alias, .. })
            | Relational::ScanRelation(ScanRelation { alias, .. }) => alias.clone(),
            Relational::ScanCte(ScanCte { alias, .. }) => Some(alias.clone()),
            _ => None,
        };

        // If child is a motion, we can squash multiple motions into one
        let mut_child_node = self.get_mut_relation_node(child_id)?;
        if let MutRelational::Motion(Motion {
            policy: new_policy,
            program: new_program,
            ..
        }) = mut_child_node
        {
            *new_policy = policy.clone();
            new_program.0.extend_from_slice(&program.0);
            return Ok(child_id);
        }

        let output = self.add_row_for_output(child_id, &[], true, None)?;
        match policy {
            MotionPolicy::None => {
                panic!(
                    "None policy is not expected for `add_motion` method for child_id: {child_id}."
                );
            }
            MotionPolicy::Segment(key) | MotionPolicy::LocalSegment(key) => {
                if let Ok(keyset) = KeySet::try_from(key) {
                    self.set_dist(output, Distribution::Segment { keys: keyset })?;
                } else {
                    self.set_dist(output, Distribution::Any)?;
                }
            }
            MotionPolicy::Full => {
                self.set_dist(output, Distribution::Global)?;
            }
            MotionPolicy::Local => {
                self.set_dist(output, Distribution::Any)?;
            }
        }

        let motion = Motion {
            alias,
            children: vec![child_id],
            policy: policy.clone(),
            program,
            output,
        };
        let motion_id = self.add_relational(motion.into())?;
        self.replace_parent_in_subtree(output, None, Some(motion_id))?;
        let mut context = self.context_mut();
        context
            .shard_col_info
            .borrow_mut()
            .handle_node_insertion(motion_id, self)?;
        Ok(motion_id)
    }

    // TODO: we need a more flexible projection constructor (constants, etc)

    /// Adds projection node.
    ///
    /// # Errors
    /// - child node is not relational
    /// - child output tuple is invalid
    /// - column name do not match the ones in the child output tuple
    pub fn add_proj(
        &mut self,
        child: NodeId,
        col_names: &[&str],
        is_distinct: bool,
        needs_shard_col: bool,
    ) -> Result<NodeId, SbroadError> {
        let output = self.add_row_for_output(child, col_names, needs_shard_col, None)?;
        let proj = Projection {
            children: vec![child],
            output,
            is_distinct,
        };

        let proj_id = self.add_relational(proj.into())?;
        self.replace_parent_in_subtree(output, None, Some(proj_id))?;
        Ok(proj_id)
    }

    /// Adds projection node (use a list of expressions instead of alias names).
    ///
    /// # Errors
    /// - child node is not relational
    /// - child output tuple is invalid
    /// - columns are not aliases or have duplicate names
    pub fn add_proj_internal(
        &mut self,
        child: NodeId,
        columns: &[NodeId],
        is_distinct: bool,
    ) -> Result<NodeId, SbroadError> {
        let output = self.nodes.add_row(columns.to_vec(), None);
        let proj = Projection {
            children: vec![child],
            output,
            is_distinct,
        };

        let proj_id = self.add_relational(proj.into())?;
        self.replace_parent_in_subtree(output, None, Some(proj_id))?;
        Ok(proj_id)
    }

    /// Adds projection node (use a list of expressions instead of alias names).
    ///
    /// # Errors
    /// - child node is not relational
    /// - child output tuple is invalid
    /// - columns are not aliases or have duplicate names
    pub fn add_select_without_scan(&mut self, columns: &[NodeId]) -> Result<NodeId, SbroadError> {
        let output = self.nodes.add_row(columns.to_vec(), None);
        let sel = SelectWithoutScan {
            children: vec![],
            output,
        };

        let sel_id = self.add_relational(sel.into())?;
        Ok(sel_id)
    }

    /// Adds `Select` node.
    ///
    /// # Errors
    /// - children list is empty
    /// - filter expression is not boolean
    /// - children nodes are not relational
    /// - first child output tuple is not valid
    ///
    /// # Panics
    /// - `children` is empty
    pub fn add_select(
        &mut self,
        children: &[NodeId],
        filter: NodeId,
    ) -> Result<NodeId, SbroadError> {
        let first_child: NodeId = *children
            .first()
            .expect("No children passed to `add_select`");

        if !self.is_trivalent(filter)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some("filter expression is not a trivalent expression.".into()),
            ));
        }

        let output = self.add_row_for_output(first_child, &[], true, None)?;
        let select = Selection {
            children: children.into(),
            filter,
            output,
        };

        let select_id = self.add_relational(select.into())?;
        self.replace_parent_in_subtree(filter, None, Some(select_id))?;
        self.replace_parent_in_subtree(output, None, Some(select_id))?;
        Ok(select_id)
    }

    /// Adds having node
    ///
    /// # Errors
    /// - children list is empty
    /// - filter expression is not boolean
    /// - children nodes are not relational
    /// - first child output tuple is not valid
    pub fn add_having(
        &mut self,
        children: &[NodeId],
        filter: NodeId,
    ) -> Result<NodeId, SbroadError> {
        let first_child: NodeId = match children.len() {
            0 => {
                return Err(SbroadError::UnexpectedNumberOfValues(
                    "children list is empty".into(),
                ))
            }
            _ => children[0],
        };

        if !self.is_trivalent(filter)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some("filter expression is not a trivalent expression.".into()),
            ));
        }

        for child in children {
            if let Node::Relational(_) = self.get_node(*child)? {
            } else {
                return Err(SbroadError::Invalid(Entity::Relational, None));
            }
        }

        let output = self.add_row_for_output(first_child, &[], true, None)?;
        let having = Having {
            children: children.into(),
            filter,
            output,
        };

        let having_id = self.add_relational(having.into())?;
        self.replace_parent_in_subtree(filter, None, Some(having_id))?;
        self.replace_parent_in_subtree(output, None, Some(having_id))?;
        Ok(having_id)
    }

    /// Add `OrderBy` node into the plan.
    ///
    /// Returns both ids of:
    /// * `OrderBy` node and
    /// * Top projection node
    ///
    /// # Errors
    /// - Unable to add output row from child.
    /// - Unable to replace parent in subtree.
    ///
    /// # Panics
    /// - Relational node child not found.
    pub fn add_order_by(
        &mut self,
        child: NodeId,
        order_by_elements: Vec<OrderByElement>,
    ) -> Result<(NodeId, NodeId), SbroadError> {
        let output = self.add_row_for_output(child, &[], true, None)?;
        let order_by = OrderBy {
            children: vec![child],
            output,
            order_by_elements: order_by_elements.clone(),
        };

        let plan_order_by_id = self.add_relational(order_by.into())?;
        for order_by_element in order_by_elements {
            if let OrderByElement {
                entity: OrderByEntity::Expression { expr_id },
                ..
            } = order_by_element
            {
                self.replace_parent_in_subtree(expr_id, None, Some(plan_order_by_id))?;
            }
        }
        self.replace_parent_in_subtree(output, None, Some(plan_order_by_id))?;
        let top_proj_id = self.add_proj(plan_order_by_id, &[], false, true)?;
        Ok((plan_order_by_id, top_proj_id))
    }

    /// Adds sub query scan node.
    ///
    /// # Errors
    /// - child node is not relational
    /// - child node output is not a correct tuple
    pub fn add_sub_query(
        &mut self,
        child: NodeId,
        alias: Option<&str>,
    ) -> Result<NodeId, SbroadError> {
        let name: Option<SmolStr> = alias.map(SmolStr::from);

        let output = self.add_row_for_output(child, &[], true, None)?;
        let sq = ScanSubQuery {
            alias: name,
            children: vec![child],
            output,
        };

        let sq_id = self.add_relational(sq.into())?;
        self.replace_parent_in_subtree(output, None, Some(sq_id))?;
        Ok(sq_id)
    }

    /// Appends a new CTE node to the plan arena.
    ///
    /// # Errors
    /// - CTE has incorrect amount of columns;
    ///
    /// # Panics
    /// - child node is not a valid relational node.
    pub fn add_cte(
        &mut self,
        child: NodeId,
        alias: SmolStr,
        columns: Vec<SmolStr>,
    ) -> Result<NodeId, SbroadError> {
        let child_node = self
            .get_relation_node(child)
            .expect("CTE child node is not a relational node");
        let mut child_id = child;

        if !columns.is_empty() {
            let mut child_output_id = child_node.output();
            // Child must be a projection, but sometimes we need to get our hand dirty to maintain
            // this invariant. For instance, child can be VALUES, LIMIT or UNION. In such cases
            // we wrap the child with a subquery and change names in the subquery's projection.
            if !matches!(child_node, Relational::Projection { .. }) {
                let sq_id = self
                    .add_sub_query(child_id, Some(&alias))
                    .expect("add subquery in cte");
                child_id = self
                    .add_proj(sq_id, &[], false, false)
                    .expect("add projection in cte");
                child_output_id = self
                    .get_relational_output(child_id)
                    .expect("projection has an output tuple");
            }

            // If CTE has explicit column names, let's rename the columns in the child projection.
            let child_columns = self
                .get_expression_node(child_output_id)
                .expect("output row")
                .clone_row_list()?;
            if child_columns.len() != columns.len() {
                return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                    "expected {} columns in CTE, got {}",
                    child_columns.len(),
                    columns.len()
                )));
            }
            for (col_id, col_name) in child_columns.into_iter().zip(columns.into_iter()) {
                let col_alias = self
                    .get_mut_expression_node(col_id)
                    .expect("column expression");
                if let MutExpression::Alias(Alias { name, .. }) = col_alias {
                    *name = col_name;
                } else {
                    panic!("Expected a row of aliases in the output tuple");
                };
            }
        }

        let output = self
            .add_row_for_output(child_id, &[], true, None)
            .expect("output row for CTE");
        let cte = ScanCte {
            alias,
            child: child_id,
            output,
        };
        let cte_id = self.add_relational(cte.into())?;
        Ok(cte_id)
    }

    /// Adds union all node.
    ///
    /// # Errors
    /// - children nodes are not relational
    /// - children tuples are invalid
    /// - children tuples have mismatching structure
    pub fn add_union(
        &mut self,
        left: NodeId,
        right: NodeId,
        remove_duplicates: bool,
    ) -> Result<NodeId, SbroadError> {
        let child_row_len = |child: NodeId, plan: &Plan| -> Result<usize, SbroadError> {
            let child_output = plan.get_relation_node(child)?.output();
            Ok(plan
                .get_expression_node(child_output)?
                .get_row_list()?
                .len())
        };

        let left_row_len = child_row_len(left, self)?;
        let right_row_len = child_row_len(right, self)?;
        if left_row_len != right_row_len {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "children tuples have mismatching amount of columns in union all node: left {left_row_len}, right {right_row_len}"
            )));
        }

        let output = self.add_row_for_union_except(left, right)?;
        let union_all: NodeAligned = if remove_duplicates {
            Union {
                left,
                right,
                output,
            }
            .into()
        } else {
            UnionAll {
                left,
                right,
                output,
            }
            .into()
        };

        let union_id = self.add_relational(union_all)?;
        self.replace_parent_in_subtree(output, None, Some(union_id))?;
        Ok(union_id)
    }

    /// Adds a limit node to the top of select node.
    ///
    /// # Errors
    /// - Row node is not of a row type
    pub fn add_limit(&mut self, select: NodeId, limit: u64) -> Result<NodeId, SbroadError> {
        let output = self.add_row_for_output(select, &[], true, None)?;
        let limit = Limit {
            output,
            limit,
            child: select,
        };

        let limit_id = self.add_relational(limit.into())?;
        self.replace_parent_in_subtree(output, None, Some(limit_id))?;
        Ok(limit_id)
    }

    /// Adds a values row node.
    ///
    /// # Errors
    /// - Row node is not of a row type
    pub fn add_values_row(
        &mut self,
        expr_row_id: NodeId,
        col_idx: &mut usize,
    ) -> Result<NodeId, SbroadError> {
        let row = self.get_expression_node(expr_row_id)?;
        let columns = row.clone_row_list()?;
        let mut aliases: Vec<NodeId> = Vec::with_capacity(columns.len());
        for col_id in columns {
            // Generate a row of aliases for the incoming row.
            *col_idx += 1;
            // The column names are generated according to tarantool naming of anonymous columns
            let name = format!("COLUMN_{col_idx}");
            let alias_id = self.nodes.add_alias(&name, col_id)?;
            aliases.push(alias_id);
        }
        let output = self.nodes.add_row(aliases, None);

        let values_row = ValuesRow {
            output,
            data: expr_row_id,
            children: vec![],
        };
        let values_row_id = self.add_relational(values_row.into())?;
        self.replace_parent_in_subtree(expr_row_id, None, Some(values_row_id))?;
        Ok(values_row_id)
    }

    /// Adds values node.
    ///
    /// # Errors
    /// - No child nodes
    /// - Child node is not relational
    pub fn add_values(&mut self, value_rows: Vec<NodeId>) -> Result<NodeId, SbroadError> {
        // Check that all rows in the values node have the same amount of columns.
        let mut last_len = None;
        for value_row_id in &value_rows {
            let Relational::ValuesRow(ValuesRow { output, .. }) =
                self.get_relation_node(*value_row_id)?
            else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some("all children of a Values node must be ValuesRow".into()),
                ));
            };
            let len = self.get_row_list(*output)?.len();
            match last_len {
                None => last_len = Some(len),
                Some(last_len) if last_len != len => {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some("all values rows must have the same length".into()),
                    ));
                }
                _ => {}
            }
        }

        // In case we have several `ValuesRow` under `Values`
        // (e.g. VALUES (1, "test_1"), (2, "test_2")),
        // the list of alias column names for it will look like:
        // (COLUMN_1, COLUMN_2), (COLUMN_3, COLUMN_4).
        // As soon as we want to assign name for column and not for the specific value,
        // we choose the names of last `ValuesRow` and set them as names of all the columns of `Values`.
        // The assumption always is that the child `ValuesRow` has the same number of elements.
        let last_id = if let Some(last_id) = value_rows.last() {
            *last_id
        } else {
            return Err(SbroadError::UnexpectedNumberOfValues(
                "Values node has no children, expected at least one child.".into(),
            ));
        };
        let value_row_last = self.get_relation_node(last_id)?;
        let last_output_id = if let Relational::ValuesRow(ValuesRow { output, .. }) = value_row_last
        {
            *output
        } else {
            return Err(SbroadError::UnexpectedNumberOfValues(
                "Values node must have at least one child row.".into(),
            ));
        };
        let last_output = self.get_expression_node(last_output_id)?;
        let names = if let Expression::Row(Row { list, .. }) = last_output {
            let mut aliases: Vec<SmolStr> = Vec::with_capacity(list.len());
            for alias_id in list {
                let alias = self.get_expression_node(*alias_id)?;
                if let Expression::Alias(Alias { name, .. }) = alias {
                    aliases.push(name.clone());
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some("Expected an alias".into()),
                    ));
                }
            }
            aliases
        } else {
            return Err(SbroadError::UnexpectedNumberOfValues(
                "Values node must have at least one child row.".into(),
            ));
        };

        // Generate a row of aliases referencing all the children.
        let mut aliases: Vec<NodeId> = Vec::with_capacity(names.len());
        let columns = last_output.clone_row_list()?;
        for (pos, name) in names.iter().enumerate() {
            let col_id = *columns.get(pos).ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                    "Values node has no column at position {pos}"
                ))
            })?;
            let col_expr = self.get_expression_node(col_id)?;
            let col_type = col_expr.calculate_type(self)?;
            let ref_id = self.nodes.add_ref(
                None,
                Some((0..value_rows.len()).collect::<Vec<usize>>()),
                pos,
                col_type,
                None,
            );
            let alias_id = self.nodes.add_alias(name, ref_id)?;
            aliases.push(alias_id);
        }
        let output = self.nodes.add_row(aliases, None);

        let values = Values {
            output,
            children: value_rows,
        };
        let values_id = self.add_relational(values.into())?;
        self.replace_parent_in_subtree(output, None, Some(values_id))?;
        Ok(values_id)
    }

    /// Gets an output tuple from relational node id
    ///
    /// # Errors
    /// - node is not relational
    pub fn get_relational_output(&self, rel_id: NodeId) -> Result<NodeId, SbroadError> {
        let rel_node = self.get_relation_node(rel_id)?;
        Ok(rel_node.output())
    }

    /// Gets list of aliases in output tuple of `rel_id`.
    ///
    /// # Errors
    /// - node is not relational
    /// - output is not `Expression::Row`
    /// - any node in the output tuple is not `Expression::Alias`
    pub fn get_relational_aliases(&self, rel_id: NodeId) -> Result<Vec<SmolStr>, SbroadError> {
        let output = self.get_relational_output(rel_id)?;
        if let Expression::Row(Row { list, .. }) = self.get_expression_node(output)? {
            return list
                .iter()
                .map(|alias_id| {
                    self.get_expression_node(*alias_id)?
                        .get_alias_name()
                        .map(smol_str::ToSmolStr::to_smolstr)
                })
                .collect::<Result<Vec<SmolStr>, SbroadError>>();
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!(
                "expected output of Relational node {rel_id:?} to be Row"
            )),
        ))
    }

    /// Gets children from relational node.
    pub fn get_relational_children(&self, rel_id: NodeId) -> Result<Children<'_>, SbroadError> {
        if let Node::Relational(_) = self.get_node(rel_id)? {
            Ok(self.children(rel_id))
        } else {
            panic!("Expected relational node for getting children.")
        }
    }

    /// Gets child with specified index
    ///
    /// # Errors
    /// - Unable to get node.
    ///
    /// # Panics
    /// - Node is not relational.
    /// - Node does not have child with specified index.
    pub fn get_relational_child(
        &self,
        rel_id: NodeId,
        child_idx: usize,
    ) -> Result<NodeId, SbroadError> {
        if let Node::Relational(rel) = self.get_node(rel_id)? {
            return Ok(*rel.children().get(child_idx).unwrap_or_else(|| {
                panic!("Rel node {rel:?} has no child with idx ({child_idx}).")
            }));
        }
        panic!("Relational node with id {rel_id} is not found.")
    }

    /// Some nodes (like Having, Selection, Join),
    /// may have 0 or more subqueries in addition to
    /// their required children. This is a helper method
    /// to return the number of required children.
    ///
    /// # Errors
    /// - Node is not Join, Having, Selection
    pub fn get_required_children_len(&self, rel_id: NodeId) -> Result<Option<usize>, SbroadError> {
        let rel_node = self.get_relation_node(rel_id)?;
        let len = match rel_node {
            Relational::Join { .. } => 2,
            Relational::Having { .. }
            | Relational::Selection { .. }
            | Relational::Projection { .. }
            | Relational::GroupBy { .. }
            | Relational::OrderBy { .. } => 1,
            Relational::ValuesRow { .. } | Relational::SelectWithoutScan { .. } => 0,
            _ => return Ok(None),
        };
        Ok(Some(len))
    }

    /// Finds the parent of the given relational node.
    ///
    /// # Errors
    /// - node is not relational
    /// - Plan has no top
    pub fn find_parent_rel(&self, target_id: NodeId) -> Result<Option<NodeId>, SbroadError> {
        for (id, _) in self.nodes.arena32.iter().enumerate() {
            let parent_id = NodeId {
                offset: u32::try_from(id).unwrap(),
                arena_type: ArenaType::Arena32,
            };

            if !matches!(self.get_node(parent_id)?, Node::Relational(_)) {
                continue;
            }

            for child_id in self.nodes.rel_iter(parent_id) {
                if child_id == target_id {
                    return Ok(Some(parent_id));
                }
            }
        }

        for (id, _) in self.nodes.arena64.iter().enumerate() {
            let parent_id = NodeId {
                offset: u32::try_from(id).unwrap(),
                arena_type: ArenaType::Arena64,
            };

            if !matches!(self.get_node(parent_id)?, Node::Relational(_)) {
                continue;
            }

            for child_id in self.nodes.rel_iter(parent_id) {
                if child_id == target_id {
                    return Ok(Some(parent_id));
                }
            }
        }

        for (id, _) in self.nodes.arena96.iter().enumerate() {
            let parent_id = NodeId {
                offset: u32::try_from(id).unwrap(),
                arena_type: ArenaType::Arena96,
            };

            if !matches!(self.get_node(parent_id)?, Node::Relational(_)) {
                continue;
            }

            for child_id in self.nodes.rel_iter(parent_id) {
                if child_id == target_id {
                    return Ok(Some(parent_id));
                }
            }
        }

        for (id, _) in self.nodes.arena136.iter().enumerate() {
            let parent_id = NodeId {
                offset: u32::try_from(id).unwrap(),
                arena_type: ArenaType::Arena136,
            };

            if !matches!(self.get_node(parent_id)?, Node::Relational(_)) {
                continue;
            }

            for child_id in self.nodes.rel_iter(parent_id) {
                if child_id == target_id {
                    return Ok(Some(parent_id));
                }
            }
        }

        for (id, _) in self.nodes.arena224.iter().enumerate() {
            let parent_id = NodeId {
                offset: u32::try_from(id).unwrap(),
                arena_type: ArenaType::Arena232,
            };

            if !matches!(self.get_node(parent_id)?, Node::Relational(_)) {
                continue;
            }

            for child_id in self.nodes.rel_iter(parent_id) {
                if child_id == target_id {
                    return Ok(Some(parent_id));
                }
            }
        }

        Ok(None)
    }

    /// Change child of relational node.
    ///
    /// # Errors
    /// - node is not relational
    /// - node does not have child with specified id
    pub fn change_child(
        &mut self,
        parent_id: NodeId,
        old_child_id: NodeId,
        new_child_id: NodeId,
    ) -> Result<(), SbroadError> {
        let mut node = self.get_mut_relation_node(parent_id)?;
        let children = node.mut_children();
        for child_id in children {
            if *child_id == old_child_id {
                *child_id = new_child_id;
                return Ok(());
            }
        }

        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!(
                "node ({parent_id:?}) has no child with id ({old_child_id:?})"
            )),
        ))
    }

    /// Create a mapping between column positions
    /// in table and corresponding positions in
    /// relational node's output. Sharding
    /// column is skipped.
    ///
    /// # Errors
    /// - Node is not relational
    /// - Output tuple is invalid
    /// - Some table column is not found among output columns
    pub fn table_position_map(
        &self,
        table_name: &str,
        rel_id: NodeId,
    ) -> Result<HashMap<ColumnPosition, ColumnPosition>, SbroadError> {
        let table = self.get_relation_or_error(table_name)?;
        let alias_to_pos = ColumnPositionMap::new(self, rel_id)?;
        let mut map: HashMap<ColumnPosition, ColumnPosition> =
            HashMap::with_capacity(table.columns.len());
        for (table_pos, col) in table.columns.iter().enumerate() {
            if let ColumnRole::Sharding = col.role {
                continue;
            }
            let output_pos = alias_to_pos.get(col.name.as_str())?;
            map.insert(table_pos, output_pos);
        }
        Ok(map)
    }

    /// Sets children for relational node
    pub fn set_relational_children(&mut self, rel_id: NodeId, children: Vec<NodeId>) {
        if let MutNode::Relational(ref mut rel) =
            self.get_mut_node(rel_id).expect("Rel node must be valid.")
        {
            rel.set_children(children);
        } else {
            panic!("Expected relational node for {rel_id}.");
        }
    }

    /// Get relational Scan name that given `output_alias_position` (`Expression::Alias`)
    /// references to.
    ///
    /// # Errors
    /// - plan tree is invalid (failed to retrieve child nodes)
    pub fn scan_name(
        &self,
        id: NodeId,
        output_alias_position: usize,
    ) -> Result<Option<&str>, SbroadError> {
        let node = self.get_relation_node(id)?;
        match node {
            Relational::Insert(Insert { relation, .. })
            | Relational::Delete(Delete { relation, .. }) => Ok(Some(relation.as_str())),
            Relational::ScanRelation(ScanRelation {
                alias, relation, ..
            }) => Ok(alias.as_deref().or(Some(relation.as_str()))),
            Relational::Projection { .. }
            | Relational::SelectWithoutScan { .. }
            | Relational::GroupBy { .. }
            | Relational::OrderBy { .. }
            | Relational::Intersect { .. }
            | Relational::Having { .. }
            | Relational::Selection { .. }
            | Relational::Update { .. }
            | Relational::Join { .. } => {
                let output_row = self.get_expression_node(node.output())?;
                let list = output_row.get_row_list()?;
                let col_id = *list.get(output_alias_position).ok_or_else(|| {
                    SbroadError::NotFound(
                        Entity::Column,
                        format_smolstr!(
                            "at position {output_alias_position} of Row, {self:?}, {output_row:?}"
                        ),
                    )
                })?;
                let col_node = self.get_expression_node(col_id)?;
                if let Expression::Alias(Alias { child, .. }) = col_node {
                    let child_node = self.get_expression_node(*child)?;
                    if let Expression::Reference(Reference { position: pos, .. }) = child_node {
                        let rel_id = self.get_relational_from_reference_node(*child)?;
                        let rel_node = self.get_relation_node(rel_id)?;
                        if rel_node == node {
                            return Err(SbroadError::DuplicatedValue(format_smolstr!(
                                "Reference to the same node {rel_node:?} at position {output_alias_position}"
                            )));
                        }
                        return self.scan_name(rel_id, *pos);
                    }
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some("expected an alias in the output row".into()),
                    ));
                }
                Ok(None)
            }
            Relational::ScanCte(ScanCte { alias, .. }) => Ok(Some(alias)),
            Relational::ScanSubQuery(ScanSubQuery { alias, .. })
            | Relational::Motion(Motion { alias, .. }) => {
                if let Some(name) = alias.as_ref() {
                    if !name.is_empty() {
                        return Ok(alias.as_deref());
                    }
                }
                Ok(None)
            }
            Relational::Except { .. }
            | Relational::Union { .. }
            | Relational::UnionAll { .. }
            | Relational::Values { .. }
            | Relational::Limit { .. }
            | Relational::ValuesRow { .. } => Ok(None),
        }
    }

    /// Checks if the node is an additional child of some relational node.
    /// We can use a simple node scan instead of the tree traversal as we are interested
    /// only in relational nodes that can't be unlinked from the tree by our transformations.
    /// This is done for performance reasons.
    ///
    /// # Errors
    /// - Failed to get plan top
    /// - Node returned by the relational iterator is not relational (bug)
    pub fn is_additional_child(&self, sq_id: NodeId) -> Result<bool, SbroadError> {
        let parent_rel_id = self.find_parent_rel(sq_id)?;
        if let Some(parent_rel_id) = parent_rel_id {
            if self.is_additional_child_of_rel(parent_rel_id, sq_id)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Checks that the sub-query is an additional child of the parent relational node.
    ///
    /// # Errors
    /// - If the node is not relational.
    pub fn is_additional_child_of_rel(
        &self,
        rel_id: NodeId,
        sq_id: NodeId,
    ) -> Result<bool, SbroadError> {
        let children = self.get_relational_children(rel_id)?;
        let to_skip = self.get_required_children_len(rel_id)?;
        let Some(to_skip) = to_skip else {
            return Ok(false);
        };
        if children.iter().skip(to_skip).any(|&c| c == sq_id) {
            return Ok(true);
        }
        Ok(false)
    }

    /// Get `Motion`'s node policy
    ///
    /// # Errors
    /// - node is not motion
    pub fn get_motion_policy(&self, motion_id: NodeId) -> Result<&MotionPolicy, SbroadError> {
        let node = self.get_relation_node(motion_id)?;
        if let Relational::Motion(Motion { policy, .. }) = node {
            return Ok(policy);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!("expected Motion, got: {node:?}")),
        ))
    }

    // Gets an immutable reference to the children nodes.
    /// # Panics
    #[must_use]
    pub fn children(&self, rel_id: NodeId) -> Children<'_> {
        let node = self.get_relation_node(rel_id).unwrap();
        match node {
            Relational::Limit(Limit { child, .. }) | Relational::ScanCte(ScanCte { child, .. }) => {
                Children::Single(child)
            }
            Relational::Except(Except { left, right, .. })
            | Relational::Intersect(Intersect { left, right, .. })
            | Relational::UnionAll(UnionAll { left, right, .. })
            | Relational::Union(Union { left, right, .. }) => Children::Couple(left, right),
            Relational::GroupBy(GroupBy { children, .. })
            | Relational::Update(Update { children, .. })
            | Relational::Join(Join { children, .. })
            | Relational::Having(Having { children, .. })
            | Relational::OrderBy(OrderBy { children, .. })
            | Relational::Delete(Delete { children, .. })
            | Relational::Insert(Insert { children, .. })
            | Relational::Motion(Motion { children, .. })
            | Relational::Projection(Projection { children, .. })
            | Relational::ScanSubQuery(ScanSubQuery { children, .. })
            | Relational::Selection(Selection { children, .. })
            | Relational::SelectWithoutScan(SelectWithoutScan { children, .. })
            | Relational::ValuesRow(ValuesRow { children, .. })
            | Relational::Values(Values { children, .. }) => Children::Many(children),
            Relational::ScanRelation(_) => Children::None,
        }
    }
}

#[cfg(test)]
mod tests;
