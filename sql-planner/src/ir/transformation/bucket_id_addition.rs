//! Extend condition on primary key (without bucket_id)
//! with condition on bucket_id
//! (in case of primary key containts bucket_id as first part).
//!
//! For example, the following query:
//! ```sql
//! CREATE TABLE t (pk int, PRIMARY KEY (bucket_id, pk))
//! SELECT * FROM t WHERE pk = 1
//! ```
//! would be converted to:
//! ```sql
//! SELECT * FROM t WHERE bucket_id IN (...) AND pk = 1
//! ```

use crate::errors::SbroadError;
use crate::executor::bucket::Buckets;
use crate::ir::distribution::Distribution;
use crate::ir::helpers::RepeatableState;
use crate::ir::node::{ArenaType, Node64, NodeId, Reference};
use crate::ir::operator::Bool;
use crate::ir::relation::ColumnRole;
use crate::ir::value::Value;
use crate::ir::{Expression, MutRelational, Plan, Relational, Selection};
use itertools::Itertools;
use std::collections::HashSet;

impl Plan {
    /// Add condition on "bucket_id" if needed.
    pub fn add_condition_on_bucket_id(&mut self, buckets: &Buckets) -> Result<(), SbroadError> {
        // We need to have exact number of buckets.
        let Buckets::Filtered(filtered) = buckets else {
            return Ok(());
        };
        if filtered.is_empty() {
            return Ok(());
        }
        // Do not support complex cases (more than one table) for now.
        if self.relations.tables.len() != 1 {
            return Ok(());
        }
        let Some((_, table)) = self.relations.tables.iter().last() else {
            return Ok(());
        };
        let Some(first_column) = table.columns.first() else {
            return Ok(());
        };
        // First column must be "bucket_id" for this transformation.
        if first_column.role != ColumnRole::Sharding {
            return Ok(());
        }

        transform_expr_trees(self, filtered)
    }
}

/// Apply transformation [`add_condition_on_bucket_id_impl`]
/// to all expressions that are [`Relational::Selection`]` filters.
fn transform_expr_trees(
    plan: &mut Plan,
    buckets: &HashSet<u64, RepeatableState>,
) -> Result<(), SbroadError> {
    let node_ids: Vec<_> = plan
        .nodes
        .iter64()
        .enumerate()
        .filter(|(_, n)| matches!(n, Node64::Selection(_)))
        .map(|(i, _)| NodeId {
            offset: i.try_into().unwrap(),
            arena_type: ArenaType::Arena64,
        })
        .collect();

    for id in node_ids {
        let rel = plan.get_relation_node(id)?;
        let Relational::Selection(Selection { filter, output, .. }) = rel else {
            unreachable!("Selection node expected for transformation application");
        };
        let Some((tree_id, bucket_col_id)) = ({
            let output_expr = plan.get_expression_node(*output)?;
            let row_list = output_expr.get_row_list()?;
            let bucket_col = row_list.iter().find_map(|col_id| {
                let col_id = plan.get_child_under_alias(*col_id).ok()?;
                let expr = plan.get_expression_node(col_id).ok()?;
                let Expression::Reference(Reference { is_system, .. }) = expr else {
                    return None;
                };
                if *is_system {
                    Some(col_id)
                } else {
                    None
                }
            });
            bucket_col.map(|bucket_col| (*filter, bucket_col))
        }) else {
            // If this Selection output doesn't carry bucket_id as a system column,
            // we can't safely inject the predicate here.
            continue;
        };

        let new_id = add_condition_on_bucket_id_impl(plan, tree_id, bucket_col_id, buckets)?;

        let rel = plan.get_mut_relation_node(id)?;
        let MutRelational::Selection(Selection { filter, .. }) = rel else {
            unreachable!("Selection node expected for transformation application");
        };
        *filter = new_id;
    }
    Ok(())
}

/// Adds condition "bucket_id IN (...) AND" to `top_id`.
fn add_condition_on_bucket_id_impl(
    plan: &mut Plan,
    top_id: NodeId,
    first_col_ref: NodeId,
    buckets: &HashSet<u64, RepeatableState>,
) -> Result<NodeId, SbroadError> {
    let mut values = Vec::with_capacity(buckets.len());
    for b in buckets.iter().sorted() {
        let value_id = plan.add_const(Value::Integer(*b as i64));
        values.push(value_id);
    }
    // This row is created after distribution inference has already run.
    // Mark it explicitly to keep later bucket discovery from failing on
    // uninitialized row distribution.
    let values_row = plan.nodes.add_row(values, Some(Distribution::Any));

    let mut new_top_id = plan.add_cond(first_col_ref, Bool::In, values_row)?;
    new_top_id = plan.concat_and(new_top_id, top_id)?;

    Ok(new_top_id)
}

#[cfg(feature = "mock")]
#[cfg(test)]
mod tests;
