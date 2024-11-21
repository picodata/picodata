use crate::errors::{Entity, SbroadError};
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{Delete, Insert, Motion, NodeId, Update};
use crate::ir::operator::{ConflictStrategy, UpdateStrategy};
use crate::ir::relation::{Column, Table};
use crate::ir::transformation::redistribution::MotionOpcode;
use crate::ir::Plan;
use ahash::AHashMap;
use smol_str::format_smolstr;

use super::{MotionKey, Target};

impl Plan {
    /// Return first child of `Insert` node
    pub fn dml_child_id(&self, dml_node_id: NodeId) -> Result<NodeId, SbroadError> {
        let dml_node = self.get_relation_node(dml_node_id)?;
        if let Relational::Insert(Insert { children, .. })
        | Relational::Update(Update { children, .. })
        | Relational::Delete(Delete { children, .. }) = dml_node
        {
            if let (Some(child), None) = (children.first(), children.get(1)) {
                return Ok(*child);
            }
            panic!("DML node must have exactly a single child node.");
        }
        panic!("Expected DML node to get child from. Got {dml_node:?}.");
    }

    /// Return `ConflictStrategy` for given insert node
    ///
    /// # Errors
    /// - node is not an `Insert`
    pub fn insert_conflict_strategy(
        &self,
        insert_id: NodeId,
    ) -> Result<&ConflictStrategy, SbroadError> {
        let insert = self.get_relation_node(insert_id)?;
        if let Relational::Insert(Insert {
            conflict_strategy, ..
        }) = insert
        {
            return Ok(conflict_strategy);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!(
                "INSERT with id {insert_id:?} (conflict strategy))"
            )),
        ))
    }

    pub(crate) fn insert_motion_key(&self, insert_id: NodeId) -> Result<MotionKey, SbroadError> {
        let columns = self.insert_columns(insert_id)?;
        // Revert map of { pos_in_child_node -> pos_in_relation }
        // into map of { pos_in_relation -> pos_in_child_node }.
        let columns_map: AHashMap<usize, usize> = columns
            .iter()
            .enumerate()
            .map(|(pos, id)| (*id, pos))
            .collect::<AHashMap<_, _>>();

        // Check that the INSERT columns have the same length as the child node's
        // output columns.
        let child_id = self.dml_child_id(insert_id)?;
        let child_output_id = self.get_relation_node(child_id)?.output();
        let child_row = self.get_row_list(child_output_id)?;
        if columns.len() != child_row.len() {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!(
                    "INSERT with id {:?} has {} columns, but the child node with id {:?} has {}",
                    insert_id,
                    columns.len(),
                    child_id,
                    child_row.len()
                )),
            ));
        }

        let mut motion_key: MotionKey = MotionKey::new();
        let table = self.dml_node_table(insert_id)?;
        for pos in table.get_sk()? {
            if let Some(child_pos) = columns_map.get(pos) {
                // We can use insert column's position instead of
                // the position in the child node as their lengths
                // are the same.
                motion_key.targets.push(Target::Reference(*child_pos));
            } else {
                // Check that the column exists on the requested position.
                table.columns.get(*pos).ok_or_else(|| {
                    SbroadError::NotFound(
                        Entity::Column,
                        format_smolstr!("in the table {table:?} at position {pos}"),
                    )
                })?;
                // We need a default value for the key column.
                motion_key
                    .targets
                    .push(Target::Value(Column::default_value()));
            }
        }
        Ok(motion_key)
    }

    /// Return ids of `Insert` columns
    ///
    /// # Errors
    /// - node is not `Insert`
    pub(crate) fn insert_columns(&self, insert_id: NodeId) -> Result<&[usize], SbroadError> {
        let insert = self.get_relation_node(insert_id)?;
        if let Relational::Insert(Insert { ref columns, .. }) = insert {
            return Ok(columns);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!("expected insert node on id {insert_id:?}")),
        ))
    }

    /// Return the table for given `Insert` node
    pub fn dml_node_table(&self, node_id: NodeId) -> Result<&Table, SbroadError> {
        let node = self.get_relation_node(node_id)?;
        if let Relational::Insert(Insert { relation, .. })
        | Relational::Update(Update { relation, .. })
        | Relational::Delete(Delete { relation, .. }) = node
        {
            return self.get_relation_or_error(relation);
        }
        panic!("Expected DML node to get table from. Got {node:?}.")
    }

    /// Set the length of tuple to delete for
    /// sharded `Update`.
    ///
    /// # Errors
    /// - Node is not an sharded `Update`
    pub fn set_update_delete_tuple_len(
        &mut self,
        update_id: NodeId,
        len: usize,
    ) -> Result<(), SbroadError> {
        let node = self.get_mut_relation_node(update_id)?;
        if let MutRelational::Update(Update {
            strategy:
                UpdateStrategy::ShardedUpdate {
                    delete_tuple_len, ..
                },
            ..
        }) = node
        {
            *delete_tuple_len = Some(len);
        } else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("expected sharded Update, got: {node:?}")),
            ));
        }
        Ok(())
    }

    /// Return the length of tuple to delete for
    /// sharded `Update`.
    ///
    /// # Errors
    /// - Node is not an sharded `Update`
    /// - length not set on current `Update` node
    pub fn get_update_delete_tuple_len(&self, update_id: NodeId) -> Result<usize, SbroadError> {
        let node = self.get_relation_node(update_id)?;
        if let Relational::Update(Update {
            strategy:
                UpdateStrategy::ShardedUpdate {
                    delete_tuple_len: Some(len),
                    ..
                },
            ..
        }) = node
        {
            return Ok(*len);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!(
                "expected sharded Update with delete len, got: {node:?}"
            )),
        ))
    }

    /// Get `Motion`'s node opcode with given index
    ///
    /// # Errors
    /// - node is not motion
    /// - invalid index
    pub fn get_motion_opcode(
        &self,
        motion_id: NodeId,
        opcode_idx: usize,
    ) -> Result<&MotionOpcode, SbroadError> {
        let node = self.get_relation_node(motion_id)?;
        if let Relational::Motion(Motion { program, .. }) = node {
            if let Some(op) = program.0.get(opcode_idx) {
                return Ok(op);
            }
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "Invalid motion opcode index: {opcode_idx}"
            )));
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!("expected Motion, got: {node:?}")),
        ))
    }

    /// Return true if `Update` updates
    /// some column of sharding key.
    ///
    /// # Errors
    /// - Node is not an `Update`
    pub fn is_sharded_update(&self, update_id: NodeId) -> Result<bool, SbroadError> {
        let node = self.get_relation_node(update_id)?;
        if let Relational::Update(Update { strategy, .. }) = node {
            return Ok(matches!(strategy, UpdateStrategy::ShardedUpdate { .. }));
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!("expected Update node, got: {node:?}")),
        ))
    }
}
