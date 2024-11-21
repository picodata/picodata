use std::collections::HashMap;
use std::rc::Rc;

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, ToSmolStr};

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::Vshard;
use crate::executor::vtable::{VirtualTable, VirtualTableMap};
use crate::ir::node::expression::ExprOwned;
use crate::ir::node::expression::{Expression, MutExpression};
use crate::ir::node::relational::{MutRelational, RelOwned, Relational};
use crate::ir::node::{
    Alias, ArenaType, ArithmeticExpr, BoolExpr, Case, Cast, Concat, Delete, ExprInParentheses,
    GroupBy, Having, Insert, Join, Like, Motion, Node, Node136, NodeId, NodeOwned, OrderBy,
    Reference, Row, ScanCte, ScanRelation, Selection, StableFunction, Trim, UnaryExpr, Update,
    ValuesRow,
};
use crate::ir::operator::{OrderByElement, OrderByEntity};
use crate::ir::relation::SpaceEngine;
use crate::ir::transformation::redistribution::{MotionOpcode, MotionPolicy};
use crate::ir::tree::traversal::{LevelNode, PostOrder};
use crate::ir::Plan;

/// Query type (used to parse the returned results).
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum QueryType {
    /// SELECT query.
    DQL,
    /// INSERT query.
    DML,
}

/// Connection type to the Tarantool instance.
#[derive(Debug)]
pub enum ConnectionType {
    Read,
    Write,
}

impl ConnectionType {
    #[must_use]
    pub fn is_readonly(&self) -> bool {
        match self {
            ConnectionType::Read => true,
            ConnectionType::Write => false,
        }
    }
}

/// Wrapper over `Plan` containing `vtables` map.
#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ExecutionPlan {
    plan: Plan,
    /// Virtual tables for `Motion` nodes.
    /// Map of { `Motion` node_id -> it's corresponding data }
    pub vtables: Option<VirtualTableMap>,
}

impl From<Plan> for ExecutionPlan {
    fn from(plan: Plan) -> Self {
        ExecutionPlan {
            plan,
            vtables: None,
        }
    }
}

/// Translates the original plan's node id to the new sub-plan one.
struct SubtreeMap {
    inner: AHashMap<NodeId, NodeId>,
}

impl SubtreeMap {
    fn with_capacity(capacity: usize) -> Self {
        SubtreeMap {
            inner: AHashMap::with_capacity(capacity),
        }
    }

    fn get_id(&self, expr_id: NodeId) -> NodeId {
        *self
            .inner
            .get(&expr_id)
            .unwrap_or_else(|| panic!("Could not find expr with id {expr_id:?} in subtree map"))
    }

    fn contains_key(&self, expr_id: NodeId) -> bool {
        self.inner.contains_key(&expr_id)
    }

    fn insert(&mut self, old_id: NodeId, new_id: NodeId) {
        self.inner.insert(old_id, new_id);
    }
}

impl ExecutionPlan {
    #[must_use]
    pub fn get_ir_plan(&self) -> &Plan {
        &self.plan
    }

    #[must_use]
    pub fn get_vtable_max_rows(&self) -> u64 {
        self.plan.options.vtable_max_rows
    }

    #[allow(dead_code)]
    pub fn get_mut_ir_plan(&mut self) -> &mut Plan {
        &mut self.plan
    }

    #[must_use]
    pub fn get_vtables(&self) -> Option<&HashMap<NodeId, Rc<VirtualTable>>> {
        self.vtables.as_ref().map(VirtualTableMap::map)
    }

    pub fn get_mut_vtables(&mut self) -> Option<&mut HashMap<NodeId, Rc<VirtualTable>>> {
        self.vtables.as_mut().map(VirtualTableMap::mut_map)
    }

    pub fn set_vtables(&mut self, vtables: HashMap<NodeId, Rc<VirtualTable>>) {
        self.vtables = Some(VirtualTableMap::new(vtables));
    }

    pub fn contains_vtable_for_motion(&self, motion_id: NodeId) -> bool {
        self.get_vtables()
            .map_or(false, |map| map.contains_key(&motion_id))
    }

    /// Get motion virtual table
    pub fn get_motion_vtable(&self, motion_id: NodeId) -> Result<Rc<VirtualTable>, SbroadError> {
        if let Some(vtable) = self.get_vtables() {
            if let Some(result) = vtable.get(&motion_id) {
                return Ok(Rc::clone(result));
            }
        }
        let motion_node = self.get_ir_plan().get_relation_node(motion_id)?;
        panic!("Virtual table for motion {motion_node:?} with id {motion_id} not found.")
    }

    /// Add materialize motion result to map of virtual tables.
    ///
    /// # Errors
    /// - invalid motion node
    pub fn set_motion_vtable(
        &mut self,
        motion_id: &NodeId,
        vtable: VirtualTable,
        runtime: &impl Vshard,
    ) -> Result<(), SbroadError> {
        let mut vtable = vtable;
        let program_len = if let Relational::Motion(Motion { program, .. }) =
            self.get_ir_plan().get_relation_node(*motion_id)?
        {
            program.0.len()
        } else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some("invalid motion node".into()),
            ));
        };
        for op_idx in 0..program_len {
            let plan = self.get_ir_plan();
            let opcode = plan.get_motion_opcode(*motion_id, op_idx)?;
            match opcode {
                MotionOpcode::RemoveDuplicates => {
                    vtable.remove_duplicates();
                }
                MotionOpcode::ReshardIfNeeded => {
                    // Resharding must be done before applying projection
                    // to the virtual table. Otherwise projection can
                    // remove sharding columns.
                    match plan.get_motion_policy(*motion_id)? {
                        MotionPolicy::Segment(shard_key)
                        | MotionPolicy::LocalSegment(shard_key) => {
                            vtable.reshard(shard_key, runtime)?;
                        }
                        MotionPolicy::Full | MotionPolicy::Local | MotionPolicy::None => {}
                    }
                }
                MotionOpcode::PrimaryKey(positions) => {
                    vtable.set_primary_key(positions)?;
                }
                MotionOpcode::RearrangeForShardedUpdate {
                    update_id,
                    old_shard_columns_len,
                    new_shard_columns_positions,
                } => {
                    let update_id = *update_id;

                    if let Some(v) = vtable.rearrange_for_update(
                        runtime,
                        *old_shard_columns_len,
                        new_shard_columns_positions,
                    )? {
                        let plan = self.get_mut_ir_plan();
                        plan.set_update_delete_tuple_len(update_id, v)?;
                    }
                }
                MotionOpcode::AddMissingRowsForLeftJoin { motion_id, .. } => {
                    let motion_id = *motion_id;
                    let Some(vtables) = &mut self.vtables else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "expected at least one virtual table".into(),
                        ));
                    };
                    let Some(from_vtable) = vtables.map().get(&motion_id) else {
                        return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                            "expected virtual table for motion {motion_id:?}"
                        )));
                    };
                    vtable.add_missing_rows(from_vtable)?;
                }
                MotionOpcode::SerializeAsEmptyTable(_) => {}
            }
        }

        let need_init = self.get_vtables().is_none();
        if need_init {
            self.set_vtables(HashMap::new());
        }

        if let Some(vtables) = self.get_mut_vtables() {
            vtables.insert(*motion_id, Rc::new(vtable));
        }

        Ok(())
    }

    #[must_use]
    pub fn has_segmented_tables(&self) -> bool {
        self.vtables.as_ref().map_or(false, |vtable_map| {
            vtable_map
                .map()
                .values()
                .any(|t| !t.get_bucket_index().is_empty())
        })
    }

    /// Return true if plan needs to be customized for each storage.
    /// I.e we can't send the same plan to all storages.
    ///
    /// The check is done by iterating over the plan nodes arena,
    /// and checking whether motion node contains serialize as empty
    /// opcode. Be sure there are no dead nodes in the plan arena:
    /// nodes that are not referenced by actual plan tree.
    #[must_use]
    pub fn has_customization_opcodes(&self) -> bool {
        for node in self.get_ir_plan().nodes.iter136() {
            if let Node136::Motion(Motion { program, .. }) = node {
                if program
                    .0
                    .iter()
                    .any(|op| matches!(op, MotionOpcode::SerializeAsEmptyTable(_)))
                {
                    return true;
                }
            }
        }
        false
    }

    /// Extract policy from motion node
    ///
    /// # Errors
    /// - node is not `Relation` type
    /// - node is not `Motion` type
    pub fn get_motion_policy(&self, node_id: NodeId) -> Result<MotionPolicy, SbroadError> {
        if let Relational::Motion(Motion { policy, .. }) = &self.plan.get_relation_node(node_id)? {
            return Ok(policy.clone());
        }

        Err(SbroadError::Invalid(
            Entity::Relational,
            Some("invalid motion".into()),
        ))
    }

    /// Get root from motion sub tree
    ///
    /// # Errors
    /// - node is not valid
    pub fn get_motion_subtree_root(&self, node_id: NodeId) -> Result<NodeId, SbroadError> {
        let top_id = &self.get_motion_child(node_id)?;
        let rel = self.get_ir_plan().get_relation_node(*top_id)?;
        match rel {
            Relational::ScanSubQuery { .. } | Relational::ScanCte { .. } => {
                self.get_ir_plan().get_relational_child(*top_id, 0)
            }
            Relational::Except { .. }
            | Relational::GroupBy { .. }
            | Relational::OrderBy { .. }
            | Relational::Intersect { .. }
            | Relational::Join { .. }
            | Relational::Projection { .. }
            | Relational::ScanRelation { .. }
            | Relational::Selection { .. }
            | Relational::SelectWithoutScan { .. }
            | Relational::Union { .. }
            | Relational::UnionAll { .. }
            | Relational::Update { .. }
            | Relational::Values { .. }
            | Relational::Having { .. }
            | Relational::ValuesRow { .. }
            | Relational::Limit { .. } => Ok(*top_id),
            Relational::Motion { .. } | Relational::Insert { .. } | Relational::Delete { .. } => {
                Err(SbroadError::Invalid(
                    Entity::Relational,
                    Some(format_smolstr!("invalid motion child node: {rel:?}.")),
                ))
            }
        }
    }

    /// Extract a child from the motion node. Motion node must contain only a single child.
    ///
    /// # Errors
    /// - node is not `Relation` type
    /// - node does not contain children
    pub(crate) fn get_motion_child(&self, node_id: NodeId) -> Result<NodeId, SbroadError> {
        let node = self.get_ir_plan().get_relation_node(node_id)?;
        if !node.is_motion() {
            return Err(SbroadError::Invalid(
                Entity::Relational,
                Some(format_smolstr!("current node ({node_id:?}) is not motion")),
            ));
        }

        let children = self.plan.get_relational_children(node_id)?;

        assert!(
            children.len() == 1,
            "Motion node ({node_id:?}:{node:?}) must have a single child only (actual {})",
            children.len()
        );

        let child_id = children.get(0).expect("Motion has no children");

        Ok(*child_id)
    }

    /// Unlink the subtree of the motion node.
    /// This logic is applied as an optimization: when we dispatch plan subtrees to the storages,
    /// we don't need to dispatch a subtrees of `Motion` nodes as soon as they are already replaced
    /// with vtables. So we replace its `children` field with an empty vec.
    ///
    /// # Errors
    /// - not a motion node
    pub fn unlink_motion_subtree(&mut self, motion_id: NodeId) -> Result<(), SbroadError> {
        let motion = self.get_mut_ir_plan().get_mut_relation_node(motion_id)?;
        if let MutRelational::Motion(Motion {
            ref mut children, ..
        }) = motion
        {
            *children = vec![];
        } else {
            return Err(SbroadError::Invalid(
                Entity::Relational,
                Some(format_smolstr!("node ({motion_id:?}) is not motion")),
            ));
        }
        Ok(())
    }

    /// Build a new execution plan from the subtree of the existing execution plan.
    /// The operation is destructive and the subtree is removed from the original plan.
    ///
    /// # Errors
    /// - the original execution plan is invalid
    ///
    /// # Panics
    /// - Plan is in invalid state
    #[allow(clippy::too_many_lines)]
    pub fn take_subtree(&mut self, top_id: NodeId) -> Result<Self, SbroadError> {
        // Get the subtree nodes indexes.
        let plan = self.get_ir_plan();
        let top = plan.get_top()?;
        let mut subtree =
            PostOrder::with_capacity(|node| plan.exec_plan_subtree_iter(node), plan.nodes.len());
        subtree.populate_nodes(top_id);
        let nodes = subtree.take_nodes();

        // We can't replace CTE subtree as it can be reused in other slices of the plan.
        // So, collect all CTE nodes and their subtree nodes (relational and expression)
        // as a set to avoid their removal.
        let cte_scans = nodes
            .iter()
            .map(|LevelNode(_, id)| *id)
            .filter(|id| {
                matches!(
                    plan.get_node(*id),
                    Ok(Node::Relational(Relational::ScanCte(_)))
                )
            })
            .collect::<Vec<_>>();

        // Get the capacity of the CTE nodes. We expect that CTE subtree nodes are located
        // in the beginning of the plan arena (while CTE scans can be located anywhere).
        // So, we get the biggest child id of the CTE nodes and add 1 to get the capacity.
        let mut all_cte_nodes_capacity = 0;
        let mut cte_amount = 0;
        for cte_id in &cte_scans {
            let cte_node = plan.get_relation_node(*cte_id)?;
            let Relational::ScanCte(ScanCte { child, .. }) = cte_node else {
                unreachable!("Expected CTE scan node.");
            };
            let child_id = *child;
            if all_cte_nodes_capacity < child_id.offset as usize {
                all_cte_nodes_capacity = child_id.offset as usize;
            }
            cte_amount += 1;
        }
        all_cte_nodes_capacity += 1;
        let single_cte_capacity = if cte_amount <= 1 {
            all_cte_nodes_capacity
        } else {
            all_cte_nodes_capacity / cte_amount * 2
        };

        let mut cte_ids: AHashSet<NodeId> = AHashSet::new();
        let mut is_reserved = false;
        for cte_id in cte_scans {
            if !is_reserved {
                is_reserved = true;
                cte_ids.reserve(all_cte_nodes_capacity);
            }
            let mut cte_subtree = PostOrder::with_capacity(
                |node| plan.exec_plan_subtree_iter(node),
                single_cte_capacity,
            );
            for LevelNode(_, id) in cte_subtree.iter(cte_id) {
                cte_ids.insert(id);
            }
        }

        // TODO: See note in `add_local_projection` function about SubQueries
        //       to understand why we don't want to replace SubQueries with stubs sometimes.
        //       The reason is that several relational nodes may point to it as a child.
        let sqs = nodes
            .iter()
            .map(|LevelNode(_, id)| *id)
            .filter(|id| {
                matches!(
                    plan.get_node(*id),
                    Ok(Node::Relational(Relational::ScanSubQuery(_)))
                )
            })
            .collect::<Vec<_>>();
        let mut sq_ids: AHashSet<NodeId> = AHashSet::new();
        for sq_id in sqs {
            let mut sq_subtree = PostOrder::with_capacity(
                |node| plan.exec_plan_subtree_iter(node),
                plan.nodes.len(),
            );
            for LevelNode(_, id) in sq_subtree.iter(sq_id) {
                sq_ids.insert(id);
            }
        }

        let mut subtree_map = SubtreeMap::with_capacity(nodes.len());
        let vtables_capacity = self.get_vtables().map_or_else(|| 1, HashMap::len);
        // Map of { plan node_id -> virtual table }.
        let mut new_vtables: HashMap<NodeId, Rc<VirtualTable>> =
            HashMap::with_capacity(vtables_capacity);

        let mut new_plan = Plan::new();

        // In case we have a Motion among rel node children (maybe not direct), we
        // need to rename rel output aliases, because Motion
        // may have changed them according to its vtable column names.
        // This map tracks outputs of rel nodes that have changed their aliases.
        let mut rel_renamed_output_lists: HashMap<NodeId, Vec<NodeId>> = HashMap::new();

        for LevelNode(_, node_id) in nodes {
            // We have already processed this node (sub-queries in BETWEEN
            // and CTEs can be referred twice).
            if subtree_map.contains_key(node_id) {
                continue;
            }

            let mut_plan = self.get_mut_ir_plan();

            // Replace the node with some invalid value.
            let node = mut_plan.get_node(node_id)?;
            let mut node: NodeOwned = if cte_ids.contains(&node_id) || sq_ids.contains(&node_id) {
                node.into_owned()
            } else {
                mut_plan.replace_with_stub(node_id)
            };

            let mut relational_output_id: Option<NodeId> = None;
            let ir_plan = self.get_ir_plan();
            match node {
                NodeOwned::Relational(ref mut rel) => {
                    match rel {
                        RelOwned::Selection(Selection {
                            filter: ref mut expr_id,
                            ..
                        })
                        | RelOwned::Having(Having {
                            filter: ref mut expr_id,
                            ..
                        })
                        | RelOwned::Join(Join {
                            condition: ref mut expr_id,
                            ..
                        }) => {
                            let next_id = new_plan.nodes.next_id(ArenaType::Arena64);
                            // We transform selection's, having's filter and join's condition to DNF for a better bucket calculation.
                            // But as a result we can produce an extremely verbose SQL query from such a plan (tarantool's
                            // parser can fail to parse such SQL).

                            // XXX: UNDO operation can cause problems if we introduce more complicated transformations
                            // for filter/condition (but then the UNDO logic should be changed as well).
                            let undo_expr_id = ir_plan.undo.get_oldest(expr_id).unwrap_or(expr_id);
                            *expr_id = subtree_map.get_id(*undo_expr_id);
                            new_plan.replace_parent_in_subtree(*expr_id, None, Some(next_id))?;
                        }
                        RelOwned::ScanRelation(ScanRelation { relation, .. })
                        | RelOwned::Insert(Insert { relation, .. })
                        | RelOwned::Delete(Delete { relation, .. })
                        | RelOwned::Update(Update { relation, .. }) => {
                            let table = ir_plan
                                .relations
                                .get(relation)
                                .unwrap_or_else(|| {
                                    panic!(
                                        "could not find relation {relation} in the original plan"
                                    )
                                })
                                .clone();
                            new_plan.add_rel(table);
                        }
                        RelOwned::Motion(Motion {
                            children,
                            policy,
                            output,
                            ..
                        }) => {
                            if let Some(vtable) =
                                self.get_vtables().map_or_else(|| None, |v| v.get(&node_id))
                            {
                                let next_id = new_plan.nodes.next_id(ArenaType::Arena136);
                                new_vtables.insert(next_id, Rc::clone(vtable));

                                // We need to fix motion aliases based on materialized
                                // vtable. Motion's aliases will copy "COL_i" naming of
                                // vtable.
                                let output_list: Vec<NodeId> =
                                    new_plan.get_row_list(subtree_map.get_id(*output))?.to_vec();

                                if node_id != top {
                                    // We should rename Motion aliases only in case it's not a
                                    // top node. Otherwise, it has no effect as soon as `to_sql`
                                    // will use column names of vtable and ignore motion aliases.
                                    vtable
                                        .get_columns().iter()
                                        .zip(output_list.iter())
                                        .map(|(vtable_column, alias_id)| {
                                            let alias = new_plan.get_mut_expression_node(
                                                *alias_id
                                            )?;
                                            let MutExpression::Alias(Alias { ref mut name, .. }) = alias else {
                                                return Err(SbroadError::Invalid(
                                                    Entity::Expression,
                                                    Some(format_smolstr!("Expected Alias under Motion output, got {alias:?}"))
                                                ))
                                            };
                                            *name = vtable_column.name.clone();
                                            Ok(())
                                        })
                                        .collect::<Result<Vec<()>, SbroadError>>()?;
                                }
                            }
                            // We should not remove the child of a local motion node.
                            // The subtree is needed to compile the SQL on the storage.
                            if !policy.is_local() {
                                *children = Vec::new();
                            }
                        }
                        RelOwned::GroupBy(GroupBy { gr_cols, .. }) => {
                            let next_id = new_plan.nodes.next_id(ArenaType::Arena64);
                            let mut new_cols: Vec<NodeId> = Vec::with_capacity(gr_cols.len());
                            for col_id in gr_cols.iter() {
                                let new_col_id = subtree_map.get_id(*col_id);
                                new_plan.replace_parent_in_subtree(
                                    new_col_id,
                                    None,
                                    Some(next_id),
                                )?;
                                new_cols.push(new_col_id);
                            }
                            *gr_cols = new_cols;
                        }
                        RelOwned::OrderBy(OrderBy {
                            order_by_elements, ..
                        }) => {
                            let next_id = new_plan.nodes.next_id(ArenaType::Arena64);
                            let mut new_elements: Vec<OrderByElement> =
                                Vec::with_capacity(order_by_elements.len());
                            for element in order_by_elements.iter() {
                                let new_entity = match element.entity {
                                    OrderByEntity::Expression { expr_id } => {
                                        let new_element_id = subtree_map.get_id(expr_id);
                                        new_plan.replace_parent_in_subtree(
                                            new_element_id,
                                            None,
                                            Some(next_id),
                                        )?;
                                        OrderByEntity::Expression {
                                            expr_id: new_element_id,
                                        }
                                    }
                                    OrderByEntity::Index { value } => {
                                        OrderByEntity::Index { value }
                                    }
                                };
                                new_elements.push(OrderByElement {
                                    entity: new_entity,
                                    order_type: element.order_type.clone(),
                                });
                            }
                            *order_by_elements = new_elements;
                        }
                        RelOwned::ValuesRow(ValuesRow { data, .. }) => {
                            *data = subtree_map.get_id(*data);
                        }
                        RelOwned::Except { .. }
                        | RelOwned::Intersect { .. }
                        | RelOwned::Projection { .. }
                        | RelOwned::SelectWithoutScan { .. }
                        | RelOwned::ScanSubQuery { .. }
                        | RelOwned::ScanCte { .. }
                        | RelOwned::Union { .. }
                        | RelOwned::UnionAll { .. }
                        | RelOwned::Values { .. }
                        | RelOwned::Limit { .. } => {}
                    }

                    for child_id in rel.mut_children() {
                        *child_id = subtree_map.get_id(*child_id);

                        let child_rel_node = new_plan.get_relation_node(*child_id)?;
                        if let Relational::Motion(Motion { output, .. }) = child_rel_node {
                            let motion_output_list: Vec<NodeId> =
                                new_plan.get_row_list(*output)?.to_vec();
                            rel_renamed_output_lists.insert(*child_id, motion_output_list);
                        }
                    }

                    if rel.has_output() {
                        let output = rel.mut_output();
                        *rel.mut_output() = subtree_map.get_id(*output);
                        relational_output_id = Some(*rel.mut_output());

                        // If we deal with Projection we have to fix
                        // only References that have an Asterisk source.
                        // References without asterisks would be covered with aliases like
                        // "COL_1 as <alias>".
                        let is_projection = matches!(rel, RelOwned::Projection(_));
                        if !rel_renamed_output_lists.is_empty() {
                            let rel_output_list: Vec<NodeId> =
                                new_plan.get_row_list(*rel.mut_output())?.to_vec();

                            for output_id in &rel_output_list {
                                let ref_under_alias = new_plan.get_child_under_alias(*output_id)?;
                                let ref_expr = new_plan.get_expression_node(ref_under_alias)?;
                                let Expression::Reference(Reference {
                                    position,
                                    targets,
                                    asterisk_source,
                                    ..
                                }) = ref_expr
                                else {
                                    continue;
                                };

                                if is_projection && asterisk_source.is_none() {
                                    continue;
                                }

                                let mut ref_rel_node = None;

                                let target = if let Some(targets) = targets {
                                    *targets
                                        .first()
                                        .expect("Reference must have at least one target.")
                                } else {
                                    break;
                                };
                                for (index, child) in rel.children().iter().enumerate() {
                                    if index != target {
                                        continue;
                                    }
                                    ref_rel_node = Some(*child);
                                }
                                let Some(ref_rel_node) = ref_rel_node else {
                                    continue;
                                };

                                if let Some(child_output_list) =
                                    rel_renamed_output_lists.get(&ref_rel_node)
                                {
                                    let child_output_alias_id =
                                        child_output_list.get(*position).unwrap_or_else(|| {
                                            panic!(
                                                "Unable to get motion output at requested position."
                                            );
                                        });
                                    let child_alias =
                                        new_plan.get_expression_node(*child_output_alias_id)?;
                                    let child_alias_name =
                                        child_alias.get_alias_name()?.to_smolstr();

                                    let rel_alias = new_plan.get_mut_expression_node(*output_id)?;
                                    if let MutExpression::Alias(Alias { ref mut name, .. }) =
                                        rel_alias
                                    {
                                        *name = child_alias_name;
                                    } else {
                                        panic!("Expected alias under Row output list");
                                    }
                                }
                            }

                            let arena_type = rel.arena_type();
                            let next_id = new_plan.nodes.next_id(arena_type);

                            rel_renamed_output_lists.insert(next_id, rel_output_list);
                        }
                    }
                }
                NodeOwned::Expression(ref mut expr) => match expr {
                    ExprOwned::Alias(Alias { ref mut child, .. })
                    | ExprOwned::ExprInParentheses(ExprInParentheses { ref mut child })
                    | ExprOwned::Cast(Cast { ref mut child, .. })
                    | ExprOwned::Unary(UnaryExpr { ref mut child, .. }) => {
                        *child = subtree_map.get_id(*child);
                    }
                    ExprOwned::Bool(BoolExpr {
                        ref mut left,
                        ref mut right,
                        ..
                    })
                    | ExprOwned::Arithmetic(ArithmeticExpr {
                        ref mut left,
                        ref mut right,
                        ..
                    })
                    | ExprOwned::Concat(Concat {
                        ref mut left,
                        ref mut right,
                        ..
                    }) => {
                        *left = subtree_map.get_id(*left);
                        *right = subtree_map.get_id(*right);
                    }
                    ExprOwned::Like(Like {
                        escape: ref mut escape_id,
                        ref mut right,
                        ref mut left,
                    }) => {
                        *left = subtree_map.get_id(*left);
                        *right = subtree_map.get_id(*right);
                        *escape_id = subtree_map.get_id(*escape_id);
                    }
                    ExprOwned::Trim(Trim {
                        ref mut pattern,
                        ref mut target,
                        ..
                    }) => {
                        if let Some(pattern) = pattern {
                            *pattern = subtree_map.get_id(*pattern);
                        }
                        *target = subtree_map.get_id(*target);
                    }
                    ExprOwned::Reference(Reference { ref mut parent, .. }) => {
                        // The new parent node id MUST be set while processing the relational nodes.
                        *parent = None;
                    }
                    ExprOwned::Row(Row {
                        list: ref mut children,
                        ..
                    })
                    | ExprOwned::StableFunction(StableFunction {
                        ref mut children, ..
                    }) => {
                        for child in children {
                            *child = subtree_map.get_id(*child);
                        }
                    }
                    ExprOwned::Constant { .. } | ExprOwned::CountAsterisk { .. } => {}
                    ExprOwned::Case(Case {
                        search_expr,
                        when_blocks,
                        else_expr,
                    }) => {
                        if let Some(search_expr) = search_expr {
                            *search_expr = subtree_map.get_id(*search_expr);
                        }
                        for (cond_expr, res_expr) in when_blocks {
                            *cond_expr = subtree_map.get_id(*cond_expr);
                            *res_expr = subtree_map.get_id(*res_expr);
                        }
                        if let Some(else_expr) = else_expr {
                            *else_expr = subtree_map.get_id(*else_expr);
                        }
                    }
                },
                NodeOwned::Parameter { .. } => {}
                NodeOwned::Invalid { .. }
                | NodeOwned::Ddl { .. }
                | NodeOwned::Acl { .. }
                | NodeOwned::Plugin { .. }
                | NodeOwned::Block { .. } => {
                    panic!("Unexpected node in `take_subtree`: {node:?}")
                }
            }

            let id = new_plan.nodes.push(node.into());
            if let Some(output_id) = relational_output_id {
                new_plan.replace_parent_in_subtree(output_id, None, Some(id))?;
            }

            subtree_map.insert(node_id, id);
            if top_id == node_id {
                new_plan.set_top(id)?;
            }
        }

        new_plan.stash_constants()?;
        new_plan.options = self.get_ir_plan().options.clone();
        new_plan.tier.clone_from(&self.get_ir_plan().tier);

        let vtables = if new_vtables.is_empty() {
            None
        } else {
            Some(VirtualTableMap::new(new_vtables))
        };
        let new_exec_plan = ExecutionPlan {
            plan: new_plan,
            vtables,
        };
        Ok(new_exec_plan)
    }
    /// # Errors
    /// - execution plan is invalid
    pub fn query_type(&self) -> Result<QueryType, SbroadError> {
        let top_id = self.get_ir_plan().get_top()?;
        let top = self.get_ir_plan().get_relation_node(top_id)?;
        if top.is_dml() {
            Ok(QueryType::DML)
        } else {
            Ok(QueryType::DQL)
        }
    }

    pub fn vtables_empty(&self) -> bool {
        self.get_vtables().map_or(true, HashMap::is_empty)
    }

    /// Calculates an engine for the virtual tables in the plan.
    /// The main problem is that we can't use different engines
    /// for an SQL statement within transaction.
    ///
    /// # Errors
    /// - execution plan is invalid
    /// - multiple engines are used in the plan
    pub fn vtable_engine(&self) -> Result<SpaceEngine, SbroadError> {
        let mut engine: Option<SpaceEngine> = None;
        for table in self.get_ir_plan().relations.tables.values() {
            let table_engine = table.engine();
            if engine.is_none() {
                engine = Some(table_engine);
            } else if engine != Some(table_engine) {
                return Err(SbroadError::FailedTo(
                    Action::Build,
                    Some(Entity::Plan),
                    format_smolstr!(
                        "cannot build execution plan for DML query with multiple engines: {:?}",
                        self.get_ir_plan().relations.tables
                    ),
                ));
            }
        }
        Ok(engine.map_or_else(|| SpaceEngine::Memtx, |e| e))
    }

    /// # Errors
    /// - execution plan is invalid
    pub fn connection_type(&self) -> Result<ConnectionType, SbroadError> {
        match self.query_type()? {
            QueryType::DML => Ok(ConnectionType::Write),
            QueryType::DQL => {
                if self.vtables_empty() {
                    Ok(ConnectionType::Read)
                } else {
                    Ok(ConnectionType::Write)
                }
            }
        }
    }
}
