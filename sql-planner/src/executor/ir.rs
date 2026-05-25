use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use ahash::{AHashMap, AHashSet};
use itertools::Itertools;
use smol_str::{format_smolstr, SmolStr};

use sql_dynfilter::DynamicFilter;
use sql_protocol::dql_encoder::ApplySpec;

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::Vshard;
use crate::executor::vtable::{VirtualTable, VirtualTableMap};
use crate::executor::Buckets;
use crate::ir::bucket::BucketSet;
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::Relational;
use crate::ir::node::{Alias, Motion, Node, Node136, NodeId, Reference, SubQueryReference, Update};
use crate::ir::operator::UpdateStrategy;
use crate::ir::relation::SpaceEngine;
use crate::ir::transformation::redistribution::{MotionOpcode, MotionPolicy, Program};
use crate::ir::tree::subtree::ExecPlanSubtreeIterator;
use crate::ir::tree::traversal::{LevelNode, PostOrder, PostOrderWithFilter};
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;
use crate::ir::{Plan, SubtreeViewKey};

/// Query type (used to parse the returned results).
#[derive(Debug, PartialEq, Eq, Clone)]
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

/// Effective state of a motion's `SerializeAsEmptyTable` opcode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SerializeAsEmptyState {
    /// The motion program has no `SerializeAsEmptyTable` opcode.
    Absent,
    /// The opcode is present and effective SQL must render an empty table.
    Enabled,
    /// The opcode is present but disabled by its value or an execution overlay.
    Disabled,
}

impl SerializeAsEmptyState {
    /// Returns whether effective SQL must render the motion as an empty table.
    pub(crate) fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }

    /// Returns whether the opcode is present but effectively disabled.
    pub(crate) fn is_disabled(self) -> bool {
        matches!(self, Self::Disabled)
    }
}

/// Wrapper over `Plan` containing `vtables` map.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExecutionPlan {
    request_id: String,
    /// IR plan contains motions.
    pub(crate) plan: Plan,
    /// Virtual tables for `Motion` nodes.
    /// Map of { `Motion` node_id -> it's corresponding data }
    vtables: VirtualTableMap,
    /// It is calculated via `set_plan_id` during `materialize_motion`.
    pub(crate) plan_id: Option<u64>,
    /// Plan id cache for motion subtrees during the current execution.
    pub(crate) plan_id_cache: AHashMap<NodeId, u64>,
    /// SQL parameter count cache for motion subtrees during the current execution.
    plan_id_sql_parameter_count_cache: AHashMap<NodeId, usize>,
    /// SQL parameter count for the currently selected `plan_id`.
    plan_id_sql_parameter_count: Option<usize>,
    /// Delete tuple lengths calculated for sharded updates during execution.
    update_delete_tuple_lens: AHashMap<NodeId, usize>,
    /// Read-only bucket predicate overlay for local SQL.
    ///
    /// Used for sharded tables whose primary key starts with `bucket_id`.
    /// The generated `bucket_id IN (...)` predicate lets the local SQL planner
    /// use the primary key without mutating the IR plan.
    bucket_filter: Option<BucketFilter>,
    /// Read-only override for `SerializeAsEmptyTable`.
    ///
    /// Used by per-replicaset customization to render selected motions as if
    /// the opcode were disabled without mutating the IR plan.
    serialize_as_empty_disabled_motions: HashSet<NodeId>,
    /// Read-only overlay for motions whose subtree has already been materialized.
    ///
    /// Such motions are rendered and traversed as leaves without mutating the IR
    /// motion node.
    unlinked_motions: HashSet<NodeId>,
    /// Derived cache for effective subtree metadata during this execution.
    subtree_view_cache: SubtreeViewCache,
    /// Dynamic-filter map keyed by `filter_id`.
    ///
    /// Each entry holds the build-side `DynamicFilter` produced by the §5.4
    /// executor pass and the expected `keys.len()` from the matching
    /// `ApplyFilter` (used to validate the wire-decoded filter on the probe
    /// side). The map lives only for the duration of an execution and is
    /// intentionally NOT part of the plan-cache identity.
    pub filter_state: AHashMap<u32, FilterState>,
}

/// Build-side state for a single dynamic filter (`filter_id`).
///
/// `apply_spec` is captured at build time from the matching `ApplyFilter`
/// IR node so the DQL packet encoder can ship it to the storage without
/// re-walking the IR. Positions inside `apply_spec` are storage-row
/// indices (per `ApplyFilter.keys[i].position`, which references the
/// storage subtree's output schema).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterState {
    pub filter: DynamicFilter,
    pub key_arity: usize,
    pub apply_spec: ApplySpec,
}

/// Read-only execution plan borrow.
///
/// While this value exists, Rust's borrow checker prevents mutable access to
/// the underlying `ExecutionPlan`.
#[derive(Clone, Copy, Debug)]
pub struct FrozenPlan<'plan> {
    exec_plan: &'plan ExecutionPlan,
}

impl<'plan> FrozenPlan<'plan> {
    /// Creates a read-only view over the frozen execution plan.
    ///
    /// The returned view keeps the same borrow lifetime and can be narrowed to
    /// a DQL subtree without cloning or mutating the plan.
    #[must_use]
    pub fn execution_view(self) -> ExecutionView<'plan> {
        ExecutionView { frozen: self }
    }
}

/// Read-only execution plan view available after the plan is frozen.
#[derive(Debug)]
pub struct ExecutionView<'plan> {
    frozen: FrozenPlan<'plan>,
}

impl<'plan> ExecutionView<'plan> {
    /// Narrows the read-only view to a DQL subtree rooted at `sql_top_id`.
    ///
    /// # Errors
    /// - If `sql_top_id` does not reference a node in the underlying plan.
    pub fn dql_subtree(self, sql_top_id: NodeId) -> Result<DqlSubtree<'plan>, SbroadError> {
        self.get_ir_plan().get_node(sql_top_id)?;
        Ok(DqlSubtree {
            view: self,
            sql_top_id,
        })
    }

    /// Returns the execution plan behind this read-only view.
    #[must_use]
    pub(crate) fn as_execution_plan(&self) -> &'plan ExecutionPlan {
        self.frozen.exec_plan
    }

    /// Returns the IR plan behind this read-only view.
    #[must_use]
    pub(crate) fn get_ir_plan(&self) -> &'plan Plan {
        self.as_execution_plan().get_ir_plan()
    }
}

/// Read-only DQL subtree view available after the plan is frozen.
#[derive(Debug)]
pub struct DqlSubtree<'plan> {
    view: ExecutionView<'plan>,
    sql_top_id: NodeId,
}

impl<'plan> DqlSubtree<'plan> {
    /// Returns the node id used as the SQL root for this subtree.
    #[must_use]
    pub(crate) fn sql_top_id(&self) -> NodeId {
        self.sql_top_id
    }

    /// Returns the execution plan behind this DQL subtree view.
    #[must_use]
    pub(crate) fn as_execution_plan(&self) -> &'plan ExecutionPlan {
        self.view.as_execution_plan()
    }

    /// Returns the IR plan behind this DQL subtree view.
    #[must_use]
    pub(crate) fn get_ir_plan(&self) -> &'plan Plan {
        self.view.get_ir_plan()
    }

    fn effective_reference_alias(&self, expr: &Expression) -> Result<SmolStr, SbroadError> {
        let plan = self.get_ir_plan();
        let Some((rel_id, position)) = reference_target(expr) else {
            return Ok(SmolStr::from(plan.get_alias_from_reference_node(expr)?));
        };
        if let Some(alias) = self.materialized_motion_alias(rel_id, position)? {
            return Ok(alias);
        }
        if let Some(alias) = self.renamed_output_alias(rel_id, position, &mut AHashSet::new())? {
            return Ok(alias);
        }
        Ok(SmolStr::from(plan.get_alias_from_reference_node(expr)?))
    }

    fn renamed_output_alias(
        &self,
        rel_id: NodeId,
        position: usize,
        visited: &mut AHashSet<NodeId>,
    ) -> Result<Option<SmolStr>, SbroadError> {
        if !visited.insert(rel_id) {
            return Ok(None);
        }

        let plan = self.get_ir_plan();
        let rel = plan.get_relation_node(rel_id)?;
        let output = rel.output();
        let output_list = plan.get_row_list(output)?;
        let Some(alias_id) = output_list.get(position) else {
            return Ok(None);
        };
        let Expression::Alias(Alias { child, .. }) = plan.get_expression_node(*alias_id)? else {
            return Ok(None);
        };
        let Expression::Reference(Reference {
            target,
            position,
            asterisk_source,
            ..
        }) = plan.get_expression_node(*child)?
        else {
            return Ok(None);
        };
        if matches!(rel, Relational::Projection(_)) && asterisk_source.is_none() {
            return Ok(None);
        }

        let Some(child_rel_id) = target.first() else {
            return Ok(None);
        };
        if let Some(alias) = self.materialized_motion_alias(*child_rel_id, *position)? {
            return Ok(Some(alias));
        }
        self.renamed_output_alias(*child_rel_id, *position, visited)
    }

    fn materialized_motion_alias(
        &self,
        motion_id: NodeId,
        position: usize,
    ) -> Result<Option<SmolStr>, SbroadError> {
        if !self.contains_vtable_for_motion(motion_id) {
            return Ok(None);
        }
        if !self.get_ir_plan().get_relation_node(motion_id)?.is_motion() {
            return Ok(None);
        }
        let vtable = self.get_motion_vtable(motion_id)?;
        let Some(column) = vtable.get_columns().get(position) else {
            return Err(SbroadError::NotFound(
                Entity::Name,
                format_smolstr!("for column at position {position}"),
            ));
        };
        Ok(Some(column.name.clone()))
    }
}

fn reference_target(expr: &Expression) -> Option<(NodeId, usize)> {
    match expr {
        Expression::Reference(Reference {
            target, position, ..
        }) => target.first().map(|rel_id| (*rel_id, *position)),
        Expression::SubQueryReference(SubQueryReference {
            rel_id, position, ..
        }) => Some((*rel_id, *position)),
        _ => None,
    }
}

/// Common SQL rendering interface for a full plan or a read-only subtree view.
pub(crate) trait SqlExecutionView: std::fmt::Debug {
    /// Returns the underlying execution plan used for shared metadata access.
    fn as_execution_plan(&self) -> &ExecutionPlan;

    /// Returns the IR plan visible through this view.
    fn get_ir_plan(&self) -> &Plan {
        self.as_execution_plan().get_ir_plan()
    }

    /// Returns virtual tables visible through this view.
    fn get_vtables(&self) -> &VirtualTableMap {
        self.as_execution_plan().get_vtables()
    }

    /// Returns the virtual table materialized for `motion_id`.
    fn get_motion_vtable(&self, motion_id: NodeId) -> Result<Rc<VirtualTable>, SbroadError> {
        self.as_execution_plan().get_motion_vtable(motion_id)
    }

    /// Returns whether `motion_id` has a materialized virtual table.
    fn contains_vtable_for_motion(&self, motion_id: NodeId) -> bool {
        self.as_execution_plan()
            .contains_vtable_for_motion(motion_id)
    }

    /// Resolves a reference alias as it should be rendered in SQL.
    fn reference_alias(&self, expr: &Expression) -> Result<SmolStr, SbroadError> {
        Ok(SmolStr::from(
            self.get_ir_plan().get_alias_from_reference_node(expr)?,
        ))
    }

    /// Returns the synthetic `bucket_id` reference for a selection node.
    fn get_bucket_ref(&self, selection_id: NodeId) -> Option<NodeId> {
        self.as_execution_plan().get_bucket_ref(selection_id)
    }

    /// Returns bucket ids appended as SQL parameters by the active bucket filter.
    fn get_bucket_filter_ids(&self) -> Option<&[u64]> {
        self.as_execution_plan().get_bucket_filter_ids()
    }

    /// Returns the effective child of a motion after execution overlays.
    fn effective_motion_child(&self, motion_id: NodeId) -> Result<Option<NodeId>, SbroadError> {
        self.as_execution_plan().effective_motion_child(motion_id)
    }

    /// Returns the output node if the motion is rendered as a leaf in this view.
    fn effective_motion_leaf_output(&self, node_id: NodeId) -> Option<NodeId> {
        self.as_execution_plan()
            .effective_motion_leaf_output(node_id)
    }

    /// Returns the effective `SerializeAsEmptyTable` state for a motion.
    fn effective_serialize_as_empty_state(
        &self,
        motion_id: NodeId,
        program: &Program,
    ) -> SerializeAsEmptyState {
        self.as_execution_plan()
            .effective_serialize_as_empty_state(motion_id, program)
    }
}

impl SqlExecutionView for ExecutionPlan {
    fn as_execution_plan(&self) -> &ExecutionPlan {
        self
    }
}

impl SqlExecutionView for ExecutionView<'_> {
    fn as_execution_plan(&self) -> &ExecutionPlan {
        ExecutionView::as_execution_plan(self)
    }
}

impl SqlExecutionView for DqlSubtree<'_> {
    fn as_execution_plan(&self) -> &ExecutionPlan {
        DqlSubtree::as_execution_plan(self)
    }

    fn reference_alias(&self, expr: &Expression) -> Result<SmolStr, SbroadError> {
        self.effective_reference_alias(expr)
    }
}

/// Iterator over an effective execution subtree.
///
/// It either yields a single replacement leaf or delegates to the normal IR
/// subtree iterator.
pub(crate) enum EffectiveExecPlanSubtreeIterator<'plan> {
    /// A subtree collapsed to one effective leaf node.
    Once(std::iter::Once<NodeId>),
    /// A regular IR execution subtree traversal.
    Subtree(ExecPlanSubtreeIterator<'plan>),
}

impl Iterator for EffectiveExecPlanSubtreeIterator<'_> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            EffectiveExecPlanSubtreeIterator::Once(iter) => iter.next(),
            EffectiveExecPlanSubtreeIterator::Subtree(iter) => iter.next(),
        }
    }
}

/// Flags derived from an effective subtree and used to choose dispatch mode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SubtreeDispatchFlags {
    /// Whether the subtree reads segmented virtual tables.
    pub has_segmented_tables: bool,
    /// Whether the subtree contains per-replicaset customization opcodes.
    pub has_customization_opcodes: bool,
    /// Motion whose materialized vtable makes the subtree segmented.
    pub(crate) segmented_motion_id: Option<NodeId>,
}

#[derive(Debug)]
struct SubtreeViewSummary {
    key: SubtreeViewKey,
    constant_ids: Vec<NodeId>,
    dispatch_flags: SubtreeDispatchFlags,
}

#[derive(Debug, Default)]
struct SubtreeViewCache {
    inner: Rc<RefCell<AHashMap<NodeId, Rc<SubtreeViewSummary>>>>,
}

impl Clone for SubtreeViewCache {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl SubtreeViewCache {
    fn get(&self, top_id: NodeId) -> Option<Rc<SubtreeViewSummary>> {
        self.inner.borrow().get(&top_id).cloned()
    }

    fn insert(&self, top_id: NodeId, summary: Rc<SubtreeViewSummary>) {
        self.inner.borrow_mut().insert(top_id, summary);
    }

    fn clear(&self) {
        self.inner.borrow_mut().clear();
    }
}

impl PartialEq for SubtreeViewCache {
    fn eq(&self, _other: &Self) -> bool {
        // The cache stores derived per-execution subtree metadata. Warm and
        // cold caches must compare equally so `ExecutionPlan` equality reflects
        // only plan data and execution overlays that affect behavior.
        true
    }
}

impl Eq for SubtreeViewCache {}

/// Builds and caches effective subtree metadata for one execution plan root.
pub(crate) struct SubtreeViewBuilder<'plan> {
    exec_plan: &'plan ExecutionPlan,
    summary: Rc<SubtreeViewSummary>,
}

impl<'plan> SubtreeViewBuilder<'plan> {
    /// Returns a builder for `top_id`, reusing cached metadata when available.
    ///
    /// # Errors
    /// - If the effective subtree rooted at `top_id` cannot be traversed.
    pub(crate) fn new(
        exec_plan: &'plan ExecutionPlan,
        top_id: NodeId,
    ) -> Result<Self, SbroadError> {
        if let Some(summary) = exec_plan.subtree_view_cache.get(top_id) {
            return Ok(Self { exec_plan, summary });
        }

        let summary = Rc::new(Self::build_summary(exec_plan, top_id)?);
        exec_plan
            .subtree_view_cache
            .insert(top_id, Rc::clone(&summary));

        Ok(Self { exec_plan, summary })
    }

    fn build_summary(
        exec_plan: &'plan ExecutionPlan,
        top_id: NodeId,
    ) -> Result<SubtreeViewSummary, SbroadError> {
        let ir_plan = exec_plan.get_ir_plan();
        let subtree = PostOrder::new(
            |node| exec_plan.effective_sql_subtree_iter(node, Snapshot::Oldest),
            ir_plan.nodes.len(),
        );
        let subtree_nodes = subtree.traverse_into_iter(top_id).collect::<Vec<_>>();
        let node_ids = subtree_nodes
            .iter()
            .map(|LevelNode(_, id)| *id)
            .collect::<Vec<_>>();
        let node_levels = subtree_nodes
            .iter()
            .map(|LevelNode(level, _)| *level as u32)
            .collect::<Vec<_>>();
        let constants = PostOrderWithFilter::new(
            |node| exec_plan.effective_sql_subtree_output_first_iter(node, Snapshot::Oldest),
            |node| {
                matches!(
                    ir_plan.get_node(node),
                    Ok(Node::Expression(Expression::Constant(_)))
                )
            },
            ir_plan.nodes.len(),
        );
        let mut constant_ids = Vec::new();
        let mut constant_set = HashSet::new();
        for LevelNode(_, node_id) in constants.traverse_into_iter(top_id) {
            if constant_set.insert(node_id) {
                constant_ids.push(node_id);
            }
        }

        let mut leaf_motions = Vec::new();
        let mut serialize_as_empty = Vec::new();
        let mut dispatch_flags = SubtreeDispatchFlags::default();

        for node_id in &node_ids {
            if exec_plan.effective_motion_leaf_output(*node_id).is_some() {
                leaf_motions.push(*node_id);
            }
            if !dispatch_flags.has_segmented_tables {
                if let Some(vtable) = exec_plan.get_vtables().get(node_id) {
                    if !vtable.get_bucket_index().is_empty() {
                        dispatch_flags.has_segmented_tables = true;
                        dispatch_flags.segmented_motion_id = Some(*node_id);
                    }
                }
            }
            if let Node::Relational(Relational::Motion(Motion { program, .. })) =
                ir_plan.get_node(*node_id)?
            {
                if program
                    .0
                    .iter()
                    .any(|op| matches!(op, MotionOpcode::SerializeAsEmptyTable(_)))
                {
                    dispatch_flags.has_customization_opcodes = true;
                }
                match exec_plan.effective_serialize_as_empty_state(*node_id, program) {
                    SerializeAsEmptyState::Absent => {}
                    SerializeAsEmptyState::Enabled => serialize_as_empty.push((*node_id, true)),
                    SerializeAsEmptyState::Disabled => serialize_as_empty.push((*node_id, false)),
                }
            }
        }

        Ok(SubtreeViewSummary {
            key: SubtreeViewKey {
                top_id,
                node_ids,
                node_levels,
                leaf_motions,
                serialize_as_empty,
            },
            constant_ids,
            dispatch_flags,
        })
    }

    /// Returns the execution plan for which this subtree summary was built.
    pub(crate) fn exec_plan(&self) -> &'plan ExecutionPlan {
        self.exec_plan
    }

    /// Returns the cache key describing the effective subtree shape.
    pub(crate) fn key(&self) -> &SubtreeViewKey {
        &self.summary.key
    }

    /// Returns node ids rendered by the effective SQL subtree.
    pub(crate) fn node_ids(&self) -> &[NodeId] {
        &self.summary.key.node_ids
    }

    /// Returns tree levels for nodes rendered by the effective SQL subtree.
    pub(crate) fn node_levels(&self) -> &[u32] {
        &self.summary.key.node_levels
    }

    /// Returns constant node ids in SQL parameter order.
    pub(crate) fn constant_ids(&self) -> &[NodeId] {
        &self.summary.constant_ids
    }

    /// Returns dispatch flags derived for this effective subtree.
    pub(crate) fn dispatch_flags(&self) -> SubtreeDispatchFlags {
        self.summary.dispatch_flags
    }
}

/// SQL parameter data collected without mutating the IR plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSqlParams {
    constant_ids: Vec<NodeId>,
    params: Vec<Value>,
}

impl LocalSqlParams {
    /// Returns constant node ids in SQL placeholder order.
    #[must_use]
    pub fn constant_ids(&self) -> &[NodeId] {
        &self.constant_ids
    }

    /// Returns SQL parameter values in the order expected by local SQL.
    #[must_use]
    pub fn params(&self) -> &[Value] {
        &self.params
    }

    /// Splits the collected node ids and parameter values without cloning.
    pub fn into_parts(self) -> (Vec<NodeId>, Vec<Value>) {
        (self.constant_ids, self.params)
    }
}

/// SQL-visible bucket filter attached to selected storage subtrees.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct BucketFilter {
    /// Normalized bucket ids used as SQL suffix parameters.
    bucket_ids: Vec<u64>,
    /// Map of `Selection` node id to the system `bucket_id` reference node id
    /// rendered on the left-hand side of the generated `IN (...)` predicate.
    targets: AHashMap<NodeId, NodeId>,
}

fn bucket_filter_calculate(
    exec_plan: &ExecutionPlan,
    top_id: NodeId,
    buckets: &Buckets,
) -> Result<Option<BucketFilter>, SbroadError> {
    let Buckets::Filtered(BucketSet::Exact(bucket_ids)) = buckets else {
        return Ok(None);
    };
    if bucket_ids.is_empty() {
        return Ok(None);
    }

    let plan = exec_plan.get_ir_plan();
    let mut relation_name: Option<&SmolStr> = None;
    let mut targets = AHashMap::new();
    let subtree = PostOrder::new(
        |node| exec_plan.effective_exec_plan_subtree_iter(node, Snapshot::Oldest),
        plan.nodes.len(),
    );
    for LevelNode(_, node_id) in subtree.traverse_into_iter(top_id) {
        let Ok(Node::Relational(rel)) = plan.get_node(node_id) else {
            continue;
        };
        if let Some(node_relation_name) = bucket_filter_relation_name(&rel) {
            match relation_name {
                Some(name) if name != node_relation_name => return Ok(None),
                Some(_) => {}
                None => relation_name = Some(node_relation_name),
            }
        };
        if let Relational::Selection(selection) = rel {
            if let Some(bucket_ref_id) = bucket_ref_from_output(plan, selection.output)? {
                targets.insert(node_id, bucket_ref_id);
            }
        }
    }

    if targets.is_empty() {
        return Ok(None);
    }

    let Some(relation_name) = relation_name else {
        return Ok(None);
    };
    let Some(table) = plan.relations.tables.get(relation_name) else {
        return Ok(None);
    };
    if !table.has_bucket_id_primary_key_prefix() {
        return Ok(None);
    }

    let bucket_ids = bucket_ids.iter().copied().sorted_unstable().collect();

    Ok(Some(BucketFilter {
        bucket_ids,
        targets,
    }))
}

fn bucket_filter_relation_name<'plan>(rel: &Relational<'plan>) -> Option<&'plan SmolStr> {
    match rel {
        Relational::ScanRelation(scan) => Some(&scan.relation),
        Relational::Insert(insert) => Some(&insert.relation),
        Relational::Delete(delete) => Some(&delete.relation),
        Relational::Update(update) => Some(&update.relation),
        _ => None,
    }
}

fn bucket_ref_from_output(plan: &Plan, output_id: NodeId) -> Result<Option<NodeId>, SbroadError> {
    let output_expr = plan.get_expression_node(output_id)?;
    let row_list = output_expr.get_row_list()?;
    let bucket_ref_id = row_list.iter().find_map(|col_id| {
        let col_id = plan.get_child_under_alias(*col_id).ok()?;
        let expr = plan.get_expression_node(col_id).ok()?;
        let Expression::Reference(Reference { is_system, .. }) = expr else {
            return None;
        };
        is_system.then_some(col_id)
    });

    Ok(bucket_ref_id)
}

impl ExecutionPlan {
    pub fn new(plan: Plan) -> Self {
        ExecutionPlan {
            request_id: uuid::Uuid::new_v4().to_string(),
            plan,
            vtables: VirtualTableMap::new(),
            plan_id: None,
            plan_id_cache: AHashMap::new(),
            plan_id_sql_parameter_count_cache: AHashMap::new(),
            plan_id_sql_parameter_count: None,
            update_delete_tuple_lens: AHashMap::new(),
            bucket_filter: None,
            serialize_as_empty_disabled_motions: HashSet::new(),
            unlinked_motions: HashSet::new(),
            subtree_view_cache: SubtreeViewCache::default(),
            filter_state: AHashMap::new(),
        }
    }

    #[must_use]
    pub fn get_request_id(&self) -> &str {
        &self.request_id
    }

    /// Freeze this execution plan as a read-only borrow.
    ///
    /// The guarantee is provided by Rust borrow checking: code holding the
    /// returned value cannot also hold a mutable borrow of this plan.
    #[must_use]
    pub fn freeze(&self) -> FrozenPlan<'_> {
        FrozenPlan { exec_plan: self }
    }

    /// Returns the bucket reference attached to `selection_id` by the active
    /// bucket filter overlay.
    pub(crate) fn get_bucket_ref(&self, selection_id: NodeId) -> Option<NodeId> {
        self.bucket_filter
            .as_ref()?
            .targets
            .get(&selection_id)
            .copied()
    }

    /// Prepares a read-only bucket filter overlay for dispatching `top_id`.
    ///
    /// The overlay appends bucket ids as local SQL parameters without changing
    /// the IR plan.
    ///
    /// # Errors
    /// - If the effective subtree cannot be inspected.
    pub(crate) fn prepare_bucket_filter_for_dispatch(
        &mut self,
        top_id: NodeId,
        buckets: &Buckets,
    ) -> Result<(), SbroadError> {
        self.bucket_filter = bucket_filter_calculate(self, top_id, buckets)?;
        Ok(())
    }

    /// Clears the bucket filter overlay after dispatch customization.
    pub(crate) fn clear_bucket_filter(&mut self) {
        self.bucket_filter = None;
    }

    /// Disables `SerializeAsEmptyTable` for selected motions through an overlay.
    ///
    /// Cached effective subtree and plan-id metadata is cleared because SQL
    /// shape can change.
    pub(crate) fn disable_serialize_as_empty_for_motions(
        &mut self,
        motion_ids: impl IntoIterator<Item = NodeId>,
    ) {
        self.subtree_view_cache.clear();
        self.plan_id = None;
        self.plan_id_sql_parameter_count = None;
        self.plan_id_cache.clear();
        self.plan_id_sql_parameter_count_cache.clear();
        self.serialize_as_empty_disabled_motions.extend(motion_ids);
    }

    /// Returns effective `SerializeAsEmptyTable` state.
    pub(crate) fn effective_serialize_as_empty_state(
        &self,
        motion_id: NodeId,
        program: &Program,
    ) -> SerializeAsEmptyState {
        let Some(MotionOpcode::SerializeAsEmptyTable(enabled)) = program
            .0
            .iter()
            .find(|op| matches!(op, MotionOpcode::SerializeAsEmptyTable(_)))
        else {
            return SerializeAsEmptyState::Absent;
        };
        if self
            .serialize_as_empty_disabled_motions
            .contains(&motion_id)
        {
            return SerializeAsEmptyState::Disabled;
        }
        if *enabled {
            SerializeAsEmptyState::Enabled
        } else {
            SerializeAsEmptyState::Disabled
        }
    }

    /// Marks a materialized motion as an effective leaf without mutating the IR.
    ///
    /// # Errors
    /// - If `motion_id` is not a motion node.
    pub(crate) fn mark_motion_subtree_unlinked(
        &mut self,
        motion_id: NodeId,
    ) -> Result<(), SbroadError> {
        let Relational::Motion(_) = self.get_ir_plan().get_relation_node(motion_id)? else {
            return Err(SbroadError::Invalid(
                Entity::Relational,
                Some(format_smolstr!("node ({motion_id:?}) is not motion")),
            ));
        };
        self.subtree_view_cache.clear();
        self.unlinked_motions.insert(motion_id);
        Ok(())
    }

    /// Returns whether a motion subtree is currently treated as materialized.
    pub(crate) fn is_motion_subtree_unlinked(&self, motion_id: NodeId) -> bool {
        self.unlinked_motions.contains(&motion_id)
    }

    /// Returns the effective motion child after applying execution overlays.
    ///
    /// `Ok(None)` means the motion is rendered as a leaf because it has a
    /// materialized non-local virtual table or was explicitly unlinked.
    ///
    /// # Errors
    /// - If `motion_id` is not a motion node.
    pub(crate) fn effective_motion_child(
        &self,
        motion_id: NodeId,
    ) -> Result<Option<NodeId>, SbroadError> {
        let Relational::Motion(Motion { child, policy, .. }) =
            self.get_ir_plan().get_relation_node(motion_id)?
        else {
            return Err(SbroadError::Invalid(
                Entity::Relational,
                Some(format_smolstr!("node ({motion_id:?}) is not motion")),
            ));
        };
        if self.is_motion_subtree_unlinked(motion_id)
            || self.contains_vtable_for_motion(motion_id) && !policy.is_local()
        {
            return Ok(None);
        }
        Ok(*child)
    }

    /// Returns the output node used when `node_id` is an effective motion leaf.
    pub(crate) fn effective_motion_leaf_output(&self, node_id: NodeId) -> Option<NodeId> {
        let Ok(Node::Relational(Relational::Motion(Motion { output, policy, .. }))) =
            self.get_ir_plan().get_node(node_id)
        else {
            return None;
        };
        if self.is_motion_subtree_unlinked(node_id)
            || self.contains_vtable_for_motion(node_id) && !policy.is_local()
        {
            return Some(*output);
        }
        None
    }

    /// Returns the root below a motion if the motion subtree is still visible.
    ///
    /// # Errors
    /// - If `motion_id` is not a motion node.
    pub(crate) fn effective_motion_subtree_root(
        &self,
        motion_id: NodeId,
    ) -> Result<Option<NodeId>, SbroadError> {
        if self.effective_motion_child(motion_id)?.is_none() {
            return Ok(None);
        }
        Ok(Some(self.get_ir_plan().get_motion_subtree_root(motion_id)?))
    }

    /// Iterates an execution subtree after materialized motion overlays.
    pub(crate) fn effective_exec_plan_subtree_iter(
        &self,
        current: NodeId,
        snapshot: Snapshot,
    ) -> EffectiveExecPlanSubtreeIterator<'_> {
        if let Some(output) = self.effective_motion_leaf_output(current) {
            return EffectiveExecPlanSubtreeIterator::Once(std::iter::once(output));
        }
        EffectiveExecPlanSubtreeIterator::Subtree(
            self.get_ir_plan().exec_plan_subtree_iter(current, snapshot),
        )
    }

    /// Iterates a SQL subtree after all SQL-rendering overlays.
    ///
    /// Unlike execution traversal, this also collapses enabled
    /// `SerializeAsEmptyTable` motions to their output node.
    pub(crate) fn effective_sql_subtree_iter(
        &self,
        current: NodeId,
        snapshot: Snapshot,
    ) -> EffectiveExecPlanSubtreeIterator<'_> {
        self.effective_sql_subtree_iter_with(current, snapshot, false)
    }

    fn effective_sql_subtree_output_first_iter(
        &self,
        current: NodeId,
        snapshot: Snapshot,
    ) -> EffectiveExecPlanSubtreeIterator<'_> {
        self.effective_sql_subtree_iter_with(current, snapshot, true)
    }

    fn effective_sql_subtree_iter_with(
        &self,
        current: NodeId,
        snapshot: Snapshot,
        output_first: bool,
    ) -> EffectiveExecPlanSubtreeIterator<'_> {
        if let Some(output) = self.effective_motion_leaf_output(current) {
            return EffectiveExecPlanSubtreeIterator::Once(std::iter::once(output));
        }
        if let Ok(Node::Relational(Relational::Motion(Motion {
            output, program, ..
        }))) = self.get_ir_plan().get_node(current)
        {
            if self
                .effective_serialize_as_empty_state(current, program)
                .is_enabled()
            {
                return EffectiveExecPlanSubtreeIterator::Once(std::iter::once(*output));
            }
        }
        let ir_plan = self.get_ir_plan();
        let subtree = if output_first {
            ir_plan.exec_plan_subtree_output_first_iter(current, snapshot)
        } else {
            ir_plan.exec_plan_subtree_iter(current, snapshot)
        };
        EffectiveExecPlanSubtreeIterator::Subtree(subtree)
    }

    /// Returns bucket ids appended to SQL parameters by the active filter.
    pub(crate) fn get_bucket_filter_ids(&self) -> Option<&[u64]> {
        Some(self.bucket_filter.as_ref()?.bucket_ids.as_slice())
    }

    /// Returns the number of SQL parameters in the current effective SQL tree.
    ///
    /// # Errors
    /// - If the plan top or effective subtree cannot be inspected.
    pub(crate) fn sql_parameter_count(&self) -> Result<usize, SbroadError> {
        let top_id = self.get_ir_plan().get_top()?;
        Ok(SubtreeViewBuilder::new(self, top_id)?.constant_ids().len())
    }

    /// Returns the plan-id parameter count plus active bucket-filter parameters.
    ///
    /// # Errors
    /// - If the SQL parameter count cannot be calculated.
    pub(crate) fn parameter_count(&self) -> Result<usize, SbroadError> {
        Ok(self
            .plan_id_sql_parameter_count
            .map_or_else(|| self.sql_parameter_count(), Ok)?
            + self
                .bucket_filter
                .as_ref()
                .map_or(0, |filter| filter.bucket_ids.len()))
    }

    /// Collects constant node ids and SQL parameter values for local SQL.
    ///
    /// For `Snapshot::Oldest`, constants are collected from the cached effective
    /// SQL subtree. Bucket filter values are appended after IR constants.
    ///
    /// # Errors
    /// - If the effective subtree cannot be inspected.
    /// - If a collected constant cannot be converted to a parameter value.
    /// - If the parameter count exceeds the local SQL placeholder limit.
    pub fn local_sql_params(
        &self,
        top_id: NodeId,
        snapshot: Snapshot,
    ) -> Result<LocalSqlParams, SbroadError> {
        let constant_ids = match snapshot {
            Snapshot::Oldest => SubtreeViewBuilder::new(self, top_id)?
                .constant_ids()
                .to_vec(),
            Snapshot::Latest => self.get_ir_plan().get_const_list(top_id, snapshot),
        };
        let mut params = Vec::with_capacity(constant_ids.len());
        for (num, const_id) in constant_ids.iter().enumerate() {
            let _: u16 = (num + 1).try_into().map_err(|_| {
                SbroadError::Other(format_smolstr!("too many parameters in local sql: {num}"))
            })?;
            params.push(self.get_ir_plan().as_const_value_ref(*const_id)?.clone());
        }
        if let Some(filter) = &self.bucket_filter {
            params.extend(
                filter
                    .bucket_ids
                    .iter()
                    .map(|bucket_id| Value::Integer(*bucket_id as i64)),
            );
        }
        Ok(LocalSqlParams {
            constant_ids,
            params,
        })
    }

    /// Returns the node id from which the `plan_id` should be calculated.
    /// This is a bit tricky for DML.
    /// If plan is cacheable, it never returns `None`.
    pub fn get_plan_id_target(&self) -> Result<Option<NodeId>, SbroadError> {
        let query_type = self.query_type()?;
        let ir = self.get_ir_plan();
        let top_id = ir.get_top()?;

        let node_id = match query_type {
            QueryType::DQL => Some(top_id),
            QueryType::DML => {
                let top = ir.get_relation_node(top_id)?;
                let top_children = ir.children(top_id);
                if matches!(top, Relational::Delete(_)) && top_children.is_empty() {
                    Some(top_id)
                } else {
                    let child_id = top_children[0];
                    if let Relational::Motion(Motion {
                        policy: MotionPolicy::Local | MotionPolicy::LocalSegment { .. },
                        ..
                    }) = ir.get_relation_node(child_id)?
                    {
                        self.effective_motion_subtree_root(child_id)?
                    } else {
                        None
                    }
                }
            }
        };

        Ok(node_id)
    }

    /// Calculates or loads the cached plan id for the effective subtree.
    ///
    /// The matching SQL parameter count is cached together with the id so later
    /// calls can salt cache ids without walking the subtree again.
    ///
    /// The selected `plan_id` and parameter count are valid for the current
    /// effective SQL shape. After calling this method, callers must not mutate
    /// IR shape or virtual-table metadata for that selected shape before using
    /// `plan_id` or `parameter_count`. Shape-changing execution overlays must
    /// clear or recalculate their own plan-id metadata.
    ///
    /// # Errors
    /// - If the subtree plan id or SQL parameter count cannot be calculated.
    pub fn set_plan_id(&mut self, top_id: NodeId) -> Result<u64, SbroadError> {
        let (plan_id, sql_parameter_count) = if let Some(plan_id) = self.plan_id_cache.get(&top_id)
        {
            let sql_parameter_count =
                if let Some(count) = self.plan_id_sql_parameter_count_cache.get(&top_id) {
                    *count
                } else {
                    let count = self.sql_parameter_count()?;
                    self.plan_id_sql_parameter_count_cache.insert(top_id, count);
                    count
                };
            (*plan_id, sql_parameter_count)
        } else {
            let (plan_id, sql_parameter_count) =
                self.calculate_plan_id_with_parameter_count(top_id)?;
            self.plan_id_cache.insert(top_id, plan_id);
            self.plan_id_sql_parameter_count_cache
                .insert(top_id, sql_parameter_count);
            (plan_id, sql_parameter_count)
        };

        self.plan_id = Some(plan_id);
        self.plan_id_sql_parameter_count = Some(sql_parameter_count);
        Ok(plan_id)
    }

    /// Stores the delete tuple width for a sharded update overlay.
    ///
    /// # Errors
    /// - If `update_id` is not a sharded update node.
    pub(crate) fn set_update_delete_tuple_len(
        &mut self,
        update_id: NodeId,
        len: usize,
    ) -> Result<(), SbroadError> {
        let node = self.get_ir_plan().get_relation_node(update_id)?;
        if !matches!(
            node,
            Relational::Update(Update {
                strategy: UpdateStrategy::ShardedUpdate,
                ..
            })
        ) {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("expected sharded Update, got: {node:?}")),
            ));
        }

        self.update_delete_tuple_lens.insert(update_id, len);
        Ok(())
    }

    /// Returns the delete tuple width calculated for a sharded update.
    ///
    /// # Errors
    /// - If `update_id` is not a sharded update node.
    /// - If the width has not been stored in the execution overlay.
    pub fn get_update_delete_tuple_len(&self, update_id: NodeId) -> Result<usize, SbroadError> {
        let node = self.get_ir_plan().get_relation_node(update_id)?;
        if !matches!(
            node,
            Relational::Update(Update {
                strategy: UpdateStrategy::ShardedUpdate,
                ..
            })
        ) {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("expected sharded Update, got: {node:?}")),
            ));
        }

        self.update_delete_tuple_lens
            .get(&update_id)
            .copied()
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!(
                        "expected sharded Update with delete len, got: {node:?}"
                    )),
                )
            })
    }

    #[must_use]
    pub fn get_ir_plan(&self) -> &Plan {
        &self.plan
    }

    #[must_use]
    pub fn get_sql_motion_row_max(&self) -> u64 {
        self.plan.effective_options.sql_motion_row_max as u64
    }

    #[allow(dead_code)]
    pub fn get_mut_ir_plan(&mut self) -> &mut Plan {
        self.subtree_view_cache.clear();
        &mut self.plan
    }

    #[must_use]
    pub fn get_vtables(&self) -> &VirtualTableMap {
        &self.vtables
    }

    pub fn get_mut_vtables(&mut self) -> &mut VirtualTableMap {
        self.subtree_view_cache.clear();
        &mut self.vtables
    }

    pub fn set_vtables(&mut self, vtables: VirtualTableMap) {
        self.subtree_view_cache.clear();
        self.vtables = vtables;
    }

    pub fn contains_vtable_for_motion(&self, motion_id: NodeId) -> bool {
        self.get_vtables().contains_key(&motion_id)
    }

    /// Get motion virtual table
    pub fn get_motion_vtable(&self, motion_id: NodeId) -> Result<Rc<VirtualTable>, SbroadError> {
        let node = self.get_ir_plan().get_relation_node(motion_id)?;
        if !node.is_motion() {
            return Err(SbroadError::Invalid(
                Entity::Motion,
                Some(format_smolstr!("got {node:?}")),
            ));
        }
        if let Some(result) = self.get_vtables().get(&motion_id) {
            return Ok(Rc::clone(result));
        }
        Err(SbroadError::NotFound(
            Entity::VirtualTable,
            format_smolstr!("for {node:?} with id {motion_id}"),
        ))
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
                        self.set_update_delete_tuple_len(update_id, v)?;
                    }
                }
                MotionOpcode::AddMissingRowsForLeftJoin { motion_id, .. } => {
                    let motion_id = *motion_id;
                    let Some(from_vtable) = self.vtables.get(&motion_id) else {
                        return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                            "expected virtual table for motion {motion_id:?}"
                        )));
                    };
                    vtable.add_missing_rows(from_vtable)?;
                }
                MotionOpcode::SerializeAsEmptyTable(_) => {}
            }
        }

        self.get_mut_vtables().insert(*motion_id, Rc::new(vtable));

        Ok(())
    }

    #[must_use]
    pub fn has_segmented_tables(&self) -> bool {
        self.vtables
            .values()
            .any(|t| !t.get_bucket_index().is_empty())
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

    /// Returns dispatch flags derived from the effective subtree at `top_id`.
    ///
    /// # Errors
    /// - If the effective subtree cannot be inspected.
    pub fn subtree_dispatch_flags_at(
        &self,
        top_id: NodeId,
    ) -> Result<SubtreeDispatchFlags, SbroadError> {
        Ok(SubtreeViewBuilder::new(self, top_id)?.dispatch_flags())
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
        self.get_vtables().is_empty()
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
        Ok(engine.unwrap_or(SpaceEngine::Memtx))
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
