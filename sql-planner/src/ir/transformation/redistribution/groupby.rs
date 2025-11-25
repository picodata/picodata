use ahash::AHashMap;
use smol_str::{format_smolstr, ToSmolStr};

use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::to_user;
use crate::frontend::sql::ir::SubtreeCloner;
use crate::ir::aggregates::Aggregate;
use crate::ir::distribution::Distribution;
use crate::ir::expression::{ColumnPositionMap, Comparator, EXPR_HASH_DEPTH};
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{
    ArenaType, GroupBy, Having, NodeId, Projection, Reference, ReferenceTarget, SubQueryReference,
};
use crate::ir::transformation::redistribution::{MotionPolicy, Program, Strategy};
use crate::ir::tree::traversal::{LevelNode, PostOrder, PostOrderWithFilter, EXPR_CAPACITY};
use crate::ir::{Node, Plan};
use std::collections::HashMap;

use crate::ir::helpers::RepeatableState;
use crate::utils::{OrderedMap, OrderedSet};
use std::hash::{Hash, Hasher};
use std::rc::Rc;

/// Helper struct to hold information about
/// location of grouping expressions used in
/// nodes other than `GroupBy`.
///
/// E.g. for query `select 1 + a from t group by a`
/// location for grouping expression `a` will look like
/// {
///   `expr`: id of a under sum expr,
///   `parent_expr`: Some(id of sum expr),
///   `rel`: Projection
/// }
#[derive(Debug, Clone)]
struct ExpressionLocationId {
    /// Id of grouping expression.
    pub expr_id: NodeId,
    /// Id of expression which is a parent of `expr`.
    pub parent_expr_id: Option<NodeId>,
    /// Relational node in which this `expr` is used.
    pub rel_id: NodeId,
}

impl ExpressionLocationId {
    pub fn new(expr_id: NodeId, parent_expr_id: Option<NodeId>, rel_id: NodeId) -> Self {
        ExpressionLocationId {
            parent_expr_id,
            expr_id,
            rel_id,
        }
    }
}

/// Id of grouping expression united with reference to plan
/// for the ease of expressions comparison (see
/// implementation of `Hash` and `PartialEq` traits).
#[derive(Debug, Clone)]
struct GroupingExpression<'plan> {
    pub id: NodeId,
    pub plan: &'plan Plan,
}

impl<'plan> GroupingExpression<'plan> {
    pub fn new(id: NodeId, plan: &'plan Plan) -> Self {
        GroupingExpression { id, plan }
    }
}

impl Hash for GroupingExpression<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut comp = Comparator::new(self.plan);
        comp.set_hasher(state);
        comp.hash_for_expr(self.id, EXPR_HASH_DEPTH);
    }
}

impl PartialEq for GroupingExpression<'_> {
    fn eq(&self, other: &Self) -> bool {
        let comp = Comparator::new(self.plan);
        comp.are_subtrees_equal(self.id, other.id).unwrap_or(false)
    }
}

impl Eq for GroupingExpression<'_> {}

/// Maps id of `GroupBy` expression used in `GroupBy` (from local stage)
/// to list of locations where this expression is used in other relational
/// operators like `Having`, `Projection`.
///
/// For example:
/// `select a from t group by a having a = 1`
/// Here expression in `GroupBy` is mapped to `a` in `Projection` and `a` in `Having`
///
/// In case there is a reference (or an expression containing references like `"a" + "b"`)
/// in the location relational operator, there will be a corresponding mapping for it.
/// In case there is a reference (or expression containing it) in the final relational operator
/// that doesn't correspond to any GroupBy expression, an error should have been thrown on the
/// stage of `collect_grouping_exprs`.
type GroupbyExpressionsMap = AHashMap<NodeId, Vec<ExpressionLocationId>>;

/// Maps id of `GroupBy` expression used in `GroupBy` (from local stage)
/// to corresponding local alias used in local Projection. Note:
/// this map does not contain mappings between grouping expressions from
/// distinct aggregates (it is stored in corresponding `Aggregate` for that
/// aggregate)
///
/// For example:
/// initial query: `select a, count(distinct b) from t group by a`
/// map query: `select a as l1, b group by a, b`
/// Then this map will map id of `a` to `l1`
type LocalAliasesMap = HashMap<NodeId, Rc<String>>;

/// Helper struct to map expressions used in `GroupBy` to
/// expressions used in some other node (`Projection`, `Having`, `OrderBy`)
struct ExpressionMapper<'plan> {
    /// List of expressions ids of `GroupBy`
    gr_exprs: &'plan Vec<NodeId>,
    map: &'plan mut GroupbyExpressionsMap,
    plan: &'plan Plan,
    /// Id of relational node (`Projection`, `Having`, `OrderBy`)
    rel_id: NodeId,
}

impl<'plan> ExpressionMapper<'plan> {
    fn new(
        gr_exprs: &'plan Vec<NodeId>,
        plan: &'plan Plan,
        rel_id: NodeId,
        map: &'plan mut GroupbyExpressionsMap,
    ) -> ExpressionMapper<'plan> {
        ExpressionMapper {
            gr_exprs,
            map,
            plan,
            rel_id,
        }
    }

    /// Traverses given expression from top to bottom, trying
    /// to find subexpressions that match expressions located in `GroupBy`,
    /// when match is found it is stored in map passed to [`ExpressionMapper`]'s
    /// constructor.
    fn find_matches(&mut self, expr_root: NodeId) -> Result<(), SbroadError> {
        self.find(expr_root, None)?;
        Ok(())
    }

    /// Helper function for `find_matches` which compares current node to `GroupBy` expressions
    /// and if no match is found recursively calls itself.
    fn find(&mut self, current: NodeId, parent_expr: Option<NodeId>) -> Result<(), SbroadError> {
        let expr_node = self.plan.get_expression_node(current)?;

        if matches!(expr_node, Expression::SubQueryReference { .. }) {
            return Ok(());
        }

        let comparator = Comparator::new(self.plan);
        if let Some(gr_expr) = self
            .gr_exprs
            .iter()
            .find(|gr_expr| {
                comparator
                    .are_subtrees_equal(current, **gr_expr)
                    .unwrap_or(false)
            })
            .copied()
        {
            let location = ExpressionLocationId::new(current, parent_expr, self.rel_id);
            if let Some(v) = self.map.get_mut(&gr_expr) {
                v.push(location);
            } else {
                self.map.insert(gr_expr, vec![location]);
            }
            return Ok(());
        }
        if matches!(expr_node, Expression::Reference(_)) {
            // We found a column which is not inside aggregate function
            // and it is not a grouping expression:
            // select a from t group by b - is invalid
            let column_name = {
                let ref_node: Expression<'_> = self.plan.get_expression_node(current)?;
                self.plan
                    .get_alias_from_reference_node(&ref_node)
                    .unwrap_or("'failed to get column name'")
            };
            return Err(SbroadError::Invalid(
                Entity::Query,
                Some(format_smolstr!(
                    "column {} is not found in grouping expressions!",
                    to_user(column_name)
                )),
            ));
        }
        for child in self.plan.nodes.aggregate_iter(current, false) {
            self.find(child, Some(current))?;
        }
        Ok(())
    }
}

/// Generate alias for grouping expression used under local GroupBy.
/// Read more about local aliases under `add_local_projection` comments.
fn grouping_expr_local_alias(index: usize) -> Rc<String> {
    Rc::new(format!("gr_expr_{index}"))
}

/// Capacity for the vecs/maps of grouping expressions we expect
/// to extract from nodes like Projection, GroupBy and Having.
const GR_EXPR_CAPACITY: usize = 5;

/// Info helpful to generate final GroupBy node (on Reduce stage).
struct GroupByReduceInfo {
    local_aliases_map: LocalAliasesMap,
    /// Positions of grouping expressions added to the output of local
    /// Projection. Used for generating MotionKey for segmented motion.
    /// That's the reason we don't count grouping expressions came from
    /// distinct aggregates here as they don't influence distribution.
    grouping_positions: Vec<usize>,
}

impl GroupByReduceInfo {
    fn new() -> Self {
        Self {
            local_aliases_map: HashMap::with_capacity(GR_EXPR_CAPACITY),
            grouping_positions: Vec::with_capacity(GR_EXPR_CAPACITY),
        }
    }
}

/// Info about both local and final GroupBy nodes. Such info is not
/// generated in case query doesn't require GroupBy nodes. E.g. `select sum(a) from t`
/// will require only two additional nodes: local Projection and a Motion node (see
/// logic under `add_two_stage_aggregation`).
struct GroupByInfo {
    id: NodeId,
    grouping_exprs: Vec<NodeId>,
    grouping_exprs_map: GroupbyExpressionsMap,
    /// Map of { grouping_expr under local GroupBy -> its alias }.
    grouping_expr_to_alias_map: OrderedMap<NodeId, Rc<String>, RepeatableState>,
    reduce_info: GroupByReduceInfo,
}

impl GroupByInfo {
    fn new(id: NodeId) -> Self {
        Self {
            id,
            grouping_exprs: Vec::with_capacity(GR_EXPR_CAPACITY),
            grouping_exprs_map: AHashMap::with_capacity(GR_EXPR_CAPACITY),
            grouping_expr_to_alias_map: OrderedMap::with_hasher(RepeatableState),
            reduce_info: GroupByReduceInfo::new(),
        }
    }
}

impl Plan {
    /// Used to create a `GroupBy` IR node from AST.
    /// The added `GroupBy` node is local - meaning
    /// that it is part of local stage in 2-stage
    /// aggregation. For more info, see `add_two_stage_aggregation`.
    pub fn add_groupby_from_ast(&mut self, children: &[NodeId]) -> Result<NodeId, SbroadError> {
        let Some((first_child, other)) = children.split_first() else {
            return Err(SbroadError::UnexpectedNumberOfValues(
                "GroupBy ast has no children".into(),
            ));
        };

        let groupby_id = self.add_groupby(*first_child, other)?;
        Ok(groupby_id)
    }

    /// Helper function to add `group by` to IR.
    pub fn add_groupby(
        &mut self,
        child_id: NodeId,
        grouping_exprs: &[NodeId],
    ) -> Result<NodeId, SbroadError> {
        let final_output = self.add_row_for_output(child_id, &[], true, None)?;
        let groupby = GroupBy {
            children: [child_id].to_vec(),
            gr_exprs: grouping_exprs.to_vec(),
            output: final_output,
        };

        self.add_relational(groupby.into())
    }

    /// Get first child from relational node
    ///
    /// Errors:
    /// - expected relation node `rel_id` to have children!
    ///
    /// Panics:
    /// - node is not relational
    ///
    /// Returns:
    /// - id of the first child
    pub fn get_first_rel_child(&self, rel_id: NodeId) -> Result<NodeId, SbroadError> {
        self.get_rel_child(rel_id, 0)
    }

    /// Get id of GroupBy under Projection.
    ///
    /// # Examples:
    ///
    /// ```txt
    /// Projection (1)
    ///     Having (2)
    ///     GroupBy (3)
    ///         Scan (4)
    /// ```
    /// Then this function will return `Some(3)`
    ///
    /// ```txt
    /// Projection (1)
    ///     Scan (2)
    /// ```
    /// Then this function will return `None`
    ///
    /// # Errors:
    /// - `proj_id` does not point to `Relational` node
    ///
    /// # Panics:
    /// - `proj_id` is `Relational` but not `Projection` node
    ///
    /// # Returns:
    /// - group_by_id if exists
    pub(crate) fn get_group_by(&self, proj_id: NodeId) -> Result<Option<NodeId>, SbroadError> {
        if let Relational::Projection(Projection { group_by, .. }) =
            self.get_relation_node(proj_id)?
        {
            Ok(*group_by)
        } else {
            unreachable!("expected Projection IR node");
        }
    }

    /// Get id of Having under Projection.
    ///
    /// # Examples:
    /// ```txt
    /// Projection (1)
    ///     Having (2)
    ///     GroupBy (3)
    ///         Scan (4)
    /// ```
    /// Then this function will return `Some(2)`
    ///
    /// ```txt
    /// Projection (1)
    ///     GroupBy (2)
    ///         Scan (3)
    /// ```
    /// Then this function will return `None`
    ///
    /// # Errors:
    /// - `proj_id` does not point to `Relational` node
    ///
    /// # Panics:
    /// - `proj_id` is `Relational` but not `Projection` node
    ///
    /// # Returns:
    /// - group_by_id if exists
    pub(crate) fn get_having(&self, proj_id: NodeId) -> Result<Option<NodeId>, SbroadError> {
        if let Relational::Projection(Projection { having, .. }) =
            self.get_relation_node(proj_id)?
        {
            Ok(*having)
        } else {
            unreachable!("expected Projection IR node");
        }
    }

    /// In case we deal with a query containing "distinct" qualifier and
    /// not containing aggregates or user defined GroupBy, we have to add
    /// GroupBy node for fulfill "distinct" semantics.
    ///
    /// Add GroupBy corresponding to `distinct` in Projection to IR plan.
    ///
    /// # Errors:
    /// - `proj_id` is not `Relational` node
    /// - cannot get `Projection` columns
    /// - incorrect `Projection` column subtree
    /// - cannot add `GroupBy` node to IR plan
    ///
    /// # Returns:
    /// - GroupBy node id if in Projection `distinct = true`
    fn add_group_by_for_distinct(
        &mut self,
        proj_id: NodeId,
        group_by_child: NodeId,
        scalar_sqs: &mut OrderedSet<NodeId, RepeatableState>,
    ) -> Result<Option<NodeId>, SbroadError> {
        let Relational::Projection(Projection {
            is_distinct: true,
            output,
            group_by: None,
            ..
        }) = self.get_relation_node(proj_id)?
        else {
            return Ok(None);
        };
        let proj_cols_len = self.get_row_list(*output)?.len();
        let mut grouping_exprs: Vec<NodeId> = Vec::with_capacity(proj_cols_len);

        for i in 0..proj_cols_len {
            let aliased_col = self.get_proj_col(proj_id, i)?;
            let proj_col_id = self.get_child_under_alias(aliased_col)?;

            // For queries of the form `SELECT DISTINCT a, <SQ_1>, ..., <SQ_n> FROM t`,
            // where each SQ_i is a scalar subquery,
            // we need to collect these subqueries so they can be attached to subsequent local nodes.
            for ref_id in self.get_refs_from_subtree(proj_col_id)? {
                if matches!(
                    self.get_expression_node(ref_id)?,
                    Expression::SubQueryReference { .. }
                ) {
                    scalar_sqs.insert(self.get_relational_from_reference_node(ref_id)?);
                }
            }

            let col = SubtreeCloner::clone_subtree(self, proj_col_id)?;
            grouping_exprs.push(col);
        }
        let group_by_id = self.add_groupby(group_by_child, &grouping_exprs)?;
        Ok(Some(group_by_id))
    }

    /// Fill grouping expression map (see comments next to
    /// `GroupbyExpressionsMap` definition).
    #[allow(clippy::too_many_lines)]
    fn fill_grouping_exprs_map(
        &mut self,
        proj: NodeId,
        groupby_info: &mut GroupByInfo,
    ) -> Result<(), SbroadError> {
        match self.get_relation_node(proj)? {
            Relational::Projection(Projection { output, .. }) => {
                let mut mapper = ExpressionMapper::new(
                    &groupby_info.grouping_exprs,
                    self,
                    proj,
                    &mut groupby_info.grouping_exprs_map,
                );
                for col in self.get_row_list(*output)? {
                    mapper.find_matches(*col)?;
                }
            }
            _ => {
                unreachable!("expected Projection IR node");
            }
        }

        if let Some(having) = self.get_having(proj)? {
            match self.get_relation_node(having)? {
                Relational::Having(Having { filter, .. }) => {
                    let mut mapper = ExpressionMapper::new(
                        &groupby_info.grouping_exprs,
                        self,
                        having,
                        &mut groupby_info.grouping_exprs_map,
                    );
                    mapper.find_matches(*filter)?;
                }
                _ => {
                    unreachable!("expected Having IR node");
                }
            }
        }
        Ok(())
    }

    /// In case query doesn't contain user defined GroupBy, check that all
    /// column references under `finals` are inside aggregate functions.
    fn check_refs_out_of_aggregates(&self, proj: NodeId) -> Result<(), SbroadError> {
        let output = self.get_relational_output(proj)?;
        for col in self.get_row_list(output)? {
            let filter = |node_id: NodeId| -> bool {
                matches!(
                    self.get_node(node_id),
                    Ok(Node::Expression(Expression::Reference(_)))
                )
            };
            let mut dfs = PostOrderWithFilter::with_capacity(
                |x| self.nodes.aggregate_iter(x, false),
                EXPR_CAPACITY,
                Box::new(filter),
            );
            dfs.populate_nodes(*col);
            let nodes = dfs.take_nodes();
            for LevelNode(_, id) in nodes {
                let n = self.get_expression_node(id)?;
                if matches!(n, Expression::Reference(_)) {
                    let alias = match self.get_alias_from_reference_node(&n) {
                        Ok(v) => v.to_smolstr(),
                        Err(e) => e.to_smolstr(),
                    };
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!(
                            "found column reference ({}) outside aggregate function",
                            to_user(alias)
                        )),
                    ));
                }
            }
        }

        let Some(having) = self.get_having(proj)? else {
            return Ok(());
        };

        let Relational::Having(Having { filter, .. }) = self.get_relation_node(having)? else {
            unreachable!("expected having node");
        };

        let mut dfs =
            PostOrder::with_capacity(|x| self.nodes.aggregate_iter(x, false), EXPR_CAPACITY);
        dfs.populate_nodes(*filter);
        let nodes = dfs.take_nodes();
        for LevelNode(_, id) in nodes {
            if matches!(self.get_expression_node(id)?, Expression::Reference(_)) {
                return Err(SbroadError::Invalid(
					Entity::Query,
					Some("HAVING argument must appear in the GROUP BY clause or be used in an aggregate function".into())
				));
            }
        }
        Ok(())
    }

    /// Check for GroupBy on bucket_id column.
    /// In that case GroupBy can be done locally.
    fn check_bucket_id_under_group_by(
        &self,
        grouping_exprs: &Vec<NodeId>,
    ) -> Result<bool, SbroadError> {
        for expr_id in grouping_exprs {
            let Expression::Reference(Reference { position, .. }) =
                self.get_expression_node(*expr_id)?
            else {
                continue;
            };
            let child_id = self.get_relational_from_reference_node(*expr_id)?;
            let mut context = self.context_mut();
            if let Some(shard_positions) = context.get_shard_columns_positions(child_id, self)? {
                if shard_positions[0] == Some(*position) || shard_positions[1] == Some(*position) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// "Grouping exprs" are expressions that are used in GroupBy clause
    /// (on both Map and Reduce stages of the algorithm). In this function we try
    /// to identify which grouping exprs we have to add in order to
    /// execute the query correctly.
    fn collect_grouping_exprs(
        &mut self,
        groupby_info: &mut Option<GroupByInfo>,
        aggrs: &mut [Aggregate],
        proj: NodeId,
        scalar_sqs: &mut OrderedSet<NodeId, RepeatableState>,
    ) -> Result<(), SbroadError> {
        let distinct_aggr_grouping_exprs =
            self.collect_grouping_exprs_from_distinct_aggrs(aggrs, scalar_sqs)?;

        // Index for generating local grouping expressions aliases.
        let mut local_alias_index = 1;
        // Map of { grouping_expr -> local_alias }.
        // We are using an OrderedMap to get the same order of grouping exprs
        // so that our tests are not flaky.
        let mut unique_grouping_expr_to_alias_map: OrderedMap<
            GroupingExpression,
            Rc<String>,
            RepeatableState,
        > = OrderedMap::with_capacity_and_hasher(GR_EXPR_CAPACITY, RepeatableState);
        // Grouping expressions for local GroupBy.
        let mut grouping_exprs_local = Vec::with_capacity(GR_EXPR_CAPACITY);
        if let Some(groupby_info) = groupby_info.as_mut() {
            // Leave only unique expressions under local GroupBy.
            let gr_exprs = self.get_grouping_exprs(groupby_info.id)?;
            for gr_expr in gr_exprs {
                let local_alias = grouping_expr_local_alias(local_alias_index);
                let new_expr = GroupingExpression::new(*gr_expr, self);
                if !unique_grouping_expr_to_alias_map.contains_key(&new_expr) {
                    unique_grouping_expr_to_alias_map.insert(new_expr, local_alias);
                    local_alias_index += 1;
                }
            }
            for (expr, _) in unique_grouping_expr_to_alias_map.iter() {
                let expr_id = expr.id;
                grouping_exprs_local.push(expr_id);
                groupby_info.grouping_exprs.push(expr_id);
            }
        }

        // Set local aggregates aliases for distinct aggregatees. For non-distinct aggregates
        // they would be set under `add_local_aggregates`.
        for (gr_expr, aggr) in distinct_aggr_grouping_exprs {
            let new_expr = GroupingExpression::new(gr_expr, self);
            if let Some(local_alias) = unique_grouping_expr_to_alias_map.get(&new_expr) {
                aggr.lagg_aliases.insert(aggr.kind, local_alias.clone());
            } else {
                let local_alias = grouping_expr_local_alias(local_alias_index);
                local_alias_index += 1;
                aggr.lagg_aliases.insert(aggr.kind, local_alias.clone());

                // Add expressions used as arguments to distinct aggregates to local `GroupBy`.
                //
                // E.g: For query below, we should add b*b to local `GroupBy`
                // `select a, sum(distinct b*b), count(c) from t group by a`
                // Map: `select a as l1, b*b as l2, count(c) as l3 from t group by a, b, b*b`
                // Reduce: `select l1, sum(distinct l2), sum(l3) from tmp_space group by l1`
                grouping_exprs_local.push(gr_expr);
                unique_grouping_expr_to_alias_map.insert(new_expr, local_alias);
            }
        }

        if let Some(groupby_info) = groupby_info.as_mut() {
            for (expr, local_alias) in unique_grouping_expr_to_alias_map.iter() {
                groupby_info
                    .grouping_expr_to_alias_map
                    .insert(expr.id, local_alias.clone());
            }

            let child = self.get_first_rel_child(groupby_info.id)?;

            // Move scalar subqueries that came from DISTINCT qualifier to GroupBy children.
            let groupby = self.get_mut_relation_node(groupby_info.id)?;
            let MutRelational::GroupBy(GroupBy { children, .. }) = groupby else {
                unreachable!("GroupBy node should be met for groupby info")
            };
            children.extend(scalar_sqs.iter());

            for gr_expr_local in &grouping_exprs_local {
                self.set_target_in_subtree(*gr_expr_local, child)?;

                // For query `SELECT 1 FROM t GROUP BY (SELECT 1)`
                // we'd like to clone scalar subquery from local GroupBy
                // to local Projection node.
                for ref_id in self.get_sq_refs_from_subtree(*gr_expr_local)? {
                    let ref_node = self.get_expression_node(ref_id)?;
                    match ref_node {
                        Expression::SubQueryReference(SubQueryReference { rel_id, .. }) => {
                            scalar_sqs.insert(*rel_id);
                        }
                        _ => unreachable!("Expected SubQueryReference, got: {ref_node:?}"),
                    }
                }
            }

            self.set_grouping_exprs(groupby_info.id, grouping_exprs_local)?;
            self.fill_grouping_exprs_map(proj, groupby_info)?;

            self.set_rel_output_distribution(groupby_info.id)?;
        }

        Ok(())
    }

    /// In case we have distinct aggregates like `count(distinct a)` they result
    /// in adding its argument expressions (expression `a` for the case above) under
    /// local GroupBy node.
    fn collect_grouping_exprs_from_distinct_aggrs<'aggr>(
        &self,
        aggrs: &'aggr mut [Aggregate],
        scalar_sqs: &mut OrderedSet<NodeId, RepeatableState>,
    ) -> Result<Vec<(NodeId, &'aggr mut Aggregate)>, SbroadError> {
        let mut res = Vec::with_capacity(aggrs.len());
        for aggr in aggrs.iter_mut().filter(|x| x.is_distinct) {
            let arg: NodeId = *self
                .nodes
                .expr_iter(aggr.fun_id, false)
                .collect::<Vec<NodeId>>()
                .first()
                .expect("Number of args for aggregate should have been already checked");

            // For such aggregates we should move their sqs from final Projection to local GroupBy.
            for ref_id in self.get_sq_refs_from_subtree(arg)? {
                let ref_node = self.get_expression_node(ref_id)?;
                match ref_node {
                    Expression::SubQueryReference(SubQueryReference { rel_id, .. }) => {
                        scalar_sqs.insert(*rel_id);
                    }
                    _ => unreachable!("Expected SubQueryReference, got: {ref_node:?}"),
                }
            }

            res.push((arg, aggr));
        }
        Ok(res)
    }

    /// Adds grouping expressions to columns of local projection.
    fn add_grouping_exprs_for_local_proj(
        &mut self,
        groupby_info: &mut GroupByInfo,
        output_cols: &mut Vec<NodeId>,
    ) -> Result<(), SbroadError> {
        // Map of { grouping_expr_alias -> proj_output_position }.
        let mut alias_to_pos: HashMap<Rc<String>, usize> = HashMap::with_capacity(EXPR_CAPACITY);
        // Add grouping expressions to local projection.
        for (pos, (gr_expr, local_alias)) in
            groupby_info.grouping_expr_to_alias_map.iter().enumerate()
        {
            let new_gr_expr = SubtreeCloner::clone_subtree(self, *gr_expr)?;
            let new_alias = self.nodes.add_alias(local_alias, new_gr_expr)?;
            output_cols.push(new_alias);
            alias_to_pos.insert(local_alias.clone(), pos);
        }
        // Note: we need to iterate only over grouping expressions that were present
        // in original user query here. We must not use the grouping expressions
        // that come from distinct aggregates. This is because they are handled separately:
        // local aliases map is needed only for GroupBy expressions in the original query and
        // grouping positions are used to create a Motion later, which should take into account
        // only positions from GroupBy expressions in the original user query.
        for expr_id in &groupby_info.grouping_exprs {
            let local_alias = groupby_info
                .grouping_expr_to_alias_map
                .get(expr_id)
                .expect("grouping expressions map should contain given expr_id")
                .clone();
            groupby_info
                .reduce_info
                .local_aliases_map
                .insert(*expr_id, local_alias.clone());
            let pos = alias_to_pos
                .get(&local_alias)
                .expect("alias map should contain given local alias");
            groupby_info.reduce_info.grouping_positions.push(*pos);
        }

        Ok(())
    }

    /// Creates columns for local projection
    ///
    /// local projection contains groupby columns + local aggregates,
    /// this function removes duplicated among them and creates the list for output
    /// `Row` for local projection.
    ///
    /// In case we have distinct aggregates and no groupby in original query,
    /// local `GroupBy` node will created.
    fn create_columns_for_local_proj(
        &mut self,
        aggrs: &mut [Aggregate],
        groupby_info: &mut Option<GroupByInfo>,
    ) -> Result<Vec<NodeId>, SbroadError> {
        let mut output_cols: Vec<NodeId> = vec![];

        if let Some(groupby_info) = groupby_info.as_mut() {
            self.add_grouping_exprs_for_local_proj(groupby_info, &mut output_cols)?;
        };

        self.add_local_aggregates(aggrs, &mut output_cols)?;

        Ok(output_cols)
    }

    /// Create Projection node for Map(local) stage of 2-stage aggregation
    ///
    /// # Arguments
    /// * `child` - id of child for Projection node to be created.
    /// * `aggrs` - vector of metadata for each aggregate function that was found in final
    ///   projection.
    ///
    /// Local Projection is created by creating columns for grouping exprs and columns
    /// for local aggregates. If there is `GroupBy` in the original query, then distinct
    /// expressions will be added to that. If there is no `GroupBy` in the original query
    /// then `child_id` refers to other node and in case there are distinct aggregates,
    /// `GroupBy` node will be created to contain expressions from distinct aggregates:
    ///
    /// E.g. for a query `select sum(distinct a + b) from t` plan before calling this function
    /// would look like:
    /// ```text
    /// - Projection sum(distinct a + b) from t
    /// -     Scan t
    /// ```
    /// After calling the this function:
    /// ```text
    /// - Projection sum(distinct a + b) from t <- did not changed
    /// -     Projection a + b as l1            <- created local Projection
    /// -         GroupBy a + b                 <- created a GroupBy node for distinct aggregate
    /// -             Scan t
    /// ```
    ///
    /// # Local aliases
    /// For each column in local `Projection` alias is created (see `grouping_expr_local_alias`).
    /// Aggregates encapsulate this logic in themselves (see `aggr_local_alias`).
    /// These local aliases are used later in 2-stage aggregation pipeline to replace
    /// original expressions in nodes like `Projection`, `Having`, `GroupBy`.
    ///
    /// E.g. if initially final Projection looked like
    /// ```text
    /// - Projection count(expr)
    /// -     ...
    /// ```
    /// When we create local Projection, we take expr from final Projection,
    /// and later(not in this function) replace expression in final
    /// Projection with corresponding local alias:
    /// ```text
    /// - Projection sum(l1)
    /// -     ...
    /// -         Projection count(expr) as l1 <- l1 - is generated local alias
    /// ```
    ///
    /// The same logic must be applied to any node in final stage of 2-stage aggregation:
    /// `Having`, `GroupBy`, `OrderBy`.
    fn add_local_projection(
        &mut self,
        child: NodeId,
        aggrs: &mut [Aggregate],
        groupby_info: &mut Option<GroupByInfo>,
        scalar_sqs: &OrderedSet<NodeId, RepeatableState>,
    ) -> Result<NodeId, SbroadError> {
        let proj_output_cols = self.create_columns_for_local_proj(aggrs, groupby_info)?;
        let proj_output: NodeId = self.nodes.add_row(proj_output_cols, None);

        let (mut children, having_id, group_by_id) = match self.get_relation_node(child)? {
            Relational::GroupBy(_) => (vec![], None, Some(child)),
            Relational::Having(_) => {
                let having = child;
                let group_by = self.get_first_rel_child(having)?;
                let Relational::GroupBy(_) = self.get_relation_node(group_by)? else {
                    return Err(SbroadError::NotImplemented(
                        Entity::Operator,
                        format_smolstr!("having for entire set group"),
                    ));
                };
                (vec![], Some(having), Some(group_by))
            }
            _ => (vec![child], None, None),
        };

        // Handle scalar subqueries which we have to move
        // from final Projection to this local one.
        children.extend(scalar_sqs.iter());

        let proj = Projection {
            output: proj_output,
            children,
            windows: vec![],
            is_distinct: false,
            group_by: group_by_id,
            having: having_id,
        };
        let proj_id = self.add_relational(proj.into())?;

        // Expressions used under newly created output are referencing final Projection. We
        // have to fix it so that they reference newly created Projection.

        self.set_target_in_subtree(proj_output, child)?;

        self.set_rel_output_distribution(proj_id)?;

        Ok(proj_id)
    }

    fn add_final_groupby(
        &mut self,
        upper_local_proj: NodeId,
        groupby_info: &GroupByInfo,
    ) -> Result<NodeId, SbroadError> {
        let grouping_exprs = &groupby_info.grouping_exprs;
        let local_aliases_map = &groupby_info.reduce_info.local_aliases_map;

        let mut gr_exprs: Vec<NodeId> = Vec::with_capacity(grouping_exprs.len());
        let child_map: ColumnPositionMap = ColumnPositionMap::new(self, upper_local_proj)?;
        let mut nodes = Vec::with_capacity(grouping_exprs.len());
        for expr_id in grouping_exprs {
            let Some(local_alias) = local_aliases_map.get(expr_id) else {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "could not find local alias for GroupBy expr ({expr_id:?})"
                    )),
                ));
            };
            let position = child_map.get(local_alias)?;
            let col_type = self.get_expression_node(*expr_id)?.calculate_type(self)?;
            if let Some(col_type) = col_type.get() {
                if !col_type.is_scalar() {
                    return Err(SbroadError::Invalid(
                        Entity::Type,
                        Some(format_smolstr!(
                            "add_final_groupby: GroupBy expr ({expr_id:?}) is not scalar ({col_type})!"
                        )),
                    ));
                }
            }
            let new_col = Reference {
                position,
                target: ReferenceTarget::Single(upper_local_proj),
                col_type,
                asterisk_source: None,
                is_system: false,
            };
            nodes.push(new_col);
        }
        for node in nodes {
            let new_col_id = self.nodes.push(node.into());
            gr_exprs.push(new_col_id);
        }
        let output = self.add_row_for_output(upper_local_proj, &[], true, None)?;

        // Because GroupBy node lies in the Arena64.
        let final_id = self.nodes.next_id(ArenaType::Arena64);
        let final_groupby = GroupBy {
            gr_exprs,
            children: vec![upper_local_proj],
            output,
        };
        self.add_relational(final_groupby.into())?;

        Ok(final_id)
    }

    /// Replace grouping expressions in finals with corresponding
    /// references to local aliases.
    ///
    /// For example:
    /// original query: `select a + b as user_alias from t group by a + b`
    /// map query: `select a + b as l1 from t group by a + b` `l1` is local alias
    /// reduce query: `select l1 as user_alias from tmp_space group by l1`
    /// In above example this function will replace `a + b` expression in final `Projection`
    #[allow(clippy::too_many_lines)]
    fn adjust_grouping_exprs_from_grby_info(
        &mut self,
        groupby_info: &GroupByInfo,
    ) -> Result<(), SbroadError> {
        let local_aliases_map = &groupby_info.reduce_info.local_aliases_map;
        let gr_exprs_map = &groupby_info.grouping_exprs_map;

        type RelationalID = NodeId;
        type GroupByExpressionID = NodeId;
        type ExpressionID = NodeId;
        type ExpressionParent = Option<NodeId>;
        // Map of { Relation -> vec![(
        //                           expr_id under group by
        //                           expr_id of the same expr under other relation (e.g. Projection)
        //                           parent expr over group by expr
        //                          )] }
        type ParentExpressionMap =
            HashMap<RelationalID, Vec<(GroupByExpressionID, ExpressionID, ExpressionParent)>>;
        let map: ParentExpressionMap = {
            let mut new_map: ParentExpressionMap = HashMap::with_capacity(gr_exprs_map.len());
            for (groupby_expr_id, locations) in gr_exprs_map {
                for location in locations {
                    let rec = (*groupby_expr_id, location.expr_id, location.parent_expr_id);
                    if let Some(u) = new_map.get_mut(&location.rel_id) {
                        u.push(rec);
                    } else {
                        new_map.insert(location.rel_id, vec![rec]);
                    }
                }
            }
            new_map
        };
        for (rel_id, group) in map {
            // E.g. GroupBy under final Projection.
            let child_id = self.get_first_rel_child(rel_id)?;
            let alias_to_pos_map = ColumnPositionMap::new(self, child_id)?;
            let mut nodes = Vec::with_capacity(group.len());
            for (gr_expr_id, expr_id, parent_expr_id) in group {
                let Some(local_alias) = local_aliases_map.get(&gr_expr_id) else {
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some(format_smolstr!(
                            "failed to find local alias for groupby expression {gr_expr_id:?}"
                        )),
                    ));
                };
                let position = alias_to_pos_map.get(local_alias)?;
                let col_type = self.get_expression_node(expr_id)?.calculate_type(self)?;
                if let Some(col_type) = col_type.get() {
                    if !col_type.is_scalar() {
                        return Err(SbroadError::Invalid(
                            Entity::Type,
                            Some(format_smolstr!(
                                "adjust_finals: expected scalar expression, found: {col_type}"
                            )),
                        ));
                    };
                }
                let new_ref = Reference {
                    target: ReferenceTarget::Single(child_id),
                    position,
                    col_type,
                    asterisk_source: None,
                    is_system: false,
                };
                nodes.push((parent_expr_id, expr_id, new_ref));
            }
            for (parent_expr_id, expr_id, node) in nodes {
                let ref_id = self.nodes.push(node.into());
                if let Some(parent_expr_id) = parent_expr_id {
                    self.replace_expression(parent_expr_id, expr_id, ref_id)?;
                } else {
                    // Grouping expression doesn't have parent grouping expression.
                    let rel_node = self.get_mut_relation_node(rel_id)?;
                    match rel_node {
                        MutRelational::Having(Having { filter, .. }) => {
                            // E.g. `select a from t group by a having a`.
                            if *filter == expr_id {
                                *filter = ref_id;
                            }
                        }
                        _ => {
                            // Currently Having is the only relational node in which grouping expression
                            // can not have a parent expression (under Projection all expressions are covered
                            // with output Row node).
                            panic!("Unexpected final node met for expression replacement: {rel_node:?}")
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Make Reduce (final) nodes in 2-stage aggregation valid, after Map (local) stage nodes were created.
    ///
    /// After reduce stage was created, finals nodes contain invalid
    /// references and aggregate functions. This function replaces
    /// grouping expressions with corresponding local aliases and
    /// replaces old aggregate functions with final aggregates.
    ///
    /// For example:
    /// original query: `select a, sum(b) from t group by a having count(distinct b) > 3`
    /// map: `select a as l1, b as l2, sum(b) as l3 from t group by a, b`
    /// reduce:  `select l1 as a, sum(l3) from t group by l1 having count(distinct l2) > 3`
    ///
    /// This function replaces `a` to `l1`, `sum(b)` to `sum(l3)`,
    /// `count(distinct b)` to `count(distinct l2)`
    ///
    /// # Arguments
    /// * `final_proj` - id of final Projection node
    /// * `upper_local_proj` - id of the upper local Projection node
    /// * `aggrs` - list of metadata about aggregates
    /// * `groupby_info` - information about Map (local) and Reduce (final) GroupBy
    /// * `scalar_sqs` - ScanSubQuery nodes with scalar results which we met earlier in Projection/GroupBy/Having
    fn adjust_finals(
        &mut self,
        final_proj: NodeId,
        upper_local_proj: NodeId,
        aggrs: &Vec<Aggregate>,
        groupby_info: &Option<GroupByInfo>,
        scalar_sqs: &OrderedSet<NodeId, RepeatableState>,
    ) -> Result<(), SbroadError> {
        let having = self.get_having(final_proj)?;
        let group_by = self.get_group_by(final_proj)?;

        match (having, group_by) {
            (None, None) => {
                self.set_relation_child(final_proj, 0, upper_local_proj)?;
            }
            (Some(having), None) => {
                self.set_relation_child(having, 0, upper_local_proj)?;
            }
            (Some(having), Some(group_by)) => {
                let output = self.add_row_for_output(group_by, &[], true, None)?;
                *self.get_mut_relation_node(having)?.mut_output() = output;

                self.set_relation_child(having, 0, group_by)?;
                self.set_relation_child(group_by, 0, upper_local_proj)?;
            }
            (None, Some(group_by)) => {
                self.set_relation_child(group_by, 0, upper_local_proj)?;
            }
        }

        // After we added a Map (local) stage, we need to
        // update output of Having in Reduce (final) stage.
        if let Some(having) = having {
            let Relational::Having(_) = self.get_relation_node(having)? else {
                unreachable!("expected Having IR node");
            };
            let child_id = self.get_first_rel_child(having)?;
            let output = self.add_row_for_output(child_id, &[], true, None)?;
            *self.get_mut_relation_node(having)?.mut_output() = output;
        }

        if let Some(groupby_info) = groupby_info {
            self.adjust_grouping_exprs_from_grby_info(groupby_info)?;
        }

        let mut parent_to_aggrs: HashMap<NodeId, Vec<Aggregate>> = HashMap::with_capacity(2);
        for aggr in aggrs {
            if let Some(parent_aggrs_vec) = parent_to_aggrs.get_mut(&aggr.parent_rel) {
                parent_aggrs_vec.push(aggr.clone());
            } else {
                parent_to_aggrs.insert(aggr.parent_rel, vec![aggr.clone()]);
            }
        }
        for (parent, aggrs) in parent_to_aggrs {
            let child_id = self.get_first_rel_child(parent)?;

            // We construct mapping { AggrKind -> Pos in the output }
            // out of maps
            // { AggrKind -> LocalAlias } and { LocalAlias -> Pos in the output }.
            let alias_to_pos_map: ColumnPositionMap = ColumnPositionMap::new(self, child_id)?;
            for aggr in aggrs {
                // Position in the output with aggregate kind.
                let pos_kinds = aggr.get_position_kinds(&alias_to_pos_map)?;
                let final_expr = aggr.create_final_aggregate_expr(self, pos_kinds)?;
                self.replace_expression(aggr.parent_expr, aggr.fun_id, final_expr)?;
            }
        }

        // In case final Projection or Having had an aggregate referencing scalar SubQuery,
        // we have to remove this SubQuery from children.
        // After this we have to fix `target` fields of references because of
        // children shift.
        // Note that we are doing it after there are no references that we've copied to other operators
        // left (e.g. for aggregates they are replaced with aliases above).

        let filter = |children: &Vec<NodeId>| -> Vec<NodeId> {
            let mut res = Vec::with_capacity(children.len());
            children.iter().for_each(|child_id| {
                if !scalar_sqs.contains_key(child_id) {
                    res.push(*child_id);
                }
            });
            res
        };

        if let Some(having) = having {
            if let Relational::Having(Having { children, .. }) = self.get_relation_node(having)? {
                let filtered_children = filter(children);
                let mut having_node = self.get_mut_relation_node(having)?;
                having_node.set_children(filtered_children);
            } else {
                unreachable!("expected Having IR node");
            }
        }

        if let Relational::Projection(Projection { children, .. }) =
            self.get_relation_node(final_proj)?
        {
            let filtered_children = filter(children);
            let mut proj_node = self.get_mut_relation_node(final_proj)?;
            proj_node.set_children(filtered_children);
        } else {
            unreachable!("expected Projection IR node");
        }

        Ok(())
    }

    /// Add Motion node between Reduce (final) and Map (local) Projections.
    ///
    /// # Errors:
    /// - cannot get Having from Projection
    /// - cannot get GroupBy from Projection
    /// - cannot insert Motion node with certain Strategy
    /// - cannot set distribution for output of Projection/GroupBy/Having
    ///
    /// # Returns:
    /// - nothing if succeed
    fn add_motion_to_two_stage(
        &mut self,
        final_proj: NodeId,
        upper_local_proj: NodeId,
    ) -> Result<(), SbroadError> {
        let final_having = self.get_having(final_proj)?;
        let final_group_by = self.get_group_by(final_proj)?;

        let lower_final_node = match (final_having, final_group_by) {
            (None, None) => final_proj,
            (Some(id), None) => id,
            (_, Some(id)) => id,
        };

        let mut strategy = Strategy::new(lower_final_node);
        strategy.upsert_child(upper_local_proj, MotionPolicy::Full, Program::default());
        self.insert_motion_nodes(strategy)?;

        self.set_dist(
            self.get_relational_output(final_proj)?,
            Distribution::Single,
        )?;
        if let Some(id) = final_group_by {
            self.set_dist(self.get_relational_output(id)?, Distribution::Single)?;
        }
        if let Some(id) = final_having {
            self.set_dist(self.get_relational_output(id)?, Distribution::Single)?;
        }

        Ok(())
    }

    /// Create Motion nodes for scalar subqueries present under Having node.
    ///
    /// # Errors:
    /// - cannot calculate Motion strategies (`resolve_sq_conflicts`).
    /// - cannot insert Motion nodes for certain Strategy
    /// - cannot set node distribution for Having output
    ///
    /// # Returns:
    /// - nothing if succeed
    fn adjust_sqs_under_having(&mut self, having: NodeId) -> Result<(), SbroadError> {
        if let Relational::Having(Having { filter, output, .. }) = self.get_relation_node(having)? {
            let (filter, output) = (*filter, *output);
            let strategy = self.resolve_sq_conflicts(having, filter)?;

            let correct_rel_ids = strategy.get_rel_ids();
            self.insert_motion_nodes(strategy)?;
            self.adjust_sqs_of_rel_node(having, &correct_rel_ids)?;

            self.try_dist_from_subqueries(having, output)?;
        } else {
            unreachable!("Expected Having IR node");
        }
        Ok(())
    }

    fn collect_sqs_from_aggregate(
        &self,
        aggr: &Aggregate,
        scalar_sqs: &mut OrderedSet<NodeId, RepeatableState>,
    ) -> Result<(), SbroadError> {
        // All aggregates have no more than 1 argument
        let arg: NodeId = *self
            .nodes
            .expr_iter(aggr.fun_id, false)
            .collect::<Vec<NodeId>>()
            .first()
            .expect("Number of args for aggregate should have been already checked");

        for ref_id in self.get_sq_refs_from_subtree(arg)? {
            let ref_node = self.get_expression_node(ref_id)?;
            match ref_node {
                Expression::SubQueryReference(SubQueryReference { rel_id, .. }) => {
                    scalar_sqs.insert(*rel_id);
                }
                _ => unreachable!("Expected SubQueryReference IR node, got: {ref_node:?}"),
            }
        }

        Ok(())
    }

    /// Handle GroupBy, aggregates and distinct qualifier.
    /// Create additional relational nodes: Projection, GroupBy, Motion.
    ///
    /// # Terms
    /// - Map - local stage (on storage)
    /// - Reduce - final stage (on router)
    ///
    /// On a Map stage on each storage we execute:
    /// * GroupBy which is pushed down in certain cases
    /// * Aggregate calculation
    ///
    /// On a Reduce stage we finalize calculation by gathering all the results
    /// and applying the same logic (grouping or aggregation) on a whole set of
    /// data. Map stage is present in order not to apply all the calculations
    /// (e.g. aggregates) on the router but spread them between all instances.
    ///
    /// # Returns
    /// - `true` if there are any aggregate functions, distinct qualifier or `GroupBy` is present. Otherwise, `false`.
    pub fn add_two_stage_aggregation(&mut self, final_proj: NodeId) -> Result<bool, SbroadError> {
        let mut groupby_info = {
            let gb_id = self.get_group_by(final_proj)?;
            gb_id.map(GroupByInfo::new)
        };

        let final_having = self.get_having(final_proj)?;
        let mut upper_local_node = match final_having {
            Some(id) => self.get_first_rel_child(id)?,
            None => self.get_first_rel_child(final_proj)?,
        };
        let mut aggrs = self.collect_aggregates(final_proj, final_having)?;

        // In case scalar sq are met in queries like
        // * `select distinct (select 1) from t`
        // * `select sum((select 1)) from t`
        // they should be moved from final Projection/Having to the local Projection/GroupBy
        // (we remove them from additional children of final node when call `adjust_finals`).
        //
        // Here is a map of { (ref_parent, ref_target) -> sq_id }.
        // Mapping is needed only for the stage of local targets fixing (not for final nodes fixing).
        let mut scalar_sqs = OrderedSet::with_hasher(RepeatableState);

        if groupby_info.is_none() && aggrs.is_empty() {
            if let Some(distinct_group_by) =
                self.add_group_by_for_distinct(final_proj, upper_local_node, &mut scalar_sqs)?
            {
                upper_local_node = distinct_group_by;

                // In case aggregates or GroupBy are present, "distinct" qualifier under
                // Projection doesn't add any new features to the plan. Otherwise, we should add
                // a new GroupBy node for a Map (local) stage.
                //
                // Example: `select distinct a, b + 42 from t`.
                if let MutRelational::Projection(Projection {
                    group_by, children, ..
                }) = self.get_mut_relation_node(final_proj)?
                {
                    if group_by.is_none() {
                        // Remove the first child because it is linked through the `distinct_group_by` GroupBy IR node
                        children.remove(0);
                    }
                    *group_by = Some(distinct_group_by);
                }

                if final_having.is_none() {
                    self.set_target_in_subtree(
                        self.get_relational_output(final_proj)?,
                        distinct_group_by,
                    )?;
                }

                groupby_info = Some(GroupByInfo::new(distinct_group_by));
            } else {
                // Query doesn't contain GroupBy, aggregates or "distinct" qualifier.
                //
                // Example: `select a, b + 42 from t`
                return Ok(false);
            }
        }

        if groupby_info.is_none() {
            self.check_refs_out_of_aggregates(final_proj)?;
        }

        let distinct_aggrs_are_present = aggrs.iter().any(|a| a.is_distinct);
        if groupby_info.is_none() && distinct_aggrs_are_present {
            // GroupBy doesn't exist and we have to create it for distinct aggregates.
            //
            // Example: `select sum(distinct a) from t`
            //
            // Currently it's the only case when GroupBy will be present
            // on a Map (local) stage, but will not be generated for Reduce (final) stage.

            let group_by_id = self.add_groupby(upper_local_node, &[])?;
            upper_local_node = group_by_id;
            groupby_info = Some(GroupByInfo::new(upper_local_node));
        }

        // Note: All the cases in which a local GroupBy node is created (in
        //       case there are no user-defined GroupBy) are handled above.
        //       In case some new logic is added that requires generation of a
        //       local GroupBy node, please try to implement it above (so that
        //       such node creation is located in one place).

        self.collect_grouping_exprs(&mut groupby_info, &mut aggrs, final_proj, &mut scalar_sqs)?;

        if let Some(groupby_info) = groupby_info.as_ref() {
            if !groupby_info.grouping_exprs.is_empty()
                && self.check_bucket_id_under_group_by(&groupby_info.grouping_exprs)?
            {
                return Ok(false);
            }
        }

        // We have to move scalar subqueries from non distinct aggregates to local Projection.
        for aggr in aggrs.iter().filter(|x| !x.is_distinct) {
            self.collect_sqs_from_aggregate(aggr, &mut scalar_sqs)?;
        }

        let upper_local_proj = self.add_local_projection(
            upper_local_node,
            &mut aggrs,
            &mut groupby_info,
            &scalar_sqs,
        )?;

        if let Some(groupby_info) = groupby_info.as_ref() {
            if !groupby_info.grouping_exprs.is_empty() {
                let final_group_by = self.add_final_groupby(upper_local_proj, groupby_info)?;
                if let MutRelational::Projection(Projection { group_by, .. }) =
                    self.get_mut_relation_node(final_proj)?
                {
                    *group_by = Some(final_group_by);
                } else {
                    unreachable!("expected Projection IR node")
                }
            }
        }

        self.adjust_finals(
            final_proj,
            upper_local_proj,
            &aggrs,
            &groupby_info,
            &scalar_sqs,
        )?;

        self.add_motion_to_two_stage(final_proj, upper_local_proj)?;

        if let Some(id) = final_having {
            self.adjust_sqs_under_having(id)?;
        }

        Ok(true)
    }
}
