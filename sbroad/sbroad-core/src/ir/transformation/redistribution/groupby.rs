use smol_str::{format_smolstr, ToSmolStr};

use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::to_user;
use crate::frontend::sql::ir::SubtreeCloner;
use crate::ir::aggregates::{generate_local_alias_for_aggr, AggregateKind, SimpleAggregate};
use crate::ir::distribution::Distribution;
use crate::ir::expression::{ColumnPositionMap, Comparator, FunctionFeature, EXPR_HASH_DEPTH};
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{
    Alias, ArenaType, GroupBy, Having, NodeId, Projection, Reference, StableFunction,
};
use crate::ir::relation::Type;
use crate::ir::transformation::redistribution::{
    MotionKey, MotionPolicy, Program, Strategy, Target,
};
use crate::ir::tree::traversal::{BreadthFirst, PostOrderWithFilter, EXPR_CAPACITY};
use crate::ir::{Node, Plan};
use std::collections::{HashMap, HashSet};

use crate::ir::function::{Behavior, Function};
use crate::ir::helpers::RepeatableState;
use crate::utils::{OrderedMap, OrderedSet};
use std::hash::{Hash, Hasher};
use std::rc::Rc;

const AGGR_CAPACITY: usize = 10;

/// Helper struct to store metadata about aggregates
#[derive(Clone, Debug)]
struct AggrInfo {
    /// id of Relational node in which this aggregate is located.
    /// It can be located in `Projection`, `Having`, `OrderBy`
    parent_rel: NodeId,
    /// id of parent expression of aggregate function,
    /// if there is no parent it's `None`
    parent_expr: Option<NodeId>,
    /// info about what aggregate it is: sum, count, ...
    aggr: SimpleAggregate,
    /// whether this aggregate was marked distinct in original user query
    is_distinct: bool,
}

/// Helper struct to find aggregates in expressions of finals
struct AggrCollector<'plan> {
    /// id of final node in which matches are searched
    parent_rel: Option<NodeId>,
    /// collected aggregates
    infos: Vec<AggrInfo>,
    plan: &'plan Plan,
}

/// Helper struct to hold information about
/// location of grouping expressions used in
/// nodes other than `GroupBy`.
///
/// For example grouping expressions can appear
/// in `Projection`, `Having`, `OrderBy`
struct ExpressionLocationIds {
    pub parent_expr: Option<NodeId>,
    pub expr: NodeId,
    pub rel: NodeId,
}

impl ExpressionLocationIds {
    pub fn new(expr_id: NodeId, parent_expr_id: Option<NodeId>, rel_id: NodeId) -> Self {
        ExpressionLocationIds {
            parent_expr: parent_expr_id,
            expr: expr_id,
            rel: rel_id,
        }
    }
}

/// Helper struct to filter duplicate aggregates in local stage.
///
/// Consider user query: `select sum(a), avg(a) from t`
/// at local stage we need to compute `sum(a)` only once.
///
/// This struct contains info needed to compute hash and compare aggregates
/// used at local stage.
struct AggregateSignature<'plan, 'args> {
    pub kind: AggregateKind,
    /// ids of expressions used as arguments to aggregate
    pub arguments: &'args Vec<NodeId>,
    pub plan: &'plan Plan,
    /// reference to local alias of this local aggregate
    pub local_alias: Option<Rc<String>>,
}

impl<'plan, 'args> AggregateSignature<'plan, 'args> {
    pub fn get_alias(&self) -> Result<Rc<String>, SbroadError> {
        let r = self
            .local_alias
            .as_ref()
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::AggregateSignature,
                    Some("missing local alias".into()),
                )
            })?
            .clone();
        Ok(r)
    }
}

impl<'plan, 'args> Hash for AggregateSignature<'plan, 'args> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        let mut comp = Comparator::new(self.plan);
        comp.set_hasher(state);
        for arg in self.arguments {
            comp.hash_for_expr(*arg, EXPR_HASH_DEPTH);
        }
    }
}

impl<'plan, 'args> PartialEq<Self> for AggregateSignature<'plan, 'args> {
    fn eq(&self, other: &Self) -> bool {
        let comparator = Comparator::new(self.plan);
        self.kind == other.kind
            && self
                .arguments
                .iter()
                .zip(other.arguments.iter())
                .all(|(l, r)| comparator.are_subtrees_equal(*l, *r).unwrap_or(false))
    }
}

impl<'plan, 'args> Eq for AggregateSignature<'plan, 'args> {}

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

impl<'plan> Hash for GroupingExpression<'plan> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut comp = Comparator::new(self.plan);
        comp.set_hasher(state);
        comp.hash_for_expr(self.id, EXPR_HASH_DEPTH);
    }
}

impl<'plan> PartialEq for GroupingExpression<'plan> {
    fn eq(&self, other: &Self) -> bool {
        let comp = Comparator::new(self.plan);
        comp.are_subtrees_equal(self.id, other.id).unwrap_or(false)
    }
}

impl<'plan> Eq for GroupingExpression<'plan> {}

impl<'plan> AggrCollector<'plan> {
    pub fn with_capacity(plan: &'plan Plan, capacity: usize) -> AggrCollector<'plan> {
        AggrCollector {
            infos: Vec::with_capacity(capacity),
            parent_rel: None,
            plan,
        }
    }

    pub fn take_aggregates(&mut self) -> Vec<AggrInfo> {
        std::mem::take(&mut self.infos)
    }

    /// Collect aggregates in internal field by traversing expression tree `top`
    ///
    /// # Arguments
    /// * `top` - id of expression root in which to look for aggregates
    /// * `parent_rel` - id of parent relational node, where `top` is located. It is used to
    ///   create `AggrInfo`
    ///
    /// # Errors
    /// - invalid expression tree pointed by `top`
    pub fn collect_aggregates(
        &mut self,
        top: NodeId,
        parent_rel: NodeId,
    ) -> Result<(), SbroadError> {
        self.parent_rel = Some(parent_rel);
        self.find(top, None)?;
        self.parent_rel = None;
        Ok(())
    }

    fn find(&mut self, current: NodeId, parent: Option<NodeId>) -> Result<(), SbroadError> {
        let expr = self.plan.get_expression_node(current)?;
        if let Expression::StableFunction(StableFunction { name, feature, .. }) = expr {
            let is_distinct = matches!(feature, Some(FunctionFeature::Distinct));
            if let Some(aggr) = SimpleAggregate::new(name, current) {
                let Some(parent_rel) = self.parent_rel else {
                    return Err(SbroadError::Invalid(Entity::AggregateCollector, None));
                };
                let info = AggrInfo {
                    parent_rel,
                    parent_expr: parent,
                    aggr,
                    is_distinct,
                };
                self.infos.push(info);
                return Ok(());
            };
        }
        for child in self.plan.nodes.expr_iter(current, false) {
            self.find(child, Some(current))?;
        }
        Ok(())
    }
}

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
/// stage of `collect_grouping_expressions`.
type GroupbyExpressionsMap = HashMap<NodeId, Vec<ExpressionLocationIds>>;
/// Maps id of `GroupBy` expression used in `GroupBy` (from local stage)
/// to corresponding local alias used in local Projection. Note:
/// this map does not contain mappings between grouping expressions from
/// distinct aggregates (it is stored in corresponding `AggrInfo` for that
/// aggregate)
///
/// For example:
/// initial query: `select a, count(distinct b) from t group by a`
/// map query: `select a as l1, b group by a, b`
/// Then this map will map id of `a` to `l1`
type LocalAliasesMap = HashMap<NodeId, Rc<String>>;
type LocalAggrInfo = (AggregateKind, Vec<NodeId>, Rc<String>);

/// Helper struct to map expressions used in `GroupBy` to
/// expressions used in some other node (`Projection`, `Having`, `OrderBy`)
struct ExpressionMapper<'plan> {
    /// List of expressions ids of `GroupBy`
    gr_exprs: &'plan Vec<NodeId>,
    map: GroupbyExpressionsMap,
    plan: &'plan Plan,
    /// Id of relational node (`Projection`, `Having`, `OrderBy`)
    node_id: Option<NodeId>,
}

impl<'plan> ExpressionMapper<'plan> {
    fn new(gr_expressions: &'plan Vec<NodeId>, plan: &'plan Plan) -> ExpressionMapper<'plan> {
        let map: GroupbyExpressionsMap = HashMap::new();
        ExpressionMapper {
            gr_exprs: gr_expressions,
            map,
            plan,
            node_id: None,
        }
    }

    /// Traverses given expression from top to bottom, trying
    /// to find subexpressions that match expressions located in `GroupBy`,
    /// when match is found it is stored in map passed to [`ExpressionMapper`]'s
    /// constructor.
    ///
    /// # Arguments
    /// * `expr_root` - expression id from which matching will start
    /// * `node_id` - id of relational node (`Having`, `Projection`, `OrderBy`),
    ///   where expression pointed by `expr_root` is located
    ///
    /// # Errors
    /// - invalid references in any expression (`GroupBy`'s or node's one)
    /// - invalid query: node expression contains references that are not
    ///   found in `GroupBy` expression. The reason is that user specified expression in
    ///   node that does not match any expression in `GroupBy`
    fn find_matches(&mut self, expr_root: NodeId, node_id: NodeId) -> Result<(), SbroadError> {
        self.node_id = Some(node_id);
        self.find(expr_root, None)?;
        self.node_id = None;
        Ok(())
    }

    /// Helper function for `find_matches` which compares current node to `GroupBy` expressions
    /// and if no match is found recursively calls itself.
    fn find(&mut self, current: NodeId, parent: Option<NodeId>) -> Result<(), SbroadError> {
        let Some(node_id) = self.node_id else {
            return Err(SbroadError::Invalid(Entity::ExpressionMapper, None));
        };
        let is_ref = matches!(
            self.plan.get_expression_node(current),
            Ok(Expression::Reference(_))
        );
        let is_sq_ref = is_ref
            && self.plan.is_additional_child_of_rel(
                node_id,
                self.plan.get_relational_from_reference_node(current)?,
            )?;
        // Because subqueries are replaced with References, we must not
        // try to match these references against any GroupBy expressions
        if is_sq_ref {
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
            let location = ExpressionLocationIds::new(current, parent, node_id);
            if let Some(v) = self.map.get_mut(&gr_expr) {
                v.push(location);
            } else {
                self.map.insert(gr_expr, vec![location]);
            }
            return Ok(());
        }
        if is_ref {
            // We found a column which is not inside aggregate function
            // and it is not a grouping expression:
            // select a from t group by b - is invalid
            let column_name = {
                let node = self.plan.get_expression_node(current)?;
                self.plan
                    .get_alias_from_reference_node(&node)
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

    pub fn get_matches(&mut self) -> GroupbyExpressionsMap {
        std::mem::take(&mut self.map)
    }
}

impl Plan {
    #[allow(unreachable_code)]
    fn generate_local_alias(id: NodeId) -> String {
        #[cfg(feature = "mock")]
        {
            return format!("column_{id}");
        }
        format!("{}_{id}", uuid::Uuid::new_v4().as_simple())
    }

    /// Used to create a `GroupBy` IR node from AST.
    /// The added `GroupBy` node is local - meaning
    /// that it is part of local stage in 2-stage
    /// aggregation. For more info, see `add_two_stage_aggregation`.
    ///
    /// # Arguments
    /// * `children` - plan's ids of `group by` children from AST
    ///
    /// # Errors
    /// - invalid children count
    /// - failed to create output for `GroupBy`
    pub fn add_groupby_from_ast(&mut self, children: &[NodeId]) -> Result<NodeId, SbroadError> {
        let Some((first_child, other)) = children.split_first() else {
            return Err(SbroadError::UnexpectedNumberOfValues(
                "GroupBy ast has no children".into(),
            ));
        };

        // Check grouping expression:
        // 1) aggregates are not allowed
        // 2) must contain at least one column (group by 1 - is not valid)
        for (pos, grouping_expr_id) in other.iter().enumerate() {
            let filter = |node_id: NodeId| -> bool {
                matches!(
                    self.get_node(node_id),
                    Ok(Node::Expression(
                        Expression::StableFunction(_) | Expression::Reference(_)
                    ))
                )
            };
            let mut dfs = PostOrderWithFilter::with_capacity(
                |x| self.nodes.expr_iter(x, false),
                EXPR_CAPACITY,
                Box::new(filter),
            );
            let mut contains_at_least_one_col = false;
            for level_node in dfs.iter(*grouping_expr_id) {
                let node_id = level_node.1;
                let node = self.get_node(node_id)?;
                match node {
                    Node::Expression(Expression::Reference(_)) => {
                        contains_at_least_one_col = true;
                    }
                    Node::Expression(Expression::StableFunction(StableFunction {
                        name, ..
                    })) => {
                        if Expression::is_aggregate_name(name) {
                            return Err(SbroadError::Invalid(
                                Entity::Query,
                                Some(format_smolstr!("aggregate functions are not allowed inside grouping expression. Got aggregate: {name}"))
                            ));
                        }
                    }
                    _ => {}
                }
            }
            if !contains_at_least_one_col {
                return Err(SbroadError::Invalid(
                    Entity::Query,
                    Some(format_smolstr!("grouping expression must contain at least one column. Invalid expression number: {pos:?}"))
                ));
            }
        }

        let groupby_id = self.add_groupby(*first_child, other, false, None)?;
        Ok(groupby_id)
    }

    /// Helper function to add `group by` to IR
    ///
    /// # Errors
    /// - `child_id` - invalid `Relational` node
    /// - `grouping_exprs` - contains non-expr id
    pub fn add_groupby(
        &mut self,
        child_id: NodeId,
        grouping_exprs: &[NodeId],
        is_final: bool,
        expr_parent: Option<NodeId>,
    ) -> Result<NodeId, SbroadError> {
        let final_output = self.add_row_for_output(child_id, &[], true, None)?;
        let groupby = GroupBy {
            children: [child_id].to_vec(),
            gr_cols: grouping_exprs.to_vec(),
            output: final_output,
            is_final,
        };

        let groupby_id = self.add_relational(groupby.into())?;

        self.replace_parent_in_subtree(final_output, None, Some(groupby_id))?;
        for expr in grouping_exprs {
            self.replace_parent_in_subtree(*expr, expr_parent, Some(groupby_id))?;
        }

        Ok(groupby_id)
    }

    /// Collect information about aggregates
    ///
    /// Aggregates can appear in `Projection`, `Having`, `OrderBy`
    ///
    /// # Arguments
    /// [`finals`] - ids of nodes in final (reduce stage) before adding two stage aggregation.
    /// It may contain ids of `Projection`, `Having`, `Limit`, `OrderBy`.
    /// Note: final `GroupBy` is not present because it will be added later in 2-stage pipeline.
    fn collect_aggregates(&self, finals: &Vec<NodeId>) -> Result<Vec<AggrInfo>, SbroadError> {
        let mut collector = AggrCollector::with_capacity(self, AGGR_CAPACITY);
        for node_id in finals {
            let node = self.get_relation_node(*node_id)?;
            match node {
                Relational::Projection(Projection { output, .. }) => {
                    for col in self.get_row_list(*output)? {
                        collector.collect_aggregates(*col, *node_id)?;
                    }
                }
                Relational::Having(Having { filter, .. }) => {
                    collector.collect_aggregates(*filter, *node_id)?;
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some(format_smolstr!(
                            "unexpected relational node ({node_id:?}): {node:?}"
                        )),
                    ))
                }
            }
        }

        let aggr_infos = collector.take_aggregates();
        self.validate_aggregates(&aggr_infos)?;

        Ok(aggr_infos)
    }

    /// Validates expressions used in aggregates
    ///
    /// Currently we only check that there is no aggregates inside aggregates
    fn validate_aggregates(&self, aggr_infos: &Vec<AggrInfo>) -> Result<(), SbroadError> {
        for info in aggr_infos {
            let top = info.aggr.fun_id;
            if self.contains_aggregates(top, false)? {
                return Err(SbroadError::Invalid(
                    Entity::Query,
                    Some("aggregate function inside aggregate function is not allowed.".into()),
                ));
            }
        }

        Ok(())
    }

    /// Get ids of nodes in Reduce stage (finals) and id of the top node in Map stage.
    ///
    /// Finals are nodes in Reduce stage without final `GroupBy`.
    ///
    /// # Example
    /// original query: `select sum(a), b from t group by by t having sum(a) > 1`
    /// Approximate plan before adding 2-stage aggregation:
    /// ```txt
    /// Projection (1)
    ///     Having (2)
    ///         GroupBy (3)
    ///             Scan (4)
    /// ```
    /// Then this function will return `([1, 2], 4)`
    fn split_reduce_stage(
        &self,
        final_proj_id: NodeId,
    ) -> Result<(Vec<NodeId>, NodeId), SbroadError> {
        let mut finals: Vec<NodeId> = vec![];
        let get_first_child = |rel_id: NodeId| -> Result<NodeId, SbroadError> {
            let c = *self
                .get_relational_children(rel_id)?
                .get(0)
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "expected relation node ({rel_id:?}) to have children!"
                    ))
                })?;
            Ok(c)
        };
        let mut next = final_proj_id;
        let max_reduce_nodes = 2;
        for _ in 0..=max_reduce_nodes {
            match self.get_relation_node(next)? {
                Relational::Projection(_) | Relational::Having(_) => {
                    finals.push(next);
                    next = get_first_child(next)?;
                }
                _ => return Ok((finals, next)),
            }
        }
        Err(SbroadError::Invalid(
            Entity::Plan,
            Some(format_smolstr!(
                "too many nodes ({}) in Reduce stage",
                finals.len()
            )),
        ))
    }

    /// Collects information about grouping expressions for future use.
    /// In case there is a `Projection` with `distinct` modifier and
    /// no `GroupBy` node, a `GroupBy` node with projection expressions
    /// will be created.
    /// This function also does all the validation of incorrect usage of
    /// expressions used outside of aggregate functions.
    ///
    /// # Returns
    /// - id of `GroupBy` node if is was created or `upper` otherwise
    /// - list of ids of expressions used in `GroupBy`. Duplicate expressions are removed.
    /// - mapping between `GroupBy` expressions and corresponding expressions in final nodes
    ///   (`Projection`, `Having`, `GroupBy`, `OrderBy`).
    ///
    /// # Arguments
    /// * `upper` - id of the top node in reduce stage, if `GroupBy` is present in the query
    ///   the top node in Reduce stage will be `GroupBy`.
    /// * `finals` - ids of nodes in final stage starting from `Projection`
    ///
    /// # Errors
    /// - invalid references in `GroupBy`
    /// - invalid query with `GroupBy`: some expression in some final node wasn't matched to
    ///   some `GroupBy` expression
    /// - invalid query without `GroupBy` and with aggregates: there are column references outside
    ///   aggregate functions
    /// - invalid query with `Having`: in case there's no `GroupBy`, `Having` may contain
    ///   only expressions with constants and aggregates. References outside of aggregate functions
    ///   are illegal.
    #[allow(clippy::too_many_lines)]
    fn collect_grouping_expressions(
        &mut self,
        upper: NodeId,
        finals: &Vec<NodeId>,
        has_aggregates: bool,
    ) -> Result<(NodeId, Vec<NodeId>, GroupbyExpressionsMap), SbroadError> {
        let mut grouping_expr = vec![];
        let mut gr_expr_map: GroupbyExpressionsMap = HashMap::new();
        let mut upper = upper;

        let mut has_groupby = matches!(self.get_relation_node(upper)?, Relational::GroupBy(_));

        if !has_groupby && !has_aggregates {
            if let Some(proj_id) = finals.first() {
                if let Relational::Projection(Projection {
                    is_distinct,
                    output,
                    ..
                }) = self.get_relation_node(*proj_id)?
                {
                    if *is_distinct {
                        let proj_cols_len = self.get_row_list(*output)?.len();
                        let mut grouping_exprs: Vec<NodeId> = Vec::with_capacity(proj_cols_len);
                        for i in 0..proj_cols_len {
                            let aliased_col = self.get_proj_col(*proj_id, i)?;
                            let proj_col_id = if let Expression::Alias(Alias { child, .. }) =
                                self.get_expression_node(aliased_col)?
                            {
                                *child
                            } else {
                                aliased_col
                            };
                            // Copy expression from Projection to GroupBy.
                            let col = SubtreeCloner::clone_subtree(self, proj_col_id)?;
                            grouping_exprs.push(col);
                        }
                        upper = self.add_groupby(upper, &grouping_exprs, false, Some(*proj_id))?;

                        has_groupby = true;
                    }
                }
            }
        }

        if has_groupby {
            let old_gr_cols = self.get_grouping_cols(upper)?;
            // remove duplicate expressions
            let mut unique_grouping_exprs: OrderedSet<GroupingExpression, _> =
                OrderedSet::with_capacity_and_hasher(old_gr_cols.len(), RepeatableState);
            for gr_expr in old_gr_cols {
                unique_grouping_exprs.insert(GroupingExpression::new(*gr_expr, self));
            }
            let grouping_exprs: Vec<NodeId> = unique_grouping_exprs.iter().map(|e| e.id).collect();
            grouping_expr.extend(grouping_exprs.iter());
            self.set_grouping_cols(upper, grouping_exprs)?;

            let mut mapper = ExpressionMapper::new(&grouping_expr, self);
            for node_id in finals {
                match self.get_relation_node(*node_id)? {
                    Relational::Projection(Projection { output, .. }) => {
                        for col in self.get_row_list(*output)? {
                            mapper.find_matches(*col, *node_id)?;
                        }
                    }
                    Relational::Having(Having { filter, .. }) => {
                        mapper.find_matches(*filter, *node_id)?;
                    }
                    _ => {}
                }
            }
            gr_expr_map = mapper.get_matches();
        }

        if has_aggregates && !has_groupby {
            // check that all column references are inside aggregate functions
            for id in finals {
                let node = self.get_relation_node(*id)?;
                match node {
                    Relational::Projection(Projection { output, .. }) => {
                        for col in self.get_row_list(*output)? {
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
                            for level_node in nodes {
                                let id = level_node.1;
                                let n = self.get_expression_node(id)?;
                                if let Expression::Reference(_) = n {
                                    let alias = match self.get_alias_from_reference_node(&n) {
                                        Ok(v) => v.to_smolstr(),
                                        Err(e) => e.to_smolstr(),
                                    };
                                    return Err(SbroadError::Invalid(Entity::Query,
                                                                    Some(format_smolstr!("found column reference ({}) outside aggregate function", to_user(alias)))));
                                }
                            }
                        }
                    }
                    Relational::Having(Having { filter, .. }) => {
                        let mut bfs = BreadthFirst::with_capacity(
                            |x| self.nodes.aggregate_iter(x, false),
                            EXPR_CAPACITY,
                            EXPR_CAPACITY,
                        );
                        bfs.populate_nodes(*filter);
                        let nodes = bfs.take_nodes();
                        for level_node in nodes {
                            let id = level_node.1;
                            if let Expression::Reference(_) = self.get_expression_node(id)? {
                                return Err(SbroadError::Invalid(
                                    Entity::Query,
                                    Some("HAVING argument must appear in the GROUP BY clause or be used in an aggregate function".into())
                                ));
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok((upper, grouping_expr, gr_expr_map))
    }

    /// Add expressions used as arguments to distinct aggregates to `GroupBy` in reduce stage
    ///
    /// E.g: For query below, this func should add b*b to reduce `GroupBy`
    /// `select a, sum(distinct b*b), count(c) from t group by a`
    /// Map: `select a as l1, b*b as l2, count(c) as l3 from t group by a, b`
    /// Reduce: `select l1, sum(distinct l2), sum(l3) from tmp_space group by l1`
    fn add_distinct_aggregates_to_local_groupby(
        &mut self,
        upper: NodeId,
        additional_grouping_exprs: Vec<NodeId>,
    ) -> Result<NodeId, SbroadError> {
        let mut local_proj_child_id = upper;
        if !additional_grouping_exprs.is_empty() {
            if let MutRelational::GroupBy(GroupBy { gr_cols, .. }) =
                self.get_mut_relation_node(local_proj_child_id)?
            {
                gr_cols.extend(additional_grouping_exprs);
            } else {
                local_proj_child_id =
                    self.add_groupby(upper, &additional_grouping_exprs, true, None)?;
                self.set_distribution(self.get_relational_output(local_proj_child_id)?)?;
            }
        }
        Ok(local_proj_child_id)
    }

    /// Create Projection node for Map(local) stage of 2-stage aggregation
    ///
    /// # Arguments
    ///
    /// * `child_id` - id of child for Projection node to be created.
    /// * `aggr_infos` - vector of metadata for each aggregate function that was found in final
    ///   projection. Each info specifies what kind of aggregate it is (sum, avg, etc) and location
    ///   in final projection.
    /// * `grouping_exprs` - ids of grouping expressions from local `GroupBy`, empty if there is
    ///   no `GroupBy` in original query.
    /// * `finals` - ids of nodes from final stage, starting from `Projection`.
    ///   Final stage may contain `Projection`, `Limit`, `OrderBy`, `Having` nodes.
    ///
    /// Local Projection is created by creating columns for grouping expressions and columns
    /// for local aggregates. If there is no `GroupBy` in the original query then `child_id` refers
    /// to other node and in case there are distinct aggregates, `GroupBy` node will be created
    /// to contain expressions from distinct aggregates:
    /// ```text
    /// select sum(distinct a + b) from t
    /// // Plan before calling this function:
    /// Projection sum(distinct a + b) from t
    ///     Scan t
    /// // After calling this function
    /// Projection sum(distinct a + b) from t <- did not changed
    ///     Projection a + b as l1 <- created local Projection
    ///         GroupBy a <- created a GroupBy node for distinct aggregate
    ///             Scan t
    /// ```
    ///
    /// If there is `GroupBy` in the original query, then distinct expressions will be added
    /// to that.
    ///
    /// # Local aliases
    /// For each column in local `Projection` alias is created. It is generated
    /// as `{uuid}_{node_id}`, for the purpose of not matching to some of the user aliases.
    /// Aggregates encapsulate this logic in themselves, see [`create_columns_for_local_proj`]
    /// of [`SimpleAggregate`]. For grouping expressions it is done manually using
    /// [`generate_local_alias`].
    /// These local aliases are used later in 2-stage aggregation pipeline to replace
    /// original expressions in nodes like `Projection`, `Having`, `GroupBy`. For example:
    /// ```text
    /// // initially final Projection
    /// Projection count(expr)
    /// // when we create local Projection, we take expr from final Projection,
    /// // and later(not in this function) replace expression in final
    /// // Projection with corresponding local alias:
    /// Projection sum(l1)
    ///     ...
    ///         Projection count(expr) as l1 // l1 - is generated local alias
    /// ```
    /// The same logic must be applied to any node in final stage of 2-stage aggregation:
    /// `Having`, `GroupBy`, `OrderBy`. See [`add_two_stage_aggregation`] for more details.
    ///
    /// # Returns
    /// - id of local `Projection` that was created.
    /// - vector of positions by which `GroupBy` is done. Positions are relative to local `Projection`
    ///   output.
    /// - map between `GroupBy` expression and corresponding local alias.
    fn add_local_projection(
        &mut self,
        child_id: NodeId,
        aggr_infos: &mut Vec<AggrInfo>,
        grouping_exprs: &Vec<NodeId>,
    ) -> Result<(NodeId, Vec<usize>, LocalAliasesMap), SbroadError> {
        let (child_id, proj_output_cols, groupby_local_aliases, grouping_positions) =
            self.create_columns_for_local_proj(aggr_infos, child_id, grouping_exprs)?;
        let proj_output = self.nodes.add_row(proj_output_cols, None);

        let ref_rel_nodes = self.get_relational_nodes_from_row(proj_output)?;

        let mut children = vec![child_id];

        for ref_rel_node_id in ref_rel_nodes {
            let rel_node = self.get_relation_node(ref_rel_node_id)?;
            if matches!(rel_node, Relational::ScanSubQuery { .. })
                && self.is_additional_child(ref_rel_node_id)?
            {
                // Note: For queries like `select sum((VALUES (1))) from t` it may be a problem
                //       that we don't make a copy of a SubQuery node, but just copy its id so that
                //       several Relational node consider it to be a child.
                //       It may be a problem on a `take_subtree` call for the query above:
                //       * `take_subtree` will replace SQ with Invalid while traversing Local(Map)
                //         stage
                //       * Later Final(Reduce) stage won't see the SubQuery.
                // TODO: Solution would be to use `SubtreeCloner::clone`?
                children.push(ref_rel_node_id);
            }
        }

        let proj = Projection {
            output: proj_output,
            children,
            is_distinct: false,
        };
        let proj_id = self.add_relational(proj.into())?;

        for info in aggr_infos {
            // We take expressions inside aggregate functions from Final projection,
            // so we need to update parent
            self.replace_parent_in_subtree(info.aggr.fun_id, Some(info.parent_rel), Some(proj_id))?;
        }
        self.set_distribution(proj_output)?;

        Ok((proj_id, grouping_positions, groupby_local_aliases))
    }

    fn create_local_aggregate(
        &mut self,
        kind: AggregateKind,
        arguments: &[NodeId],
        local_alias: &str,
    ) -> Result<NodeId, SbroadError> {
        let fun = Function {
            name: kind.to_smolstr(),
            behavior: Behavior::Stable,
            func_type: Type::from(kind),
            is_system: true,
        };
        // We can reuse aggregate expression between local aggregates, because
        // all local aggregates are located inside the same motion subtree and we
        // assume that each local aggregate does not need to modify its expression
        let local_fun_id = self.add_stable_function(&fun, arguments.to_vec(), None)?;
        let alias_id = self.nodes.add_alias(local_alias, local_fun_id)?;
        Ok(alias_id)
    }

    /// Creates columns for local projection
    ///
    /// local projection contains groupby columns + local aggregates,
    /// this function removes duplicated among them and creates the list for output
    /// `Row` for local projection.
    ///
    /// In case we have distinct aggregates and no groupby in original query,
    /// local `GroupBy` node will created.
    ///
    /// # Returns
    /// - id of local Projection child.
    /// - created list of columns
    /// - mapping between `GroupBy` expressions and local aliases
    /// - grouping positions: positions of columns by which `GroupBy` is done
    fn create_columns_for_local_proj(
        &mut self,
        aggr_infos: &mut [AggrInfo],
        upper_id: NodeId,
        grouping_exprs: &Vec<NodeId>,
    ) -> Result<(NodeId, Vec<NodeId>, LocalAliasesMap, Vec<usize>), SbroadError> {
        let mut output_cols: Vec<NodeId> = vec![];
        let (local_aliases, child_id, grouping_positions) =
            self.add_grouping_exprs(aggr_infos, upper_id, grouping_exprs, &mut output_cols)?;
        self.add_local_aggregates(aggr_infos, &mut output_cols)?;

        Ok((child_id, output_cols, local_aliases, grouping_positions))
    }

    /// Adds grouping expressions to columns of local projection
    ///
    /// # Arguments
    /// * `aggr_infos` - list of metadata info for each aggregate
    /// * `upper_id` - first node in local stage, if `GroupBy` was
    ///   present in the original user query, then it is the id of that
    ///   `GroupBy`
    /// * `grouping_exprs` - ids of grouping expressions from local
    ///   `GroupBy`. It is assumed that there are no duplicate expressions
    ///   among them.
    /// * `output_cols` - list of projection columns, where to push grouping
    ///   expressions.
    ///
    /// # Returns
    /// - map between grouping expression id and corresponding local alias
    /// - id of a Projection child, in case there are distinct aggregates and
    ///   no local `GroupBy` node, this node will be created
    /// - list of positions in projection columns by which `GroupBy` is done. These
    ///   positions are later used to create Motion node and they include only positions
    ///   from original `GroupBy`. Grouping expressions from distinct aggregates are not
    ///   included in this list as they shouldn't be used for Motion node.
    fn add_grouping_exprs(
        &mut self,
        aggr_infos: &mut [AggrInfo],
        upper_id: NodeId,
        grouping_exprs: &Vec<NodeId>,
        output_cols: &mut Vec<NodeId>,
    ) -> Result<(LocalAliasesMap, NodeId, Vec<usize>), SbroadError> {
        let mut unique_grouping_exprs_for_local_stage_full: OrderedMap<
            GroupingExpression,
            Rc<String>,
            RepeatableState,
        > = OrderedMap::with_hasher(RepeatableState);
        for gr_expr in grouping_exprs {
            unique_grouping_exprs_for_local_stage_full.insert(
                GroupingExpression::new(*gr_expr, self),
                Rc::new(Self::generate_local_alias(*gr_expr)),
            );
        }

        // add grouping expressions found from distinct aggregates to local groupby
        let mut grouping_exprs_from_aggregates: Vec<NodeId> = vec![];
        for info in aggr_infos.iter_mut().filter(|x| x.is_distinct) {
            let argument = {
                let args = self
                    .nodes
                    .expr_iter(info.aggr.fun_id, false)
                    .collect::<Vec<NodeId>>();
                if args.len() > 1 && !matches!(info.aggr.kind, AggregateKind::GRCONCAT) {
                    return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "aggregate ({info:?}) have more than one argument"
                    )));
                }
                *args.first().ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "Aggregate function has no children: {info:?}"
                    ))
                })?
            };
            let expr = GroupingExpression::new(argument, self);
            if let Some(local_alias) = unique_grouping_exprs_for_local_stage_full.get(&expr) {
                info.aggr
                    .lagg_alias
                    .insert(info.aggr.kind, local_alias.clone());
            } else {
                grouping_exprs_from_aggregates.push(argument);
                let local_alias = Rc::new(Self::generate_local_alias(argument));
                unique_grouping_exprs_for_local_stage_full.insert(expr, local_alias.clone());
                info.aggr.lagg_alias.insert(info.aggr.kind, local_alias);
            }
        }

        // Because of borrow checker we need to remove references to Plan from map
        let mut unique_grouping_exprs_for_local_stage: OrderedMap<
            NodeId,
            Rc<String>,
            RepeatableState,
        > = OrderedMap::with_capacity_and_hasher(
            unique_grouping_exprs_for_local_stage_full.len(),
            RepeatableState,
        );
        for (gr_expr, name) in unique_grouping_exprs_for_local_stage_full.iter() {
            unique_grouping_exprs_for_local_stage.insert(gr_expr.id, name.clone())
        }

        let mut alias_to_pos: HashMap<Rc<String>, usize> = HashMap::new();
        // add grouping expressions to local projection
        for (pos, (gr_expr, local_alias)) in
            unique_grouping_exprs_for_local_stage.iter().enumerate()
        {
            let new_alias = self.nodes.add_alias(local_alias, *gr_expr)?;
            output_cols.push(new_alias);
            alias_to_pos.insert(local_alias.clone(), pos);
        }

        let mut local_aliases: LocalAliasesMap =
            HashMap::with_capacity(unique_grouping_exprs_for_local_stage.len());
        let mut grouping_positions: Vec<usize> = Vec::with_capacity(grouping_exprs.len());

        // Note: we need to iterate only over grouping expressions that were present
        // in original user query here. We must not use the grouping expressions
        // that come from distinct aggregates. This is because they are handled separately:
        // local aliases map is needed only for GroupBy expressions in the original query and
        // grouping positions are used to create a Motion later, which should take into account
        // only positions from GroupBy expressions in the original user query.
        for expr_id in grouping_exprs {
            if let Some(local_alias) = unique_grouping_exprs_for_local_stage.remove(expr_id) {
                local_aliases.insert(*expr_id, local_alias.clone());
                if let Some(pos) = alias_to_pos.get(&local_alias) {
                    grouping_positions.push(*pos);
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some(format_smolstr!("missing position for local GroupBy column with local alias: {local_alias}"))
                    ));
                }
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!("invalid map with unique grouping expressions. Could not find grouping expression with id: {expr_id:?}"))));
            }
        }
        let child_id = self
            .add_distinct_aggregates_to_local_groupby(upper_id, grouping_exprs_from_aggregates)?;
        Ok((local_aliases, child_id, grouping_positions))
    }

    /// Adds aggregates columns in `output_cols` for local `Projection`
    ///
    /// This function collects local aggregates from each `AggrInfo`,
    /// then it removes duplicates from them using `AggregateSignature`.
    /// Next, it creates for each unique aggregate local alias and column.
    #[allow(clippy::mutable_key_type)]
    fn add_local_aggregates(
        &mut self,
        aggr_infos: &mut [AggrInfo],
        output_cols: &mut Vec<NodeId>,
    ) -> Result<(), SbroadError> {
        // Aggregate expressions can appear in `Projection`, `Having`, `OrderBy`, if the
        // same expression appears in different places, we must not calculate it separately:
        // `select sum(a) from t group by b having sum(a) > 10`
        // Here `sum(a)` appears both in projection and having, so we need to calculate it only once.
        let mut unique_local_aggregates: HashSet<AggregateSignature, RepeatableState> =
            HashSet::with_hasher(RepeatableState);
        for pos in 0..aggr_infos.len() {
            let info = aggr_infos.get(pos).ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                    "invalid idx of aggregate infos ({pos})"
                ))
            })?;
            if info.is_distinct {
                continue;
            }
            let arguments = {
                if let Expression::StableFunction(StableFunction { children, .. }) =
                    self.get_expression_node(info.aggr.fun_id)?
                {
                    children
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Aggregate,
                        Some(format_smolstr!("invalid fun_id: {:?}", info.aggr.fun_id)),
                    ));
                }
            };
            for kind in info.aggr.kind.get_local_aggregates_kinds() {
                let mut signature = AggregateSignature {
                    kind,
                    arguments,
                    plan: self,
                    local_alias: None,
                };
                if let Some(sig) = unique_local_aggregates.get(&signature) {
                    if let Some(alias) = &sig.local_alias {
                        let info = aggr_infos.get_mut(pos).ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                                "invalid idx of aggregate infos ({pos})"
                            ))
                        })?;
                        info.aggr.lagg_alias.insert(kind, alias.clone());
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::AggregateSignature,
                            Some("no local alias".into()),
                        ));
                    }
                } else {
                    let info = aggr_infos.get_mut(pos).ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                            "invalid idx of aggregate infos ({pos})"
                        ))
                    })?;
                    let alias = Rc::new(generate_local_alias_for_aggr(
                        &kind,
                        &format_smolstr!("{}", info.aggr.fun_id),
                    ));
                    info.aggr.lagg_alias.insert(kind, alias.clone());
                    signature.local_alias = Some(alias);
                    unique_local_aggregates.insert(signature);
                }
            }
        }

        // add non-distinct aggregates to local projection
        let local_aggregates: Result<Vec<LocalAggrInfo>, SbroadError> = unique_local_aggregates
            .into_iter()
            .map(
                |x| -> Result<(AggregateKind, Vec<NodeId>, Rc<String>), SbroadError> {
                    match x.get_alias() {
                        Ok(s) => Ok((x.kind, x.arguments.clone(), s)),
                        Err(e) => Err(e),
                    }
                },
            )
            .collect();
        for (kind, arguments, local_alias) in local_aggregates? {
            let alias_id = self.create_local_aggregate(kind, &arguments, local_alias.as_str())?;
            output_cols.push(alias_id);
        }

        Ok(())
    }

    /// Add final `GroupBy` node in case `grouping_exprs` are not empty
    ///
    /// # Arguments
    /// * `child_id` - id if relational node that will the child of `GroupBy`
    /// * `grouping_exprs` - list of grouping expressions ids (which does not include
    ///   grouping expressions from distinct arguments)
    /// * `local_aliases_map` - map between expression from `GroupBy` to alias used
    ///   at local stage
    ///
    /// # Returns
    /// - if `GroupBy` node was created, return its id
    /// - if `GroupBy` node was not created, return `child_id`
    fn add_final_groupby(
        &mut self,
        child_id: NodeId,
        grouping_exprs: &Vec<NodeId>,
        local_aliases_map: &LocalAliasesMap,
    ) -> Result<NodeId, SbroadError> {
        if grouping_exprs.is_empty() {
            // no GroupBy in the original query, nothing to do
            return Ok(child_id);
        }
        let mut gr_cols: Vec<NodeId> = Vec::with_capacity(grouping_exprs.len());
        let child_map = ColumnPositionMap::new(self, child_id)?;
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
            if !col_type.is_scalar() {
                return Err(SbroadError::Invalid(
                    Entity::Type,
                    Some(format_smolstr!(
                        "add_final_groupby: GroupBy expr ({expr_id:?}) is not scalar ({col_type})!"
                    )),
                ));
            }
            let new_col = Reference {
                position,
                parent: None,
                targets: Some(vec![0]),
                col_type,
                asterisk_source: None,
            };
            nodes.push(new_col);
        }
        for node in nodes {
            let new_col_id = self.nodes.push(node.into());
            gr_cols.push(new_col_id);
        }
        let output = self.add_row_for_output(child_id, &[], true, None)?;
        let final_id = self.nodes.next_id(ArenaType::Arena64);
        for col in &gr_cols {
            self.replace_parent_in_subtree(*col, None, Some(final_id))?;
        }
        let final_groupby = GroupBy {
            gr_cols,
            children: vec![child_id],
            is_final: true,
            output,
        };
        self.replace_parent_in_subtree(output, None, Some(final_id))?;
        self.add_relational(final_groupby.into())?;

        Ok(final_id)
    }

    /// Replace grouping expressions in finals with corresponding
    /// references to local aliases.
    ///
    /// For example:
    /// original query: `select a + b from t group by a + b`
    /// map query: `select a + b as l1 from t group by a + b` `l1` is local alias
    /// reduce query: `select l1 as user_alias from tmp_space group by l1`
    /// In above example this function will replace `a+b` expression in final `Projection`
    #[allow(clippy::too_many_lines)]
    fn patch_grouping_expressions(
        &mut self,
        local_aliases_map: &LocalAliasesMap,
        map: GroupbyExpressionsMap,
    ) -> Result<(), SbroadError> {
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
            let mut new_map: ParentExpressionMap = HashMap::with_capacity(map.len());
            for (groupby_expr_id, locations) in map {
                for location in locations {
                    let rec = (groupby_expr_id, location.expr, location.parent_expr);
                    if let Some(u) = new_map.get_mut(&location.rel) {
                        u.push(rec);
                    } else {
                        new_map.insert(location.rel, vec![rec]);
                    }
                }
            }
            new_map
        };
        for (rel_id, group) in map {
            // E.g. GroupBy under final Projection.
            let child_id = *self
                .get_relational_children(rel_id)?
                .get(0)
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "expected relation node ({rel_id:?}) to have children!"
                    ))
                })?;
            let alias_to_pos_map = ColumnPositionMap::new(self, child_id)?;
            let mut nodes = Vec::with_capacity(group.len());
            for (gr_expr_id, expr_id, parent) in group {
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
                if !col_type.is_scalar() {
                    return Err(SbroadError::Invalid(
                        Entity::Type,
                        Some(format_smolstr!(
                            "patch_finals: expected scalar expression, found: {col_type}"
                        )),
                    ));
                };
                let new_ref = Reference {
                    parent: Some(rel_id),
                    targets: Some(vec![0]),
                    position,
                    col_type,
                    asterisk_source: None,
                };
                nodes.push((parent, expr_id, gr_expr_id, new_ref));
            }
            for (parent, expr_id, gr_expr_id, node) in nodes {
                let ref_id = self.nodes.push(node.into());
                if let Some(parent_expr_id) = parent {
                    self.replace_expression(parent_expr_id, expr_id, ref_id)?;
                } else {
                    match self.get_mut_relation_node(rel_id)? {
                        MutRelational::Projection(_) => {
                            return Err(SbroadError::Invalid(
                                Entity::Plan,
                                Some(format_smolstr!(
                                    "{} {gr_expr_id:?} {} {expr_id:?} {}",
                                    "invalid mapping between group by expression",
                                    "and projection one: expression",
                                    "has no parent",
                                )),
                            ))
                        }
                        MutRelational::Having(Having { filter, .. }) => {
                            *filter = ref_id;
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Plan,
                                Some(format_smolstr!(
                                    "unexpected node in Reduce stage: {rel_id:?}"
                                )),
                            ))
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Make finals nodes in 2-stage aggregation valid, after local stage was created.
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
    /// * `finals` - ids of nodes to be patched
    /// * `finals_child_id` - id of a relational node right after `finals` in the plan. In case
    ///    original query had `GroupBy`, this will be final `GroupBy` id.
    /// * `local_aliases_map` - map between grouping expressions ids and corresponding local aliases.
    /// * `aggr_infos` - list of metadata about aggregates
    /// * `gr_expr_map` - map between grouping expressions in `GroupBy` and grouping expressions
    ///    used in `finals`.
    fn patch_finals(
        &mut self,
        finals: &[NodeId],
        finals_child_id: NodeId,
        local_aliases_map: &LocalAliasesMap,
        aggr_infos: &Vec<AggrInfo>,
        gr_expr_map: GroupbyExpressionsMap,
    ) -> Result<(), SbroadError> {
        // After we added a Map stage, we need to update output
        // of nodes in Reduce stage
        if let Some(last) = finals.last() {
            if let Some(first) = self.get_mut_relation_node(*last)?.mut_children().get_mut(0) {
                *first = finals_child_id;
            }
        }
        for node_id in finals.iter().rev() {
            let node = self.get_relation_node(*node_id)?;
            match node {
                // Projection node is the top node in finals: its aliases
                // must not be changed (because those are user aliases), so
                // nothing to do here
                Relational::Projection(_) => {}
                Relational::Having(Having { children, .. }) => {
                    let child_id = *children.first().ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Node,
                            Some(format_smolstr!("Having ({node_id:?}) has no children!")),
                        )
                    })?;
                    let output = self.add_row_for_output(child_id, &[], true, None)?;
                    *self.get_mut_relation_node(*node_id)?.mut_output() = output;
                    self.replace_parent_in_subtree(output, None, Some(*node_id))?;
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some(format_smolstr!("Unexpected node in reduce stage: {node:?}")),
                    ))
                }
            }
        }

        self.patch_grouping_expressions(local_aliases_map, gr_expr_map)?;
        let mut parent_to_infos: HashMap<NodeId, Vec<AggrInfo>> =
            HashMap::with_capacity(finals.len());
        for info in aggr_infos {
            if let Some(v) = parent_to_infos.get_mut(&info.parent_rel) {
                v.push(info.clone());
            } else {
                parent_to_infos.insert(info.parent_rel, vec![info.clone()]);
            }
        }
        for (parent, infos) in parent_to_infos {
            let child_id = {
                let children = self.get_relational_children(parent)?;
                *children.get(0).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Node,
                        Some(format_smolstr!(
                            "patch aggregates: rel node ({parent:?}) has no children!"
                        )),
                    )
                })?
            };
            let alias_to_pos_map = ColumnPositionMap::new(self, child_id)?;
            let mut position_kinds = Vec::with_capacity(infos.len());
            for info in &infos {
                position_kinds.push(
                    info.aggr
                        .get_position_kinds(&alias_to_pos_map, info.is_distinct)?,
                );
            }
            for (info, pos_kinds) in infos.into_iter().zip(position_kinds) {
                let fun_expr = self.get_expression_node(info.aggr.fun_id)?;
                let fun_type = fun_expr.calculate_type(self)?;
                let final_expr = info.aggr.create_final_aggregate_expr(
                    parent,
                    self,
                    fun_type,
                    pos_kinds,
                    info.is_distinct,
                )?;
                if let Some(parent_expr) = info.parent_expr {
                    self.replace_expression(parent_expr, info.aggr.fun_id, final_expr)?;
                } else {
                    let node = self.get_mut_relation_node(parent)?;
                    return Err(SbroadError::Invalid(
                        Entity::Aggregate,
                        Some(format_smolstr!(
                            "aggregate info for {node:?} that hat no parent! Info: {info:?}"
                        )),
                    ));
                }
            }
        }
        Ok(())
    }

    fn add_motion_to_2stage(
        &mut self,
        grouping_positions: &[usize],
        motion_parent: NodeId,
        finals: &[NodeId],
    ) -> Result<(), SbroadError> {
        let proj_id = *finals.first().ok_or_else(|| {
            SbroadError::Invalid(Entity::Plan, Some("no nodes in Reduce stage!".into()))
        })?;
        if let Relational::Projection(_) = self.get_relation_node(proj_id)? {
        } else {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some("expected Projection as first node in reduce stage!".into()),
            ));
        }
        if grouping_positions.is_empty() {
            // no GroupBy
            let last_final_id = *finals.last().ok_or_else(|| {
                SbroadError::Invalid(Entity::Plan, Some("Reduce stage has no nodes!".into()))
            })?;
            let mut strategy = Strategy::new(last_final_id);
            strategy.add_child(motion_parent, MotionPolicy::Full, Program::default());
            self.create_motion_nodes(strategy)?;

            self.set_dist(self.get_relational_output(proj_id)?, Distribution::Single)?;
        } else {
            // we have GroupBy, then finals_child_id is final GroupBy
            let child_id = if let Relational::GroupBy(GroupBy { children, .. }) =
                self.get_relation_node(motion_parent)?
            {
                *children.first().ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Node,
                        Some(format_smolstr!(
                            "final GroupBy ({motion_parent:?}) has no children!"
                        )),
                    )
                })?
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "expected to have GroupBy under reduce nodes on id: {motion_parent:?}"
                    )),
                ));
            };
            let mut strategy = Strategy::new(motion_parent);
            strategy.add_child(
                child_id,
                MotionPolicy::Segment(MotionKey {
                    targets: grouping_positions
                        .iter()
                        .map(|x| Target::Reference(*x))
                        .collect::<Vec<Target>>(),
                }),
                Program::default(),
            );
            self.create_motion_nodes(strategy)?;

            // When we created final GroupBy we didn't set its distribution, because its
            // actual child (Motion) wasn't created yet.
            self.set_distribution(self.get_relational_output(motion_parent)?)?;
        }
        Ok(())
    }

    /// Adds 2-stage aggregation and returns `true` if there are any aggregate
    /// functions or `GroupBy` is present. Otherwise, returns `false` and
    /// does nothing.
    ///
    /// # Errors
    /// - failed to create local `GroupBy` node
    /// - failed to create local `Projection` node
    /// - failed to create `SQ` node
    /// - failed to change final `GroupBy` child to `SQ`
    /// - failed to update expressions in final `Projection`
    pub fn add_two_stage_aggregation(
        &mut self,
        final_proj_id: NodeId,
    ) -> Result<bool, SbroadError> {
        let (finals, upper) = self.split_reduce_stage(final_proj_id)?;
        let mut aggr_infos = self.collect_aggregates(&finals)?;
        let has_aggregates = !aggr_infos.is_empty();
        let (upper, grouping_exprs, gr_expr_map) =
            self.collect_grouping_expressions(upper, &finals, has_aggregates)?;
        if grouping_exprs.is_empty() && aggr_infos.is_empty() {
            return Ok(false);
        }

        // Check for group by on bucket_id column
        // in that case groupby can be done locally.
        if !grouping_exprs.is_empty() {
            // let shard_col_info = self.track_shard_column_pos(final_proj_id)?;
            for expr_id in &grouping_exprs {
                let Expression::Reference(Reference { position, .. }) =
                    self.get_expression_node(*expr_id)?
                else {
                    continue;
                };
                let child_id = self.get_relational_from_reference_node(*expr_id)?;
                let mut context = self.context_mut();
                if let Some(shard_positions) =
                    context.get_shard_columns_positions(child_id, self)?
                {
                    if shard_positions[0] == Some(*position)
                        || shard_positions[1] == Some(*position)
                    {
                        return Ok(false);
                    }
                }
            }
        }

        let (local_proj_id, grouping_positions, local_aliases_map) =
            self.add_local_projection(upper, &mut aggr_infos, &grouping_exprs)?;

        self.set_distribution(self.get_relational_output(local_proj_id)?)?;
        let finals_child_id =
            self.add_final_groupby(local_proj_id, &grouping_exprs, &local_aliases_map)?;

        self.patch_finals(
            &finals,
            finals_child_id,
            &local_aliases_map,
            &aggr_infos,
            gr_expr_map,
        )?;
        self.add_motion_to_2stage(&grouping_positions, finals_child_id, &finals)?;

        let mut having_id: Option<NodeId> = None;
        // skip Projection
        for node_id in finals.iter().skip(1).rev() {
            self.set_distribution(self.get_relational_output(*node_id)?)?;
            if let Relational::Having(_) = self.get_relation_node(*node_id)? {
                having_id = Some(*node_id);
            }
        }

        if matches!(
            self.get_relation_node(finals_child_id)?,
            Relational::GroupBy(_)
        ) {
            self.set_distribution(self.get_relational_output(final_proj_id)?)?;
        } else {
            self.set_dist(
                self.get_relational_output(final_proj_id)?,
                Distribution::Single,
            )?;
        }

        if let Some(having_id) = having_id {
            if let Relational::Having(Having { filter, output, .. }) =
                self.get_relation_node(having_id)?
            {
                let (filter, output) = (*filter, *output);
                let strategy = self.resolve_sub_query_conflicts(having_id, filter)?;
                let fixed_subquery_ids = strategy.get_rel_ids();
                self.create_motion_nodes(strategy)?;
                self.fix_additional_subqueries(having_id, &fixed_subquery_ids)?;

                self.try_dist_from_subqueries(having_id, output)?;
            }
        }

        Ok(true)
    }
}
