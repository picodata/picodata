use ahash::AHashMap;
use smol_str::{format_smolstr, ToSmolStr};

use crate::errors::{Entity, SbroadError};
use crate::ir::helpers::RepeatableState;
use crate::ir::node::{NodeId, ReferenceTarget, ScalarFunction};
use crate::ir::operator::Arithmetic;
use crate::ir::types::{CastType, UnrestrictedType as RelType};
use crate::ir::Plan;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use super::expression::{
    ColumnPositionMap, Comparator, FunctionFeature, Position, EXPR_HASH_DEPTH,
};
use super::function::Function;
use super::node::expression::Expression;
use super::node::relational::Relational;
use super::node::{Having, Projection};
use super::types::DerivedType;
use crate::frontend::sql::ir::SubtreeCloner;

/// The kind of aggregate function.
///
/// Examples: avg, sum, count.
#[derive(Clone, Debug, Hash, Eq, PartialEq, Copy)]
pub enum AggregateKind {
    COUNT,
    SUM,
    AVG,
    TOTAL,
    MIN,
    MAX,
    GRCONCAT,
}

impl Display for AggregateKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            AggregateKind::COUNT => "count",
            AggregateKind::SUM => "sum",
            AggregateKind::AVG => "avg",
            AggregateKind::TOTAL => "total",
            AggregateKind::MIN => "min",
            AggregateKind::MAX => "max",
            AggregateKind::GRCONCAT => "group_concat",
        };
        write!(f, "{name}")
    }
}

impl AggregateKind {
    /// Returns None in case passed function name is not aggregate.
    #[must_use]
    pub fn from_name(func_name: &str) -> Option<AggregateKind> {
        let normalized = func_name.to_lowercase();
        let kind = match normalized.as_str() {
            "count" => AggregateKind::COUNT,
            "sum" => AggregateKind::SUM,
            "avg" => AggregateKind::AVG,
            "total" => AggregateKind::TOTAL,
            "min" => AggregateKind::MIN,
            "max" => AggregateKind::MAX,
            "group_concat" | "string_agg" => AggregateKind::GRCONCAT,
            _ => return None,
        };
        Some(kind)
    }

    /// Get type of the corresponding aggregate function.
    pub fn get_type(self, plan: &Plan, args: &[NodeId]) -> Result<DerivedType, SbroadError> {
        let ty =
            match self {
                AggregateKind::COUNT => RelType::Integer,
                AggregateKind::TOTAL => RelType::Double,
                AggregateKind::GRCONCAT => RelType::String,
                AggregateKind::SUM | AggregateKind::AVG => RelType::Decimal,
                AggregateKind::MIN | AggregateKind::MAX => {
                    let child_node = args.first().ok_or(SbroadError::UnexpectedNumberOfValues(
                        format_smolstr!("expected at least 1 argument, got 0"),
                    ))?;
                    let expr_node = plan.get_expression_node(*child_node)?;
                    return expr_node.calculate_type(plan);
                }
            };
        Ok(DerivedType::new(ty))
    }

    /// Get aggregate functions that must be present on the local (Map) stage
    /// of two stage aggregation in order to calculate given aggregate (`self`)
    /// on the reduce stage.
    #[must_use]
    pub fn get_local_aggregates_kinds(&self) -> Vec<AggregateKind> {
        match self {
            AggregateKind::COUNT => vec![AggregateKind::COUNT],
            AggregateKind::SUM => vec![AggregateKind::SUM],
            AggregateKind::AVG => vec![AggregateKind::SUM, AggregateKind::COUNT],
            AggregateKind::TOTAL => vec![AggregateKind::TOTAL],
            AggregateKind::MIN => vec![AggregateKind::MIN],
            AggregateKind::MAX => vec![AggregateKind::MAX],
            AggregateKind::GRCONCAT => vec![AggregateKind::GRCONCAT],
        }
    }

    /// Calculate argument type of aggregate function
    ///
    /// # Errors
    /// - Invalid index
    /// - Node doesn't exist in the plan
    /// - Node is not an expression type
    pub fn get_arg_type(
        idx: usize,
        plan: &Plan,
        args: &[NodeId],
    ) -> Result<DerivedType, SbroadError> {
        let arg_id = *args.get(idx).ok_or(SbroadError::NotFound(
            Entity::Index,
            format_smolstr!("no element at index {idx} in args {args:?}"),
        ))?;
        let expr = plan.get_expression_node(arg_id)?;
        expr.calculate_type(plan)
    }

    /// Get final aggregate corresponding to given local aggregate.
    /// 1) Checks that `local_aggregate` and final `self` aggregate corresponds to each other
    /// 2) Gets type of final aggregate
    pub fn get_final_aggregate_kind(
        &self,
        local_aggregate: &AggregateKind,
    ) -> Result<AggregateKind, SbroadError> {
        let res = match (self, local_aggregate) {
            (AggregateKind::COUNT | AggregateKind::AVG, AggregateKind::COUNT)
            | (AggregateKind::SUM | AggregateKind::AVG, AggregateKind::SUM) => AggregateKind::SUM,
            (AggregateKind::TOTAL, AggregateKind::TOTAL) => AggregateKind::TOTAL,
            (AggregateKind::MIN, AggregateKind::MIN) => AggregateKind::MIN,
            (AggregateKind::MAX, AggregateKind::MAX) => AggregateKind::MAX,
            (AggregateKind::GRCONCAT, AggregateKind::GRCONCAT) => AggregateKind::GRCONCAT,
            (_, _) => {
                return Err(SbroadError::Invalid(
                    Entity::Aggregate,
                    Some(format_smolstr!(
                        "invalid local aggregate {local_aggregate} for original aggregate: {self}"
                    )),
                ))
            }
        };
        Ok(res)
    }
}

/// Pair of (aggregate kind, its position in the output).
pub(crate) type PositionKind = (Position, AggregateKind);

/// Metadata about aggregates.
#[derive(Clone, Debug)]
pub struct Aggregate {
    /// Id of Relational node in which this aggregate is located.
    /// It can be located in `Projection`, `Having`, `OrderBy`.
    pub parent_rel: NodeId,
    /// Id of parent expression of aggregate function.
    pub parent_expr: NodeId,
    /// The aggregate function being added, like COUNT, SUM, etc.
    pub kind: AggregateKind,
    /// "local aggregate aliases".
    ///
    /// For non-distinct aggregate maps local aggregate kind to
    /// corresponding local alias. For distinct aggregate maps
    /// its aggregate kind to local alias used for corresponding
    /// grouping expr.
    ///
    /// For example, if `AggregateKind` is `AVG` then we have two local aggregates:
    /// `sum` and `count`. Each of those aggregates will have its local alias in map
    /// query:
    /// original query: `select avg(b) from t`
    /// map query: `select sum(b) as l1, count(b) as l2 from t`
    ///
    /// So, this map will contain `sum` -> `l1`, `count` -> `l2`.
    ///
    /// Example for distinct aggregate:
    /// original query: `select avg(distinct b) from t`
    /// map query: `select b as l1 from t group by b)`
    /// map will contain: `avg` -> `l1`
    pub lagg_aliases: AHashMap<AggregateKind, Rc<String>>,
    /// Id of aggregate function in plan.
    pub fun_id: NodeId,
    /// Whether this aggregate was marked distinct in original user query
    pub is_distinct: bool,
}

impl Aggregate {
    #[must_use]
    pub fn from_name(
        name: &str,
        fun_id: NodeId,
        parent_rel: NodeId,
        parent_expr: NodeId,
        is_distinct: bool,
    ) -> Option<Self> {
        let kind = AggregateKind::from_name(name)?;
        let aggr = Self {
            kind,
            fun_id,
            lagg_aliases: AHashMap::with_capacity(2),
            parent_rel,
            parent_expr,
            is_distinct,
        };
        Some(aggr)
    }

    pub(crate) fn get_position_kinds(
        &self,
        alias_to_pos: &ColumnPositionMap,
    ) -> Result<Vec<PositionKind>, SbroadError> {
        let res = if self.is_distinct {
            // For distinct aggregates kinds of
            // local and final aggregates are the same.
            let local_alias = self
                .lagg_aliases
                .get(&self.kind)
                .expect("missing local alias for distinct aggregate: {self:?}");
            let pos = alias_to_pos.get(local_alias)?;
            vec![(pos, self.kind)]
        } else {
            let aggr_kinds = self.kind.get_local_aggregates_kinds();
            let mut res = Vec::with_capacity(aggr_kinds.len());
            for aggr_kind in aggr_kinds {
                let local_alias = self
                    .lagg_aliases
                    .get(&aggr_kind)
                    .expect("missing local alias for local aggregate ({aggr_kind}): {self:?}");
                let pos = alias_to_pos.get(local_alias)?;
                res.push((pos, aggr_kind));
            }
            res
        };
        Ok(res)
    }

    fn create_final_aggr(
        &self,
        plan: &mut Plan,
        position: Position,
        final_kind: AggregateKind,
    ) -> Result<NodeId, SbroadError> {
        let fun_expr = plan.get_expression_node(self.fun_id)?;
        let col_type = fun_expr.calculate_type(plan)?;
        let child_id = plan.get_first_rel_child(self.parent_rel)?;
        let ref_id = plan.nodes.add_ref(
            ReferenceTarget::Single(child_id),
            position,
            col_type,
            None,
            false,
        );
        let children: Vec<NodeId> = match self.kind {
            AggregateKind::AVG => vec![plan.add_cast(ref_id, CastType::Double)?],
            AggregateKind::GRCONCAT => {
                let Expression::ScalarFunction(ScalarFunction { children, .. }) =
                    plan.get_expression_node(self.fun_id)?
                else {
                    unreachable!("Aggregate should reference expression by fun_id")
                };

                if let Some(delimiter_id) = children.get(1) {
                    vec![ref_id, SubtreeCloner::clone_subtree(plan, *delimiter_id)?]
                } else {
                    vec![ref_id]
                }
            }
            _ => vec![ref_id],
        };
        let feature = if self.is_distinct {
            Some(FunctionFeature::Distinct)
        } else {
            None
        };
        let func_type = self.kind.get_type(plan, &children)?;
        let final_aggr = ScalarFunction {
            name: final_kind.to_smolstr(),
            children,
            feature,
            func_type,
            is_system: true,
            volatility_type: super::expression::VolatilityType::Stable,
            is_window: false,
        };
        let aggr_id = plan.nodes.push(final_aggr.into());
        Ok(aggr_id)
    }

    /// Create final aggregate expression and return its id.
    ///
    /// # Examples
    /// Suppose this aggregate is non-distinct `AVG` and at local stage
    /// `SUM` and `COUNT` were computed with corresponding local
    /// aliases `sum_1` and `count_1`, then this function
    /// will create the following expression:
    ///
    /// ```txt
    /// sum(sum_1) / sum(count_1)
    /// ```
    ///
    /// If we had `AVG(distinct a)` in user query, then at local stage
    /// we must have used `a` as `group by` expression and assign it
    /// a local alias. Let's say local alias is `column_1`, then this
    /// function will create the following expression:
    ///
    /// ```txt
    /// avg(column_1)
    /// ```
    #[allow(clippy::too_many_lines)]
    pub(crate) fn create_final_aggregate_expr(
        &self,
        plan: &mut Plan,
        position_kinds: Vec<PositionKind>,
    ) -> Result<NodeId, SbroadError> {
        // Map of {local AggregateKind -> finalized expression of that aggregate}.
        let mut final_aggregates: HashMap<AggregateKind, NodeId> =
            HashMap::with_capacity(AGGR_CAPACITY);

        if self.is_distinct {
            // For distinct aggregates kinds of local and final aggregates are the same.
            let (position, local_kind) = position_kinds
                .first()
                .expect("Distinct aggregate should have the only position kind");
            let aggr_id = self.create_final_aggr(plan, *position, self.kind)?;
            final_aggregates.insert(*local_kind, aggr_id);
        } else {
            for (position, local_kind) in position_kinds {
                let final_aggregate_kind = self.kind.get_final_aggregate_kind(&local_kind)?;
                let aggr_id = self.create_final_aggr(plan, position, final_aggregate_kind)?;
                final_aggregates.insert(local_kind, aggr_id);
            }
        }

        let final_expr_id = if final_aggregates.len() == 1 {
            *final_aggregates.values().next().unwrap()
        } else {
            match self.kind {
                AggregateKind::AVG => {
                    let sum_aggr = *final_aggregates
                        .get(&AggregateKind::SUM)
                        .expect("SUM aggregate expr should exist for final AVG");
                    let count_aggr = *final_aggregates
                        .get(&AggregateKind::COUNT)
                        .expect("COUNT aggregate expr should exist for final AVG");
                    plan.add_arithmetic_to_plan(sum_aggr, Arithmetic::Divide, count_aggr)?
                }
                _ => {
                    unreachable!("The only aggregate with multiple final aggregates is AVG")
                }
            }
        };
        Ok(final_expr_id)
    }
}

/// Capacity for the vec of aggregates which we expect to extract
/// from final nodes like Projection and Having.
const AGGR_CAPACITY: usize = 10;

/// Helper struct to find aggregates in expressions of finals.
struct AggrCollector<'plan> {
    /// Id of final node in which matches are searched.
    parent_rel: NodeId,
    /// Collected aggregates.
    aggrs: Vec<Aggregate>,
    plan: &'plan Plan,
}

impl<'plan> AggrCollector<'plan> {
    pub fn with_capacity(
        plan: &'plan Plan,
        capacity: usize,
        parent_rel: NodeId,
    ) -> AggrCollector<'plan> {
        AggrCollector {
            aggrs: Vec::with_capacity(capacity),
            parent_rel,
            plan,
        }
    }

    /// Collect aggregates in internal field by traversing expression tree `top`
    ///
    /// # Arguments
    /// * `top` - id of expression root in which to look for aggregates
    /// * `parent_rel` - id of parent relational node, where `top` is located. It is used to
    ///   create `AggrInfo`
    pub fn collect_aggregates(&mut self, top: NodeId) -> Result<Vec<Aggregate>, SbroadError> {
        self.find(top, None)?;
        Ok(std::mem::take(&mut self.aggrs))
    }

    fn find(&mut self, current: NodeId, parent_expr: Option<NodeId>) -> Result<(), SbroadError> {
        let expr = self.plan.get_expression_node(current)?;
        if let Expression::ScalarFunction(ScalarFunction { name, feature, .. }) = expr {
            let is_distinct = matches!(feature, Some(FunctionFeature::Distinct));
            let parent_expr = parent_expr.expect(
                "Aggregate stable function under final relational node should have a parent expr",
            );
            if let Some(aggr) =
                Aggregate::from_name(name, current, self.parent_rel, parent_expr, is_distinct)
            {
                self.aggrs.push(aggr);
                return Ok(());
            };
        }
        for child in self.plan.nodes.expr_iter(current, false) {
            self.find(child, Some(current))?;
        }
        Ok(())
    }
}

/// Helper struct to filter duplicate aggregates in local stage.
///
/// Consider user query: `select sum(a), avg(a) from t`
/// at local stage we need to compute `sum(a)` only once.
///
/// This struct contains info needed to compute hash and compare aggregates
/// used at local stage.
struct AggregateSignature<'plan> {
    pub kind: AggregateKind,
    /// Ids of expressions used as arguments to aggregate.
    pub arguments: Vec<NodeId>,
    pub plan: &'plan Plan,
    /// Local alias of this local aggregate.
    pub local_alias: Rc<String>,
}

impl Hash for AggregateSignature<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        let mut comp = Comparator::new(self.plan);
        comp.set_hasher(state);
        for arg in &self.arguments {
            comp.hash_for_expr(*arg, EXPR_HASH_DEPTH);
        }
    }
}

impl PartialEq<Self> for AggregateSignature<'_> {
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

impl Eq for AggregateSignature<'_> {}

fn aggr_local_alias(kind: AggregateKind, index: usize) -> String {
    format!("{kind}_{index}")
}

impl Plan {
    /// Collect information about aggregates.
    ///
    /// Aggregates can appear in `Projection`, `Having`.
    /// TODO: We should also support OrderBy.
    ///
    /// # Arguments
    /// - `proj` - id of final Projection node.
    /// - `having` - id of Having node under final Projection
    ///   Note: final `GroupBy` is not present because it will be added later in 2-stage pipeline.
    pub fn collect_aggregates(
        &self,
        proj: NodeId,
        having: Option<NodeId>,
    ) -> Result<Vec<Aggregate>, SbroadError> {
        let mut aggrs = Vec::with_capacity(AGGR_CAPACITY);
        match self.get_relation_node(proj)? {
            Relational::Projection(Projection { output, .. }) => {
                let mut collector = AggrCollector::with_capacity(self, AGGR_CAPACITY, proj);
                for col in self.get_row_list(*output)? {
                    aggrs.extend(collector.collect_aggregates(*col)?);
                }
            }
            _ => {
                unreachable!("expected Projection node");
            }
        }

        if let Some(having) = having {
            let Relational::Having(Having { filter, .. }) = self.get_relation_node(having)? else {
                unreachable!("expected Having node");
            };
            let mut collector = AggrCollector::with_capacity(self, AGGR_CAPACITY, having);
            aggrs.extend(collector.collect_aggregates(*filter)?);
        };

        for aggr in &aggrs {
            let top = aggr.fun_id;
            if self.contains_aggregates(top, false)? {
                return Err(SbroadError::Invalid(
                    Entity::Query,
                    Some("aggregate functions inside aggregate function are not allowed.".into()),
                ));
            }
        }

        Ok(aggrs)
    }

    pub fn create_local_aggregate(
        &mut self,
        kind: AggregateKind,
        arguments: &[NodeId],
        local_alias: &str,
    ) -> Result<NodeId, SbroadError> {
        let fun: Function = Function {
            name: kind.to_smolstr(),
            func_type: kind.get_type(self, arguments)?,
            is_system: true,
            volatility: super::expression::VolatilityType::Stable,
        };
        // We can reuse aggregate expression between local aggregates, because
        // all local aggregates are located inside the same motion subtree and we
        // assume that each local aggregate does not need to modify its expression
        let local_fun_id = self.add_stable_function(&fun, arguments.to_vec(), None)?;
        let alias_id = self.nodes.add_alias(local_alias, local_fun_id)?;
        Ok(alias_id)
    }

    /// Adds aggregates columns in `output_cols` for local `Projection`
    ///
    /// This function collects local aggregates from each `Aggregate`,
    /// then it removes duplicates from them using `AggregateSignature`.
    /// Next, it creates for each unique aggregate local alias and column.
    #[allow(clippy::mutable_key_type)]
    pub fn add_local_aggregates(
        &mut self,
        aggrs: &mut [Aggregate],
        output_cols: &mut Vec<NodeId>,
    ) -> Result<(), SbroadError> {
        let mut local_alias_index = 1;

        // Aggregate expressions can appear in `Projection`, `Having`, `OrderBy`, if the
        // same expression appears in different places, we must not calculate it separately:
        // `select sum(a) from t group by b having sum(a) > 10`
        // Here `sum(a)` appears both in projection and having, so we need to calculate it only once.
        let mut unique_local_aggregates: HashSet<AggregateSignature, RepeatableState> =
            HashSet::with_hasher(RepeatableState);
        for pos in 0..aggrs.len() {
            let (final_kind, arguments, aggr_kinds) = {
                let aggr: &Aggregate = aggrs.get(pos).unwrap();
                if aggr.is_distinct {
                    continue;
                }

                let Expression::ScalarFunction(ScalarFunction {
                    children: arguments,
                    ..
                }) = self.get_expression_node(aggr.fun_id)?
                else {
                    unreachable!("Aggregate should reference ScalarFunction by fun_id")
                };

                (
                    aggr.kind,
                    arguments.clone(),
                    aggr.kind.get_local_aggregates_kinds(),
                )
            };

            for kind in aggr_kinds {
                let local_alias = Rc::new(aggr_local_alias(final_kind, local_alias_index));

                let signature = AggregateSignature {
                    kind,
                    arguments: arguments.clone(),
                    plan: self,
                    local_alias: local_alias.clone(),
                };
                if let Some(sig) = unique_local_aggregates.get(&signature) {
                    let aggr: &mut Aggregate = aggrs.get_mut(pos).unwrap();
                    aggr.lagg_aliases.insert(kind, sig.local_alias.clone());
                } else {
                    let aggr = aggrs.get_mut(pos).unwrap();

                    // New aggregate was really added.
                    local_alias_index += 1;
                    aggr.lagg_aliases.insert(kind, local_alias.clone());
                    unique_local_aggregates.insert(signature);
                }
            }
        }

        type LocalAggregate = (AggregateKind, Vec<NodeId>, Rc<String>);
        // Add non-distinct aggregates to local projection.
        let local_aggregates: Vec<LocalAggregate> = unique_local_aggregates
            .into_iter()
            .map(|x| (x.kind, x.arguments.clone(), x.local_alias.clone()))
            .collect();
        for (kind, arguments, local_alias) in local_aggregates {
            let alias_id = self.create_local_aggregate(kind, &arguments, local_alias.as_str())?;
            output_cols.push(alias_id);
        }

        Ok(())
    }
}
