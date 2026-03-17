use std::collections::HashSet;
use std::fmt::Display;

use itertools::FoldWhile::{Continue, Done};
use itertools::Itertools;
use smallvec::SmallVec;
use smol_str::{format_smolstr, SmolStr};

use crate::errors::{Entity, SbroadError};
use crate::ir::distribution::Distribution;
use crate::ir::helpers::RepeatableState;
use crate::ir::node::{expression::Expression, BoolExpr, NodeId};
use crate::ir::operator::Bool;
use crate::ir::value::Value;
use crate::ir::Plan;

/// We can apply some conservative optimizations during planning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BucketSet {
    /// The actual `bucket_id`, which we can only determine at runtime.
    Exact(HashSet<u64, RepeatableState>),
    /// Lower and upper bound, inclusive for the number of distinct `bucket_id`s.
    /// This field is used only for static analysis during planning.
    Unknown(usize, usize),
}

impl BucketSet {
    fn conjunct(&self, other: &BucketSet) -> Result<BucketSet, SbroadError> {
        match (self, other) {
            (BucketSet::Exact(a), BucketSet::Exact(b)) => {
                Ok(BucketSet::Exact(a.intersection(b).copied().collect()))
            }
            (BucketSet::Exact(a), BucketSet::Unknown(_, rb))
            | (BucketSet::Unknown(_, rb), BucketSet::Exact(a)) => {
                Ok(BucketSet::Unknown(0, a.len().min(*rb)))
            }
            (BucketSet::Unknown(_, ra), BucketSet::Unknown(_, rb)) => {
                Ok(BucketSet::Unknown(0, *ra.min(rb)))
            }
        }
    }

    fn disjunct(&self, other: &BucketSet) -> Result<BucketSet, SbroadError> {
        match (self, other) {
            (BucketSet::Exact(a), BucketSet::Exact(b)) => {
                Ok(BucketSet::Exact(a.union(b).copied().collect()))
            }
            (BucketSet::Exact(a), BucketSet::Unknown(lb, rb))
            | (BucketSet::Unknown(lb, rb), BucketSet::Exact(a)) => Ok(BucketSet::Unknown(
                a.len().saturating_add(*lb),
                a.len().saturating_add(*rb),
            )),
            (BucketSet::Unknown(la, ra), BucketSet::Unknown(lb, rb)) => Ok(BucketSet::Unknown(
                la.saturating_add(*lb),
                ra.saturating_add(*rb),
            )),
        }
    }
}

/// Buckets are used to determine which nodes to send the query to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Buckets {
    // We don't want to keep thousands of buckets in memory
    // so we use a special enum to represent all the buckets
    // in a cluster.
    All,
    // A filtered set of buckets.
    Filtered(BucketSet),
    // Execute query on any single node, maybe locally.
    Any,
}

impl Buckets {
    /// Get no buckets in the cluster (coordinator).
    #[must_use]
    pub fn new_empty() -> Self {
        Buckets::Filtered(BucketSet::Exact(HashSet::with_hasher(RepeatableState)))
    }

    /// Get a filtered set of buckets.
    #[must_use]
    pub fn new_filtered(buckets: HashSet<u64, RepeatableState>) -> Self {
        Buckets::Filtered(BucketSet::Exact(buckets))
    }

    /// Get an unknown set of buckets.
    #[must_use]
    pub fn new_unknown(count: usize) -> Self {
        Buckets::Filtered(BucketSet::Unknown(count, count))
    }

    /// Returns `true` if a query with these buckets can be executed
    /// on a single node and does not require motion.
    ///
    /// For queries with conflicting conditions, such as `a = 1 and a = 2`,
    /// returns `false` to avoid skipping the reduce stage.
    pub fn is_single_node(&self) -> bool {
        match self {
            // We should insert `Motion(Full)` when lower bound is 0.
            // https://git.picodata.io/core/picodata/-/issues/2788
            Buckets::Filtered(BucketSet::Unknown(1, 1)) => true,
            Buckets::Filtered(BucketSet::Exact(a)) if a.len() == 1 => true,
            Buckets::Any | Buckets::All | Buckets::Filtered(_) => false,
        }
    }

    pub fn determine_exec_location(&self) -> &str {
        match self {
            Buckets::Any => "ROUTER",
            Buckets::All => "STORAGE",
            Buckets::Filtered(_) => "FILTERED STORAGE",
        }
    }

    /// Conjunction of two sets of buckets.
    ///
    /// # Errors
    /// - Buckets that can't be conjuncted
    pub fn conjunct(&self, buckets: &Buckets) -> Result<Buckets, SbroadError> {
        let buckets = match (self, buckets) {
            (Buckets::All, Buckets::All) => Buckets::All,
            (Buckets::Filtered(b), Buckets::All) | (Buckets::All, Buckets::Filtered(b)) => {
                Buckets::Filtered(b.clone())
            }
            (Buckets::Filtered(a), Buckets::Filtered(b)) => Buckets::Filtered(a.conjunct(b)?),
            (Buckets::Any, _) => buckets.clone(),
            (_, Buckets::Any) => self.clone(),
        };
        Ok(buckets)
    }

    /// Disjunction of two sets of buckets.
    ///
    /// # Errors
    /// - Buckets that can't be disjuncted
    pub fn disjunct(&self, buckets: &Buckets) -> Result<Buckets, SbroadError> {
        let buckets = match (self, buckets) {
            (Buckets::All, _) | (_, Buckets::All) => Buckets::All,
            (Buckets::Filtered(a), Buckets::Filtered(b)) => Buckets::Filtered(a.disjunct(b)?),
            (Buckets::Any, _) => buckets.clone(),
            (_, Buckets::Any) => self.clone(),
        };
        Ok(buckets)
    }
}

impl Display for Buckets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Buckets::All => write!(f, "all"),
            Buckets::Any => write!(f, "any"),
            Buckets::Filtered(BucketSet::Exact(hash_set)) => {
                let mut buckets = hash_set.iter().collect::<SmallVec<[_; 16]>>();
                buckets.sort_unstable();
                write!(f, "[")?;
                if let Some((first, others)) = buckets.split_first() {
                    write!(f, "{}", first)?;
                    for bucket in others {
                        write!(f, ", {}", bucket)?;
                    }
                }
                write!(f, "]")
            }
            Buckets::Filtered(BucketSet::Unknown(l, r)) => {
                if l != r {
                    write!(f, "unknown({l}..={r})")
                } else {
                    write!(f, "unknown({l})")
                }
            }
        }
    }
}

pub trait BucketsResolver {
    /// `None` means that buckets cannot be determined.
    fn buckets_from_motion(&self, motion_id: NodeId) -> Result<Option<Buckets>, SbroadError>;

    fn buckets_from_values(
        &self,
        values: &[&Value],
        tier: Option<&SmolStr>,
    ) -> Result<Buckets, SbroadError>;
}

impl Plan {
    /// Inner logic of `get_expression_tree_buckets` for simple expressions (without OR and AND
    /// operators).
    /// In general it returns `Buckets::All`, but in some cases (e.g. `Eq` and `In` operators) it
    /// will return `Buckets::Filtered` (if such a result is met in SELECT or JOIN filter, it means
    /// that we can execute the query only on some of the replicasets).
    fn get_buckets_from_expr(
        &self,
        expr_id: NodeId,
        resolver: &impl BucketsResolver,
    ) -> Result<Option<Buckets>, SbroadError> {
        // The only possible case there will be several `Buckets` in the vec is when we have `Eq`.
        // See the logic of its handling below.
        let mut buckets = Vec::new();
        let tier = self.tier.as_ref();
        let expr = self.get_expression_node(expr_id)?;

        // Try to collect buckets from expression of type `sharding_key = value`
        if let Expression::Bool(BoolExpr {
            op: op @ (Bool::Eq | Bool::In),
            left,
            right,
            ..
        }) = expr
        {
            let pairs = [(*left, *right), (*right, *left)];
            for (left_id, right_id) in pairs {
                let left_expr = self.get_expression_node(left_id)?;
                if !matches!(left_expr, Expression::Row(_) | Expression::Reference(_)) {
                    continue;
                }

                let right_expr = self.get_expression_node(right_id)?;
                let right_columns = match right_expr {
                    Expression::Row(_) => right_expr.get_row_list()?.as_slice(),
                    Expression::Constant(_)
                    | Expression::Reference(_)
                    | Expression::Parameter(_) => &[right_id],
                    _ => continue,
                };

                // Get the distribution of the left row.
                let left_dist = self.get_distribution(left_id)?;

                // Gather buckets from the right row.
                if let Distribution::Segment { keys } = left_dist {
                    // If the right side is a row referencing the motion
                    // it means that the corresponding virtual table contains
                    // tuple with the same distribution as the left side (because this motion
                    // was specially added in order to fulfill `Eq` of `In` conditions).
                    let motion_id: Option<_> = match right_expr {
                        Expression::Row(_) => self.get_motion_from_row(right_id)?,
                        Expression::Reference(_) => self.get_motion_from_ref(right_id)?,
                        _ => None,
                    };

                    if let Some(motion_id) = motion_id {
                        if let Some(buckets) = resolver.buckets_from_motion(motion_id)? {
                            return Ok(Some(buckets));
                        }
                    }

                    let mut values: Vec<&Value> = Vec::new();
                    // The right side is a regular row or subquery which distribution
                    // didn't result in Motion creation (e.g. `"a" in (select "a" from t)`).
                    // So we have a case of `Eq` operator.
                    // If we have a case of constants on the positions of the left keys,
                    // we can return `Buckets::Filtered`.
                    // E.g. we have query
                    // `SELECT * FROM (SELECT A.a, B.b FROM A JOIN B ON A.a = B.b)
                    //  WHERE (a, b) = (0, 1)`.
                    // Here (a, b) row will have Distribution::Segment(keys = {[a], [b]}).
                    // After handling key "a" we will leave buckets which satisfy `a = 0`.
                    // After handling key "b" we will leave buckets which satisfy `b = 1`.
                    // In the end (when `conjunct` function is called) we will leave buckets
                    // which satisfy `(a, b) = (0, 1)`.
                    for key in keys.iter() {
                        // For cases with composite keys, we check that the condition fields
                        // are at least the size of the composite key.
                        // E.g. t1 sharded by (a, b), and we have query, with single value of
                        // the condition:
                        // `SELECT * FROM t1 JOIN t1 AS t ON 1 IN (t.a, t.b)`
                        // Since in this case it is not possible to calculate the buckets,
                        // therefore there is no need for further action.
                        if key.positions.len() > right_columns.len() {
                            continue;
                        }
                        values.clear();
                        values.reserve(key.positions.len());
                        let mut has_parameter = false;
                        let mut is_fixed_key = true;
                        for position in &key.positions {
                            // Since the sides in the "In" query can be of different lengths,
                            // we need to find a suitable position, as if they were the
                            // same length.
                            let pos = if *op == Bool::In {
                                *position % right_columns.len()
                            } else {
                                *position
                            };
                            let right_column_id = *right_columns.get(pos).ok_or_else(|| {
                                SbroadError::NotFound(
                                    Entity::Column,
                                    format_smolstr!("at position {position} for right row"),
                                )
                            })?;

                            let right_column_expr = self.get_expression_node(right_column_id)?;
                            match right_column_expr {
                                Expression::Constant(_) => {
                                    values.push(self.as_const_value_ref(right_column_id)?)
                                }
                                // Before `bind_params` we can't get actual value, but the
                                // query is still single-node if all sharding-key columns are
                                // fixed by constants or parameters.
                                Expression::Parameter(_) => {
                                    has_parameter = true;
                                }
                                _ => {
                                    // Row-dependent expressions on any sharding-key column make
                                    // the whole candidate key unresolved, even if other columns
                                    // are fixed by constants or parameters.
                                    is_fixed_key = false;
                                    values.clear();
                                    break;
                                }
                            }
                        }
                        if !is_fixed_key {
                            continue;
                        }
                        if has_parameter {
                            buckets.push(Buckets::new_unknown(1));
                        } else if !values.is_empty() {
                            let values_buckets = resolver.buckets_from_values(&values, tier)?;
                            buckets.push(values_buckets);
                        }
                    }
                }
            }
        }

        if buckets.is_empty() {
            return Ok(None);
        }

        let merged = buckets
            .into_iter()
            .fold_while(Ok(Buckets::Any), |acc, b| match acc {
                Ok(a) => Continue(a.conjunct(&b)),
                Err(_) => Done(acc),
            })
            .into_inner()?;

        Ok(Some(merged))
    }

    /// Inner logic of `bucket_discovery` for expressions (currently it's called only on
    /// `Selection` or `Join` filters).
    /// `rel_children` is a set of relational children of `Selection` or `Join` node.
    /// It splits given expression into DNF chains (`ORed` expressions) and calls `get_buckets_from_expr` on each simple
    /// chain subexpression, later disjuncting results.
    ///
    /// In case there is just one expression (not `ANDed`) `chains` will remain empty.
    pub fn get_expression_tree_buckets(
        &self,
        expr_id: NodeId,
        rel_children: &[NodeId],
        subqueries: &[NodeId],
        resolver: &impl BucketsResolver,
    ) -> Result<Buckets, SbroadError> {
        let chains = self.get_dnf_chains(expr_id)?;
        // When we can't extract buckets from expression,
        // we use default buckets for that expression with
        // idea that expression does not influence the result
        // bucket set for given subtree. But we must choose
        // between Buckets::Any and Buckets::All, we can't use
        // one of them in all cases:
        // `select a from global where true`
        // If expression `true` gives Buckets::All,
        // then we will get that query must be executed on all
        // nodes, which is wrong.
        // On the other hand, if Buckets::Any is the default:
        // `select a from segment_a where a = 1 or true`
        // `true` -> Buckets::Any || `a=1` -> Buckets::Filtered = Buckets::Filtered
        // which is wrong, because query must be executed on all nodes.
        // So, we should choose default buckets depending on
        // children's buckets: if some child subtree must be
        // executed on several nodes, we must use Buckets::All,
        // otherwise we should use Buckets::Any.
        let default_buckets = {
            let mut default_buckets = Buckets::Any;
            for child_id in rel_children.iter().chain(subqueries) {
                let child_dist = self.get_rel_distribution(*child_id)?;
                if matches!(child_dist, Distribution::Any | Distribution::Segment { .. }) {
                    default_buckets = Buckets::All;
                    break;
                }
            }
            default_buckets
        };
        let mut result = Vec::with_capacity(chains.len());
        for mut chain in chains {
            let mut chain_buckets = default_buckets.clone();
            let nodes = chain.get_mut_nodes();
            // Nodes in the chain are in the top-down order (from left to right).
            // We need to pop back the chain to get nodes in the bottom-up order.
            while let Some(node_id) = nodes.pop_back() {
                let node_buckets = self
                    .get_buckets_from_expr(node_id, resolver)?
                    .unwrap_or_else(|| default_buckets.clone());
                chain_buckets = chain_buckets.conjunct(&node_buckets)?;
            }
            result.push(chain_buckets);
        }

        if let Some((first, other)) = result.split_first_mut() {
            for buckets in other {
                *first = first.disjunct(buckets)?;
            }
            return Ok(first.clone());
        }

        Ok(Buckets::All)
    }
}
