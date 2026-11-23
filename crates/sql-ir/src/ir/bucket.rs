use rustc_hash::FxHashSet;
use std::cell::RefCell;
use std::fmt::Display;
use std::slice;

use crate::errors::{Entity, SbroadError};
use crate::ir::distribution::Distribution;
use crate::ir::node::{
    expression::Expression, BoolExpr, Constant, NodeId, Parameter, Reference, ReferenceTarget,
    UnaryExpr,
};
use crate::ir::operator::{Bool, Unary};
use crate::ir::value::{value_to_decimal_or_error, Trivalent, Value};
use crate::ir::{Plan, Positions};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smol_str::{format_smolstr, SmolStr};

pub mod facts_route;
mod proof;
pub(crate) use proof::{BucketProof, StaticResolver};

/// Memoizes `bucket_id` position lookups within a single expression.
pub(crate) type ShardPositionsCache = SmallVec<[(NodeId, Option<Positions>); 4]>;

/// The only bucket a `bucket_id = <value>` predicate can match, if any.
#[must_use]
pub fn value_to_bucket_id(value: &Value) -> Option<u64> {
    // Going through a decimal keeps us in step with `Value::eq`, which compares
    // the numeric types the very same way.
    value_to_decimal_or_error(value).ok()?.floor().to_u64()
}

/// We can apply some conservative optimizations during planning.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum BucketSet {
    /// The actual `bucket_id`, which we can only determine at runtime.
    ///
    /// The `bucket_id`s are known here, so for a pair of `Exact` sets both
    /// operations are exact as well: conjunction (`AND`) intersects the sets,
    /// disjunction (`OR`) unites them, dropping the duplicates.
    Exact(FxHashSet<u64>),
    /// Lower and upper bound, inclusive for the number of distinct `bucket_id`s.
    /// This field is used only for static analysis during planning.
    ///
    /// The `bucket_id`s behind the bounds are unknown, so as soon as such an
    /// operand takes part in an operation, the result can't be an `Exact` set
    /// anymore and we propagate the bounds only (an `Exact` operand contributes
    /// its length as both of its bounds):
    /// - conjunction (`AND`) takes the minimum of the upper bounds, as an
    ///   intersection is never larger than the smaller operand, and resets the
    ///   lower bound to 0, as the operands may be disjoint (`a = 1 and a = 2`);
    /// - disjunction (`OR`) sums both bounds, as we can't detect an overlap
    ///   between the operands and have to treat them as disjoint. The sums are
    ///   saturating to avoid overflow.
    EstimatedCount { lower: usize, upper: usize },
}

impl BucketSet {
    /// Intersection of the `bucket_id`s the sets stand for. Of two exact
    /// sets, the smaller one is kept instead of collecting a new one.
    fn conjunct_owned(self, other: BucketSet) -> BucketSet {
        match (self, other) {
            (BucketSet::Exact(a_set), BucketSet::Exact(b_set)) => {
                let (mut small, large) = if a_set.len() <= b_set.len() {
                    (a_set, b_set)
                } else {
                    (b_set, a_set)
                };
                small.retain(|bucket| large.contains(bucket));
                BucketSet::Exact(small)
            }
            (BucketSet::Exact(set), BucketSet::EstimatedCount { upper, .. })
            | (BucketSet::EstimatedCount { upper, .. }, BucketSet::Exact(set)) => {
                BucketSet::EstimatedCount {
                    lower: 0,
                    upper: set.len().min(upper),
                }
            }
            (
                BucketSet::EstimatedCount { upper: a_upper, .. },
                BucketSet::EstimatedCount { upper: b_upper, .. },
            ) => {
                let lower = 0;
                let upper = a_upper.min(b_upper);
                BucketSet::EstimatedCount { lower, upper }
            }
        }
    }

    /// Union of the `bucket_id`s the sets stand for.
    fn disjunct(&self, other: &BucketSet) -> BucketSet {
        match (self, other) {
            (BucketSet::Exact(a_set), BucketSet::Exact(b_set)) => {
                BucketSet::Exact(a_set.union(b_set).copied().collect())
            }
            (BucketSet::Exact(set), BucketSet::EstimatedCount { lower, upper })
            | (BucketSet::EstimatedCount { lower, upper }, BucketSet::Exact(set)) => {
                let final_lower = set.len().saturating_add(*lower);
                let final_upper = set.len().saturating_add(*upper);
                BucketSet::EstimatedCount {
                    lower: final_lower,
                    upper: final_upper,
                }
            }
            (
                BucketSet::EstimatedCount {
                    lower: a_lower,
                    upper: a_upper,
                },
                BucketSet::EstimatedCount {
                    lower: b_lower,
                    upper: b_upper,
                },
            ) => {
                let lower = a_lower.saturating_add(*b_lower);
                let upper = a_upper.saturating_add(*b_upper);

                BucketSet::EstimatedCount { lower, upper }
            }
        }
    }

    /// Returns `true` if `BucketSet` is proven to be empty.
    pub fn is_empty(&self) -> bool {
        match self {
            BucketSet::EstimatedCount { upper, .. } => *upper == 0,
            BucketSet::Exact(b) => b.is_empty(),
        }
    }
}

/// Buckets are used to determine which nodes to send the query to.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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
        Buckets::Filtered(BucketSet::Exact(FxHashSet::default()))
    }

    /// Get a filtered set of buckets.
    #[must_use]
    pub fn new_filtered(buckets: FxHashSet<u64>) -> Self {
        Buckets::Filtered(BucketSet::Exact(buckets))
    }

    /// Get one bucket. The set is built by an insert, as a collect from an
    /// iterator runs more instructions for the same allocation.
    #[must_use]
    pub fn new_single(bucket: u64) -> Self {
        let mut buckets = FxHashSet::with_capacity_and_hasher(1, Default::default());
        buckets.insert(bucket);
        Buckets::new_filtered(buckets)
    }

    /// Get an unknown set of buckets.
    #[must_use]
    pub fn new_estimated(count: usize) -> Self {
        Buckets::Filtered(BucketSet::EstimatedCount {
            lower: count,
            upper: count,
        })
    }

    /// Get at most one bucket, possibly none.
    #[must_use]
    pub fn new_at_most_one() -> Self {
        Buckets::Filtered(BucketSet::EstimatedCount { lower: 0, upper: 1 })
    }

    /// Returns `true` if a query with these buckets can be executed
    /// on a single node and does not require motion.
    pub fn is_single_node(&self) -> bool {
        match self {
            Buckets::Filtered(BucketSet::EstimatedCount { upper, .. }) => *upper <= 1,
            Buckets::Filtered(BucketSet::Exact(a)) => a.len() <= 1,
            Buckets::Any => true,
            Buckets::All => false,
        }
    }

    /// Conjunction of two sets of buckets. No hot path borrows the operands,
    /// so the clones keep one set of rules in [`Self::conjunct_owned`].
    pub fn conjunct(&self, buckets: &Buckets) -> Buckets {
        self.clone().conjunct_owned(buckets.clone())
    }

    /// [`Self::conjunct`] that moves a filtered set into the result instead
    /// of cloning it.
    #[must_use]
    pub fn conjunct_owned(self, buckets: Buckets) -> Buckets {
        match (self, buckets) {
            (Buckets::Filtered(a), Buckets::Filtered(b)) => Buckets::Filtered(a.conjunct_owned(b)),
            (Buckets::All | Buckets::Any, filtered @ Buckets::Filtered(_))
            | (filtered @ Buckets::Filtered(_), Buckets::All | Buckets::Any) => filtered,
            (Buckets::All, Buckets::All) => Buckets::All,
            (Buckets::Any, _) | (_, Buckets::Any) => Buckets::Any,
        }
    }

    /// [`Buckets::conjunct`] into `self`, which keeps its set if it has one.
    pub fn conjunct_assign(&mut self, buckets: &Buckets) {
        match (&mut *self, buckets) {
            (Buckets::Filtered(BucketSet::Exact(a)), Buckets::Filtered(BucketSet::Exact(b))) => {
                a.retain(|id| b.contains(id));
            }
            _ => *self = self.conjunct(buckets),
        }
    }

    /// [`Buckets::disjunct`] that adds to a set of the operands instead of
    /// building a new one: the larger set takes in the other.
    #[must_use]
    pub fn disjunct_owned(self, buckets: Buckets) -> Buckets {
        match (self, buckets) {
            (Buckets::Filtered(BucketSet::Exact(a)), Buckets::Filtered(BucketSet::Exact(b))) => {
                let (mut large, small) = if a.len() >= b.len() { (a, b) } else { (b, a) };
                large.extend(small);
                Buckets::new_filtered(large)
            }
            (Buckets::Any, Buckets::Filtered(b)) | (Buckets::Filtered(b), Buckets::Any)
                if !b.is_empty() =>
            {
                Buckets::Filtered(b)
            }
            (a, b) => a.disjunct(&b),
        }
    }

    /// Disjunction of two sets of buckets.
    pub fn disjunct(&self, buckets: &Buckets) -> Buckets {
        match (self, buckets) {
            (Buckets::All, _) | (_, Buckets::All) => Buckets::All,
            (Buckets::Filtered(a), Buckets::Filtered(b)) => Buckets::Filtered(a.disjunct(b)),
            (Buckets::Any, Buckets::Filtered(b)) | (Buckets::Filtered(b), Buckets::Any) => {
                if b.is_empty() {
                    Buckets::Any
                } else {
                    Buckets::Filtered(b.clone())
                }
            }
            (Buckets::Any, Buckets::Any) => Buckets::Any,
        }
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
            Buckets::Filtered(BucketSet::EstimatedCount { lower, upper }) => {
                if lower != upper {
                    write!(f, "estimated count ({lower}..={upper})")
                } else {
                    write!(f, "estimated count ({lower})")
                }
            }
        }
    }
}

/// Operations shared by expression discovery, with distinct routing and proof results.
pub trait BucketAlgebra: Clone {
    fn all() -> Self;
    fn any() -> Self;
    fn new_empty() -> Self;
    fn new_estimated(count: usize) -> Self;
    fn new_at_most_one() -> Self;
    fn conjunct(&self, other: &Self) -> Self;
    /// `conjunct` into `self`.
    fn conjunct_assign(&mut self, other: &Self) {
        *self = self.conjunct(other);
    }
    fn disjunct(&self, other: &Self) -> Self;
    /// [`Self::conjunct`] that may reuse the operands instead of cloning them.
    fn conjunct_owned(self, other: Self) -> Self {
        self.conjunct(&other)
    }
    /// `disjunct`, free to reuse the operands.
    fn disjunct_owned(self, other: Self) -> Self {
        self.disjunct(&other)
    }
}

impl BucketAlgebra for Buckets {
    fn all() -> Self {
        Self::All
    }
    fn any() -> Self {
        Self::Any
    }
    fn new_empty() -> Self {
        Self::new_empty()
    }
    fn new_estimated(count: usize) -> Self {
        Self::new_estimated(count)
    }
    fn new_at_most_one() -> Self {
        Self::new_at_most_one()
    }
    fn conjunct(&self, other: &Self) -> Self {
        self.conjunct(other)
    }
    fn conjunct_assign(&mut self, other: &Self) {
        self.conjunct_assign(other);
    }
    fn disjunct(&self, other: &Self) -> Self {
        self.disjunct(other)
    }
    fn conjunct_owned(self, other: Self) -> Self {
        self.conjunct_owned(other)
    }
    fn disjunct_owned(self, other: Self) -> Self {
        self.disjunct_owned(other)
    }
}

/// A sharding-key column fixed by a constant or by a parameter.
#[derive(Clone, Copy, Debug)]
pub enum UnboundKeyPart<'v> {
    Value(&'v Value),
    /// The index of a parameter.
    Param(u16),
    /// `CURRENT_USER`, replaced with a constant on bind: one value per query.
    CurrentUser,
}

/// The buckets of the sharding keys hashed in one bucket discovery, by the
/// values of their parts in hash order. The condition walk and the facts
/// routes share it: a key is hashed once, whichever of them meets it first.
/// Past `MAX_HASHED_KEYS` keys it stops growing: a new key is hashed every time.
pub type HashedKeys<'p, B> = SmallVec<[(SmallVec<[&'p Value; 4]>, B); 2]>;

/// The keys [`HashedKeys`] keeps. A key repeats on the few nodes of its route,
/// while each `IN` list brings up to 512 distinct ones: a search over all of
/// them made a discovery quadratic in the keys of the whole statement.
const MAX_HASHED_KEYS: usize = 8;

/// Conjuncts the buckets of the sharding key `values` into `acc`, see
/// [`conjunct_into`]. The buckets are kept in `hashed` if every part hashes
/// by its value: INT, TEXT, UUID and BOOL parts of one type and value hash to
/// one bucket. A key with another part is hashed every time: decimal `1` and
/// `1.0` are equal, but hash apart. A kept key narrows `acc` without a copy.
pub fn conjunct_key_buckets<'p, R: BucketsResolver>(
    resolver: &R,
    values: &[&'p Value],
    tier: Option<&SmolStr>,
    hashed: Option<&RefCell<HashedKeys<'p, R::Buckets>>>,
    acc: &mut Option<R::Buckets>,
) -> Result<(), SbroadError> {
    let narrow = |acc: &mut Option<R::Buckets>, buckets: &R::Buckets| match acc {
        Some(acc) => acc.conjunct_assign(buckets),
        None => *acc = Some(buckets.clone()),
    };
    let hashed = hashed.filter(|_| values.iter().all(|v| v.get_type().hashes_by_value()));
    if let Some(hashed) = hashed {
        let hashed = hashed.borrow();
        let same_key = |key: &SmallVec<[&Value; 4]>| {
            key.len() == values.len() && key.iter().zip(values).all(|(a, b)| same_part(a, b))
        };
        if let Some((_, buckets)) = hashed.iter().find(|(key, _)| same_key(key)) {
            narrow(acc, buckets);
            return Ok(());
        }
    }
    let buckets = resolver.buckets_from_values(values, tier)?;
    match hashed.filter(|hashed| hashed.borrow().len() < MAX_HASHED_KEYS) {
        Some(hashed) => {
            narrow(acc, &buckets);
            hashed
                .borrow_mut()
                .push((SmallVec::from_slice(values), buckets));
        }
        None => conjunct_into(acc, buckets),
    }
    Ok(())
}

/// Parts of one type and one value. Not `Value::eq`, which compares across
/// types.
fn same_part(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Uuid(a), Value::Uuid(b)) => a == b,
        (Value::Boolean(a), Value::Boolean(b)) => a == b,
        _ => false,
    }
}

pub trait BucketsResolver {
    type Buckets: BucketAlgebra;

    /// `None` means that buckets cannot be determined.
    fn buckets_from_motion(&self, motion_id: NodeId) -> Result<Option<Self::Buckets>, SbroadError>;

    fn buckets_from_values(
        &self,
        values: &[&Value],
        tier: Option<&SmolStr>,
    ) -> Result<Self::Buckets, SbroadError>;

    /// Buckets of a sharding key fixed by constants and unbound parameters:
    /// at most one.
    fn buckets_from_unbound_key(
        &self,
        _parts: &[UnboundKeyPart<'_>],
        _tier: Option<&SmolStr>,
    ) -> Result<Self::Buckets, SbroadError> {
        Ok(Self::Buckets::new_at_most_one())
    }

    /// Buckets implied by an explicit `bucket_id = <value>` predicate.
    ///
    /// The result must never hold more than one bucket. The planner counts on
    /// such a predicate to drop the reduce stage.
    fn buckets_from_bucket_id(
        &self,
        value: &Value,
        tier: Option<&SmolStr>,
    ) -> Result<Self::Buckets, SbroadError>;
}

/// Whether `l = r` is provably not `TRUE`: `NULL` on either side, or two values
/// of the same kind that differ. Values of different kinds are left undecided:
/// `Value::eq` is not symmetric across numeric kinds (an integer beyond the
/// exact double range) and need not match the storage coercions. Tuples are
/// left undecided too, as `Value::eq` never reports them equal.
fn constant_pair_rejects_eq(l: &Value, r: &Value) -> bool {
    match (l, r) {
        (Value::Null, _) | (_, Value::Null) => true,
        (Value::Tuple(_), _) | (_, Value::Tuple(_)) => false,
        _ if std::mem::discriminant(l) == std::mem::discriminant(r) => l.eq(r) == Trivalent::False,
        _ => false,
    }
}

/// Accumulate `other` into a conjunction with `acc`.
pub(crate) fn conjunct_into<B: BucketAlgebra>(acc: &mut Option<B>, other: B) {
    *acc = Some(match acc.take() {
        Some(acc) => acc.conjunct_owned(other),
        None => other,
    });
}

impl Plan {
    /// Whether the expression is a reference to a one-column sharding key of
    /// the node it refers to: then its distribution is the key at position 0.
    ///
    /// `None` when the expression is not a reference to one node: then only
    /// [`Plan::get_distribution`] can tell. That function builds the
    /// distribution of the whole expression, which is too costly for the
    /// common `key = value` filter.
    fn ref_is_sharding_key(&self, expr: &Expression) -> Result<Option<bool>, SbroadError> {
        let Expression::Reference(Reference {
            target: ReferenceTarget::Single(rel_id),
            position,
            ..
        }) = expr
        else {
            return Ok(None);
        };
        let is_key = match self.rel_distr_ref(*rel_id)? {
            Distribution::Segment { keys } => keys.iter().any(|key| key.positions == [*position]),
            _ => false,
        };
        Ok(Some(is_key))
    }

    /// Positions of the `bucket_id` system column in the node's output, memoized
    /// in `cache` for the duration of one expression.
    pub(crate) fn shard_columns_positions(
        &self,
        node_id: NodeId,
        cache: &mut ShardPositionsCache,
    ) -> Result<Option<Positions>, SbroadError> {
        if let Some((_, positions)) = cache.iter().find(|(id, _)| *id == node_id) {
            return Ok(*positions);
        }
        let positions = self.shard_column_positions(node_id)?;
        cache.push((node_id, positions));
        Ok(positions)
    }

    /// Whether the reference points to the `bucket_id` system column in the
    /// output of every node it targets.
    fn is_shard_column_ref(
        &self,
        target: &ReferenceTarget,
        position: usize,
        cache: &mut ShardPositionsCache,
    ) -> Result<bool, SbroadError> {
        if target.is_empty() {
            return Ok(false);
        }
        for child_id in target.iter() {
            // For a node with multiple targets every target must refer to the shard column.
            let Some(positions) = self.shard_columns_positions(*child_id, cache)? else {
                return Ok(false);
            };
            if positions[0] != Some(position) && positions[1] != Some(position) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Collect buckets from an `Eq` whose operands pin the `bucket_id` system
    /// column to a constant or a parameter. `left` and `right` are the ids of
    /// the operands, or of the elements of row operands.
    fn collect_buckets_from_bucket_id_eq<R: BucketsResolver>(
        &self,
        left: &[NodeId],
        right: &[NodeId],
        resolver: &R,
        tier: Option<&SmolStr>,
        buckets: &mut Option<R::Buckets>,
    ) -> Result<(), SbroadError> {
        if left.len() != right.len() {
            return Ok(());
        }
        let mut cache = ShardPositionsCache::new();
        for (cols, vals) in [(left, right), (right, left)] {
            for (&col_id, &val_id) in cols.iter().zip(vals) {
                // `None` here stands for "the value is not known yet".
                let value = match self.get_expression_node(val_id)? {
                    Expression::Constant(Constant { value }) => Some(value),
                    Expression::Parameter(_) => None,
                    _ => continue,
                };
                let Expression::Reference(Reference {
                    target, position, ..
                }) = self.get_expression_node(col_id)?
                else {
                    continue;
                };
                if !self.is_shard_column_ref(target, *position, &mut cache)? {
                    continue;
                }

                let value_buckets = match value {
                    Some(value) => resolver.buckets_from_bucket_id(value, tier)?,
                    // The binding has not yet occurred.
                    None => R::Buckets::new_at_most_one(),
                };
                conjunct_into(buckets, value_buckets);
            }
        }

        Ok(())
    }

    /// The ids of the elements of a row, or the id of the expression itself
    /// if it is not a row.
    fn row_ids<'e>(expr: &'e Expression, id: &'e NodeId) -> &'e [NodeId] {
        match expr {
            Expression::Row(row) => row.list(),
            _ => slice::from_ref(id),
        }
    }

    /// The buckets of `key = constant` with a one-column sharding key, the
    /// most common filter; `None` for other comparisons. The general path
    /// gives the same buckets with more lookups: a sharding key column is
    /// never `bucket_id`, so its check is not needed.
    fn key_eq_constant_buckets<'p, R: BucketsResolver>(
        &'p self,
        left: &Expression<'p>,
        right: &Expression<'p>,
        resolver: &R,
        hashed: Option<&RefCell<HashedKeys<'p, R::Buckets>>>,
    ) -> Result<Option<R::Buckets>, SbroadError> {
        let (key, value) = match (left, right) {
            (key @ Expression::Reference(_), Expression::Constant(Constant { value }))
            | (Expression::Constant(Constant { value }), key @ Expression::Reference(_)) => {
                (key, value)
            }
            _ => return Ok(None),
        };
        if self.ref_is_sharding_key(key)? != Some(true) {
            return Ok(None);
        }
        #[cfg(debug_assertions)]
        if let Expression::Reference(Reference {
            target, position, ..
        }) = key
        {
            debug_assert!(
                !self.is_shard_column_ref(target, *position, &mut ShardPositionsCache::new())?,
                "a sharding key column is bucket_id"
            );
        }
        // `key = NULL` is never `TRUE`.
        if matches!(value, Value::Null) {
            return Ok(Some(R::Buckets::new_empty()));
        }
        let mut buckets = None;
        conjunct_key_buckets(resolver, &[value], self.tier.as_ref(), hashed, &mut buckets)?;
        Ok(buckets)
    }

    /// Whether constants make `left = right` provably not `TRUE`, either
    /// directly or at some position of two rows. Such pairs survive binding
    /// when a parameter was merged into a row (`(b, $1) = (1, 1)`), which
    /// constant folding does not look into.
    ///
    /// A `NULL` constant on either side is enough: `x = NULL` is never `TRUE`
    /// whatever `x` is.
    fn constants_reject_eq(
        &self,
        left: &Expression,
        right: &Expression,
    ) -> Result<bool, SbroadError> {
        match (left, right) {
            (Expression::Constant(Constant { value: Value::Null }), _)
            | (_, Expression::Constant(Constant { value: Value::Null })) => Ok(true),
            (
                Expression::Constant(Constant { value: l }),
                Expression::Constant(Constant { value: r }),
            ) => Ok(constant_pair_rejects_eq(l, r)),
            (Expression::Row(_), Expression::Row(_)) => {
                let left_list = left.get_row_list()?;
                let right_list = right.get_row_list()?;
                if left_list.len() != right_list.len() {
                    return Ok(false);
                }
                for (&l, &r) in left_list.iter().zip(right_list) {
                    let l = self.get_expression_node(l)?;
                    let r = self.get_expression_node(r)?;
                    if self.constants_reject_eq(&l, &r)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    /// Inner logic of `get_expression_tree_buckets` for simple expressions (without OR and AND
    /// operators).
    /// In general it returns `Buckets::All`, but in some cases (e.g. `Eq` and `In` operators) it
    /// will return `Buckets::Filtered` (if such a result is met in SELECT or JOIN filter, it means
    /// that we can execute the query only on some of the replicasets).
    fn get_buckets_from_expr<'p, R: BucketsResolver>(
        &'p self,
        expr: Expression<'p>,
        resolver: &R,
        hashed: Option<&RefCell<HashedKeys<'p, R::Buckets>>>,
    ) -> Result<Option<R::Buckets>, SbroadError> {
        let mut buckets = None;
        let tier = self.tier.as_ref();

        // As a conjunct, `x IS TRUE` holds exactly when `x` does, and
        // `x IS FALSE` exactly when `x = false`.
        if let Expression::Unary(UnaryExpr {
            op: op @ (Unary::IsTrue | Unary::IsFalse),
            child,
        }) = expr
        {
            let child_expr = self.get_expression_node(*child)?;
            if !child_expr.is_ref() {
                return match op {
                    Unary::IsTrue => self.get_buckets_from_expr(child_expr, resolver, hashed),
                    _ => Ok(None),
                };
            }
            let is_sharding_key = match self.ref_is_sharding_key(&child_expr)? {
                Some(is_sharding_key) => is_sharding_key,
                None => match self.get_distribution(*child)? {
                    Distribution::Segment { keys } => {
                        keys.iter().any(|key| key.positions.len() == 1)
                    }
                    _ => false,
                },
            };
            if is_sharding_key {
                // A value of this call: not kept in `hashed`.
                let value = Value::Boolean(*op == Unary::IsTrue);
                let values_buckets = resolver.buckets_from_values(&[&value], tier)?;
                conjunct_into(&mut buckets, values_buckets);
            }
            return Ok(buckets);
        }

        let Expression::Bool(BoolExpr {
            op: op @ (Bool::Eq | Bool::In),
            left,
            right,
            ..
        }) = expr
        else {
            return Ok(buckets);
        };
        let (left_id, right_id) = (*left, *right);
        let left = self.get_expression_node(left_id)?;
        let right = self.get_expression_node(right_id)?;

        if *op == Bool::Eq {
            if let Some(key_buckets) =
                self.key_eq_constant_buckets(&left, &right, resolver, hashed)?
            {
                return Ok(Some(key_buckets));
            }
        }
        self.get_buckets_from_cmp(*op, left_id, &left, right_id, &right, resolver, hashed)
    }

    /// [`Self::get_buckets_from_expr`] of `left op right`, where `op` is `=`
    /// or `IN` and `key = constant` is already checked.
    #[allow(clippy::too_many_arguments)]
    fn get_buckets_from_cmp<'p, R: BucketsResolver>(
        &'p self,
        op: Bool,
        left_id: NodeId,
        left: &Expression<'p>,
        right_id: NodeId,
        right: &Expression<'p>,
        resolver: &R,
        hashed: Option<&RefCell<HashedKeys<'p, R::Buckets>>>,
    ) -> Result<Option<R::Buckets>, SbroadError> {
        // Conjunction of everything collected for this expression.
        // The only case several `Buckets` are conjuncted here is `Eq`.
        let mut buckets = None;
        let tier = self.tier.as_ref();

        if op == Bool::Eq {
            // The expression is a conjunct of a DNF chain, so a `FALSE` or `NULL`
            // comparison of constants rejects every row of the chain.
            if self.constants_reject_eq(left, right)? {
                return Ok(Some(R::Buckets::new_empty()));
            }
            // Try to collect buckets from an explicit `bucket_id = value` predicate.
            self.collect_buckets_from_bucket_id_eq(
                Self::row_ids(left, &left_id),
                Self::row_ids(right, &right_id),
                resolver,
                tier,
                &mut buckets,
            )?;
        }

        // Try to collect buckets from expression of type `sharding_key = value`
        let pairs = [
            (left_id, left, right_id, right),
            (right_id, right, left_id, left),
        ];
        for (left_id, left_expr, right_id, right_expr) in pairs {
            if !left_expr.is_row() && !left_expr.is_ref() {
                continue;
            }

            let right_columns = match right_expr {
                Expression::Row(row) => row.list(),
                Expression::Constant(_)
                | Expression::Reference(_)
                | Expression::Parameter(_)
                | Expression::CurrentUser(_) => slice::from_ref(&right_id),
                _ => continue,
            };

            // The sharding keys of the left row, as positions in the row;
            // `None` if the row is not segmented.
            let left_dist;
            let keys: Option<SmallVec<[&[usize]; 4]>> = match self.ref_is_sharding_key(left_expr)? {
                Some(true) => Some(SmallVec::from_slice(&[&[0][..]])),
                Some(false) => None,
                None => {
                    left_dist = self.get_distribution(left_id)?;
                    match &left_dist {
                        Distribution::Segment { keys } => {
                            Some(keys.iter().map(|key| key.positions.as_slice()).collect())
                        }
                        _ => None,
                    }
                }
            };

            // Gather buckets from the right row.
            if let Some(keys) = keys {
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
                    if let Some(motion_buckets) = resolver.buckets_from_motion(motion_id)? {
                        conjunct_into(&mut buckets, motion_buckets);
                        return Ok(buckets);
                    }
                }

                let mut parts: SmallVec<[UnboundKeyPart; 4]> = SmallVec::new();
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
                for positions in keys {
                    // For cases with composite keys, we check that the condition fields
                    // are at least the size of the composite key.
                    // E.g. t1 sharded by (a, b), and we have query, with single value of
                    // the condition:
                    // `SELECT * FROM t1 JOIN t1 AS t ON 1 IN (t.a, t.b)`
                    // Since in this case it is not possible to calculate the buckets,
                    // therefore there is no need for further action.
                    if positions.len() > right_columns.len() {
                        continue;
                    }
                    parts.clear();
                    let mut has_current_user = false;
                    let mut is_fixed_key = true;
                    for position in positions {
                        // Since the sides in the "In" query can be of different lengths,
                        // we need to find a suitable position, as if they were the
                        // same length.
                        let pos = if op == Bool::In {
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

                        // A right side that is not a row is the column itself.
                        let right_column_expr = if right_expr.is_row() {
                            self.get_expression_node(right_column_id)?
                        } else {
                            right_expr.clone()
                        };
                        match right_column_expr {
                            Expression::Constant(Constant { value }) => {
                                parts.push(UnboundKeyPart::Value(value));
                            }
                            // Before `bind_params` we can't get actual value, but the
                            // query is still single-node if all sharding-key columns are
                            // fixed by constants or parameters. CURRENT_USER is replaced
                            // with a constant on bind as well.
                            Expression::Parameter(Parameter { index, .. }) => {
                                parts.push(UnboundKeyPart::Param(*index));
                            }
                            Expression::CurrentUser(_) => {
                                has_current_user = true;
                            }
                            _ => {
                                // Row-dependent expressions on any sharding-key column make
                                // the whole candidate key unresolved, even if other columns
                                // are fixed by constants or parameters.
                                is_fixed_key = false;
                                break;
                            }
                        }
                    }
                    if !is_fixed_key {
                        continue;
                    }
                    if has_current_user {
                        conjunct_into(&mut buckets, R::Buckets::new_estimated(1));
                        continue;
                    }
                    // `None` if some part is a parameter.
                    let values: Option<SmallVec<[&Value; 4]>> = parts
                        .iter()
                        .map(|part| match part {
                            UnboundKeyPart::Value(value) => Some(*value),
                            UnboundKeyPart::Param(_) | UnboundKeyPart::CurrentUser => None,
                        })
                        .collect();
                    match values {
                        None => {
                            let key_buckets = resolver.buckets_from_unbound_key(&parts, tier)?;
                            conjunct_into(&mut buckets, key_buckets);
                        }
                        Some(values) if !values.is_empty() => {
                            conjunct_key_buckets(resolver, &values, tier, hashed, &mut buckets)?;
                        }
                        Some(_) => {}
                    }
                }
            }
        }

        Ok(buckets)
    }

    /// Inner logic of `bucket_discovery` for expressions (currently it's called only on
    /// `Selection` or `Join` filters).
    /// `rel_children` is a set of relational children of `Selection` or `Join` node.
    /// It splits given expression into DNF chains (`ORed` expressions) and calls `get_buckets_from_expr` on each simple
    /// chain subexpression, later disjuncting results.
    ///
    /// In case there is just one expression (not `ANDed`) `chains` will remain empty.
    pub fn get_expression_tree_buckets<R: BucketsResolver>(
        &self,
        expr_id: NodeId,
        rel_children: &[NodeId],
        subqueries: &[NodeId],
        resolver: &R,
    ) -> Result<R::Buckets, SbroadError> {
        self.get_expression_tree_buckets_cached(expr_id, rel_children, subqueries, resolver, None)
    }

    /// As `get_expression_tree_buckets`, with the keys of the discovery in
    /// `hashed`, see [`conjunct_key_buckets`].
    pub fn get_expression_tree_buckets_cached<'p, R: BucketsResolver>(
        &'p self,
        expr_id: NodeId,
        rel_children: &[NodeId],
        subqueries: &[NodeId],
        resolver: &R,
        hashed: Option<&RefCell<HashedKeys<'p, R::Buckets>>>,
    ) -> Result<R::Buckets, SbroadError> {
        let expr = self.get_expression_node(expr_id)?;
        // Only the whole filter is handled here. A false conjunct follows the
        // expression-discovery rules below, just as it does during execution.
        if matches!(
            expr,
            Expression::Constant(Constant {
                value: Value::Boolean(false) | Value::Null,
            })
        ) {
            return Ok(R::Buckets::new_empty());
        }
        // `key = constant` gives filtered buckets, and their conjunction with
        // the default buckets below, `All` or `Any`, is the same buckets, so
        // the default is not needed.
        let eq = match &expr {
            Expression::Bool(BoolExpr {
                op: Bool::Eq,
                left,
                right,
            }) => {
                let left_expr = self.get_expression_node(*left)?;
                let right_expr = self.get_expression_node(*right)?;
                if let Some(buckets) =
                    self.key_eq_constant_buckets(&left_expr, &right_expr, resolver, hashed)?
                {
                    return Ok(buckets);
                }
                Some((*left, left_expr, *right, right_expr))
            }
            _ => None,
        };
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
        let default_buckets = self.default_filter_buckets::<R>(rel_children, subqueries)?;
        let conjunct_buckets = |expr| -> Result<R::Buckets, SbroadError> {
            Ok(self
                .get_buckets_from_expr(expr, resolver, hashed)?
                .unwrap_or_else(|| default_buckets.clone()))
        };
        // Without AND and OR the expression is the only conjunct of the only chain.
        if !matches!(
            expr,
            Expression::Bool(BoolExpr {
                op: Bool::And | Bool::Or,
                ..
            })
        ) {
            // A top `=` is not `key = constant`, so it is not checked again.
            let expr_buckets = match eq {
                Some((left_id, left, right_id, right)) => self.get_buckets_from_cmp(
                    Bool::Eq,
                    left_id,
                    &left,
                    right_id,
                    &right,
                    resolver,
                    hashed,
                )?,
                None => self.get_buckets_from_expr(expr, resolver, hashed)?,
            };
            let expr_buckets = expr_buckets.unwrap_or_else(|| default_buckets.clone());
            return Ok(default_buckets.conjunct_owned(expr_buckets));
        }
        let Some(chains) = self.get_dnf_chains(expr_id)? else {
            return Ok(default_buckets);
        };
        let mut result: Option<R::Buckets> = None;
        for mut chain in chains {
            let mut chain_buckets = default_buckets.clone();
            let nodes = chain.get_mut_nodes();
            // Nodes in the chain are in the top-down order (from left to right).
            // We need to pop back the chain to get nodes in the bottom-up order.
            while let Some(node_id) = nodes.pop_back() {
                let node_buckets = conjunct_buckets(self.get_expression_node(node_id)?)?;
                chain_buckets = chain_buckets.conjunct_owned(node_buckets);
            }
            result = Some(match result {
                Some(result) => result.disjunct_owned(chain_buckets),
                None => chain_buckets,
            });
        }

        Ok(result.unwrap_or_else(R::Buckets::all))
    }

    /// The buckets of a filter that restricts nothing, see
    /// [`Plan::get_expression_tree_buckets`].
    pub(crate) fn default_filter_buckets<R: BucketsResolver>(
        &self,
        rel_children: &[NodeId],
        subqueries: &[NodeId],
    ) -> Result<R::Buckets, SbroadError> {
        for child_id in rel_children.iter().chain(subqueries) {
            let child_dist = self.rel_distr_ref(*child_id)?;
            if matches!(child_dist, Distribution::Any | Distribution::Segment { .. }) {
                return Ok(R::Buckets::all());
            }
        }
        Ok(R::Buckets::any())
    }
}

#[cfg(test)]
mod tests;
