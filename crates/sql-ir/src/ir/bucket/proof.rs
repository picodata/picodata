//! Static routing bounds. Distinct keys may map to the same real bucket.

use std::collections::HashSet;

use smol_str::SmolStr;
use tarantool::tuple::FieldType;

use super::{value_to_bucket_id, BucketAlgebra, BucketsResolver};
use crate::errors::SbroadError;
use crate::ir::helpers::RepeatableState;
use crate::ir::node::NodeId;
use crate::ir::value::{MsgPackValue, Value};

/// The inputs to the router's key hash, compared in full even on hash collisions.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RoutingKey {
    types: Vec<FieldType>,
    bytes: Vec<u8>,
}

impl RoutingKey {
    fn new(values: &[&Value]) -> Result<Self, SbroadError> {
        let tuple: Vec<_> = values.iter().map(|v| MsgPackValue::from(*v)).collect();
        Ok(Self {
            types: values.iter().map(|v| v.field_type()).collect(),
            bytes: rmp_serde::to_vec(&tuple)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BucketProof {
    All,
    Any,
    /// Every selected bucket must equal the bucket of *each* key in `keys`.
    /// An empty set of keys says nothing about whether there are any buckets.
    Filtered {
        upper: usize,
        nonempty: bool,
        keys: HashSet<RoutingKey, RepeatableState>,
    },
    /// May include Any: unlike All, this cannot preserve a key through OR.
    Unknown,
}

impl BucketProof {
    fn bounded(upper: usize, nonempty: bool) -> Self {
        Self::Filtered {
            upper,
            nonempty,
            keys: HashSet::with_hasher(RepeatableState),
        }
    }

    pub(crate) fn is_single_node(&self) -> bool {
        matches!(self, Self::Any | Self::Filtered { upper: 0..=1, .. })
    }

    pub(crate) fn is_nonempty_filtered(&self) -> bool {
        matches!(self, Self::Filtered { nonempty: true, .. })
    }
}

impl BucketAlgebra for BucketProof {
    fn all() -> Self {
        Self::All
    }
    fn any() -> Self {
        Self::Any
    }
    fn new_empty() -> Self {
        Self::bounded(0, false)
    }
    fn new_estimated(count: usize) -> Self {
        // A parameter may bind to NULL and empty the filter.
        Self::bounded(count, false)
    }
    fn new_at_most_one() -> Self {
        Self::bounded(1, false)
    }

    fn conjunct(&self, other: &Self) -> Self {
        use BucketProof::{All, Any, Filtered, Unknown};
        match (self, other) {
            (Filtered { upper: 0, .. }, _) | (_, Filtered { upper: 0, .. }) => Self::new_empty(),
            (
                Filtered {
                    upper: a,
                    nonempty: an,
                    keys: ak,
                },
                Filtered {
                    upper: b,
                    nonempty: bn,
                    keys: bk,
                },
            ) => Filtered {
                upper: (*a).min(*b),
                nonempty: *an && *bn && !ak.is_disjoint(bk),
                keys: ak.union(bk).cloned().collect(),
            },
            (Unknown, Filtered { upper, keys, .. }) | (Filtered { upper, keys, .. }, Unknown) => {
                Filtered {
                    upper: *upper,
                    nonempty: false,
                    keys: keys.clone(),
                }
            }
            (Unknown, _) | (_, Unknown) => Unknown,
            (All | Any, filtered @ Filtered { .. }) | (filtered @ Filtered { .. }, All | Any) => {
                filtered.clone()
            }
            (Any, _) | (_, Any) => Any,
            (All, All) => All,
        }
    }

    fn disjunct(&self, other: &Self) -> Self {
        use BucketProof::{All, Any, Filtered, Unknown};
        match (self, other) {
            (All, _) | (_, All) => All,
            (Unknown, _) | (_, Unknown) => Unknown,
            (Filtered { upper: 0, .. }, other) | (other, Filtered { upper: 0, .. }) => {
                other.clone()
            }
            (
                Filtered {
                    upper: a,
                    nonempty: an,
                    keys: ak,
                },
                Filtered {
                    upper: b,
                    nonempty: bn,
                    keys: bk,
                },
            ) => {
                let keys: HashSet<_, _> = ak.intersection(bk).cloned().collect();
                let upper = if keys.is_empty() {
                    a.saturating_add(*b)
                } else {
                    1
                };
                Filtered {
                    upper,
                    nonempty: *an || *bn,
                    keys,
                }
            }
            (Any, filtered @ Filtered { nonempty: true, .. })
            | (filtered @ Filtered { nonempty: true, .. }, Any) => filtered.clone(),
            // If the filtered set turns out empty, the executor returns Any.
            // Keeping its keys could then incorrectly constrain a later AND.
            (Any, Filtered { .. }) | (Filtered { .. }, Any) => Unknown,
            (Any, Any) => Any,
        }
    }
}

pub(crate) struct StaticResolver;

impl BucketsResolver for StaticResolver {
    type Buckets = BucketProof;

    fn buckets_from_motion(&self, _: NodeId) -> Result<Option<BucketProof>, SbroadError> {
        Ok(None)
    }

    fn buckets_from_values(
        &self,
        values: &[&Value],
        _: Option<&SmolStr>,
    ) -> Result<BucketProof, SbroadError> {
        Ok(BucketProof::Filtered {
            upper: 1,
            nonempty: true,
            keys: HashSet::from_iter([RoutingKey::new(values)?]),
        })
    }

    fn buckets_from_bucket_id(
        &self,
        value: &Value,
        _: Option<&SmolStr>,
    ) -> Result<BucketProof, SbroadError> {
        Ok(match value_to_bucket_id(value) {
            Some(id) if id >= 1 => BucketProof::new_at_most_one(),
            _ => BucketProof::new_empty(),
        })
    }
}

#[cfg(test)]
mod tests;
