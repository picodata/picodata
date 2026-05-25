//! IR nodes for dynamic filter pushdown.
//!
//! The resolver pass inserts a pair of nodes around an INNER JOIN with
//! (Single, Single) distribution and a Motion(Full) on the probe side:
//!
//! * `BuildFilter` sits over the build subtree. At execute time it
//!   hashes the JOIN-key columns of each materialized build row into a
//!   `DynamicFilter` payload.
//!
//! * `ApplyFilter` sits over the probe subtree. At execute time it
//!   decodes the build's filter from the DQL wire buffer and retains
//!   only the rows whose JOIN-key hash hits.
//!
//! The two nodes are linked by a `filter_source` edge — semantically
//! the same "subquery-style" dependency `Selection` has on its sub-
//! queries, but pointing at a sibling subtree rather than a child.
//! `calculate_slices` traverses this edge through `Relational::subqueries`
//! so build-side slices land strictly before probe-side slices.

use serde::{Deserialize, Serialize};

use super::NodeId;

/// How an `ApplyFilter` should handle rows whose JOIN-key columns
/// include NULL.
///
/// SQL `=` is unknown when either operand is NULL, so the row would
/// never match — `Skip` reflects that semantics and drops the row
/// before hashing. `Insert` mixes a distinct NULL tag into the hash;
/// it exists so the filter can be used over `IS NOT DISTINCT FROM`
/// (NULL-equating) equalities. v1 resolver only emits `Skip`.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[repr(u8)]
pub enum NullPolicy {
    Skip = 0,
    Insert = 1,
}

impl From<NullPolicy> for sql_dynfilter::NullPolicy {
    fn from(policy: NullPolicy) -> Self {
        match policy {
            NullPolicy::Skip => sql_dynfilter::NullPolicy::Skip,
            NullPolicy::Insert => sql_dynfilter::NullPolicy::Insert,
        }
    }
}

/// Build-side node. Wraps the subtree that produces the JOIN-key rows
/// and tags them with a stable `filter_id` used by the matching
/// `ApplyFilter` to look up the encoded bytes in the DQL packet.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct BuildFilter {
    /// Subtree producing the build-side rows.
    pub child: NodeId,
    /// Expression `NodeId`s forming the JOIN key. Order must match
    /// `ApplyFilter::keys` so the canonical hash aligns on both sides.
    pub keys: Vec<NodeId>,
    /// Stable identifier shared with the paired `ApplyFilter`. Used as
    /// the map key in the DQL packet's filters field.
    pub filter_id: u32,
    /// NULL handling — must match the paired `ApplyFilter`'s policy or
    /// the filter is wrong by construction.
    pub null_policy: NullPolicy,
    /// Output tuple node.
    pub output: NodeId,
}

/// Probe-side node. Wraps the subtree being filtered and references
/// the matching `BuildFilter` via `filter_source`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ApplyFilter {
    /// Subtree the filter is being pushed down onto.
    pub child: NodeId,
    /// Expression `NodeId`s forming the JOIN key. Order must match
    /// `BuildFilter::keys`.
    pub keys: Vec<NodeId>,
    /// Stable identifier matching `BuildFilter::filter_id`. Used to
    /// look up the decoded `FilterView` from the DQL packet on the
    /// storage side.
    pub filter_id: u32,
    pub null_policy: NullPolicy,
    /// Build subtree node — a `BuildFilter`. Treated as a dependency
    /// edge for slice ordering: the build slice must execute before
    /// this node's slice.
    pub filter_source: NodeId,
    /// Output tuple node.
    pub output: NodeId,
}
