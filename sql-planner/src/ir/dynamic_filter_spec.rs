//! Sidecar specification for dynamic filter pushdown.
//!
//! `DynamicFilterSpec` records — outside the IR tree — every dynamic
//! filter the resolver chose to install. The coordinator-side executor
//! consults this list instead of walking the IR for filter nodes; this
//! keeps filter knowledge out of the 80+ match arms that touch
//! `Relational` and confines it to the executor (build/apply) plus the
//! resolver (which produces the spec).

use serde::{Deserialize, Serialize};

use crate::ir::node::{NodeId, NullPolicy};

/// One installed dynamic filter, populated by the resolver and consumed
/// by the §5.4 build/apply executor pass. The spec is wire-encoded into
/// the DQL packet keyed by `filter_id`; the build side emits the
/// `DynamicFilter` and the probe side reads it back.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DynamicFilterSpec {
    /// Stable identifier matching `BuildFilter::filter_id` /
    /// `ApplyFilter::filter_id`. Used as the map key in the DQL packet's
    /// filters field.
    pub filter_id: u32,
    /// The build-side Motion whose materialized vtable feeds the
    /// filter. The §5.4 build pass hashes this vtable's rows.
    pub build_motion_id: NodeId,
    /// The probe-side Motion whose materialized vtable is retained
    /// against the filter. Slice scheduling guarantees `build_motion_id`
    /// is materialized before this motion.
    pub probe_motion_id: NodeId,
    /// Column positions on `build_motion_id`'s output that form the
    /// JOIN key. Order must match `probe_columns`.
    pub build_columns: Vec<usize>,
    /// Column positions on `probe_motion_id`'s output (or the storage
    /// subtree below it) that form the JOIN key. Order must match
    /// `build_columns`.
    pub probe_columns: Vec<usize>,
    /// How `Null` in any key column should be handled. The v1 resolver
    /// only emits `Skip`.
    pub null_policy: NullPolicy,
}
