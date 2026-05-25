//! NULL handling policy for dynamic filter pushdown.
//!
//! Dynamic filter pushdown for INNER JOIN is recorded in
//! `Plan::dynamic_filters` (a sidecar list of `DynamicFilterSpec`) rather
//! than via IR nodes. The only artefact this module still exposes is the
//! `NullPolicy` enum, shared by the planner sidecar and the executor.

use serde::{Deserialize, Serialize};

/// How a dynamic filter should handle rows whose JOIN-key columns
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
