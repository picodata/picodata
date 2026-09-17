pub(super) mod model;

use model::{RawIndex, RawTable, RawTier};

#[derive(Debug, Clone)]
pub(super) struct Catalog {
    /// More than one means an upgrade is in progress.
    pub(super) instance_versions: Vec<String>,
    pub(super) catalog_version: Option<String>,
    pub(super) tiers: Vec<RawTier>,
    pub(super) tables: Vec<RawTable>,
    pub(super) indexes: Vec<RawIndex>,
}
