use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::ir::helpers::RepeatableState;

use crate::executor::engine::VersionMap;
use crate::ir::node::NodeId;

use super::vtable::VirtualTableMeta;

pub type VTablesMeta = HashMap<NodeId, VirtualTableMeta>;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
pub struct SchemaInfo {
    pub table_version_map: VersionMap,
    pub index_version_map: HashMap<[u32; 2], u64, RepeatableState>,
}

impl SchemaInfo {
    #[must_use]
    pub fn new(
        table_version_map: VersionMap,
        index_version_map: HashMap<[u32; 2], u64, RepeatableState>,
    ) -> Self {
        SchemaInfo {
            table_version_map,
            index_version_map,
        }
    }
}
