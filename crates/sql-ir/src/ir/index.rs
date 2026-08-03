use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::collections::HashMap;

/// Table is a tuple storage in the cluster.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Index {
    pub table_id: u32,
    pub id: u32,
}

impl Index {
    pub fn from(table_id: u32, id: u32) -> Index {
        Index { table_id, id }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Indexes {
    pub indexes: HashMap<SmolStr, Index>,
}

impl Default for Indexes {
    fn default() -> Self {
        Self::new()
    }
}

impl Indexes {
    #[must_use]
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
        }
    }

    pub fn insert(&mut self, index_name: SmolStr, index: Index) {
        self.indexes.insert(index_name, index);
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Index> {
        self.indexes.get(name)
    }

    pub fn drain(&mut self) -> HashMap<SmolStr, Index> {
        std::mem::take(&mut self.indexes)
    }
}
