mod node;
mod storage;

pub use node::Node;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
pub use storage::Storage;
pub mod row {
    mod entry;
    mod message;
    mod peer;

    pub use entry::Entry;
    pub use message::Message;
    pub use peer::Peer;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct LogicalClock {
    id: u64,
    gen: u64,
    count: u64,
}

impl LogicalClock {
    pub fn new(id: u64, gen: u64) -> Self {
        Self { id, gen, count: 0 }
    }

    pub fn inc(&mut self) {
        self.count += 1;
    }
}

impl TryFrom<&[u8]> for LogicalClock {
    type Error = rmp_serde::decode::Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        rmp_serde::from_read_ref(data)
    }
}

impl From<&LogicalClock> for Vec<u8> {
    fn from(lc: &LogicalClock) -> Vec<u8> {
        rmp_serde::to_vec(lc).unwrap()
    }
}
