use serde::Serialize;

use super::{CallProcedure, NodeAligned};

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum BlockOwned {
    CallProcedure(CallProcedure),
}

impl From<BlockOwned> for NodeAligned {
    fn from(value: BlockOwned) -> Self {
        match value {
            BlockOwned::CallProcedure(proc) => proc.into(),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Eq, PartialEq, Serialize)]
pub enum MutBlock<'a> {
    CallProcedure(&'a mut CallProcedure),
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum Block<'a> {
    CallProcedure(&'a CallProcedure),
}

impl Block<'_> {
    #[must_use]
    pub fn get_block_owned(&self) -> BlockOwned {
        match self {
            Block::CallProcedure(call) => BlockOwned::CallProcedure((*call).clone()),
        }
    }
}
