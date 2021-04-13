use raft::prelude::{Entry, HardState, Snapshot};
use raft::storage::Storage;
use raft::{Error as RaftError, RaftState};

use crate::error::Error;

pub struct NodeStorage {}

impl NodeStorage {
    pub fn new() -> Result<Self, Error> {
        Ok(NodeStorage {})
    }

    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<(), Error> {
        unimplemented!()
    }

    pub fn append(&mut self, entries: &[Entry]) -> Result<(), Error> {
        unimplemented!()
    }

    pub fn set_hard_state(&mut self, hs: HardState) -> Result<(), Error> {
        unimplemented!()
    }

    pub fn set_last_apply_index(&mut self, index: u64) -> Result<(), Error> {
        unimplemented!()
    }
}

impl Storage for NodeStorage {
    fn initial_state(&self) -> Result<RaftState, RaftError> {
        unimplemented!()
    }

    fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<Vec<Entry>, RaftError> {
        unimplemented!()
    }

    fn term(&self, idx: u64) -> Result<u64, RaftError> {
        unimplemented!()
    }

    fn first_index(&self) -> Result<u64, RaftError> {
        unimplemented!()
    }

    fn last_index(&self) -> Result<u64, RaftError> {
        unimplemented!()
    }

    fn snapshot(&self) -> Result<Snapshot, RaftError> {
        unimplemented!()
    }
}
