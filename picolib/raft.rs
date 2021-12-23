use slog::{debug, o};
use raft::eraftpb::ConfState;
use raft::StorageError;
use serde::{Deserialize, Serialize};
use ::tarantool::space::Space;
use ::tarantool::tuple::Tuple;
use ::tarantool::index::IteratorType;

pub use raft::prelude::*;
pub use raft::Error as RaftError;

pub type Node = RawNode<Storage>;
pub struct Storage;

impl Storage {
    #[allow(dead_code)]
    pub fn init_schema() {}

    pub fn persist_entries(entries: &Vec<Entry>) -> Result<(), RaftError> {
        let mut space = Space::find("raft_log").unwrap();
        for entry in entries {
            let row: LogRow = LogRow::from(entry);
            space.insert(&row).unwrap();
        }
        Ok(())
    }

    pub fn persist_hard_state(hs: &HardState) -> Result<(), RaftError> {
        let mut space: Space = Space::find("raft_state").unwrap();
        let row: HardStateRow = HardStateRow::from(hs);
        space.insert(&row).unwrap();
        Ok(())
    }

    pub fn persist_commit(commit: u64) -> Result<(), RaftError> {
        // let mut space: Space = Space::find("raft_state").unwrap();
        // space.primary_key().update()
        println!("--- persist_commit(idx = {})", commit);
        Ok(())
    }
}

impl raft::Storage for Storage {
    fn initial_state(&self) -> Result<RaftState, RaftError> {
        let space: Space = Space::find("raft_state").unwrap();
        let tuple: Option<Tuple> = space.primary_key().max(&()).unwrap();
        let row: Option<HardStateRow> = tuple
            .and_then(|t| t.into_struct().unwrap());
        let hs: HardState = row
            .map(|row| row.into())
            .unwrap_or_default();

        // See also: https://github.com/etcd-io/etcd/blob/main/raft/raftpb/raft.pb.go
        let cs: ConfState = ConfState {
            voters: vec![1],
            ..Default::default()
        };

        let ret: RaftState = RaftState::new(hs, cs);
        let logger = slog::Logger::root(crate::tarantool::SlogDrain, o!());
        debug!(logger, "+++ initial_state() -> {:?}", ret);
        Ok(ret)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        _max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<Entry>, RaftError> {
        let mut ret: Vec<Entry> = vec![];
        let space = Space::find("raft_log").unwrap();
        let iter = space.primary_key().select(IteratorType::GE, &(low,)).unwrap();

        for tuple in iter {
            let row: LogRow = tuple.into_struct().unwrap();
            if row.raft_index >= high {
                break;
            }
            ret.push(row.into());
        }
        // let max_size: Option<u64> = max_size.into();
        // let ret = self.0.entries(low, high, max_size);
        // let logger = slog::Logger::root(crate::tarantool::SlogDrain, o!());
        // debug!(logger, "+++ entries(low={}, high={}, max_size={:?}) -> {:?}",
        //     low, high, max_size, ret
        // );
        Ok(ret)
    }

    fn term(&self, idx: u64) -> Result<u64, RaftError> {
        if idx == 0 {
            return Ok(0);
        }

        let space = Space::find("raft_log").unwrap();

        let tuple = space.primary_key().get(&(idx,)).unwrap();
        let row: Option<LogRow> = tuple
            .and_then(|t| t.into_struct().unwrap());

        let logger = slog::Logger::root(crate::tarantool::SlogDrain, o!());
        if let Some(row) = row {
            debug!(logger, "+++ term(idx={}) -> {:?}", idx, row.raft_term);
            return Ok(row.raft_term)
        } else {
            debug!(logger, "+++ term(idx={}) -> Unavailable", idx);
            return Err(RaftError::Store(StorageError::Unavailable));
        }
    }

    fn first_index(&self) -> Result<u64, RaftError> {
        Ok(1)
    }

    fn last_index(&self) -> Result<u64, RaftError> {
        let space: Space = Space::find("raft_log").unwrap();
        let tuple: Option<Tuple> = space.primary_key().max(&()).unwrap();
        let row: Option<LogRow> = tuple
            .and_then(|t| t.into_struct().unwrap());
        let ret: u64 = row
            .map(|row| row.raft_index)
            .unwrap_or(0);

        let logger = slog::Logger::root(crate::tarantool::SlogDrain, o!());
        debug!(logger, "+++ last_index() -> {:?}", ret);
        Ok(ret)
    }

    fn snapshot(&self, request_index: u64) -> Result<Snapshot, RaftError> {
        let logger = slog::Logger::root(crate::tarantool::SlogDrain, o!());
        debug!(logger, "+++ snapshot(idx={}) -> unimplemented", request_index);
        unimplemented!();
    }
}


#[derive(Debug, Serialize, Deserialize)]
struct HardStateRow {
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
}

impl HardStateRow {
    fn from(hs: &HardState) -> HardStateRow {
        HardStateRow {
            term: hs.term,
            vote: hs.vote,
            commit: hs.commit,
        }
    }
}

impl From<HardStateRow> for HardState {
    fn from(hs: HardStateRow) -> HardState {
        HardState {
            term: hs.term,
            vote: hs.vote,
            commit: hs.commit,
            ..Default::default()
        }
    }
}

impl ::tarantool::tuple::AsTuple for HardStateRow {}

#[derive(Debug, Serialize, Deserialize)]
struct LogRow {
    pub raft_index: u64,
    pub raft_term: u64,
    pub raft_id: u64,
    pub command: String,
    pub data: Vec<u8>,
}

impl LogRow {
    fn from(e: &Entry) -> LogRow {
        LogRow {
            raft_index: e.get_index(),
            raft_term: e.get_term(),
            raft_id: 1,
            command: format!("{:?}", e.get_entry_type()),
            data: Vec::from(e.get_data()),
        }
    }
}

impl From<LogRow> for Entry {
    fn from(row: LogRow) -> Entry {
        let mut ret = Entry::new();

        let entry_type = match row.command.as_ref() {
            "EntryNormal" => EntryType::EntryNormal,
            _ => unreachable!(),
        };

        ret.set_entry_type(entry_type);
        ret.set_term(row.raft_term);
        ret.set_index(row.raft_index);
        ret.set_data(row.data.into());
        ret
    }

}

impl ::tarantool::tuple::AsTuple for LogRow {}
