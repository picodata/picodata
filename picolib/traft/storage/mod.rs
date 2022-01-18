use ::tarantool::index::IteratorType;
use ::tarantool::space::Space;
use ::tarantool::tuple::Tuple;
use raft::eraftpb::ConfState;
use raft::StorageError;
use std::convert::TryInto;

use crate::tlog;
use raft::prelude::Entry as RaftEntry;
use raft::prelude::HardState as RaftHardState;
use raft::prelude::RaftState;
use raft::prelude::Snapshot as RaftSnapshot;
use raft::Error as RaftError;

mod entry_row;
use entry_row::RaftEntryRow;

mod message;
pub use message::Message;

pub struct Storage;

impl Storage {
    pub fn init_schema() {
        crate::tarantool::eval(
            r#"
            box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})

            box.schema.space.create('raft_log', {
                if_not_exists = true,
                is_local = true,
                format = {
                    {name = 'entry_type', type = 'string', is_nullable = false},
                    {name = 'index', type = 'unsigned', is_nullable = false},
                    {name = 'term', type = 'unsigned', is_nullable = false},
                    {name = 'msg', type = 'any', is_nullable = true},
                }
            })
            box.space.raft_log:create_index('pk', {
                if_not_exists = true,
                parts = {{'index'}},
            })

            box.schema.space.create('raft_state', {
                if_not_exists = true,
                is_local = true,
                format = {
                    {name = 'key', type = 'string', is_nullable = false},
                    {name = 'value', type = 'any', is_nullable = false},
                }
            })

            box.space.raft_state:create_index('pk', {
                if_not_exists = true,
                parts = {{'key'}},
            })

            box.schema.space.create('raft_group', {
                if_not_exists = true,
                is_local = true,
                format = {
                    {name = 'raft_id', type = 'unsigned', is_nullable = false},
                    -- {name = 'raft_role', type = 'string', is_nullable = false},
                    -- {name = 'instance_id', type = 'string', is_nullable = false},
                    -- {name = 'instance_uuid', type = 'string', is_nullable = false},
                    -- {name = 'replicaset_id', type = 'string', is_nullable = false},
                    -- {name = 'replicaset_uuid', type = 'string', is_nullable = false},
                }
            })

            box.space.raft_group:create_index('pk', {
                if_not_exists = true,
                parts = {{'raft_id'}},
            })

            box.cfg({log_level = 6})
        "#,
        );
    }

    pub fn term() -> Option<u64> {
        let space: Space = Space::find("raft_state").unwrap();
        let row = space.get(&("term",)).unwrap();
        row.and_then(|row| row.field(1).unwrap())
    }

    pub fn vote() -> Option<u64> {
        let space: Space = Space::find("raft_state").unwrap();
        let row = space.get(&("vote",)).unwrap();
        row.and_then(|row| row.field(1).unwrap())
    }

    pub fn commit() -> Option<u64> {
        let space: Space = Space::find("raft_state").unwrap();
        let row = space.get(&("commit",)).unwrap();
        row.and_then(|row| row.field(1).unwrap())
    }

    pub fn persist_commit(commit: u64) {
        let mut space: Space = Space::find("raft_state").unwrap();
        space.replace(&("commit", commit)).unwrap();
    }

    pub fn applied() -> Option<u64> {
        let space: Space = Space::find("raft_state").unwrap();
        let row = space.get(&("applied",)).unwrap();
        row.and_then(|row| row.field(1).unwrap())
    }

    pub fn persist_applied(applied: u64) {
        let mut space: Space = Space::find("raft_state").unwrap();
        space.replace(&("applied", applied)).unwrap();
    }

    pub fn entries(low: u64, high: u64) -> Vec<RaftEntry> {
        let mut ret: Vec<RaftEntry> = vec![];
        let space = Space::find("raft_log").unwrap();
        let iter = space
            .primary_key()
            .select(IteratorType::GE, &(low,))
            .unwrap();

        for tuple in iter {
            let row: RaftEntryRow = tuple.into_struct().unwrap();
            if row.index >= high {
                break;
            }
            ret.push(row.into());
        }

        ret
    }

    pub fn persist_entries(entries: &Vec<RaftEntry>) {
        let mut space = Space::find("raft_log").unwrap();
        for entry in entries {
            let row: RaftEntryRow = entry.try_into().unwrap();
            space.insert(&row).unwrap();
        }
    }

    pub fn hard_state() -> RaftHardState {
        let mut ret = RaftHardState::default();
        Storage::term().map(|v| ret.term = v);
        Storage::vote().map(|v| ret.vote = v);
        Storage::commit().map(|v| ret.commit = v);
        ret
    }

    pub fn persist_hard_state(hs: &RaftHardState) {
        let mut space: Space = Space::find("raft_state").unwrap();
        space.replace(&("term", hs.term)).unwrap();
        space.replace(&("vote", hs.vote)).unwrap();
        space.replace(&("commit", hs.commit)).unwrap();
    }
}

impl raft::Storage for Storage {
    fn initial_state(&self) -> Result<RaftState, RaftError> {
        let hs = Storage::hard_state();

        // See also: https://github.com/etcd-io/etcd/blob/main/raft/raftpb/raft.pb.go
        let cs: ConfState = ConfState {
            voters: vec![1],
            ..Default::default()
        };

        let ret: RaftState = RaftState::new(hs, cs);
        tlog!(Debug, "+++ initial_state() -> {:?}", ret);
        Ok(ret)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        _max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<RaftEntry>, RaftError> {
        Ok(Storage::entries(low, high))
    }

    fn term(&self, idx: u64) -> Result<u64, RaftError> {
        if idx == 0 {
            return Ok(0);
        }

        let space = Space::find("raft_log").unwrap();

        let tuple = space.primary_key().get(&(idx,)).unwrap();
        let row: Option<RaftEntryRow> = tuple.and_then(|t| t.into_struct().unwrap());

        if let Some(row) = row {
            tlog!(Debug, "+++ term(idx={}) -> {:?}", idx, row.term);
            return Ok(row.term);
        } else {
            tlog!(Debug, "+++ term(idx={}) -> Unavailable", idx);
            return Err(RaftError::Store(StorageError::Unavailable));
        }
    }

    fn first_index(&self) -> Result<u64, RaftError> {
        Ok(1)
    }

    fn last_index(&self) -> Result<u64, RaftError> {
        let space: Space = Space::find("raft_log").unwrap();
        let tuple: Option<Tuple> = space.primary_key().max(&()).unwrap();
        let row: Option<RaftEntryRow> = tuple.and_then(|t| t.into_struct().unwrap());
        let ret: u64 = row.map(|row| row.index).unwrap_or(0);

        Ok(ret)
    }

    fn snapshot(&self, request_index: u64) -> Result<RaftSnapshot, RaftError> {
        tlog!(
            Critical,
            "+++ snapshot(idx={}) -> unimplemented",
            request_index
        );
        unimplemented!();
    }
}
