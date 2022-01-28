use std::convert::TryFrom;

use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::StorageError;
use ::tarantool::index::IteratorType;
use ::tarantool::space::Space;
use ::tarantool::tuple::Tuple;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::tlog;
use crate::traft::row;

pub struct Storage;

pub const SPACE_RAFT_STATE: &str = "raft_state";
pub const SPACE_RAFT_LOG: &str = "raft_log";

impl Storage {
    pub fn init_schema() {
        crate::tarantool::eval(
            r#"
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
        "#,
        );
    }

    fn persist_raft_state<T: Serialize>(key: &str, value: T) {
        Space::find(SPACE_RAFT_STATE)
            .unwrap()
            .replace(&(key, value))
            .unwrap();
    }

    fn raft_state<T: DeserializeOwned>(key: &str) -> Option<T> {
        Space::find(SPACE_RAFT_STATE)?
            .get(&(key,))
            .unwrap()?
            .field(1)
            .unwrap()
    }

    pub fn term() -> Option<u64> {
        Storage::raft_state("term")
    }

    pub fn vote() -> Option<u64> {
        Storage::raft_state("vote")
    }

    pub fn commit() -> Option<u64> {
        Storage::raft_state("commit")
    }

    pub fn applied() -> Option<u64> {
        Storage::raft_state("applied")
    }

    pub fn persist_commit(commit: u64) {
        Storage::persist_raft_state("commit", commit)
    }

    pub fn persist_applied(applied: u64) {
        Storage::persist_raft_state("applied", applied)
    }

    pub fn persist_term(term: u64) {
        Storage::persist_raft_state("term", term)
    }

    pub fn persist_vote(vote: u64) {
        Storage::persist_raft_state("vote", vote)
    }

    pub fn entries(low: u64, high: u64) -> Vec<raft::Entry> {
        let mut ret: Vec<raft::Entry> = vec![];
        let space = Space::find(SPACE_RAFT_LOG).unwrap();
        let iter = space
            .primary_key()
            .select(IteratorType::GE, &(low,))
            .unwrap();

        for tuple in iter {
            let row: row::Entry = tuple.into_struct().unwrap();
            if row.index >= high {
                break;
            }
            ret.push(raft::Entry::from(row));
        }

        ret
    }

    pub fn persist_entries(entries: &Vec<raft::Entry>) {
        let mut space = Space::find(SPACE_RAFT_LOG).unwrap();
        for e in entries {
            let row = row::Entry::try_from(e.clone()).unwrap();
            space.insert(&row).unwrap();
        }
    }

    pub fn hard_state() -> raft::HardState {
        let mut ret = raft::HardState::default();
        Storage::term().map(|v| ret.term = v);
        Storage::vote().map(|v| ret.vote = v);
        Storage::commit().map(|v| ret.commit = v);
        ret
    }

    pub fn persist_hard_state(hs: &raft::HardState) {
        Storage::persist_term(hs.term);
        Storage::persist_vote(hs.vote);
        Storage::persist_commit(hs.commit);
    }
}

impl raft::Storage for Storage {
    fn initial_state(&self) -> Result<raft::RaftState, RaftError> {
        let hs = Storage::hard_state();

        // See also: https://github.com/etcd-io/etcd/blob/main/raft/raftpb/raft.pb.go
        let cs = raft::ConfState {
            voters: vec![1],
            ..Default::default()
        };

        let ret = raft::RaftState::new(hs, cs);
        tlog!(Debug, "+++ initial_state() -> {:?}", ret);
        Ok(ret)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        _max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<raft::Entry>, RaftError> {
        Ok(Storage::entries(low, high))
    }

    fn term(&self, idx: u64) -> Result<u64, RaftError> {
        if idx == 0 {
            return Ok(0);
        }

        let space = Space::find("raft_log").unwrap();

        let tuple = space.primary_key().get(&(idx,)).unwrap();
        let row: Option<row::Entry> = tuple.and_then(|t| t.into_struct().unwrap());

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
        let row: Option<row::Entry> = tuple.and_then(|t| t.into_struct().unwrap());
        let ret: u64 = row.map(|row| row.index).unwrap_or(0);

        Ok(ret)
    }

    fn snapshot(&self, request_index: u64) -> Result<raft::Snapshot, RaftError> {
        tlog!(
            Critical,
            "+++ snapshot(idx={}) -> unimplemented",
            request_index
        );
        unimplemented!();
    }
}
