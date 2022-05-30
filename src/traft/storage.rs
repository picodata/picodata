use std::convert::TryFrom;

use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::StorageError;
use ::raft::INVALID_ID;
use ::tarantool::index::IteratorType;
use ::tarantool::space::Space;
use ::tarantool::tuple::Tuple;
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

use crate::tlog;
use crate::traft;

pub struct Storage;

const RAFT_GROUP: &str = "raft_group";
const RAFT_STATE: &str = "raft_state";
const RAFT_LOG: &str = "raft_log";

#[derive(Debug, Error)]
enum Error {
    #[error("no such space \"{0}\"")]
    NoSuchSpace(String),
    #[error("no such index \"{1}\" in space \"{0}\"")]
    NoSuchIndex(String, String),
}

macro_rules! box_err {
    () => {
        |e| StorageError::Other(Box::new(e))
    };
}

impl Storage {
    pub fn init_schema() {
        crate::tarantool::eval(
            r#"
            box.schema.space.create('raft_log', {
                if_not_exists = true,
                is_local = true,
                format = {
                    {name = 'entry_type', type = 'unsigned', is_nullable = false},
                    {name = 'index', type = 'unsigned', is_nullable = false},
                    {name = 'term', type = 'unsigned', is_nullable = false},
                    {name = 'data', type = 'any', is_nullable = true},
                    {name = 'context', type = 'any', is_nullable = true},
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
                    {name = 'peer_address', type = 'string', is_nullable = false},
                    {name = 'voter', type = 'boolean', is_nullable = false},
                    {name = 'instance_id', type = 'string', is_nullable = false},
                    {name = 'replicaset_id', type = 'string', is_nullable = false},
                    {name = 'instance_uuid', type = 'string', is_nullable = false},
                    {name = 'replicaset_uuid', type = 'string', is_nullable = false},
                    {name = 'commit_index', type = 'unsigned', is_nullable = false},
                }
            })

            box.space.raft_group:create_index('pk', {
                if_not_exists = true,
                parts = {{'raft_id'}},
            })
            box.space.raft_group:create_index('instance_id', {
                if_not_exists = true,
                parts = {{'instance_id'}},
                unique = true,
            })
            box.space.raft_group:create_index('replicaset_id', {
                if_not_exists = true,
                parts = {{'replicaset_id'}, {'commit_index'}},
                unique = false,
            })
        "#,
        );
    }

    fn space(name: &str) -> Result<Space, StorageError> {
        Space::find(name)
            .ok_or_else(|| Error::NoSuchSpace(name.into()))
            .map_err(box_err!())
    }

    fn persist_raft_state<T: Serialize>(key: &str, value: T) -> Result<(), StorageError> {
        Storage::space(RAFT_STATE)?
            .put(&(key, value))
            .map_err(box_err!())?;
        Ok(())
    }

    fn raft_state<T: DeserializeOwned>(key: &str) -> Result<Option<T>, StorageError> {
        let tuple: Option<Tuple> = Storage::space(RAFT_STATE)?
            .get(&(key,))
            .map_err(box_err!())?;

        match tuple {
            Some(t) => t.field(1).map_err(box_err!()),
            None => Ok(None),
        }
    }

    pub fn peer_by_raft_id(raft_id: u64) -> Result<Option<traft::Peer>, StorageError> {
        if raft_id == INVALID_ID {
            unreachable!("peer_by_raft_id called with invalid id ({})", INVALID_ID);
        }

        let tuple = Storage::space(RAFT_GROUP)?
            .get(&(raft_id,))
            .map_err(box_err!())?;

        match tuple {
            None => Ok(None),
            Some(v) => Ok(Some(v.into_struct().map_err(box_err!())?)),
        }
    }

    pub fn peer_by_instance_id(instance_id: &str) -> Result<Option<traft::Peer>, StorageError> {
        const IDX: &str = "instance_id";
        let tuple = Storage::space(RAFT_GROUP)?
            .index(IDX)
            .ok_or_else(|| Error::NoSuchIndex(RAFT_GROUP.into(), IDX.into()))
            .map_err(box_err!())?
            .get(&(instance_id,))
            .map_err(box_err!())?;

        match tuple {
            None => Ok(None),
            Some(v) => Ok(Some(v.into_struct().map_err(box_err!())?)),
        }
    }

    pub fn peers() -> Result<Vec<traft::Peer>, StorageError> {
        let mut ret = Vec::new();

        let iter = Storage::space(RAFT_GROUP)?
            .select(IteratorType::All, &())
            .map_err(box_err!())?;

        for tuple in iter {
            ret.push(tuple.into_struct().map_err(box_err!())?);
        }

        Ok(ret)
    }

    pub fn box_replication(peer: &traft::Peer) -> Result<Vec<String>, StorageError> {
        let mut ret = Vec::new();

        const IDX: &str = "replicaset_id";
        let iter = Storage::space(RAFT_GROUP)?
            .index(IDX)
            .ok_or_else(|| Error::NoSuchIndex(RAFT_GROUP.into(), IDX.into()))
            .map_err(box_err!())?
            .select(IteratorType::GE, &(&peer.replicaset_id,))
            .map_err(box_err!())?;

        for tuple in iter {
            let replica: traft::Peer = tuple.into_struct().map_err(box_err!())?;

            if replica.replicaset_id != peer.replicaset_id
                || replica.commit_index > peer.commit_index
            {
                // In Tarantool the iteration must be interrupted explicitly.
                break;
            }

            ret.push(replica.peer_address);
        }

        Ok(ret)
    }

    pub fn id() -> Result<Option<u64>, StorageError> {
        Storage::raft_state("id")
    }

    pub fn cluster_id() -> Result<Option<String>, StorageError> {
        Storage::raft_state("cluster_id")
    }

    /// Node generation i.e. the number of restarts.
    pub fn gen() -> Result<Option<u64>, StorageError> {
        Storage::raft_state("gen")
    }

    pub fn term() -> Result<Option<u64>, StorageError> {
        Storage::raft_state("term")
    }

    pub fn vote() -> Result<Option<u64>, StorageError> {
        Storage::raft_state("vote")
    }

    pub fn commit() -> Result<Option<u64>, StorageError> {
        Storage::raft_state("commit")
    }

    pub fn applied() -> Result<Option<u64>, StorageError> {
        Storage::raft_state("applied")
    }

    pub fn persist_commit(commit: u64) -> Result<(), StorageError> {
        // tlog!(Info, "++++++ persist commit {commit}");
        Storage::persist_raft_state("commit", commit)
    }

    pub fn persist_applied(applied: u64) -> Result<(), StorageError> {
        Storage::persist_raft_state("applied", applied)
    }

    pub fn persist_term(term: u64) -> Result<(), StorageError> {
        Storage::persist_raft_state("term", term)
    }

    pub fn persist_vote(vote: u64) -> Result<(), StorageError> {
        Storage::persist_raft_state("vote", vote)
    }

    pub fn persist_gen(gen: u64) -> Result<(), StorageError> {
        Storage::persist_raft_state("gen", gen)
    }

    pub fn persist_id(id: u64) -> Result<(), StorageError> {
        Storage::space(RAFT_STATE)?
            // We use `insert` instead of `replace` here
            // because `id` can never be changed.
            .insert(&("id", id))
            .map_err(box_err!())?;

        Ok(())
    }

    pub fn persist_cluster_id(id: &str) -> Result<(), StorageError> {
        Storage::space(RAFT_STATE)?
            // We use `insert` instead of `replace` here
            // because `cluster_id` should never be changed.
            .insert(&("cluster_id", id))
            .map_err(box_err!())?;
        Ok(())
    }

    pub fn persist_peer(peer: &traft::Peer) -> Result<(), StorageError> {
        Storage::space(RAFT_GROUP)?
            .replace(peer)
            .map_err(box_err!())?;

        Ok(())
    }

    pub fn entries(low: u64, high: u64) -> Result<Vec<raft::Entry>, StorageError> {
        // idx \in [low, high)
        let mut ret: Vec<raft::Entry> = vec![];
        let iter = Storage::space(RAFT_LOG)?
            .select(IteratorType::GE, &(low,))
            .map_err(box_err!())?;

        for tuple in iter {
            let row: traft::Entry = tuple.into_struct().map_err(box_err!())?;
            if row.index >= high {
                break;
            }
            let entry = raft::Entry::try_from(row)?;
            ret.push(entry);
        }

        Ok(ret)
    }

    pub fn persist_entries(entries: &[raft::Entry]) -> Result<(), StorageError> {
        let mut space = Storage::space(RAFT_LOG)?;
        for e in entries {
            let row = traft::Entry::try_from(e).unwrap();
            space.replace(&row).map_err(box_err!())?;
        }

        Ok(())
    }

    pub fn conf_state() -> Result<raft::ConfState, StorageError> {
        Ok(raft::ConfState {
            voters: Storage::raft_state("voters")?.unwrap_or_default(),
            learners: Storage::raft_state("learners")?.unwrap_or_default(),
            ..Default::default()
        })
    }

    pub fn persist_conf_state(cs: &raft::ConfState) -> Result<(), StorageError> {
        Storage::persist_raft_state("voters", &cs.voters)?;
        Storage::persist_raft_state("learners", &cs.learners)?;
        Ok(())
    }

    pub fn hard_state() -> Result<raft::HardState, StorageError> {
        let mut ret = raft::HardState::default();
        if let Some(term) = Storage::term()? {
            ret.term = term;
        }
        if let Some(vote) = Storage::vote()? {
            ret.vote = vote;
        }
        if let Some(commit) = Storage::commit()? {
            ret.commit = commit;
        }

        Ok(ret)
    }

    pub fn persist_hard_state(hs: &raft::HardState) -> Result<(), StorageError> {
        Storage::persist_term(hs.term)?;
        Storage::persist_vote(hs.vote)?;
        Storage::persist_commit(hs.commit)?;
        Ok(())
    }
}

impl raft::Storage for Storage {
    fn initial_state(&self) -> Result<raft::RaftState, RaftError> {
        // See also: https://github.com/etcd-io/etcd/blob/main/raft/raftpb/raft.pb.go
        let hs = Storage::hard_state()?;
        let cs = Storage::conf_state()?;

        let ret = raft::RaftState::new(hs, cs);
        Ok(ret)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        _max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<raft::Entry>, RaftError> {
        // tlog!(Info, "++++++ entries {low} {high}");
        Ok(Storage::entries(low, high)?)
    }

    fn term(&self, idx: u64) -> Result<u64, RaftError> {
        if idx == 0 {
            return Ok(0);
        }
        // tlog!(Info, "++++++ term {idx}");

        let tuple = Storage::space(RAFT_LOG)?.get(&(idx,)).map_err(box_err!())?;

        if let Some(tuple) = tuple {
            Ok(tuple.field(2).map_err(box_err!())?.unwrap())
        } else {
            Err(RaftError::Store(StorageError::Unavailable))
        }
    }

    fn first_index(&self) -> Result<u64, RaftError> {
        // tlog!(Info, "++++++ first_index");
        Ok(1)
    }

    fn last_index(&self) -> Result<u64, RaftError> {
        let space: Space = Storage::space(RAFT_LOG)?;
        let tuple: Option<Tuple> = space.primary_key().max(&()).map_err(box_err!())?;

        if let Some(t) = tuple {
            Ok(t.field(1).map_err(box_err!())?.unwrap())
        } else {
            Ok(0)
        }
    }

    fn snapshot(&self, idx: u64) -> Result<raft::Snapshot, RaftError> {
        tlog!(Critical, "snapshot"; "request_index" => idx);
        unimplemented!();

        // Ok(Storage::snapshot()?)
    }
}

macro_rules! assert_err {
    ($expr:expr, $err:expr) => {
        assert_eq!($expr.map_err(|e| format!("{e}")), Err($err.into()))
    };
}

inventory::submit!(crate::InnerTest {
    name: "test_storage_log",
    body: || {
        use ::raft::Storage as _;
        let test_entries = vec![raft::Entry {
            term: 9u64,
            index: 99u64,
            ..Default::default()
        }];

        Storage::persist_entries(&test_entries).unwrap();

        assert_eq!(Storage.first_index(), Ok(1));
        assert_eq!(Storage.last_index(), Ok(99));
        assert_eq!(Storage.term(99), Ok(9));
        assert_eq!(Storage.entries(1, 99, u64::MAX), Ok(vec![]));
        assert_eq!(Storage.entries(1, 100, u64::MAX), Ok(test_entries));

        assert_eq!(
            Storage.term(100).map_err(|e| format!("{e}")),
            Err("log unavailable".into())
        );

        let mut raft_log = Storage::space("raft_log").unwrap();

        raft_log.put(&(1337, 99, 1, "", ())).unwrap();
        assert_err!(
            Storage.entries(1, 100, u64::MAX),
            "unknown error unknown entry type (1337)"
        );

        raft_log.put(&(0, 99, 1, "", false)).unwrap();
        assert_err!(
            Storage.entries(1, 100, u64::MAX),
            concat!(
                "unknown error",
                " Failed to decode tuple:",
                " data did not match any variant",
                " of untagged enum EntryContext"
            )
        );

        raft_log.primary_key().drop().unwrap();
        assert_err!(
            Storage.entries(1, 100, u64::MAX),
            concat!(
                "unknown error",
                " Tarantool error:",
                " NoSuchIndexID:",
                " No index #0 is defined in space 'raft_log'"
            )
        );

        raft_log.drop().unwrap();
        assert_err!(
            Storage.entries(1, 100, u64::MAX),
            "unknown error no such space \"raft_log\""
        );
    }
});

inventory::submit!(crate::InnerTest {
    name: "test_storage_state",
    body: || {
        use ::raft::Storage as _;

        Storage::persist_term(9u64).unwrap();
        Storage::persist_vote(1u64).unwrap();
        Storage::persist_commit(98u64).unwrap();
        Storage::persist_applied(97u64).unwrap();

        let state = Storage.initial_state().unwrap();
        assert_eq!(
            state.hard_state,
            raft::HardState {
                term: 9,
                vote: 1,
                commit: 98,
                ..Default::default()
            }
        );

        let mut raft_state = Storage::space("raft_state").unwrap();

        raft_state.delete(&("id",)).unwrap();
        assert_eq!(Storage::id(), Ok(None));

        Storage::persist_id(16).unwrap();
        assert_err!(
            Storage::persist_id(32),
            concat!(
                "unknown error",
                " Tarantool error:",
                " TupleFound:",
                " Duplicate key exists in unique index \"pk\" in space \"raft_state\"",
                " with old tuple - [\"id\", 16]",
                " and new tuple - [\"id\", 32]"
            )
        );

        raft_state.primary_key().drop().unwrap();
        assert_err!(
            Storage::term(),
            concat!(
                "unknown error",
                " Tarantool error:",
                " NoSuchIndexID:",
                " No index #0 is defined in space 'raft_state'"
            )
        );

        raft_state.drop().unwrap();
        assert_err!(
            Storage::commit(),
            "unknown error no such space \"raft_state\""
        );
    }
});

inventory::submit!(crate::InnerTest {
    name: "test_storage_peers",
    body: || {
        let mut raft_group = Storage::space(RAFT_GROUP).unwrap();

        for peer in vec![
            // r1
            (1u64, "addr:1", true, "i1", "r1", "i1-uuid", "r1-uuid", 1u64),
            (2, "addr:2", true, "i2", "r1", "i2-uuid", "r1-uuid", 2),
            // r2
            (3, "addr:3", true, "i3", "r2", "i3-uuid", "r2-uuid", 10),
            (4, "addr:4", true, "i4", "r2", "i4-uuid", "r2-uuid", 10),
            // r3
            (5, "addr:5", true, "i5", "r3", "i5-uuid", "r3-uuid", 10),
        ] {
            raft_group.put(&peer).unwrap();
        }

        let peers = Storage::peers().unwrap();
        assert_eq!(
            peers.iter().map(|p| &p.instance_id).collect::<Vec<_>>(),
            vec!["i1", "i2", "i3", "i4", "i5"]
        );

        assert_err!(
            Storage::persist_peer(&traft::Peer {
                raft_id: 99,
                instance_id: "i1".into(),
                ..Default::default()
            }),
            concat!(
                "unknown error",
                " Tarantool error:",
                " TupleFound: Duplicate key exists",
                " in unique index \"instance_id\"",
                " in space \"raft_group\"",
                " with old tuple",
                " - [1, \"addr:1\", true, \"i1\", \"r1\", \"i1-uuid\", \"r1-uuid\", 1]",
                " and new tuple",
                " - [99, \"\", false, \"i1\", \"\", \"\", \"\", 0]"
            )
        );

        {
            // Ensure traft storage doesn't impose restrictions
            // on peer_address uniqueness.
            let peer = |id: u64, addr: &str| traft::Peer {
                raft_id: id,
                instance_id: format!("i{id}"),
                peer_address: addr.into(),
                ..Default::default()
            };

            Storage::persist_peer(&peer(10, "addr:collision")).unwrap();
            Storage::persist_peer(&peer(11, "addr:collision")).unwrap();
        }

        let peer_by_raft_id = |id: u64| Storage::peer_by_raft_id(id).unwrap().unwrap();
        {
            assert_eq!(peer_by_raft_id(1).instance_id, "i1");
            assert_eq!(peer_by_raft_id(2).instance_id, "i2");
            assert_eq!(peer_by_raft_id(3).instance_id, "i3");
            assert_eq!(peer_by_raft_id(4).instance_id, "i4");
            assert_eq!(peer_by_raft_id(5).instance_id, "i5");
            assert_eq!(Storage::peer_by_raft_id(6), Ok(None));
        }

        let peer_by_instance_id = |iid| Storage::peer_by_instance_id(iid).unwrap().unwrap();
        {
            assert_eq!(peer_by_instance_id("i1").peer_address, "addr:1");
            assert_eq!(peer_by_instance_id("i2").peer_address, "addr:2");
            assert_eq!(peer_by_instance_id("i3").peer_address, "addr:3");
            assert_eq!(peer_by_instance_id("i4").peer_address, "addr:4");
            assert_eq!(peer_by_instance_id("i5").peer_address, "addr:5");
            assert_eq!(
                peer_by_instance_id("i10").peer_address,
                peer_by_instance_id("i11").peer_address
            );
            assert_eq!(Storage::peer_by_instance_id("i6"), Ok(None));
        }

        let box_replication = |rsid: &str, idx: u64| {
            let peer = traft::Peer {
                replicaset_id: rsid.into(),
                commit_index: idx,
                ..Default::default()
            };
            Storage::box_replication(&peer).unwrap()
        };

        {
            assert_eq!(box_replication("r1", 0), Vec::<&str>::new());
            assert_eq!(box_replication("XX", 99), Vec::<&str>::new());

            assert_eq!(box_replication("r1", 1), vec!["addr:1"]);
            assert_eq!(box_replication("r1", 2), vec!["addr:1", "addr:2"]);
            assert_eq!(box_replication("r1", 99), vec!["addr:1", "addr:2"]);

            assert_eq!(box_replication("r2", 10), vec!["addr:3", "addr:4"]);
            assert_eq!(box_replication("r2", 10), vec!["addr:3", "addr:4"]);

            assert_eq!(box_replication("r3", 10), vec!["addr:5"]);
        }

        raft_group.index("instance_id").unwrap().drop().unwrap();

        assert_err!(
            Storage::peer_by_instance_id("i1"),
            concat!(
                "unknown error",
                " no such index \"instance_id\"",
                " in space \"raft_group\""
            )
        );

        raft_group.index("replicaset_id").unwrap().drop().unwrap();

        assert_err!(
            Storage::box_replication(&traft::Peer::default()),
            concat!(
                "unknown error",
                " no such index \"replicaset_id\"",
                " in space \"raft_group\""
            )
        );

        raft_group.primary_key().drop().unwrap();

        assert_err!(
            Storage::peer_by_raft_id(1),
            concat!(
                "unknown error",
                " Tarantool error:",
                " NoSuchIndexID:",
                " No index #0 is defined in space 'raft_group'"
            )
        );

        raft_group.drop().unwrap();

        assert_err!(
            Storage::peers(),
            "unknown error no such space \"raft_group\""
        );
    }
});
