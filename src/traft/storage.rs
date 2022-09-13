use ::raft::StorageError;
use ::raft::INVALID_ID;
use ::tarantool::index::IteratorType;
use ::tarantool::space::Space;
use ::tarantool::tuple::{ToTupleBuffer, Tuple};
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::define_str_enum;
use crate::traft;
use crate::traft::RaftId;
use crate::traft::RaftIndex;

pub struct Storage;

////////////////////////////////////////////////////////////////////////////////
// RaftSpace
////////////////////////////////////////////////////////////////////////////////

define_str_enum! {
    /// An enumeration of builtin raft spaces
    pub enum RaftSpace {
        Group = "raft_group",
        State = "raft_state",
        Log = "raft_log",
    }

    FromStr::Err = UnknownRaftSpace;
}

#[derive(Error, Debug)]
#[error("unknown raft space {0}")]
pub struct UnknownRaftSpace(pub String);

// TODO(gmoshkin): remove this
const RAFT_GROUP: &str = RaftSpace::Group.as_str();
const RAFT_STATE: &str = RaftSpace::State.as_str();

////////////////////////////////////////////////////////////////////////////////
// RaftStateKey
////////////////////////////////////////////////////////////////////////////////

define_str_enum! {
    /// An enumeration of builtin raft spaces
    pub enum RaftStateKey {
        ReplicationFactor = "replication_factor",
        Commit = "commit",
        Applied = "applied",
        Term = "term",
        Vote = "vote",
        Gen = "gen",
        Voters = "voters",
        Learners = "learners",
        VotersOutgoing = "voters_outgoing",
        LearnersNext = "learners_next",
        AutoLeave = "auto_leave",
    }

    FromStr::Err = UnknownRaftStateKey;
}

#[derive(Error, Debug)]
#[error("unknown raft state key {0}")]
pub struct UnknownRaftStateKey(pub String);

////////////////////////////////////////////////////////////////////////////////
// Error
////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
enum Error {
    #[error("no such space \"{0}\"")]
    NoSuchSpace(String),
    #[error("no such index \"{1}\" in space \"{0}\"")]
    NoSuchIndex(String, String),
}

fn box_err(e: impl std::error::Error + Sync + Send + 'static) -> StorageError {
    StorageError::Other(Box::new(e))
}

impl Storage {
    pub fn init_schema() {
        crate::tarantool::eval(
            r#"
            box.schema.space.create('raft_group', {
                if_not_exists = true,
                is_local = true,
                format = {
                    {name = 'instance_id', type = 'string', is_nullable = false},
                    {name = 'instance_uuid', type = 'string', is_nullable = false},
                    {name = 'raft_id', type = 'unsigned', is_nullable = false},
                    {name = 'peer_address', type = 'string', is_nullable = false},
                    {name = 'replicaset_id', type = 'string', is_nullable = false},
                    {name = 'replicaset_uuid', type = 'string', is_nullable = false},
                    {name = 'commit_index', type = 'unsigned', is_nullable = false},
                    {name = 'grade', type = 'string', is_nullable = false},
                    {name = 'target_grade', type = 'string', is_nullable = false},
                    {name = 'failure_domain', type = 'map', is_nullable = false},
                }
            })
            box.space.raft_group:create_index('instance_id', {
                if_not_exists = true,
                parts = {{'instance_id'}},
                unique = true,
            })
            box.space.raft_group:create_index('raft_id', {
                if_not_exists = true,
                parts = {{'raft_id'}},
                unique = true,
            })
            box.space.raft_group:create_index('replicaset_id', {
                if_not_exists = true,
                parts = {{'replicaset_id'}, {'commit_index'}},
                unique = false,
            })
        "#,
        )
        .unwrap();
    }

    fn space(name: &str) -> Result<Space, StorageError> {
        Space::find(name)
            .ok_or_else(|| Error::NoSuchSpace(name.into()))
            .map_err(box_err)
    }

    fn raft_state<T: DeserializeOwned>(key: &str) -> Result<Option<T>, StorageError> {
        let tuple: Option<Tuple> = Storage::space(RAFT_STATE)?.get(&(key,)).map_err(box_err)?;

        match tuple {
            Some(t) => t.field(1).map_err(box_err),
            None => Ok(None),
        }
    }

    pub fn peer_by_raft_id(raft_id: RaftId) -> Result<Option<traft::Peer>, StorageError> {
        if raft_id == INVALID_ID {
            unreachable!("peer_by_raft_id called with invalid id ({})", INVALID_ID);
        }

        const IDX: &str = "raft_id";

        let tuple = Storage::space(RAFT_GROUP)?
            .index(IDX)
            .ok_or_else(|| Error::NoSuchIndex(RAFT_GROUP.into(), IDX.into()))
            .map_err(box_err)?
            .get(&(raft_id,))
            .map_err(box_err)?;

        match tuple {
            None => Ok(None),
            Some(v) => Ok(Some(v.decode().map_err(box_err)?)),
        }
    }

    pub fn peer_by_instance_id(instance_id: &str) -> Result<Option<traft::Peer>, StorageError> {
        const IDX: &str = "instance_id";
        let tuple = Storage::space(RAFT_GROUP)?
            .index(IDX)
            .ok_or_else(|| Error::NoSuchIndex(RAFT_GROUP.into(), IDX.into()))
            .map_err(box_err)?
            .get(&(instance_id,))
            .map_err(box_err)?;

        match tuple {
            None => Ok(None),
            Some(v) => Ok(Some(v.decode().map_err(box_err)?)),
        }
    }

    pub fn peers() -> Result<Vec<traft::Peer>, StorageError> {
        let mut ret = Vec::new();

        let iter = Storage::space(RAFT_GROUP)?
            .select(IteratorType::All, &())
            .map_err(box_err)?;

        for tuple in iter {
            ret.push(tuple.decode().map_err(box_err)?);
        }

        Ok(ret)
    }

    pub fn box_replication(
        replicaset_id: &str,
        max_index: Option<RaftIndex>,
    ) -> Result<Vec<String>, StorageError> {
        let mut ret = Vec::new();

        const IDX: &str = "replicaset_id";
        let iter = Storage::space(RAFT_GROUP)?
            .index(IDX)
            .ok_or_else(|| Error::NoSuchIndex(RAFT_GROUP.into(), IDX.into()))
            .map_err(box_err)?
            .select(IteratorType::GE, &(replicaset_id,))
            .map_err(box_err)?;

        for tuple in iter {
            let replica: traft::Peer = tuple.decode().map_err(box_err)?;

            if replica.replicaset_id != replicaset_id {
                // In Tarantool the iteration must be interrupted explicitly.
                break;
            }

            if matches!(max_index, Some(idx) if replica.commit_index > idx) {
                break;
            }

            ret.push(replica.peer_address);
        }

        Ok(ret)
    }

    pub fn replication_factor() -> Result<Option<u8>, StorageError> {
        Storage::raft_state(RaftStateKey::ReplicationFactor.as_str())
    }

    pub fn persist_peer(peer: &traft::Peer) -> Result<(), StorageError> {
        Storage::space(RAFT_GROUP)?.replace(peer).map_err(box_err)?;

        Ok(())
    }

    #[allow(dead_code)]
    pub fn delete_peer(instance_id: &str) -> Result<(), StorageError> {
        Storage::space(RAFT_GROUP)?
            .delete(&[instance_id])
            .map_err(box_err)?;

        Ok(())
    }

    pub fn insert(space: RaftSpace, tuple: &impl ToTupleBuffer) -> Result<Tuple, StorageError> {
        Storage::space(space.as_str())?
            .insert(tuple)
            .map_err(box_err)
    }

    pub fn replace(space: RaftSpace, tuple: &impl ToTupleBuffer) -> Result<Tuple, StorageError> {
        Storage::space(space.as_str())?
            .replace(tuple)
            .map_err(box_err)
    }

    pub fn update(
        space: RaftSpace,
        key: &impl ToTupleBuffer,
        ops: &[impl ToTupleBuffer],
    ) -> Result<Option<Tuple>, StorageError> {
        Storage::space(space.as_str())?
            .update(key, ops)
            .map_err(box_err)
    }

    #[rustfmt::skip]
    pub fn delete(
        space: RaftSpace,
        key: &impl ToTupleBuffer,
    ) -> Result<Option<Tuple>, StorageError> {
        Storage::space(space.as_str())?
            .delete(key)
            .map_err(box_err)
    }
}

macro_rules! assert_err {
    ($expr:expr, $err:expr) => {
        assert_eq!($expr.map_err(|e| format!("{e}")), Err($err.into()))
    };
}

#[rustfmt::skip]
inventory::submit!(crate::InnerTest {
    name: "test_storage_peers",
    body: || {
        use traft::{Grade, TargetGrade};

        let mut raft_group = Storage::space(RAFT_GROUP).unwrap();

        let faildom = crate::traft::FailureDomain::from([("a", "b")]);

        for peer in vec![
            // r1
            ("i1", "i1-uuid", 1u64, "addr:1", "r1", "r1-uuid", 1u64, Grade::Online, TargetGrade::Online, &faildom,),
            ("i2", "i2-uuid", 2u64, "addr:2", "r1", "r1-uuid",    2, Grade::Online, TargetGrade::Online, &faildom,),
            // r2
            ("i3", "i3-uuid", 3u64, "addr:3", "r2", "r2-uuid",   10, Grade::Online, TargetGrade::Online, &faildom,),
            ("i4", "i4-uuid", 4u64, "addr:4", "r2", "r2-uuid",   10, Grade::Online, TargetGrade::Online, &faildom,),
            // r3
            ("i5", "i5-uuid", 5u64, "addr:5", "r3", "r3-uuid",   10, Grade::Online, TargetGrade::Online, &faildom,),
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
                raft_id: 1,
                instance_id: "i99".into(),
                ..Default::default()
            }),
            format!(
                concat!(
                    "unknown error",
                    " Tarantool error:",
                    " TupleFound: Duplicate key exists",
                    " in unique index \"raft_id\"",
                    " in space \"raft_group\"",
                    " with old tuple",
                    r#" - ["i1", "i1-uuid", 1, "addr:1", "r1", "r1-uuid", 1, "{gon}", "{tgon}", {{"A": "B"}}]"#,
                    " and new tuple",
                    r#" - ["i99", "", 1, "", "", "", 0, "{goff}", "{tgon}", {{}}]"#,
                ),
                gon = Grade::Online,
                goff = Grade::Offline,
                tgon = TargetGrade::Online,
            )
        );

        {
            // Ensure traft storage doesn't impose restrictions
            // on peer_address uniqueness.
            let peer = |id: RaftId, addr: &str| traft::Peer {
                raft_id: id,
                instance_id: format!("i{id}"),
                peer_address: addr.into(),
                ..Default::default()
            };

            Storage::persist_peer(&peer(10, "addr:collision")).unwrap();
            Storage::persist_peer(&peer(11, "addr:collision")).unwrap();
        }

        let peer_by_raft_id = |id: RaftId| Storage::peer_by_raft_id(id).unwrap().unwrap();
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

        let box_replication = |replicaset_id: &str, max_index: Option<RaftIndex>| {
            Storage::box_replication(replicaset_id, max_index).unwrap()
        };

        {
            assert_eq!(box_replication("r1", Some(0)), Vec::<&str>::new());
            assert_eq!(box_replication("XX", None), Vec::<&str>::new());

            assert_eq!(box_replication("r1", Some(1)), vec!["addr:1"]);
            assert_eq!(box_replication("r1", Some(2)), vec!["addr:1", "addr:2"]);
            assert_eq!(box_replication("r1", Some(99)), vec!["addr:1", "addr:2"]);
            assert_eq!(box_replication("r1", None), vec!["addr:1", "addr:2"]);

            assert_eq!(box_replication("r2", Some(10)), vec!["addr:3", "addr:4"]);
            assert_eq!(box_replication("r2", Some(10)), vec!["addr:3", "addr:4"]);
            assert_eq!(box_replication("r2", None), vec!["addr:3", "addr:4"]);

            assert_eq!(box_replication("r3", Some(10)), vec!["addr:5"]);
            assert_eq!(box_replication("r3", None), vec!["addr:5"]);
        }

        raft_group.index("raft_id").unwrap().drop().unwrap();

        assert_err!(
            Storage::peer_by_raft_id(1),
            concat!(
                "unknown error",
                " no such index \"raft_id\"",
                " in space \"raft_group\""
            )
        );

        raft_group.index("replicaset_id").unwrap().drop().unwrap();

        assert_err!(
            Storage::box_replication("", None),
            concat!(
                "unknown error",
                " no such index \"replicaset_id\"",
                " in space \"raft_group\""
            )
        );

        raft_group.primary_key().drop().unwrap();

        assert_err!(
            Storage::peer_by_instance_id("i1"),
            concat!(
                "unknown error",
                " no such index \"instance_id\"",
                " in space \"raft_group\""
            )
        );

        raft_group.drop().unwrap();

        assert_err!(
            Storage::peers(),
            "unknown error no such space \"raft_group\""
        );
    }
});
