use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::StorageError;
use ::tarantool::index::IteratorType;
use ::tarantool::space::Space;
use ::tarantool::tuple::Tuple;
use std::convert::TryFrom as _;

use crate::tlog;
use crate::traft;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use crate::util::str_eq;

fn box_err(e: impl std::error::Error + Sync + Send + 'static) -> StorageError {
    StorageError::Other(Box::new(e))
}

#[derive(Clone, Debug)]
pub struct RaftSpaceAccess {
    space_raft_log: Space,
    space_raft_state: Space,
}

macro_rules! auto_impl {
    (
        // getters
        $(
            $(#[$meta:meta])*
            $vis:vis fn $getter:ident(&self) -> _<$ty:ty>;
        )+
    ) => {
        $(
            $(#[$meta])*
            $vis fn $getter(&self) -> tarantool::Result<Option<$ty>> {
                let key: &str = stringify!($getter);
                let tuple: Option<Tuple> = self.space_raft_state.get(&(key,))?;

                match tuple {
                    Some(t) => t.field(Self::FIELD_STATE_VALUE),
                    None => Ok(None),
                }
            }
        )+
    };

    (
        // setters
        $(
            $(#[$meta:meta])*
            $vis:vis fn $setter:ident(
                &self,
                $mod:ident $key:ident: $ty:ty
            ) -> _;
        )+
    ) => {
        $(
            $(#[$meta])*
            $vis fn $setter(&self, value: $ty) -> tarantool::Result<()> {
                const _: () = assert!(str_eq(
                    stringify!($setter),
                    concat!("persist_", stringify!($key))
                ));
                let key: &str = stringify!($key);
                self.space_raft_state.$mod(&(key, value))?;
                Ok(())
            }
        )+
    };
}

impl RaftSpaceAccess {
    const SPACE_RAFT_LOG: &'static str = "raft_log";
    const SPACE_RAFT_STATE: &'static str = "raft_state";
    const FIELD_STATE_VALUE: u32 = 1;
    const FIELD_ENTRY_INDEX: u32 = 1;
    const FIELD_ENTRY_TERM: u32 = 2;

    pub fn new() -> tarantool::Result<Self> {
        use tarantool::space::Field;

        let space_raft_log = Space::builder(Self::SPACE_RAFT_LOG)
            .is_local(true)
            .is_temporary(false)
            .field(Field::unsigned("entry_type"))
            .field(Field::unsigned("index"))
            .field(Field::unsigned("term"))
            .field(Field::any("data"))
            .field(Field::any("context"))
            .if_not_exists(true)
            .create()?;

        space_raft_log
            .index_builder("pk")
            .unique(true)
            .part("index")
            .if_not_exists(true)
            .create()?;

        let space_raft_state = Space::builder(Self::SPACE_RAFT_STATE)
            .is_local(true)
            .is_temporary(false)
            .field(Field::string("key"))
            .field(Field::any("value"))
            .if_not_exists(true)
            .create()?;

        space_raft_state
            .index_builder("pk")
            .unique(true)
            .part("key")
            .if_not_exists(true)
            .create()?;

        Ok(Self {
            space_raft_log,
            space_raft_state,
        })
    }

    // Find the meaning of the fields here:
    // https://github.com/etcd-io/etcd/blob/main/raft/raftpb/raft.pb.go

    auto_impl! {
        pub fn raft_id(&self) -> _<RaftId>;
        #[allow(dead_code)]
        pub fn instance_id(&self) -> _<String>;
        pub fn cluster_id(&self) -> _<String>;

        /// Node generation i.e. the number of restarts.
        pub fn gen(&self) -> _<u64>;
        pub(super) fn term(&self) -> _<RaftTerm>;
        fn vote(&self) -> _<RaftId>;
        pub(super) fn commit(&self) -> _<RaftIndex>;
        pub fn applied(&self) -> _<RaftIndex>;

        pub(crate) fn voters(&self) -> _<Vec<RaftId>>;
        pub(crate) fn learners(&self) -> _<Vec<RaftId>>;
        fn voters_outgoing(&self) -> _<Vec<RaftId>>;
        fn learners_next(&self) -> _<Vec<RaftId>>;
        fn auto_leave(&self) -> _<bool>;
    }

    pub fn conf_state(&self) -> tarantool::Result<raft::ConfState> {
        Ok(raft::ConfState {
            voters: self.voters()?.unwrap_or_default(),
            learners: self.learners()?.unwrap_or_default(),
            voters_outgoing: self.voters_outgoing()?.unwrap_or_default(),
            learners_next: self.learners_next()?.unwrap_or_default(),
            auto_leave: self.auto_leave()?.unwrap_or_default(),
            ..Default::default()
        })
    }

    pub fn hard_state(&self) -> tarantool::Result<raft::HardState> {
        Ok(raft::HardState {
            term: self.term()?.unwrap_or_default(),
            vote: self.vote()?.unwrap_or_default(),
            commit: self.commit()?.unwrap_or_default(),
            ..Default::default()
        })
    }

    pub fn entries(&self, low: RaftIndex, high: RaftIndex) -> tarantool::Result<Vec<raft::Entry>> {
        // low <= idx < high
        let mut ret: Vec<raft::Entry> = vec![];
        let iter = self.space_raft_log.select(IteratorType::GE, &(low,))?;

        for tuple in iter {
            let row: traft::Entry = tuple.decode()?;
            if row.index >= high {
                break;
            }
            ret.push(row.into());
        }

        Ok(ret)
    }

    pub fn all_traft_entries(&self) -> tarantool::Result<Vec<traft::Entry>> {
        self.space_raft_log
            .select(IteratorType::All, &())?
            .map(|tuple| tuple.decode())
            .collect()
    }

    auto_impl! {
        pub fn persist_raft_id(&self, insert raft_id: RaftId) -> _;
        pub fn persist_instance_id(&self, insert instance_id: &str) -> _;
        pub fn persist_cluster_id(&self, insert cluster_id: &str) -> _;

        pub fn persist_gen(&self, replace gen: u64) -> _;
        fn persist_term(&self, replace term: RaftTerm) -> _;
        fn persist_vote(&self, replace vote: RaftId) -> _;
        pub fn persist_commit(&self, replace commit: RaftIndex) -> _;
        pub fn persist_applied(&self, replace applied: RaftIndex) -> _;

        fn persist_voters(&self, replace voters: &[RaftId]) -> _;
        fn persist_learners(&self, replace learners: &[RaftId]) -> _;
        fn persist_voters_outgoing(&self, replace voters_outgoing: &[RaftId]) -> _;
        fn persist_learners_next(&self, replace learners_next: &[RaftId]) -> _;
        fn persist_auto_leave(&self, replace auto_leave: bool) -> _;
    }

    pub fn persist_conf_state(&self, cs: &raft::ConfState) -> tarantool::Result<()> {
        self.persist_voters(&cs.voters)?;
        self.persist_learners(&cs.learners)?;
        self.persist_voters_outgoing(&cs.voters_outgoing)?;
        self.persist_learners_next(&cs.learners_next)?;
        self.persist_auto_leave(cs.auto_leave)?;
        Ok(())
    }

    pub fn persist_hard_state(&self, hs: &raft::HardState) -> tarantool::Result<()> {
        self.persist_term(hs.term)?;
        self.persist_vote(hs.vote)?;
        self.persist_commit(hs.commit)?;
        Ok(())
    }

    pub fn persist_entries(&self, entries: &[raft::Entry]) -> tarantool::Result<()> {
        for e in entries {
            let row = traft::Entry::try_from(e).unwrap();
            self.space_raft_log.replace(&row)?;
        }
        Ok(())
    }
}

impl raft::Storage for RaftSpaceAccess {
    fn initial_state(&self) -> Result<raft::RaftState, RaftError> {
        let hs = self.hard_state().map_err(box_err)?;
        let cs = self.conf_state().map_err(box_err)?;
        let ret = raft::RaftState::new(hs, cs);
        Ok(ret)
    }

    fn entries(
        &self,
        low: RaftIndex,
        high: RaftIndex,
        _max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<raft::Entry>, RaftError> {
        // tlog!(Info, "++++++ entries {low} {high}");
        Ok(self.entries(low, high).map_err(box_err)?)
    }

    fn term(&self, idx: RaftIndex) -> Result<RaftTerm, RaftError> {
        if idx == 0 {
            return Ok(0);
        }
        // tlog!(Info, "++++++ term {idx}");

        let tuple = self.space_raft_log.get(&(idx,)).map_err(box_err)?;

        if let Some(tuple) = tuple {
            // Unwrap is fine here: term isn't nullable.
            Ok(tuple
                .field(Self::FIELD_ENTRY_TERM)
                .map_err(box_err)?
                .unwrap())
        } else {
            Err(RaftError::Store(StorageError::Unavailable))
        }
    }

    fn first_index(&self) -> Result<RaftIndex, RaftError> {
        // tlog!(Info, "++++++ first_index");
        Ok(1)
    }

    fn last_index(&self) -> Result<RaftIndex, RaftError> {
        let tuple: Option<Tuple> = self
            .space_raft_log
            .primary_key()
            .max(&())
            .map_err(box_err)?;

        if let Some(t) = tuple {
            // Unwrap is fine here: index isn't nullable.
            Ok(t.field(Self::FIELD_ENTRY_INDEX).map_err(box_err)?.unwrap())
        } else {
            Ok(0)
        }
    }

    fn snapshot(&self, idx: RaftIndex) -> Result<raft::Snapshot, RaftError> {
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
    name: "test_storage2",
    body: || {
        // It's fine to create two access objects
        let s1 = RaftSpaceAccess::new().unwrap();
        let s2 = RaftSpaceAccess::new().unwrap();
        assert_eq!(s1.space_raft_log.id(), s2.space_raft_log.id());
        assert_eq!(s1.space_raft_state.id(), s2.space_raft_state.id());

        // Though it's better to clone one
        #[allow(clippy::redundant_clone)]
        let _s3 = s2.clone();
    }
});

inventory::submit!(crate::InnerTest {
    name: "test_storage2_log",
    body: || {
        use ::raft::Storage as S;
        let test_entries = vec![raft::Entry {
            term: 9u64,
            index: 99u64,
            ..Default::default()
        }];

        let storage = RaftSpaceAccess::new().unwrap();
        storage.persist_entries(&test_entries).unwrap();

        assert_eq!(S::first_index(&storage), Ok(1));
        assert_eq!(S::last_index(&storage), Ok(99));
        assert_eq!(S::term(&storage, 99), Ok(9));
        assert_eq!(S::entries(&storage, 1, 99, u64::MAX), Ok(vec![]));
        assert_eq!(S::entries(&storage, 1, 100, u64::MAX), Ok(test_entries));

        assert_eq!(
            S::term(&storage, 100).map_err(|e| format!("{e}")),
            Err("log unavailable".into())
        );

        let raft_log = Space::find(RaftSpaceAccess::SPACE_RAFT_LOG).unwrap();

        raft_log.put(&(1337, 99, 1, "", ())).unwrap();
        assert_err!(
            S::entries(&storage, 1, 100, u64::MAX),
            "unknown error Failed to decode tuple: unknown entry type (1337)"
        );

        raft_log.put(&(0, 99, 1, "", false)).unwrap();
        assert_err!(
            S::entries(&storage, 1, 100, u64::MAX),
            concat!(
                "unknown error",
                " Failed to decode tuple:",
                " data did not match any variant",
                " of untagged enum EntryContext"
            )
        );

        raft_log.primary_key().drop().unwrap();
        assert_err!(
            S::entries(&storage, 1, 100, u64::MAX),
            concat!(
                "unknown error",
                " Tarantool error:",
                " NoSuchIndexID:",
                " No index #0 is defined in space 'raft_log'"
            )
        );

        raft_log.drop().unwrap();
        assert_err!(
            S::entries(&storage, 1, 100, u64::MAX),
            format!(
                concat!(
                    "unknown error",
                    " Tarantool error:",
                    " NoSuchSpace:",
                    " Space '{}' does not exist",
                ),
                storage.space_raft_log.id()
            )
        );
    }
});

inventory::submit!(crate::InnerTest {
    name: "test_storage2_hard_state",
    body: || {
        use ::raft::Storage as S;

        let storage = RaftSpaceAccess::new().unwrap();

        assert_eq!(
            S::initial_state(&storage).unwrap().hard_state,
            raft::HardState::default()
        );

        let hs = raft::HardState {
            term: 9,
            vote: 1,
            commit: 98,
            ..Default::default()
        };
        storage.persist_hard_state(&hs).unwrap();
        assert_eq!(S::initial_state(&storage).unwrap().hard_state, hs);
    }
});

inventory::submit!(crate::InnerTest {
    name: "test_storage2_conf_state",
    body: || {
        use ::raft::Storage as S;

        let storage = RaftSpaceAccess::new().unwrap();

        assert_eq!(
            S::initial_state(&storage).unwrap().conf_state,
            raft::ConfState::default()
        );

        let cs = raft::ConfState {
            auto_leave: true,
            voters: vec![1],
            learners: vec![2],
            learners_next: vec![3, 4],
            voters_outgoing: vec![5, 6],
            ..Default::default()
        };

        storage.persist_conf_state(&cs).unwrap();
        assert_eq!(S::initial_state(&storage).unwrap().conf_state, cs);
    }
});

inventory::submit!(crate::InnerTest {
    name: "test_storage2_other_state",
    body: || {
        let storage = RaftSpaceAccess::new().unwrap();
        let raft_state = Space::find(RaftSpaceAccess::SPACE_RAFT_STATE).unwrap();

        assert_eq!(storage.cluster_id().unwrap(), None);

        storage.persist_cluster_id("test_cluster").unwrap();
        storage.persist_cluster_id("test_cluster_2").unwrap_err();
        assert_eq!(storage.cluster_id().unwrap(), Some("test_cluster".into()));

        raft_state.delete(&("id",)).unwrap();
        assert_eq!(storage.raft_id().unwrap(), None);

        storage.persist_raft_id(16).unwrap();
        assert_err!(
            storage.persist_raft_id(32),
            concat!(
                "Tarantool error:",
                " TupleFound:",
                " Duplicate key exists in unique index \"pk\" in space \"raft_state\"",
                " with old tuple - [\"raft_id\", 16]",
                " and new tuple - [\"raft_id\", 32]"
            )
        );

        raft_state.primary_key().drop().unwrap();
        assert_err!(
            storage.term(),
            concat!(
                "Tarantool error:",
                " NoSuchIndexID:",
                " No index #0 is defined in space 'raft_state'"
            )
        );

        raft_state.drop().unwrap();
        assert_err!(
            storage.commit(),
            format!(
                concat!(
                    "Tarantool error:",
                    " NoSuchSpace:",
                    " Space '{}' does not exist",
                ),
                storage.space_raft_state.id()
            )
        );
    }
});
