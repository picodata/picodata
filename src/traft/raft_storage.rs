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
    const SPACE_RAFT_LOG: &'static str = "_picodata_raft_log";
    const SPACE_RAFT_STATE: &'static str = "_picodata_raft_state";
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
        pub(crate) fn term(&self) -> _<RaftTerm>;
        fn vote(&self) -> _<RaftId>;
        pub(crate) fn commit(&self) -> _<RaftIndex>;
        pub fn applied(&self) -> _<RaftIndex>;

        pub(crate) fn voters(&self) -> _<Vec<RaftId>>;
        pub(crate) fn learners(&self) -> _<Vec<RaftId>>;
        fn voters_outgoing(&self) -> _<Vec<RaftId>>;
        fn learners_next(&self) -> _<Vec<RaftId>>;
        fn auto_leave(&self) -> _<bool>;

        fn compacted_term(&self) -> _<RaftTerm>;
        fn compacted_index(&self) -> _<RaftIndex>;
    }

    pub fn conf_state(&self) -> tarantool::Result<raft::ConfState> {
        Ok(raft::ConfState {
            voters: self.voters()?.unwrap_or_default(),
            learners: self.learners()?.unwrap_or_default(),
            voters_outgoing: self.voters_outgoing()?.unwrap_or_default(),
            learners_next: self.learners_next()?.unwrap_or_default(),
            auto_leave: self.auto_leave()?.unwrap_or(false),
            ..Default::default()
        })
    }

    pub fn hard_state(&self) -> tarantool::Result<raft::HardState> {
        Ok(raft::HardState {
            term: self.term()?.unwrap_or(0),
            vote: self.vote()?.unwrap_or(0),
            commit: self.commit()?.unwrap_or(0),
            ..Default::default()
        })
    }

    /// Returns a slice of log entries in the range `[low, high)`.
    /// It also can be thought of as `[first_index, last_index+1)`.
    ///
    /// As distinct from [`raft::Storage::entries`], this function
    /// doesn't perform bound checks. In case some entries are missing,
    /// the returned vec would be shorter than expected.
    ///
    /// # Panics
    ///
    /// Panics if `high` < `low`.
    ///
    pub fn entries(&self, low: RaftIndex, high: RaftIndex) -> tarantool::Result<Vec<raft::Entry>> {
        let iter = self.space_raft_log.select(IteratorType::GE, &(low,))?;
        let mut ret = Vec::with_capacity((high - low) as usize);

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

        fn persist_compacted_term(&self, replace compacted_term: RaftTerm) -> _;
        fn persist_compacted_index(&self, replace compacted_index: RaftTerm) -> _;
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

    /// Trims raft log up to the given index (excluding the index
    /// itself).
    ///
    /// It also updates the `compacted_index` & `compacted_term`
    /// raft-state values, so it **should be invoked within a
    /// transaction**.
    ///
    /// Returns the number of entries deleted.
    ///
    pub fn compact_log(&self, up_to: RaftIndex) -> tarantool::Result<u64> {
        // IteratorType::LT means tuples are returned in descending order
        let mut iter = self.space_raft_log.select(IteratorType::LT, &(up_to,))?;

        let Some(tuple) = iter.next() else { return Ok(0) };

        let index = tuple
            .field::<RaftIndex>(Self::FIELD_ENTRY_INDEX)?
            .expect("index is non-nullable");
        let term = tuple
            .field::<RaftTerm>(Self::FIELD_ENTRY_TERM)?
            .expect("term is non-nullable");
        self.persist_compacted_index(index)?;
        self.persist_compacted_term(term)?;

        self.space_raft_log.delete(&(index,))?;
        let mut n_deleted = 1;

        for tuple in iter {
            let index = tuple
                .field::<RaftIndex>(Self::FIELD_ENTRY_INDEX)?
                .expect("index is non-nullable");
            if let Some(_) = self.space_raft_log.delete(&(index,))? {
                n_deleted += 1;
            }
        }

        Ok(n_deleted)
    }
}

impl raft::Storage for RaftSpaceAccess {
    fn initial_state(&self) -> Result<raft::RaftState, RaftError> {
        let hs = self.hard_state().cvt_err()?;
        let cs = self.conf_state().cvt_err()?;
        let ret = raft::RaftState::new(hs, cs);
        Ok(ret)
    }

    /// Returns a slice of log entries in the range `[low, high)`.
    ///
    /// As distinct from [`RaftSpaceAccess::entries`] this function
    /// returns either all entries or an error.
    ///
    /// Returns `Err(Compacted)` if `low <= compacted_index`. Notice
    /// that an entry for `compacted_index` doesn't exist.
    ///
    /// Returns `Err(Unavailable)` in case at least one entry in the
    /// range is missing (either `high > last_index` or it's really
    /// missing). Raft-rs will panic in this case, but it's fair.
    ///
    /// The result is handled in raft_log.rs:583 (`RaftLog::slice`).
    ///
    /// # Panics
    ///
    /// Panics if `high` < `low`.
    ///
    fn entries(
        &self,
        low: RaftIndex,
        high: RaftIndex,
        _max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<raft::Entry>, RaftError> {
        if low <= self.compacted_index().cvt_err()?.unwrap_or(0) {
            return Err(RaftError::Store(StorageError::Compacted));
        }

        let ret = self.entries(low, high).cvt_err()?;
        if ret.len() < (high - low) as usize {
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        Ok(ret)
    }

    /// Returns the term of entry `idx`.
    ///
    /// The valid range is `[compacted_index, last_index()]`, that's why
    /// we also persist `compacted_term`.
    ///
    /// For `idx < compacted_index` the functon returns `Err(Compacted)`.
    /// For `idx > last_index()` the functon returns `Err(Unavailable)`.
    ///
    fn term(&self, idx: RaftIndex) -> Result<RaftTerm, RaftError> {
        let compacted_index = self.compacted_index().cvt_err()?.unwrap_or(0);
        let compacted_term = self.compacted_term().cvt_err()?.unwrap_or(0);

        if idx == compacted_index {
            return Ok(compacted_term);
        } else if idx < compacted_index {
            // Returning `Err(Compacted)` is safe. It's handled in
            // raft_log.rs:134 (`RaftLog::term`)
            return Err(RaftError::Store(StorageError::Compacted));
        }

        let tuple = self.space_raft_log.get(&(idx,)).cvt_err()?;
        if let Some(tuple) = tuple {
            return Ok(tuple
                .field(Self::FIELD_ENTRY_TERM)
                .cvt_err()?
                .expect("term is non-nullable"));
        }

        // Returning `Err(Unavailable)` is safe. It's handled in
        // raft_log.rs:134 (`RaftLog::term`)
        Err(RaftError::Store(StorageError::Unavailable))
    }

    /// Returns `compacted_index` plus 1.
    ///
    /// The naming is actually misleading, as returned index might not
    /// exist. And that means `last_index < first_index` and indicates
    /// that the log was compacted. Raft-rs treats it the same way.
    ///
    /// Empty log is considered compacted at index 0 with term 0.
    ///
    fn first_index(&self) -> Result<RaftIndex, RaftError> {
        let compacted_index = self.compacted_index().cvt_err()?.unwrap_or(0);
        Ok(1 + compacted_index)
    }

    /// Returns the last index which term is available.
    ///
    /// And again, the naming is actually misleading. The entry itself
    /// might not exist if the whole log was compacted or is actually
    /// empty. That case also implies `last_index < first_index`.
    ///
    /// Empty log is considered compacted at index 0 with term 0.
    ///
    fn last_index(&self) -> Result<RaftIndex, RaftError> {
        let tuple = self.space_raft_log.primary_key().max(&()).cvt_err()?;

        if let Some(t) = tuple {
            Ok(t.field(Self::FIELD_ENTRY_INDEX)
                .cvt_err()?
                .expect("index is non-nullabe"))
        } else {
            Ok(self.compacted_index().cvt_err()?.unwrap_or(0))
        }
    }

    fn snapshot(&self, idx: RaftIndex) -> Result<raft::Snapshot, RaftError> {
        tlog!(Critical, "snapshot"; "request_index" => idx);
        unimplemented!();

        // Ok(Storage::snapshot()?)
    }
}

/// Convert errors from tarantool to raft-rs types.
/// The only purpose of this trait is to reduce boilerplate.
trait CvtErr {
    type T;
    fn cvt_err(self) -> Result<Self::T, StorageError>;
}

impl<T> CvtErr for Result<T, tarantool::error::Error> {
    type T = T;
    #[inline(always)]
    fn cvt_err(self) -> Result<T, StorageError> {
        self.map_err(|e| StorageError::Other(Box::new(e)))
    }
}

macro_rules! assert_err {
    ($expr:expr, $err:expr) => {
        assert_eq!($expr.map_err(|e| format!("{e}")), Err($err.into()))
    };
}

mod tests {
    use super::*;

    fn dummy_entry(index: RaftIndex, term: RaftTerm) -> raft::Entry {
        raft::Entry {
            term,
            index,
            ..Default::default()
        }
    }

    #[::tarantool::test]
    fn test_cloning() {
        // It's fine to create two access objects
        let s1 = RaftSpaceAccess::new().unwrap();
        let s2 = RaftSpaceAccess::new().unwrap();
        assert_eq!(s1.space_raft_log.id(), s2.space_raft_log.id());
        assert_eq!(s1.space_raft_state.id(), s2.space_raft_state.id());

        // Though it's better to clone one
        #[allow(clippy::redundant_clone)]
        let _s3 = s2.clone();
    }

    #[::tarantool::test]
    fn test_log() {
        use ::raft::Storage as S;

        // Part 1. No log, no snapshot.
        let storage = RaftSpaceAccess::new().unwrap();

        assert_eq!(S::first_index(&storage), Ok(1));
        assert_eq!(S::last_index(&storage), Ok(0));

        assert_eq!(S::term(&storage, 0), Ok(0));
        assert_err!(S::term(&storage, 1), "log unavailable");

        // Part 2. Whole log was compacted.
        storage.persist_compacted_term(9).unwrap();
        storage.persist_compacted_index(99).unwrap();

        assert_eq!(S::first_index(&storage), Ok(100));
        assert_eq!(S::last_index(&storage), Ok(99));

        assert_err!(S::term(&storage, 0), "log compacted");
        assert_eq!(S::term(&storage, 99), Ok(9));
        assert_err!(S::term(&storage, 100), "log unavailable");

        // Part 3. Add some new entries.
        let test_entries = vec![
            dummy_entry(100, 10),
            dummy_entry(101, 10),
            dummy_entry(102, 10),
        ];
        storage.persist_entries(&test_entries).unwrap();

        let (first, last) = (100, 102);
        assert_eq!(S::first_index(&storage), Ok(first));
        assert_eq!(S::last_index(&storage), Ok(last));

        assert_eq!(S::term(&storage, first - 1), Ok(9));
        assert_eq!(S::term(&storage, first), Ok(10));
        assert_eq!(S::term(&storage, last), Ok(10));
        assert_err!(S::term(&storage, last + 1), "log unavailable");

        assert_err!(
            S::entries(&storage, first - 1, last + 1, u64::MAX),
            "log compacted"
        );

        assert_err!(
            S::entries(&storage, first, last + 2, u64::MAX),
            "log unavailable"
        );

        // This is what raft-rs literally does, see raft_log.rs:388
        // (`RaftLog::entries`)
        assert_eq!(
            S::entries(&storage, first, last + 1, u64::MAX),
            Ok(test_entries.clone())
        );
    }

    #[::tarantool::test]
    fn test_damaged_storage() {
        use ::raft::Storage as S;

        let storage = RaftSpaceAccess::new().unwrap();
        let raft_log = storage.space_raft_log.clone();
        let term = 666;

        let (first, last) = (1, 1);
        raft_log.put(&(1337, first, term, "", ())).unwrap();
        assert_err!(
            S::entries(&storage, first, last + 1, u64::MAX),
            "unknown error Failed to decode tuple: unknown entry type (1337)"
        );

        raft_log.put(&(0, first, term, "", false)).unwrap();
        assert_err!(
            S::entries(&storage, first, last + 1, u64::MAX),
            concat!(
                "unknown error",
                " Failed to decode tuple:",
                " data did not match any variant",
                " of untagged enum EntryContext"
            )
        );

        raft_log.put(&(0, 1, term, "", ())).unwrap();
        // skip index 2
        raft_log.put(&(0, 3, term, "", ())).unwrap();
        assert_err!(S::entries(&storage, 1, 4, u64::MAX), "log unavailable");

        raft_log.primary_key().drop().unwrap();
        assert_err!(
            S::entries(&storage, first, last + 1, u64::MAX),
            concat!(
                "unknown error",
                " Tarantool error:",
                " NoSuchIndexID:",
                " No index #0 is defined in space '_picodata_raft_log'"
            )
        );

        raft_log.drop().unwrap();
        assert_err!(
            S::entries(&storage, first, last + 1, u64::MAX),
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

    #[::tarantool::test]
    fn test_log_compaction() {
        use ::raft::Storage as S;

        let storage = RaftSpaceAccess::new().unwrap();

        let (first, last) = (1, 10);
        for i in first..=last {
            storage.persist_entries(&vec![dummy_entry(i, i)]).unwrap();
        }
        let entries = |lo, hi| S::entries(&storage, lo, hi, u64::MAX);

        assert_eq!(S::first_index(&storage), Ok(first));
        assert_eq!(S::last_index(&storage), Ok(last));
        assert_eq!(entries(first, last + 1).unwrap().len(), 10);

        let first = 6;
        storage.compact_log(6).unwrap();
        assert_eq!(S::first_index(&storage), Ok(first));
        assert_eq!(S::last_index(&storage), Ok(last));
        assert_eq!(S::term(&storage, 5), Ok(5));
        assert_eq!(S::term(&storage, first), Ok(6));
        assert_err!(S::term(&storage, 4), "log compacted");
        assert_err!(entries(1, last + 1), "log compacted");
        assert_eq!(entries(first, last + 1).unwrap().len(), 5);

        let tuple = storage
            .space_raft_log
            .primary_key()
            .min(&())
            .unwrap()
            .unwrap();

        let first_entry = tuple.decode::<traft::Entry>().unwrap();
        assert_eq!(first_entry.index, 6);
    }

    #[::tarantool::test]
    fn test_hard_state() {
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

    #[::tarantool::test]
    fn test_conf_state() {
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

    #[::tarantool::test]
    fn test_other_state() {
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
                " Duplicate key exists in unique index \"pk\" in space \"_picodata_raft_state\"",
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
                " No index #0 is defined in space '_picodata_raft_state'"
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
}
