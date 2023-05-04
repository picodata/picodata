use crate::storage::{Clusterwide, TClusterwideSpace as _};
use crate::storage::{ClusterwideSpace, Indexes, Spaces};
use crate::tlog;
use crate::traft::error::Error as TraftError;
use crate::traft::node;
use crate::traft::op::{Ddl, Dml, Op};
use crate::traft::Result;
use crate::traft::{EntryContext, EntryContextNormal};
use crate::traft::{RaftIndex, RaftTerm};

use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::StorageError;

use tarantool::error::Error as TntError;
use tarantool::tlua;
use tarantool::tuple::{KeyDef, Tuple, TupleBuffer};

use once_cell::sync::Lazy;

const PROHIBITED_SPACES: &[&str] = &[Spaces::SPACE_NAME, Indexes::SPACE_NAME];

crate::define_rpc_request! {
    fn proc_cas(req: Request) -> Result<Response> {
        let node = node::global()?;
        let raft_storage = &node.raft_storage;
        let storage = &node.storage;
        let cluster_id = raft_storage
            .cluster_id()?
            .expect("cluster_id is set on boot");

        if req.cluster_id != cluster_id {
            return Err(TraftError::ClusterIdMismatch {
                instance_cluster_id: req.cluster_id,
                cluster_cluster_id: cluster_id,
            });
        }

        let Predicate { index: requested, term: requested_term, .. } = req.predicate;
        // N.B. lock the mutex before getting status
        let mut node_impl = node.node_impl();
        let status = node.status();

        let check_term = |requested_term| -> Result<()> {
            if requested_term != status.term {
                Err(TraftError::TermMismatch {
                    requested: requested_term,
                    current: status.term,
                })
            } else {
                Ok(())
            }
        };

        check_term(requested_term)?;
        if status.leader_id != Some(node.raft_id()) {
            // Nearly impossible error indicating invalid request.
            return Err(TraftError::NotALeader);
        }

        let raft_log = &node_impl.raw_node.raft.raft_log;

        let first = raft_log.first_index();
        let last = raft_log.last_index();
        assert!(first >= 1);
        if requested > last {
            return Err(Error::NoSuchIndex { requested, last_index: last }.into());
        } else if (requested + 1) < first {
            return Err(Error::Compacted{ requested, compacted_index: first - 1 }.into());
        }

        assert!(requested >= first - 1);
        assert!(requested <= last);
        assert_eq!(requested_term, status.term);

        // Also check that requested index actually belongs to the
        // requested term.
        check_term(raft_log.term(requested).unwrap_or(0))?;

        let last_persisted = raft::Storage::last_index(raft_storage)?;
        assert!(last_persisted <= last);

        // Check if ranges in predicate contain prohibited spaces.
        for range in &req.predicate.ranges {
            if PROHIBITED_SPACES.contains(&range.space.as_str())
            {
                return Err(Error::SpaceNotAllowed { space: range.space.clone() }.into())
            }
        }

        // It's tempting to just use `raft_log.entries()` here and only
        // write the body of the loop once, but this would mean
        // converting entries from our storage representation to raft-rs
        // and than back again for all the persisted entries, which we
        // obviously don't want to do, so we instead write the body of
        // the loop twice

        // General case:
        //                              ,- last_persisted
        //                  ,- first    |             ,- last
        // entries: - - - - x x x x x x x x x x x x x x
        //                | [ persisted ] [ unstable  ]
        //                | [ checked                 ]
        //      requested ^
        //

        // Corner case 1:
        //                ,- last_persisted
        //                | ,- first    ,- last
        // entries: - - - - x x x x x x x
        //                  [ unstable  ]
        //

        if requested < last_persisted { // there's at least one persisted entry to check
            let persisted = raft_storage.entries(requested + 1, last_persisted + 1)?;
            if persisted.len() < (last_persisted - requested) as usize {
                return Err(RaftError::Store(StorageError::Unavailable).into());
            }

            for entry in persisted {
                assert_eq!(entry.term, status.term);
                let entry_index = entry.index;
                let Some(op) = entry.into_op() else { continue };
                req.predicate.check_entry(entry_index, &op, storage)?;
            }
        }

        // Check remaining unstable entries.
        let unstable = raft_log.entries(last_persisted + 1, u64::MAX)?;
        for entry in unstable {
            assert_eq!(entry.term, status.term);
            let Ok(cx) = EntryContext::from_raft_entry(&entry) else {
                tlog!(Warning, "raft entry has invalid context"; "entry" => ?entry);
                continue;
            };
            let Some(EntryContext::Normal(EntryContextNormal { op, .. })) = cx else {
                continue;
            };
            req.predicate.check_entry(entry.index, &op, storage)?;
        }

        // TODO: apply to limbo first

        // Don't wait for the proposal to be accepted, instead return the index
        // to the requestor, so that they can wait for it.

        let notify = node_impl.propose_async(req.op)?;
        let raft_log = &node_impl.raw_node.raft.raft_log;
        let index = raft_log.last_index();
        let term = raft_log.term(index).unwrap();
        assert_eq!(index, last + 1);
        assert_eq!(term, requested_term);
        drop(node_impl); // unlock the mutex
        drop(notify); // don't wait for commit

        Ok(Response { index, term })
    }

    pub struct Request {
        pub cluster_id: String,
        pub predicate: Predicate,
        pub op: Op,
    }

    pub struct Response {
        pub index: RaftIndex,
        pub term: RaftTerm,
    }
}

/// Error kinds specific to Compare and Swap algorithm.
///
/// Usually it can't be handled and should be either retried from
/// scratch or returned to a user as is.
///
#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    /// Can't check the predicate because raft log is compacted.
    #[error("raft index {requested} is compacted at {compacted_index}")]
    Compacted {
        requested: RaftIndex,
        compacted_index: RaftIndex,
    },

    /// Nearly impossible error indicating invalid request.
    #[error("raft entry at index {requested} does not exist yet, the last is {last_index}")]
    NoSuchIndex {
        requested: RaftIndex,
        last_index: RaftIndex,
    },

    /// Checking the predicate revealed a collision.
    #[error("comparison failed for index {requested} as it conflicts with {conflict_index}")]
    ConflictFound {
        requested: RaftIndex,
        conflict_index: RaftIndex,
    },

    #[error("space {space} is prohibited for use in a predicate")]
    SpaceNotAllowed { space: String },

    /// An error related to `key_def` operation arised from tarantool
    /// depths while checking the predicate.
    #[error("failed comparing predicate ranges: {0}")]
    KeyTypeMismatch(#[from] TntError),
}

/// Predicate that will be checked by the leader, before accepting the proposed `op`.
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead)]
pub struct Predicate {
    /// CaS sender's current raft index.
    pub index: RaftIndex,
    /// CaS sender's current raft term.
    pub term: RaftTerm,
    /// Range that the CaS sender have read and expects it to be unmodified.
    pub ranges: Vec<Range>,
}

impl Predicate {
    /// Checks if `entry_op` changes anything within the ranges specified in the predicate.
    pub fn check_entry(
        &self,
        entry_index: RaftIndex,
        entry_op: &Op,
        storage: &Clusterwide,
    ) -> std::result::Result<(), Error> {
        let error = || Error::ConflictFound {
            requested: self.index,
            conflict_index: entry_index,
        };
        let check_bounds = |key_def: &KeyDef, key: &Tuple, range: &Range| {
            let min_satisfied = match &range.key_min {
                Bound::Included(bound) => key_def.compare_with_key(key, bound).is_ge(),
                Bound::Excluded(bound) => key_def.compare_with_key(key, bound).is_gt(),
                Bound::Unbounded => true,
            };
            // short-circuit
            if !min_satisfied {
                return Ok(());
            }
            let max_satisfied = match &range.key_max {
                Bound::Included(bound) => key_def.compare_with_key(key, bound).is_le(),
                Bound::Excluded(bound) => key_def.compare_with_key(key, bound).is_lt(),
                Bound::Unbounded => true,
            };
            // min_satisfied && max_satisfied
            if max_satisfied {
                // If entry found that modified a tuple in bounds - cancel CaS.
                Err(error())
            } else {
                Ok(())
            }
        };

        let ddl_keys: Lazy<Vec<Tuple>> = Lazy::new(|| {
            use crate::storage::PropertyName::*;

            [
                PendingSchemaChange.into(),
                PendingSchemaVersion.into(),
                CurrentSchemaVersion.into(),
            ]
            .into_iter()
            .map(|key: &str| Tuple::new(&(key,)))
            .collect::<tarantool::Result<_>>()
            .expect("keys should convert to tuple")
        });
        for range in &self.ranges {
            if modifies_operable(entry_op, &range.space, storage) {
                return Err(error());
            }
            let Some(space) = space(entry_op) else {
                continue
            };
            let index = space.primary_index();
            if space.as_str() != range.space {
                continue;
            }

            match entry_op {
                Op::Dml(Dml::Update { key, .. } | Dml::Delete { key, .. }) => {
                    let key = Tuple::new(key)?;
                    let key_def = index.key_def_for_key()?;
                    check_bounds(key_def, &key, range)?;
                }
                Op::Dml(Dml::Insert { tuple, .. } | Dml::Replace { tuple, .. }) => {
                    let tuple = Tuple::new(tuple)?;
                    let key_def = index.key_def()?;
                    check_bounds(key_def, &tuple, range)?;
                }
                Op::DdlPrepare { .. } | Op::DdlCommit | Op::DdlAbort => {
                    let key_def = index.key_def_for_key()?;
                    for key in ddl_keys.iter() {
                        check_bounds(key_def, key, range)?;
                    }
                }
                Op::PersistInstance(op) => {
                    let key = Tuple::new(&(&op.0.instance_id,))?;
                    let key_def = index.key_def_for_key()?;
                    check_bounds(key_def, &key, range)?;
                }
                Op::Nop => (),
            };
        }
        Ok(())
    }
}

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead)]
pub struct Range {
    pub space: String,
    pub key_min: Bound,
    pub key_max: Bound,
}

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum Bound {
    #[serde(with = "serde_bytes")]
    Included(TupleBuffer),
    #[serde(with = "serde_bytes")]
    Excluded(TupleBuffer),
    Unbounded,
}

/// Get space that the operation touches.
fn space(op: &Op) -> Option<ClusterwideSpace> {
    match op {
        Op::Dml(dml) => Some(dml.space()),
        Op::DdlPrepare { .. } | Op::DdlCommit | Op::DdlAbort => Some(ClusterwideSpace::Property),
        Op::PersistInstance(_) => Some(ClusterwideSpace::Instance),
        Op::Nop => None,
    }
}

/// Checks if the operation would by its semantics modify `operable` flag of the provided `space`.
fn modifies_operable(op: &Op, space: &str, storage: &Clusterwide) -> bool {
    let ddl_modifies = |ddl: &Ddl| match ddl {
        Ddl::CreateSpace { name, .. } => name == space,
        Ddl::DropSpace { id } => {
            if let Some(space_def) = storage
                .spaces
                .get(*id)
                .expect("deserialization should not fail")
            {
                space_def.name == space
            } else {
                false
            }
        }
        Ddl::CreateIndex { .. } => false,
        Ddl::DropIndex { .. } => false,
    };
    match op {
        Op::DdlPrepare { ddl, .. } => ddl_modifies(ddl),
        Op::DdlCommit | Op::DdlAbort => {
            if let Some(change) = storage
                .properties
                .pending_schema_change()
                .expect("conversion should not fail")
            {
                ddl_modifies(&change)
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Predicate tests based on the CaS Design Document.
mod tests {
    use serde::Serialize;
    use tarantool::tuple::ToTupleBuffer;

    use crate::schema::{Distribution, SpaceDef};
    use crate::storage::TClusterwideSpace as _;
    use crate::storage::{Clusterwide, Properties, PropertyName};
    use crate::traft::op::DdlBuilder;

    use super::*;

    #[::tarantool::test]
    fn ddl() {
        #[track_caller]
        fn test<T: Serialize + ?Sized>(
            op: &Op,
            space: &str,
            key: &T,
            storage: &Clusterwide,
        ) -> std::result::Result<(), Error> {
            let predicate = Predicate {
                index: 1,
                term: 1,
                ranges: vec![Range {
                    space: space.into(),
                    key_min: Bound::Included((key,).to_tuple_buffer().unwrap()),
                    key_max: Bound::Included((key,).to_tuple_buffer().unwrap()),
                }],
            };
            predicate.check_entry(2, op, storage)
        }
        let storage = Clusterwide::new().unwrap();

        let builder = DdlBuilder::with_schema_version(1);

        let space_name = "space1";
        let space_id = 1;
        let index_id = 1;

        let create_space = builder.create_space(
            space_id,
            space_name.into(),
            vec![],
            vec![],
            Distribution::Global,
        );
        let drop_space = builder.drop_space(space_id);
        let create_index = builder.create_index(space_id, index_id, vec![]);
        let drop_index = builder.drop_index(space_id, index_id);

        let commit = Op::DdlCommit;
        let abort = Op::DdlAbort;

        let all = [
            &create_space,
            &drop_space,
            &create_index,
            &drop_index,
            &commit,
            &abort,
        ];

        for op in all {
            assert!(test(
                op,
                Properties::SPACE_NAME,
                PropertyName::PendingSchemaChange.into(),
                &storage
            )
            .is_err());
            assert!(test(
                op,
                Properties::SPACE_NAME,
                PropertyName::PendingSchemaVersion.into(),
                &storage
            )
            .is_err());
            assert!(test(op, Properties::SPACE_NAME, "unrelated_key", &storage).is_ok());
            assert!(test(op, "unrelated_space", "unrelated_key", &storage).is_ok())
        }

        // `DropSpace` needs `SpaceDef` to get space name
        storage
            .spaces
            .insert(&SpaceDef {
                id: space_id,
                name: space_name.into(),
                operable: true,
                distribution: Distribution::Global,
                format: vec![],
                schema_version: 1,
            })
            .unwrap();
        for op in [&create_space, &drop_space] {
            assert!(test(op, space_name, "any_key", &storage).is_err());
        }

        for op in [&create_index, &drop_index] {
            assert!(test(op, space_name, "any_key", &storage).is_ok());
        }

        // Abort and Commit need a pending schema change to get space name
        let Op::DdlPrepare{ ddl: create_space_ddl, ..} = create_space.clone() else {
            unreachable!();
        };
        storage
            .properties
            .put(PropertyName::PendingSchemaChange, &create_space_ddl)
            .unwrap();
        for op in [&abort, &commit] {
            assert!(test(
                op,
                Properties::SPACE_NAME,
                PropertyName::CurrentSchemaVersion.into(),
                &storage
            )
            .is_err());
        }
    }

    #[::tarantool::test]
    fn dml() {
        #[derive(Debug)]
        enum TestOp {
            Insert,
            Replace,
            Update,
            Delete,
        }

        #[track_caller]
        fn test<T: Serialize>(
            op: &TestOp,
            range: &Range,
            test_cases: &[T],
            storage: &Clusterwide,
        ) -> Vec<bool> {
            let predicate = Predicate {
                index: 1,
                term: 1,
                ranges: vec![range.clone()],
            };
            test_cases
                .iter()
                .map(|case| (case,).to_tuple_buffer().unwrap())
                .map(|key| {
                    let space = ClusterwideSpace::Property;
                    match op {
                        TestOp::Insert => Dml::Insert { space, tuple: key },
                        TestOp::Replace => Dml::Replace { space, tuple: key },
                        TestOp::Update => Dml::Update {
                            space,
                            key,
                            ops: vec![],
                        },
                        TestOp::Delete => Dml::Delete { space, key },
                    }
                })
                .map(|op| predicate.check_entry(2, &Op::Dml(op), storage).is_err())
                .collect()
        }

        let storage = Clusterwide::new().unwrap();

        let test_cases = ["a", "b", "c", "d", "e"];
        let ops = &[
            TestOp::Insert,
            TestOp::Replace,
            TestOp::Update,
            TestOp::Delete,
        ];

        // For range ["b", "d"]
        // Test cases a,b,c,d,e
        // Error      0,1,1,1,0
        let range = Range {
            space: Properties::SPACE_NAME.into(),
            key_min: Bound::Included(("b",).to_tuple_buffer().unwrap()),
            key_max: Bound::Included(("d",).to_tuple_buffer().unwrap()),
        };
        for op in ops {
            let errors = test(op, &range, &test_cases, &storage);
            assert_eq!(errors, vec![false, true, true, true, false], "{op:?}");
        }

        // For range ("b", "d"]
        // Test cases a,b,c,d,e
        // Error      0,0,1,1,0
        let range = Range {
            space: Properties::SPACE_NAME.into(),
            key_min: Bound::Excluded(("b",).to_tuple_buffer().unwrap()),
            key_max: Bound::Included(("d",).to_tuple_buffer().unwrap()),
        };
        for op in ops {
            let errors = test(op, &range, &test_cases, &storage);
            assert_eq!(errors, vec![false, false, true, true, false], "{op:?}");
        }

        // For range ["b", "d")
        // Test cases a,b,c,d,e
        // Error      0,1,1,0,0
        let range = Range {
            space: Properties::SPACE_NAME.into(),
            key_min: Bound::Included(("b",).to_tuple_buffer().unwrap()),
            key_max: Bound::Excluded(("d",).to_tuple_buffer().unwrap()),
        };
        for op in ops {
            let errors = test(op, &range, &test_cases, &storage);
            assert_eq!(errors, vec![false, true, true, false, false], "{op:?}");
        }

        // For range _, "d"]
        // Test cases a,b,c,d,e
        // Error      1,1,1,1,0
        let range = Range {
            space: Properties::SPACE_NAME.into(),
            key_min: Bound::Unbounded,
            key_max: Bound::Included(("d",).to_tuple_buffer().unwrap()),
        };
        for op in ops {
            let errors = test(op, &range, &test_cases, &storage);
            assert_eq!(errors, vec![true, true, true, true, false], "{op:?}");
        }

        // For range ["b", _
        // Test cases a,b,c,d,e
        // Error      0,1,1,1,1
        let range = Range {
            space: Properties::SPACE_NAME.into(),
            key_min: Bound::Included(("b",).to_tuple_buffer().unwrap()),
            key_max: Bound::Unbounded,
        };
        for op in ops {
            let errors = test(op, &range, &test_cases, &storage);
            assert_eq!(errors, vec![false, true, true, true, true], "{op:?}");
        }

        // For range _, _
        // Test cases a,b,c,d,e
        // Error      1,1,1,1,1
        let range = Range {
            space: Properties::SPACE_NAME.into(),
            key_min: Bound::Unbounded,
            key_max: Bound::Unbounded,
        };
        for op in ops {
            let errors = test(op, &range, &test_cases, &storage);
            assert_eq!(errors, vec![true, true, true, true, true], "{op:?}");
        }

        // Different space
        // For range _, _
        // Test cases a
        // Error      0
        let range = Range {
            space: Spaces::SPACE_NAME.into(),
            key_min: Bound::Unbounded,
            key_max: Bound::Unbounded,
        };
        for op in ops {
            let errors = test(op, &range, &[1], &storage);
            assert_eq!(errors, vec![false], "{op:?}");
        }
    }
}
