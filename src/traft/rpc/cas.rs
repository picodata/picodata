use crate::storage::ClusterwideSpace;
use crate::tlog;
use crate::traft::error::Error as TraftError;
use crate::traft::node;
use crate::traft::op::{Dml, Op};
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

crate::define_rpc_request! {
    fn proc_cas(req: Request) -> Result<Response> {
        let node = node::global()?;
        let raft_storage = &node.raft_storage;
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
                req.predicate.check_entry(entry_index, &op)?;
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
            req.predicate.check_entry(entry.index, &op)?;
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
    Rejected {
        requested: RaftIndex,
        conflict_index: RaftIndex,
    },

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
    pub fn check_entry(
        &self,
        entry_index: RaftIndex,
        entry_op: &Op,
    ) -> std::result::Result<(), Error> {
        let error = || Error::Rejected {
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

mod tests {
    use tarantool::tuple::ToTupleBuffer;

    use crate::storage::{Clusterwide, Properties, PropertyName, TClusterwideSpace};

    use super::*;

    #[::tarantool::test]
    fn check_ddl_predicate() {
        Clusterwide::new().unwrap();

        let predicate = Predicate {
            index: 1,
            term: 1,
            ranges: vec![Range {
                space: Properties::SPACE_NAME.into(),
                key_min: Bound::Included(
                    (PropertyName::PendingSchemaChange,)
                        .to_tuple_buffer()
                        .unwrap(),
                ),
                key_max: Bound::Included(
                    (PropertyName::PendingSchemaChange,)
                        .to_tuple_buffer()
                        .unwrap(),
                ),
            }],
        };
        let err = predicate.check_entry(2, &Op::DdlCommit).unwrap_err();
        assert_eq!(
            &err.to_string(),
            "comparison failed for index 1 as it conflicts with 2"
        );

        Predicate {
            index: 1,
            term: 1,
            ranges: vec![],
        }
        .check_entry(2, &Op::DdlCommit)
        .unwrap();
    }

    #[::tarantool::test]
    fn check_bounds() {
        #[track_caller]
        fn test(range: Range, test_cases: &[&str]) -> Vec<bool> {
            let predicate = Predicate {
                index: 1,
                term: 1,
                ranges: vec![range],
            };
            test_cases
                .iter()
                .map(|&case| (String::from(case),).to_tuple_buffer().unwrap())
                .map(|key| {
                    predicate
                        .check_entry(
                            2,
                            &Op::Dml(Dml::Delete {
                                space: ClusterwideSpace::Property,
                                key,
                            }),
                        )
                        .is_err()
                })
                .collect()
        }

        Clusterwide::new().unwrap();

        let test_cases = ["a", "b", "c", "d", "e"];

        // For range ["b", "d"]
        // Test cases a,b,c,d,e
        // Error      0,1,1,1,0
        let range = Range {
            space: Properties::SPACE_NAME.into(),
            key_min: Bound::Included(("b",).to_tuple_buffer().unwrap()),
            key_max: Bound::Included(("d",).to_tuple_buffer().unwrap()),
        };
        let errors = test(range, &test_cases);
        assert_eq!(errors, vec![false, true, true, true, false]);

        // For range ("b", "d"]
        // Test cases a,b,c,d,e
        // Error      0,0,1,1,0
        let range = Range {
            space: Properties::SPACE_NAME.into(),
            key_min: Bound::Excluded(("b",).to_tuple_buffer().unwrap()),
            key_max: Bound::Included(("d",).to_tuple_buffer().unwrap()),
        };
        let errors = test(range, &test_cases);
        assert_eq!(errors, vec![false, false, true, true, false]);

        // For range ["b", "d")
        // Test cases a,b,c,d,e
        // Error      0,1,1,0,0
        let range = Range {
            space: Properties::SPACE_NAME.into(),
            key_min: Bound::Included(("b",).to_tuple_buffer().unwrap()),
            key_max: Bound::Excluded(("d",).to_tuple_buffer().unwrap()),
        };
        let errors = test(range, &test_cases);
        assert_eq!(errors, vec![false, true, true, false, false]);

        // For range _, "d"]
        // Test cases a,b,c,d,e
        // Error      1,1,1,1,0
        let range = Range {
            space: Properties::SPACE_NAME.into(),
            key_min: Bound::Unbounded,
            key_max: Bound::Included(("d",).to_tuple_buffer().unwrap()),
        };
        let errors = test(range, &test_cases);
        assert_eq!(errors, vec![true, true, true, true, false]);

        // For range ["b", _
        // Test cases a,b,c,d,e
        // Error      0,1,1,1,1
        let range = Range {
            space: Properties::SPACE_NAME.into(),
            key_min: Bound::Included(("b",).to_tuple_buffer().unwrap()),
            key_max: Bound::Unbounded,
        };
        let errors = test(range, &test_cases);
        assert_eq!(errors, vec![false, true, true, true, true]);
    }
}
