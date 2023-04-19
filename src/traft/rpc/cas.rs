use crate::op::{Dml, Op};
use crate::tlog;
use crate::traft::error::Error as TraftError;
use crate::traft::node;
use crate::traft::Result;
use crate::traft::{EntryContext, EntryContextNormal};
use crate::traft::{RaftIndex, RaftTerm};

use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::StorageError;

use tarantool::error::Error as TntError;
use tarantool::tlua;
use tarantool::tuple::{Tuple, TupleBuffer};

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
                let Some(Op::Dml(op)) = entry.into_op() else { continue };
                check_predicate_for_entry(&req.predicate, entry_index, &op)?;
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
            let Some(EntryContext::Normal(EntryContextNormal { op: Op::Dml(op), .. })) = cx else {
                continue;
            };
            check_predicate_for_entry(&req.predicate, entry.index, &op)?;
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
        pub op: Dml,
    }

    pub struct Response {
        pub index: RaftIndex,
        pub term: RaftTerm,
    }
}

pub fn check_predicate_for_entry(
    predicate: &Predicate,
    entry_index: RaftIndex,
    entry_op: &Dml,
) -> std::result::Result<(), Error> {
    for range in &predicate.ranges {
        let space = entry_op.space();
        let index = entry_op.index();
        if space.as_str() != range.space || index.index_name() != range.index {
            continue;
        }

        let error = || Error::Rejected {
            requested: predicate.index,
            conflict_index: entry_index,
        };

        match entry_op {
            Dml::Update { key, .. } | // prevent rustfmt from spoiling linewraps
            Dml::Delete { key, .. } => {
                let key = Tuple::new(key)?;
                let key_def = index.key_def_for_key()?;

                if key_def.compare_with_key(&key, &range.key_min).is_ge()
                && key_def.compare_with_key(&key, &range.key_max).is_le()
                {
                    return Err(error());
                }
            }
            Dml::Insert { tuple, .. } | // prevent rustfmt from spoiling linewraps
            Dml::Replace { tuple, .. } => {
                let tuple = Tuple::new(tuple)?;
                let key_def = index.key_def()?;

                if key_def.compare_with_key(&tuple, &range.key_min).is_ge()
                && key_def.compare_with_key(&tuple, &range.key_max).is_le()
                {
                    return Err(error());
                }
            },
        };
    }
    Ok(())
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

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead)]
pub struct Predicate {
    pub index: RaftIndex,
    pub term: RaftTerm,
    pub ranges: Vec<Range>,
}

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead)]
pub struct Range {
    pub space: String,
    pub index: String,
    #[serde(with = "serde_bytes")]
    pub key_min: TupleBuffer,
    #[serde(with = "serde_bytes")]
    pub key_max: TupleBuffer,
}
