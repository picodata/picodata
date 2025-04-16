use crate::access_control;
use crate::config;
use crate::error_code::ErrorCode;
use crate::metrics;
use crate::proc_name;
use crate::static_ref;
use crate::storage;
use crate::storage::Catalog;
use crate::storage::Properties;
use crate::storage::SystemTable;
use crate::tlog;
use crate::traft;
use crate::traft::error::Error as TraftError;
use crate::traft::node;
use crate::traft::op::{Acl, Ddl, Dml, Op};
use crate::traft::EntryContext;
use crate::traft::Result;
use crate::traft::{RaftIndex, RaftTerm, ResRowCount};
use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::GetEntriesContext;
use ::raft::StorageError;
use tarantool::error::Error as TntError;
use tarantool::error::TarantoolErrorCode;
use tarantool::fiber;
use tarantool::fiber::r#async::timeout::IntoTimeout;
use tarantool::session::UserId;
use tarantool::space::{Space, SpaceId};
use tarantool::time::Instant;
use tarantool::tlua;
use tarantool::transaction;
use tarantool::tuple::{Decode, KeyDef, ToTupleBuffer, Tuple, TupleBuffer};

/// These configs are defined once at cluster bootstrap
/// and they cannot be modified afterwards
const PROHIBITED_CONFIGS: &[&str] = &[config::SHREDDING_PARAM_NAME];

/// These tables cannot be changed directly dy a [`Dml`] operation. They have
/// dedicated operation types (e.g. Ddl, Acl) because updating these tables
/// requires automatically updating corresponding local spaces.
const PROHIBITED_TABLES: &[SpaceId] = &[
    storage::PicoTable::TABLE_ID,
    storage::Indexes::TABLE_ID,
    storage::Users::TABLE_ID,
    storage::Privileges::TABLE_ID,
    storage::Routines::TABLE_ID,
];

pub fn check_table_operable(storage: &Catalog, space_id: SpaceId) -> traft::Result<()> {
    if let Some(table) = storage.pico_table.get(space_id)? {
        if !table.operable {
            tlog!(Warning, "Table is not operable; skipping DML operation");
            return Err(Error::TableNotOperable {
                table: table.name.clone(),
            }
            .into());
        }
    }

    Ok(())
}

pub fn check_dml_prohibited(storage: &Catalog, dml: &Dml) -> traft::Result<()> {
    if PROHIBITED_TABLES.contains(&dml.table_id()) {
        return Err(Error::TableNotAllowed {
            table: storage
                .pico_table
                .get(dml.table_id())?
                .expect("system tables exist")
                .name,
        }
        .into());
    }

    let check_config_prohibited = |config_name| {
        if PROHIBITED_CONFIGS.contains(&config_name) {
            Err(Error::ConfigNotAllowed {
                config: config_name.to_string(),
            })
        } else {
            Ok(())
        }
    };
    if dml.table_id() == storage::DbConfig::TABLE_ID {
        match dml {
            Dml::Insert { tuple, .. } => {
                let (key, _scope, _value) =
                    <(String, String, rmpv::Value)>::decode(tuple.as_ref())?;
                check_config_prohibited(&key)?
            }
            Dml::Replace { tuple, .. } => {
                let (key, _scope, _value) =
                    <(String, String, rmpv::Value)>::decode(tuple.as_ref())?;
                check_config_prohibited(&key)?
            }
            Dml::Update { key, .. } => check_config_prohibited(&String::decode(key.as_ref())?)?,
            Dml::Delete { key, .. } => check_config_prohibited(&String::decode(key.as_ref())?)?,
        };
    }

    Ok(())
}

pub fn check_acl_limits(storage: &Catalog, acl: &Acl) -> traft::Result<()> {
    if matches!(acl, Acl::CreateUser { .. } | Acl::CreateRole { .. }) {
        storage.users.check_user_limit()
    } else {
        Ok(())
    }
}

/// Performs a clusterwide compare and swap operation. Waits until the
/// resulting entry is applied locally.
///
/// May or may not redirect a request to the current raft leader via RPC.
///
/// E.g. it checks the `predicate` on leader and if no conflicting entries were found
/// appends the `op` to the raft log and returns its index and term.
///
/// # Errors
/// See [`Error`] for CaS-specific errors.
/// It can also return general picodata errors in cases of faulty network or storage.
#[inline(always)]
pub fn compare_and_swap_and_wait(request: &Request, deadline: Instant) -> traft::Result<CasResult> {
    let start = Instant::now_fiber();
    let result = compare_and_swap(request, true, false, deadline);
    if result.is_err() {
        metrics::record_cas_errors_total(&request.op);
    }

    let duration = Instant::now_fiber().duration_since(start).as_millis();
    metrics::observe_cas_ops_duration(duration as f64);
    metrics::record_cas_ops_total(&request.op);

    result
}

/// Performs a clusterwide compare and swap operation.
///
/// Does not redirect the request to a different instance, returns and error if
/// the current instance is not the raft leader.
///
/// Checks the `predicate` and if no conflicting entries were found
/// appends the `op` to the raft log and returns its index and term.
///
/// # Errors
/// See [`Error`] for CaS-specific errors.
/// It can also return general picodata errors in cases of faulty storage.
#[inline(always)]
pub fn compare_and_swap_local(request: &Request, deadline: Instant) -> traft::Result<CasResult> {
    let start = Instant::now_fiber();
    let result = compare_and_swap(request, true, true, deadline);
    if result.is_err() {
        metrics::record_cas_errors_total(&request.op);
    }

    let duration = Instant::now_fiber().duration_since(start).as_millis();
    metrics::observe_cas_ops_duration(duration as f64);
    metrics::record_cas_ops_total(&request.op);

    result
}

/// Performs a clusterwide compare and swap operation.
///
/// E.g. it checks the `predicate` on leader and if no conflicting entries were found
/// appends the `op` to the raft log and returns its index and term.
///
/// # Errors
/// See [`Error`] for CaS-specific errors.
/// It can also return general picodata errors in cases of faulty network or storage.
pub fn compare_and_swap(
    request: &Request,
    wait_index: bool,
    force_local: bool,
    deadline: Instant,
) -> traft::Result<CasResult> {
    let node = node::global()?;

    if let Op::BatchDml { ops } = &request.op {
        if ops.is_empty() {
            return Err(Error::EmptyBatch.into());
        }
    }

    let Some(leader_id) = node.status().leader_id else {
        return Err(TraftError::LeaderUnknown);
    };

    let i_am_leader = leader_id == node.raft_id;

    let res;
    if i_am_leader {
        // cas has to be called locally in cases when listen ports are closed,
        // for example on shutdown
        res = proc_cas_v2_local(request);
    } else if force_local {
        return Err(TraftError::NotALeader);
    } else {
        let future = async {
            let timeout = deadline.duration_since(fiber::clock());
            node.pool
                .call(&leader_id, proc_name!(proc_cas_v2), request, timeout)?
                .timeout(timeout)
                .await
                .map_err(Into::into)
        };
        res = fiber::block_on(future);
    }

    let response = crate::unwrap_ok_or!(res,
        Err(e) => {
            if e.is_retriable() {
                return Ok(CasResult::RetriableError(e));
            } else {
                return Err(e);
            }
        }
    );

    if wait_index {
        node.wait_index(response.index, deadline.duration_since(fiber::clock()))?;

        let actual_term = raft::Storage::term(&node.raft_storage, response.index)?;
        if response.term != actual_term {
            // Leader has changed and the entry got rolled back, ok to retry.
            return Ok(CasResult::RetriableError(TraftError::TermMismatch {
                requested: response.term,
                current: actual_term,
            }));
        }
    }

    return Ok(CasResult::Ok((
        response.index,
        response.term,
        response.res_row_count,
    )));
}

#[must_use = "You must decide if you're retrying the error or returning it to user"]
pub enum CasResult {
    Ok((RaftIndex, RaftTerm, ResRowCount)),
    RetriableError(TraftError),
}

impl CasResult {
    #[inline(always)]
    pub fn into_retriable_error(self) -> Option<TraftError> {
        match self {
            Self::RetriableError(e) => Some(e),
            Self::Ok(_) => None,
        }
    }

    #[inline(always)]
    pub fn is_retriable_error(&self) -> bool {
        matches!(self, Self::RetriableError { .. })
    }

    /// Converts the result into `std::result::Result` for your convenience if
    /// you want to return the retriable error to the user.
    #[inline(always)]
    pub fn no_retries(self) -> traft::Result<(RaftIndex, RaftTerm, ResRowCount)> {
        match self {
            Self::Ok(v) => Ok(v),
            Self::RetriableError(e) => Err(e),
        }
    }
}

crate::define_rpc_request! {
    /// Performs a clusterwide compare and swap operation.
    /// Should be called only on the raft leader.
    ///
    /// The leader checks the predicate and if no conflicting
    /// entries were found appends the `op` to the raft log and returns its
    /// index and term.
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving instance is not yet initialized
    /// 2. Storage failure
    /// 3. Cluster name mismatch
    /// 4. Request has an incorrect term - leader changed
    /// 5. Receiving instance is not a raft-leader
    /// 6. [Compare and swap error](Error)
    #[deprecated(note = "Use `proc_cas_v2` instead")]
    fn proc_cas(req: RequestDeprecated) -> Result<ResponseDeprecated> {
        let res = proc_cas_v2_local(&Request{
            cluster_name: req.0.cluster_name,
            predicate: req.0.predicate,
            op: req.0.op,
            as_user: req.0.as_user,
        })?;
        Ok(ResponseDeprecated{
            index: res.index,
            term: res.term,
        })
    }

    pub struct RequestDeprecated(pub Request);

    pub struct ResponseDeprecated {
        pub index: RaftIndex,
        pub term: RaftTerm,
    }
}

fn proc_cas_v2_local(req: &Request) -> Result<Response> {
    let node = node::global()?;
    let raft_storage = &node.raft_storage;
    let storage = &node.storage;
    let cluster_name = raft_storage.cluster_name()?;

    if req.cluster_name != cluster_name {
        return Err(TraftError::ClusterNameMismatch {
            instance_cluster_name: req.cluster_name.clone(),
            cluster_name,
        });
    }

    let Predicate {
        index: requested,
        term: requested_term,
        ..
    } = req.predicate;
    // N.B. lock the mutex before getting status
    let mut node_impl = node.node_impl();
    let status = node.status();

    if requested_term != status.term {
        return Err(TraftError::TermMismatch {
            requested: requested_term,
            current: status.term,
        });
    }
    if status.leader_id != Some(node.raft_id()) {
        // Invalid request. This node is not a leader at this moment.
        return Err(TraftError::NotALeader);
    }

    let raft_log = &node_impl.raw_node.raft.raft_log;

    let first = raft_log.first_index();
    let last = raft_log.last_index();
    assert!(first >= 1);
    if requested > last {
        return Err(Error::NoSuchIndex {
            requested,
            last_index: last,
        }
        .into());
    } else if (requested + 1) < first {
        return Err(Error::Compacted {
            requested,
            compacted_index: first - 1,
        }
        .into());
    }

    assert!(requested >= first - 1);
    assert!(requested <= last);
    assert_eq!(requested_term, status.term);

    // Also check that requested index actually belongs to the
    // requested term.
    let entry_term = raft_log.term(requested).unwrap_or(0);
    if entry_term != status.term {
        return Err(Error::EntryTermMismatch {
            index: requested,
            expected_term: status.term,
            actual_term: entry_term,
        }
        .into());
    }

    // Check that sender is allowed to apply this operation.
    // Executed as one of the first checks to prevent spending time on
    // expensive range checks if the sender has no permissions for this operation.
    //
    // Note: audit log record is automatically emitted in case there is an error,
    // because it is hooked into AccessDenied error creation (on_access_denied) trigger
    access_control::access_check_op(storage, &req.op, req.as_user)?;

    let last_persisted = raft::Storage::last_index(raft_storage)?;
    assert!(last_persisted <= last);

    match &req.op {
        Op::Dml(dml) => {
            check_table_operable(storage, dml.space())?;
            check_dml_prohibited(storage, dml)?;
        }
        Op::BatchDml { ops: dmls } => {
            for dml in dmls {
                check_table_operable(storage, dml.space())?;
                check_dml_prohibited(storage, dml)?;
            }
        }
        Op::Acl(acl) => {
            check_acl_limits(storage, acl)?;
        }
        _ => {}
    }

    let mut ranges = req.predicate.ranges.clone();
    let implicit_ranges = Range::for_op(&req.op)?;
    ranges.extend_from_slice(&implicit_ranges);

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

    if requested < last_persisted {
        // there's at least one persisted entry to check
        let persisted = raft_storage.entries(requested + 1, last_persisted + 1, None)?;
        if persisted.len() < (last_persisted - requested) as usize {
            return Err(RaftError::Store(StorageError::Unavailable).into());
        }

        for entry in persisted {
            assert_eq!(entry.term, status.term);
            let entry_index = entry.index;
            let Some(op) = entry.into_op() else { continue };
            check_predicate(entry_index, &op, &ranges, storage)?;
        }
    }

    // Check remaining unstable entries.
    let unstable = raft_log.entries(
        last_persisted + 1,
        u64::MAX,
        GetEntriesContext::empty(false),
    )?;
    for entry in unstable {
        assert_eq!(entry.term, status.term);
        let Ok(cx) = EntryContext::from_raft_entry(&entry) else {
            tlog!(Warning, "raft entry has invalid context"; "entry" => ?entry);
            continue;
        };
        let EntryContext::Op(op) = cx else {
            continue;
        };
        check_predicate(entry.index, &op, &ranges, storage)?;
    }

    let mut res_row_count = 0;

    // Performs preliminary checks on an operation so that it will not fail when applied.
    match &req.op {
        Op::Dml(dml) => {
            // Check if the requested dml is applicable to the local storage.
            // This will run the required on_replace triggers which will check among
            // other things conformity to space format, user defined constraints etc.
            //
            // FIXME: this works for explicit constraints which would be directly
            // violated by the operation, but there are still some cases where an
            // invalid dml operation would be proposed to the raft log, which would
            // fail to apply, e.g. if there's a number of dml operations in flight
            // which conflict via the secondary key.
            // To fix these cases we would need to implement the so-called "limbo".
            // See https://git.picodata.io/picodata/picodata/picodata/-/issues/368
            transaction::begin()?;
            let res = storage.do_dml(dml);
            transaction::rollback().expect("can't fail");
            // Return the error if it happened.
            // Increment res_row_count if there was tuple.
            if res?.is_some() {
                res_row_count += 1;
            }
        }
        Op::BatchDml { ops, .. } => {
            let mut res = Ok(None);
            transaction::begin()?;
            for op in ops {
                res = storage.do_dml(op);
                match res {
                    Ok(Some(_)) => res_row_count += 1,
                    Err(_) => break,
                    _ => {}
                }
            }
            transaction::rollback().expect("can't fail");
            _ = res?;
        }
        // We can't use a `do_acl` inside of a transaction here similarly to `do_dml`,
        // as acl should be applied only on replicaset leader. Raft leader is not always
        // a replicaset leader.
        Op::Acl(acl) => acl.validate(storage)?,
        _ => (),
    }

    // Don't wait for the proposal to be accepted, instead return the index
    // to the requestor, so that they can wait for it.

    let entry_id = node_impl.propose_async(req.op.clone())?;
    let index = entry_id.index;
    let term = entry_id.term;
    // TODO: return number of raft entries and check this
    // assert_eq!(index, last + 1);
    assert_eq!(term, requested_term);
    drop(node_impl); // unlock the mutex

    // Tell raft_main_loop that we're expecting it to handle our request ASAP.
    // This is important, because otherwise we would stall for ~100ms and RPS
    // would drop to 10.
    node.main_loop.wakeup();

    Ok(Response {
        index,
        term,
        res_row_count,
    })
}

crate::define_rpc_request! {
    /// Performs a clusterwide compare and swap operation.
    /// Should be called only on the raft leader.
    ///
    /// The leader checks the predicate and if no conflicting
    /// entries were found appends the `op` to the raft log and returns its
    /// index, term and the number of INSERT operations applied in accordance with the corresponding ON REPLACE strategy (see `do_dml` in storage::Catalog).
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving instance is not yet initialized
    /// 2. Storage failure
    /// 3. Cluster name mismatch
    /// 4. Request has an incorrect term - leader changed
    /// 5. Receiving instance is not a raft-leader
    /// 6. [Compare and swap error](Error)
    // TODO Result<Either<Response, Error>>
    fn proc_cas_v2(req: Request) -> Result<Response> {
        proc_cas_v2_local(&req)
    }

    pub struct Request {
        pub cluster_name: String,
        pub predicate: Predicate,
        pub op: Op,
        pub as_user: UserId,
    }

    pub struct Response {
        pub index: RaftIndex,
        pub term: RaftTerm,
        pub res_row_count: ResRowCount,
    }
}

impl Request {
    #[inline(always)]
    pub fn new(op: impl Into<Op>, predicate: Predicate, as_user: UserId) -> traft::Result<Self> {
        let node = node::global()?;
        Ok(Request {
            cluster_name: node.raft_storage.cluster_name()?,
            predicate,
            op: op.into(),
            as_user,
        })
    }
}

/// Error kinds specific to Compare and Swap algorithm.
///
/// Usually it can't be handled and should be either retried from
/// scratch or returned to a user as is.
#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    /// Can't check the predicate because raft log is compacted.
    #[error("Compacted: raft index {requested} is compacted at {compacted_index}")]
    Compacted {
        requested: RaftIndex,
        compacted_index: RaftIndex,
    },

    /// Nearly impossible error indicating invalid request.
    #[error(
        "NoSuchIndex: raft entry at index {requested} does not exist yet, the last is {last_index}"
    )]
    NoSuchIndex {
        requested: RaftIndex,
        last_index: RaftIndex,
    },

    /// Checking the predicate revealed a collision.
    #[error("ConflictFound: found a conflicting entry at index {conflict_index}")]
    ConflictFound { conflict_index: RaftIndex },

    /// Checking the predicate revealed a collision.
    #[error("EntryTermMismatch: entry at index {index} has term {actual_term}, request implies term {expected_term}")]
    EntryTermMismatch {
        index: RaftIndex,
        expected_term: RaftTerm,
        actual_term: RaftTerm,
    },

    #[error("TableNotAllowed: table {table} cannot be modified by DML Raft Operation directly")]
    TableNotAllowed { table: String },

    #[error("ConfigNotAllowed: config {config} cannot be modified")]
    ConfigNotAllowed { config: String },

    #[error(
        "TableNotOperable: table {table} cannot be modified now as DDL operation is in progress"
    )]
    TableNotOperable { table: String },

    /// An error related to `key_def` operation arised from tarantool
    /// depths while checking the predicate.
    #[error("KeyTypeMismatch: failed comparing predicate ranges: {0}")]
    KeyTypeMismatch(#[from] TntError),

    #[error("InvalidOp: empty batch")]
    EmptyBatch,
}

impl Error {
    #[inline]
    pub fn error_code(&self) -> u32 {
        match self {
            Self::Compacted { .. } => ErrorCode::RaftLogCompacted as _,
            Self::NoSuchIndex { .. } => ErrorCode::CasNoSuchRaftIndex as _,
            Self::ConflictFound { .. } => ErrorCode::CasConflictFound as _,
            Self::EntryTermMismatch { .. } => ErrorCode::CasEntryTermMismatch as _,
            Self::TableNotAllowed { .. } => ErrorCode::CasTableNotAllowed as _,
            Self::ConfigNotAllowed { .. } => ErrorCode::CasConfigNotAllowed as _,
            Self::TableNotOperable { .. } => ErrorCode::CasTableNotOperable as _,
            Self::KeyTypeMismatch { .. } => ErrorCode::StorageCorrupted as _,
            Self::EmptyBatch => TarantoolErrorCode::IllegalParams as _,
        }
    }

    #[allow(non_snake_case)]
    #[inline(always)]
    pub fn ConflictFound(conflict_index: RaftIndex) -> Self {
        Self::ConflictFound { conflict_index }
    }
}

/// Represents a lua table describing a [`Predicate`].
///
/// This is only used to parse lua arguments from lua api functions such as
/// `pico.cas`.
#[derive(Clone, Debug, Default, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead)]
pub struct PredicateInLua {
    /// CaS sender's current raft index.
    pub index: Option<RaftIndex>,
    /// CaS sender's current raft term.
    pub term: Option<RaftTerm>,
    /// Range that the CaS sender have read and expects it to be unmodified.
    pub ranges: Option<Vec<RangeInLua>>,
}

/// Predicate that will be checked by the leader, before accepting the proposed `op`.
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct Predicate {
    /// CaS sender's current raft index.
    pub index: RaftIndex,
    /// CaS sender's current raft term.
    pub term: RaftTerm,
    /// Range that the CaS sender have read and expects it to be unmodified.
    pub ranges: Vec<Range>,
}

impl Predicate {
    /// Constructs a predicate consisting of:
    /// - current raft term
    /// - provided `index`
    /// - provided `ranges`
    #[inline]
    pub fn new(index: RaftIndex, ranges: impl Into<Vec<Range>>) -> Self {
        let node = traft::node::global().expect("shouldn't be called before node is initialized");

        Self {
            index,
            term: node.status().term,
            ranges: ranges.into(),
        }
    }

    /// Constructs a predicate consisting of:
    /// - current raft term
    /// - current applied index
    /// - provided `ranges`
    #[inline]
    pub fn with_applied_index(ranges: impl Into<Vec<Range>>) -> Self {
        let node = traft::node::global().expect("shouldn't be called before node is initialized");

        Self {
            index: node.get_index(),
            term: node.status().term,
            ranges: ranges.into(),
        }
    }

    pub fn from_lua_args(predicate: PredicateInLua) -> traft::Result<Self> {
        let node = traft::node::global()?;
        let (index, term) = if let Some(index) = predicate.index {
            if let Some(term) = predicate.term {
                (index, term)
            } else {
                let term = node.status().term;
                (index, term)
            }
        } else {
            let index = node.get_index();
            let term = node.status().term;
            (index, term)
        };
        Ok(Self {
            index,
            term,
            ranges: predicate
                .ranges
                .unwrap_or_default()
                .into_iter()
                .map(Range::from_lua_args)
                .collect::<traft::Result<_>>()?,
        })
    }
}

/// Checks if `entry_op` changes anything within the `predicate_ranges`.
///
/// `entry_index` is only used to report error `ConflictFound`.
pub fn check_predicate(
    entry_index: RaftIndex,
    entry_op: &Op,
    predicate_ranges: &[Range],
    storage: &Catalog,
) -> std::result::Result<(), Error> {
    let check_dml = |op: &Dml, space_id: u32, range: &Range| -> std::result::Result<(), Error> {
        match op {
            Dml::Update { key, .. } | Dml::Delete { key, .. } => {
                let key = Tuple::new(key)?;
                let key_def = storage::cached_key_def_for_key(space_id, 0)?;
                if range.contains(&key_def, &key) {
                    return Err(Error::ConflictFound(entry_index));
                }
            }
            Dml::Insert { tuple, .. } | Dml::Replace { tuple, .. } => {
                let tuple = Tuple::new(tuple)?;
                let key_def = storage::cached_key_def(space_id, 0)?;
                if range.contains(&key_def, &tuple) {
                    return Err(Error::ConflictFound(entry_index));
                }
            }
        }
        Ok(())
    };
    for range in predicate_ranges {
        if modifies_operable(entry_op, range.table, storage) {
            return Err(Error::ConflictFound(entry_index));
        }

        // TODO: check operation's space exists
        match entry_op {
            Op::BatchDml { ops } => {
                // TODO: remove O(n*n) complexity,
                // use a hashtable for dml/range lookup?
                for dml in ops {
                    if dml.space() == range.table {
                        check_dml(dml, dml.space(), range)?;
                    }
                }
            }
            Op::Dml(dml) => {
                if dml.space() == range.table {
                    check_dml(dml, dml.space(), range)?
                }
            }
            Op::DdlPrepare { .. } | Op::DdlCommit | Op::DdlAbort { .. } | Op::Acl { .. } => {
                let space = storage::Properties::TABLE_ID;
                if space != range.table {
                    continue;
                }
                let key_def = storage::cached_key_def_for_key(space, 0)?;
                for key in schema_related_property_keys() {
                    // NOTE: this is just a string comparison
                    if range.contains(&key_def, key) {
                        return Err(Error::ConflictFound(entry_index));
                    }
                }
            }
            Op::Plugin { .. } => {
                let space = storage::Properties::TABLE_ID;
                if space != range.table {
                    continue;
                }
                let key_def = storage::cached_key_def_for_key(space, 0)?;
                let key = pending_plugin_operation_key();
                // NOTE: this is just a string comparison
                if range.contains(&key_def, key) {
                    return Err(Error::ConflictFound(entry_index));
                }
            }
            Op::Nop => (),
        };
    }
    Ok(())
}

const SCHEMA_RELATED_PROPERTIES: &'static [&str] = &[
    storage::PropertyName::PendingSchemaChange.as_str(),
    storage::PropertyName::PendingSchemaVersion.as_str(),
    storage::PropertyName::GlobalSchemaVersion.as_str(),
    storage::PropertyName::NextSchemaVersion.as_str(),
    // Governor operations can be DDL operations.
    storage::PropertyName::PendingGovernorOpId.as_str(),
    storage::PropertyName::PendingCatalogVersion.as_str(),
];

/// Returns a slice of tuples representing keys of space _pico_property which
/// should be used to check predicates of schema changing CaS operations.
fn schema_related_property_keys() -> &'static [Tuple] {
    static mut DATA: Option<Vec<Tuple>> = None;

    // SAFETY:
    // - only called from main thread
    // - never mutated after initialization
    unsafe {
        if static_ref!(const DATA).is_none() {
            let mut data = Vec::with_capacity(SCHEMA_RELATED_PROPERTIES.len());
            for key in SCHEMA_RELATED_PROPERTIES {
                let t = Tuple::new(&(key,)).expect("keys should convert to tuple");
                data.push(t);
            }
            DATA = Some(data);
        }

        static_ref!(const DATA).as_ref().unwrap()
    }
}

fn pending_plugin_operation_key() -> &'static Tuple {
    static mut TUPLE: Option<Tuple> = None;

    // SAFETY:
    // - only called from main thread
    // - never mutated after initialization
    unsafe {
        if static_ref!(const TUPLE).is_none() {
            let tuple =
                Tuple::new(&[storage::PropertyName::PendingPluginOperation]).expect("cannot fail");
            TUPLE = Some(tuple);
        }

        static_ref!(const TUPLE).as_ref().unwrap()
    }
}

/// Returns a slice of [`Range`] structs which are needed for the CaS
/// request which performs a schema change operation.
pub fn schema_change_ranges() -> &'static [Range] {
    static mut DATA: Option<Vec<Range>> = None;

    // SAFETY:
    // - only called from main thread
    // - never mutated after initialization
    unsafe {
        if static_ref!(const DATA).is_none() {
            let mut data = Vec::with_capacity(SCHEMA_RELATED_PROPERTIES.len());
            for key in SCHEMA_RELATED_PROPERTIES {
                let r = Range::new(storage::Properties::TABLE_ID).eq((key,));
                data.push(r);
            }
            DATA = Some(data);
        }

        static_ref!(const DATA).as_ref().unwrap()
    }
}

/// Represents a lua table describing a [`Range`].
///
/// This is only used to parse lua arguments from lua api functions such as
/// `pico.cas`.
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead, PartialEq)]
pub struct RangeInLua {
    /// Table name.
    pub table: String,
    pub key_min: Bound,
    pub key_max: Bound,
}

/// A range of keys used as an argument for a [`Predicate`].
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Range {
    pub table: SpaceId,
    pub bounds: Option<RangeBounds>,
}

impl PartialEq for Range {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        // Explicitly check the table id first, so that the fast path is
        // actually fast. The derived implementation of `PartialEq` would
        // *probably* be the same, but rust doesn't give any guarantees,
        // so we have to do it explicitly...
        self.table == other.table && self.bounds == other.bounds
    }
}

impl Range {
    pub fn from_lua_args(range: RangeInLua) -> traft::Result<Self> {
        let node = traft::node::global()?;
        let table = if let Some(table) = node.storage.pico_table.by_name(&range.table)? {
            table.id
        } else if let Some(table) = Space::find(&range.table) {
            table.id()
        } else {
            return Err(TraftError::other(format!(
                "table '{}' not found",
                range.table
            )));
        };
        Ok(Self::from_parts(
            table,
            range.key_min.key,
            range.key_min.kind == BoundKind::Included,
            range.key_max.key,
            range.key_max.kind == BoundKind::Included,
        ))
    }

    /// Creates new unbounded range in `table`. Use other methods to restrict it.
    ///
    /// # Example
    /// ```
    /// use picodata::cas::Range;
    ///
    /// // Creates a range for tuples with keys from 1 (excluding) to 10 (excluding)
    /// let my_table_id: u32 = 2222;
    /// let range = Range::new(my_table_id).gt((1,)).lt((10,));
    /// ```
    #[inline(always)]
    pub fn new(table: impl Into<SpaceId>) -> Self {
        Self {
            table: table.into(),
            bounds: None,
        }
    }

    pub fn for_dml(dml: &Dml) -> Result<Self> {
        let table = dml.table_id();
        let key = match dml {
            Dml::Update { key, .. } | Dml::Delete { key, .. } => key.clone(),
            Dml::Insert { tuple, .. } | Dml::Replace { tuple, .. } => {
                let key_def = storage::cached_key_def(table, 0)?;
                let tuple = Tuple::new(tuple)?;
                key_def.extract_key(&tuple)?
            }
        };
        Ok(Self {
            table,
            bounds: Some(RangeBounds::Eq { key }),
        })
    }

    pub fn for_op(op: &Op) -> Result<Vec<Self>> {
        match op {
            Op::Nop => Ok(vec![]),
            Op::Dml(dml) => {
                let range = Self::for_dml(dml)?;
                Ok(vec![range])
            }
            Op::BatchDml { ops } => {
                // N.B.: if there's multiple operations on the same keys
                // of the same table there'll be duplicate predicates.
                // It may affect performance because predicate checking
                // takes O(N*M) checks (N being the length of the batch
                // and M the length of the raft log).
                ops.iter().map(Self::for_dml).collect()
            }
            Op::DdlPrepare { .. } | Op::DdlCommit | Op::DdlAbort { .. } | Op::Acl { .. } => {
                let range = Self::new(Properties::TABLE_ID)
                    .eq([storage::PropertyName::GlobalSchemaVersion]);
                Ok(vec![range])
            }
            Op::Plugin { .. } => {
                let range = Self::new(Properties::TABLE_ID)
                    .eq([storage::PropertyName::PendingPluginOperation]);
                Ok(vec![range])
            }
        }
    }

    #[inline(always)]
    pub fn from_parts(
        table: impl Into<SpaceId>,
        key_min: Option<TupleBuffer>,
        key_min_included: bool,
        key_max: Option<TupleBuffer>,
        key_max_included: bool,
    ) -> Self {
        let bounds = if key_min == key_max {
            key_min.map(|key| RangeBounds::Eq { key })
        } else {
            Some(RangeBounds::Range {
                key_min_included,
                key_min,
                key_max_included,
                key_max,
            })
        };
        Self {
            table: table.into(),
            bounds,
        }
    }

    /// Add a "greater than" restriction.
    #[inline(always)]
    pub fn gt(mut self, key: impl ToTupleBuffer) -> Self {
        let bounds = self.bounds.get_or_insert_with(RangeBounds::default);
        let key = key.to_tuple_buffer().expect("never fails");
        bounds.set_min(key, false);
        self
    }

    /// Add a "greater or equal" restriction.
    #[inline(always)]
    pub fn ge(mut self, key: impl ToTupleBuffer) -> Self {
        let bounds = self.bounds.get_or_insert_with(RangeBounds::default);
        let key = key.to_tuple_buffer().expect("never fails");
        bounds.set_min(key, true);
        self
    }

    /// Add a "less than" restriction.
    #[inline(always)]
    pub fn lt(mut self, key: impl ToTupleBuffer) -> Self {
        let bounds = self.bounds.get_or_insert_with(RangeBounds::default);
        let key = key.to_tuple_buffer().expect("never fails");
        bounds.set_max(key, false);
        self
    }

    /// Add a "less or equal" restriction.
    #[inline(always)]
    pub fn le(mut self, key: impl ToTupleBuffer) -> Self {
        let bounds = self.bounds.get_or_insert_with(RangeBounds::default);
        let key = key.to_tuple_buffer().expect("never fails");
        bounds.set_max(key, true);
        self
    }

    /// Add a "equal" restriction.
    #[inline(always)]
    pub fn eq(mut self, key: impl ToTupleBuffer) -> Self {
        let key = key.to_tuple_buffer().expect("never fails");
        self.bounds = Some(RangeBounds::Eq { key });
        self
    }

    pub fn contains(&self, key_def: &KeyDef, tuple: &Tuple) -> bool {
        let Some(bounds) = &self.bounds else {
            return true;
        };

        match bounds {
            RangeBounds::Eq { key } => {
                return key_def.compare_with_key(tuple, key).is_eq();
            }
            RangeBounds::Range {
                key_min_included,
                key_min,
                key_max_included,
                key_max,
            } => {
                if let Some(key_min) = key_min {
                    let cmp = key_def.compare_with_key(tuple, key_min);
                    if cmp.is_lt() || !key_min_included && cmp.is_eq() {
                        return false;
                    }
                }

                if let Some(key_max) = key_max {
                    let cmp = key_def.compare_with_key(tuple, key_max);
                    if cmp.is_gt() || !key_max_included && cmp.is_eq() {
                        return false;
                    }
                }

                return true;
            }
        }
    }
}

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead, PartialEq)]
#[serde(tag = "kind")]
#[serde(rename_all = "snake_case")]
pub enum RangeBounds {
    Eq {
        #[serde(with = "serde_bytes")]
        key: TupleBuffer,
    },
    Range {
        key_min_included: bool,
        #[serde(with = "serde_bytes")]
        key_min: Option<TupleBuffer>,
        key_max_included: bool,
        #[serde(with = "serde_bytes")]
        key_max: Option<TupleBuffer>,
    },
}

impl Default for RangeBounds {
    #[inline(always)]
    fn default() -> Self {
        Self::Range {
            key_min_included: false,
            key_min: None,
            key_max_included: false,
            key_max: None,
        }
    }
}

impl RangeBounds {
    #[inline(always)]
    fn set_min(&mut self, key: TupleBuffer, included: bool) {
        match self {
            Self::Eq { .. } => panic!("already set to Eq"),
            Self::Range {
                key_min,
                key_min_included,
                ..
            } => {
                *key_min = Some(key);
                *key_min_included = included;
            }
        }
    }

    #[inline(always)]
    fn set_max(&mut self, key: TupleBuffer, included: bool) {
        match self {
            Self::Eq { .. } => panic!("already set to Eq"),
            Self::Range {
                key_max,
                key_max_included,
                ..
            } => {
                *key_max = Some(key);
                *key_max_included = included;
            }
        }
    }
}

/// A bound for keys.
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead, PartialEq)]
pub struct Bound {
    kind: BoundKind,
    #[serde(with = "serde_bytes")]
    key: Option<TupleBuffer>,
}

::tarantool::define_str_enum! {
    /// A bound for keys.
    #[derive(Default)]
    pub enum BoundKind {
        Included = "included",
        Excluded = "excluded",
        #[default]
        Unbounded = "unbounded",
    }
}

/// Checks if the operation would by its semantics modify `operable` flag of the provided `space`.
fn modifies_operable(op: &Op, space: SpaceId, storage: &Catalog) -> bool {
    let ddl_modifies = |ddl: &Ddl| match ddl {
        Ddl::CreateTable { id, .. } => *id == space,
        Ddl::DropTable { id, .. } => *id == space,
        Ddl::TruncateTable { id, .. } => *id == space,
        Ddl::ChangeFormat { table_id, .. } => *table_id == space,
        Ddl::RenameTable { table_id, .. } => *table_id == space,
        Ddl::Backup { .. } => false,
        Ddl::CreateIndex { .. } => false,
        Ddl::DropIndex { .. } => false,
        Ddl::CreateProcedure { .. } => false,
        Ddl::DropProcedure { .. } => false,
        Ddl::RenameProcedure { .. } => false,
    };
    match op {
        Op::DdlPrepare { ddl, .. } => ddl_modifies(ddl),
        Op::DdlCommit | Op::DdlAbort { .. } => {
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
    use sbroad::ir::operator::ConflictStrategy;
    use storage::PicoTable;
    use tarantool::index::IndexType;
    use tarantool::space::SpaceEngineType;
    use tarantool::tuple::ToTupleBuffer;

    use crate::schema::{Distribution, IndexOption, TableDef, ADMIN_ID};
    use crate::storage::SystemTable as _;
    use crate::storage::{Catalog, Properties, PropertyName};
    use crate::traft::op::DdlBuilder;

    use super::*;

    #[::tarantool::test]
    fn ddl() {
        let storage = Catalog::for_tests();

        let t = |op: &Op, range: Range| -> std::result::Result<(), Error> {
            check_predicate(2, op, &[range], &storage)
        };

        let builder = DdlBuilder::with_schema_version(1);

        let table_def = TableDef::for_tests();
        let space_name = &table_def.name;
        let space_id = table_def.id;
        let index_id = 1;

        let create_space = builder.with_op(Ddl::CreateTable {
            id: space_id,
            name: space_name.into(),
            format: vec![],
            primary_key: vec![],
            distribution: Distribution::Global,
            engine: SpaceEngineType::Memtx,
            owner: ADMIN_ID,
        });
        let drop_space = builder.with_op(Ddl::DropTable {
            id: space_id,
            initiator: ADMIN_ID,
        });
        let create_index = builder.with_op(Ddl::CreateIndex {
            space_id,
            index_id,
            name: "index1".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            by_fields: vec![],
            initiator: ADMIN_ID,
        });
        let drop_index = builder.with_op(Ddl::DropIndex {
            space_id,
            index_id,
            initiator: ADMIN_ID,
        });

        let commit = Op::DdlCommit;
        let abort = Op::DdlAbort {
            cause: crate::traft::error::ErrorInfo::for_tests(),
        };
        let props = Properties::TABLE_ID;
        let pending_schema_change = (PropertyName::PendingSchemaChange.to_string(),);
        let pending_schema_version = (PropertyName::PendingSchemaChange.to_string(),);
        let global_schema_version = (PropertyName::GlobalSchemaVersion.to_string(),);
        let next_schema_version = (PropertyName::NextSchemaVersion.to_string(),);

        // create_space
        t(&create_space, Range::new(props).eq(&pending_schema_change)).unwrap_err();
        t(&create_space, Range::new(props).eq(&pending_schema_version)).unwrap_err();
        t(&create_space, Range::new(props).eq(("another_key",))).unwrap();

        t(&create_space, Range::new(69105u32).eq(("any_key",))).unwrap();
        t(&create_space, Range::new(space_id).eq(("any_key",))).unwrap_err();

        // drop_space
        // `DropTable` needs `TableDef` to get space name
        storage.pico_table.insert(&table_def).unwrap();
        t(&drop_space, Range::new(props).eq(&pending_schema_change)).unwrap_err();
        t(&drop_space, Range::new(props).eq(&pending_schema_version)).unwrap_err();
        t(&drop_space, Range::new(props).eq(("another_key",))).unwrap();

        t(&drop_space, Range::new(69105u32).eq(("any_key",))).unwrap();
        t(&drop_space, Range::new(space_id).eq(("any_key",))).unwrap_err();

        // create_index
        t(&create_index, Range::new(props).eq(&pending_schema_change)).unwrap_err();
        t(&create_index, Range::new(props).eq(&pending_schema_version)).unwrap_err();
        t(&create_index, Range::new(props).eq(("another_key",))).unwrap();

        t(&create_index, Range::new(69105u32).eq(("any_key",))).unwrap();
        t(&create_index, Range::new(space_id).eq(("any_key",))).unwrap();

        // drop_index
        t(&drop_index, Range::new(props).eq(&pending_schema_change)).unwrap_err();
        t(&drop_index, Range::new(props).eq(&pending_schema_version)).unwrap_err();
        t(&drop_index, Range::new(props).eq(("another_key",))).unwrap();

        t(&drop_index, Range::new(69105u32).eq(("any_key",))).unwrap();
        t(&drop_index, Range::new(space_id).eq(("any_key",))).unwrap();

        // Abort and Commit need a pending schema change to get space name
        let Op::DdlPrepare {
            ddl: create_space_ddl,
            ..
        } = create_space
        else {
            unreachable!();
        };
        storage
            .properties
            .put(PropertyName::PendingSchemaChange, &create_space_ddl)
            .unwrap();
        // commit
        t(&commit, Range::new(props).eq(&pending_schema_change)).unwrap_err();
        t(&commit, Range::new(props).eq(&pending_schema_version)).unwrap_err();
        t(&commit, Range::new(props).eq(&global_schema_version)).unwrap_err();
        t(&commit, Range::new(props).eq(("another_key",))).unwrap();

        t(&commit, Range::new(69105u32).eq(("any_key",))).unwrap();
        t(&commit, Range::new(space_id).eq(("any_key",))).unwrap_err();

        // abort
        t(&abort, Range::new(props).eq(&pending_schema_change)).unwrap_err();
        t(&abort, Range::new(props).eq(&pending_schema_version)).unwrap_err();
        t(&abort, Range::new(props).eq(&global_schema_version)).unwrap_err();
        t(&abort, Range::new(props).eq(next_schema_version)).unwrap_err();
        t(&abort, Range::new(props).eq(("another_key",))).unwrap();

        t(&abort, Range::new(69105u32).eq(("any_key",))).unwrap();
        t(&abort, Range::new(space_id).eq(("any_key",))).unwrap_err();
    }

    #[::tarantool::test]
    fn dml() {
        let key = (12,).to_tuple_buffer().unwrap();
        let tuple = (12, "twelve").to_tuple_buffer().unwrap();
        let storage = Catalog::for_tests();

        let test =
            |op: &Dml, range: Range| check_predicate(2, &Op::Dml(op.clone()), &[range], &storage);

        let table = PicoTable::TABLE_ID;
        let ops = &[
            Dml::Insert {
                table,
                tuple: tuple.clone(),
                initiator: ADMIN_ID,
                conflict_strategy: ConflictStrategy::DoFail,
            },
            Dml::Replace {
                table,
                tuple,
                initiator: ADMIN_ID,
            },
            Dml::Update {
                table,
                key: key.clone(),
                ops: vec![],
                initiator: ADMIN_ID,
            },
            Dml::Delete {
                table,
                key: key.clone(),
                initiator: ADMIN_ID,
            },
        ];

        for op in ops {
            test(op, Range::new(table)).unwrap_err();
            test(op, Range::new(table).le((12,))).unwrap_err();
            test(op, Range::new(table).ge((12,))).unwrap_err();
            test(op, Range::new(table).eq((12,))).unwrap_err();

            test(op, Range::new(table).lt((12,))).unwrap();
            test(op, Range::new(table).le((11,))).unwrap();

            test(op, Range::new(table).gt((12,))).unwrap();
            test(op, Range::new(table).ge((13,))).unwrap();

            test(op, Range::new(69105u32)).unwrap();

            assert_eq!(
                Range::for_dml(op).unwrap(),
                Range::new(table).eq(key.clone())
            )
        }
    }
}
