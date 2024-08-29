use crate::access_control;
use crate::error_code::ErrorCode;
use crate::proc_name;
use crate::storage;
use crate::storage::Clusterwide;
use crate::storage::ClusterwideTable;
use crate::tlog;
use crate::traft;
use crate::traft::error::Error as TraftError;
use crate::traft::node;
use crate::traft::op::{Ddl, Dml, Op};
use crate::traft::EntryContext;
use crate::traft::Result;
use crate::traft::{RaftIndex, RaftTerm};

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
use tarantool::tuple::{KeyDef, ToTupleBuffer, Tuple, TupleBuffer};

/// These tables cannot be changed directly dy a [`Dml`] operation. They have
/// dedicated operation types (e.g. Ddl, Acl) because updating these tables
/// requires automatically updating corresponding local spaces.
const PROHIBITED_TABLES: &[ClusterwideTable] = &[
    ClusterwideTable::Table,
    ClusterwideTable::Index,
    ClusterwideTable::User,
    ClusterwideTable::Privilege,
    ClusterwideTable::Routine,
];

pub fn check_dml_prohibited(dml: &Dml) -> traft::Result<()> {
    let space = dml.space();
    let Ok(table) = &ClusterwideTable::try_from(space) else {
        return Ok(());
    };
    if PROHIBITED_TABLES.contains(table) {
        return Err(Error::TableNotAllowed {
            table: table.name().into(),
        }
        .into());
    }

    Ok(())
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
        res = proc_cas_local(request);
    } else {
        let future = async {
            let timeout = deadline.duration_since(fiber::clock());
            node.pool
                .call(&leader_id, proc_name!(proc_cas), request, timeout)?
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

    return Ok(CasResult::Ok((response.index, response.term)));
}

#[must_use = "You must decide if you're retrying the error or returning it to user"]
pub enum CasResult {
    Ok((RaftIndex, RaftTerm)),
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
    pub fn no_retries(self) -> traft::Result<(RaftIndex, RaftTerm)> {
        match self {
            Self::Ok(v) => Ok(v),
            Self::RetriableError(e) => Err(e),
        }
    }
}

fn proc_cas_local(req: &Request) -> Result<Response> {
    let node = node::global()?;
    let raft_storage = &node.raft_storage;
    let storage = &node.storage;
    let cluster_id = raft_storage.cluster_id()?;

    if req.cluster_id != cluster_id {
        return Err(TraftError::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id.clone(),
            cluster_cluster_id: cluster_id,
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
            check_dml_prohibited(dml)?;
        }
        Op::BatchDml { ops: dmls } => {
            for dml in dmls {
                check_dml_prohibited(dml)?;
            }
        }
        _ => {}
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
            req.predicate.check_entry(entry_index, &op, storage)?;
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
        req.predicate.check_entry(entry.index, &op, storage)?;
    }

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
            // Return the error if it happened. Ignore the tuple if there was one.
            _ = res?;
        }
        Op::BatchDml { ops, .. } => {
            let mut res = Ok(None);
            transaction::begin()?;
            for op in ops {
                res = storage.do_dml(op);
                if res.is_err() {
                    break;
                }
            }
            transaction::rollback().expect("can't fail");
            _ = res?;
        }
        // We can't use a `do_acl` inside of a transaction here similarly to `do_dml`,
        // as acl should be applied only on replicaset leader. Raft leader is not always
        // a replicaset leader.
        Op::Acl(acl) => acl.validate()?,
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

    Ok(Response { index, term })
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
    /// 3. Cluster id mismatch
    /// 4. Request has an incorrect term - leader changed
    /// 5. Receiveing instance is not a raft-leader
    /// 6. [Compare and swap error](Error)
    // TODO Result<Either<Response, Error>>
    fn proc_cas(req: Request) -> Result<Response> {
        proc_cas_local(&req)
    }

    pub struct Request {
        pub cluster_id: String,
        pub predicate: Predicate,
        pub op: Op,
        pub as_user: UserId,
    }

    pub struct Response {
        pub index: RaftIndex,
        pub term: RaftTerm,
    }
}

impl Request {
    #[inline(always)]
    pub fn new(op: impl Into<Op>, predicate: Predicate, as_user: UserId) -> traft::Result<Self> {
        let node = node::global()?;
        Ok(Request {
            cluster_id: node.raft_storage.cluster_id()?,
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

    /// Checks if `entry_op` changes anything within the ranges specified in the predicate.
    pub fn check_entry(
        &self,
        entry_index: RaftIndex,
        entry_op: &Op,
        storage: &Clusterwide,
    ) -> std::result::Result<(), Error> {
        let check_dml =
            |op: &Dml, space_id: u32, range: &Range| -> std::result::Result<(), Error> {
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
        for range in &self.ranges {
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
                    let space = ClusterwideTable::Property.id();
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
                    let space = ClusterwideTable::Property.id();
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
}

const SCHEMA_RELATED_PROPERTIES: [&str; 4] = [
    crate::storage::PropertyName::PendingSchemaChange.as_str(),
    crate::storage::PropertyName::PendingSchemaVersion.as_str(),
    crate::storage::PropertyName::GlobalSchemaVersion.as_str(),
    crate::storage::PropertyName::NextSchemaVersion.as_str(),
];

/// Returns a slice of tuples representing keys of space _pico_property which
/// should be used to check predicates of schema changing CaS operations.
fn schema_related_property_keys() -> &'static [Tuple] {
    static mut DATA: Option<Vec<Tuple>> = None;

    // Safety: we only call this from tx thread, so it's ok, trust me
    unsafe {
        if DATA.is_none() {
            let mut data = Vec::with_capacity(SCHEMA_RELATED_PROPERTIES.len());
            for key in SCHEMA_RELATED_PROPERTIES {
                let t = Tuple::new(&(key,)).expect("keys should convert to tuple");
                data.push(t);
            }
            DATA = Some(data);
        }

        DATA.as_ref().unwrap()
    }
}

fn pending_plugin_operation_key() -> &'static Tuple {
    static mut TUPLE: Option<Tuple> = None;
    // Safety: only called from main thread
    unsafe {
        TUPLE.get_or_insert_with(|| {
            Tuple::new(&[storage::PropertyName::PendingPluginOperation]).expect("cannot fail")
        })
    }
}

/// Returns a slice of [`Range`] structs which are needed for the CaS
/// request which performs a schema change operation.
pub fn schema_change_ranges() -> &'static [Range] {
    static mut DATA: Option<Vec<Range>> = None;

    // Safety: we only call this from tx thread, so it's ok, trust me
    unsafe {
        if DATA.is_none() {
            let mut data = Vec::with_capacity(SCHEMA_RELATED_PROPERTIES.len());
            for key in SCHEMA_RELATED_PROPERTIES {
                let r = Range::new(ClusterwideTable::Property).eq((key,));
                data.push(r);
            }
            DATA = Some(data);
        }

        DATA.as_ref().unwrap()
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

impl Range {
    pub fn from_lua_args(range: RangeInLua) -> traft::Result<Self> {
        let node = traft::node::global()?;
        let table = if let Some(table) = node.storage.tables.by_name(&range.table)? {
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
            Op::BatchDml { ops } => ops.iter().map(Self::for_dml).collect(),
            Op::DdlPrepare { .. } | Op::DdlCommit | Op::DdlAbort { .. } | Op::Acl { .. } => {
                let range = Self::new(ClusterwideTable::Property)
                    .eq([storage::PropertyName::GlobalSchemaVersion]);
                Ok(vec![range])
            }
            Op::Plugin { .. } => {
                let range = Self::new(ClusterwideTable::Property)
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

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead)]
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
fn modifies_operable(op: &Op, space: SpaceId, storage: &Clusterwide) -> bool {
    let ddl_modifies = |ddl: &Ddl| match ddl {
        Ddl::CreateTable { id, .. } => *id == space,
        Ddl::DropTable { id, .. } => *id == space,
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
    use tarantool::index::IndexType;
    use tarantool::space::SpaceEngineType;
    use tarantool::tuple::ToTupleBuffer;

    use crate::schema::{Distribution, IndexOption, TableDef, ADMIN_ID};
    use crate::storage::TClusterwideTable as _;
    use crate::storage::{Clusterwide, Properties, PropertyName};
    use crate::traft::op::DdlBuilder;

    use super::*;

    #[::tarantool::test]
    fn ddl() {
        let storage = Clusterwide::for_tests();

        let t = |op: &Op, range: Range| -> std::result::Result<(), Error> {
            let predicate = Predicate {
                index: 1,
                term: 1,
                ranges: vec![range],
            };
            predicate.check_entry(2, op, &storage)
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
        assert!(t(&create_space, Range::new(props).eq(&pending_schema_change)).is_err());
        assert!(t(&create_space, Range::new(props).eq(&pending_schema_version)).is_err());
        assert!(t(&create_space, Range::new(props).eq(("another_key",))).is_ok());

        assert!(t(&create_space, Range::new(69105u32).eq(("any_key",))).is_ok());
        assert!(t(&create_space, Range::new(space_id).eq(("any_key",))).is_err());

        // drop_space
        // `DropTable` needs `TableDef` to get space name
        storage.tables.insert(&table_def).unwrap();
        assert!(t(&drop_space, Range::new(props).eq(&pending_schema_change)).is_err());
        assert!(t(&drop_space, Range::new(props).eq(&pending_schema_version)).is_err());
        assert!(t(&drop_space, Range::new(props).eq(("another_key",))).is_ok());

        assert!(t(&drop_space, Range::new(69105u32).eq(("any_key",))).is_ok());
        assert!(t(&drop_space, Range::new(space_id).eq(("any_key",))).is_err());

        // create_index
        assert!(t(&create_index, Range::new(props).eq(&pending_schema_change)).is_err());
        assert!(t(&create_index, Range::new(props).eq(&pending_schema_version)).is_err());
        assert!(t(&create_index, Range::new(props).eq(("another_key",))).is_ok());

        assert!(t(&create_index, Range::new(69105u32).eq(("any_key",))).is_ok());
        assert!(t(&create_index, Range::new(space_id).eq(("any_key",))).is_ok());

        // drop_index
        assert!(t(&drop_index, Range::new(props).eq(&pending_schema_change)).is_err());
        assert!(t(&drop_index, Range::new(props).eq(&pending_schema_version)).is_err());
        assert!(t(&drop_index, Range::new(props).eq(("another_key",))).is_ok());

        assert!(t(&drop_index, Range::new(69105u32).eq(("any_key",))).is_ok());
        assert!(t(&drop_index, Range::new(space_id).eq(("any_key",))).is_ok());

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
        assert!(t(&commit, Range::new(props).eq(&pending_schema_change)).is_err());
        assert!(t(&commit, Range::new(props).eq(&pending_schema_version)).is_err());
        assert!(t(&commit, Range::new(props).eq(&global_schema_version)).is_err());
        assert!(t(&commit, Range::new(props).eq(("another_key",))).is_ok());

        assert!(t(&commit, Range::new(69105u32).eq(("any_key",))).is_ok());
        assert!(t(&commit, Range::new(space_id).eq(("any_key",))).is_err());

        // abort
        assert!(t(&abort, Range::new(props).eq(&pending_schema_change)).is_err());
        assert!(t(&abort, Range::new(props).eq(&pending_schema_version)).is_err());
        assert!(t(&abort, Range::new(props).eq(&global_schema_version)).is_err());
        assert!(t(&abort, Range::new(props).eq(next_schema_version)).is_err());
        assert!(t(&abort, Range::new(props).eq(("another_key",))).is_ok());

        assert!(t(&abort, Range::new(69105u32).eq(("any_key",))).is_ok());
        assert!(t(&abort, Range::new(space_id).eq(("any_key",))).is_err());
    }

    #[::tarantool::test]
    fn dml() {
        let key = (12,).to_tuple_buffer().unwrap();
        let tuple = (12, "twelve").to_tuple_buffer().unwrap();
        let storage = Clusterwide::for_tests();

        let test = |op: &Dml, range: Range| {
            let predicate = Predicate {
                index: 1,
                term: 1,
                ranges: vec![range],
            };
            predicate.check_entry(2, &Op::Dml(op.clone()), &storage)
        };

        let table = ClusterwideTable::Table;
        let ops = &[
            Dml::Insert {
                table: table.into(),
                tuple: tuple.clone(),
                initiator: ADMIN_ID,
            },
            Dml::Replace {
                table: table.into(),
                tuple,
                initiator: ADMIN_ID,
            },
            Dml::Update {
                table: table.into(),
                key: key.clone(),
                ops: vec![],
                initiator: ADMIN_ID,
            },
            Dml::Delete {
                table: table.into(),
                key,
                initiator: ADMIN_ID,
            },
        ];

        let space = ClusterwideTable::Table.id();
        for op in ops {
            assert!(test(op, Range::new(space)).is_err());
            assert!(test(op, Range::new(space).le((12,))).is_err());
            assert!(test(op, Range::new(space).ge((12,))).is_err());
            assert!(test(op, Range::new(space).eq((12,))).is_err());

            assert!(test(op, Range::new(space).lt((12,))).is_ok());
            assert!(test(op, Range::new(space).le((11,))).is_ok());

            assert!(test(op, Range::new(space).gt((12,))).is_ok());
            assert!(test(op, Range::new(space).ge((13,))).is_ok());

            assert!(test(op, Range::new(69105u32)).is_ok());
        }
    }
}
