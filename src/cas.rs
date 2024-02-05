use std::time::Duration;

use crate::access_control;
use crate::rpc;
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
use crate::unwrap_ok_or;

use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::StorageError;

use tarantool::error::Error as TntError;
use tarantool::fiber;
use tarantool::fiber::r#async::sleep;
use tarantool::fiber::r#async::timeout::IntoTimeout;
use tarantool::session::UserId;
use tarantool::space::{Space, SpaceId};
use tarantool::tlua;
use tarantool::transaction;
use tarantool::tuple::{KeyDef, ToTupleBuffer, Tuple, TupleBuffer};

/// This spaces cannot be changed directly dy a [`Dml`] operation. They have
/// dedicated operation types (e.g. Ddl, Acl) because updating these spaces
/// requires automatically updating corresponding local spaces.
const PROHIBITED_SPACES: &[ClusterwideTable] = &[
    ClusterwideTable::Table,
    ClusterwideTable::Index,
    ClusterwideTable::User,
    ClusterwideTable::Privilege,
    ClusterwideTable::Routine,
];

// FIXME: cas::Error will be returned as a string when rpc is called
/// Performs a clusterwide compare and swap operation.
///
/// E.g. it checks the `predicate` on leader and if no conflicting
/// entries were found appends the `op` to the raft log and returns its
/// index and term.
///
/// # Errors
/// See [`cas::Error`][Error] for CaS-specific errors.
/// It can also return general picodata errors in cases of faulty network or storage.
pub async fn compare_and_swap_async(
    op: Op,
    predicate: Predicate,
    as_user: UserId,
) -> traft::Result<(RaftIndex, RaftTerm)> {
    let node = node::global()?;
    let request = Request {
        cluster_id: node.raft_storage.cluster_id()?,
        predicate,
        op,
        as_user,
    };
    loop {
        let Some(leader_id) = node.status().leader_id else {
            tlog!(
                Warning,
                "leader id is unknown, waiting for status change..."
            );
            node.wait_status();
            continue;
        };
        let leader_address = unwrap_ok_or!(
            node.storage.peer_addresses.try_get(leader_id),
            Err(e) => {
                tlog!(Warning, "failed getting leader address: {e}");
                tlog!(Info, "going to retry in a while...");
                sleep(Duration::from_millis(250)).await;
                continue;
            }
        );
        let resp = if leader_id == node.raft_id {
            // cas has to be called locally in cases when listen ports are closed,
            // for example on shutdown
            proc_cas_local(request.clone())
        } else {
            rpc::network_call(&leader_address, &request)
                .await
                .map_err(TraftError::from)
        };
        match resp {
            Ok(Response { index, term }) => return Ok((index, term)),
            Err(e) => {
                tlog!(Warning, "{e}");
                if e.is_not_leader_err() {
                    tlog!(Info, "going to retry in a while...");
                    node.wait_status();
                    continue;
                } else {
                    return Err(e);
                }
            }
        }
    }
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
    op: Op,
    predicate: Predicate,
    as_user: UserId,
    timeout: Duration,
) -> traft::Result<(RaftIndex, RaftTerm)> {
    fiber::block_on(compare_and_swap_async(op, predicate, as_user).timeout(timeout))
        .map_err(Into::into)
}

fn proc_cas_local(req: Request) -> Result<Response> {
    let node = node::global()?;
    let raft_storage = &node.raft_storage;
    let storage = &node.storage;
    let cluster_id = raft_storage.cluster_id()?;

    if req.cluster_id != cluster_id {
        return Err(TraftError::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id,
            cluster_cluster_id: cluster_id,
        });
    }

    // Other operation types are not used with CaS
    if !matches!(
        req.op,
        Op::Acl(..) | Op::Dml(..) | Op::DdlPrepare { .. } | Op::DdlAbort
    ) {
        return Err(TraftError::Cas(Error::InvalidOpKind(Box::new(req.op))));
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
    // Note: audit log record is automatically emmitted in case there is an error,
    // because it is hooked into AccessDenied error creation (on_access_denied) trigger
    access_control::access_check_op(storage, &req.op, req.as_user)?;

    let last_persisted = raft::Storage::last_index(raft_storage)?;
    assert!(last_persisted <= last);

    // Check if this dml operation does not modify PROHIBITED_SPACES
    {
        let check_dml_prohibited = |dml: &Dml| -> traft::Result<()> {
            let space = dml.space();
            let Ok(table) = &ClusterwideTable::try_from(space) else {
                return Ok(());
            };
            if PROHIBITED_SPACES.contains(table) {
                return Err(Error::SpaceNotAllowed {
                    space: table.name().into(),
                }
                .into());
            }
            Ok(())
        };
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
    let unstable = raft_log.entries(last_persisted + 1, u64::MAX)?;
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
        // We can't use a `do_acl` inside of a transaction here similarly to `do_dml`,
        // as acl should be applied only on replicaset leader. Raft leader is not always
        // a replicaset leader.
        Op::Acl(acl) => acl.validate()?,
        _ => (),
    }

    // Don't wait for the proposal to be accepted, instead return the index
    // to the requestor, so that they can wait for it.

    let entry_id = node_impl.propose_async(req.op)?;
    let index = entry_id.index;
    let term = entry_id.term;
    assert_eq!(index, last + 1);
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
        proc_cas_local(req)
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

/// Error kinds specific to Compare and Swap algorithm.
///
/// Usually it can't be handled and should be either retried from
/// scratch or returned to a user as is.
///
// NOTE: these error messages are relied on in luamod.lua,
// don't forget to update them everywhere if you're changing them.
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

    #[error("SpaceNotAllowed: space {space} is prohibited for use in a predicate")]
    SpaceNotAllowed { space: String },

    /// An error related to `key_def` operation arised from tarantool
    /// depths while checking the predicate.
    #[error("KeyTypeMismatch: failed comparing predicate ranges: {0}")]
    KeyTypeMismatch(#[from] TntError),

    /// NOTE: Boxing is caused by big difference in size compared to other variants
    /// See <https://rust-lang.github.io/rust-clippy/master/index.html#/result_large_err> for more context
    #[error("InvalidOpKind: Expected one of Acl, Dml, DdlPrepare or DdlAbort, got {0}")]
    InvalidOpKind(Box<Op>),
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
    pub fn from_lua_args(predicate: PredicateInLua) -> traft::Result<Self> {
        let node = traft::node::global()?;
        let (index, term) = if let Some(index) = predicate.index {
            if let Some(term) = predicate.term {
                (index, term)
            } else {
                let term = raft::Storage::term(&node.raft_storage, index)?;
                (index, term)
            }
        } else {
            let index = node.get_index();
            let term = raft::Storage::term(&node.raft_storage, index)?;
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
        let error = || Error::ConflictFound {
            conflict_index: entry_index,
        };
        for range in &self.ranges {
            if modifies_operable(entry_op, range.table, storage) {
                return Err(error());
            }
            let Some(space) = space(entry_op) else {
                continue;
            };
            // TODO: check `space` exists
            if space != range.table {
                continue;
            }

            match entry_op {
                Op::Dml(Dml::Update { key, .. } | Dml::Delete { key, .. }) => {
                    let key = Tuple::new(key)?;
                    let key_def = storage.key_def_for_key(space, 0)?;
                    if range.contains(&key_def, &key) {
                        return Err(error());
                    }
                }
                Op::Dml(Dml::Insert { tuple, .. } | Dml::Replace { tuple, .. }) => {
                    let tuple = Tuple::new(tuple)?;
                    let key_def = storage.key_def(space, 0)?;
                    if range.contains(&key_def, &tuple) {
                        return Err(error());
                    }
                }
                Op::DdlPrepare { .. } | Op::DdlCommit | Op::DdlAbort | Op::Acl { .. } => {
                    let key_def = storage.key_def_for_key(space, 0)?;
                    for key in schema_related_property_keys() {
                        if range.contains(&key_def, key) {
                            return Err(error());
                        }
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
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead)]
pub struct RangeInLua {
    /// Table name.
    pub table: String,
    pub key_min: Bound,
    pub key_max: Bound,
}

/// A range of keys used as an argument for a [`Predicate`].
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead)]
pub struct Range {
    pub table: SpaceId,
    pub key_min: Bound,
    pub key_max: Bound,
}

impl Range {
    pub fn from_lua_args(range: RangeInLua) -> traft::Result<Self> {
        let node = traft::node::global()?;
        let table_id = if let Some(table) = node.storage.tables.by_name(&range.table)? {
            table.id
        } else if let Some(table) = Space::find(&range.table) {
            table.id()
        } else {
            return Err(TraftError::other(format!(
                "table '{}' not found",
                range.table
            )));
        };
        Ok(Self {
            table: table_id,
            key_min: range.key_min,
            key_max: range.key_max,
        })
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
            key_min: Bound::unbounded(),
            key_max: Bound::unbounded(),
        }
    }

    /// Add a "greater than" restriction.
    #[inline(always)]
    pub fn gt(mut self, key: impl ToTupleBuffer) -> Self {
        self.key_min = Bound::excluded(&key);
        self
    }

    /// Add a "greater or equal" restriction.
    #[inline(always)]
    pub fn ge(mut self, key: impl ToTupleBuffer) -> Self {
        self.key_min = Bound::included(&key);
        self
    }

    /// Add a "less than" restriction.
    #[inline(always)]
    pub fn lt(mut self, key: impl ToTupleBuffer) -> Self {
        self.key_max = Bound::excluded(&key);
        self
    }

    /// Add a "less or equal" restriction.
    #[inline(always)]
    pub fn le(mut self, key: impl ToTupleBuffer) -> Self {
        self.key_max = Bound::included(&key);
        self
    }

    /// Add a "equal" restriction.
    #[inline(always)]
    pub fn eq(mut self, key: impl ToTupleBuffer) -> Self {
        self.key_min = Bound::included(&key);
        self.key_max = Bound::included(&key);
        self
    }

    pub fn contains(&self, key_def: &KeyDef, tuple: &Tuple) -> bool {
        let min_satisfied = match self.key_min.kind {
            BoundKind::Included => key_def
                .compare_with_key(tuple, self.key_min.key.as_ref().unwrap())
                .is_ge(),
            BoundKind::Excluded => key_def
                .compare_with_key(tuple, self.key_min.key.as_ref().unwrap())
                .is_gt(),
            BoundKind::Unbounded => true,
        };
        // short-circuit
        if !min_satisfied {
            return false;
        }
        let max_satisfied = match self.key_max.kind {
            BoundKind::Included => key_def
                .compare_with_key(tuple, self.key_max.key.as_ref().unwrap())
                .is_le(),
            BoundKind::Excluded => key_def
                .compare_with_key(tuple, self.key_max.key.as_ref().unwrap())
                .is_lt(),
            BoundKind::Unbounded => true,
        };
        // min_satisfied && max_satisfied
        max_satisfied
    }
}

/// A bound for keys.
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead)]
pub struct Bound {
    kind: BoundKind,
    #[serde(with = "serde_bytes")]
    key: Option<TupleBuffer>,
}

impl Bound {
    pub fn included(key: &impl ToTupleBuffer) -> Self {
        Self {
            kind: BoundKind::Included,
            key: Some(key.to_tuple_buffer().expect("cannot fail")),
        }
    }

    pub fn excluded(key: &impl ToTupleBuffer) -> Self {
        Self {
            kind: BoundKind::Excluded,
            key: Some(key.to_tuple_buffer().expect("cannot fail")),
        }
    }

    pub fn unbounded() -> Self {
        Self {
            kind: BoundKind::Unbounded,
            key: None,
        }
    }
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

/// Get space that the operation touches.
fn space(op: &Op) -> Option<SpaceId> {
    match op {
        Op::Dml(dml) => Some(dml.space()),
        Op::DdlPrepare { .. } | Op::DdlCommit | Op::DdlAbort | Op::Acl { .. } => {
            Some(ClusterwideTable::Property.id())
        }
        Op::Nop => None,
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
    use tarantool::space::SpaceEngineType;
    use tarantool::tuple::ToTupleBuffer;

    use crate::schema::{Distribution, TableDef, ADMIN_ID};
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

        let space_name = "space1";
        let space_id = 1;
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
            by_fields: vec![],
        });
        let drop_index = builder.with_op(Ddl::DropIndex { space_id, index_id });

        let commit = Op::DdlCommit;
        let abort = Op::DdlAbort;
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
        storage
            .tables
            .insert(&TableDef {
                id: space_id,
                name: space_name.into(),
                operable: true,
                distribution: Distribution::Global,
                format: vec![],
                schema_version: 1,
                engine: SpaceEngineType::Memtx,
                owner: ADMIN_ID,
            })
            .unwrap();
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
