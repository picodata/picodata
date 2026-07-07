use crate::access_control;
use crate::config;
use crate::error_code::ErrorCode;
use crate::mailbox::Mailbox;
use crate::metrics;
use crate::proc_name;
use crate::schema::IndexOption;
use crate::static_ref;
use crate::storage;
use crate::storage::Catalog;
use crate::storage::Properties;
use crate::storage::SystemTable;
use crate::tlog;
use crate::traft;
use crate::traft::error::Error as TraftError;
use crate::traft::node;
use crate::traft::op::{Acl, CasKey, Ddl, Dml, DmlCas, Op};
use crate::traft::EntryContext;
use crate::traft::Result;
use crate::traft::{RaftIndex, RaftTerm, ResRowCount};
use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::GetEntriesContext;
use ::raft::StorageError;
use smol_str::SmolStr;
use sql::ir::operator::ConflictStrategy;
use tarantool::error::Error as TntError;
use tarantool::error::TarantoolErrorCode;
use tarantool::fiber;
use tarantool::fiber::r#async::oneshot;
use tarantool::fiber::r#async::timeout::IntoTimeout;
use tarantool::index::IndexId;
use tarantool::session::UserId;
use tarantool::space::{Space, SpaceId};
use tarantool::time::Instant;
use tarantool::tlua;
use tarantool::transaction;
use tarantool::tuple::{Decode, KeyDef, ToTupleBuffer, Tuple, TupleBuffer};

/// These tables cannot be changed directly dy a [`Dml`] operation. They have
/// dedicated operation types (e.g. Ddl, Acl) because updating these tables
/// requires automatically updating corresponding local spaces.
const PROHIBITED_TABLES: &[SpaceId] = &[
    crate::catalog::pico_table::PicoTable::TABLE_ID,
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
        if config::AlterSystemParameters::is_read_only(config_name) {
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
            Dml::Delete { .. } => {
                let error = format!(
                    "DELETE on table '{}' is denied for all users",
                    storage::DbConfig::TABLE_NAME
                );
                return Err(traft::error::Error::other(error));
            }
        }
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

    let duration = Instant::now_fiber().duration_since(start);
    metrics::observe_cas_ops_duration(&duration);
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

    let duration = Instant::now_fiber().duration_since(start);
    metrics::observe_cas_ops_duration(&duration);
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

    if let Some(inbox) = &node.cas_test_override {
        return handle_test_override(inbox, request);
    }

    if let Op::BatchDml { ops } = &request.op {
        if ops.is_empty() {
            return Err(Error::EmptyBatch.into());
        }
    }

    let Some(leader_id) = node.status().leader_id else {
        return if force_local {
            // If we do not return it as a fatal error here, a preemptively sent
            // `handle_update_instance_request_and_wait` will loop indefinitely and
            // prevent a leader from being elected during a cold restart.
            Err(TraftError::LeaderUnknown)
        } else {
            Ok(CasResult::RetriableError(TraftError::LeaderUnknown))
        };
    };

    let i_am_leader = leader_id == node.raft_id;

    let res;
    if i_am_leader {
        // cas has to be called locally in cases when listen ports are closed,
        // for example on shutdown
        res = proc_cas_v2_local(request);
    } else if force_local {
        return Err(TraftError::NotALeader { leader_id });
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
    let cluster_name = node.topology_cache.cluster_name;

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
    let Some(leader_id) = status.leader_id else {
        return Err(TraftError::LeaderUnknown);
    };
    if leader_id != node.raft_id() {
        // Invalid request. This node is not a leader at this moment.
        return Err(TraftError::NotALeader { leader_id });
    }

    let raft_log = &node_impl.raw_node.raft.raft_log;

    let first = raft_log.first_index();
    let last = raft_log.last_index();
    debug_assert!(first >= 1, "{first}");
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

    debug_assert!(
        requested >= first - 1,
        "requested: {requested}, first: {first}"
    );
    debug_assert!(requested <= last, "requested: {requested}, last: {last}");
    debug_assert_eq!(requested_term, status.term);

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

    match &req.op {
        Op::Dml(dml) => {
            check_table_operable(storage, dml.table_id())?;
            check_dml_prohibited(storage, dml)?;
        }
        Op::BatchDml { ops: dmls } => {
            for dml in dmls {
                check_table_operable(storage, dml.table_id())?;
                check_dml_prohibited(storage, dml)?;
            }
        }
        Op::Acl(acl) => {
            check_acl_limits(storage, acl)?;
        }
        _ => {}
    }

    let mut predicates = CasPredicates::default();
    predicates.extend_ranges(req.predicate.ranges.iter().cloned());
    predicates.extend_ranges(Range::for_op(&req.op)?);

    let (proposal, res_row_count) = match prepare_proposal(storage, &req.op) {
        Ok(prepared) => prepared,
        Err(err) => {
            // Proposal preparation reads the leader's current storage state.
            // If the request is stale, that read may observe a conflicting
            // in-flight raft entry after `requested` and surface it as a hard
            // storage error. Check the tail before returning the error so such
            // requests stay on the retriable CasConflictFound path.
            //
            // Candidate secondary keys are only needed on this path: the success
            // path gets trusted CAS metadata from the prepared proposal. Add
            // best-effort Insert/Replace candidate keys, then add conservative
            // same-table predicates for Update/Delete, whose secondary keys
            // cannot be computed after proposal preparation has failed.
            add_candidate_cas_keys(storage, &req.op, &mut predicates);
            add_prepare_error_cas_predicates(&req.op, &mut predicates);
            check_raft_tail(
                &node_impl,
                raft_storage,
                storage,
                requested,
                last,
                status.term,
                &predicates,
            )?;
            return Err(err);
        }
    };

    predicates.extend_dml_cas(&proposal);

    // Proposal preparation computes additional predicates for old/new secondary
    // unique keys, so the tail must be checked with the full dependency set.
    check_raft_tail(
        &node_impl,
        raft_storage,
        storage,
        requested,
        last,
        status.term,
        &predicates,
    )?;

    // Don't wait for the proposal to be accepted, instead return the index
    // to the requestor, so that they can wait for it.

    let entry_id = node_impl.propose_async(proposal)?;
    let index = entry_id.index;
    let term = entry_id.term;
    // TODO: return number of raft entries and check this
    // assert_eq!(index, last + 1);
    debug_assert_eq!(term, requested_term);
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

fn check_raft_tail(
    node_impl: &node::NodeImpl,
    raft_storage: &traft::RaftSpaceAccess,
    storage: &Catalog,
    requested: RaftIndex,
    last: RaftIndex,
    term: RaftTerm,
    predicates: &CasPredicates,
) -> Result<()> {
    let raft_log = &node_impl.raw_node.raft.raft_log;
    let mut last_persisted = raft::Storage::last_index(raft_storage)?;
    if last_persisted > last {
        // This is possible right after raft leader change, when the persisted
        // raft log needs to be truncated
        last_persisted = last;
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

    // Also it's possible that last_persisted > last, right after leader change
    // when uncommitted raft log tail gets truncated.

    if requested < last_persisted {
        // there's at least one persisted entry to check
        let persisted = raft_storage.entries(requested + 1, last_persisted + 1, None)?;
        if persisted.len() < (last_persisted - requested) as usize {
            return Err(RaftError::Store(StorageError::Unavailable).into());
        }

        for entry in persisted {
            debug_assert_eq!(entry.term, term);
            let entry_index = entry.index;
            let Some(op) = entry.into_op() else { continue };
            check_predicate(entry_index, &op, predicates, storage)?;
        }
    }

    // Check remaining unstable entries.
    let unstable = raft_log.entries(
        last_persisted + 1,
        u64::MAX,
        GetEntriesContext::empty(false),
    )?;
    for entry in unstable {
        debug_assert_eq!(entry.term, term);
        let Ok(cx) = EntryContext::from_raft_entry(&entry) else {
            tlog!(Warning, "raft entry has invalid context"; "entry" => ?entry);
            continue;
        };
        let EntryContext::Op(op) = cx else {
            continue;
        };
        check_predicate(entry.index, &op, predicates, storage)?;
    }

    Ok(())
}

fn prepare_proposal(storage: &Catalog, op: &Op) -> Result<(Op, ResRowCount)> {
    let mut proposal = op.clone();

    // Performs preliminary checks on an operation so that it will not fail when applied.
    let res_row_count = match &mut proposal {
        Op::Dml(dml) => {
            // Check if the requested dml is applicable to the local storage.
            // This will run the required on_replace triggers which will check among
            // other things conformity to space format, user defined constraints etc.
            //
            // At the same time compute CAS conflict data from trusted local
            // storage state. Any CAS data that arrived in the request is discarded.
            transaction::begin()?;
            let res = prepare_dml_proposal(storage, dml);
            transaction::rollback().expect("can't fail");

            let (res, cas) = res?;
            // Do not reuse CAS data from the request. It is internal raft
            // metadata and must be computed from trusted local storage while
            // preparing the proposal.
            dml.set_cas(cas);

            if res.is_some() {
                1
            } else {
                0
            }
        }
        Op::BatchDml { ops, .. } => {
            let mut res_row_count = 0;

            transaction::begin()?;
            let res = (|| -> Result<()> {
                for dml in ops {
                    let (res, cas) = prepare_dml_proposal(storage, dml)?;
                    // Do not reuse CAS data from the request. It is internal
                    // raft metadata and must be computed from trusted local
                    // storage while preparing the proposal.
                    dml.set_cas(cas);

                    if res.is_some() {
                        res_row_count += 1;
                    }
                }
                Ok(())
            })();
            transaction::rollback().expect("can't fail");

            res?;
            res_row_count
        }
        // We can't use a `do_acl` inside of a transaction here similarly to `do_dml`,
        // as acl should be applied only on replicaset leader. Raft leader is not always
        // a replicaset leader.
        Op::Acl(acl) => {
            acl.validate(storage)?;
            0
        }
        _ => 0,
    };

    Ok((proposal, res_row_count))
}

fn prepare_dml_proposal(storage: &Catalog, dml: &Dml) -> Result<(Option<Tuple>, DmlCas)> {
    let table_id = dml.table_id();
    let new_candidate = match dml {
        Dml::Insert { tuple, .. } | Dml::Replace { tuple, .. } => Some(Tuple::new(tuple)?),
        Dml::Update { .. } | Dml::Delete { .. } => None,
    };
    let table_full_change = matches!(
        dml,
        Dml::Delete {
            metainfo: Some(_),
            ..
        }
    );
    let old = if table_full_change {
        None
    } else {
        old_tuple_for_dml(dml)?
    };

    let res = storage.do_dml(dml)?;

    let mut cas = DmlCas::default();
    if table_full_change {
        mark_table_full_change(storage, table_id, &mut cas)?;
    }
    if let Some(old) = &old {
        add_tuple_cas_keys(storage, table_id, &mut cas, old)?;
    }
    if let Some(new_candidate) = &new_candidate {
        // Insert DoNothing may return None, but the no-op still depends on the
        // conflicting unique key staying present until this entry is applied.
        add_tuple_cas_keys(storage, table_id, &mut cas, new_candidate)?;
    }
    if let Some(new) = &res {
        add_tuple_cas_keys(storage, table_id, &mut cas, new)?;
    }

    Ok((res, cas))
}

fn add_candidate_cas_keys(storage: &Catalog, op: &Op, predicates: &mut CasPredicates) {
    let Some(dmls) = op.dmls() else {
        return;
    };

    for dml in dmls {
        let (table, tuple) = match dml {
            Dml::Insert { table, tuple, .. } | Dml::Replace { table, tuple, .. } => (*table, tuple),
            // Computing secondary keys for Update/Delete needs the old tuple
            // and, for Update, applying the update ops. The success path does
            // that while preparing the proposal; the prepare-error path adds a
            // conservative same-table predicate before checking the raft tail.
            Dml::Update { .. } | Dml::Delete { .. } => continue,
        };
        // Candidate secondary predicates are only needed to classify stale
        // failures as retriable CAS conflicts. Do not let their best-effort
        // extraction change the user-visible DML validation error: keep a
        // conservative same-table predicate and let prepare_proposal decide.
        let Ok(tuple) = Tuple::new(tuple) else {
            predicates.add(CasPredicate::TableFullChange(table));
            continue;
        };
        let mut cas = DmlCas::default();
        if add_tuple_cas_keys(storage, table, &mut cas, &tuple).is_ok() {
            predicates.extend_table_dml_cas(table, &cas);
        } else {
            predicates.add(CasPredicate::TableFullChange(table));
        }
    }
}

fn add_prepare_error_cas_predicates(op: &Op, predicates: &mut CasPredicates) {
    let Some(dmls) = op.dmls() else {
        return;
    };

    for dml in dmls {
        match dml {
            Dml::Update { .. } | Dml::Delete { .. } => {
                predicates.add(CasPredicate::TableFullChange(dml.table_id()));
            }
            Dml::Insert { .. } | Dml::Replace { .. } => {}
        }
    }
}

fn old_tuple_for_dml(dml: &Dml) -> Result<Option<Tuple>> {
    let table_id = dml.table_id();
    let space = storage::space_by_id_unchecked(table_id);

    match dml {
        Dml::Insert {
            tuple,
            conflict_strategy: ConflictStrategy::DoReplace,
            ..
        }
        | Dml::Replace { tuple, .. } => {
            let tuple = Tuple::new(tuple)?;
            let key_def = storage::cached_key_def(table_id, 0)?;
            let key = key_def.extract_key(&tuple)?;
            Ok(space.get(&key)?)
        }
        Dml::Update { key, .. }
        | Dml::Delete {
            key,
            metainfo: None,
            ..
        } => Ok(space.get(key)?),
        Dml::Insert { .. } | Dml::Delete { .. } => Ok(None),
    }
}

fn mark_table_full_change(storage: &Catalog, table_id: SpaceId, cas: &mut DmlCas) -> Result<()> {
    if let Some(table) = storage.pico_table.get(table_id)? {
        cas.table_schema_version = table.schema_version;
    }
    cas.table_full_change = true;
    Ok(())
}

fn add_tuple_cas_keys(
    storage: &Catalog,
    table_id: SpaceId,
    cas: &mut DmlCas,
    tuple: &Tuple,
) -> Result<()> {
    let Some(table_def) = storage.pico_table.get(table_id)? else {
        return Ok(());
    };

    for index_def in storage.indexes.by_space_id(table_id)? {
        if index_def.id == 0 {
            continue;
        }
        if !index_def
            .opts
            .iter()
            .any(|opt| matches!(opt, IndexOption::Unique(true)))
        {
            continue;
        }

        // Inoperable unique indexes still matter while DropIndex/RenameIndex
        // is pending: their DDL entry must conflict with stale CAS requests.
        let metadata = index_def.to_index_metadata(&table_def);
        let key_def = metadata.to_key_def();
        let key = key_def.extract_key(tuple)?;
        let unique_key = CasKey {
            index_id: index_def.id,
            key,
        };
        if !cas.unique_keys.contains(&unique_key) {
            cas.unique_keys.push(unique_key);
        }
    }

    if !cas.unique_keys.is_empty() {
        cas.table_schema_version = table_def.schema_version;
    }

    Ok(())
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

    #[derive(PartialEq, Eq)]
    pub struct Request {
        pub cluster_name: SmolStr,
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
            cluster_name: node.topology_cache.cluster_name.into(),
            predicate,
            op: op.into(),
            as_user,
        })
    }
}

impl Response {
    pub fn for_tests() -> Self {
        Self {
            index: 1,
            term: 1,
            res_row_count: 1,
        }
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
    TableNotAllowed { table: SmolStr },

    #[error("ConfigNotAllowed: config {config} cannot be modified")]
    ConfigNotAllowed { config: String },

    #[error(
        "TableNotOperable: table {table} cannot be modified now as DDL operation is in progress"
    )]
    TableNotOperable { table: SmolStr },

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
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, PartialEq, Eq)]
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

/// Checks if `entry_op` changes anything within the `predicates`.
///
/// `entry_index` is only used to report error `ConflictFound`.
fn check_predicate(
    entry_index: RaftIndex,
    entry_op: &Op,
    predicates: &CasPredicates,
    storage: &Catalog,
) -> std::result::Result<(), Error> {
    if predicates.is_empty() {
        return Ok(());
    }

    match entry_op {
        Op::Dml(dml) => check_dml_predicates(entry_index, dml, predicates, storage)?,
        Op::BatchDml { ops } => {
            // TODO: remove O(n*n) complexity,
            // use a hashtable for dml/predicate lookup?
            for dml in ops {
                check_dml_predicates(entry_index, dml, predicates, storage)?;
            }
        }
        Op::DdlPrepare { ddl, .. } => {
            check_schema_op_predicates(entry_index, Some(ddl), predicates)?;
        }
        Op::DdlCommit | Op::DdlAbort { .. } => {
            let ddl = storage
                .properties
                .pending_schema_change()
                .expect("conversion should not fail");
            check_schema_op_predicates(entry_index, ddl.as_ref(), predicates)?;
        }
        Op::Acl(_) => check_schema_op_predicates(entry_index, None, predicates)?,
        Op::Plugin { .. } => check_plugin_predicates(entry_index, predicates)?,
        Op::Nop => {}
    }

    Ok(())
}

#[derive(Default)]
struct CasPredicates(Vec<CasPredicate>);

impl CasPredicates {
    fn extend_ranges(&mut self, ranges: impl IntoIterator<Item = Range>) {
        for range in ranges {
            self.add(CasPredicate::PrimaryRange(range));
        }
    }

    fn extend_dml_cas(&mut self, op: &Op) {
        let Some(dmls) = op.dmls() else {
            return;
        };

        for dml in dmls {
            if let Some(cas) = dml.cas() {
                self.extend_table_dml_cas(dml.table_id(), cas);
            }
        }
    }

    fn extend_table_dml_cas(&mut self, table: SpaceId, cas: &DmlCas) {
        if cas.table_full_change {
            self.add(CasPredicate::TableFullChange(table));
        }

        for key in &cas.unique_keys {
            self.add(CasPredicate::UniqueKey {
                table,
                table_schema_version: cas.table_schema_version,
                index_id: key.index_id,
                key: key.key.clone(),
            });
        }
    }

    fn add(&mut self, predicate: CasPredicate) {
        if !self.0.contains(&predicate) {
            self.0.push(predicate);
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn contains_table(&self, table: SpaceId) -> bool {
        self.0.iter().any(|predicate| predicate.table() == table)
    }

    fn contains_schema_dependent_table(&self, table: SpaceId) -> bool {
        self.0.iter().any(|predicate| match predicate {
            CasPredicate::UniqueKey {
                table: predicate_table,
                ..
            }
            | CasPredicate::TableFullChange(predicate_table) => *predicate_table == table,
            CasPredicate::PrimaryRange(_) => false,
        })
    }

    fn has_table_full_change(&self, table: SpaceId) -> bool {
        self.0
            .iter()
            .any(|predicate| matches!(predicate, CasPredicate::TableFullChange(t) if *t == table))
    }
}

#[derive(PartialEq, Eq)]
enum CasPredicate {
    PrimaryRange(Range),
    UniqueKey {
        table: SpaceId,
        table_schema_version: u64,
        index_id: IndexId,
        key: TupleBuffer,
    },
    TableFullChange(SpaceId),
}

impl CasPredicate {
    fn table(&self) -> SpaceId {
        match self {
            CasPredicate::PrimaryRange(range) => range.table,
            CasPredicate::UniqueKey { table, .. } | CasPredicate::TableFullChange(table) => *table,
        }
    }
}

fn check_dml_predicates(
    entry_index: RaftIndex,
    dml: &Dml,
    predicates: &CasPredicates,
    storage: &Catalog,
) -> std::result::Result<(), Error> {
    let table = dml.table_id();
    if !predicates.contains_table(table) {
        return Ok(());
    }

    if predicates.has_table_full_change(table) || dml.cas().is_some_and(|cas| cas.table_full_change)
    {
        return Err(Error::ConflictFound(entry_index));
    }

    for predicate in &predicates.0 {
        match predicate {
            CasPredicate::PrimaryRange(range) if range.table == table => {
                check_primary_dml_range(entry_index, dml, range)?;
            }
            CasPredicate::UniqueKey {
                table: predicate_table,
                table_schema_version,
                index_id,
                key,
            } if *predicate_table == table => {
                check_unique_key_dml_predicate(
                    entry_index,
                    dml,
                    *table_schema_version,
                    *index_id,
                    key,
                    storage,
                )?;
            }
            CasPredicate::TableFullChange(_)
            | CasPredicate::PrimaryRange(_)
            | CasPredicate::UniqueKey { .. } => {}
        }
    }

    Ok(())
}

fn check_primary_dml_range(
    entry_index: RaftIndex,
    dml: &Dml,
    range: &Range,
) -> std::result::Result<(), Error> {
    let table = dml.table_id();

    match dml {
        Dml::Delete {
            metainfo: Some(_), ..
        } => return Err(Error::ConflictFound(entry_index)),
        Dml::Update { key, .. } | Dml::Delete { key, .. } => {
            let key = Tuple::new(key)?;
            let key_def = storage::cached_key_def_for_key(table, 0)?;
            if range.contains(&key_def, &key) {
                return Err(Error::ConflictFound(entry_index));
            }
        }
        Dml::Insert { tuple, .. } | Dml::Replace { tuple, .. } => {
            let tuple = Tuple::new(tuple)?;
            let key_def = storage::cached_key_def(table, 0)?;
            if range.contains(&key_def, &tuple) {
                return Err(Error::ConflictFound(entry_index));
            }
        }
    }

    Ok(())
}

fn check_unique_key_dml_predicate(
    entry_index: RaftIndex,
    dml: &Dml,
    table_schema_version: u64,
    index_id: IndexId,
    predicate_key: &TupleBuffer,
    storage: &Catalog,
) -> std::result::Result<(), Error> {
    let table = dml.table_id();
    if let Some(cas) = dml.cas() {
        if cas.table_schema_version != table_schema_version {
            return Err(Error::ConflictFound(entry_index));
        }

        if cas.unique_keys.iter().any(|key| key.index_id == index_id) {
            // Missing metadata means the predicate and the in-flight entry were
            // built against different schema state, so conflict conservatively.
            let Some(table_def) = storage.pico_table.get(table)? else {
                return Err(Error::ConflictFound(entry_index));
            };
            let Some(index_def) = storage.indexes.get(table, index_id)? else {
                return Err(Error::ConflictFound(entry_index));
            };

            let key_def = index_def.to_index_metadata(&table_def).to_key_def_for_key();
            let predicate_key = Tuple::new(predicate_key)?;
            for key in cas
                .unique_keys
                .iter()
                .filter(|key| key.index_id == index_id)
            {
                let key = Tuple::new(&key.key)?;
                if key_def.compare(&key, &predicate_key).is_eq() {
                    return Err(Error::ConflictFound(entry_index));
                }
            }
        }

        return Ok(());
    }

    // Entries without CAS metadata can only come from older code or from a mixed
    // version tail. DoFail/DoNothing inserts contain the only tuple they can
    // affect, so we can still check them precisely. Replace-style entries,
    // updates and deletes do not contain the old tuple and must conservatively
    // conflict with secondary predicates on the same table.
    match dml {
        Dml::Insert {
            tuple,
            conflict_strategy: ConflictStrategy::DoFail | ConflictStrategy::DoNothing,
            ..
        } => {
            let tuple = Tuple::new(tuple)?;
            // Missing metadata means the predicate and the in-flight entry were
            // built against different schema state, so conflict conservatively.
            let Some(table_def) = storage.pico_table.get(table)? else {
                return Err(Error::ConflictFound(entry_index));
            };
            let Some(index_def) = storage.indexes.get(table, index_id)? else {
                return Err(Error::ConflictFound(entry_index));
            };

            let key_def = index_def.to_index_metadata(&table_def).to_key_def();
            if key_def.compare_with_key(&tuple, predicate_key).is_eq() {
                return Err(Error::ConflictFound(entry_index));
            }
        }
        Dml::Insert {
            conflict_strategy: ConflictStrategy::DoReplace | ConflictStrategy::DoUpdate { .. },
            ..
        }
        | Dml::Replace { .. }
        | Dml::Update { .. }
        | Dml::Delete { .. } => {
            return Err(Error::ConflictFound(entry_index));
        }
    }

    Ok(())
}

fn check_schema_op_predicates(
    entry_index: RaftIndex,
    ddl: Option<&Ddl>,
    predicates: &CasPredicates,
) -> std::result::Result<(), Error> {
    check_property_key_predicates(entry_index, predicates, schema_related_property_keys())?;

    let Some(ddl) = ddl else {
        return Ok(());
    };

    match ddl_table_change(ddl) {
        Some(DdlTableChange::Table(table)) if predicates.contains_table(table) => {
            Err(Error::ConflictFound(entry_index))
        }
        Some(DdlTableChange::Index(table)) if predicates.contains_schema_dependent_table(table) => {
            Err(Error::ConflictFound(entry_index))
        }
        _ => Ok(()),
    }
}

fn check_plugin_predicates(
    entry_index: RaftIndex,
    predicates: &CasPredicates,
) -> std::result::Result<(), Error> {
    check_property_key_predicates(
        entry_index,
        predicates,
        std::slice::from_ref(pending_plugin_operation_key()),
    )
}

fn check_property_key_predicates(
    entry_index: RaftIndex,
    predicates: &CasPredicates,
    keys: &[Tuple],
) -> std::result::Result<(), Error> {
    let space = storage::Properties::TABLE_ID;
    let key_def = storage::cached_key_def_for_key(space, 0)?;

    for predicate in &predicates.0 {
        if let CasPredicate::PrimaryRange(range) = predicate {
            if range.table == space {
                for key in keys {
                    // NOTE: this is just a string comparison
                    if range.contains(&key_def, key) {
                        return Err(Error::ConflictFound(entry_index));
                    }
                }
            }
        }
    }

    Ok(())
}

enum DdlTableChange {
    Table(SpaceId),
    Index(SpaceId),
}

fn ddl_table_change(ddl: &Ddl) -> Option<DdlTableChange> {
    match ddl {
        Ddl::CreateTable { id, .. } | Ddl::DropTable { id, .. } | Ddl::TruncateTable { id, .. } => {
            Some(DdlTableChange::Table(*id))
        }
        Ddl::ChangeFormat { table_id, .. } | Ddl::RenameTable { table_id, .. } => {
            Some(DdlTableChange::Table(*table_id))
        }
        Ddl::CreateIndex { space_id, .. }
        | Ddl::DropIndex { space_id, .. }
        | Ddl::RenameIndex { space_id, .. } => Some(DdlTableChange::Index(*space_id)),
        Ddl::Backup { .. }
        | Ddl::CreateProcedure { .. }
        | Ddl::DropProcedure { .. }
        | Ddl::RenameProcedure { .. } => None,
    }
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
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, Eq)]
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
        if matches!(
            dml,
            Dml::Delete {
                metainfo: Some(_),
                ..
            }
        ) {
            return Ok(Self::new(table));
        }

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
            Op::DdlPrepare { .. } | Op::DdlCommit | Op::DdlAbort { .. } | Op::Acl(_) => {
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

#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize, tlua::LuaRead, PartialEq, Eq)]
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

////////////////////////////////////////////////////////////////////////////////
// test override
////////////////////////////////////////////////////////////////////////////////

/// A channel for CAS requests which is used for the mock implementation in tests.
pub type OverrideChannel = Mailbox<(Request, oneshot::Sender<Result<Response>>)>;

#[inline(never)]
#[cold]
fn handle_test_override(inbox: &OverrideChannel, request: &Request) -> Result<CasResult> {
    let (tx, rx) = oneshot::channel();
    inbox.send((request.clone(), tx));
    let res = match fiber::block_on(rx) {
        Ok(v) => v,
        Err(e) => panic!("channel sender dropped unexpectedly: {e}"),
    };
    let response = match res {
        Ok(v) => v,
        Err(e) => {
            if e.is_retriable() {
                return Ok(CasResult::RetriableError(e));
            } else {
                return Err(e);
            }
        }
    };
    Ok(CasResult::Ok((
        response.index,
        response.term,
        response.res_row_count,
    )))
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

/// Predicate tests based on the CaS Design Document.
mod tests {
    use sql::ir::operator::ConflictStrategy;
    use tarantool::index::{FieldType as IndexFieldType, IndexId, IndexType, Part};
    use tarantool::space::{
        Field, FieldType as SpaceFieldType, SpaceEngineType, SpaceType, UpdateOps,
    };
    use tarantool::tuple::ToTupleBuffer;

    use crate::catalog::pico_table::PicoTable;
    use crate::schema::{Distribution, IndexDef, IndexOption, TableDef, ADMIN_ID};
    use crate::storage::SystemTable as _;
    use crate::storage::{Catalog, Properties, PropertyName};
    use crate::traft::op::DdlBuilder;

    use super::*;

    #[::tarantool::test]
    fn ddl() {
        let storage = Catalog::for_tests();

        let t = |op: &Op, range: Range| -> std::result::Result<(), Error> {
            check_predicate(2, op, &predicates_from_ranges([range]), &storage)
        };

        let builder = DdlBuilder::with_schema_version(1);

        let table_def = TableDef::for_tests();
        let space_name = &table_def.name;
        let space_id = table_def.id;
        let index_id = 1;

        let create_space = builder.with_op(Ddl::CreateTable {
            id: space_id,
            name: space_name.clone(),
            format: vec![],
            primary_key: vec![],
            distribution: Distribution::Global,
            engine: SpaceEngineType::Memtx,
            owner: ADMIN_ID,
            opts: vec![],
            index_opts: vec![],
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

        let test = |op: &Dml, range: Range| {
            check_predicate(
                2,
                &Op::Dml(op.clone()),
                &predicates_from_ranges([range]),
                &storage,
            )
        };

        let table = PicoTable::TABLE_ID;
        let ops = &[
            Dml::Insert {
                table,
                tuple: tuple.clone(),
                initiator: ADMIN_ID,
                conflict_strategy: ConflictStrategy::DoFail,
                cas: None,
            },
            Dml::Replace {
                table,
                tuple,
                initiator: ADMIN_ID,
                cas: None,
            },
            Dml::Update {
                table,
                key: key.clone(),
                ops: vec![],
                initiator: ADMIN_ID,
                cas: None,
            },
            Dml::Delete {
                table,
                key: key.clone(),
                initiator: ADMIN_ID,
                metainfo: None,
                cas: None,
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

    #[::tarantool::test]
    fn cas_extracts_unique_keys_from_table_metadata() {
        let storage = Catalog::for_tests();
        let mut table = TableDef::for_tests();
        table.id = 50_001;
        table.name = "cas_keys_test".into();
        table.schema_version = 77;
        storage.pico_table.put(&table).unwrap();

        let index = IndexDef {
            table_id: table.id,
            id: 1,
            name: "secondary_unique".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![Part::from(("field_1", IndexFieldType::Unsigned)).is_nullable(false)],
            operable: true,
            schema_version: table.schema_version,
        };
        storage.indexes.put(&index).unwrap();
        let duplicate_index = IndexDef {
            table_id: table.id,
            id: 2,
            name: "secondary_unique_dup".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![Part::from(("field_1", IndexFieldType::Unsigned)).is_nullable(false)],
            operable: true,
            schema_version: table.schema_version,
        };
        storage.indexes.put(&duplicate_index).unwrap();

        let tuple = Tuple::new(&(7,)).unwrap();
        let mut cas = DmlCas::default();
        add_tuple_cas_keys(&storage, table.id, &mut cas, &tuple).unwrap();

        assert_eq!(cas.table_schema_version, table.schema_version);
        assert!(!cas.table_full_change);
        assert_eq!(cas.unique_keys.len(), 2);

        let mut index_ids = cas
            .unique_keys
            .iter()
            .map(|key| key.index_id)
            .collect::<Vec<_>>();
        index_ids.sort_unstable();
        assert_eq!(index_ids, [1, 2]);
        for key in &cas.unique_keys {
            assert_eq!(key.key, (7,).to_tuple_buffer().unwrap());
        }

        storage.indexes.delete(table.id, index.id).unwrap();
        storage
            .indexes
            .delete(table.id, duplicate_index.id)
            .unwrap();
        storage.pico_table.delete(table.id).unwrap();
    }

    #[::tarantool::test]
    fn cas_adds_candidate_unique_keys_for_insert() {
        let storage = Catalog::for_tests();
        let table = 50_007;
        put_unique_secondary_index(&storage, table, 10, 1);

        let op = Op::Dml(Dml::Insert {
            table,
            tuple: (10,).to_tuple_buffer().unwrap(),
            initiator: ADMIN_ID,
            conflict_strategy: ConflictStrategy::DoFail,
            cas: None,
        });
        let mut predicates = CasPredicates::default();

        add_candidate_cas_keys(&storage, &op, &mut predicates);

        assert!(predicates.0.contains(&CasPredicate::UniqueKey {
            table,
            table_schema_version: 10,
            index_id: 1,
            key: (10,).to_tuple_buffer().unwrap(),
        }));
    }

    #[::tarantool::test]
    fn cas_candidate_key_error_adds_conservative_predicate() {
        let storage = Catalog::for_tests();
        let table_id = 50_016;
        let mut table = TableDef::for_tests();
        table.id = table_id;
        table.name = "cas_candidate_key_error".into();
        table.schema_version = 91;
        table.format = vec![
            Field::from(("id", SpaceFieldType::Unsigned)).is_nullable(false),
            Field::from(("u", SpaceFieldType::Unsigned)).is_nullable(false),
        ];
        storage.pico_table.put(&table).unwrap();

        let index = IndexDef {
            table_id,
            id: 1,
            name: "u".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![Part::from(("u", IndexFieldType::Unsigned)).is_nullable(false)],
            operable: true,
            schema_version: table.schema_version,
        };
        storage.indexes.put(&index).unwrap();

        let op = Op::Dml(Dml::Insert {
            table: table_id,
            tuple: (1_u64, Option::<u64>::None).to_tuple_buffer().unwrap(),
            initiator: ADMIN_ID,
            conflict_strategy: ConflictStrategy::DoFail,
            cas: None,
        });
        let mut predicates = CasPredicates::default();

        add_candidate_cas_keys(&storage, &op, &mut predicates);

        assert!(predicates
            .0
            .contains(&CasPredicate::TableFullChange(table_id)));

        storage.indexes.delete(table_id, index.id).unwrap();
        storage.pico_table.delete(table_id).unwrap();
    }

    #[::tarantool::test]
    fn cas_prepare_error_adds_conservative_predicates_for_update_delete() {
        let storage = Catalog::for_tests();
        let update_table = 50_012;
        let delete_table = 50_013;
        let insert_table = 50_014;
        let op = Op::BatchDml {
            ops: vec![
                Dml::Update {
                    table: update_table,
                    key: (1,).to_tuple_buffer().unwrap(),
                    ops: vec![],
                    initiator: ADMIN_ID,
                    cas: None,
                },
                Dml::Delete {
                    table: delete_table,
                    key: (1,).to_tuple_buffer().unwrap(),
                    initiator: ADMIN_ID,
                    metainfo: None,
                    cas: None,
                },
                Dml::Insert {
                    table: insert_table,
                    tuple: (1,).to_tuple_buffer().unwrap(),
                    initiator: ADMIN_ID,
                    conflict_strategy: ConflictStrategy::DoFail,
                    cas: None,
                },
            ],
        };
        let mut predicates = CasPredicates::default();

        add_prepare_error_cas_predicates(&op, &mut predicates);

        assert!(predicates
            .0
            .contains(&CasPredicate::TableFullChange(update_table)));
        assert!(predicates
            .0
            .contains(&CasPredicate::TableFullChange(delete_table)));
        assert!(!predicates
            .0
            .contains(&CasPredicate::TableFullChange(insert_table)));

        let entry = Dml::Insert {
            table: update_table,
            tuple: (2,).to_tuple_buffer().unwrap(),
            initiator: ADMIN_ID,
            conflict_strategy: ConflictStrategy::DoFail,
            cas: None,
        };
        check_predicate(42, &Op::Dml(entry), &predicates, &storage).unwrap_err();
    }

    #[::tarantool::test]
    fn cas_prepare_error_update_unique_conflict_checks_tail_conservatively() {
        let storage = Catalog::for_tests();
        let table_id = 50_015;
        let space_name = format!("cas_update_error_{table_id}");

        if let Some(space) = Space::find(&space_name) {
            space.drop().unwrap();
        }

        let space = Space::builder(&space_name)
            .id(table_id)
            .space_type(SpaceType::Temporary)
            .engine(SpaceEngineType::Memtx)
            .format([
                Field::from(("id", SpaceFieldType::Unsigned)).is_nullable(false),
                Field::from(("u", SpaceFieldType::Unsigned)).is_nullable(false),
            ])
            .create()
            .unwrap();
        space
            .index_builder("primary")
            .id(0)
            .unique(true)
            .part("id")
            .create()
            .unwrap();
        space
            .index_builder("u")
            .id(1)
            .unique(true)
            .part("u")
            .create()
            .unwrap();
        space.insert(&(1_u64, 10_u64)).unwrap();
        space.insert(&(2_u64, 20_u64)).unwrap();

        let mut table = TableDef::for_tests();
        table.id = table_id;
        table.name = space_name.clone().into();
        table.schema_version = 89;
        table.engine = SpaceEngineType::Memtx;
        table.format = vec![
            Field::from(("id", SpaceFieldType::Unsigned)).is_nullable(false),
            Field::from(("u", SpaceFieldType::Unsigned)).is_nullable(false),
        ];
        storage.pico_table.put(&table).unwrap();

        let index = IndexDef {
            table_id,
            id: 1,
            name: "u".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![Part::from(("u", IndexFieldType::Unsigned)).is_nullable(false)],
            operable: true,
            schema_version: table.schema_version,
        };
        storage.indexes.put(&index).unwrap();

        let mut ops = UpdateOps::new();
        ops.assign(1, 10_u64).unwrap();
        let op = Op::Dml(Dml::Update {
            table: table_id,
            key: (2_u64,).to_tuple_buffer().unwrap(),
            ops: ops.into(),
            initiator: ADMIN_ID,
            cas: None,
        });

        let mut predicates = CasPredicates::default();
        predicates.extend_ranges(Range::for_op(&op).unwrap());

        let tail_entry = Op::Dml(Dml::Insert {
            table: table_id,
            tuple: (1_u64, 10_u64).to_tuple_buffer().unwrap(),
            initiator: ADMIN_ID,
            conflict_strategy: ConflictStrategy::DoFail,
            cas: dml_cas(table.schema_version, index.id, 10),
        });
        check_predicate(42, &tail_entry, &predicates, &storage).unwrap();

        let prepare_res = prepare_proposal(&storage, &op);
        assert!(prepare_res.is_err());

        add_candidate_cas_keys(&storage, &op, &mut predicates);
        add_prepare_error_cas_predicates(&op, &mut predicates);
        check_predicate(42, &tail_entry, &predicates, &storage).unwrap_err();

        storage.indexes.delete(table_id, index.id).unwrap();
        storage.pico_table.delete(table_id).unwrap();
        space.drop().unwrap();
    }

    #[::tarantool::test]
    fn cas_records_unique_keys_for_inoperable_unique_index() {
        let storage = Catalog::for_tests();
        let table = 50_009;
        put_unique_secondary_index_with_operable(&storage, table, 10, 1, false);

        let tuple = Tuple::new(&(10,)).unwrap();
        let mut cas = DmlCas::default();
        add_tuple_cas_keys(&storage, table, &mut cas, &tuple).unwrap();

        assert!(cas.unique_keys.contains(&CasKey {
            index_id: 1,
            key: (10,).to_tuple_buffer().unwrap(),
        }));
    }

    #[::tarantool::test]
    fn cas_records_unique_keys_for_do_nothing_insert() {
        let storage = Catalog::for_tests();
        let table_id = 50_008;
        let space_name = format!("cas_do_nothing_{table_id}");

        if let Some(space) = Space::find(&space_name) {
            space.drop().unwrap();
        }

        let space = Space::builder(&space_name)
            .id(table_id)
            .space_type(SpaceType::Temporary)
            .engine(SpaceEngineType::Memtx)
            .format([
                Field::from(("id", SpaceFieldType::Unsigned)).is_nullable(false),
                Field::from(("name", SpaceFieldType::String)).is_nullable(false),
            ])
            .create()
            .unwrap();
        space
            .index_builder("primary")
            .id(0)
            .unique(true)
            .part("id")
            .create()
            .unwrap();
        space
            .index_builder("name")
            .id(1)
            .unique(true)
            .part("name")
            .create()
            .unwrap();
        space.insert(&(1_u64, "same")).unwrap();

        let mut table = TableDef::for_tests();
        table.id = table_id;
        table.name = space_name.clone().into();
        table.schema_version = 88;
        table.engine = SpaceEngineType::Memtx;
        table.format = vec![
            Field::from(("id", SpaceFieldType::Unsigned)).is_nullable(false),
            Field::from(("name", SpaceFieldType::String)).is_nullable(false),
        ];
        storage.pico_table.put(&table).unwrap();

        let index = IndexDef {
            table_id,
            id: 1,
            name: "name".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![Part::from(("name", IndexFieldType::String)).is_nullable(false)],
            operable: true,
            schema_version: table.schema_version,
        };
        storage.indexes.put(&index).unwrap();

        let dml = Dml::Insert {
            table: table_id,
            tuple: (2_u64, "same").to_tuple_buffer().unwrap(),
            initiator: ADMIN_ID,
            conflict_strategy: ConflictStrategy::DoNothing,
            cas: None,
        };

        transaction::begin().unwrap();
        let res = prepare_dml_proposal(&storage, &dml);
        transaction::rollback().unwrap();

        let (res, cas) = res.unwrap();
        assert!(res.is_none());
        assert_eq!(
            cas,
            DmlCas {
                table_schema_version: table.schema_version,
                unique_keys: vec![CasKey {
                    index_id: 1,
                    key: ("same",).to_tuple_buffer().unwrap(),
                }],
                table_full_change: false,
            }
        );

        storage.indexes.delete(table_id, index.id).unwrap();
        storage.pico_table.delete(table_id).unwrap();
        space.drop().unwrap();
    }

    #[::tarantool::test]
    fn cas_conflicts_on_same_unique_key() {
        let storage = Catalog::for_tests();
        let table = 50_002;
        put_unique_secondary_index(&storage, table, 10, 1);

        let entry = Dml::Update {
            table,
            key: (1,).to_tuple_buffer().unwrap(),
            ops: vec![],
            initiator: ADMIN_ID,
            cas: dml_cas(10, 1, 10),
        };
        let request = Dml::Update {
            table,
            key: (2,).to_tuple_buffer().unwrap(),
            ops: vec![],
            initiator: ADMIN_ID,
            cas: dml_cas(10, 1, 10),
        };
        let predicates = predicates_from_op(&Op::Dml(request));

        check_predicate(42, &Op::Dml(entry), &predicates, &storage).unwrap_err();
    }

    #[::tarantool::test]
    fn cas_allows_different_unique_key() {
        let storage = Catalog::for_tests();
        let table = 50_003;
        put_unique_secondary_index(&storage, table, 10, 1);

        let entry = Dml::Update {
            table,
            key: (1,).to_tuple_buffer().unwrap(),
            ops: vec![],
            initiator: ADMIN_ID,
            cas: dml_cas(10, 1, 11),
        };
        let request = Dml::Update {
            table,
            key: (2,).to_tuple_buffer().unwrap(),
            ops: vec![],
            initiator: ADMIN_ID,
            cas: dml_cas(10, 1, 12),
        };
        let predicates = predicates_from_op(&Op::Dml(request));

        check_predicate(42, &Op::Dml(entry), &predicates, &storage).unwrap();
    }

    #[::tarantool::test]
    fn cas_conflicts_on_unique_key_schema_version_mismatch() {
        let storage = Catalog::for_tests();
        let table = 50_006;

        let entry = Dml::Update {
            table,
            key: (1,).to_tuple_buffer().unwrap(),
            ops: vec![],
            initiator: ADMIN_ID,
            cas: dml_cas(11, 1, 10),
        };
        let request = Dml::Update {
            table,
            key: (2,).to_tuple_buffer().unwrap(),
            ops: vec![],
            initiator: ADMIN_ID,
            cas: dml_cas(10, 1, 10),
        };
        let predicates = predicates_from_op(&Op::Dml(request));

        check_predicate(42, &Op::Dml(entry), &predicates, &storage).unwrap_err();
    }

    #[::tarantool::test]
    fn cas_conflicts_with_old_update_without_cas() {
        let storage = Catalog::for_tests();
        let table = 50_004;

        let entry = Dml::Update {
            table,
            key: (1,).to_tuple_buffer().unwrap(),
            ops: vec![],
            initiator: ADMIN_ID,
            cas: None,
        };
        let request = Dml::Update {
            table,
            key: (2,).to_tuple_buffer().unwrap(),
            ops: vec![],
            initiator: ADMIN_ID,
            cas: dml_cas(10, 1, 10),
        };
        let predicates = predicates_from_op(&Op::Dml(request));

        check_predicate(42, &Op::Dml(entry), &predicates, &storage).unwrap_err();
    }

    #[::tarantool::test]
    fn cas_conflicts_with_old_replace_without_cas() {
        let storage = Catalog::for_tests();
        let table = 50_010;

        let entry = Dml::Replace {
            table,
            tuple: (1, 12).to_tuple_buffer().unwrap(),
            initiator: ADMIN_ID,
            cas: None,
        };
        let request = Dml::Update {
            table,
            key: (2,).to_tuple_buffer().unwrap(),
            ops: vec![],
            initiator: ADMIN_ID,
            cas: dml_cas(10, 1, 10),
        };
        let predicates = predicates_from_op(&Op::Dml(request));

        check_predicate(42, &Op::Dml(entry), &predicates, &storage).unwrap_err();
    }

    #[::tarantool::test]
    fn cas_conflicts_with_old_do_replace_insert_without_cas() {
        let storage = Catalog::for_tests();
        let table = 50_011;

        let entry = Dml::Insert {
            table,
            tuple: (1, 12).to_tuple_buffer().unwrap(),
            initiator: ADMIN_ID,
            conflict_strategy: ConflictStrategy::DoReplace,
            cas: None,
        };
        let request = Dml::Update {
            table,
            key: (2,).to_tuple_buffer().unwrap(),
            ops: vec![],
            initiator: ADMIN_ID,
            cas: dml_cas(10, 1, 10),
        };
        let predicates = predicates_from_op(&Op::Dml(request));

        check_predicate(42, &Op::Dml(entry), &predicates, &storage).unwrap_err();
    }

    #[::tarantool::test]
    fn cas_conflicts_with_index_ddl_on_same_table() {
        let storage = Catalog::for_tests();
        let table = 50_005;
        let request = Dml::Update {
            table,
            key: (1,).to_tuple_buffer().unwrap(),
            ops: vec![],
            initiator: ADMIN_ID,
            cas: dml_cas(10, 1, 10),
        };
        let predicates = predicates_from_op(&Op::Dml(request));

        let builder = DdlBuilder::with_schema_version(11);
        let same_table_ddl = builder.with_op(Ddl::CreateIndex {
            space_id: table,
            index_id: 1,
            name: "secondary_unique".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            by_fields: vec![],
            initiator: ADMIN_ID,
        });
        let other_table_ddl = builder.with_op(Ddl::CreateIndex {
            space_id: table + 1,
            index_id: 1,
            name: "secondary_unique".into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            by_fields: vec![],
            initiator: ADMIN_ID,
        });

        check_predicate(42, &same_table_ddl, &predicates, &storage).unwrap_err();
        check_predicate(42, &other_table_ddl, &predicates, &storage).unwrap();
    }

    #[::tarantool::test]
    fn cas_full_table_change_conflicts_with_same_table_dml() {
        let storage = Catalog::for_tests();
        let table = PicoTable::TABLE_ID;
        let entry = Dml::Insert {
            table,
            tuple: (1, "one").to_tuple_buffer().unwrap(),
            initiator: ADMIN_ID,
            conflict_strategy: ConflictStrategy::DoFail,
            cas: None,
        };
        let mut predicates = CasPredicates::default();
        predicates.add(CasPredicate::TableFullChange(table));

        check_predicate(42, &Op::Dml(entry), &predicates, &storage).unwrap_err();
    }

    fn predicates_from_ranges(ranges: impl IntoIterator<Item = Range>) -> CasPredicates {
        let mut predicates = CasPredicates::default();
        predicates.extend_ranges(ranges);
        predicates
    }

    fn predicates_from_op(op: &Op) -> CasPredicates {
        let mut predicates = CasPredicates::default();
        predicates.extend_dml_cas(op);
        predicates
    }

    fn put_unique_secondary_index(
        storage: &Catalog,
        table_id: SpaceId,
        table_schema_version: u64,
        index_id: IndexId,
    ) {
        put_unique_secondary_index_with_operable(
            storage,
            table_id,
            table_schema_version,
            index_id,
            true,
        );
    }

    fn put_unique_secondary_index_with_operable(
        storage: &Catalog,
        table_id: SpaceId,
        table_schema_version: u64,
        index_id: IndexId,
        operable: bool,
    ) {
        let mut table = TableDef::for_tests();
        table.id = table_id;
        table.name = format!("cas_keys_test_{table_id}").into();
        table.schema_version = table_schema_version;
        storage.pico_table.put(&table).unwrap();

        let index = IndexDef {
            table_id,
            id: index_id,
            name: format!("secondary_unique_{index_id}").into(),
            ty: IndexType::Tree,
            opts: vec![IndexOption::Unique(true)],
            parts: vec![Part::from(("field_1", IndexFieldType::Unsigned)).is_nullable(false)],
            operable,
            schema_version: table_schema_version,
        };
        storage.indexes.put(&index).unwrap();
    }

    fn dml_cas(table_schema_version: u64, index_id: IndexId, value: u64) -> Option<DmlCas> {
        Some(DmlCas {
            table_schema_version,
            unique_keys: vec![CasKey {
                index_id,
                key: (value,).to_tuple_buffer().unwrap(),
            }],
            table_full_change: false,
        })
    }

    #[tarantool::test]
    fn validate_check_dml_prohibited() {
        let storage = Catalog::for_tests();

        {
            let error = check_dml_prohibited(
                &storage,
                &Dml::Delete {
                    table: storage::DbConfig::TABLE_ID,
                    key: (1,).to_tuple_buffer().unwrap(),
                    initiator: ADMIN_ID,
                    metainfo: None,
                    cas: None,
                },
            )
            .unwrap_err();
            assert_eq!(
                error.to_string(),
                format!(
                    "DELETE on table '{}' is denied for all users",
                    storage::DbConfig::TABLE_NAME
                )
            );
        }
    }
}
