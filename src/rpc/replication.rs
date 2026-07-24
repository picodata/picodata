//! # The wait_index policy
//!
//! Note that stored procedures in this module do not do [`Node::wait_index`].
//! This is different from all other stored procedures called by governor in
//! [`governor::Loop::governor_loop`]
//! (for example [`rpc::sharding::proc_sharding`] calls wait_index at the
//! start and so do most others procs).
//!
//! The surface-level reason for this difference is that raft_main_loop
//! ([`NodeImpl::advance`]) in some cases needs the tarantool replication
//! to be configured before it can advance the raft replication (which advances
//! the applied index which `wait_index` is waiting for). The specific place
//! where we the deadlock will happen is [`NodeImpl::prepare_for_snapshot`]
//! (see the "awaiting replication" status).
//!
//! The deeper reason is how our DDL is implemented. We have a
//! [`Op::DdlPrepare`] raft operation, which when applied in
//! [`NodeImpl::handle_committed_normal_entry`] is handled differently on
//! replicaset master vs read-only replica. Masters apply the changes (create
//! table, etc.) directly to the storage engine, while the read-only replicas
//! are simply waiting for the master to transfer to them the storage state via
//! tarantool replication. This means that raft_main_loop in some cases depends
//! on the tarantool replication being configured and hence tarantool
//! replication configuring ([`proc_replication`] is responsible for this) must
//! not depend on raft_main_loop ([`Node::wait_index`]).
//!
//! As for [`proc_replication_sync`] and [`proc_replication_demote`], they also
//! must not depend on `wait_index`, for a related reason. These procs are part
//! of replicaset master switchover step of the governor loop
//! (see [`plan::stage::Plan::ReplicasetMasterConsistentSwitchover`]).
//! And this step must also be done before we can advance the raft_main_loop,
//! because otherwise the instance would not know if it should apply the DDL
//! itself or wait for the tarantool replication.
//!
use crate::catalog::pico_table::PicoTable;
use crate::config::{PicodataConfig, ReplicationMode, DEFAULT_REPLICATION_MODE};
use crate::error_code::ErrorCode;
#[allow(unused_imports)]
use crate::governor;
#[allow(unused_imports)]
use crate::governor::plan;
use crate::has_states;
use crate::luamod::lua_function;
#[allow(unused_imports)]
use crate::rpc;
use crate::sync::wait_vclock;
use crate::tarantool::{box_promote, box_ro_reason, set_cfg_field, ListenConfig};
use crate::tlog;
use crate::traft::error::Error;
#[allow(unused_imports)]
use crate::traft::node::{Node, NodeImpl};
#[allow(unused_imports)]
use crate::traft::op::Op;
use crate::traft::{node, RaftTerm, Result};
use smol_str::SmolStr;
use std::cell::Cell;
use std::time::Duration;
use tarantool::clock::INFINITY;
use tarantool::error::{BoxError, Error as TarantoolError, TarantoolErrorCode};
use tarantool::index::IteratorType;
use tarantool::space::{Space, SystemSpace};
use tarantool::tlua::Object;
use tarantool::tlua::StringInLua;
use tarantool::transaction::transaction_force_async;
use tarantool::vclock::Vclock;

crate::define_rpc_request! {
    /// Configures replication on the target replica.
    /// Specifies addresses of all the replicas in the replicaset
    /// and whether the target instance should be a replicaset master.
    ///
    /// Returns errors in the following cases:
    /// 1. Lua error during call to `box.cfg`
    /// 2. Storage failure
    fn proc_replication(req: ConfigureReplicationRequest) -> Result<Response> {
        let node = node::global()?;
        // Must not call node.wait_index(...) here. See doc-comments at the top
        // of the file for explanation.
        node.status().check_term(req.term)?;

        // TODO: check this configuration is newer then the one currently
        // applied. For this we'll probably need to store the governor's applied
        // index at the moment of request generation in box.space._schema on the
        // requestee. And if the new request has index less then the one in our
        // _schema, then we ignore it.

        if check_if_replication_is_broken(node).is_err() {
            // reset replication if it is broken
            tlog!(Error, "replication is broken, trying to restart it...");
            let replication_cfg: Vec<String> = vec![];
            set_cfg_field("replication", &replication_cfg)?;
        }

        let mut replication_cfg = Vec::with_capacity(req.replicaset_peers.len());
        let tls_config = &PicodataConfig::get().instance.iproto.tls;
        for address in &req.replicaset_peers {
            replication_cfg.push(ListenConfig::new_for_pico_service(address, tls_config));
        }

        crate::error_injection!("BROKEN_REPLICATION" => { replication_cfg.clear(); });

        // box.cfg checks if the replication is already the same
        // and ignores it if nothing changed
        set_cfg_field("replication", &replication_cfg)?;

        let (replication_mode, replication_factor) = get_this_tier_replication_mode_and_factor()?;
        let synchronous_replication_enabled = replication_mode.is_sync();

        if synchronous_replication_enabled {
            set_cfg_field("replication_synchro_quorum", replication_factor / 2 + 1)?;
            set_cfg_field("replication_synchro_timeout", INFINITY.as_secs())?;
            set_cfg_field("election_mode", "manual")?;
            // Effective read-onlyness is supplied by the election role.
            set_cfg_field("read_only", false)?;
        } else if req.is_master {
            set_read_only(false)?;
        } else {
            // Everybody else should be read-only
            set_read_only(true)?;
        }

        if !node.is_readonly() {
            // _cluster is replicated from master to replicas, so we need to
            // update it on the master only.
            // Errors are not fatal here, we do not need to stop the process,
            // because cleaning up _cluster is not critical and we can do
            // it later.
            if let Err(e) = update_sys_cluster() {
                tlog!(Error, "failed to update _cluster: {e}");
            }
        }

        Ok(Response {})
    }

    /// Request to configure tarantool replication.
    pub struct ConfigureReplicationRequest {
        pub term: RaftTerm,
        /// If this is `true` the target replica will become the new `master`.
        /// See [tarantool documentation](https://www.tarantool.io/en/doc/latest/reference/configuration/#cfg-basic-read-only)
        /// for more.
        pub is_master: bool,
        /// URIs of all replicas in the replicaset.
        /// See [tarantool documentation](https://www.tarantool.io/en/doc/latest/reference/configuration/#confval-replication)
        /// for more.
        pub replicaset_peers: Vec<SmolStr>,
    }

    /// Response to [`ConfigureReplicationRequest`].
    pub struct Response {}
}

crate::define_rpc_request! {
    /// Waits until instance synchronizes tarantool replication.
    fn proc_replication_sync(req: ReplicationSyncRequest) -> Result<ReplicationSyncResponse> {
        let node = node::global()?;
        // Must not call node.wait_index(...) here. See doc-comments at the top
        // of the file for explanation.
        node.status().check_term(req.term)?;

        debug_assert!(node.is_readonly());

        crate::error_injection!("TIMEOUT_WHEN_SYNCHING_BEFORE_PROMOTION_TO_MASTER" => return Err(Error::timeout()));

        // If replication is broken, we don't care about the vclock, exit ASAP
        check_if_replication_is_broken(node)?;

        // Wait until replication progresses.
        let my_vclock = wait_vclock(node, &req.vclock, req.timeout)?;
        let my_vclock = my_vclock.ignore_zero();

        Ok(ReplicationSyncResponse {
            vclock: Some(my_vclock),
        })
    }

    /// Request to wait until instance synchronizes tarantool replication.
    pub struct ReplicationSyncRequest {
        /// Current term of the sender.
        pub term: RaftTerm,

        /// Wait until instance progresses replication past this vclock value.
        pub vclock: Vclock,

        /// Wait for this long.
        pub timeout: Duration,
    }

    pub struct ReplicationSyncResponse {
        #[serde(default)]
        pub vclock: Option<Vclock>,
    }
}

#[track_caller]
pub fn check_if_replication_is_broken(node: &Node) -> Result<()> {
    // `pico._check_if_replication_is_broken` is defined in `src/luamod.lua`
    let func = lua_function("_check_if_replication_is_broken")?;
    let result: Option<Object<_>> = func.into_call()?;

    let mut instance_reachability = node.instance_reachability.borrow_mut();

    let Some(values) = result else {
        instance_reachability.reset_replication_error(node.raft_id);
        return Ok(());
    };

    let instance_uuid: StringInLua<_> = values.read(0)?;
    let status: StringInLua<_> = values.read(1)?;
    let message: StringInLua<_> = values.read(2)?;

    let instance_id;
    let topology_ref = node.topology_cache.get();
    if let Ok(instance) = topology_ref.instance_by_uuid(&instance_uuid) {
        instance_id = &*instance.name;
    } else {
        instance_id = &*instance_uuid;
    }

    let err = replication_broken(instance_id, &status, &message);
    instance_reachability.set_replication_error(node.raft_id, &err);

    tlog!(Error, "replication is broken: {err}");

    return Err(err.into());
}

#[track_caller]
#[inline]
fn replication_broken(instance_id: &str, status: &str, message: &str) -> BoxError {
    return BoxError::new(
        ErrorCode::ReplicationBroken,
        format!("upstream from {instance_id} is {status}: {message}"),
    );
}

// Updates space._cluster to remove expelled or non-existing instances.
// Tarantool uses this space to store replication configuration.
// Tarantool only adds new instances to this space, but never removes them.
// So we have to do it manually here.
fn update_sys_cluster() -> Result<()> {
    let sys_cluster = Space::from(SystemSpace::Cluster);
    let node = node::global()?;
    let topology_ref = node.topology_cache.get();

    let mut ids_to_delete = Vec::new();
    for tuple in sys_cluster.select(IteratorType::All, &())? {
        let instance_id: u32 = tuple.field(0)?.ok_or_else(|| {
            Error::other("failed to decode 'instance_id' field of table _cluster")
        })?;
        let instance_uuid: &str = tuple.field(1)?.ok_or_else(|| {
            Error::other("failed to decode 'instance_uuid' field of table _cluster")
        })?;
        let Ok(instance) = topology_ref.instance_by_uuid(instance_uuid) else {
            tlog!(Warning, "instance with uuid {instance_uuid} not found in _pico_instance, but there's a record for it in _cluster. Not doing anything for now");
            // Note: we cannot remove instance from _cluster if it's not in
            // _pico_instance, because in some cases instances will be added
            // into _cluster before _pico_instance. This happens for example
            // when a replicaset is joining a cluster in which a DDL operation
            // was performed, because _pico_instance is a global table it will
            // be updated only after the DDL was applied, but that requires
            // tarantool replication to be configured and hence records being
            // added to _cluster.
            //
            // This also means that we cannot remove Expelled instance records
            // from _pico_instance before they're removed from _cluster, as that
            // would leave _cluster broken without a way to know when to remove
            // these records...
            //
            // Although now that I think about it there is a way to know, it's
            // just that proc_replication is called without wait_index and
            // because of that it has outdated _pico_instance contents. So the
            // solution is to do an explicit request to update _cluster which
            // involves wait_index, that way it would be safe to remove instance
            // from _cluster if it's not in _pico_instance.
            continue;
        };
        if has_states!(instance, Expelled -> *) {
            tlog!(
                Debug,
                "instance with uuid {instance_uuid} is expelled, removing it from _cluster"
            );
            ids_to_delete.push(instance_id);
        }
    }
    // Must not hold this reference across yields
    drop(topology_ref);

    // All operations on _cluster are done in a single transaction
    // to be written to WAL in one batch.
    // Make asynchronous intentionally because instance
    // which we want to delete may be dead and cannot
    // acknowledge the transaction about this deletion.
    transaction_force_async(|| -> Result<()> {
        for id in ids_to_delete {
            sys_cluster.delete(&[id])?;
        }
        Ok(())
    })?;

    Ok(())
}

/// Changes the current instance's read-only parameter.
/// See [tarantool documentation](https://www.tarantool.io/en/doc/latest/reference/configuration/#cfg-basic-read-only)
/// for more.
///
/// Calls the [`Service::on_leader_change`] callbacks if the parameter actually
/// changed.
///
/// [`Service::on_leader_change`]: picodata_plugin::plugin::interface::Service::on_leader_change
pub fn set_read_only(new_read_only: bool) -> Result<()> {
    let node = node::global()?;
    // XXX: Currently we just change the box.cfg.read_only option of the
    // instance but at some point we will implement support for
    // tarantool synchronous transactions then this operation will probably
    // become more involved.
    let old_read_only = node.is_readonly();

    set_cfg_field("read_only", new_read_only)?;

    if !new_read_only {
        #[rustfmt::skip]
        if let Some(ro_reason) = box_ro_reason() {
            tlog!(Warning, "failed to promote self to replication leader, reason = {ro_reason}");
            return Err(Error::other(format!("instance is still in read only mode: {ro_reason}")));
        };
    } else {
        truncate_unlogged_tables()?;
    }

    if old_read_only != new_read_only {
        call_plugin_leader_change_callbacks(node);
    }

    Ok(())
}

/// Truncates all unlogged tables. Must happen whenever this instance stops
/// being the replicaset master, so that the data written to unlogged tables
/// while it was the master doesn't resurface if it becomes the master again.
fn truncate_unlogged_tables() -> Result<()> {
    // Make asynchronous intentionally because do not want to block
    // "configure replication" step due to synchronous transactions.
    transaction_force_async(|| -> Result<()> {
        let pico_table = PicoTable::new();
        pico_table.truncate_unlogged_tables()?;
        Ok(())
    })?;
    Ok(())
}

/// Notifies plugins of a replicaset leadership change by calling their
/// [`Service::on_leader_change`] callbacks.
///
/// [`Service::on_leader_change`]: picodata_plugin::plugin::interface::Service::on_leader_change
fn call_plugin_leader_change_callbacks(node: &Node) {
    // errors ignored because it must be already handled by plugin manager itself
    let res = node.plugin_manager.handle_replicaset_leader_change();
    if let Err(e) = res {
        tlog!(Error, "on_leader_change error: {e}");
    }
}

thread_local! {
    /// The role for which [`handle_election_leader_change`] last ran the
    /// leadership-transition side effects.
    static LAST_HANDLED_IS_LEADER: Cell<bool> = Cell::default();
}

/// The single idempotent role-transition handler for election-driven
/// (sync-tier) replicaset leadership changes:
///
/// - on losing leadership the unlogged tables are truncated, so that the data
///   written while this instance was the master doesn't resurface if it wins
///   an election later (e.g. it was fenced on quorum loss and re-promoted);
/// - in both directions plugins are notified via their `on_leader_change`
///   callbacks, so they don't retain a stale `is_master` state.
pub(crate) fn handle_election_leader_change(is_leader: bool) -> Result<()> {
    let node = node::global()?;

    if LAST_HANDLED_IS_LEADER.get() == is_leader {
        return Ok(());
    }

    if is_leader {
        tlog!(
            Info,
            "gained replicaset leadership in a tarantool raft election"
        );
    } else {
        tlog!(
            Info,
            "lost replicaset leadership, truncating unlogged tables"
        );
        // On error the handled role is not updated, so a later election event
        // will retry the truncation.
        truncate_unlogged_tables()?;
    }

    call_plugin_leader_change_callbacks(node);
    LAST_HANDLED_IS_LEADER.set(is_leader);

    Ok(())
}

/// Get replication mode and factor for current instance's tier.
///
/// We do not use wait_index in [`proc_replication`] (see header of this file),
/// so cannot get values from topology_cache reliably.
/// That's why we get values from config.
fn get_this_tier_replication_mode_and_factor() -> Result<(ReplicationMode, u8)> {
    get_tier_replication_mode_and_factor(PicodataConfig::get())
}

/// Same as [`get_this_tier_replication_mode_and_factor`], but reads from the provided
/// config instead of the global one.
pub(crate) fn get_tier_replication_mode_and_factor(
    config: &PicodataConfig,
) -> Result<(ReplicationMode, u8)> {
    let my_tier_name = config.effective_instance_tier();
    let Some(tiers) = &config.cluster.tier else {
        return Ok((
            DEFAULT_REPLICATION_MODE,
            config.cluster.default_replication_factor(),
        ));
    };
    let (_, tier) = tiers
        .iter()
        .find(|(tier_name, _)| my_tier_name == tier_name)
        .ok_or_else(|| {
            Error::other(format!(
                "failed to get tier info from config: tier name = {my_tier_name}"
            ))
        })?;

    Ok((
        tier.replication_mode,
        tier.replication_factor
            .unwrap_or_else(|| config.cluster.default_replication_factor()),
    ))
}

crate::define_rpc_request! {
    /// Demotes the target instance from master to read-only replica.
    ///
    /// Returns errors in the following cases:
    /// 1. Lua error during call to `box.cfg`
    fn proc_replication_demote(req: DemoteRequest) -> Result<DemoteResponse> {
        let _ = req;

        let node = node::global()?;
        // Must not call node.wait_index(...) here. See doc-comments at the top
        // of the file for explanation.
        node.status().check_term(req.term)?;

        set_read_only(true)?;

        let vclock = Vclock::current();
        let vclock = vclock.ignore_zero();
        Ok(DemoteResponse { vclock })
    }

    /// Request to promote instance to tarantool replication leader.
    pub struct DemoteRequest {
        pub term: RaftTerm,
    }

    /// Response to [`DemoteRequest`].
    pub struct DemoteResponse {
        pub vclock: Vclock,
    }
}

crate::define_rpc_request! {
    /// Promotes the target instance to the tarantool raft election leader of
    /// its replicaset. Only supported for synchronous-replication tiers, where
    /// replicaset writability is gated by tarantool raft elections
    /// (`election_mode = 'manual'`).
    ///
    /// Initiates an election with this instance as the candidate
    /// (`box.ctl.promote`) and waits for its result (in `manual` mode this is
    /// bounded by the election timeout). The caller must give
    /// the request a deadline which outlasts a whole election round (see
    /// [`crate::tarantool::promote_rpc_timeout`]), otherwise a lost election
    /// is never observed as the typed response and looks like a transport
    /// timeout instead. Winning the election
    /// also claims the synchro txn limbo via the PROMOTE entry, which makes the
    /// instance writable. Note that the election itself provides the
    /// "synchronize before promotion" guarantee: a candidate only gets a vote
    /// from a peer whose vclock is not ahead of the candidate's, so a stale
    /// target cannot win until it has caught up.
    fn proc_replication_promote(req: PromoteRequest) -> Result<PromoteResponse> {
        let node = node::global()?;
        // Must not call node.wait_index(...) here. See doc-comments at the top
        // of the file for explanation.
        node.status().check_term(req.term)?;

        let (replication_mode, _) = get_this_tier_replication_mode_and_factor()?;
        if !replication_mode.is_sync() {
            return Err(Error::other("proc_replication_promote is only supported for synchronous-replication tiers"));
        }

        crate::error_injection!("TIMEOUT_WHEN_SYNCHING_BEFORE_PROMOTION_TO_MASTER" => return Err(Error::timeout()));

        if !crate::tarantool::box_is_ro() {
            // Already the writable election leader, nothing to do.
            return Ok(PromoteResponse::Promoted {
                vclock: Vclock::current().ignore_zero(),
            });
        }

        let promote_result = match injected_election_loss() {
            Some(error) => Err(error),
            None => box_promote(),
        };

        if let Err(error) = promote_result {
            let error_code = tarantool_error_code(&error);
            let error_type = tarantool_error_type(&error);
            if classify_local_promotion_error(error_code, error_type)
                == PromotionErrorDisposition::LostElection
            {
                let (leader_id, leader_uuid) = current_election_leader()?;
                return Ok(PromoteResponse::LostElection {
                    error_code,
                    leader_id,
                    leader_uuid,
                });
            }
            return Err(error.into());
        }

        if let Some(ro_reason) = box_ro_reason() {
            return Err(Error::other(format!("instance is still in read only mode after promotion: {ro_reason}")));
        };

        Ok(PromoteResponse::Promoted {
            vclock: Vclock::current().ignore_zero(),
        })
    }

    /// Request to promote the instance to the tarantool raft election leader
    /// of its replicaset.
    pub struct PromoteRequest {
        pub term: RaftTerm,
    }

    pub enum PromoteResponse {
        Promoted {
            vclock: Vclock,
        },
        LostElection {
            error_code: u32,
            /// Tarantool-local replica id, if an election leader is known.
            leader_id: Option<u32>,
            /// UUID corresponding to `leader_id`, used by the governor to map
            /// the leader back to an eligible Picodata instance.
            leader_uuid: Option<SmolStr>,
        },
    }
}

/// Pretend `box_promote` ended with a lost election.
///
/// This is for governor's handling of a lost election can be tested
/// deterministically.
fn injected_election_loss() -> Option<TarantoolError> {
    crate::error_injection!("LOSE_ELECTION_ON_PROMOTION" => {
        return Some(TarantoolError::Tarantool(BoxError::new(
            TarantoolErrorCode::Timeout as u32,
            "injected election loss",
        )));
    });

    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PromotionErrorDisposition {
    LostElection,
    RetrySameTarget,
    Propagate,
}

/// Classify an error produced locally by `box_promote`.
///
/// Only a lost/stale election becomes a typed outcome. Other retryable
/// failures remain RPC errors, which preserves the governor's existing
/// retry/backoff behavior; safety errors are likewise propagated unchanged.
fn classify_local_promotion_error(error_code: u32, error_type: &str) -> PromotionErrorDisposition {
    // An election which ended with nobody elected is reported by
    // `box_raft_try_promote` via `diag_set(TimedOut)`, which is a system
    // error (ER_SYSTEM, "timed out") rather than ER_TIMEOUT, so it has to be
    // recognized by the error type.
    if error_type == TIMED_OUT_ERROR_TYPE
        || error_code == TarantoolErrorCode::Timeout as u32
        || error_code == TarantoolErrorCode::InterferingPromote as u32
    {
        return PromotionErrorDisposition::LostElection;
    }

    if error_code == TarantoolErrorCode::NoElectionQuorum as u32
        || error_code == TarantoolErrorCode::OldTerm as u32
        || error_code == TarantoolErrorCode::InterferingElections as u32
        // ER_IN_ANOTHER_PROMOTE is newer than the generated Rust enum.
        || error_code == 278
    {
        return PromotionErrorDisposition::RetrySameTarget;
    }

    PromotionErrorDisposition::Propagate
}

/// `type_TimedOut` from tarantool's `exception.cc`.
const TIMED_OUT_ERROR_TYPE: &str = "TimedOut";

fn tarantool_error_code(error: &TarantoolError) -> u32 {
    match error {
        TarantoolError::Tarantool(error) | TarantoolError::Remote(error) => error.error_code(),
        _ => ErrorCode::Other as u32,
    }
}

fn tarantool_error_type(error: &TarantoolError) -> &str {
    match error {
        TarantoolError::Tarantool(error) | TarantoolError::Remote(error) => error.error_type(),
        _ => "Unknown",
    }
}

/// Return the current election leader and its UUID from `_cluster`.
fn current_election_leader() -> Result<(Option<u32>, Option<SmolStr>)> {
    let leader_id = crate::tarantool::box_info_election()?.leader;
    if leader_id == 0 {
        return Ok((None, None));
    }

    let sys_cluster = Space::from(SystemSpace::Cluster);
    let leader_uuid = match sys_cluster.get(&[leader_id])? {
        Some(tuple) => tuple.field::<&str>(1)?.map(SmolStr::new),
        None => None,
    };

    Ok((Some(leader_id), leader_uuid))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn promotion_error_classification() {
        // This is what a lost election actually looks like: a system error of
        // type "TimedOut", see `box_raft_try_promote`.
        assert_eq!(
            classify_local_promotion_error(TarantoolErrorCode::System as u32, TIMED_OUT_ERROR_TYPE),
            PromotionErrorDisposition::LostElection
        );
        assert_eq!(
            classify_local_promotion_error(TarantoolErrorCode::Timeout as u32, "ClientError"),
            PromotionErrorDisposition::LostElection
        );
        assert_eq!(
            classify_local_promotion_error(
                TarantoolErrorCode::InterferingPromote as u32,
                "ClientError"
            ),
            PromotionErrorDisposition::LostElection
        );
        assert_eq!(
            classify_local_promotion_error(
                TarantoolErrorCode::NoElectionQuorum as u32,
                "ClientError"
            ),
            PromotionErrorDisposition::RetrySameTarget
        );
        assert_eq!(
            classify_local_promotion_error(TarantoolErrorCode::SplitBrain as u32, "ClientError"),
            PromotionErrorDisposition::Propagate
        );
    }
}
