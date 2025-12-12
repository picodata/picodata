//! # The wait_index policy
//!
//! Note that stored procedures in this module do not do [`Node::wait_index`].
//! This is different from all other stored procedures called by governor in
//! [`governor::Loop::iter_fn`]
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
use crate::config::PicodataConfig;
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
use crate::tarantool::{box_ro_reason, set_cfg_field, ListenConfig};
use crate::tlog;
use crate::traft::error::Error;
#[allow(unused_imports)]
use crate::traft::node::{Node, NodeImpl};
#[allow(unused_imports)]
use crate::traft::op::Op;
use crate::traft::{node, RaftTerm, Result};
use smol_str::SmolStr;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::index::IteratorType;
use tarantool::space::{Space, SystemSpace};
use tarantool::tlua::Object;
use tarantool::tlua::StringInLua;
use tarantool::transaction::transaction;
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
        let tls_config = &PicodataConfig::get().instance.iproto_tls;
        for address in &req.replicaset_peers {
            replication_cfg.push(ListenConfig::new_for_pico_service(address,
                tls_config));
        }

        crate::error_injection!("BROKEN_REPLICATION" => { replication_cfg.clear(); });

        // box.cfg checks if the replication is already the same
        // and ignores it if nothing changed
        set_cfg_field("replication", &replication_cfg)?;

        if req.is_master {
            set_read_only(false)?;
            // _cluster is replicated from master to replicas, so we need to
            // update it on the master only.
            // Errors are not fatal here, we do not need to stop the process,
            // because cleaning up _cluster is not critical and we can do
            // it later.
            if let Err(e) = update_sys_cluster() {
                tlog!(Error, "failed to update _cluster: {e}");
            }
        } else {
            // Everybody else should be read-only
            set_read_only(true)?;
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

    let Some(values) = result else {
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
    transaction(|| -> Result<()> {
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
fn set_read_only(new_read_only: bool) -> Result<()> {
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
    }

    if old_read_only != new_read_only {
        // errors ignored because it must be already handled by plugin manager itself
        let res = node.plugin_manager.handle_rs_leader_change();
        if let Err(e) = res {
            tlog!(Error, "on_leader_change error: {e}");
        }
    }

    Ok(())
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
