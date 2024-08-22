use crate::pico_service::pico_service_password;
use crate::plugin::PluginEvent;
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::tarantool::set_cfg_field;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::{node, Result};
use std::time::Duration;
use tarantool::tlua;
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
        // TODO: check this configuration is newer then the one currently
        // applied. For this we'll probably need to store the governor's applied
        // index at the moment of request generation in box.space._schema on the
        // requestee. And if the new request has index less then the one in our
        // _schema, then we ignore it.

        let mut replication_cfg = Vec::with_capacity(req.replicaset_peers.len());
        let password = pico_service_password();
        for address in &req.replicaset_peers {
            replication_cfg.push(format!("{PICO_SERVICE_USER_NAME}:{password}@{address}"))
        }

        // box.cfg checks if the replication is already the same
        // and ignores it if nothing changed
        set_cfg_field("replication", &replication_cfg)?;

        if let Some(sync) = req.sync_and_promote {
            crate::error_injection!("TIMEOUT_WHEN_SYNCHING_BEFORE_PROMOTION_TO_MASTER" => return Err(Error::Timeout));

            synchronize_and_promote_to_master(sync)?;
        } else {
            // Everybody else should be read-only
            set_cfg_field("read_only", true)?;
        }

        Ok(Response {})
    }

    /// Request to configure tarantool replication.
    pub struct ConfigureReplicationRequest {
        /// If this is not `None` the target replica will synchronize and become the new `master`.
        /// See [tarantool documentation](https://www.tarantool.io/en/doc/latest/reference/configuration/#cfg-basic-read-only)
        /// for more.
        pub sync_and_promote: Option<SyncAndPromoteRequest>,
        /// URIs of all replicas in the replicaset.
        /// See [tarantool documentation](https://www.tarantool.io/en/doc/latest/reference/configuration/#confval-replication)
        /// for more.
        pub replicaset_peers: Vec<String>,
    }

    /// Response to [`ConfigureReplicationRequest`].
    pub struct Response {}
}

/// Promotes the target instance from read-only replica to master.
/// See [tarantool documentation](https://www.tarantool.io/en/doc/latest/reference/configuration/#cfg-basic-read-only)
/// for more.
///
/// Returns errors in the following cases: See implementation.
fn synchronize_and_promote_to_master(req: SyncAndPromoteRequest) -> Result<()> {
    crate::sync::wait_vclock(req.vclock, req.timeout)?;

    // XXX: Currently we just change the box.cfg.read_only option of the
    // instance but at some point we will implement support for
    // tarantool synchronous transactions then this operation will probably
    // become more involved.
    let lua = tarantool::lua_state();
    let ro_reason: Option<tlua::StringInLua<_>> = lua.eval(
        "box.cfg { read_only = false }
        return box.info.ro_reason",
    )?;

    #[rustfmt::skip]
    if let Some(ro_reason) = ro_reason.as_deref() {
        tlog!(Warning, "failed to promote self to replication leader, reason = {ro_reason}");
        return Err(Error::other(format!("instance is still in read only mode: {ro_reason}")));
    };

    // errors ignored because it must be already handled by plugin manager itself
    _ = node::global()?
        .plugin_manager
        .handle_event_sync(PluginEvent::InstancePromote);

    Ok(())
}

/// Request to promote instance to tarantool replication leader.
#[derive(Clone, Debug, ::serde::Serialize, ::serde::Deserialize)]
pub struct SyncAndPromoteRequest {
    pub vclock: Vclock,
    pub timeout: Duration,
}
impl ::tarantool::tuple::Encode for SyncAndPromoteRequest {}

crate::define_rpc_request! {
    /// Demotes the target instance from master to read-only replica.
    ///
    /// Returns errors in the following cases:
    /// 1. Lua error during call to `box.cfg`
    fn proc_replication_demote(req: DemoteRequest) -> Result<DemoteResponse> {
        let _ = req;
        // TODO: find a way to guard against stale governor requests.
        crate::tarantool::exec("box.cfg { read_only = true }")?;

        // errors ignored because it must be already handled by plugin manager itself
        _ = node::global()?.plugin_manager.handle_event_sync(PluginEvent::InstanceDemote);

        let vclock = Vclock::current();
        Ok(DemoteResponse { vclock })
    }

    /// Request to promote instance to tarantool replication leader.
    pub struct DemoteRequest {}

    /// Response to [`DemoteRequest`].
    pub struct DemoteResponse {
        pub vclock: Vclock,
    }
}
