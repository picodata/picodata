use std::time::Duration;

use crate::error_code::ErrorCode;
use crate::has_states;
use crate::instance::StateVariant::*;
use crate::rpc;
use crate::rpc::update_instance::handle_update_instance_request_and_wait;
use crate::traft::Result;
use crate::traft::{error::Error, node};
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;

crate::define_rpc_request! {
    /// Submits a request to expel the specified instance. If successful
    /// the instance's target state - expelled - will be replicated
    /// on all of the cluster instances through Raft.
    ///
    /// Can be called on any instance that has already joined the cluster.
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving instance is not yet initialized
    /// 2. Storage failure
    /// 3. Incorrect request (e.g. instance already expelled)
    /// 4. Compare and swap request to commit updated instance failed
    /// with an error that cannot be retried.
    fn proc_expel(req: Request) -> Result<Response> {
        let node = node::global()?;
        let raft_storage = &node.raft_storage;
        let cluster_name = raft_storage.cluster_name()?;

        if req.cluster_name != cluster_name {
            return Err(Error::ClusterIdMismatch {
                instance_cluster_name: req.cluster_name,
                cluster_name,
            });
        }

        let topology_ref = node.topology_cache.get();
        let instance = topology_ref.instance_by_uuid(&req.instance_uuid)?;
        if has_states!(instance, * -> Expelled) {
            // Idempotency
            return Ok(Response {});
        }

        if !req.force && !has_states!(instance, Offline -> Offline) {
            let current = instance.current_state.variant;
            let target = instance.target_state.variant;
            let state_repr = if current != target {
                format!("{current} -> {target}")
            } else {
                current.to_string()
            };
            return Err(BoxError::new(ErrorCode::ExpelOnlineInstance, format!("attempt to expel instance which is not Offline, but {state_repr}")).into());
        }

        let timeout = req.timeout;
        let req = rpc::update_instance::Request::new(instance.name.clone(), req.cluster_name)
            .with_target_state(Expelled);

        // Must not hold this reference across yields
        drop(topology_ref);

        handle_update_instance_request_and_wait(req, timeout)?;

        Ok(Response {})
    }

    /// A request to expel an instance.
    ///
    /// Use [`redirect::Request`] for automatic redirection from any instance to leader.
    pub struct Request {
        pub cluster_name: String,
        pub instance_uuid: String,
        pub force: bool,
        pub timeout: Duration,
    }

    pub struct Response {}
}

pub mod redirect {
    use super::*;
    use ::tarantool::fiber;

    use crate::rpc::network_call_to_leader;
    use crate::traft::Result;

    crate::define_rpc_request! {
        fn proc_expel_redirect(req: Request) -> Result<Response> {
            let Request(req_to_leader) = req;

            let deadline = fiber::clock().saturating_add(req_to_leader.timeout);
            fiber::block_on(network_call_to_leader(crate::proc_name!(super::proc_expel), &req_to_leader))?;

            let node = node::global()?;
            let instance_uuid = &req_to_leader.instance_uuid;
            loop {
                let topology_ref = node.topology_cache.get();
                let instance = topology_ref.instance_by_uuid(instance_uuid)?;
                if has_states!(instance, Expelled -> *) {
                    break
                }

                let now = fiber::clock();
                if now > deadline {
                    return Err(BoxError::new(TarantoolErrorCode::Timeout, "expel confirmation didn't arrive in time").into());
                }

                // Must not hold this reference across yields
                drop(topology_ref);

                let timeout = deadline.duration_since(now);
                _ = node.wait_index(node.get_index() + 1, timeout);
            }

            Ok(Response {})
        }

        /// A request to expel an instance.
        ///
        /// Can be sent to any instance and will be automatically redirected to
        /// leader.
        pub struct Request(pub super::Request);
        pub struct Response {}
    }
}
