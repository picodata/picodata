use std::time::Duration;

use crate::instance::InstanceName;
use crate::instance::StateVariant::*;
use crate::rpc;
use crate::rpc::update_instance::handle_update_instance_request_and_wait;
use crate::traft::Result;
use crate::traft::{error::Error, node};

const TIMEOUT: Duration = Duration::from_secs(10);

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

        let req = rpc::update_instance::Request::new(req.instance_name, req.cluster_name)
            .with_target_state(Expelled);
        handle_update_instance_request_and_wait(req, TIMEOUT)?;

        Ok(Response {})
    }

    /// A request to expel an instance.
    ///
    /// Use [`redirect::Request`] for automatic redirection from any instance to leader.
    pub struct Request {
        pub cluster_name: String,
        pub instance_name: InstanceName,
    }

    pub struct Response {}
}

pub mod redirect {
    use ::tarantool::fiber;

    use crate::rpc::network_call_to_leader;
    use crate::traft::Result;

    crate::define_rpc_request! {
        fn proc_expel_redirect(req: Request) -> Result<Response> {
            let Request(req_to_leader) = req;
            fiber::block_on(network_call_to_leader(crate::proc_name!(super::proc_expel), &req_to_leader))?;
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
