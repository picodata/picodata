use crate::traft;
use crate::traft::Result;
use crate::traft::{error::Error, node, rpc::update_instance, InstanceId};

crate::define_rpc_request! {
    fn proc_expel_on_leader(req: Request) -> Result<Response> {
        let node = node::global()?;
        let raft_storage = &node.raft_storage;
        let cluster_id = raft_storage
            .cluster_id()?
            .expect("cluster_id is set on boot");

        if req.cluster_id != cluster_id {
            return Err(Error::ClusterIdMismatch {
                instance_cluster_id: req.cluster_id,
                cluster_cluster_id: cluster_id,
            });
        }

        let leader_id = node.status().leader_id.ok_or(Error::LeaderUnknown)?;
        if node.raft_id() != leader_id {
            return Err(Error::NotALeader);
        }

        let req2 = update_instance::Request::new(req.instance_id, req.cluster_id)
            .with_target_grade(traft::TargetGradeVariant::Expelled);
        node.handle_topology_request_and_wait(req2.into())?;

        Ok(Response {})
    }

    /// A request to expel an instance.
    ///
    /// This request is only handled by the leader.
    /// Use [`redirect::Request`] for automatic redirection from any instance to
    /// leader.
    pub struct Request {
        pub cluster_id: String,
        pub instance_id: InstanceId,
    }

    pub struct Response {}
}

pub mod redirect {
    use crate::traft::rpc::net_box_call_to_leader;
    use crate::traft::Result;

    use std::time::Duration;

    crate::define_rpc_request! {
        fn proc_expel_redirect(req: Request) -> Result<Response> {
            let Request(req_to_leader) = req;
            net_box_call_to_leader(&req_to_leader, Duration::MAX)?;
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
