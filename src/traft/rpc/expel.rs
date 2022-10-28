use ::tarantool::proc;

use crate::traft;
use crate::traft::Result;
use crate::traft::{error::Error, node, InstanceId, UpdatePeerRequest};

// Netbox entrypoint. For run on Leader only. Don't call directly, use `raft_expel` instead.
#[proc(packed_args)]
fn proc_expel_on_leader(req: Request) -> Result<Response> {
    let node = node::global()?;
    let raft_storage = &node.storage.raft;
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

    let req2 = UpdatePeerRequest::new(req.instance_id, req.cluster_id)
        .with_target_grade(traft::TargetGrade::Expelled)
        .with_current_grade(traft::CurrentGrade::Expelled);
    node.handle_topology_request_and_wait(req2.into())?;

    Ok(Response {})
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Request {
    pub cluster_id: String,
    pub instance_id: InstanceId,
}
impl ::tarantool::tuple::Encode for Request {}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Response {}
impl ::tarantool::tuple::Encode for Response {}

impl super::Request for Request {
    const PROC_NAME: &'static str = crate::stringify_cfunc!(proc_expel_on_leader);
    type Response = Response;
}

pub mod redirect {
    use ::tarantool::proc;

    use crate::traft::rpc::{expel::redirect, net_box_call_to_leader};
    use crate::traft::Result;

    use std::time::Duration;

    // NetBox entrypoint. Run on any node.
    #[proc(packed_args)]
    fn proc_expel_redirect(req: redirect::Request) -> Result<redirect::Response> {
        let redirect::Request(req_to_leader) = req;
        net_box_call_to_leader(&req_to_leader, Duration::MAX)?;
        Ok(redirect::Response {})
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct Request(pub super::Request);
    impl ::tarantool::tuple::Encode for Request {}

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct Response {}
    impl ::tarantool::tuple::Encode for Response {}

    impl crate::traft::rpc::Request for Request {
        const PROC_NAME: &'static str = crate::stringify_cfunc!(proc_expel_redirect);
        type Response = Response;
    }
}
