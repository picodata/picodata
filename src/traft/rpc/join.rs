use crate::traft::{error::Error, node, FailureDomain, InstanceId, Peer, ReplicasetId, Result};

crate::define_rpc_request! {
    fn proc_raft_join(req: Request) -> Result<Response> {
        let node = node::global()?;

        let cluster_id = node
            .storage
            .raft
            .cluster_id()?
            .expect("cluster_id is set on boot");

        if req.cluster_id != cluster_id {
            return Err(Error::ClusterIdMismatch {
                instance_cluster_id: req.cluster_id,
                cluster_cluster_id: cluster_id,
            });
        }

        let peer = node.handle_topology_request_and_wait(req.into())?;
        let box_replication = node
            .storage
            .peers
            .replicaset_peer_addresses(&peer.replicaset_id, Some(peer.commit_index))?;

        // A joined peer needs to communicate with other nodes.
        // Provide it the list of raft voters in response.
        let mut raft_group = vec![];
        for raft_id in node.storage.raft.voters()?.unwrap_or_default().into_iter() {
            match node.storage.peers.get(&raft_id) {
                Err(e) => {
                    crate::warn_or_panic!("failed reading peer with id `{}`: {}", raft_id, e);
                }
                Ok(peer) => raft_group.push(peer),
            }
        }

        Ok(Response {
            peer,
            raft_group,
            box_replication,
        })
    }

    /// Request to join the cluster.
    pub struct Request {
        pub cluster_id: String,
        pub instance_id: Option<InstanceId>,
        pub replicaset_id: Option<ReplicasetId>,
        pub advertise_address: String,
        pub failure_domain: FailureDomain,
    }

    /// Response to a [`join::Request`](Request).
    pub struct Response {
        pub peer: Box<Peer>,
        pub raft_group: Vec<Peer>,
        pub box_replication: Vec<String>,
        // Other parameters necessary for box.cfg()
        // TODO
    }
}
