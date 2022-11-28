use crate::traft::{
    error::Error, node, FailureDomain, InstanceId, Peer, PeerAddress, ReplicasetId, Result,
};

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
        let mut box_replication = vec![];
        for replica in node.storage.peers.replicaset_peers(&peer.replicaset_id)? {
            box_replication.extend(node.storage.peer_addresses.get(replica.raft_id)?);
        }

        // A joined peer needs to communicate with other nodes.
        // TODO: limit the number of entries sent to reduce response size.
        let peer_addresses = node.storage.peer_addresses.iter()?.collect();

        Ok(Response {
            peer,
            peer_addresses,
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
        pub peer_addresses: Vec<PeerAddress>,
        pub box_replication: Vec<String>,
        // Other parameters necessary for box.cfg()
        // TODO
    }
}
