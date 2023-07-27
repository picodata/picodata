use std::time::Duration;

use crate::failure_domain::FailureDomain;
use crate::instance::{Instance, InstanceId};
use crate::replicaset::ReplicasetId;
use crate::storage::ToEntryIter as _;
use crate::traft::{error::Error, node, Address, PeerAddress, Result};

const TIMEOUT: Duration = Duration::from_secs(10);

crate::define_rpc_request! {
    /// Submits a request to join a new instance to the cluster. If successful, the information about
    /// the new instance and its address will be replicated on all of the cluster instances
    /// through Raft.
    ///
    /// Can be called by a joining instance on any instance that has already joined the cluster.
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving instance is not yet initialized
    /// 2. Storage failure
    /// 3. Incorrect request (e.g. instance already joined or an error in validation of failure domains)
    /// 4. Compare and swap request to commit new instance and its address failed
    /// with an error that cannot be retried.
    fn proc_raft_join(req: Request) -> Result<Response> {
        let node = node::global()?;
        let cluster_id = node.raft_storage.cluster_id()?;

        if req.cluster_id != cluster_id {
            return Err(Error::ClusterIdMismatch {
                instance_cluster_id: req.cluster_id,
                cluster_cluster_id: cluster_id,
            });
        }

        let (instance, replication_addresses) = node.handle_join_request_and_wait(req, TIMEOUT)?;
        // A joined instance needs to communicate with other nodes.
        // TODO: limit the number of entries sent to reduce response size.
        let peer_addresses = node.storage.peer_addresses.iter()?.collect();

        Ok(Response {
            instance,
            peer_addresses,
            box_replication: replication_addresses.into_iter().collect()})
    }

    /// Request to join the cluster.
    pub struct Request {
        pub cluster_id: String,
        pub instance_id: Option<InstanceId>,
        pub replicaset_id: Option<ReplicasetId>,
        pub advertise_address: String,
        pub failure_domain: FailureDomain,
    }

    pub struct Response {
        pub instance: Box<Instance>,
        /// Addresses of other peers in a cluster.
        /// They are needed for Raft node to communicate with other nodes
        /// at startup.
        pub peer_addresses: Vec<PeerAddress>,
        /// Replication sources in a replica set that the joining instance will belong to.
        /// See [tarantool documentation](https://www.tarantool.io/en/doc/latest/reference/configuration/#confval-replication)
        pub box_replication: Vec<Address>,
    }
}
