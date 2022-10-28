use crate::tarantool::set_cfg_field;
use crate::traft::{
    self,
    error::Error,
    node,
    storage::peer_field::{PeerAddress, ReplicasetId},
    RaftIndex, RaftTerm, Result,
};
use crate::InstanceId;

use std::time::Duration;

crate::define_rpc_request! {
    fn proc_replication(req: Request) -> Result<Response> {
        let node = node::global()?;
        node.status().check_term(req.term)?;
        super::sync::wait_for_index_timeout(req.commit, &node.storage.raft, req.timeout)?;

        let peer_storage = &node.storage.peers;
        let this_rsid = peer_storage.peer_field::<ReplicasetId>(&node.raft_id())?;
        let mut peer_addresses = Vec::with_capacity(req.replicaset_instances.len());
        for id in &req.replicaset_instances {
            let (address, rsid) = peer_storage.peer_field::<(PeerAddress, ReplicasetId)>(id)?;
            if rsid != this_rsid {
                return Err(Error::ReplicasetIdMismatch {
                    instance_rsid: this_rsid,
                    requested_rsid: rsid,
                });
            }
            peer_addresses.push(address)
        }
        // box.cfg checks if the replication is already the same
        // and ignores it if nothing changed
        set_cfg_field("replication", peer_addresses)?;
        let lsn = crate::tarantool::eval("return box.info.lsn")?;
        Ok(Response { lsn })
    }

    /// Request to configure tarantool replication.
    pub struct Request {
        pub term: RaftTerm,
        pub commit: RaftIndex,
        pub timeout: Duration,
        pub replicaset_instances: Vec<InstanceId>,
        pub replicaset_id: traft::ReplicasetId,
    }

    /// Response to [`replication::Request`].
    ///
    /// [`replication::Request`]: Request
    pub struct Response {
        pub lsn: u64,
    }
}

pub mod promote {
    use crate::traft::{node, rpc, RaftIndex, RaftTerm, Result};
    use std::time::Duration;

    crate::define_rpc_request! {
        fn proc_replication_promote(req: Request) -> Result<Response> {
            let node = node::global()?;
            node.status().check_term(req.term)?;
            rpc::sync::wait_for_index_timeout(req.commit, &node.storage.raft, req.timeout)?;
            crate::tarantool::exec(
                "box.cfg { read_only = false }
                box.ctl.promote()",
            )?;
            Ok(Response {})
        }

        /// Request to promote peer to tarantool replication leader.
        pub struct Request {
            pub term: RaftTerm,
            pub commit: RaftIndex,
            pub timeout: Duration,
        }

        /// Response to [`replication::promote::Request`].
        ///
        /// [`replication::promote::Request`]: Request
        pub struct Response {}
    }
}
