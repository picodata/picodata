use crate::storage::instance_field::ReplicasetId;
use crate::tarantool::set_cfg_field;
use crate::traft::{node, RaftIndex, RaftTerm, Result};

use std::time::Duration;

crate::define_rpc_request! {
    fn proc_replication(req: Request) -> Result<Response> {
        let node = node::global()?;
        node.status().check_term(req.term)?;
        super::sync::wait_for_index_timeout(req.applied, &node.raft_storage, req.timeout)?;

        let storage = &node.storage;
        let rsid = storage.instances.field::<ReplicasetId>(&node.raft_id())?;
        let mut box_replication = vec![];
        for replica in storage.instances.replicaset_instances(&rsid)? {
            let Some(address) = storage.peer_addresses.get(replica.raft_id)? else {
                crate::tlog!(Warning, "address unknown for instance";
                    "raft_id" => replica.raft_id,
                );
                continue;
            };
            box_replication.push(address);
        }
        // box.cfg checks if the replication is already the same
        // and ignores it if nothing changed
        set_cfg_field("replication", box_replication)?;
        let lsn = crate::tarantool::eval("return box.info.lsn")?;
        Ok(Response { lsn })
    }

    /// Request to configure tarantool replication.
    pub struct Request {
        pub term: RaftTerm,
        pub applied: RaftIndex,
        pub timeout: Duration,
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
            rpc::sync::wait_for_index_timeout(req.applied, &node.raft_storage, req.timeout)?;
            crate::tarantool::exec("box.cfg { read_only = false }")?;
            Ok(Response {})
        }

        /// Request to promote instance to tarantool replication leader.
        pub struct Request {
            pub term: RaftTerm,
            pub applied: RaftIndex,
            pub timeout: Duration,
        }

        /// Response to [`replication::promote::Request`].
        ///
        /// [`replication::promote::Request`]: Request
        pub struct Response {}
    }
}
