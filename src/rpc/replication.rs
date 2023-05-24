use crate::tarantool::set_cfg_field;
use crate::traft::Result;

crate::define_rpc_request! {
    fn proc_replication(req: Request) -> Result<Response> {
        // TODO: check this configuration is newer then the one currently
        // applied. For this we'll probably need to store the governor's applied
        // index at the moment of request generation in box.space._schema on the
        // requestee. And if the new request has index less then the one in our
        // _schema, then we ignore it.

        // box.cfg checks if the replication is already the same
        // and ignores it if nothing changed
        set_cfg_field("replication", &req.replicaset_peers)?;
        let lsn = crate::tarantool::eval("return box.info.lsn")?;

        // We do this everytime because firstly it helps when waking up.
        // And secondly just in case, it doesn't hurt anyways.
        if req.is_master {
            set_cfg_field("read_only", false)?;
        }

        Ok(Response { lsn })
    }

    /// Request to configure tarantool replication.
    pub struct Request {
        pub is_master: bool,
        pub replicaset_peers: Vec<String>,
    }

    /// Response to [`replication::Request`].
    ///
    /// [`replication::Request`]: Request
    pub struct Response {
        pub lsn: u64,
    }
}

pub mod promote {
    use crate::traft::node;
    use crate::traft::RaftIndex;
    use crate::traft::RaftTerm;
    use crate::traft::Result;
    use std::time::Duration;

    crate::define_rpc_request! {
        fn proc_replication_promote(req: Request) -> Result<Response> {
            let node = node::global()?;
            node.wait_index(req.applied, req.timeout)?;
            node.status().check_term(req.term)?;
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
