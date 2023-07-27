use crate::tarantool::set_cfg_field;
use crate::traft::Result;

crate::define_rpc_request! {
    /// Configures replication on the target replica.
    /// Specifies addresses of all the replicas in the replicaset
    /// and whether the target instance should be a replicaset master.
    ///
    /// Returns errors in the following cases:
    /// 1. Lua error during call to `box.cfg`
    /// 2. Storage failure
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
        /// If the target replica should be made a replicaset `master`.
        /// See [tarantool documentation](https://www.tarantool.io/en/doc/latest/reference/configuration/#cfg-basic-read-only)
        /// for more.
        pub is_master: bool,
        /// URIs of all replicas in the replicaset.
        /// See [tarantool documentation](https://www.tarantool.io/en/doc/latest/reference/configuration/#confval-replication)
        /// for more.
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
    use crate::traft::Result;

    crate::define_rpc_request! {
        /// Promotes the target instance from read-only replica to master.
        /// See [tarantool documentation](https://www.tarantool.io/en/doc/latest/reference/configuration/#cfg-basic-read-only)
        /// for more.
        ///
        /// Returns errors in the following cases:
        /// 1. Lua error during call to `box.cfg`
        fn proc_replication_promote(req: Request) -> Result<Response> {
            let _ = req;
            // TODO: find a way to guard against stale governor requests.
            crate::tarantool::exec("box.cfg { read_only = false }")?;
            Ok(Response {})
        }

        /// Request to promote instance to tarantool replication leader.
        pub struct Request {}

        /// Response to [`replication::promote::Request`].
        ///
        /// [`replication::promote::Request`]: Request
        pub struct Response {}
    }
}
