pub mod apply {
    use crate::traft::{error::Error, node, rpc::sync, RaftIndex, RaftTerm, Result};
    use std::time::Duration;

    crate::define_rpc_request! {
        fn proc_apply_migration(req: Request) -> Result<Response> {
            let node = node::global()?;
            node.status().check_term(req.term)?;
            sync::wait_for_index_timeout(req.commit, &node.storage.raft, req.timeout)?;

            let storage = &node.storage;

            match storage.migrations.get(req.migration_id)? {
                Some(migration) => {
                    match crate::tarantool::exec(migration.body.as_str()) {
                        Ok(_) => Ok(Response{}),
                        Err(e) => Err(e.into())
                    }
                }
                None => Err(Error::other(format!("Migration {0} not found", req.migration_id))),
            }
        }

        pub struct Request {
            pub term: RaftTerm,
            pub commit: RaftIndex,
            pub timeout: Duration,
            pub migration_id: u64,
        }

        pub struct Response {}
    }
}
