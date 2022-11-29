pub mod apply {
    use crate::traft::{error::Error, node, rpc::sync, RaftIndex, RaftTerm, Result};
    use std::time::Duration;
    use tarantool::{lua_state, tlua::LuaError};

    crate::define_rpc_request! {
        fn proc_apply_migration(req: Request) -> Result<Response> {
            let node = node::global()?;
            node.status().check_term(req.term)?;
            sync::wait_for_index_timeout(req.commit, &node.raft_storage, req.timeout)?;

            let storage = &node.storage;

            match storage.migrations.get(req.migration_id)? {
                Some(migration) => {
                    match lua_state().exec_with("box.execute(...)", migration.body) {
                        Ok(_) => Ok(Response{}),
                        Err(e) => Err(LuaError::from(e).into()),
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
