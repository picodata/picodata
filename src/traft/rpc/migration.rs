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

            let Some(migration) = storage.migrations.get(req.migration_id)? else {
                return Err(Error::other(format!("migration {0} not found", req.migration_id)));
            };

            lua_state()
                .exec_with(
                    "local ok, err = box.execute(...)
                    if not ok then
                        box.error(err)
                    end",
                    migration.body,
                )
                .map_err(LuaError::from)?;

            Ok(Response {})
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
