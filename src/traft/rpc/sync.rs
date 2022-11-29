use crate::traft::Result;
use crate::traft::{error::Error, event, node, RaftIndex, RaftSpaceAccess};

use std::time::{Duration, Instant};

crate::define_rpc_request! {
    fn proc_sync_raft(req: Request) -> Result<Response> {
        let raft_storage = &node::global()?.raft_storage;
        let commit = wait_for_index_timeout(req.commit, raft_storage, req.timeout)?;
        Ok(Response { commit })
    }

    pub struct Request {
        pub commit: RaftIndex,
        pub timeout: Duration,
    }

    pub struct Response {
        pub commit: RaftIndex,
    }
}

#[inline]
pub fn wait_for_index_timeout(
    commit: RaftIndex,
    raft_storage: &RaftSpaceAccess,
    timeout: Duration,
) -> Result<RaftIndex> {
    let deadline = Instant::now() + timeout;
    loop {
        let cur_commit = raft_storage.commit()?.expect("commit is always persisted");
        if cur_commit >= commit {
            return Ok(cur_commit);
        }

        if let Some(timeout) = deadline.checked_duration_since(Instant::now()) {
            event::wait_timeout(event::Event::RaftEntryApplied, timeout)?;
        } else {
            return Err(Error::Timeout);
        }
    }
}
