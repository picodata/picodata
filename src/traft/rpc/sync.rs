use crate::tlog;
use crate::traft::Result;
use crate::traft::{error::Error, event, node, RaftIndex, RaftSpaceAccess};

use std::time::{Duration, Instant};

crate::define_rpc_request! {
    fn proc_sync_raft(req: Request) -> Result<Response> {
        let raft_storage = &node::global()?.raft_storage;
        let applied = wait_for_index_timeout(req.applied, raft_storage, req.timeout)?;
        Ok(Response { applied })
    }

    pub struct Request {
        pub applied: RaftIndex,
        pub timeout: Duration,
    }

    pub struct Response {
        pub applied: RaftIndex,
    }
}

#[inline]
pub fn wait_for_index_timeout(
    applied: RaftIndex,
    raft_storage: &RaftSpaceAccess,
    timeout: Duration,
) -> Result<RaftIndex> {
    let deadline = Instant::now() + timeout;
    loop {
        let cur_applied = raft_storage.applied()?.unwrap_or(0);
        if cur_applied >= applied {
            tlog!(Debug, "synchronized raft log to index {applied}"; "applied" => cur_applied);
            return Ok(cur_applied);
        }

        if let Some(timeout) = deadline.checked_duration_since(Instant::now()) {
            event::wait_timeout(event::Event::CommitIndexPersisted, timeout)?;
        } else {
            return Err(Error::Timeout);
        }
    }
}
