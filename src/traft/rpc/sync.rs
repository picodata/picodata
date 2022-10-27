use ::tarantool::proc;

use crate::traft::{error::Error, event, node, RaftIndex, RaftSpaceAccess};

use std::time::{Duration, Instant};

#[proc(packed_args)]
fn proc_sync_raft(req: Request) -> Result<Response, Error> {
    let storage = &node::global()?.storage;
    let commit = wait_for_index_timeout(req.commit, &storage.raft, req.timeout)?;
    Ok(Response { commit })
}

#[inline]
pub fn wait_for_index_timeout(
    commit: RaftIndex,
    raft_storage: &RaftSpaceAccess,
    timeout: Duration,
) -> Result<RaftIndex, Error> {
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Request {
    pub commit: RaftIndex,
    pub timeout: Duration,
}
impl ::tarantool::tuple::Encode for Request {}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Response {
    pub commit: RaftIndex,
}
impl ::tarantool::tuple::Encode for Response {}

impl super::Request for Request {
    const PROC_NAME: &'static str = crate::stringify_cfunc!(proc_sync_raft);
    type Response = Response;
}
