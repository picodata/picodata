use std::time::{Duration, Instant};

use ::tarantool::fiber;

use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::event;
use crate::traft::node;
use crate::traft::rpc;
use crate::traft::CurrentGradeVariant;
use crate::traft::TargetGradeVariant;
use crate::traft::{UpdatePeerRequest, UpdatePeerResponse};
use crate::unwrap_ok_or;

pub fn on_shutdown() {
    // 1. Try setting target grade Offline in a separate fiber
    tlog!(Info, "trying to shutdown gracefully ...");
    let go_offline = fiber::Builder::new()
        .name("go_offline")
        .func(|| match go_offline() {
            Ok(()) => tlog!(Info, "target grade set Offline"),
            Err(e) => tlog!(Error, "failed setting target grade Offline: {e}"),
        });
    std::mem::forget(go_offline.start());

    // 2. Meanwhile, wait until either it succeeds or there is no quorum.
    let node = node::global().unwrap();
    let raft_id = node.raft_id();
    loop {
        let me = unwrap_ok_or!(
            node.storage.peers.get(&raft_id),
            Err(e) => {
                tlog!(Error, "{e}");
                break;
            }
        );

        if me.current_grade == CurrentGradeVariant::Offline {
            tlog!(Info, "graceful shutdown succeeded");

            // Dirty hack. Wait a little bit more before actually
            // shutting down. Raft commit index is a local value. Other
            // nodes may still be unaware that `me.current_grade` is
            // commmitted. Give them some more time to communicate.
            fiber::sleep(Duration::from_millis(100));
            break;
        }

        let voters = node
            .storage
            .raft
            .voters()
            .expect("failed reading voters")
            .unwrap_or_default();

        let quorum = voters.len() / 2 + 1;
        let voters_alive = voters
            .iter()
            .filter_map(|raft_id| node.storage.peers.get(raft_id).ok())
            .filter(|peer| peer.current_grade == CurrentGradeVariant::Online)
            .count();

        if voters_alive < quorum {
            tlog!(Info, "giving up, there is no quorum");
            break;
        }

        let Ok(()) = event::wait(event::Event::TopologyChanged) else { break };
    }
}

fn go_offline() -> traft::Result<()> {
    let node = node::global()?;
    let raft_id = node.raft_id();

    let peer = node.storage.peers.get(&raft_id)?;
    let cluster_id = node
        .storage
        .raft
        .cluster_id()?
        .ok_or_else(|| Error::other("missing cluster_id value in storage"))?;

    let req = UpdatePeerRequest::new(peer.instance_id, cluster_id)
        .with_target_grade(TargetGradeVariant::Offline);

    loop {
        let Some(leader_id) = node.status().leader_id else {
            node.wait_status();
            continue
        };

        let now = Instant::now();
        let wait_before_retry = Duration::from_millis(300);

        if leader_id == raft_id {
            match node.handle_topology_request_and_wait(req.clone().into()) {
                Err(Error::NotALeader) => {
                    // We've lost leadership while waiting for NodeImpl
                    // mutex. Retry after a small pause.
                    fiber::sleep(wait_before_retry.saturating_sub(now.elapsed()));
                    continue;
                }
                Err(e) => break Err(e),
                Ok(_) => break Ok(()),
            }
        }

        let leader = node.storage.peers.get(&leader_id)?;
        let res = match rpc::net_box_call(&leader.peer_address, &req, Duration::MAX) {
            Ok(UpdatePeerResponse::Ok) => Ok(()),
            Ok(UpdatePeerResponse::ErrNotALeader) => Err(Error::NotALeader),
            Err(e) => Err(e.into()),
        };

        match res {
            Ok(()) => break Ok(()),
            Err(e) => {
                tlog!(Warning,
                    "failed setting target grade Offline: {e}, retrying ...";
                    "peer" => %leader,
                );
                fiber::sleep(wait_before_retry.saturating_sub(now.elapsed()));
                continue;
            }
        };
    }
}
