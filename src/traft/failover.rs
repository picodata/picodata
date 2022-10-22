use std::time::{Duration, Instant};

use ::tarantool::fiber::sleep;
use ::tarantool::unwrap_or;

use crate::tlog;

use crate::traft::error::Error;
use crate::traft::event;
use crate::traft::node;
use crate::traft::rpc;
use crate::traft::TargetGradeVariant;
use crate::traft::{UpdatePeerRequest, UpdatePeerResponse};

pub fn on_shutdown() {
    let node = node::global().unwrap();
    let voters = node
        .storage
        .raft
        .voters()
        .expect("failed reading voters")
        .unwrap_or_default();
    let learners = node
        .storage
        .raft
        .learners()
        .expect("failed reading learners")
        .unwrap_or_default();
    let have_active_learners = learners.into_iter().any(|raft_id| {
        node.storage
            .peers
            .get(&raft_id)
            // TODO: change to peer.may_respond()
            .map(|peer| peer.is_online())
            .unwrap_or(false)
    });
    let raft_id = node.status().id;

    if voters == [raft_id] && !have_active_learners {
        tlog!(Warning, "the last active instance has shut down");
        return;
    }

    let peer = node.storage.peers.get(&raft_id).unwrap();
    let cluster_id = node
        .storage
        .raft
        .cluster_id()
        .unwrap()
        .expect("cluster_id must be present");
    let req = UpdatePeerRequest::new(peer.instance_id, cluster_id)
        .with_target_grade(TargetGradeVariant::Offline);

    // will run until we get successfully deactivate or tarantool shuts down
    // the on_shutdown fiber (after 3 secs)
    loop {
        let node = node::global().unwrap();
        let leader_id = unwrap_or!(node.status().leader_id, {
            node.wait_status();
            continue;
        });
        if leader_id == raft_id {
            if let Err(e) = node.handle_topology_request_and_wait(req.into()) {
                crate::warn_or_panic!("failed to deactivate myself: {}", e);
            }
            break;
        }
        let leader = node.storage.peers.get(&leader_id).unwrap();
        let wait_before_retry = Duration::from_millis(300);
        let now = Instant::now();

        let res = rpc::net_box_call(&leader.peer_address, &req, Duration::MAX);
        let res = match res {
            Ok(UpdatePeerResponse::Ok) => Ok(()),
            Ok(UpdatePeerResponse::ErrNotALeader) => Err(Error::NotALeader),
            Err(e) => Err(e.into()),
        };
        match res {
            Ok(()) => {
                break;
            }
            Err(e) => {
                tlog!(Warning, "failed to deactivate myself: {e}, retry...";
                    "peer" => &leader.peer_address,
                );
                sleep(wait_before_retry.saturating_sub(now.elapsed()));
                continue;
            }
        };
    }

    // no need to wait for demotion if we weren't a voter
    if !voters.contains(&raft_id) {
        return;
    }

    tlog!(Debug, "waiting for demotion");
    if let Err(e) = event::wait(event::Event::Demoted) {
        tlog!(Warning, "failed to wait for self demotion: {e}");
    }
}
