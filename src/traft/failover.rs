use std::time::{Duration, Instant};

use ::tarantool::fiber::sleep;
use ::tarantool::proc;
use ::tarantool::unwrap_or;

use crate::{stringify_cfunc, tarantool, tlog};

use crate::traft::error::Error;
use crate::traft::event;
use crate::traft::node;
use crate::traft::Storage;
use crate::traft::TargetGrade;
use crate::traft::{UpdatePeerRequest, UpdatePeerResponse};

pub fn on_shutdown() {
    let node = node::global().unwrap();
    let voters = node
        .storage
        .voters()
        .expect("failed reading voters")
        .unwrap_or_default();
    let learners = node
        .storage
        .learners()
        .expect("failed reading learners")
        .unwrap_or_default();
    let have_active_learners = learners.into_iter().any(|raft_id| {
        Storage::peer_by_raft_id(raft_id)
            .expect("failed reading peer")
            .map(|peer| peer.is_active())
            .unwrap_or(false)
    });
    let raft_id = node.status().id;

    if voters == [raft_id] && !have_active_learners {
        tlog!(Warning, "the last active instance has shut down");
        return;
    }

    let peer = Storage::peer_by_raft_id(raft_id).unwrap().unwrap();
    let cluster_id = node
        .storage
        .cluster_id()
        .unwrap()
        .expect("cluster_id must be present");
    let req = UpdatePeerRequest::new(peer.instance_id, cluster_id)
        .with_target_grade(TargetGrade::Offline);

    let fn_name = stringify_cfunc!(raft_update_peer);
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
        let leader = Storage::peer_by_raft_id(leader_id).unwrap().unwrap();
        let wait_before_retry = Duration::from_millis(300);
        let now = Instant::now();

        match tarantool::net_box_call(&leader.peer_address, fn_name, &req, Duration::MAX) {
            Err(e) => {
                tlog!(Warning, "failed to deactivate myself: {e}";
                    "peer" => &leader.peer_address,
                    "fn" => fn_name,
                );
                sleep(wait_before_retry.saturating_sub(now.elapsed()));
                continue;
            }
            Ok(UpdatePeerResponse { .. }) => {
                break;
            }
        };
    }

    // no need to wait for demotion if we weren't a voter
    if !voters.contains(&raft_id) {
        return;
    }

    if let Err(e) = event::wait(event::Event::Demoted) {
        tlog!(Warning, "failed to wait for self demotion: {e}");
    }
}

#[proc(packed_args)]
fn raft_update_peer(
    req: UpdatePeerRequest,
) -> Result<UpdatePeerResponse, Box<dyn std::error::Error>> {
    let node = node::global()?;

    let cluster_id = node
        .storage
        .cluster_id()?
        .ok_or("cluster_id is not set yet")?;

    if req.cluster_id != cluster_id {
        return Err(Box::new(Error::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id,
            cluster_cluster_id: cluster_id,
        }));
    }

    let mut req = req;
    let instance_id = &*req.instance_id;
    req.changes.retain(|ch| match ch {
        super::PeerChange::Grade(grade) => {
            tlog!(Warning, "attempt to change grade by peer";
                "instance_id" => instance_id,
                "grade" => grade.as_str(),
            );
            false
        }
        _ => true,
    });
    node.handle_topology_request_and_wait(req.into())?;
    Ok(UpdatePeerResponse {})
}

pub fn voters_needed(voters: usize, total: usize) -> i64 {
    let voters_expected = match total {
        0 => {
            crate::warn_or_panic!("`voters_needed` was called with `total` = 0");
            0
        }
        1 => 1,
        2 => 2,
        3..=4 => 3,
        5.. => 5,
        _ => unreachable!(
            "just another thing rust is garbage at:
             `5..` covers all the rest of the values,
             but rust can't figure this out"
        ),
    };
    voters_expected - (voters as i64)
}

#[cfg(test)]
mod tests {
    #[test]
    fn voters_needed() {
        assert_eq!(super::voters_needed(0, 1), 1);
        assert_eq!(super::voters_needed(1, 1), 0);
        assert_eq!(super::voters_needed(2, 1), -1);
        assert_eq!(super::voters_needed(0, 2), 2);
        assert_eq!(super::voters_needed(2, 3), 1);
        assert_eq!(super::voters_needed(6, 4), -3);
        assert_eq!(super::voters_needed(1, 5), 4);
        assert_eq!(super::voters_needed(1, 999), 4);
        assert_eq!(super::voters_needed(0, usize::MAX), 5);
        assert_eq!(super::voters_needed(0, u64::MAX as _), 5);
    }
}
