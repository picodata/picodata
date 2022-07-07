use std::time::{Duration, Instant};

use ::tarantool::fiber::sleep;
use ::tarantool::proc;

use crate::{stringify_cfunc, tarantool, tlog};

use crate::traft::error::Error;
use crate::traft::event;
use crate::traft::node;
use crate::traft::Storage;
use crate::traft::{UpdatePeerRequest, UpdatePeerResponse};

pub fn on_shutdown() {
    let voters = Storage::voters().expect("failed reading voters");
    let active_learners = Storage::active_learners().expect("failed reading active learners");
    let raft_id = node::global().unwrap().status().id;

    if voters == [raft_id] && active_learners.is_empty() {
        tlog!(Warning, "the last active instance has shut down");
        return;
    }

    let peer = Storage::peer_by_raft_id(raft_id).unwrap().unwrap();
    let req = UpdatePeerRequest::set_offline(
        peer.instance_id,
        Storage::cluster_id()
            .unwrap()
            .expect("cluster_id must be present"),
    );

    let fn_name = stringify_cfunc!(raft_update_peer);
    // will run until we get successfully deactivate or tarantool shuts down
    // the on_shutdown fiber (after 3 secs)
    loop {
        let status = node::global().unwrap().status();
        let leader_id = status.leader_id.expect("leader_id deinitialized");
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

    let cluster_id = Storage::cluster_id()?.ok_or("cluster_id is not set yet")?;

    if req.cluster_id != cluster_id {
        return Err(Box::new(Error::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id,
            cluster_cluster_id: cluster_id,
        }));
    }

    node.handle_topology_request(req.into())?;
    Ok(UpdatePeerResponse {})
}

pub fn voters_needed(voters: usize, total: usize) -> i64 {
    let voters_expected = match total {
        1 => 1,
        2 => 2,
        3..=4 => 3,
        5.. => 5,
        _ => unreachable!(),
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
    }
}
