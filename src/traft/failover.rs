use std::time::Duration;

use ::tarantool::proc;

use crate::{stringify_cfunc, tarantool, tlog};

use crate::traft::node;
use crate::traft::node::Error;
use crate::traft::Storage;
use crate::traft::{DeactivateRequest, DeactivateResponse};

pub fn on_shutdown() {
    let voters = Storage::voters().expect("failed reading 'voters'");
    let raft_id = node::global().unwrap().status().id;
    // raft will not let us have a cluster with no voters anyway
    if !voters.contains(&raft_id) || voters.len() == 1 {
        tlog!(Info, "not demoting");
        return;
    }

    let peer = Storage::peer_by_raft_id(raft_id).unwrap().unwrap();
    let req = DeactivateRequest {
        instance_id: peer.instance_id,
        cluster_id: Storage::cluster_id()
            .unwrap()
            .expect("cluster_id must be present"),
    };

    let fn_name = stringify_cfunc!(raft_deactivate);
    // will run until we get successfully deactivate or tarantool shuts down
    // the on_shutdown fiber (after 3 secs)
    loop {
        let status = node::global().unwrap().status();
        let leader_id = status.leader_id.expect("leader_id deinitialized");
        let leader = Storage::peer_by_raft_id(leader_id).unwrap().unwrap();

        match tarantool::net_box_call(&leader.peer_address, fn_name, &req, Duration::MAX) {
            Err(e) => {
                tlog!(Warning, "failed to deactivate myself: {e}";
                    "peer" => &leader.peer_address,
                    "fn" => fn_name,
                );
                continue;
            }
            Ok(DeactivateResponse { .. }) => {
                break;
            }
        };
    }
}

#[proc(packed_args)]
fn raft_deactivate(
    req: DeactivateRequest,
) -> Result<DeactivateResponse, Box<dyn std::error::Error>> {
    let node = node::global()?;

    let cluster_id = Storage::cluster_id()?.ok_or("cluster_id is not set yet")?;

    if req.cluster_id != cluster_id {
        return Err(Box::new(Error::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id,
            cluster_cluster_id: cluster_id,
        }));
    }

    node.change_topology(req)?;

    Ok(DeactivateResponse {})
}
