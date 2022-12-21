use std::time::{Duration, Instant};

use ::tarantool::fiber;

use crate::has_grades;
use crate::instance::grade::TargetGradeVariant;
use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::rpc;
use crate::traft::rpc::update_instance;
use crate::unwrap_ok_or;

pub async fn callback() {
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
            node.storage.instances.get(&raft_id),
            Err(e) => {
                tlog!(Error, "{e}");
                break;
            }
        );

        if has_grades!(me, Offline -> *) {
            tlog!(Info, "graceful shutdown succeeded");

            // Dirty hack. Wait a little bit more before actually
            // shutting down. Raft commit index is a local value. Other
            // nodes may still be unaware that `me.current_grade` is
            // commmitted. Give them some more time to communicate.
            fiber::sleep(Duration::from_millis(100));
            break;
        }

        let voters = node
            .raft_storage
            .voters()
            .expect("failed reading voters")
            .unwrap_or_default();

        let quorum = voters.len() / 2 + 1;
        let voters_alive = voters
            .iter()
            .filter_map(|raft_id| node.storage.instances.get(raft_id).ok())
            .filter(|instance| has_grades!(instance, Online -> *))
            .count();

        if voters_alive < quorum {
            tlog!(Info, "giving up, there is no quorum");
            break;
        }

        if let Err(e) = node.governor_loop.awoken().await {
            tlog!(Warning, "failed to shutdown gracefully: {e}");
        }
    }
}

fn go_offline() -> traft::Result<()> {
    let node = node::global()?;
    let raft_id = node.raft_id();

    let instance = node.storage.instances.get(&raft_id)?;
    let cluster_id = node
        .raft_storage
        .cluster_id()?
        .ok_or_else(|| Error::other("missing cluster_id value in storage"))?;

    let req = update_instance::Request::new(instance.instance_id, cluster_id)
        .with_target_grade(TargetGradeVariant::Offline);

    loop {
        let Some(leader_id) = node.status().leader_id else {
            node.wait_status();
            continue
        };

        let now = Instant::now();
        let wait_before_retry = Duration::from_millis(300);

        if leader_id == raft_id {
            match node.handle_update_instance_request_and_wait(req.clone()) {
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

        let Some(leader_address) = node.storage.peer_addresses.get(leader_id)? else {
            // Leader address is unknown, maybe later we'll find it out?
            fiber::sleep(wait_before_retry.saturating_sub(now.elapsed()));
            continue;
        };
        let res = match rpc::net_box_call(&leader_address, &req, Duration::MAX) {
            Ok(update_instance::Response::Ok) => Ok(()),
            Ok(update_instance::Response::ErrNotALeader) => Err(Error::NotALeader),
            Err(e) => Err(e.into()),
        };

        match res {
            Ok(()) => break Ok(()),
            Err(e) => {
                tlog!(Warning,
                    "failed setting target grade Offline: {e}, retrying ...";
                    "raft_id" => leader_id,
                );
                fiber::sleep(wait_before_retry.saturating_sub(now.elapsed()));
                continue;
            }
        };
    }
}
