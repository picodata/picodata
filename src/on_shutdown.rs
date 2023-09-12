use std::time::Duration;

use ::tarantool::fiber;

use crate::has_grades;
use crate::tlog;
use crate::traft::event;
use crate::traft::node;
use crate::unwrap_ok_or;

pub async fn callback() {
    let node = node::global().unwrap();

    // 1. Wake up the sentinel so it starts trying to set target grade Offline.
    node.sentinel_loop.on_shut_down();
    fiber::reschedule();

    // 2. Meanwhile, wait until either it succeeds or there is no quorum.
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

        let voters = node.raft_storage.voters().expect("failed reading voters");

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

        if let Err(e) = event::wait_timeout(event::Event::EntryApplied, Duration::MAX) {
            tlog!(Warning, "failed to shutdown gracefully: {e}");
        }
    }
}
