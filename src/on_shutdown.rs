use crate::has_states;
use crate::tarantool;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::Result;
use crate::unwrap_ok_or;
use ::tarantool::fiber;
use std::time::Duration;

const ON_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

pub fn setup_on_shutdown_trigger() -> Result<()> {
    let lua = ::tarantool::lua_state();
    let timeout = get_on_shutdown_timeout()?;
    lua.exec_with("box.ctl.set_on_shutdown_timeout(...)", timeout)?;

    tarantool::on_shutdown(move || fiber::block_on(callback()))?;

    Ok(())
}

pub async fn callback() {
    fiber::set_name("on_shutdown");

    tlog!(Info, "shutting down");

    let node = node::global().unwrap();

    // 1. Wake up the sentinel so it starts trying to set target state Offline.
    node.sentinel_loop.on_shut_down();

    node.plugin_manager.handle_instance_shutdown();

    // Let other fibers handle the shut down.
    // Note that this skips the epoll_wait call,
    // which is fine in this case because it is not called inside a loop.
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

        if has_states!(me, Expelled -> *) {
            tlog!(Info, "instance has been expelled");
            break;
        }

        if has_states!(me, Offline -> *) {
            tlog!(Info, "graceful shutdown succeeded");

            // Dirty hack. Wait a little bit more before actually
            // shutting down. Raft commit index is a local value. Other
            // nodes may still be unaware that `me.current_state` is
            // commmitted. Give them some more time to communicate.
            fiber::sleep(Duration::from_millis(100));
            break;
        }

        let voters = node.raft_storage.voters().expect("failed reading voters");

        let quorum = voters.len() / 2 + 1;
        let voters_alive = voters
            .iter()
            .filter_map(|raft_id| node.storage.instances.get(raft_id).ok())
            .filter(|instance| has_states!(instance, Online -> *))
            .count();

        if voters_alive < quorum {
            tlog!(Info, "giving up, there is no quorum");
            break;
        }

        let applied = node.get_index();
        if let Err(e) = node.wait_index(applied + 1, Duration::MAX) {
            tlog!(Warning, "failed to shutdown gracefully: {e}");
        }
    }
}

fn get_on_shutdown_timeout() -> Result<f64> {
    let timeout = ON_SHUTDOWN_TIMEOUT.as_secs_f64();

    let res = std::env::var("PICODATA_INTERNAL_ON_SHUTDOWN_TIMEOUT");
    let Ok(timeout_string) = res else {
        return Ok(timeout);
    };

    let res = timeout_string.parse::<f64>();
    let timeout = match res {
        Ok(v) => v,
        Err(e) => {
            return Err(Error::invalid_configuration(format!("PICODATA_INTERNAL_ON_SHUTDOWN_TIMEOUT: expected a number, got '{timeout_string}': {e}")));
        }
    };

    Ok(timeout)
}
