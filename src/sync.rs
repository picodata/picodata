//! Picodata synchronization primitives.

use ::tarantool::tuple::Encode;
use ::tarantool::vclock::Vclock;
use ::tarantool::{fiber, proc};
use serde::{Deserialize, Serialize};

use std::time::Duration;

use crate::tlog;
use crate::traft::error::Error;
#[allow(unused_imports)]
use crate::traft::network::ConnectionPool;
use crate::traft::RaftIndex;
use crate::util::duration_from_secs_f64_clamped;
use crate::{rpc, traft};

////////////////////////////////////////////////////////////////////////////////
// proc_get_vclock
////////////////////////////////////////////////////////////////////////////////

/// A stored procedure to get current [`Vclock`].
///
/// See [`Vclock::try_current`]
#[proc]
fn proc_get_vclock() -> traft::Result<Vclock> {
    let vclock = Vclock::try_current()?;
    Ok(vclock)
}

/// RPC request to [`proc_wait_vclock`].
///
/// Can be used in [`ConnectionPool::call`] to call [`proc_wait_vclock`] on
/// the corresponding instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetVclockRpc {}

impl Encode for GetVclockRpc {}

impl rpc::RequestArgs for GetVclockRpc {
    const PROC_NAME: &'static str = crate::proc_name!(proc_get_vclock);
    type Response = Vclock;
}

////////////////////////////////////////////////////////////////////////////////
// proc_wait_vclock
////////////////////////////////////////////////////////////////////////////////

/// RPC request to [`proc_wait_vclock`].
///
/// Can be used in [`ConnectionPool::call`] to call [`proc_wait_vclock`] on
/// the corresponding instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WaitVclockRpc {
    target: Vclock,
    timeout: f64,
}

impl Encode for WaitVclockRpc {}

impl rpc::RequestArgs for WaitVclockRpc {
    const PROC_NAME: &'static str = crate::proc_name!(proc_wait_vclock);
    type Response = Vclock;
}

/// A stored procedure to wait for `target` [`Vclock`].
///
/// See [`wait_vclock`]
#[proc]
fn proc_wait_vclock(target: Vclock, timeout: f64) -> traft::Result<Vclock> {
    wait_vclock(target, duration_from_secs_f64_clamped(timeout))
}

/// Block current fiber until Tarantool [`Vclock`] reaches the `target`.
///
/// Returns the actual Vclock value. It can be equal to or greater than the
/// target one. If timeout expires beforehand, the function returns
/// `Err(TimeoutError)`.
///
/// **This function yields**
///
pub fn wait_vclock(target: Vclock, timeout: Duration) -> traft::Result<Vclock> {
    let target = target.ignore_zero();
    let deadline = fiber::clock().saturating_add(timeout);
    tlog!(Debug, "waiting for vclock {target:?}");
    loop {
        let current = Vclock::current().ignore_zero();
        if current >= target {
            #[rustfmt::skip]
            tlog!(Debug, "done waiting for vclock {target:?}, current: {current:?}");
            return Ok(current);
        }

        if fiber::clock() < deadline {
            fiber::sleep(traft::node::MainLoop::TICK);
        } else {
            #[rustfmt::skip]
            tlog!(Debug, "failed waiting for vclock {target:?}: timeout, current: {current:?}");
            return Err(Error::Timeout);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// proc_get_index
////////////////////////////////////////////////////////////////////////////////

/// A stored procedure to get current [`RaftIndex`].
///
/// See [Node::get_index](traft::node::Node::get_index)
#[proc]
fn proc_get_index() -> traft::Result<RaftIndex> {
    let node = traft::node::global()?;
    Ok(node.get_index())
}

////////////////////////////////////////////////////////////////////////////////
// proc_read_index
////////////////////////////////////////////////////////////////////////////////

/// RPC request to [`proc_read_index`].
///
/// Can be used in [`ConnectionPool::call`] to call [`proc_read_index`] on
/// the corresponding instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadIndexRpc {
    timeout: f64,
}

impl Encode for ReadIndexRpc {}

impl rpc::RequestArgs for ReadIndexRpc {
    const PROC_NAME: &'static str = crate::proc_name!(proc_read_index);
    type Response = (RaftIndex,);
}

/// A stored procedure to perforam a quorum read of [`RaftIndex`].
///
/// See [Node::read_index](traft::node::Node::read_index)
#[proc]
fn proc_read_index(timeout: f64) -> traft::Result<(RaftIndex,)> {
    let node = traft::node::global()?;
    node.read_index(duration_from_secs_f64_clamped(timeout))
        .map(|index| (index,))
}

/// RPC request to [`proc_wait_index`].
///
/// Can be used in [`ConnectionPool::call`] to call [`proc_wait_index`] on
/// the corresponding instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WaitIndexRpc {
    target: RaftIndex,
    timeout: f64,
}

impl Encode for WaitIndexRpc {}

impl rpc::RequestArgs for WaitIndexRpc {
    const PROC_NAME: &'static str = crate::proc_name!(proc_wait_index);
    type Response = (RaftIndex,);
}

/// A stored procedure to wait for `target` [`RaftIndex`].
///
/// See [Node::wait_index](traft::node::Node::wait_index)
#[proc]
fn proc_wait_index(target: RaftIndex, timeout: f64) -> traft::Result<(RaftIndex,)> {
    let node = traft::node::global()?;
    node.wait_index(target, duration_from_secs_f64_clamped(timeout))
        .map(|index| (index,))
}

mod tests {
    use super::*;

    use crate::instance::Instance;
    use crate::storage::Clusterwide;
    use crate::traft::network::ConnectionPool;

    #[::tarantool::test]
    async fn vclock_proc() {
        let storage = Clusterwide::for_tests();
        // Connect to the current Tarantool instance
        let pool = ConnectionPool::new(storage.clone(), Default::default());
        let l = ::tarantool::lua_state();
        let listen: String = l.eval("return box.info.listen").unwrap();

        let instance = Instance {
            raft_id: 1337,
            ..Instance::default()
        };
        storage.instances.put(&instance).unwrap();
        storage
            .peer_addresses
            .put(instance.raft_id, &listen)
            .unwrap();
        crate::init_handlers();

        let result = pool
            .call(
                &instance.raft_id,
                crate::proc_name!(proc_get_vclock),
                &GetVclockRpc {},
                None,
            )
            .unwrap()
            .await
            .unwrap();
        assert_eq!(result, Vclock::current());

        pool.call(
            &instance.raft_id,
            crate::proc_name!(proc_wait_vclock),
            &WaitVclockRpc {
                target: Vclock::current(),
                timeout: 1.0,
            },
            None,
        )
        .unwrap()
        .await
        .unwrap();
    }
}
