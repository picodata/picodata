//! Picodata synchronization primitives.

use crate::instance::InstanceName;
use crate::rpc::RequestArgs;
use crate::topology_cache::TopologyCache;
use crate::traft::error::Error;
#[allow(unused_imports)]
use crate::traft::network::ConnectionPool;
use crate::traft::RaftIndex;
use crate::util::duration_from_secs_f64_clamped;
use crate::{proc_name, tlog};
use crate::{rpc, traft};
use ::tarantool::tuple::Encode;
use ::tarantool::vclock::Vclock;
use ::tarantool::{fiber, proc};
use futures::stream::FuturesOrdered;
use futures::{Future, StreamExt};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Duration;
use tarantool::time::Instant;

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
            return Err(Error::timeout());
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
    crate::error_injection!(block "BLOCK_PROC_WAIT_INDEX");
    node.wait_index(target, duration_from_secs_f64_clamped(timeout))
        .map(|index| (index,))
}

/// Wait the given index to be applied on all replicasets.
/// Specific waiting errors are reported in the log indicating the instance.
///
/// Note: The client must ensure that the term remains unchanged, otherwise, the operation on the
/// index may be changed. It's safe to wait for an index after committing it.
pub fn wait_for_index_globally(
    topology: &TopologyCache,
    pool: Rc<ConnectionPool>,
    index: RaftIndex,
    deadline: Instant,
) -> traft::Result<()> {
    fn broadcast_wait_index_rpc(
        pool: &ConnectionPool,
        targets: &[&&InstanceName],
        index: &RaftIndex,
        deadline: &Instant,
    ) -> traft::Result<
        FuturesOrdered<
            impl Future<Output = Result<<WaitIndexRpc as RequestArgs>::Response, Error>>,
        >,
    > {
        let mut fs = FuturesOrdered::new();
        let timeout = std::cmp::min(
            // TODO: don't hardcode timeout
            Duration::from_secs(1),
            deadline.duration_since(Instant::now_fiber()),
        );
        let rpc = crate::sync::WaitIndexRpc {
            target: *index,
            timeout: timeout.as_secs_f64(),
        };

        for instance_name in targets {
            tlog!(Debug, "calling proc_wait_index"; "instance_name" => %instance_name);
            let resp = pool.call(**instance_name, proc_name!(proc_wait_index), &rpc, timeout)?;
            fs.push_back(resp);
        }

        Ok(fs)
    }

    let instances_values: Vec<InstanceName>;
    let instances: HashSet<&InstanceName>;
    {
        // `topology_ref` is `NoYieldsRef` so we cannot use it in `fiber::block_on` below
        // that's why we need to collect all instance names here.
        let topology_ref = topology.get();
        instances_values = topology_ref
            .all_instances()
            .filter(|instance| instance.may_respond())
            .map(|instance| instance.name.clone())
            .collect();
        instances = instances_values.iter().collect();
    }

    fiber::block_on(async {
        let mut confirmed = HashSet::new();

        let mut start = Instant::now_fiber();
        loop {
            let confirmed_copy = confirmed.clone();
            let unconfirmed: Vec<_> = instances.difference(&confirmed_copy).collect();
            if unconfirmed.is_empty() {
                return Ok(());
            }

            if Instant::now_fiber() > deadline {
                for instance in unconfirmed {
                    tlog!(
                        Warning,
                        "{} hasn't confirmed operation commitment within the timeout",
                        *instance
                    )
                }
                return Err(Error::timeout());
            }

            let fs = broadcast_wait_index_rpc(&pool, &unconfirmed, &index, &deadline)?;
            let mut fs_enumerated = fs.enumerate();
            while let Some((idx, res)) = &fs_enumerated.next().await {
                let instance_name = *unconfirmed[*idx];
                match res {
                    Ok(_) => {
                        confirmed.insert(instance_name);
                    }
                    Err(err) => {
                        tlog!(Warning, "failed proc_wait_index: {err}"; "instance_name" => %instance_name);
                    }
                }
            }

            const WAIT_INDEX_RETRY_TIMEOUT: Duration = Duration::from_millis(300);
            // Make sure we yield for some time to avoid infinite looping
            fiber::sleep(
                WAIT_INDEX_RETRY_TIMEOUT.saturating_sub(Instant::now_fiber().duration_since(start)),
            );
            start = Instant::now_fiber();
        }
    })?;

    Ok(())
}

mod tests {
    use super::*;

    use crate::instance::Instance;
    use crate::traft::network::ConnectionPool;

    #[::tarantool::test]
    async fn vclock_proc() {
        let node = traft::node::Node::for_tests();

        // Connect to the current Tarantool instance
        let pool = ConnectionPool::new(node.storage.clone(), Default::default());
        let l = ::tarantool::lua_state();
        let listen: SmolStr = l.eval("return box.info.listen").unwrap();

        let instance = Instance {
            raft_id: 1337,
            name: "default_1_1".into(),
            ..Instance::default()
        };
        node.storage.instances.put(&instance).unwrap();
        node.storage
            .peer_addresses
            .put(instance.raft_id, &listen, &traft::ConnectionType::Iproto)
            .unwrap();
        crate::init_stored_procedures();

        let result = pool
            .call(
                &instance.raft_id,
                crate::proc_name!(proc_get_vclock),
                &GetVclockRpc {},
                Duration::MAX,
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
            Duration::MAX,
        )
        .unwrap()
        .await
        .unwrap();
    }
}
