//! Picodata synchronization primitives.

use ::tarantool::tuple::Encode;
use ::tarantool::vclock::Vclock;
use ::tarantool::{fiber, proc};
use futures::stream::FuturesOrdered;
use futures::{Future, StreamExt};
use serde::{Deserialize, Serialize};

use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::Duration;

use crate::instance::InstanceName;
use crate::rpc::RequestArgs;
use crate::storage::{Catalog, ToEntryIter};
use crate::traft::error::Error;
#[allow(unused_imports)]
use crate::traft::network::ConnectionPool;
use crate::traft::RaftIndex;
use crate::util::duration_from_secs_f64_clamped;
use crate::{proc_name, tlog};
use crate::{rpc, traft};
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
    storage: &Catalog,
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
            tlog!(Info, "calling proc_wait_index"; "instance_name" => %instance_name);
            let resp = pool.call(**instance_name, proc_name!(proc_wait_index), &rpc, timeout)?;
            fs.push_back(resp);
        }

        Ok(fs)
    }

    let replicasets: Vec<_> = storage
        .replicasets
        .iter()
        .expect("storage should never fail")
        .collect();
    let replicasets: HashMap<_, _> = replicasets.iter().map(|rs| (&rs.name, rs)).collect();
    let instances = storage
        .instances
        .all_instances()
        .expect("storage should never fail");

    fiber::block_on(async {
        let mut confirmed = HashSet::new();

        loop {
            let masters: HashSet<_> = crate::rpc::replicasets_masters(&replicasets, &instances)
                .into_iter()
                .map(|(instance_name, _)| instance_name)
                .collect();
            let confirmed_copy = confirmed.clone();
            let unconfirmed: Vec<_> = masters.difference(&confirmed_copy).collect();
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
                return Err(Error::Timeout);
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
        }
    })?;

    Ok(())
}

mod tests {
    use super::*;

    use crate::instance::Instance;
    use crate::storage::Catalog;
    use crate::traft::network::ConnectionPool;

    #[::tarantool::test]
    async fn vclock_proc() {
        let storage = Catalog::for_tests();
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
            .put(instance.raft_id, &listen, &traft::ConnectionType::Iproto)
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
