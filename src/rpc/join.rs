use std::collections::HashSet;
use std::time::Duration;

use crate::cas;
use crate::failure_domain::FailureDomain;
use crate::has_grades;
use crate::instance::grade::{CurrentGrade, TargetGrade};
use crate::instance::{Instance, InstanceId};
use crate::replicaset::ReplicasetId;
use crate::storage::{Clusterwide, ToEntryIter as _};
use crate::storage::{ClusterwideSpaceId, PropertyName};
use crate::traft::op::{Dml, Op};
use crate::traft::{self, RaftId};
use crate::traft::{error::Error, node, Address, PeerAddress, Result};

use ::tarantool::fiber;

const TIMEOUT: Duration = Duration::from_secs(10);

crate::define_rpc_request! {
    /// Submits a request to join a new instance to the cluster. If successful, the information about
    /// the new instance and its address will be replicated on all of the cluster instances
    /// through Raft.
    ///
    /// Can be called by a joining instance on any instance that has already joined the cluster.
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving instance is not yet initialized
    /// 2. Storage failure
    /// 3. Incorrect request (e.g. instance already joined or an error in validation of failure domains)
    /// 4. Compare and swap request to commit new instance and its address failed
    /// with an error that cannot be retried.
    fn proc_raft_join(req: Request) -> Result<Response> {
        handle_join_request_and_wait(req, TIMEOUT)
    }

    /// Request to join the cluster.
    pub struct Request {
        pub cluster_id: String,
        pub instance_id: Option<InstanceId>,
        pub replicaset_id: Option<ReplicasetId>,
        pub advertise_address: String,
        pub failure_domain: FailureDomain,
    }

    pub struct Response {
        pub instance: Box<Instance>,
        /// Addresses of other peers in a cluster.
        /// They are needed for Raft node to communicate with other nodes
        /// at startup.
        pub peer_addresses: Vec<PeerAddress>,
        /// Replication sources in a replica set that the joining instance will belong to.
        /// See [tarantool documentation](https://www.tarantool.io/en/doc/latest/reference/configuration/#confval-replication)
        pub box_replication: Vec<Address>,
    }
}

/// Processes the [`rpc::join::Request`] and appends necessary
/// entries to the raft log (if successful).
///
/// Returns the [`Response`] containing the resulting [`Instance`] when the entry is committed.
// TODO: to make this function async and have an outer timeout,
// wait_* fns also need to be async.
pub fn handle_join_request_and_wait(req: Request, timeout: Duration) -> Result<Response> {
    let node = node::global()?;
    let cluster_id = node.raft_storage.cluster_id()?;
    let storage = &node.storage;
    let raft_storage = &node.raft_storage;

    if req.cluster_id != cluster_id {
        return Err(Error::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id,
            cluster_cluster_id: cluster_id,
        });
    }

    let deadline = fiber::clock().saturating_add(timeout);
    loop {
        let instance = build_instance(
            req.instance_id.as_ref(),
            req.replicaset_id.as_ref(),
            &req.failure_domain,
            storage,
        )
        .map_err(raft::Error::ConfChangeError)?;
        let mut replication_addresses = storage
            .peer_addresses
            .addresses_by_ids(replication_ids(&instance.replicaset_id, storage))?;
        replication_addresses.insert(req.advertise_address.clone());
        let peer_address = traft::PeerAddress {
            raft_id: instance.raft_id,
            address: req.advertise_address.clone(),
        };
        let op_addr = Dml::replace(ClusterwideSpaceId::Address, &peer_address)
            .expect("encoding should not fail");
        let op_instance = Dml::replace(ClusterwideSpaceId::Instance, &instance)
            .expect("encoding should not fail");
        let ranges = vec![
            cas::Range::new(ClusterwideSpaceId::Instance),
            cas::Range::new(ClusterwideSpaceId::Address),
            cas::Range::new(ClusterwideSpaceId::Property).eq((PropertyName::ReplicationFactor,)),
        ];
        macro_rules! handle_result {
            ($res:expr) => {
                match $res {
                    Ok((index, term)) => {
                        node.wait_index(index, deadline.duration_since(fiber::clock()))?;
                        if term != raft::Storage::term(raft_storage, index)? {
                            // leader switched - retry
                            node.wait_status();
                            continue;
                        }
                    }
                    Err(err) => {
                        if err.is_cas_err() | err.is_term_mismatch_err() {
                            // cas error - retry
                            fiber::sleep(Duration::from_millis(500));
                            continue;
                        } else {
                            return Err(err);
                        }
                    }
                }
            };
        }
        // Only in this order - so that when instance exists - address will always be there.
        handle_result!(cas::compare_and_swap(
            Op::Dml(op_addr),
            cas::Predicate {
                index: raft_storage.applied()?,
                term: raft_storage.term()?,
                ranges: ranges.clone(),
            },
            deadline.duration_since(fiber::clock()),
        ));
        handle_result!(cas::compare_and_swap(
            Op::Dml(op_instance),
            cas::Predicate {
                index: raft_storage.applied()?,
                term: raft_storage.term()?,
                ranges,
            },
            deadline.duration_since(fiber::clock()),
        ));
        node.main_loop.wakeup();

        // A joined instance needs to communicate with other nodes.
        // TODO: limit the number of entries sent to reduce response size.
        let peer_addresses = node.storage.peer_addresses.iter()?.collect();
        return Ok(Response {
            instance: instance.into(),
            peer_addresses,
            box_replication: replication_addresses.into_iter().collect(),
        });
    }
}

pub fn build_instance(
    instance_id: Option<&InstanceId>,
    replicaset_id: Option<&ReplicasetId>,
    failure_domain: &FailureDomain,
    storage: &Clusterwide,
) -> std::result::Result<Instance, String> {
    if let Some(id) = instance_id {
        let existing_instance = storage.instances.get(id);
        if matches!(existing_instance, Ok(instance) if has_grades!(instance, Online -> *)) {
            let e = format!("{} is already joined", id);
            return Err(e);
        }
    }

    failure_domain.check(&storage.cache().failure_domain_names)?;

    // Anyway, `join` always produces a new raft_id.
    let raft_id = storage.cache().max_raft_id + 1;
    let instance_id = instance_id
        .map(Clone::clone)
        .unwrap_or_else(|| choose_instance_id(raft_id, storage));
    let replicaset_id = replicaset_id
        .map(Clone::clone)
        .unwrap_or_else(|| choose_replicaset_id(failure_domain, storage));

    let instance = Instance::new(
        Some(raft_id),
        Some(instance_id),
        Some(replicaset_id),
        CurrentGrade::offline(0),
        TargetGrade::offline(0),
        failure_domain.clone(),
    );
    Ok(instance)
}

/// Choose [`InstanceId`] based on `raft_id`.
fn choose_instance_id(raft_id: RaftId, storage: &Clusterwide) -> InstanceId {
    let mut suffix: Option<u64> = None;
    loop {
        let ret = match suffix {
            None => format!("i{raft_id}"),
            Some(x) => format!("i{raft_id}-{x}"),
        }
        .into();

        if !storage
            .instances
            .contains(&ret)
            .expect("storage should not fail")
        {
            return ret;
        }

        suffix = Some(suffix.map_or(2, |x| x + 1));
    }
}

/// Choose a [`ReplicasetId`] for a new instance given its `failure_domain`.
fn choose_replicaset_id(failure_domain: &FailureDomain, storage: &Clusterwide) -> ReplicasetId {
    'next_replicaset: for (replicaset_id, instances) in storage.cache().replicasets.iter() {
        let replication_factor = storage
            .properties
            .replication_factor()
            .expect("storage should not fail");

        if instances.len() < replication_factor {
            for instance_id in instances {
                let instance = storage.instances.get(instance_id).unwrap();
                if instance.failure_domain.intersects(failure_domain) {
                    continue 'next_replicaset;
                }
            }
            return replicaset_id.clone();
        }
    }

    let mut i = 0u64;
    loop {
        i += 1;
        let replicaset_id = ReplicasetId(format!("r{i}"));
        if storage.cache().replicasets.get(&replicaset_id).is_none() {
            return replicaset_id;
        }
    }
}

/// Get ids of instances that are part of a replicaset with `replicaset_id`.
pub fn replication_ids(replicaset_id: &ReplicasetId, storage: &Clusterwide) -> HashSet<RaftId> {
    if let Some(replication_ids) = storage.cache().replicasets.get(replicaset_id) {
        replication_ids
            .iter()
            .map(|id| {
                let instance = storage.instances.get(id).expect("storage should not fail");
                instance.raft_id
            })
            .collect()
    } else {
        HashSet::new()
    }
}
