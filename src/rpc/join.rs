use std::collections::BTreeMap;
use std::time::Duration;

use crate::cas;
use crate::failure_domain::FailureDomain;
use crate::has_states;
use crate::instance::State;
use crate::instance::StateVariant::*;
use crate::instance::{Instance, InstanceName};
use crate::replicaset::Replicaset;
use crate::replicaset::ReplicasetId;
use crate::schema::ADMIN_ID;
use crate::storage::ClusterwideTable;
use crate::storage::{Clusterwide, ToEntryIter as _};
use crate::tier::Tier;
use crate::traft::op::{Dml, Op};
use crate::traft::{self, RaftId};
use crate::traft::{error::Error, node, Address, PeerAddress, Result};

use tarantool::fiber;

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
        pub cluster_name: String,
        pub instance_name: Option<InstanceName>,
        pub replicaset_id: Option<ReplicasetId>,
        pub advertise_address: String,
        pub failure_domain: FailureDomain,
        pub tier: String,
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

/// Processes the [`crate::rpc::join::Request`] and appends necessary
/// entries to the raft log (if successful).
///
/// Returns the [`Response`] containing the resulting [`Instance`] when the entry is committed.
// TODO: to make this function async and have an outer timeout,
// wait_* fns also need to be async.
pub fn handle_join_request_and_wait(req: Request, timeout: Duration) -> Result<Response> {
    let node = node::global()?;
    let cluster_name = node.raft_storage.cluster_name()?;
    let storage = &node.storage;
    let guard = node.instances_update.lock();

    if req.cluster_name != cluster_name {
        return Err(Error::ClusterIdMismatch {
            instance_cluster_name: req.cluster_name,
            cluster_name,
        });
    }

    let deadline = fiber::clock().saturating_add(timeout);
    loop {
        let instance = build_instance(
            req.instance_name.as_ref(),
            req.replicaset_id.as_ref(),
            &req.failure_domain,
            storage,
            &req.tier,
        )?;
        let peer_address = traft::PeerAddress {
            raft_id: instance.raft_id,
            address: req.advertise_address.clone(),
        };

        let mut ops = Vec::with_capacity(3);
        ops.push(
            Dml::replace(ClusterwideTable::Address, &peer_address, ADMIN_ID)
                .expect("encoding should not fail"),
        );
        ops.push(
            Dml::replace(ClusterwideTable::Instance, &instance, ADMIN_ID)
                .expect("encoding should not fail"),
        );

        if storage.replicasets.get(&instance.replicaset_id)?.is_none() {
            let replicaset = Replicaset::with_one_instance(&instance);
            ops.push(
                Dml::insert(ClusterwideTable::Replicaset, &replicaset, ADMIN_ID)
                    .expect("encoding should not fail"),
            );
        }

        let ranges = vec![
            cas::Range::new(ClusterwideTable::Instance),
            cas::Range::new(ClusterwideTable::Address),
            cas::Range::new(ClusterwideTable::Tier),
            cas::Range::new(ClusterwideTable::Replicaset),
        ];
        let predicate = cas::Predicate::with_applied_index(ranges);
        let cas_req = crate::cas::Request::new(Op::BatchDml { ops }, predicate, ADMIN_ID)?;
        let res = cas::compare_and_swap_and_wait(&cas_req, deadline)?;
        if let Some(e) = res.into_retriable_error() {
            crate::tlog!(Debug, "CaS rejected: {e}");
            fiber::sleep(Duration::from_millis(250));
            continue;
        }

        node.main_loop.wakeup();

        // A joined instance needs to communicate with other nodes.
        // TODO: limit the number of entries sent to reduce response size.
        let peer_addresses = node.storage.peer_addresses.iter()?.collect();
        let mut replication_addresses = storage.peer_addresses.addresses_by_ids(
            storage
                .instances
                .replicaset_instances(&instance.replicaset_id)
                .expect("storage should not fail")
                .map(|i| i.raft_id),
        )?;
        replication_addresses.insert(req.advertise_address.clone());

        drop(guard);
        return Ok(Response {
            instance: instance.into(),
            peer_addresses,
            box_replication: replication_addresses.into_iter().collect(),
        });
    }
}

pub fn build_instance(
    instance_name: Option<&InstanceName>,
    replicaset_id: Option<&ReplicasetId>,
    failure_domain: &FailureDomain,
    storage: &Clusterwide,
    tier: &str,
) -> Result<Instance> {
    if let Some(id) = instance_name {
        if let Ok(existing_instance) = storage.instances.get(id) {
            let is_expelled = has_states!(existing_instance, Expelled -> *);
            if is_expelled {
                // The instance was expelled explicitly, it's ok to replace it
            } else {
                // NOTE: We used to allow the so called "auto expel", i.e.
                // joining an instance with the same name as an existing but
                // offline instance. But we no longer allow this, because it
                // could lead to race conditions, because when an instance is
                // joined it has both states Offline, which means it may be
                // replaced by another one of the name before it sends a request
                // for self activation.
                return Err(Error::other(format!("`{id}` is already joined")));
            }
        }
    }
    let Some(tier) = storage
        .tiers
        .by_name(tier)
        .expect("storage should not fail")
    else {
        return Err(Error::other(format!(r#"tier "{tier}" doesn't exist"#)));
    };

    let existing_fds = storage
        .instances
        .failure_domain_names()
        .expect("storage should not fail");
    failure_domain.check(&existing_fds)?;

    // Anyway, `join` always produces a new raft_id.
    let raft_id = storage
        .instances
        .max_raft_id()
        .expect("storage should not fail")
        + 1;
    let instance_name = instance_name
        .cloned()
        .unwrap_or_else(|| choose_instance_name(raft_id, storage));
    let replicaset_id = match replicaset_id {
        Some(replicaset_id) => replicaset_id.clone(),
        None => choose_replicaset_id(failure_domain, storage, &tier)?,
    };

    let instance_uuid = uuid::Uuid::new_v4().to_hyphenated().to_string();
    let replicaset_uuid;
    if let Some(replicaset) = storage.replicasets.get(&replicaset_id)? {
        if replicaset.tier != tier.name {
            return Err(Error::other(format!("tier mismatch: instance {instance_name} is from tier: '{}', but replicaset {replicaset_id} is from tier: '{}'", tier.name, replicaset.tier)));
        }
        replicaset_uuid = replicaset.uuid;
    } else {
        replicaset_uuid = uuid::Uuid::new_v4().to_hyphenated().to_string();
    }

    Ok(Instance {
        raft_id,
        name: instance_name,
        uuid: instance_uuid,
        replicaset_id,
        replicaset_uuid,
        current_state: State::new(Offline, 0),
        target_state: State::new(Offline, 0),
        failure_domain: failure_domain.clone(),
        tier: tier.name.clone(),
    })
}

// TODO: choose instance name based on tier name instead
/// Choose [`InstanceName`] based on `raft_id`.
fn choose_instance_name(raft_id: RaftId, storage: &Clusterwide) -> InstanceName {
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

/// Choose a [`ReplicasetId`] for a new instance given its `failure_domain` and `tier`.
fn choose_replicaset_id(
    failure_domain: &FailureDomain,
    storage: &Clusterwide,
    Tier {
        replication_factor,
        name: tier_name,
        ..
    }: &Tier,
) -> Result<ReplicasetId> {
    // `BTreeMap` is used so that we get a determenistic order of instance addition to replicasets.
    // E.g. if both "r1" and "r2" are suitable, "r1" will always be prefered.
    let mut replicasets: BTreeMap<_, Vec<_>> = BTreeMap::new();
    let replication_factor = (*replication_factor).into();
    for instance in storage
        .instances
        .all_instances()
        .expect("storage should not fail")
        .into_iter()
    {
        replicasets
            .entry(instance.replicaset_id.clone())
            .or_default()
            .push(instance);
    }
    'next_replicaset: for (replicaset_id, instances) in replicasets.iter() {
        if instances.len() < replication_factor
            && instances
                .first()
                .expect("should not fail, each replicaset consists of at least one instance")
                .tier
                == *tier_name
        {
            for instance in instances {
                if instance.failure_domain.intersects(failure_domain) {
                    continue 'next_replicaset;
                }
            }
            return Ok(replicaset_id.clone());
        }
    }

    let mut i = 0u64;
    loop {
        i += 1;
        let replicaset_id = ReplicasetId(format!("r{i}"));
        if !replicasets.contains_key(&replicaset_id) {
            return Ok(replicaset_id);
        }
    }
}
