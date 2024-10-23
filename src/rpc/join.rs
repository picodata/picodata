use crate::cas;
use crate::failure_domain::FailureDomain;
use crate::has_states;
use crate::instance::State;
use crate::instance::StateVariant::*;
use crate::instance::{Instance, InstanceName};
use crate::replicaset::Replicaset;
use crate::replicaset::ReplicasetName;
use crate::schema::ADMIN_ID;
use crate::storage::ClusterwideTable;
use crate::storage::{Clusterwide, ToEntryIter as _};
use crate::tier::Tier;
use crate::tlog;
use crate::traft::op::{Dml, Op};
use crate::traft::{self, RaftId};
use crate::traft::{error::Error, node, Address, PeerAddress, Result};
use std::collections::HashSet;
use std::time::Duration;
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
        let res = handle_join_request_and_wait(req, TIMEOUT)?;

        tlog!(Info, "new instance joined the cluster: {:?}", res.instance);
        Ok(res)
    }

    /// Request to join the cluster.
    pub struct Request {
        pub cluster_name: String,
        pub instance_name: Option<InstanceName>,
        pub replicaset_name: Option<ReplicasetName>,
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
            req.replicaset_name.as_ref(),
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

        if storage
            .replicasets
            .get(&instance.replicaset_name)?
            .is_none()
        {
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
        let replicas = storage
            .instances
            .replicaset_instances(&instance.replicaset_name)
            .expect("storage should not fail")
            // Ignore expelled instances
            .filter(|i| !has_states!(i, Expelled -> *))
            .map(|i| i.raft_id);
        let mut replication_addresses = storage.peer_addresses.addresses_by_ids(replicas)?;
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
    requested_instance_name: Option<&InstanceName>,
    requested_replicaset_name: Option<&ReplicasetName>,
    failure_domain: &FailureDomain,
    storage: &Clusterwide,
    tier: &str,
) -> Result<Instance> {
    // NOTE: currently we don't ever remove entries from `_pico_instance` even
    // when expelling instances. This makes it so we can get a unique raft_id by
    // selecting max raft_id from _pico_instance and adding one. However in the
    // future we may want to start deleting old instance records and at that
    // point we may face a problem of this id not being unique (i.e. belonging
    // to an instance). There doesn't seem to be any problems with this per se,
    // as raft will not allow there to be a simultaneous raft_id conflict, but
    // it's just a thing to look out for.
    let raft_id = storage
        .instances
        .max_raft_id()
        .expect("storage should not fail")
        + 1;

    // Resolve instance_name
    let instance_name;
    if let Some(name) = requested_instance_name {
        if let Ok(existing_instance) = storage.instances.get(name) {
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
                return Err(Error::other(format!("`{name}` is already joined")));
            }
        }
        instance_name = name.clone();
    } else {
        instance_name = choose_instance_name(raft_id, storage);
    }

    // Check tier exists
    let Some(tier) = storage
        .tiers
        .by_name(tier)
        .expect("storage should not fail")
    else {
        return Err(Error::other(format!(r#"tier "{tier}" doesn't exist"#)));
    };

    // Check failure domain constraints
    let existing_fds = storage
        .instances
        .failure_domain_names()
        .expect("storage should not fail");
    failure_domain.check(&existing_fds)?;

    //
    // Resolve replicaset
    //
    let replicaset_name;
    let replicaset_uuid;
    if let Some(requested_replicaset_name) = requested_replicaset_name {
        let replicaset = storage.replicasets.get(requested_replicaset_name)?;
        if let Some(replicaset) = replicaset {
            if replicaset.tier != tier.name {
                return Err(Error::other(format!("tier mismatch: instance {instance_name} is from tier: '{}', but replicaset {requested_replicaset_name} is from tier: '{}'", tier.name, replicaset.tier)));
            }

            // FIXME: must make sure the replicaset is not Expelled or ToBeExpelled
            // FIXME: must make sure the replicaset's tier is correct

            // Join instance to existing replicaset
            replicaset_name = requested_replicaset_name.clone();
            replicaset_uuid = replicaset.uuid;
        } else {
            // Create a new replicaset
            replicaset_name = requested_replicaset_name.clone();
            replicaset_uuid = uuid::Uuid::new_v4().to_hyphenated().to_string();
        }
    } else {
        let res = choose_replicaset(failure_domain, storage, &tier)?;
        match res {
            Ok(replicaset) => {
                // Join instance to existing replicaset
                replicaset_name = replicaset.name;
                replicaset_uuid = replicaset.uuid;
            }
            Err(new_replicaset_name) => {
                // Create a new replicaset
                replicaset_name = new_replicaset_name;
                replicaset_uuid = uuid::Uuid::new_v4().to_hyphenated().to_string();
            }
        }
    }

    // Generate a unique instance_uuid
    let instance_uuid = uuid::Uuid::new_v4().to_hyphenated().to_string();

    Ok(Instance {
        raft_id,
        name: instance_name,
        uuid: instance_uuid,
        replicaset_name,
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

/// Choose a replicaset for the new instance based on `failure_domain`, `tier`
/// and the list of avaliable replicasets and instances in them.
/// FIXME: a couple of problems:
/// - expelled instances are errouneosly counted towards replication factor
/// - must ignore replicasets with state ToBeExpelled & Expelled
fn choose_replicaset(
    failure_domain: &FailureDomain,
    storage: &Clusterwide,
    tier: &Tier,
) -> Result<Result<Replicaset, ReplicasetName>> {
    let replication_factor = tier.replication_factor as _;

    // The list of candidate replicasets for the new instance
    let mut replicasets = vec![];
    // The list of ids of all replicasets in the cluster
    let mut all_replicasets = HashSet::new();

    for replicaset in storage.replicasets.iter()? {
        all_replicasets.insert(replicaset.name.clone());

        // TODO: skip expelled replicasets
        if replicaset.tier != tier.name {
            continue;
        }

        replicasets.push(SomeInfoAboutReplicaset {
            replicaset,
            instances: vec![],
        });
    }
    // We sort the array so that we get a determenistic order of instance addition to replicasets.
    // E.g. if both "r1" and "r2" are suitable, "r1" will always be prefered.
    // NOTE: can't use `sort_unstable_by_key` because of borrow checker, yay rust!
    replicasets.sort_unstable_by(|lhs, rhs| lhs.replicaset.name.cmp(&rhs.replicaset.name));

    for instance in storage
        .instances
        .all_instances()
        .expect("storage should not fail")
        .into_iter()
    {
        if instance.tier != tier.name {
            continue;
        }

        if has_states!(instance, Expelled -> *) {
            // Expelled instances are ignored
            continue;
        }

        let index =
            replicasets.binary_search_by_key(&&instance.replicaset_name, |i| &i.replicaset.name);
        let index = index.expect("replicaset entries should be present for each instance");
        replicasets[index].instances.push(instance);
    }

    'next_replicaset: for info in &replicasets {
        // TODO: skip replicasets with state ToBeExpelled & Expelled

        if info.instances.len() >= replication_factor {
            continue 'next_replicaset;
        }

        for instance in &info.instances {
            if instance.failure_domain.intersects(failure_domain) {
                continue 'next_replicaset;
            }
        }

        return Ok(Ok(info.replicaset.clone()));
    }

    let mut i = 0u64;
    loop {
        i += 1;
        let replicaset_name = ReplicasetName(format!("r{i}"));
        if !all_replicasets.contains(&replicaset_name) {
            // Not found, hence id is ok
            return Ok(Err(replicaset_name));
        }
    }

    struct SomeInfoAboutReplicaset {
        replicaset: Replicaset,
        instances: Vec<Instance>,
    }

    fn key_replicaset_name(info: &SomeInfoAboutReplicaset) -> &ReplicasetName {
        &info.replicaset.name
    }
}
