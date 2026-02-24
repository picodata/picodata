use crate::cas;
use crate::failure_domain::FailureDomain;
use crate::has_states;
use crate::instance::State;
use crate::instance::StateVariant::*;
use crate::instance::{Instance, InstanceName};
use crate::replicaset::Replicaset;
use crate::replicaset::ReplicasetName;
use crate::replicaset::ReplicasetState;
use crate::schema::ADMIN_ID;
use crate::storage::SystemTable;
use crate::storage::{self, Catalog, ToEntryIter as _};
use crate::tier::Tier;
use crate::tlog;
use crate::traft::op::{Dml, Op};
use crate::traft::RaftId;
use crate::traft::{self};
use crate::traft::{error::Error, node, Address, Result};
use smol_str::format_smolstr;
use smol_str::SmolStr;
use smol_str::ToSmolStr;
use std::collections::HashSet;
use std::time::Duration;
use tarantool::datetime::Datetime;
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
        crate::error_injection!(block "BLOCK_PROC_RAFT_JOIN");

        tlog!(Info, "received join request {req:?}");
        let res = handle_join_request_and_wait(req, TIMEOUT)?;

        Ok(res)
    }

    /// Request to join the cluster.
    pub struct Request {
        pub cluster_name: SmolStr,
        pub instance_name: Option<InstanceName>,
        pub replicaset_name: Option<ReplicasetName>,
        pub advertise_address: SmolStr,
        pub pgproto_advertise_address: SmolStr,
        pub failure_domain: FailureDomain,
        pub tier: SmolStr,
        pub picodata_version: SmolStr,
        pub uuid: String,
    }

    pub struct Response {
        pub instance: Box<Instance>,
        /// Addresses of other peers in a cluster.
        /// They are needed for Raft node to communicate with other nodes
        /// at startup.
        pub peer_addresses: Vec<(RaftId, Address)>,
        /// Replication sources in a replica set that the joining instance will belong to.
        /// See [tarantool documentation](https://www.tarantool.io/en/doc/latest/reference/configuration/#confval-replication)
        pub box_replication: Vec<Address>,
        /// If `true` the joining instance is the master of the new replicaset.
        /// Currently it's only possible for the joining instance to be the
        /// master if it's the first instance in the replicaset.
        ///
        /// NOTE: It's possible for multiple instances to be joining at the same
        /// time in case some of them crash and restart during bootstrap.
        /// For that reason we must explicitly specify which of the joining
        /// instance's is the master.
        pub is_master: bool,
        pub shredding: bool,
        pub cluster_uuid: String,
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
    let cluster_name = node.topology_cache.cluster_name;
    let cluster_uuid = node.topology_cache.cluster_uuid;
    let storage = &node.storage;
    let guard = node.instances_update.lock();

    if req.cluster_name != cluster_name {
        return Err(Error::ClusterNameMismatch {
            instance_cluster_name: req.cluster_name,
            cluster_name,
        });
    }

    let global_cluster_version = storage
        .properties
        .cluster_version()
        .expect("storage should never fail");

    crate::compatibility::compare_picodata_versions(
        &global_cluster_version,
        req.picodata_version.as_ref(),
    )?;

    let deadline = fiber::clock().saturating_add(timeout);
    loop {
        let (instance, instance_exists) = build_instance(
            req.instance_name.as_ref(),
            req.replicaset_name.as_ref(),
            &req.failure_domain,
            storage,
            &req.tier,
            req.picodata_version.as_ref(),
            &req.uuid,
        )?;

        if instance_exists {
            let peer_addresses = node
                .storage
                .peer_addresses
                .iter()?
                .filter(|peer| peer.connection_type == traft::ConnectionType::Iproto)
                .map(|peer| (peer.raft_id, peer.address))
                .collect();

            let replicas = storage
                .instances
                .replicaset_instances(&instance.replicaset_name)
                .expect("storage should not fail")
                .filter(|i| !has_states!(i, Expelled -> *))
                .map(|i| i.raft_id);
            let mut replication_addresses = storage.peer_addresses.addresses_by_ids(replicas)?;
            replication_addresses.insert(req.advertise_address.clone());

            let topology_ref = node.topology_cache.get();
            let replicaset = topology_ref.replicaset_by_uuid(&instance.replicaset_uuid)?;
            let is_master = replicaset.target_master_name == instance.name;

            drop(guard);

            tlog!(
                Info,
                "joined instance requested to join again: {instance:?}"
            );

            return Ok(Response {
                instance: Box::new(instance),
                peer_addresses,
                box_replication: replication_addresses.into_iter().collect(),
                is_master,
                shredding: storage.db_config.shredding()?.expect("should be set"),
                cluster_uuid: cluster_uuid.into(),
            });
        }

        let peer_address = traft::PeerAddress {
            raft_id: instance.raft_id,
            address: req.advertise_address.clone(),
            connection_type: traft::ConnectionType::Iproto,
        };
        let pgproto_peer_address = traft::PeerAddress {
            raft_id: instance.raft_id,
            address: req.pgproto_advertise_address.clone(),
            connection_type: traft::ConnectionType::Pgproto,
        };

        let mut ops = Vec::with_capacity(4);
        ops.push(
            Dml::replace(storage::PeerAddresses::TABLE_ID, &peer_address, ADMIN_ID)
                .expect("encoding should not fail"),
        );
        ops.push(
            Dml::replace(
                storage::PeerAddresses::TABLE_ID,
                &pgproto_peer_address,
                ADMIN_ID,
            )
            .expect("encoding should not fail"),
        );
        ops.push(
            Dml::replace(storage::Instances::TABLE_ID, &instance, ADMIN_ID)
                .expect("encoding should not fail"),
        );

        let res = storage.replicasets.by_uuid_raw(&instance.replicaset_uuid);
        if let Err(Error::NoSuchReplicaset { .. }) = res {
            let replicaset = Replicaset::with_one_instance(&instance);
            ops.push(
                // NOTE: we use replace instead of insert, because at the
                // moment primary key in _pico_replicaset is the replicaset_name (name),
                // but in here we may be creating a new replicaset with
                // the name of a previously expelled replicaset.
                // The new replicaset will have a new unique uuid, so once we
                // make the uuid the primary key, we can switch back to using
                // insert here.
                Dml::replace(storage::Replicasets::TABLE_ID, &replicaset, ADMIN_ID)
                    .expect("encoding should not fail"),
            );
        }

        let ranges = vec![
            cas::Range::new(storage::Instances::TABLE_ID),
            cas::Range::new(storage::PeerAddresses::TABLE_ID),
            cas::Range::new(storage::Tiers::TABLE_ID),
            cas::Range::new(storage::Replicasets::TABLE_ID),
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
        let peer_addresses = node
            .storage
            .peer_addresses
            .iter()?
            .filter(|peer| peer.connection_type == traft::ConnectionType::Iproto)
            .map(|peer| (peer.raft_id, peer.address))
            .collect();
        let replicas = storage
            .instances
            .replicaset_instances(&instance.replicaset_name)
            .expect("storage should not fail")
            // Ignore expelled instances
            .filter(|i| !has_states!(i, Expelled -> *))
            .map(|i| i.raft_id);
        let mut replication_addresses = storage.peer_addresses.addresses_by_ids(replicas)?;
        replication_addresses.insert(req.advertise_address.clone());

        let topology_ref = node.topology_cache.get();
        let replicaset = topology_ref.replicaset_by_uuid(&instance.replicaset_uuid)?;
        let is_master = replicaset.target_master_name == instance.name;

        drop(guard);

        tlog!(Info, "new instance joined the cluster: {instance:?}");

        return Ok(Response {
            instance: instance.into(),
            peer_addresses,
            box_replication: replication_addresses.into_iter().collect(),
            is_master,
            shredding: storage.db_config.shredding()?.expect("should be set"),
            cluster_uuid: cluster_uuid.into(),
        });
    }
}

/// Creates or finds an instance by UUID.
/// Returns:
/// - `(Instance, true)` - if an instance with the specified UUID already exists
/// - `(Instance, false)` - if a new instance is created
pub fn build_instance(
    requested_instance_name: Option<&InstanceName>,
    requested_replicaset_name: Option<&ReplicasetName>,
    failure_domain: &FailureDomain,
    storage: &Catalog,
    tier: &str,
    picodata_version: &str,
    uuid: &str,
) -> Result<(Instance, bool)> {
    let existing_instance = storage.instances.by_uuid(uuid)?;
    if let Some(instance) = existing_instance {
        if let Some(requested_name) = requested_instance_name {
            if requested_name != &instance.name {
                return Err(Error::other(format!(
                    "instance with UUID {uuid} already exists but with different name",
                )));
            }
        }

        if let Some(name) = requested_replicaset_name {
            if name != &instance.replicaset_name {
                return Err(Error::other(format!(
                    "instance with UUID {uuid} already exists but with different replicaset name",
                )));
            }
        }

        if tier != instance.tier {
            return Err(Error::other(format!(
                "instance with UUID {uuid} already exists but with different tier",
            )));
        }

        if failure_domain != &instance.failure_domain {
            return Err(Error::other(format!(
                "instance with UUID {uuid} already exists but with different failure domain",
            )));
        }

        if picodata_version != instance.picodata_version {
            return Err(Error::other(format!(
                "instance with UUID {uuid} already exists but with different picodata version",
            )));
        }

        if !has_states!(instance, Offline -> Offline) {
            return Err(Error::other(format!(
                "instance with UUID {uuid} is not in Offline state",
            )));
        }

        return Ok((instance, true));
    }

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

    // Check tier exists
    let Some(tier) = storage
        .tiers
        .by_name(tier)
        .expect("storage should not fail")
    else {
        return Err(Error::other(format!(r#"tier "{tier}" doesn't exist"#)));
    };

    //
    // Resolve replicaset
    //
    let replicaset_name;
    let replicaset_uuid;
    if let Some(requested_replicaset_name) = requested_replicaset_name {
        let replicaset = storage.replicasets.get(requested_replicaset_name)?;
        match replicaset {
            Some(replicaset) if replicaset.state != ReplicasetState::Expelled => {
                if replicaset.tier != tier.name {
                    return Err(Error::other(format!(
                        "tier mismatch: requested replicaset '{}' is from tier '{}', but specified tier is '{}'",
                        requested_replicaset_name, replicaset.tier, tier.name
                    )));
                }

                if replicaset.state == ReplicasetState::ToBeExpelled {
                    #[rustfmt::skip]
                    return Err(Error::other("cannot join replicaset which is being expelled"));
                }

                // Join instance to existing replicaset
                replicaset_name = requested_replicaset_name.clone();
                replicaset_uuid = replicaset.uuid;
            }
            // Replicaset doesn't exist or was expelled
            _ => {
                // Create a new replicaset
                replicaset_name = requested_replicaset_name.clone();
                replicaset_uuid = uuid::Uuid::new_v4().to_hyphenated().to_smolstr();
            }
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
                replicaset_uuid = uuid::Uuid::new_v4().to_hyphenated().to_smolstr();
            }
        }
    }

    // Check failure domain constraints
    let existing_fds = storage
        .instances
        .failure_domain_names()
        .expect("storage should not fail");
    failure_domain.check(&existing_fds)?;

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
        instance_name = choose_instance_name(storage, replicaset_name.clone());
    }

    let instance_uuid = uuid.to_smolstr();

    let instance = Instance {
        raft_id,
        name: instance_name,
        uuid: instance_uuid,
        replicaset_name,
        replicaset_uuid,
        current_state: State::new(Offline, 0),
        target_state: State::new(Offline, 0),
        failure_domain: failure_domain.clone(),
        tier: tier.name.clone(),
        picodata_version: picodata_version.into(),
        sync_incarnation: Some(0),
        target_state_reason: Some("".into()),
        target_state_change_time: Some(Datetime::now_utc()),
    };

    Ok((instance, false))
}

/// Choose [`InstanceName`] based on `tier name`.
// TODO: use `TopologyCache` instead of `Catalog`
fn choose_instance_name(storage: &Catalog, replicaset_name: ReplicasetName) -> InstanceName {
    let mut instance_number_in_replicaset = 1;
    loop {
        // tier name is already included in replicaset name
        let instance_name = InstanceName(format_smolstr!(
            "{replicaset_name}_{instance_number_in_replicaset}"
        ));

        match storage.instances.get(&instance_name) {
            Ok(instance) => {
                if has_states!(instance, Expelled -> *) {
                    return instance_name;
                }

                instance_number_in_replicaset += 1;
            }
            Err(_) => {
                return instance_name;
            }
        }
    }
}

/// Choose a replicaset for the new instance based on `failure_domain`, `tier`
/// and the list of avaliable replicasets and instances in them.
fn choose_replicaset(
    failure_domain: &FailureDomain,
    storage: &Catalog,
    tier: &Tier,
) -> Result<Result<Replicaset, ReplicasetName>> {
    let replication_factor = tier.replication_factor as _;

    // The list of candidate replicasets for the new instance
    let mut replicasets = vec![];
    // The list of ids of all replicasets in the cluster
    let mut all_replicasets = HashSet::new();

    for replicaset in storage.replicasets.iter()? {
        all_replicasets.insert(replicaset.name.clone());

        if replicaset.tier != tier.name {
            continue;
        }

        if replicaset.state == ReplicasetState::ToBeExpelled {
            continue;
        }

        if replicaset.state == ReplicasetState::Expelled {
            // NOTE: we could allow atomatically reusing old expelled
            // replicasets, i.e. reusing the name but generating a new uuid, but
            // it's not clear why would we do this..
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
        let Ok(index) = index else {
            debug_assert!(all_replicasets.contains(&instance.replicaset_name));
            // Replicaset is skipped for some reason, so this instance's info is
            // not going to be used
            continue;
        };

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

    let mut replicaset_number = 1;
    loop {
        let replicaset_name =
            ReplicasetName(format_smolstr!("{}_{}", tier.name, replicaset_number));
        match storage.replicasets.get(&replicaset_name)? {
            Some(replicaset) => {
                match replicaset.state {
                    ReplicasetState::Expelled => {
                        // replicaset name is available
                        return Ok(Err(replicaset_name));
                    }
                    ReplicasetState::ToBeExpelled => {
                        // replicaset isn't expelled yet, generate a new name
                        replicaset_number += 1;
                        continue;
                    }
                    _ => {
                        // replicaset exists, generate a new name
                        replicaset_number += 1;
                        continue;
                    }
                }
            }
            None => {
                return Ok(Err(replicaset_name));
            }
        }
    }

    struct SomeInfoAboutReplicaset {
        replicaset: Replicaset,
        instances: Vec<Instance>,
    }
}

pub fn compress_join_response(join_response: &Response) -> Result<Vec<u8>> {
    let res = rmp_serde::to_vec(&join_response);
    let encoded = match res {
        Ok(v) => v,
        Err(e) => {
            return Err(Error::other(format!(
                "failed to encode proc_raft_join response for the self-pipe message: {e}"
            )));
        }
    };
    let compressed = crate::util::gzip_compress(&encoded)?;

    Ok(compressed)
}

pub fn decompress_join_response(compressed: &[u8]) -> Result<Response> {
    let encoded = crate::util::gzip_decompress(compressed)?;
    let res = rmp_serde::from_slice(&encoded);
    let join_response = match res {
        Ok(v) => v,
        Err(e) => {
            return Err(Error::other(format!(
                "failed to decode proc_raft_join response from the self-pipe message: {e}"
            )));
        }
    };

    Ok(join_response)
}
