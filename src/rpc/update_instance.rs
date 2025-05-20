use crate::cas;
use crate::column_name;
use crate::failure_domain::FailureDomain;
use crate::instance::State;
use crate::instance::StateVariant;
use crate::instance::StateVariant::*;
use crate::instance::{Instance, InstanceName};
use crate::replicaset::Replicaset;
use crate::schema::ADMIN_ID;
use crate::storage;
use crate::storage::SystemTable;
use crate::tier::Tier;
use crate::tlog;
use crate::traft::op::{Dml, Op};
use crate::traft::Result;
use crate::traft::{error::Error, node};
use crate::util::Uppercase;
use std::collections::HashSet;
use std::time::Duration;
use tarantool::fiber;
use tarantool::space::UpdateOps;

use super::join::compare_picodata_versions;

const TIMEOUT: Duration = Duration::from_secs(10);

crate::define_rpc_request! {
    /// Submits a request to update the specified instance. If successful
    /// the updated information about the instance will be replicated
    /// on all of the cluster instances through Raft.
    ///
    /// Can be called on any instance that has already joined the cluster.
    ///
    /// Returns errors in the following cases:
    /// 1. Raft node on a receiving instance is not yet initialized
    /// 2. Storage failure
    /// 3. Incorrect request (e.g. instance expelled or an error in validation of failure domains)
    /// 4. Compare and swap request to commit updated instance failed
    /// with an error that cannot be retried.
    fn proc_update_instance(req: Request) -> Result<Response> {
        if req.current_state.is_some() {
            tlog!(Warning, "invalid request to update current state: {req:?}");
            return Err(Error::Other("Changing current state through Proc API is not allowed.".into()));
        }

        tlog!(Debug, "got update instance request: {req:?}");
        handle_update_instance_request_and_wait(req, TIMEOUT)?;
        Ok(Response {})
    }

    /// Request to update the instance in the storage.
    #[derive(Default)]
    pub struct Request {
        pub instance_name: InstanceName,
        pub cluster_name: String,
        pub cluster_uuid: String,
        /// Only allowed to be set by leader
        pub current_state: Option<State>,
        /// Can be set by instance
        pub target_state: Option<StateVariant>,
        pub failure_domain: Option<FailureDomain>,
        /// If `true` then the resulting CaS request is not retried upon failure.
        pub dont_retry: bool,
        /// Only set by instance when it is waking up.
        pub picodata_version: Option<String>,
    }

    pub struct Response {}
}

impl Request {
    #[inline]
    pub fn new(instance_name: InstanceName, cluster_name: String, cluster_uuid: String) -> Self {
        Self {
            instance_name,
            cluster_name,
            cluster_uuid,
            dont_retry: false,
            ..Request::default()
        }
    }
    #[inline]
    pub fn with_dont_retry(mut self, value: bool) -> Self {
        self.dont_retry = value;
        self
    }
    #[inline]
    pub fn with_current_state(mut self, value: State) -> Self {
        self.current_state = Some(value);
        self
    }
    #[inline]
    pub fn with_target_state(mut self, value: StateVariant) -> Self {
        debug_assert!(
            matches!(value, Online | Offline | Expelled),
            "target state can only be Online, Offline or Expelled"
        );
        self.target_state = Some(value);
        self
    }
    #[inline]
    pub fn with_failure_domain(mut self, value: FailureDomain) -> Self {
        self.failure_domain = Some(value);
        self
    }
    #[inline]
    pub fn with_picodata_version(mut self, value: String) -> Self {
        self.picodata_version = Some(value);
        self
    }
}

/// Processes the [`crate::rpc::update_instance::Request`] and appends
/// the corresponding [`Op::Dml`] entry to the raft log (if successful).
///
/// Returns `Ok(())` when the entry is committed.
///
/// **This function yields**
#[inline(always)]
pub fn handle_update_instance_request_and_wait(req: Request, timeout: Duration) -> Result<()> {
    let node = node::global()?;
    let cluster_uuid = node.raft_storage.cluster_uuid()?;
    let storage = &node.storage;
    let guard = node.instances_update.lock();

    if req.cluster_uuid != cluster_uuid {
        return Err(Error::ClusterUuidMismatch {
            instance_uuid: req.cluster_uuid,
            cluster_uuid,
        });
    }

    let global_cluster_version = storage
        .properties
        .cluster_version()
        .expect("storage should never fail");

    let deadline = fiber::clock().saturating_add(timeout);
    loop {
        let instance = storage.instances.get(&req.instance_name)?;

        let replicaset_name = &instance.replicaset_name;
        #[rustfmt::skip]
        let Some(replicaset) = storage.replicasets.get(replicaset_name)? else {
            crate::warn_or_panic!("replicaset info for replicaset_name: `{replicaset_name}` has disappeared, needed for instance {}", instance.name);
            return Err(Error::NoSuchReplicaset { name: replicaset_name.to_string(), id_is_uuid: false });
        };

        let tier = &instance.tier;
        let tier = storage
            .tiers
            .by_name(tier)?
            .expect("tier for instance should exists");

        let existing_fds = storage.instances.failure_domain_names()?;

        let Some((ops, ranges)) = prepare_update_instance_cas_request(
            &req,
            &instance,
            &replicaset,
            &tier,
            &existing_fds,
            &global_cluster_version,
        )?
        else {
            return Ok(());
        };

        let predicate = cas::Predicate::with_applied_index(ranges);
        let op = Op::single_dml_or_batch(ops);
        let cas = crate::cas::Request::new(op, predicate, ADMIN_ID)?;
        let res = cas::compare_and_swap_local(&cas, deadline)?;
        if req.dont_retry {
            res.no_retries()?;
        } else if let Some(e) = res.into_retriable_error() {
            crate::tlog!(Debug, "local CaS rejected: {e}");
            fiber::sleep(Duration::from_millis(250));
            continue;
        }

        node.main_loop.wakeup();
        drop(guard);
        return Ok(());
    }
}

pub fn prepare_update_instance_cas_request(
    request: &Request,
    instance: &Instance,
    replicaset: &Replicaset,
    tier: &Tier,
    existing_fds: &HashSet<Uppercase>,
    global_cluster_version: &str,
) -> Result<Option<(Vec<Dml>, Vec<cas::Range>)>> {
    debug_assert_eq!(instance.replicaset_name, replicaset.name);
    debug_assert_eq!(instance.tier, replicaset.tier);
    debug_assert_eq!(instance.tier, tier.name);

    let dml = update_instance(instance, request, existing_fds, global_cluster_version)?;
    let Some((dml, version_bump_needed)) = dml else {
        // No point in proposing an operation which doesn't change anything.
        // Note: if the request tried setting target state Online while it
        // was already Online the incarnation will be increased and so
        // dml will not be None and this is the intended behaviour.
        return Ok(None);
    };
    debug_assert!(matches!(dml, Dml::Update { .. }), "{dml:?}");
    debug_assert_eq!(dml.table_id(), storage::Instances::TABLE_ID);

    let mut ops = vec![];
    ops.push(dml);

    if version_bump_needed {
        let mut update_ops = UpdateOps::new();
        #[rustfmt::skip]
        update_ops.assign(column_name!(Replicaset, target_config_version), replicaset.target_config_version + 1)?;
        #[rustfmt::skip]
        let replicaset_dml = Dml::update(storage::Replicasets::TABLE_ID, &[&replicaset.name], update_ops, ADMIN_ID)?;
        ops.push(replicaset_dml);
    }

    if version_bump_needed {
        let vshard_bump_dml = Tier::get_vshard_config_version_bump_op(tier)?;
        ops.push(vshard_bump_dml);
    }

    let ranges = vec![
        cas::Range::new(storage::Instances::TABLE_ID),
        cas::Range::new(storage::PeerAddresses::TABLE_ID),
        cas::Range::new(storage::Tiers::TABLE_ID),
    ];
    Ok(Some((ops, ranges)))
}

/// Returns a pair:
/// - A [`Dml::Update`] operation which should be applied to the `_pico_instance` table to satisfy the `req`.
///   May be `None` if the request is already satisfied.
/// - `true` if `target_config_version` column should be bumped in table  `_pico_replicaset` otherwise `false`.
pub fn update_instance(
    instance: &Instance,
    req: &Request,
    existing_fds: &HashSet<Uppercase>,
    global_cluster_version: &str,
) -> Result<Option<(Dml, bool)>> {
    if instance.current_state.variant == Expelled
        && !matches!(
            req,
            Request {
                target_state: None,
                current_state: Some(current_state),
                failure_domain: None,
                ..
            } if current_state.variant == Expelled
        )
    {
        #[rustfmt::skip]
        return Err(Error::other(format!("cannot update expelled instance \"{}\"", instance.name)));
    }

    let mut ops = UpdateOps::new();
    if let Some(fd) = req.failure_domain.as_ref() {
        fd.check(existing_fds)?;
        if fd != &instance.failure_domain {
            ops.assign(column_name!(Instance, failure_domain), fd)?;
        }
    }

    if let Some(state) = req.current_state {
        if state != instance.current_state {
            ops.assign(column_name!(Instance, current_state), state)?;
        }
    }

    if let Some(picodata_version) = req.picodata_version.as_ref() {
        compare_picodata_versions(global_cluster_version, picodata_version)?;

        if instance.picodata_version != *picodata_version {
            ops.assign(column_name!(Instance, picodata_version), picodata_version)?;
        }
    }

    let mut replication_config_version_bump_needed = false;
    if let Some(variant) = req.target_state {
        let incarnation = match variant {
            Online => instance.target_state.incarnation + 1,
            Offline | Expelled => instance.current_state.incarnation,
        };
        let state = State::new(variant, incarnation);
        if state != instance.target_state {
            replication_config_version_bump_needed = true;
            ops.assign(column_name!(Instance, target_state), state)?;
        }
    }

    if ops.as_slice().is_empty() {
        return Ok(None);
    }

    let instance_dml = Dml::update(
        storage::Instances::TABLE_ID,
        &[&instance.name],
        ops,
        ADMIN_ID,
    )?;

    Ok(Some((instance_dml, replication_config_version_bump_needed)))
}
