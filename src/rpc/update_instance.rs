use super::join::compare_picodata_versions;
use crate::backoff::SimpleBackoffManager;
use crate::cas;
use crate::column_name;
use crate::error_code::ErrorCode;
use crate::failure_domain::FailureDomain;
use crate::has_states;
use crate::instance::State;
use crate::instance::StateVariant;
use crate::instance::StateVariant::*;
use crate::instance::{Instance, InstanceName};
use crate::proc_name;
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
use crate::version::version_is_new_enough;
use smallvec::smallvec;
use smallvec::SmallVec;
use smol_str::format_smolstr;
use smol_str::SmolStr;
use std::collections::HashSet;
use std::time::Duration;
use tarantool::datetime::Datetime;
use tarantool::error::BoxError;
use tarantool::error::Error as TntError;
use tarantool::error::IntoBoxError;
use tarantool::error::TarantoolErrorCode;
use tarantool::fiber;
use tarantool::fiber::r#async::timeout;
use tarantool::fiber::r#async::timeout::IntoTimeout;
use tarantool::space::UpdateOps;
use tarantool::time::Instant;

const TIMEOUT: Duration = Duration::from_secs(10);

////////////////////////////////////////////////////////////////////////////////
// .proc_update_instance
////////////////////////////////////////////////////////////////////////////////

pub fn proc_update_instance_impl(req: Request) -> Result<Response> {
    if req.current_state.is_some() {
        tlog!(Warning, "invalid request to update current state: {req:?}");
        return Err(Error::Other(
            "Changing current state through Proc API is not allowed.".into(),
        ));
    }

    tlog!(Debug, "got update instance request: {req:?}");
    handle_update_instance_request_and_wait(req, TIMEOUT)?;
    Ok(Response {})
}

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
    fn proc_update_instance_v2(req: Request) -> Result<Response> {
        proc_update_instance_impl(req)
    }

    /// Request to update the instance in the storage.
    #[derive(Default)]
    pub struct Request {
        pub base: RequestV1,
        #[serde(default)]
        pub target_state_reason: Option<SmolStr>,
    }

    pub struct Response {}
}

impl std::ops::Deref for Request {
    type Target = RequestV1;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl std::ops::DerefMut for Request {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

crate::define_rpc_request! {
    #[deprecated(since="25.6.1", note="use `proc_update_instance_v2` instead")]
    fn proc_update_instance(base: RequestV1) -> Result<ResponseV1> {
        let req = Request::from_v1(base);
        proc_update_instance_impl(req)?;
        Ok(ResponseV1 {})
    }

    /// Request to update the instance in the storage.
    #[derive(Default)]
    pub struct RequestV1 {
        pub instance_name: InstanceName,
        pub cluster_name: SmolStr,
        pub cluster_uuid: SmolStr,
        /// Only allowed to be set by leader
        pub current_state: Option<State>,
        /// Can be set by instance
        pub target_state: Option<StateVariant>,
        pub failure_domain: Option<FailureDomain>,
        /// If `true` then the resulting CaS request is not retried upon failure.
        pub dont_retry: bool,
        /// Only set by instance when it is waking up.
        pub picodata_version: Option<SmolStr>,
    }

    pub struct ResponseV1 {}
}

impl Request {
    #[inline]
    pub fn new(
        instance_name: InstanceName,
        cluster_name: impl Into<SmolStr>,
        cluster_uuid: impl Into<SmolStr>,
    ) -> Self {
        let base = RequestV1 {
            instance_name,
            cluster_name: cluster_name.into(),
            cluster_uuid: cluster_uuid.into(),
            dont_retry: false,
            ..Default::default()
        };
        Self::from_v1(base)
    }

    #[inline]
    pub fn from_v1(base: RequestV1) -> Self {
        Self {
            base,
            ..Default::default()
        }
    }

    #[inline]
    pub fn into_v1(self) -> RequestV1 {
        self.base
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
    pub fn with_target_state_reason(mut self, value: impl Into<SmolStr>) -> Self {
        self.target_state_reason = Some(value.into());
        self
    }
    #[inline]
    pub fn with_failure_domain(mut self, value: FailureDomain) -> Self {
        self.failure_domain = Some(value);
        self
    }
    #[inline]
    pub fn with_picodata_version(mut self, value: SmolStr) -> Self {
        self.picodata_version = Some(value);
        self
    }
}

////////////////////////////////////////////////////////////////////////////////
// handle_update_instance_request_and_wait
////////////////////////////////////////////////////////////////////////////////

/// Processes the [`crate::rpc::update_instance::Request`] and appends
/// the corresponding [`Op::Dml`] entry to the raft log (if successful).
///
/// Returns `Ok(())` when the entry is committed.
///
/// **This function yields**
#[inline(always)]
pub fn handle_update_instance_request_and_wait(req: Request, timeout: Duration) -> Result<()> {
    let node = node::global()?;
    let cluster_uuid = node.topology_cache.cluster_uuid;
    let storage = &node.storage;
    let guard = node.instances_update.lock();

    if req.cluster_uuid != cluster_uuid {
        return Err(Error::ClusterUuidMismatch {
            instance_uuid: req.cluster_uuid.clone(),
            cluster_uuid,
        });
    }

    let global_cluster_version = storage
        .properties
        .cluster_version()
        .expect("storage should never fail");

    let system_catalog_version = storage
        .properties
        .system_catalog_version()
        .expect("storage should never fail")
        .expect("always available since 25.1.0");

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
            Some(&existing_fds),
            &global_cluster_version,
            &system_catalog_version,
        )?
        else {
            return Ok(());
        };

        let predicate = cas::Predicate::with_applied_index(ranges.into_vec());
        let op = Op::single_dml_or_batch(ops.into_vec());
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

////////////////////////////////////////////////////////////////////////////////
// prepare_update_instance_cas_request
////////////////////////////////////////////////////////////////////////////////

type Vec3<T> = SmallVec<[T; 3]>;

pub fn prepare_update_instance_cas_request(
    request: &Request,
    instance: &Instance,
    replicaset: &Replicaset,
    tier: &Tier,
    existing_fds: Option<&HashSet<Uppercase>>,
    global_cluster_version: &str,
    system_catalog_version: &str,
) -> Result<Option<(Vec3<Dml>, Vec3<cas::Range>)>> {
    debug_assert_eq!(instance.replicaset_name, replicaset.name);
    debug_assert_eq!(instance.tier, replicaset.tier);
    debug_assert_eq!(instance.tier, tier.name);

    let dml = update_instance(
        instance,
        request,
        existing_fds,
        global_cluster_version,
        system_catalog_version,
    )?;
    let Some((dml, version_bump_needed)) = dml else {
        // No point in proposing an operation which doesn't change anything.
        // Note: if the request tried setting target state Online while it
        // was already Online the incarnation will be increased and so
        // dml will not be None and this is the intended behaviour.
        return Ok(None);
    };
    debug_assert!(matches!(dml, Dml::Update { .. }), "{dml:?}");
    debug_assert_eq!(dml.table_id(), storage::Instances::TABLE_ID);

    let mut ops = SmallVec::new();
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

    let ranges = smallvec![
        cas::Range::new(storage::Instances::TABLE_ID),
        cas::Range::new(storage::PeerAddresses::TABLE_ID),
        cas::Range::new(storage::Tiers::TABLE_ID),
    ];
    Ok(Some((ops, ranges)))
}

////////////////////////////////////////////////////////////////////////////////
// update_instance
////////////////////////////////////////////////////////////////////////////////

/// Returns a pair:
/// - A [`Dml::Update`] operation which should be applied to the `_pico_instance` table to satisfy the `req`.
///   May be `None` if the request is already satisfied.
/// - `true` if `target_config_version` column should be bumped in table  `_pico_replicaset` otherwise `false`.
pub fn update_instance(
    instance: &Instance,
    req: &Request,
    existing_fds: Option<&HashSet<Uppercase>>,
    global_cluster_version: &str,
    system_catalog_version: &str,
) -> Result<Option<(Dml, bool)>> {
    if instance.current_state.variant == Expelled
        && !matches!(
            req,
            Request {
                base: RequestV1 {
                    target_state: None,
                    current_state: Some(current_state),
                    failure_domain: None,
                    ..
                },
                ..
            } if current_state.variant == Expelled
        )
    {
        #[rustfmt::skip]
        return Err(Error::other(format!("cannot update expelled instance \"{}\"", instance.name)));
    }

    let mut ops = UpdateOps::new();
    if let Some(fd) = req.failure_domain.as_ref() {
        let existing_fds =
            existing_fds.expect("must be provided if request changes failure domains");
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
            if version_is_new_enough(
                system_catalog_version,
                &Instance::TARGET_STATE_CHANGE_TIME_AVAILABLE_SINCE,
            )? {
                ops.assign(
                    column_name!(Instance, target_state_change_time),
                    Datetime::now_utc(),
                )?;
            }
        }
    }

    if let Some(reason) = &req.target_state_reason {
        if Some(reason) != instance.target_state_reason.as_ref() {
            if version_is_new_enough(
                system_catalog_version,
                &Instance::TARGET_STATE_CHANGE_TIME_AVAILABLE_SINCE,
            )? {
                ops.assign(column_name!(Instance, target_state_reason), reason.clone())?;
            }
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

////////////////////////////////////////////////////////////////////////////////
// update_our_target_state_to_online
////////////////////////////////////////////////////////////////////////////////

pub fn update_our_target_state_to_online(
    node: &node::Node,
    reason: &str,
    failure_domain: &FailureDomain,
    deadline: Instant,
) -> Result<()> {
    // This will be doubled on each retry, until max_retry_timeout is reached.
    let base_timeout = Duration::from_millis(250);
    let max_timeout = Duration::from_secs(5);
    let mut backoff =
        SimpleBackoffManager::new("proc_update_instance RPC", base_timeout, max_timeout);

    // When the whole cluster is restarting we use a smaller election timeout so
    // that we don't wait too long.
    const DEFAULT_BOOTSTRAP_ELECTION_TIMEOUT: Duration = Duration::from_secs(3);

    let mut bootstrap_election_timeout = DEFAULT_BOOTSTRAP_ELECTION_TIMEOUT;
    if let Ok(v) = std::env::var("PICODATA_BOOTSTRAP_ELECTION_TIMEOUT") {
        if let Ok(v) = v.parse() {
            bootstrap_election_timeout = Duration::from_secs_f64(v);
        }
    }

    // Use a random factor so that hopefully everybody doesn't start the
    // election at the same time.
    let random_factor = 1.0 + rand::random::<f64>();
    let election_timeout =
        Duration::from_secs_f64(bootstrap_election_timeout.as_secs_f64() * random_factor);
    let mut next_election_try = fiber::clock().saturating_add(election_timeout);

    // See comments bellow
    let mut use_old_proc_update_instance = false;

    // Activates instance
    loop {
        let now = fiber::clock();
        if now > deadline {
            return Err(Error::other(
                "failed to activate myself: timeout, shutting down...",
            ));
        }

        let instance = node.topology_cache.get().try_this_instance().cloned();
        let Some(instance) = instance else {
            // This can happen if for example a snapshot arrives
            // and we truncate _pico_instance (read uncommitted btw).
            // In this case we also just wait some more.
            let timeout = Duration::from_millis(100);
            fiber::sleep(timeout);
            continue;
        };

        if has_states!(instance, Expelled -> *) {
            return Err(BoxError::new(
                ErrorCode::InstanceExpelled,
                "current instance is expelled from the cluster",
            )
            .into());
        }

        let cluster_name = node.topology_cache.cluster_name;
        let cluster_uuid = node.topology_cache.cluster_uuid;
        // Doesn't have to be leader - can be any online peer
        let leader_id = node.status().leader_id;
        let leader_address = leader_id.and_then(|id| {
            node.storage
                .peer_addresses
                .try_get(id, &crate::traft::ConnectionType::Iproto)
                .ok()
        });

        #[allow(unused_mut)]
        let mut leader_info = leader_address.zip(leader_id);
        crate::error_injection!("LEADER_NOT_KNOWN_DURING_RESTART" => {
            // The error is: instance doesn't know who's the leader unless it is the leader itself
            if leader_id != Some(node.status().id) {
                leader_info = None;
            }
        });

        let Some((leader_address, leader_id)) = leader_info else {
            // FIXME: don't hard code timeout
            let timeout = Duration::from_millis(250);
            tlog!(
                Debug,
                "leader address is still unknown, retrying in {timeout:?}"
            );

            let mut i_can_vote = false;
            if let Some(tier) = node.topology_cache.get().try_this_tier() {
                i_can_vote = tier.can_vote;
            }

            // Leader has been unknown for too long
            if i_can_vote && fiber::clock() >= next_election_try {
                // Normally we should get here only if the whole cluster of
                // several instances is restarting at the same time, because
                // otherwise the raft leader should be known and the waking up
                // instance should find out about them via raft_main_loop.
                //
                // Note that everybody will not start the election at the same
                // time because of `random_factor` applied to the
                // `election_timeout` above.
                //
                // Also note that even if the raft leader is chosen in a cluster
                // a mulfanctioning instance trying to become the new leader
                // will not affect the healthy portion of the cluster thanks to
                // the pre_vote extension to the raft algorithm which is used in
                // picodata.
                tlog!(Info, "leader not known for too long, trying to promote");
                if let Err(e) = node.campaign_and_yield() {
                    tlog!(Warning, "failed to start raft election: {e}");
                }
                next_election_try = fiber::clock().saturating_add(election_timeout);
            }

            fiber::sleep(timeout);
            continue;
        };

        let leader_name;
        if let Ok(instance) = node.storage.instances.get(&leader_id) {
            leader_name = instance.name.0;
        } else {
            leader_name = format_smolstr!("raft_id:{leader_id}");
        }

        #[allow(unused_mut)]
        let mut version = SmolStr::new_static(crate::info::PICODATA_VERSION);
        crate::error_injection!("UPDATE_PICODATA_VERSION" => {
            if let Ok(v) = std::env::var("PICODATA_INTERNAL_VERSION_OVERRIDE") {
                version = v.into();
            }
        });

        #[allow(deprecated)]
        let proc_name = if use_old_proc_update_instance {
            proc_name!(proc_update_instance)
        } else {
            proc_name!(proc_update_instance_v2)
        };

        tlog!(
            Info,
            "initiating self-activation of instance {} ({}) sending RPC {proc_name} to {leader_name} at {leader_address}",
            instance.name,
            instance.uuid
        );

        let req = Request::new(instance.name, cluster_name, cluster_uuid)
            .with_target_state(Online)
            .with_target_state_reason(reason)
            .with_failure_domain(failure_domain.clone())
            .with_picodata_version(version);

        // During rolling upgrade instances on the latest picodata version will
        // still not have the latest `proc_update_instance_v2` defined, because
        // it will only be defined after every instance in cluster is updated.
        // So we need to optionally fallback to the older RPC version.
        let res;
        if use_old_proc_update_instance {
            let req = req.into_v1();
            let fut =
                crate::rpc::network_call(&leader_address, proc_name, &req).timeout(deadline - now);
            res = fiber::block_on(fut).map(|_| Response {});
        } else {
            let fut =
                crate::rpc::network_call(&leader_address, proc_name, &req).timeout(deadline - now);
            res = fiber::block_on(fut);
        }

        let error_message;
        match res {
            Ok(Response {}) => {
                // Leader has received our request to change target_state = Online
                break;
            }
            Err(timeout::Error::Failed(TntError::Tcp(e))) => {
                // A network error happened. Try again later.
                error_message = e.to_string();
            }
            Err(timeout::Error::Failed(TntError::IO(e))) => {
                // Hopefully a network error happened? Try again later.
                error_message = e.to_string();
            }
            Err(timeout::Error::Failed(e)) if e.error_code() == ErrorCode::NotALeader as u32 => {
                // Our info about raft leader is outdated, just wait a while for
                // it to update and send a request to hopefully the new leader.
                error_message = e.to_string();
            }
            Err(timeout::Error::Failed(e)) if e.error_code() == ErrorCode::LeaderUnknown as u32 => {
                // The peer no longer knows who the raft leader is. This is
                // possible for example if a leader election is in progress. We
                // should just wait some more and try again later.
                error_message = e.to_string();
            }
            Err(timeout::Error::Failed(e))
                if e.error_code() == TarantoolErrorCode::NoSuchProc as u32
                    && !use_old_proc_update_instance =>
            {
                // Newer stored procedure is not defined yet, fallback to the old version
                use_old_proc_update_instance = true;
                error_message = e.to_string();
            }
            Err(e) => {
                // Other kinds of errors, which can't/shouldn't be fixed by a "try again later" strategy
                return Err(Error::other(format!(
                    "failed to activate myself: {e}, shutting down..."
                )));
            }
        }

        backoff.handle_failure();
        let timeout = backoff.timeout();
        #[rustfmt::skip]
        tlog!(Warning, "failed to activate myself: {error_message}, retrying in {timeout:.02?}...");
        fiber::sleep(timeout);
    }

    Ok(())
}
