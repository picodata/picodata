use std::time::Duration;

use crate::cas;
use crate::failure_domain::FailureDomain;
use crate::instance::State;
use crate::instance::StateVariant;
use crate::instance::StateVariant::*;
use crate::instance::{Instance, InstanceId};
use crate::schema::ADMIN_ID;
use crate::storage::{Clusterwide, ClusterwideTable};
use crate::traft::op::{Dml, Op};
use crate::traft::Result;
use crate::traft::{error::Error, node};
use tarantool::fiber;
use tarantool::space::UpdateOps;

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
           return Err(Error::Other("Changing current state through Proc API is not allowed.".into()));
        }
        handle_update_instance_request_and_wait(req, TIMEOUT)?;
        Ok(Response {})
    }

    /// Request to update the instance in the storage.
    #[derive(Default)]
    pub struct Request {
        pub instance_id: InstanceId,
        pub cluster_id: String,
        /// Only allowed to be set by leader
        pub current_state: Option<State>,
        /// Can be set by instance
        pub target_state: Option<StateVariant>,
        pub failure_domain: Option<FailureDomain>,
        /// If `true` then the resulting CaS request is not retried upon failure.
        pub dont_retry: bool,
    }

    pub struct Response {}
}

impl Request {
    #[inline]
    pub fn new(instance_id: InstanceId, cluster_id: String) -> Self {
        Self {
            instance_id,
            cluster_id,
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
}

/// Processes the [`crate::rpc::update_instance::Request`] and appends
/// the corresponding [`Op::Dml`] entry to the raft log (if successful).
///
/// Returns `Ok(())` when the entry is committed.
///
/// **This function yields**
#[inline(always)]
pub fn handle_update_instance_request_and_wait(req: Request, timeout: Duration) -> Result<()> {
    handle_update_instance_request_in_governor_and_also_wait_too(req, None, timeout)
}

/// Processes the [`crate::rpc::update_instance::Request`] and appends
/// the corresponding operation along with the provided `additional_dml` entry
/// to the raft log within a single [`Op::BatchDml`] (if successful).
///
/// **This function should be used directly only from governor** everywhere else
/// should use [`handle_update_instance_request_and_wait`] instead.
///
/// Returns `Ok(())` when the entry is committed.
///
/// **This function yields**
pub fn handle_update_instance_request_in_governor_and_also_wait_too(
    req: Request,
    additional_dml: Option<&Dml>,
    timeout: Duration,
) -> Result<()> {
    let node = node::global()?;
    let cluster_id = node.raft_storage.cluster_id()?;
    let storage = &node.storage;
    let raft_storage = &node.raft_storage;
    let guard = node.instances_update.lock();

    if req.cluster_id != cluster_id {
        return Err(Error::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id,
            cluster_cluster_id: cluster_id,
        });
    }

    #[cfg(debug_assertions)]
    if let Some(op) = additional_dml {
        debug_assert_eq!(op.table_id(), ClusterwideTable::Property.id(), "for CaS safety reasons currently we only allow updating _pico_property simultaneously with instance");
    }

    let deadline = fiber::clock().saturating_add(timeout);
    loop {
        let instance = storage.instances.get(&req.instance_id)?;

        let replicaset_id = &instance.replicaset_id;
        #[rustfmt::skip]
        let Some(replicaset) = storage.replicasets.get(replicaset_id)? else {
            crate::warn_or_panic!("replicaset info for replicaset_id: `{replicaset_id}` has disappeared, needed for instance {}", instance.instance_id);
            return Err(Error::NoSuchReplicaset { id: replicaset_id.to_string(), id_is_uuid: false });
        };

        let dml = update_instance(&instance, &req, storage)?;
        let Some((dml, version_bump_needed)) = dml else {
            // No point in proposing an operation which doesn't change anything.
            // Note: if the request tried setting target state Online while it
            // was already Online the incarnation will be increased and so
            // dml will not be None and this is the intended behaviour.
            return Ok(());
        };
        debug_assert!(matches!(dml, Dml::Update { .. }), "{dml:?}");
        debug_assert_eq!(dml.table_id(), ClusterwideTable::Instance.id());

        let mut ops = vec![];
        ops.push(dml);

        if version_bump_needed &&
            // Don't bump version if it's already bumped
            replicaset.current_config_version == replicaset.target_config_version
        {
            let mut update_ops = UpdateOps::new();
            #[rustfmt::skip]
            update_ops.assign("target_config_version", replicaset.target_config_version + 1)?;
            #[rustfmt::skip]
            let replicaset_dml = Dml::update(ClusterwideTable::Replicaset, &[replicaset_id], update_ops, ADMIN_ID)?;
            ops.push(replicaset_dml);
        }

        if let Some(additional_dml) = additional_dml {
            // TODO: to eliminate redundant copies here we should refactor `BatchDml` and/or `Dml`
            ops.push(additional_dml.clone());
        }

        let op = Op::single_dml_or_batch(ops);

        let ranges = vec![
            cas::Range::new(ClusterwideTable::Instance),
            cas::Range::new(ClusterwideTable::Address),
            cas::Range::new(ClusterwideTable::Tier),
        ];

        let cas_req = crate::cas::Request::new(
            op,
            cas::Predicate {
                index: raft_storage.applied()?,
                term: raft_storage.term()?,
                ranges,
            },
            ADMIN_ID,
        )?;
        let res = cas::compare_and_swap(&cas_req, deadline.duration_since(fiber::clock()));
        match res {
            Ok((index, term)) => {
                node.wait_index(index, deadline.duration_since(fiber::clock()))?;
                if term != raft::Storage::term(raft_storage, index)? {
                    // leader switched - retry
                    continue;
                }
            }
            Err(err) => {
                if req.dont_retry {
                    return Err(err);
                }
                if err.is_retriable() {
                    // cas error - retry
                    fiber::sleep(Duration::from_millis(500));
                    continue;
                } else {
                    return Err(err);
                }
            }
        }
        node.main_loop.wakeup();
        drop(guard);
        return Ok(());
    }
}

/// Returns a pair:
/// - A [`Dml::Update`] operation which should be applied to the `_pico_instance` table to satisfy the `req`.
///   May be `None` if the request is already satisfied.
/// - `true` if `target_config_version` column should be bumped in table  `_pico_replicaset` otherwise `false`.
pub fn update_instance(
    instance: &Instance,
    req: &Request,
    storage: &Clusterwide,
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
        return Err(Error::other(format!("cannot update expelled instance \"{}\"", instance.instance_id)));
    }

    let mut ops = UpdateOps::new();
    if let Some(fd) = req.failure_domain.as_ref() {
        let existing_fds = storage
            .instances
            .failure_domain_names()
            .expect("storage should not fail");
        fd.check(&existing_fds)?;
        if fd != &instance.failure_domain {
            ops.assign("failure_domain", fd)?;
        }
    }

    if let Some(state) = req.current_state {
        if state != instance.current_state {
            ops.assign("current_state", state)?;
        }
    }

    let mut replication_config_version_bump_needed = false;
    if let Some(variant) = req.target_state {
        let incarnation = match variant {
            Online => {
                replication_config_version_bump_needed = true;
                instance.target_state.incarnation + 1
            }
            Offline | Expelled => instance.current_state.incarnation,
            other => {
                #[rustfmt::skip]
                return Err(Error::other(format!("target state can only be Online, Offline or Expelled, not {other}")));
            }
        };
        let state = State::new(variant, incarnation);
        if state != instance.target_state {
            ops.assign("target_state", state)?;
        }
    }

    if ops.as_slice().is_empty() {
        return Ok(None);
    }

    let instance_dml = Dml::update(
        ClusterwideTable::Instance,
        &[&instance.instance_id],
        ops,
        ADMIN_ID,
    )?;

    Ok(Some((instance_dml, replication_config_version_bump_needed)))
}

#[cfg(test)]
#[allow(non_snake_case)]
mod test {
    use super::*;

    #[test]
    #[should_panic]
    fn update_instance_req_with_target_state_Replicated() {
        Request::new("".into(), "".into()).with_target_state(Replicated);
        #[cfg(not(debug_assertions))]
        panic!("this is a synthetic panic, because the test is expected to panic, but the actual code only panics in debug mode");
    }
}
