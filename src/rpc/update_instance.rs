use std::time::Duration;

use crate::cas;
use crate::failure_domain::FailureDomain;
use crate::instance::Grade;
use crate::instance::GradeVariant;
use crate::instance::GradeVariant::*;
use crate::instance::{Instance, InstanceId};
use crate::schema::ADMIN_ID;
use crate::storage::{Clusterwide, ClusterwideTable};
use crate::traft::op::{Dml, Op};
use crate::traft::Result;
use crate::traft::{error::Error, node};

use ::tarantool::fiber;

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
        if req.current_grade.is_some() {
           return Err(Error::Other("Changing current grade through Proc API is not allowed.".into()));
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
        pub current_grade: Option<Grade>,
        /// Can be set by instance
        pub target_grade: Option<GradeVariant>,
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
    pub fn with_current_grade(mut self, value: Grade) -> Self {
        self.current_grade = Some(value);
        self
    }
    #[inline]
    pub fn with_target_grade(mut self, value: GradeVariant) -> Self {
        debug_assert!(
            matches!(value, Online | Offline | Expelled),
            "target grade can only be Online, Offline or Expelled"
        );
        self.target_grade = Some(value);
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
// TODO: for this function to be async and have an outer timeout wait_* fns need to be async
pub fn handle_update_instance_request_and_wait(req: Request, timeout: Duration) -> Result<()> {
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

    let deadline = fiber::clock().saturating_add(timeout);
    loop {
        let old_instance = storage.instances.get(&req.instance_id)?;
        let mut new_instance = old_instance.clone();
        update_instance(&mut new_instance, &req, storage).map_err(raft::Error::ConfChangeError)?;
        if old_instance == new_instance {
            // No point in proposing an operation which doesn't change anything.
            // Note: if the request tried setting target grade Online while it
            // was already Online the incarnation will be increased and so
            // old_instance will be different from new_instance and this is the
            // intended behaviour.
            return Ok(());
        }

        let dml = Dml::replace(ClusterwideTable::Instance, &new_instance, ADMIN_ID)
            .expect("encoding should not fail");

        let ranges = vec![
            cas::Range::new(ClusterwideTable::Instance),
            cas::Range::new(ClusterwideTable::Address),
            cas::Range::new(ClusterwideTable::Tier),
        ];

        let res = cas::compare_and_swap(
            Op::Dml(dml),
            cas::Predicate {
                index: raft_storage.applied()?,
                term: raft_storage.term()?,
                ranges,
            },
            ADMIN_ID,
            deadline.duration_since(fiber::clock()),
        );
        match res {
            Ok((index, term)) => {
                node.wait_index(index, deadline.duration_since(fiber::clock()))?;
                if term != raft::Storage::term(raft_storage, index)? {
                    // leader switched - retry
                    node.wait_status();
                    continue;
                }
            }
            Err(err) => {
                if req.dont_retry {
                    return Err(err);
                }
                if err.is_cas_err() || err.is_term_mismatch_err() {
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

/// Updates existing [`Instance`].
pub fn update_instance(
    instance: &mut Instance,
    req: &Request,
    storage: &Clusterwide,
) -> std::result::Result<(), String> {
    if instance.current_grade.variant == Expelled
        && !matches!(
            req,
            Request {
                target_grade: None,
                current_grade: Some(current_grade),
                failure_domain: None,
                ..
            } if current_grade.variant == Expelled
        )
    {
        return Err(format!(
            "cannot update expelled instance \"{}\"",
            instance.instance_id
        ));
    }

    if let Some(fd) = req.failure_domain.as_ref() {
        let existing_fds = storage
            .instances
            .failure_domain_names()
            .expect("storage should not fail");
        fd.check(&existing_fds)?;
        instance.failure_domain = fd.clone();
    }

    if let Some(value) = req.current_grade {
        instance.current_grade = value;
    }

    if let Some(variant) = req.target_grade {
        let incarnation = match variant {
            Online => instance.target_grade.incarnation + 1,
            Offline | Expelled => instance.current_grade.incarnation,
            other => {
                return Err(format!(
                    "target grade can only be Online, Offline or Expelled, not {other}"
                ));
            }
        };
        instance.target_grade = Grade {
            variant,
            incarnation,
        };
    }

    Ok(())
}

#[cfg(test)]
#[allow(non_snake_case)]
mod test {
    use super::*;

    #[test]
    #[should_panic]
    fn update_instance_req_with_target_grade_Replicated() {
        Request::new("".into(), "".into()).with_target_grade(Replicated);
    }
}
