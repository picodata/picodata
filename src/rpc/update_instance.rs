use std::time::Duration;

use crate::cas;
use crate::failure_domain::FailureDomain;
use crate::instance::grade::{CurrentGrade, TargetGradeVariant};
use crate::instance::InstanceId;
use crate::storage::{ClusterwideSpaceId, PropertyName};
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
        pub current_grade: Option<CurrentGrade>,
        /// Can be set by instance
        pub target_grade: Option<TargetGradeVariant>,
        pub failure_domain: Option<FailureDomain>,
    }

    pub struct Response {}
}

impl Request {
    #[inline]
    pub fn new(instance_id: InstanceId, cluster_id: String) -> Self {
        Self {
            instance_id,
            cluster_id,
            ..Request::default()
        }
    }
    #[inline]
    pub fn with_current_grade(mut self, value: CurrentGrade) -> Self {
        self.current_grade = Some(value);
        self
    }
    #[inline]
    pub fn with_target_grade(mut self, value: TargetGradeVariant) -> Self {
        self.target_grade = Some(value);
        self
    }
    #[inline]
    pub fn with_failure_domain(mut self, value: FailureDomain) -> Self {
        self.failure_domain = Some(value);
        self
    }
}

/// Processes the [`rpc::update_instance::Request`] and appends
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

    if req.cluster_id != cluster_id {
        return Err(Error::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id,
            cluster_cluster_id: cluster_id,
        });
    }

    let deadline = fiber::clock().saturating_add(timeout);
    loop {
        let instance = storage
            .instances
            .get(&req.instance_id)?
            .update(&req, storage)
            .map_err(raft::Error::ConfChangeError)?;
        let dml = Dml::replace(ClusterwideSpaceId::Instance, &instance)
            .expect("encoding should not fail");

        let ranges = vec![
            cas::Range::new(ClusterwideSpaceId::Instance),
            cas::Range::new(ClusterwideSpaceId::Address),
            cas::Range::new(ClusterwideSpaceId::Property).eq((PropertyName::ReplicationFactor,)),
        ];
        let res = cas::compare_and_swap(
            Op::Dml(dml),
            cas::Predicate {
                index: raft_storage.applied()?,
                term: raft_storage.term()?,
                ranges,
            },
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
                if err.is_cas_err() | err.is_term_mismatch_err() {
                    // cas error - retry
                    fiber::sleep(Duration::from_millis(500));
                    continue;
                } else {
                    return Err(err);
                }
            }
        }
        node.main_loop.wakeup();
        return Ok(());
    }
}
