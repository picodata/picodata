use crate::tlog;
use crate::traft::FailureDomain;
use crate::traft::Result;
use crate::traft::{error::Error, node, InstanceId};
use crate::traft::{CurrentGrade, TargetGradeVariant};

crate::define_rpc_request! {
    fn proc_update_instance(req: Request) -> Result<Response> {
        let node = node::global()?;

        let cluster_id = node
            .raft_storage
            .cluster_id()?
            .expect("cluster_id is set at boot");

        if req.cluster_id != cluster_id {
            return Err(Error::ClusterIdMismatch {
                instance_cluster_id: req.cluster_id,
                cluster_cluster_id: cluster_id,
            });
        }

        let mut req = req;
        let instance_id = &*req.instance_id;
        if let Some(current_grade) = req.current_grade.take() {
            tlog!(Warning, "attempt to change current_grade by instance";
                "instance_id" => instance_id,
                "current_grade" => %current_grade,
            );
        }
        match node.handle_topology_request_and_wait(req.into()) {
            Ok(_) => Ok(Response::Ok {}),
            Err(Error::NotALeader) => Ok(Response::ErrNotALeader),
            Err(e) => Err(e),
        }
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

    /// Response to a [`Request`]
    pub enum Response {
        Ok,
        ErrNotALeader,
    }
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
