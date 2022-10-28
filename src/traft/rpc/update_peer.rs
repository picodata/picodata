use crate::tlog;
use crate::traft::Peer;
use crate::traft::Result;
use crate::traft::{error::Error, node, InstanceId};
use crate::traft::{CurrentGrade, FailureDomain, TargetGrade};

crate::define_rpc_request! {
    fn proc_update_peer(req: Request) -> Result<Response> {
        let node = node::global()?;

        let cluster_id = node
            .storage
            .raft
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
        req.changes.retain(|ch| match ch {
            PeerChange::CurrentGrade(grade) => {
                tlog!(Warning, "attempt to change grade by peer";
                    "instance_id" => instance_id,
                    "grade" => grade.as_str(),
                );
                false
            }
            _ => true,
        });
        match node.handle_topology_request_and_wait(req.into()) {
            Ok(_) => Ok(Response::Ok {}),
            Err(Error::NotALeader) => Ok(Response::ErrNotALeader),
            Err(e) => Err(e),
        }
    }

    /// Request to update the instance in the storage.
    pub struct Request {
        pub instance_id: InstanceId,
        pub cluster_id: String,
        pub changes: Vec<PeerChange>,
    }

    /// Response to a [`Request`]
    pub enum Response {
        Ok,
        ErrNotALeader,
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum PeerChange {
    CurrentGrade(CurrentGrade),
    TargetGrade(TargetGrade),
    FailureDomain(FailureDomain),
}

impl PeerChange {
    pub fn apply(self, peer: &mut Peer) {
        match self {
            Self::CurrentGrade(value) => peer.current_grade = value,
            Self::TargetGrade(value) => peer.target_grade = value,
            Self::FailureDomain(value) => peer.failure_domain = value,
        }
    }
}

impl Request {
    #[inline]
    pub fn new(instance_id: InstanceId, cluster_id: String) -> Self {
        Self {
            instance_id,
            cluster_id,
            changes: vec![],
        }
    }
    #[inline]
    pub fn with_current_grade(mut self, value: CurrentGrade) -> Self {
        self.changes.push(PeerChange::CurrentGrade(value));
        self
    }
    #[inline]
    pub fn with_target_grade(mut self, value: TargetGrade) -> Self {
        self.changes.push(PeerChange::TargetGrade(value));
        self
    }
    #[inline]
    pub fn with_failure_domain(mut self, value: FailureDomain) -> Self {
        self.changes.push(PeerChange::FailureDomain(value));
        self
    }
}
