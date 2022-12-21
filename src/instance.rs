use super::failure_domain::FailureDomain;
use super::replicaset::ReplicasetId;
use crate::has_grades;
use crate::traft::RaftId;
use crate::util::Transition;
use ::serde::{Deserialize, Serialize};
use ::tarantool::tlua;
use ::tarantool::tuple::Encode;
use grade::{CurrentGrade, TargetGrade};

pub mod grade;

crate::define_string_newtype! {
    /// Unique id of a cluster instance.
    ///
    /// This is a new-type style wrapper around String,
    /// to distinguish it from other strings.
    pub struct InstanceId(pub String);
}

////////////////////////////////////////////////////////////////////////////////
/// Serializable struct representing a member of the raft group.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Instance {
    /// Instances are identified by name.
    pub instance_id: InstanceId,
    pub instance_uuid: String,

    /// Used for identifying raft nodes.
    /// Must be unique in the raft group.
    pub raft_id: RaftId,

    /// Name of a replicaset the instance belongs to.
    pub replicaset_id: ReplicasetId,
    pub replicaset_uuid: String,

    /// The cluster's mind about actual state of this instance's activity.
    pub current_grade: CurrentGrade,
    /// The desired state of this instance
    pub target_grade: TargetGrade,

    /// Instance failure domains. Instances with overlapping failure domains
    /// must not be in the same replicaset.
    // TODO: raft_group space is kinda bloated, maybe we should store some data
    // in different spaces/not deserialize the whole tuple every time?
    pub failure_domain: FailureDomain,
}
impl Encode for Instance {}

impl Instance {
    /// Instance has a grade that implies it may cooperate.
    /// Currently this means that target_grade is neither Offline nor Expelled.
    #[inline]
    pub fn may_respond(&self) -> bool {
        has_grades!(self, * -> not Offline) && has_grades!(self, * -> not Expelled)
    }

    #[inline]
    pub fn is_reincarnated(&self) -> bool {
        self.current_grade.incarnation < self.target_grade.incarnation
    }

    /// Only used for testing.
    pub(crate) fn default() -> Self {
        Self {
            instance_id: Default::default(),
            instance_uuid: Default::default(),
            raft_id: Default::default(),
            replicaset_id: Default::default(),
            replicaset_uuid: Default::default(),
            current_grade: Default::default(),
            target_grade: Default::default(),
            failure_domain: Default::default(),
        }
    }
}

impl std::fmt::Display for Instance {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f,
            "({}, {}, {}, {}, {})",
            self.instance_id,
            self.raft_id,
            self.replicaset_id,
            Transition { from: self.current_grade, to: self.target_grade },
            &self.failure_domain,
        )
    }
}
