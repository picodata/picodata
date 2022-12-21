use super::replicaset::ReplicasetId;
use crate::has_grades;
use crate::traft::FailureDomain;
use crate::traft::RaftId;
use crate::traft::{CurrentGrade, TargetGrade};
use crate::util::Transition;
use ::serde::{Deserialize, Serialize};
use ::tarantool::tlua;
use ::tarantool::tuple::Encode;

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

/// Check if instance's current and target grades match the specified pattern.
/// # Examples:
/// ```rust
/// // Check if current_grade == `Offline`, target_grade == `Online`
/// has_grades!(instance, Offline -> Online);
///
/// // Check if current grade == `Online`, target grade can be anything
/// has_grades!(instance, Online -> *);
///
/// // Check if target grade != `Expelled`, current grade can be anything
/// has_grades!(instance, * -> not Expelled);
///
/// // This is always `true`
/// has_grades!(instance, * -> *);
///
/// // Other combinations can also work
/// ```
#[macro_export]
macro_rules! has_grades {
    // Entry rule
    ($instance:expr, $($tail:tt)+) => {
        has_grades!(@impl $instance; current[] target[] $($tail)+)
    };

    // Parsing current
    (@impl $i:expr; current[] target[] not $($tail:tt)+) => {
        has_grades!(@impl $i; current[ ! ] target[] $($tail)+)
    };
    (@impl $i:expr; current[] target[] * -> $($tail:tt)+) => {
        has_grades!(@impl $i; current[ true ] target[] $($tail)+)
    };
    (@impl $i:expr; current[ $($not:tt)? ] target[] $current:ident -> $($tail:tt)+) => {
        has_grades!(@impl $i;
            current[ $($not)? matches!($i.current_grade.variant, $crate::traft::CurrentGradeVariant::$current) ]
            target[]
            $($tail)+
        )
    };

    // Parsing target
    (@impl $i:expr; current[ $($c:tt)* ] target[] not $($tail:tt)+) => {
        has_grades!(@impl $i; current[ $($c)* ] target[ ! ] $($tail)+)
    };
    (@impl $i:expr; current[ $($c:tt)* ] target[] *) => {
        has_grades!(@impl $i; current[ $($c)* ] target[ true ])
    };
    (@impl $i:expr; current[ $($c:tt)* ] target[ $($not:tt)? ] $target:ident) => {
        has_grades!(@impl $i;
            current[ $($c)* ]
            target[ $($not)? matches!($i.target_grade.variant, $crate::traft::TargetGradeVariant::$target) ]
        )
    };

    // Terminating rule
    (@impl $i:expr; current[ $($c:tt)+ ] target[ $($t:tt)+ ]) => {
        $($c)+ && $($t)+
    };
}

////////////////////////////////////////////////////////////////////////////////
/// tests
#[cfg(test)]
mod tests {
    use super::Instance;
    use crate::has_grades;
    use crate::traft::{CurrentGradeVariant, TargetGradeVariant};

    #[test]
    fn has_grades() {
        let mut i = Instance::default();
        i.current_grade.variant = CurrentGradeVariant::Online;
        i.target_grade.variant = TargetGradeVariant::Offline;

        assert!(has_grades!(i, * -> *));
        assert!(has_grades!(i, * -> Offline));
        assert!(has_grades!(i, * -> not Online));
        assert!(!has_grades!(i, * -> Online));
        assert!(!has_grades!(i, * -> not Offline));

        assert!(has_grades!(i, Online -> *));
        assert!(has_grades!(i, Online -> Offline));
        assert!(has_grades!(i, Online -> not Online));
        assert!(!has_grades!(i, Online -> Online));
        assert!(!has_grades!(i, Online -> not Offline));

        assert!(has_grades!(i, not Offline -> *));
        assert!(has_grades!(i, not Offline -> Offline));
        assert!(has_grades!(i, not Offline -> not Online));
        assert!(!has_grades!(i, not Offline -> Online));
        assert!(!has_grades!(i, not Offline -> not Offline));

        assert!(!has_grades!(i, Offline -> *));
        assert!(!has_grades!(i, Offline -> Offline));
        assert!(!has_grades!(i, Offline -> not Online));
        assert!(!has_grades!(i, Offline -> Online));
        assert!(!has_grades!(i, Offline -> not Offline));

        assert!(!has_grades!(i, not Online -> *));
        assert!(!has_grades!(i, not Online -> Offline));
        assert!(!has_grades!(i, not Online -> not Online));
        assert!(!has_grades!(i, not Online -> Online));
        assert!(!has_grades!(i, not Online -> not Offline));
    }
}
