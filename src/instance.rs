use super::failure_domain::FailureDomain;
use super::replicaset::ReplicasetId;
use crate::has_grades;
use crate::traft::{instance_uuid, replicaset_uuid, RaftId};
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
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
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
    /// Construct an instance.
    pub fn new(
        raft_id: Option<RaftId>,
        instance_id: Option<impl Into<InstanceId>>,
        replicaset_id: Option<impl Into<ReplicasetId>>,
        current_grade: CurrentGrade,
        target_grade: TargetGrade,
        failure_domain: FailureDomain,
    ) -> Self {
        let instance_id = instance_id.map(Into::into).unwrap_or_else(|| "i1".into());
        let replicaset_id = replicaset_id
            .map(Into::into)
            .unwrap_or_else(|| ReplicasetId::from("r1"));
        let raft_id = raft_id.unwrap_or(1);
        let instance_uuid = instance_uuid(&instance_id);
        let replicaset_uuid = replicaset_uuid(&replicaset_id);
        Self {
            instance_id,
            raft_id,
            replicaset_id,
            current_grade,
            target_grade,
            failure_domain,
            instance_uuid,
            replicaset_uuid,
        }
    }

    /// Instance has a grade that implies it may cooperate.
    /// Currently this means that target_grade is neither Offline nor Expelled.
    #[inline]
    #[allow(clippy::nonminimal_bool)]
    pub fn may_respond(&self) -> bool {
        has_grades!(self, * -> not Offline) && has_grades!(self, * -> not Expelled)
    }

    #[inline]
    pub fn is_reincarnated(&self) -> bool {
        self.current_grade.incarnation < self.target_grade.incarnation
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

#[rustfmt::skip]
mod tests {
    use std::collections::HashSet;

    use crate::failure_domain::FailureDomain;
    use crate::instance::grade::{CurrentGrade, Grade, TargetGrade, TargetGradeVariant};
    use crate::replicaset::ReplicasetId;
    use crate::rpc::join::build_instance;
    use crate::storage::Clusterwide;
    use crate::rpc;
    use crate::rpc::update_instance::update_instance;

    use super::*;

    trait IntoGrade<T> {
        fn into_grade(self) -> Grade<T>;
    }

    impl<T> IntoGrade<T> for Grade<T> {
        fn into_grade(self) -> Self {
            self
        }
    }

    impl<T> IntoGrade<T> for T {
        fn into_grade(self) -> Grade<T> {
            Grade { variant: self, incarnation: 0 }
        }
    }

    trait ModifyUpdateInstanceRequest {
        fn modify(self, req: rpc::update_instance::Request) -> rpc::update_instance::Request;
    }

    impl ModifyUpdateInstanceRequest for CurrentGrade {
        fn modify(self, req: rpc::update_instance::Request) -> rpc::update_instance::Request {
            req.with_current_grade(self)
        }
    }

    impl ModifyUpdateInstanceRequest for TargetGradeVariant {
        fn modify(self, req: rpc::update_instance::Request) -> rpc::update_instance::Request {
            req.with_target_grade(self)
        }
    }

    fn set_grade(
        mut instance: Instance,
        storage: &Clusterwide,
        grade: impl ModifyUpdateInstanceRequest,
    ) -> Result<Instance, String> {
        let req = rpc::update_instance::Request::new(instance.instance_id.clone(), "".into());
        let req = grade.modify(req);
        update_instance(&mut instance, &req, storage)?;
        Ok(instance)
    }

    fn set_faildoms(
        mut instance: Instance,
        storage: &Clusterwide,
        failure_domain: FailureDomain,
    ) -> Result<Instance, String> {
        let instance_id = instance.instance_id.clone();
        update_instance(
            &mut instance,
            &rpc::update_instance::Request::new(instance_id, "".into())
                .with_failure_domain(failure_domain),
            storage,
        )?;
        Ok(instance)
    }

    macro_rules! faildoms {
        ($(,)?) => { FailureDomain::default() };
        ($($k:tt : $v:tt),+ $(,)?) => {
            FailureDomain::from([$((stringify!($k), stringify!($v))),+])
        }
    }

    fn setup_storage(storage: &Clusterwide, instances: Vec<Instance>, replication_factor: usize) {
        for instance in instances {
            storage.instances.put(&instance).unwrap();
        }
        storage.properties.put(crate::storage::PropertyName::ReplicationFactor, &replication_factor).unwrap();
    }

    fn replication_ids(replicaset_id: &ReplicasetId, storage: &Clusterwide) -> HashSet<RaftId> {
        storage
            .instances
            .replicaset_instances(replicaset_id)
            .expect("storage should not fail")
            .map(|i| i.raft_id).collect()
    }

    #[::tarantool::test]
    fn test_simple() {
        let storage = Clusterwide::new().unwrap();
        setup_storage(&storage, vec![], 1);

        let instance = build_instance(None, None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(1), Some("i1"), Some("r1"), CurrentGrade::offline(0), TargetGrade::offline(0), FailureDomain::default()),
        );
        storage.instances.put(&instance).unwrap();

        let instance = build_instance(None, None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(2), Some("i2"), Some("r2"), CurrentGrade::offline(0), TargetGrade::offline(0), FailureDomain::default()),
        );
        storage.instances.put(&instance).unwrap();

        let instance = build_instance(None, Some(&ReplicasetId::from("R3")), &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(3), Some("i3"), Some("R3"), CurrentGrade::offline(0), TargetGrade::offline(0), FailureDomain::default()),
        );
        storage.instances.put(&instance).unwrap();

        let instance = build_instance(Some(&InstanceId::from("I4")), None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(4), Some("I4"), Some("r3"), CurrentGrade::offline(0), TargetGrade::offline(0), FailureDomain::default()),
        );
        storage.instances.put(&instance).unwrap();
    }

    #[::tarantool::test]
    fn test_override() {
        let storage = Clusterwide::new().unwrap();
        setup_storage(&storage, vec![
            Instance::new(Some(1), Some("i1"), Some("r1"), CurrentGrade::online(1), TargetGrade::online(1), FailureDomain::default()),
            Instance::new(Some(2), Some("i2"), Some("r2-original"), CurrentGrade::offline(0), TargetGrade::offline(0), FailureDomain::default()),
        ],
        2);

        // join::Request with a given instance_id online.
        // - It must be an impostor, return an error.
        // - Even if it's a fair rebootstrap, it will be marked as
        //   unreachable soon (when we implement failover) an the error
        //   will be gone.
        assert_eq!(
            build_instance(Some(&InstanceId::from("i1")), None, &FailureDomain::default(), &storage)
                .unwrap_err(),
            "i1 is already joined",
        );

        // join::Request with a given instance_id offline (or unreachable).
        // - Presumably it's a rebootstrap.
        //   1. Perform auto-expel, unless it threatens data safety (TODO).
        //   2. Assign new raft_id.
        //   3. Assign new replicaset_id, unless specified explicitly. A
        //      new replicaset_id might be the same as before, since
        //      auto-expel provided a vacant place there. Or it might be
        //      not, if replication_factor / failure_domain were edited.
        // - Even if it's an impostor, rely on auto-expel policy.
        //   Disruption isn't destructive if auto-expel allows (TODO).
        assert_eq!(
            build_instance(Some(&InstanceId::from("i2")), None, &FailureDomain::default(), &storage).unwrap(),
            (Instance::new(Some(3), Some("i2"), Some("r1"), CurrentGrade::offline(0), TargetGrade::offline(0), FailureDomain::default())),
            // Attention: generated replicaset_id differs from the
            // original one, as well as raft_id.
            // That's a desired behavior.
        );
        assert_eq!(replication_ids(&ReplicasetId::from("r1"), &storage), HashSet::from([1]));

        // TODO
        //
        // join::Request with a given instance_id bootstrtapping.
        // - Presumably it's a retry after tarantool bootstrap failure.
        //   1. Perform auto-expel (it's always ok until bootstrap
        //      finishes).
        //   2. Assign a new raft_id.
        //   3. Assign new replicaset_id. Same as above.
        // - If it's actually an impostor (instance_id collision),
        //   original instance (that didn't report it has finished
        //   bootstrapping yet) will be disrupted.
    }

    #[::tarantool::test]
    fn test_instance_id_collision() {
        let storage = Clusterwide::new().unwrap();
        setup_storage(&storage, vec![
            Instance::new(Some(1), Some("i1"), Some("r1"), CurrentGrade::online(1), TargetGrade::online(1), FailureDomain::default()),
            Instance::new(Some(2), Some("i3"), Some("r3"), CurrentGrade::online(1), TargetGrade::online(1), FailureDomain::default()),
            // Attention: i3 has raft_id=2
        ], 1);

        assert_eq!(
            build_instance(None, Some(&ReplicasetId::from("r2")), &FailureDomain::default(), &storage).unwrap(),
            Instance::new(Some(3), Some("i3-2"), Some("r2"), CurrentGrade::offline(0), TargetGrade::offline(0), FailureDomain::default()),
        );
    }

    #[::tarantool::test]
    fn test_replication_factor() {
        let storage = Clusterwide::new().unwrap();
        setup_storage(&storage, vec![
            Instance::new(Some(9), Some("i9"), Some("r9"), CurrentGrade::online(1), TargetGrade::online(1), FailureDomain::default()),
            Instance::new(Some(10), Some("i10"), Some("r9"), CurrentGrade::online(1), TargetGrade::online(1), FailureDomain::default()),
        ],
        2);

        let instance = build_instance(Some(&InstanceId::from("i1")), None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(11), Some("i1"), Some("r1"), CurrentGrade::offline(0), TargetGrade::offline(0), FailureDomain::default()),
        );
        storage.instances.put(&instance).unwrap();
        assert_eq!(replication_ids(&ReplicasetId::from("r1"), &storage), HashSet::from([11]));

        let instance = build_instance(Some(&InstanceId::from("i2")), None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(12), Some("i2"), Some("r1"), CurrentGrade::offline(0), TargetGrade::offline(0), FailureDomain::default()),
        );
        storage.instances.put(&instance).unwrap();
        assert_eq!(replication_ids(&ReplicasetId::from("r1"), &storage), HashSet::from([11, 12]));

        let instance = build_instance(Some(&InstanceId::from("i3")), None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(13), Some("i3"), Some("r2"), CurrentGrade::offline(0), TargetGrade::offline(0), FailureDomain::default()),
        );
        storage.instances.put(&instance).unwrap();
        assert_eq!(replication_ids(&ReplicasetId::from("r2"), &storage), HashSet::from([13]));

        let instance = build_instance(Some(&InstanceId::from("i4")), None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            Instance::new(Some(14), Some("i4"), Some("r2"), CurrentGrade::offline(0), TargetGrade::offline(0), FailureDomain::default()),
        );
        storage.instances.put(&instance).unwrap();
        assert_eq!(replication_ids(&ReplicasetId::from("r2"), &storage), HashSet::from([13, 14]));
    }

    #[::tarantool::test]
    fn test_update_grade() {
        let storage = Clusterwide::new().unwrap();
        let instance_v0 = Instance::new(Some(1), Some("i1"), Some("r1"), CurrentGrade::online(1), TargetGrade::online(1), FailureDomain::default());
        setup_storage(&storage, vec![instance_v0.clone()], 1);

        // Current grade incarnation is allowed to go down,
        // governor has the authority over it
        let instance_v1 = set_grade(instance_v0.clone(), &storage, CurrentGrade::offline(0)).unwrap();
        storage.instances.put(&instance_v1).unwrap();
        assert_eq!(
            instance_v1,
            Instance::new(Some(1), Some("i1"), Some("r1"), CurrentGrade::offline(0), TargetGrade::online(1), FailureDomain::default())
        );

        // idempotency
        let instance_v2 = set_grade(instance_v1.clone(), &storage, CurrentGrade::offline(0)).unwrap();
        storage.instances.put(&instance_v2).unwrap();
        assert_eq!(
            instance_v2,
            Instance::new(Some(1), Some("i1"), Some("r1"), CurrentGrade::offline(0), TargetGrade::online(1), FailureDomain::default())
        );

        // TargetGradeVariant::Offline takes incarnation from current grade
        let instance_v3 = set_grade(instance_v2.clone(), &storage, TargetGradeVariant::Offline).unwrap();
        storage.instances.put(&instance_v3).unwrap();
        assert_eq!(
            instance_v3,
            Instance::new(Some(1), Some("i1"), Some("r1"), CurrentGrade::offline(0), TargetGrade::offline(0), FailureDomain::default()),
        );

        // TargetGradeVariant::Online increases incarnation
        let instance_v4 = set_grade(instance_v3.clone(), &storage, TargetGradeVariant::Online).unwrap();
        storage.instances.put(&instance_v4).unwrap();
        assert_eq!(
            instance_v4,
            Instance::new(Some(1), Some("i1"), Some("r1"), CurrentGrade::offline(0), TargetGrade::online(1), FailureDomain::default())
        );

        // No idempotency, incarnation goes up
        let instance_v5 = set_grade(instance_v4.clone(), &storage, TargetGradeVariant::Online).unwrap();
        storage.instances.put(&instance_v5).unwrap();
        assert_eq!(
            instance_v5,
            Instance::new(Some(1), Some("i1"), Some("r1"), CurrentGrade::offline(0), TargetGrade::online(2), FailureDomain::default())
        );

        // TargetGrade::Expelled takes incarnation from current grade
        let instance_v6 = set_grade(instance_v5.clone(), &storage, TargetGradeVariant::Expelled).unwrap();
        storage.instances.put(&instance_v6).unwrap();
        assert_eq!(
            instance_v6,
            Instance::new(Some(1), Some("i1"), Some("r1"), CurrentGrade::offline(0), TargetGrade::expelled(0), FailureDomain::default()),
        );

        // Instance get's expelled
        let instance_v7 = set_grade(instance_v6.clone(), &storage, CurrentGrade::expelled(69)).unwrap();
        storage.instances.put(&instance_v7).unwrap();
        assert_eq!(
            instance_v7,
            Instance::new(Some(1), Some("i1"), Some("r1"), CurrentGrade::expelled(69), TargetGrade::expelled(0), FailureDomain::default()),
        );

        // Updating expelled instances isn't allowed
        assert_eq!(
            set_grade(instance_v7, &storage, TargetGradeVariant::Online).unwrap_err(),
            "cannot update expelled instance \"i1\"",
        );
    }

    #[::tarantool::test]
    fn failure_domain() {
        let storage = Clusterwide::new().unwrap();
        setup_storage(&storage, vec![], 3);
        
        let instance = 
            build_instance(None, None, &faildoms! {planet: Earth}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();

        let instance = 
            build_instance(None, None, &faildoms! {planet: Earth}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r2");
        storage.instances.put(&instance).unwrap();

        let instance = 
            build_instance(None, None, &faildoms! {planet: Mars}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();

        let instance = 
            build_instance(None, None, &faildoms! {planet: Earth, os: BSD}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r3");
        storage.instances.put(&instance).unwrap();

        let instance = 
            build_instance(None, None, &faildoms! {planet: Mars, os: BSD}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r2");
        storage.instances.put(&instance).unwrap();

        assert_eq!(
            build_instance(None, None, &faildoms! {os: Arch}, &storage)
                .unwrap_err(),
            "missing failure domain names: PLANET",
        );

        let instance = 
            build_instance(None, None, &faildoms! {planet: Venus, os: Arch}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();

        let instance = 
            build_instance(None, None, &faildoms! {planet: Venus, os: Mac}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r2");
        storage.instances.put(&instance).unwrap();

        let instance = 
            build_instance(None, None, &faildoms! {planet: Mars, os: Mac}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r3");
        storage.instances.put(&instance).unwrap();

        assert_eq!(
            build_instance(None, None, &faildoms! {}, &storage)
                .unwrap_err(),
            "missing failure domain names: OS, PLANET",
        );
    }

    #[::tarantool::test]
    fn reconfigure_failure_domain() {
        let storage = Clusterwide::new().unwrap();
        setup_storage(&storage, vec![], 3);

        // first instance
        let instance1_v1 = build_instance(Some(&InstanceId::from("i1")), None, &faildoms! {planet: Earth}, &storage).unwrap();
        storage.instances.put(&instance1_v1).unwrap();
        assert_eq!(instance1_v1.failure_domain, faildoms! {planet: Earth});
        assert_eq!(instance1_v1.replicaset_id, "r1");

        // reconfigure single instance, fail
        assert_eq!(
            set_faildoms(instance1_v1.clone(), &storage, faildoms! {owner: Ivan})
                .unwrap_err(),
            "missing failure domain names: PLANET",
        );

        // reconfigure single instance, success
        let instance1_v2 = set_faildoms(instance1_v1.clone(), &storage, faildoms! {planet: Mars, owner: Ivan}).unwrap();
        storage.instances.put(&instance1_v2).unwrap();
        assert_eq!(instance1_v2.failure_domain, faildoms! {planet: Mars, owner: Ivan});
        assert_eq!(instance1_v2.replicaset_id, "r1"); // same replicaset

        // second instance won't be joined without the newly added required
        // failure domain subdivision of "OWNER"
        assert_eq!(
            build_instance(Some(&InstanceId::from("i2")), None, &faildoms! {planet: Mars}, &storage)
                .unwrap_err(),
            "missing failure domain names: OWNER",
        );

        // second instance
        #[rustfmt::skip]
        let instance2_v1 = build_instance(Some(&InstanceId::from("i2")), None, &faildoms! {planet: Mars, owner: Mike}, &storage)
            .unwrap();
        storage.instances.put(&instance2_v1).unwrap();
        assert_eq!(instance2_v1.failure_domain, faildoms! {planet: Mars, owner: Mike});
        // doesn't fit into r1
        assert_eq!(instance2_v1.replicaset_id, "r2");

        // reconfigure second instance, success
        let instance2_v2 = set_faildoms(instance2_v1.clone(), &storage, faildoms! {planet: Earth, owner: Mike}).unwrap();
        storage.instances.put(&instance2_v2).unwrap();
        assert_eq!(instance2_v2.failure_domain, faildoms! {planet: Earth, owner: Mike});
        // replicaset doesn't change automatically
        assert_eq!(instance2_v2.replicaset_id, "r2");

        // add instance with new subdivision
        #[rustfmt::skip]
        let instance3_v1 = build_instance(Some(&InstanceId::from("i3")), None, &faildoms! {planet: B, owner: V, dimension: C137}, &storage)
            .unwrap();
        storage.instances.put(&instance3_v1).unwrap();
        assert_eq!(
            instance3_v1.failure_domain,
            faildoms! {planet: B, owner: V, dimension: C137}
        );
        assert_eq!(instance3_v1.replicaset_id, "r1");

        // even though the only instance with failure domain subdivision of
        // `DIMENSION` is inactive, we can't add an instance without that
        // subdivision
        #[rustfmt::skip]
        assert_eq!(
            build_instance(Some(&InstanceId::from("i4")), None, &faildoms! {planet: Theia, owner: Me}, &storage)
                .unwrap_err(),
            "missing failure domain names: DIMENSION",
        );
    }
}
