use std::collections::HashSet;

use self::grade::{CurrentGradeVariant, Grade, TargetGradeVariant};

use super::failure_domain::FailureDomain;
use super::replicaset::ReplicasetId;
use crate::has_grades;
use crate::rpc;
use crate::storage::Clusterwide;
use crate::traft::{instance_uuid, replicaset_uuid, RaftId};
use crate::util::{Transition, Uppercase};
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
    pub fn new(
        instance_id: Option<&InstanceId>,
        replicaset_id: Option<&ReplicasetId>,
        failure_domain: &FailureDomain,
        storage: &Clusterwide,
    ) -> Result<Self, String> {
        if let Some(id) = instance_id {
            let existing_instance = storage.instances.get(id);
            if matches!(existing_instance, Ok(instance) if has_grades!(instance, Online -> *)) {
                let e = format!("{} is already joined", id);
                return Err(e);
            }
        }

        check_required_failure_domain(failure_domain, &storage.cache().failure_domain_names)?;

        // Anyway, `join` always produces a new raft_id.
        let raft_id = storage.cache().max_raft_id + 1;
        let instance_id = instance_id
            .map(Clone::clone)
            .unwrap_or_else(|| choose_instance_id(raft_id, storage));
        let instance_uuid = instance_uuid(&instance_id);
        let replicaset_id = replicaset_id
            .map(Clone::clone)
            .unwrap_or_else(|| choose_replicaset_id(failure_domain, storage));
        let replicaset_uuid = replicaset_uuid(&replicaset_id);

        let instance = Instance {
            instance_id,
            instance_uuid,
            raft_id,
            replicaset_id,
            replicaset_uuid,
            current_grade: CurrentGrade::offline(0),
            target_grade: TargetGrade::offline(0),
            failure_domain: failure_domain.clone(),
        };

        Ok(instance)
    }

    /// Create first instance in the cluster
    pub fn initial(
        instance_id: Option<InstanceId>,
        replicaset_id: Option<ReplicasetId>,
        failure_domain: FailureDomain,
    ) -> Self {
        let instance_id = instance_id.unwrap_or_else(|| "i1".into());
        let replicaset_id = replicaset_id.unwrap_or_else(|| ReplicasetId::from("r1"));
        let instance_uuid = instance_uuid(&instance_id);
        let replicaset_uuid = replicaset_uuid(&replicaset_id);
        Self {
            instance_id,
            raft_id: 1,
            replicaset_id,
            current_grade: CurrentGrade::offline(0),
            target_grade: TargetGrade::offline(0),
            failure_domain,
            instance_uuid,
            replicaset_uuid,
        }
    }

    pub fn update(
        &self,
        req: &rpc::update_instance::Request,
        storage: &Clusterwide,
    ) -> Result<Self, String> {
        let mut instance = self.clone();
        if self.current_grade == CurrentGradeVariant::Expelled
            && !matches!(
                req,
                rpc::update_instance::Request {
                    target_grade: None,
                    current_grade: Some(current_grade),
                    failure_domain: None,
                    ..
                } if *current_grade == CurrentGradeVariant::Expelled
            )
        {
            return Err(format!(
                "cannot update expelled instance \"{}\"",
                self.instance_id
            ));
        }

        if let Some(fd) = req.failure_domain.as_ref() {
            check_required_failure_domain(fd, &storage.cache().failure_domain_names)?;
            instance.failure_domain = fd.clone();
        }

        if let Some(value) = req.current_grade {
            instance.current_grade = value;
        }

        if let Some(variant) = req.target_grade {
            let incarnation = match variant {
                TargetGradeVariant::Online => self.target_grade.incarnation + 1,
                _ => self.current_grade.incarnation,
            };
            instance.target_grade = Grade {
                variant,
                incarnation,
            };
        }

        Ok(instance)
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

fn choose_instance_id(raft_id: RaftId, storage: &Clusterwide) -> InstanceId {
    let mut suffix: Option<u64> = None;
    loop {
        let ret = match suffix {
            None => format!("i{raft_id}"),
            Some(x) => format!("i{raft_id}-{x}"),
        }
        .into();

        if !storage
            .instances
            .contains(&ret)
            .expect("storage should not fail")
        {
            return ret;
        }

        suffix = Some(suffix.map_or(2, |x| x + 1));
    }
}

fn choose_replicaset_id(failure_domain: &FailureDomain, storage: &Clusterwide) -> ReplicasetId {
    'next_replicaset: for (replicaset_id, instances) in storage.cache().replicasets.iter() {
        let replication_factor = storage
            .properties
            .replication_factor()
            .expect("storage should not fail");

        if instances.len() < replication_factor {
            for instance_id in instances {
                let instance = storage.instances.get(instance_id).unwrap();
                if instance.failure_domain.intersects(failure_domain) {
                    continue 'next_replicaset;
                }
            }
            return replicaset_id.clone();
        }
    }

    let mut i = 0u64;
    loop {
        i += 1;
        let replicaset_id = ReplicasetId(format!("r{i}"));
        if storage.cache().replicasets.get(&replicaset_id).is_none() {
            return replicaset_id;
        }
    }
}

pub fn check_required_failure_domain(
    fd: &FailureDomain,
    required_domains: &HashSet<Uppercase>,
) -> Result<(), String> {
    let mut res = Vec::new();
    for domain_name in required_domains {
        if !fd.contains_name(domain_name) {
            res.push(domain_name.to_string());
        }
    }

    if res.is_empty() {
        return Ok(());
    }

    res.sort();
    Err(format!("missing failure domain names: {}", res.join(", ")))
}

pub fn replication_ids(replicaset_id: &ReplicasetId, storage: &Clusterwide) -> HashSet<RaftId> {
    if let Some(replication_ids) = storage.cache().replicasets.get(replicaset_id) {
        replication_ids
            .iter()
            .map(|id| {
                let instance = storage.instances.get(id).expect("storage should not fail");
                instance.raft_id
            })
            .collect()
    } else {
        HashSet::new()
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
    use crate::storage::Clusterwide;
    use crate::traft::instance_uuid;
    use crate::traft::replicaset_uuid;
    use crate::rpc;

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

    macro_rules! instances {
        [ $( ( $($instance:tt)+ ) ),* $(,)? ] => {
            vec![$( instance!($($instance)+) ),*]
        };
    }

    macro_rules! instance {
        (
            $raft_id:expr,
            $instance_id:literal,
            $replicaset_id:literal,
            $current_grade:expr,
            $target_grade:expr
            $(, $failure_domain:expr)?
            $(,)?
        ) => {
            Instance {
                raft_id: $raft_id,
                instance_id: $instance_id.into(),
                replicaset_id: $replicaset_id.into(),
                instance_uuid: instance_uuid($instance_id),
                replicaset_uuid: replicaset_uuid($replicaset_id),

                current_grade: $current_grade.into_grade(),
                target_grade: $target_grade.into_grade(),
                failure_domain: {
                    let _f = FailureDomain::default();
                    $( let _f = $failure_domain; )?
                    _f
                },
            }
        };
    }

    fn set_grade(
        instance: Instance,
        storage: &Clusterwide,
        grade: impl ModifyUpdateInstanceRequest,
    ) -> Result<Instance, String> {
        let req = rpc::update_instance::Request::new(instance.instance_id.clone(), "".into());
        let req = grade.modify(req);
        instance.update(&req, storage)
    }

    fn set_faildoms(
        instance: Instance,
        storage: &Clusterwide,
        failure_domain: FailureDomain,
    ) -> Result<Instance, String> {
        let instance_id = instance.instance_id.clone();
        instance.update(
            &rpc::update_instance::Request::new(instance_id, "".into())
                .with_failure_domain(failure_domain),
            storage,
        )
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

    #[::tarantool::test]
    fn test_simple() {
        let storage = Clusterwide::new().unwrap();
        setup_storage(&storage, vec![], 1);

        let instance = Instance::new(None, None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);

        let instance = Instance::new(None, None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            instance!(2, "i2", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);

        let instance = Instance::new(None, Some(&ReplicasetId::from("R3")), &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            instance!(3, "i3", "R3", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);

        let instance = Instance::new(Some(&InstanceId::from("I4")), None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            instance!(4, "I4", "r3", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);
    }

    #[::tarantool::test]
    fn test_override() {
        let storage = Clusterwide::new().unwrap();
        setup_storage(&storage, instances![
            (1, "i1", "r1", CurrentGrade::online(1), TargetGrade::online(1)),
            (2, "i2", "r2-original", CurrentGrade::offline(0), TargetGrade::offline(0)),
        ],
        2);

        // join::Request with a given instance_id online.
        // - It must be an impostor, return an error.
        // - Even if it's a fair rebootstrap, it will be marked as
        //   unreachable soon (when we implement failover) an the error
        //   will be gone.
        assert_eq!(
            Instance::new(Some(&InstanceId::from("i1")), None, &FailureDomain::default(), &storage)
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
            Instance::new(Some(&InstanceId::from("i2")), None, &FailureDomain::default(), &storage).unwrap(),
            (instance!(3, "i2", "r1", CurrentGrade::offline(0), TargetGrade::offline(0))),
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
        setup_storage(&storage, instances![
            (1, "i1", "r1", CurrentGrade::online(1), TargetGrade::online(1)),
            (2, "i3", "r3", CurrentGrade::online(1), TargetGrade::online(1)),
            // Attention: i3 has raft_id=2
        ], 1);

        assert_eq!(
            Instance::new(None, Some(&ReplicasetId::from("r2")), &FailureDomain::default(), &storage).unwrap(),
            instance!(3, "i3-2", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
    }

    #[::tarantool::test]
    fn test_replication_factor() {
        let storage = Clusterwide::new().unwrap();
        setup_storage(&storage, instances![
            (9, "i9", "r9", CurrentGrade::online(1), TargetGrade::online(1)),
            (10, "i10", "r9", CurrentGrade::online(1), TargetGrade::online(1)),
        ],
        2);

        let instance = Instance::new(Some(&InstanceId::from("i1")), None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            instance!(11, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);
        assert_eq!(replication_ids(&ReplicasetId::from("r1"), &storage), HashSet::from([11]));

        let instance = Instance::new(Some(&InstanceId::from("i2")), None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            instance!(12, "i2", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);
        assert_eq!(replication_ids(&ReplicasetId::from("r1"), &storage), HashSet::from([11, 12]));

        let instance = Instance::new(Some(&InstanceId::from("i3")), None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            instance!(13, "i3", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);
        assert_eq!(replication_ids(&ReplicasetId::from("r2"), &storage), HashSet::from([13]));

        let instance = Instance::new(Some(&InstanceId::from("i4")), None, &FailureDomain::default(), &storage).unwrap();
        assert_eq!(
            instance,
            instance!(14, "i4", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);
        assert_eq!(replication_ids(&ReplicasetId::from("r2"), &storage), HashSet::from([13, 14]));
    }

    #[::tarantool::test]
    fn test_update_grade() {
        let storage = Clusterwide::new().unwrap();
        let instance_v0 = instance!(1, "i1", "r1", CurrentGrade::online(1), TargetGrade::online(1));
        setup_storage(&storage, vec![instance_v0.clone()], 1);

        // Current grade incarnation is allowed to go down,
        // governor has the authority over it
        let instance_v1 = set_grade(instance_v0.clone(), &storage, CurrentGrade::offline(0)).unwrap();
        storage.instances.put(&instance_v1).unwrap();
        storage.cache_mut().on_instance_change(instance_v1.clone(), Some(instance_v0));
        assert_eq!(
            instance_v1,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(1)),
        );

        // idempotency
        let instance_v2 = set_grade(instance_v1.clone(), &storage, CurrentGrade::offline(0)).unwrap();
        storage.instances.put(&instance_v2).unwrap();
        storage.cache_mut().on_instance_change(instance_v2.clone(), Some(instance_v1));
        assert_eq!(
            instance_v2,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(1)),
        );

        // TargetGradeVariant::Offline takes incarnation from current grade
        let instance_v3 = set_grade(instance_v2.clone(), &storage, TargetGradeVariant::Offline).unwrap();
        storage.instances.put(&instance_v3).unwrap();
        storage.cache_mut().on_instance_change(instance_v3.clone(), Some(instance_v2));
        assert_eq!(
            instance_v3,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );

        // TargetGradeVariant::Online increases incarnation
        let instance_v4 = set_grade(instance_v3.clone(), &storage, TargetGradeVariant::Online).unwrap();
        storage.instances.put(&instance_v4).unwrap();
        storage.cache_mut().on_instance_change(instance_v4.clone(), Some(instance_v3));
        assert_eq!(
            instance_v4,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(1)),
        );

        // No idempotency, incarnation goes up
        let instance_v5 = set_grade(instance_v4.clone(), &storage, TargetGradeVariant::Online).unwrap();
        storage.instances.put(&instance_v5).unwrap();
        storage.cache_mut().on_instance_change(instance_v5.clone(), Some(instance_v4));
        assert_eq!(
            instance_v5,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(2)),
        );

        // TargetGrade::Expelled takes incarnation from current grade
        let instance_v6 = set_grade(instance_v5.clone(), &storage, TargetGradeVariant::Expelled).unwrap();
        storage.instances.put(&instance_v6).unwrap();
        storage.cache_mut().on_instance_change(instance_v6.clone(), Some(instance_v5));
        assert_eq!(
            instance_v6,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::expelled(0)),
        );

        // Instance get's expelled
        let instance_v7 = set_grade(instance_v6.clone(), &storage, CurrentGrade::expelled(69)).unwrap();
        storage.instances.put(&instance_v7).unwrap();
        storage.cache_mut().on_instance_change(instance_v7.clone(), Some(instance_v6));
        assert_eq!(
            instance_v7,
            instance!(1, "i1", "r1", CurrentGrade::expelled(69), TargetGrade::expelled(0)),
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
            Instance::new(None, None, &faildoms! {planet: Earth}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);

        let instance = 
            Instance::new(None, None, &faildoms! {planet: Earth}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r2");
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);

        let instance = 
            Instance::new(None, None, &faildoms! {planet: Mars}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);

        let instance = 
            Instance::new(None, None, &faildoms! {planet: Earth, os: BSD}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r3");
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);

        let instance = 
            Instance::new(None, None, &faildoms! {planet: Mars, os: BSD}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r2");
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);

        assert_eq!(
            Instance::new(None, None, &faildoms! {os: Arch}, &storage)
                .unwrap_err(),
            "missing failure domain names: PLANET",
        );

        let instance = 
            Instance::new(None, None, &faildoms! {planet: Venus, os: Arch}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);

        let instance = 
            Instance::new(None, None, &faildoms! {planet: Venus, os: Mac}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r2");
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);

        let instance = 
            Instance::new(None, None, &faildoms! {planet: Mars, os: Mac}, &storage)
                .unwrap();
        assert_eq!(instance.replicaset_id, "r3");
        storage.instances.put(&instance).unwrap();
        storage.cache_mut().on_instance_change(instance, None);

        assert_eq!(
            Instance::new(None, None, &faildoms! {}, &storage)
                .unwrap_err(),
            "missing failure domain names: OS, PLANET",
        );
    }

    #[::tarantool::test]
    fn reconfigure_failure_domain() {
        let storage = Clusterwide::new().unwrap();
        setup_storage(&storage, instances![], 3);

        // first instance
        let instance1_v1 = Instance::new(Some(&InstanceId::from("i1")), None, &faildoms! {planet: Earth}, &storage).unwrap();
        storage.instances.put(&instance1_v1).unwrap();
        storage.cache_mut().on_instance_change(instance1_v1.clone(), None);
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
        storage.cache_mut().on_instance_change(instance1_v2.clone(), Some(instance1_v1));
        assert_eq!(instance1_v2.failure_domain, faildoms! {planet: Mars, owner: Ivan});
        assert_eq!(instance1_v2.replicaset_id, "r1"); // same replicaset

        // second instance won't be joined without the newly added required
        // failure domain subdivision of "OWNER"
        assert_eq!(
            Instance::new(Some(&InstanceId::from("i2")), None, &faildoms! {planet: Mars}, &storage)
                .unwrap_err(),
            "missing failure domain names: OWNER",
        );

        // second instance
        #[rustfmt::skip]
        let instance2_v1 = Instance::new(Some(&InstanceId::from("i2")), None, &faildoms! {planet: Mars, owner: Mike}, &storage)
            .unwrap();
        storage.instances.put(&instance2_v1).unwrap();
        storage.cache_mut().on_instance_change(instance2_v1.clone(), None);
        assert_eq!(instance2_v1.failure_domain, faildoms! {planet: Mars, owner: Mike});
        // doesn't fit into r1
        assert_eq!(instance2_v1.replicaset_id, "r2");

        // reconfigure second instance, success
        let instance2_v2 = set_faildoms(instance2_v1.clone(), &storage, faildoms! {planet: Earth, owner: Mike}).unwrap();
        storage.instances.put(&instance2_v2).unwrap();
        storage.cache_mut().on_instance_change(instance2_v2.clone(), Some(instance2_v1));
        assert_eq!(instance2_v2.failure_domain, faildoms! {planet: Earth, owner: Mike});
        // replicaset doesn't change automatically
        assert_eq!(instance2_v2.replicaset_id, "r2");

        // add instance with new subdivision
        #[rustfmt::skip]
        let instance3_v1 = Instance::new(Some(&InstanceId::from("i3")), None, &faildoms! {planet: B, owner: V, dimension: C137}, &storage)
            .unwrap();
        storage.instances.put(&instance3_v1).unwrap();
        storage.cache_mut().on_instance_change(instance3_v1.clone(), None);
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
            Instance::new(Some(&InstanceId::from("i4")), None, &faildoms! {planet: Theia, owner: Me}, &storage)
                .unwrap_err(),
            "missing failure domain names: DIMENSION",
        );
    }
}
