use std::collections::{BTreeMap, HashSet};

use crate::failure_domain::FailureDomain;
use crate::has_grades;
use crate::instance::grade::{
    CurrentGrade, CurrentGradeVariant, Grade, TargetGrade, TargetGradeVariant,
};
use crate::instance::{Instance, InstanceId};
use crate::replicaset::ReplicasetId;
use crate::rpc;
use crate::storage::Clusterwide;
use crate::traft::instance_uuid;
use crate::traft::replicaset_uuid;
use crate::traft::RaftId;
use crate::util::Uppercase;

/// A shallow wrapper around the [Storage](Clusterwide),
/// providing topology related calculations.
///
/// Should be mutated only when storage is mutated.
///
/// With this in mind it should be accessible on any peer - not only leader.
#[derive(Debug)]
pub struct Topology {
    max_raft_id: RaftId,
    failure_domain_names: HashSet<Uppercase>,
    replicasets: BTreeMap<ReplicasetId, HashSet<InstanceId>>,
    storage: Clusterwide,
}

impl Topology {
    pub fn update(&mut self, instance: Instance, old_instance: Option<Instance>) {
        self.max_raft_id = std::cmp::max(self.max_raft_id, instance.raft_id);

        let instance_id = instance.instance_id.clone();
        let replicaset_id = instance.replicaset_id.clone();

        if let Some(old_instance) = old_instance {
            self.replicasets
                .get_mut(&old_instance.replicaset_id)
                .map(|r| r.remove(&old_instance.instance_id));
        }

        self.failure_domain_names
            .extend(instance.failure_domain.names().cloned());
        self.replicasets
            .entry(replicaset_id)
            .or_default()
            .insert(instance_id);
    }

    fn replication_factor(&self) -> usize {
        self.storage
            .properties
            .replication_factor()
            .expect("storage should not fail")
    }

    fn choose_instance_id(&self, raft_id: RaftId) -> InstanceId {
        let mut suffix: Option<u64> = None;
        loop {
            let ret = match suffix {
                None => format!("i{raft_id}"),
                Some(x) => format!("i{raft_id}-{x}"),
            }
            .into();

            if !self
                .storage
                .instances
                .contains(&ret)
                .expect("storage should not fail")
            {
                return ret;
            }

            suffix = Some(suffix.map_or(2, |x| x + 1));
        }
    }

    fn choose_replicaset_id(&self, failure_domain: &FailureDomain) -> ReplicasetId {
        'next_replicaset: for (replicaset_id, instances) in self.replicasets.iter() {
            if instances.len() < self.replication_factor() {
                for instance_id in instances {
                    let instance = self.storage.instances.get(instance_id).unwrap();
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
            if self.replicasets.get(&replicaset_id).is_none() {
                return replicaset_id;
            }
        }
    }

    pub fn check_required_failure_domain(&self, fd: &FailureDomain) -> Result<(), String> {
        let mut res = Vec::new();
        for domain_name in &self.failure_domain_names {
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

    pub fn build_instance(
        &self,
        instance_id: Option<&InstanceId>,
        replicaset_id: Option<&ReplicasetId>,
        failure_domain: &FailureDomain,
    ) -> Result<Instance, String> {
        if let Some(id) = instance_id {
            let existing_instance = self.storage.instances.get(id);
            if matches!(existing_instance, Ok(instance) if has_grades!(instance, Online -> *)) {
                let e = format!("{} is already joined", id);
                return Err(e);
            }
        }

        self.check_required_failure_domain(failure_domain)?;

        // Anyway, `join` always produces a new raft_id.
        let raft_id = self.max_raft_id + 1;
        let instance_id = instance_id
            .map(Clone::clone)
            .unwrap_or_else(|| self.choose_instance_id(raft_id));
        let instance_uuid = instance_uuid(&instance_id);
        let replicaset_id = replicaset_id
            .map(Clone::clone)
            .unwrap_or_else(|| self.choose_replicaset_id(failure_domain));
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

    pub fn get_replication_ids(&self, replicaset_id: &ReplicasetId) -> HashSet<RaftId> {
        if let Some(replication_ids) = self.replicasets.get(replicaset_id) {
            replication_ids
                .iter()
                .map(|id| {
                    let instance = self
                        .storage
                        .instances
                        .get(id)
                        .expect("storage should not fail");
                    instance.raft_id
                })
                .collect()
        } else {
            HashSet::new()
        }
    }

    pub fn build_updated_instance(
        &self,
        req: &rpc::update_instance::Request,
    ) -> Result<Instance, String> {
        let mut instance = self
            .storage
            .instances
            .get(&req.instance_id)
            .map_err(|err| err.to_string())?;

        if instance.current_grade == CurrentGradeVariant::Expelled
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
                instance.instance_id
            ));
        }

        if let Some(fd) = req.failure_domain.as_ref() {
            self.check_required_failure_domain(fd)?;
            instance.failure_domain = fd.clone();
        }

        if let Some(value) = req.current_grade {
            instance.current_grade = value;
        }

        if let Some(variant) = req.target_grade {
            let incarnation = match variant {
                TargetGradeVariant::Online => instance.target_grade.incarnation + 1,
                _ => instance.current_grade.incarnation,
            };
            instance.target_grade = Grade {
                variant,
                incarnation,
            };
        }

        Ok(instance)
    }
}

impl From<Clusterwide> for Topology {
    fn from(storage: Clusterwide) -> Self {
        let instances = storage
            .instances
            .all_instances()
            .expect("storage should not fail");
        let mut topology = Self {
            max_raft_id: 0,
            failure_domain_names: Default::default(),
            replicasets: Default::default(),
            storage,
        };
        for instance in instances {
            topology.update(instance, None);
        }
        topology
    }
}

/// Create first instance in the cluster
pub fn initial_instance(
    instance_id: Option<InstanceId>,
    replicaset_id: Option<ReplicasetId>,
    failure_domain: FailureDomain,
) -> Instance {
    let instance_id = instance_id.unwrap_or_else(|| "i1".into());
    let replicaset_id = replicaset_id.unwrap_or_else(|| ReplicasetId::from("r1"));
    let instance_uuid = instance_uuid(&instance_id);
    let replicaset_uuid = replicaset_uuid(&replicaset_id);
    Instance {
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

#[rustfmt::skip]
mod tests {
    use std::collections::HashSet;

    use super::Topology;

    use crate::failure_domain::FailureDomain;
    use crate::instance::grade::{CurrentGrade, Grade, TargetGrade, TargetGradeVariant};
    use crate::replicaset::ReplicasetId;
    use crate::storage::Clusterwide;
    use crate::traft::instance_uuid;
    use crate::traft::replicaset_uuid;
    use crate::traft::Instance;
    use crate::rpc;

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

    macro_rules! build_instance {
        (
            $topology:expr,
            $instance_id:expr,
            $replicaset_id:expr
            $(, $failure_domain:expr )?
            $(,)?
        ) => {
            {
                let _f = FailureDomain::default();
                $(let _f = $failure_domain; )?
                $topology.build_instance(
                    $instance_id.map(<&str>::into).as_ref(),
                    $replicaset_id.map(<&str>::into).as_ref(),
                    &_f
                )
            }
        };
    }

    macro_rules! set_grade {
        (
            $topology:expr,
            $instance_id:expr
            $(, $current_grade:expr
            $(, $target_grade:expr)?)?
            $(,)?
        ) => {
            {
                let req = rpc::update_instance::Request::new($instance_id.into(), "".into());
                $(
                    let req = $current_grade.modify(req);
                    $( let req = $target_grade.modify(req); )?
                )?
                $topology.build_updated_instance(&req)
            }
        };
    }

    macro_rules! set_faildoms {
        (
            $topology:expr,
            $instance_id:expr,
            $failure_domain:expr $(,)?
        ) => {
            $topology.build_updated_instance(
                &rpc::update_instance::Request::new($instance_id.into(), "".into())
                    .with_failure_domain($failure_domain),
            )
        };
    }

    macro_rules! faildoms {
        ($(,)?) => { FailureDomain::default() };
        ($($k:tt : $v:tt),+ $(,)?) => {
            FailureDomain::from([$((stringify!($k), stringify!($v))),+])
        }
    }

    fn new_topology(storage: &Clusterwide, instances: Vec<Instance>, replication_factor: usize) -> Topology {
        for instance in instances {
            storage.instances.put(&instance).unwrap();
        }
        storage.properties.put(crate::storage::PropertyName::ReplicationFactor, &replication_factor).unwrap();
        Topology::from(storage.clone())
    }

    #[::tarantool::test]
    fn test_simple() {
        let storage = Clusterwide::new().unwrap();
        let mut topology = new_topology(&storage, vec![], 1);

        let instance = build_instance!(topology, None, None).unwrap();
        assert_eq!(
            instance,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        topology.update(instance, None);

        let instance = build_instance!(topology, None, None).unwrap();
        assert_eq!(
            instance,
            instance!(2, "i2", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        topology.update(instance, None);

        let instance = build_instance!(topology, None, Some("R3")).unwrap();
        assert_eq!(
            instance,
            instance!(3, "i3", "R3", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        topology.update(instance, None);

        let instance = build_instance!(topology, Some("I4"), None).unwrap();
        assert_eq!(
            instance,
            instance!(4, "I4", "r3", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        topology.update(instance, None);
    }

    #[::tarantool::test]
    fn test_override() {
        let storage = Clusterwide::new().unwrap();
        let topology = new_topology(&storage, instances![
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
            build_instance!(topology, Some("i1"), None)
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
            build_instance!(topology, Some("i2"), None).unwrap(),
            (instance!(3, "i2", "r1", CurrentGrade::offline(0), TargetGrade::offline(0))),
            // Attention: generated replicaset_id differs from the
            // original one, as well as raft_id.
            // That's a desired behavior.
        );
        assert_eq!(topology.get_replication_ids(&ReplicasetId::from("r1")), HashSet::from([1]));

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
        let topology = new_topology(&storage, instances![
            (1, "i1", "r1", CurrentGrade::online(1), TargetGrade::online(1)),
            (2, "i3", "r3", CurrentGrade::online(1), TargetGrade::online(1)),
            // Attention: i3 has raft_id=2
        ], 1);

        assert_eq!(
            build_instance!(topology, None, Some("r2")).unwrap(),
            instance!(3, "i3-2", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
    }

    #[::tarantool::test]
    fn test_replication_factor() {
        let storage = Clusterwide::new().unwrap();
        let mut topology = new_topology(&storage, instances![
            (9, "i9", "r9", CurrentGrade::online(1), TargetGrade::online(1)),
            (10, "i10", "r9", CurrentGrade::online(1), TargetGrade::online(1)),
        ],
        2);

        let instance = build_instance!(topology, Some("i1"), None).unwrap();
        assert_eq!(
            instance,
            instance!(11, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        topology.update(instance, None);
        assert_eq!(topology.get_replication_ids(&ReplicasetId::from("r1")), HashSet::from([11]));

        let instance = build_instance!(topology, Some("i2"), None).unwrap();
        assert_eq!(
            instance,
            instance!(12, "i2", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        topology.update(instance, None);
        assert_eq!(topology.get_replication_ids(&ReplicasetId::from("r1")), HashSet::from([11, 12]));

        let instance = build_instance!(topology, Some("i3"), None).unwrap();
        assert_eq!(
            instance,
            instance!(13, "i3", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        topology.update(instance, None);
        assert_eq!(topology.get_replication_ids(&ReplicasetId::from("r2")), HashSet::from([13]));

        let instance = build_instance!(topology, Some("i4"), None).unwrap();
        assert_eq!(
            instance,
            instance!(14, "i4", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );
        storage.instances.put(&instance).unwrap();
        topology.update(instance, None);
        assert_eq!(topology.get_replication_ids(&ReplicasetId::from("r2")), HashSet::from([13, 14]));
    }

    #[::tarantool::test]
    fn test_update_grade() {
        let storage = Clusterwide::new().unwrap();
        let instance_v0 = instance!(1, "i1", "r1", CurrentGrade::online(1), TargetGrade::online(1));
        let mut topology = new_topology(&storage, vec![instance_v0.clone()], 1);

        // Current grade incarnation is allowed to go down,
        // governor has the authority over it
        let instance_v1 = set_grade!(topology, "i1", CurrentGrade::offline(0)).unwrap();
        storage.instances.put(&instance_v1).unwrap();
        topology.update(instance_v1.clone(), Some(instance_v0));
        assert_eq!(
            instance_v1,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(1)),
        );

        // idempotency
        let instance_v2 = set_grade!(topology, "i1", CurrentGrade::offline(0)).unwrap();
        storage.instances.put(&instance_v2).unwrap();
        topology.update(instance_v2.clone(), Some(instance_v1));
        assert_eq!(
            instance_v2,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(1)),
        );

        // TargetGradeVariant::Offline takes incarnation from current grade
        let instance_v3 = set_grade!(topology, "i1", TargetGradeVariant::Offline).unwrap();
        storage.instances.put(&instance_v3).unwrap();
        topology.update(instance_v3.clone(), Some(instance_v2));
        assert_eq!(
            instance_v3,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );

        // TargetGradeVariant::Online increases incarnation
        let instance_v4 = set_grade!(topology, "i1", TargetGradeVariant::Online).unwrap();
        storage.instances.put(&instance_v4).unwrap();
        topology.update(instance_v4.clone(), Some(instance_v3));
        assert_eq!(
            instance_v4,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(1)),
        );

        // No idempotency, incarnation goes up
        let instance_v5 = set_grade!(topology, "i1", TargetGradeVariant::Online).unwrap();
        storage.instances.put(&instance_v5).unwrap();
        topology.update(instance_v5.clone(), Some(instance_v4));
        assert_eq!(
            instance_v5,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(2)),
        );

        // TargetGrade::Expelled takes incarnation from current grade
        let instance_v6 = set_grade!(topology, "i1", TargetGradeVariant::Expelled).unwrap();
        storage.instances.put(&instance_v6).unwrap();
        topology.update(instance_v6.clone(), Some(instance_v5));
        assert_eq!(
            instance_v6,
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::expelled(0)),
        );

        // Instance get's expelled
        let instance_v7 = set_grade!(topology, "i1", CurrentGrade::expelled(69)).unwrap();
        storage.instances.put(&instance_v7).unwrap();
        topology.update(instance_v7.clone(), Some(instance_v6));
        assert_eq!(
            instance_v7,
            instance!(1, "i1", "r1", CurrentGrade::expelled(69), TargetGrade::expelled(0)),
        );

        // Updating expelled instances isn't allowed
        assert_eq!(
            set_grade!(topology, "i1", TargetGradeVariant::Online).unwrap_err(),
            "cannot update expelled instance \"i1\"",
        );
    }

    #[::tarantool::test]
    fn failure_domain() {
        let storage = Clusterwide::new().unwrap();
        let mut t = new_topology(&storage, vec![], 3);
        
        let instance = 
            build_instance!(t, None, None, faildoms! {planet: Earth})
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();
        t.update(instance, None);

        let instance = 
            build_instance!(t, None, None, faildoms! {planet: Earth})
                .unwrap();
        assert_eq!(instance.replicaset_id, "r2");
        storage.instances.put(&instance).unwrap();
        t.update(instance, None);

        let instance = 
            build_instance!(t, None, None, faildoms! {planet: Mars})
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();
        t.update(instance, None);

        let instance = 
            build_instance!(t, None, None, faildoms! {planet: Earth, os: BSD})
                .unwrap();
        assert_eq!(instance.replicaset_id, "r3");
        storage.instances.put(&instance).unwrap();
        t.update(instance, None);

        let instance = 
            build_instance!(t, None, None, faildoms! {planet: Mars, os: BSD})
                .unwrap();
        assert_eq!(instance.replicaset_id, "r2");
        storage.instances.put(&instance).unwrap();
        t.update(instance, None);

        assert_eq!(
            build_instance!(t, None, None, faildoms! {os: Arch})
                .unwrap_err(),
            "missing failure domain names: PLANET",
        );

        let instance = 
            build_instance!(t, None, None, faildoms! {planet: Venus, os: Arch})
                .unwrap();
        assert_eq!(instance.replicaset_id, "r1");
        storage.instances.put(&instance).unwrap();
        t.update(instance, None);

        let instance = 
            build_instance!(t, None, None, faildoms! {planet: Venus, os: Mac})
                .unwrap();
        assert_eq!(instance.replicaset_id, "r2");
        storage.instances.put(&instance).unwrap();
        t.update(instance, None);

        let instance = 
            build_instance!(t, None, None, faildoms! {planet: Mars, os: Mac})
                .unwrap();
        assert_eq!(instance.replicaset_id, "r3");
        storage.instances.put(&instance).unwrap();
        t.update(instance, None);

        assert_eq!(
            build_instance!(t, None, None, faildoms! {})
                .unwrap_err(),
            "missing failure domain names: OS, PLANET",
        );
    }

    #[::tarantool::test]
    fn reconfigure_failure_domain() {
        let storage = Clusterwide::new().unwrap();
        let mut t = new_topology(&storage, instances![], 3);

        // first instance
        let instance1_v1 = build_instance!(t, Some("i1"), None, faildoms! {planet: Earth}).unwrap();
        storage.instances.put(&instance1_v1).unwrap();
        t.update(instance1_v1.clone(), None);
        assert_eq!(instance1_v1.failure_domain, faildoms! {planet: Earth});
        assert_eq!(instance1_v1.replicaset_id, "r1");

        // reconfigure single instance, fail
        assert_eq!(
            set_faildoms!(t, "i1", faildoms! {owner: Ivan})
                .unwrap_err(),
            "missing failure domain names: PLANET",
        );

        // reconfigure single instance, success
        let instance1_v2 = set_faildoms!(t, "i1", faildoms! {planet: Mars, owner: Ivan}).unwrap();
        storage.instances.put(&instance1_v2).unwrap();
        t.update(instance1_v2.clone(), Some(instance1_v1));
        assert_eq!(instance1_v2.failure_domain, faildoms! {planet: Mars, owner: Ivan});
        assert_eq!(instance1_v2.replicaset_id, "r1"); // same replicaset

        // second instance won't be joined without the newly added required
        // failure domain subdivision of "OWNER"
        assert_eq!(
            build_instance!(t, Some("i2"), None, faildoms! {planet: Mars})
                .unwrap_err(),
            "missing failure domain names: OWNER",
        );

        // second instance
        #[rustfmt::skip]
        let instance2_v1 = build_instance!(t, Some("i2"), None, faildoms! {planet: Mars, owner: Mike})
            .unwrap();
        storage.instances.put(&instance2_v1).unwrap();
        t.update(instance2_v1.clone(), None);
        assert_eq!(instance2_v1.failure_domain, faildoms! {planet: Mars, owner: Mike});
        // doesn't fit into r1
        assert_eq!(instance2_v1.replicaset_id, "r2");

        // reconfigure second instance, success
        let instance2_v2 = set_faildoms!(t, "i2", faildoms! {planet: Earth, owner: Mike}).unwrap();
        storage.instances.put(&instance2_v2).unwrap();
        t.update(instance2_v2.clone(), Some(instance2_v1));
        assert_eq!(instance2_v2.failure_domain, faildoms! {planet: Earth, owner: Mike});
        // replicaset doesn't change automatically
        assert_eq!(instance2_v2.replicaset_id, "r2");

        // add instance with new subdivision
        #[rustfmt::skip]
        let instance3_v1 = build_instance!(t, Some("i3"), None, faildoms! {planet: B, owner: V, dimension: C137})
            .unwrap();
        storage.instances.put(&instance3_v1).unwrap();
        t.update(instance3_v1.clone(), None);
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
            build_instance!(t, Some("i4"), None, faildoms! {planet: Theia, owner: Me})
                .unwrap_err(),
            "missing failure domain names: DIMENSION",
        );
    }
}
