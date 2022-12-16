use std::collections::{BTreeMap, HashMap, HashSet};

use crate::replicaset::ReplicasetId;
use crate::rpc::update_instance;
use crate::traft::instance_uuid;
use crate::traft::replicaset_uuid;
use crate::traft::Address;
use crate::traft::FailureDomain;
use crate::traft::Instance;
use crate::traft::{CurrentGrade, CurrentGradeVariant, Grade, TargetGrade, TargetGradeVariant};
use crate::traft::{InstanceId, RaftId};
use crate::util::Uppercase;

#[derive(Debug)]
pub struct Topology {
    replication_factor: u8,
    max_raft_id: RaftId,

    failure_domain_names: HashSet<Uppercase>,
    instance_map: HashMap<InstanceId, (Instance, Address)>,
    replicaset_map: BTreeMap<ReplicasetId, HashSet<InstanceId>>,
}

impl Topology {
    #[inline(always)]
    pub fn new(instances: impl IntoIterator<Item = (Instance, Address)>) -> Self {
        let mut ret = Self {
            replication_factor: 1,
            max_raft_id: 0,
            failure_domain_names: Default::default(),
            instance_map: Default::default(),
            replicaset_map: Default::default(),
        };

        for (instance, address) in instances {
            ret.put_instance(instance, address);
        }

        ret
    }

    pub fn with_replication_factor(mut self, replication_factor: u8) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    fn put_instance(&mut self, instance: Instance, address: Address) {
        self.max_raft_id = std::cmp::max(self.max_raft_id, instance.raft_id);

        let instance_id = instance.instance_id.clone();
        let replicaset_id = instance.replicaset_id.clone();

        if let Some((old_instance, ..)) = self.instance_map.remove(&instance_id) {
            self.replicaset_map
                .get_mut(&old_instance.replicaset_id)
                .map(|r| r.remove(&old_instance.instance_id));
        }

        self.failure_domain_names
            .extend(instance.failure_domain.names().cloned());
        self.instance_map
            .insert(instance_id.clone(), (instance, address));
        self.replicaset_map
            .entry(replicaset_id)
            .or_default()
            .insert(instance_id);
    }

    fn choose_instance_id(&self, raft_id: RaftId) -> InstanceId {
        let mut suffix: Option<u64> = None;
        loop {
            let ret = match suffix {
                None => format!("i{raft_id}"),
                Some(x) => format!("i{raft_id}-{x}"),
            }
            .into();

            if !self.instance_map.contains_key(&ret) {
                return ret;
            }

            suffix = Some(suffix.map_or(2, |x| x + 1));
        }
    }

    fn choose_replicaset_id(&self, failure_domain: &FailureDomain) -> ReplicasetId {
        'next_replicaset: for (replicaset_id, instances) in self.replicaset_map.iter() {
            if instances.len() < self.replication_factor as usize {
                for instance_id in instances {
                    let (instance, ..) = self.instance_map.get(instance_id).unwrap();
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
            if self.replicaset_map.get(&replicaset_id).is_none() {
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

    pub fn join(
        &mut self,
        instance_id: Option<InstanceId>,
        replicaset_id: Option<ReplicasetId>,
        advertise: Address,
        failure_domain: FailureDomain,
    ) -> Result<(Instance, Address, HashSet<Address>), String> {
        if let Some(id) = &instance_id {
            let existing_instance = self.instance_map.get(id);
            if matches!(existing_instance, Some((instance, ..)) if instance.is_online()) {
                let e = format!("{} is already joined", id);
                return Err(e);
            }
        }

        self.check_required_failure_domain(&failure_domain)?;

        // Anyway, `join` always produces a new raft_id.
        let raft_id = self.max_raft_id + 1;
        let instance_id = instance_id.unwrap_or_else(|| self.choose_instance_id(raft_id));
        let instance_uuid = instance_uuid(&instance_id);
        let replicaset_id =
            replicaset_id.unwrap_or_else(|| self.choose_replicaset_id(&failure_domain));
        let replicaset_uuid = replicaset_uuid(&replicaset_id);

        let instance = Instance {
            instance_id,
            instance_uuid,
            raft_id,
            replicaset_id: replicaset_id.clone(),
            replicaset_uuid,
            current_grade: CurrentGrade::offline(0),
            target_grade: TargetGrade::offline(0),
            failure_domain,
        };

        self.put_instance(instance.clone(), advertise.clone());

        let replication_ids = self.replicaset_map.get(&replicaset_id).unwrap();
        let replication_addresses = replication_ids
            .iter()
            .map(|id| self.instance_map.get(id).unwrap().1.clone())
            .collect();

        Ok((instance, advertise, replication_addresses))
    }

    pub fn update_instance(&mut self, req: update_instance::Request) -> Result<Instance, String> {
        let this = self as *const Self;

        let (instance, ..) = self
            .instance_map
            .get_mut(&req.instance_id)
            .ok_or_else(|| format!("unknown instance {}", req.instance_id))?;

        if instance.current_grade == CurrentGradeVariant::Expelled
            && !matches!(
                req,
                update_instance::Request {
                    target_grade: None,
                    current_grade: Some(current_grade),
                    failure_domain: None,
                    ..
                } if current_grade == CurrentGradeVariant::Expelled
            )
        {
            return Err(format!(
                "cannot update expelled instance \"{}\"",
                instance.instance_id
            ));
        }

        if let Some(fd) = req.failure_domain {
            // SAFETY: this is safe, because rust doesn't complain if you inline
            // the function
            unsafe { &*this }.check_required_failure_domain(&fd)?;
            self.failure_domain_names.extend(fd.names().cloned());
            instance.failure_domain = fd;
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

        Ok(instance.clone())
    }
}

/// Create first instance in the cluster
pub fn initial_instance(
    instance_id: Option<InstanceId>,
    replicaset_id: Option<ReplicasetId>,
    advertise: Address,
    failure_domain: FailureDomain,
) -> Result<(Instance, Address, HashSet<Address>), String> {
    let mut topology = Topology::new(vec![]);
    topology.join(instance_id, replicaset_id, advertise, failure_domain)
}

#[rustfmt::skip]
#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::Topology;

    use crate::traft::instance_uuid;
    use crate::traft::replicaset_uuid;
    use crate::traft::FailureDomain;
    use crate::traft::Instance;
    use crate::traft::rpc::update_instance;
    use crate::traft::{CurrentGrade, Grade, TargetGrade, TargetGradeVariant};
    use pretty_assertions::assert_eq;

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
        fn modify(self, req: update_instance::Request) -> update_instance::Request;
    }

    impl ModifyUpdateInstanceRequest for CurrentGrade {
        fn modify(self, req: update_instance::Request) -> update_instance::Request {
            req.with_current_grade(self)
        }
    }

    impl ModifyUpdateInstanceRequest for TargetGradeVariant {
        fn modify(self, req: update_instance::Request) -> update_instance::Request {
            req.with_target_grade(self)
        }
    }

    macro_rules! instances {
        [ $( ( $($instance:tt)+ ) ),* $(,)? ] => {
            vec![$( (instance!($($instance)+), "who-cares.biz".into()) ),*]
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
                .. Instance::default()
            }
        };
    }

    macro_rules! addresses {
        [$($address:literal),*] => [HashSet::from([$($address.to_string()),*])]
    }

    macro_rules! join {
        (
            $topology:expr,
            $instance_id:expr,
            $replicaset_id:expr,
            $advertise_address:literal
            $(, $failure_domain:expr )?
            $(,)?
        ) => {
            $topology.join(
                $instance_id.map(<&str>::into),
                $replicaset_id.map(<&str>::into),
                $advertise_address.into(),
                {
                    let _f = FailureDomain::default();
                    $(let _f = $failure_domain; )?
                    _f
                },
            )
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
            $topology.update_instance(
                {
                    let req = update_instance::Request::new($instance_id.into(), "".into());
                    $(
                        let req = $current_grade.modify(req);
                        $( let req = $target_grade.modify(req); )?
                    )?
                    req
                }
            )
        };
    }

    macro_rules! set_faildoms {
        (
            $topology:expr,
            $instance_id:expr,
            $failure_domain:expr $(,)?
        ) => {
            $topology.update_instance(
                update_instance::Request::new($instance_id.into(), "".into())
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

    #[test]
    fn test_simple() {
        let mut topology = Topology::new(vec![]).with_replication_factor(1);

        assert_eq!(
            join!(topology, None, None, "addr:1").unwrap(),
            (instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:1".into(), addresses!["addr:1"]),
        );

        assert_eq!(
            join!(topology, None, None, "addr:1").unwrap(),
            (instance!(2, "i2", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:1".into(), addresses!["addr:1"]),
        );

        assert_eq!(
            join!(topology, None, Some("R3"), "addr:1").unwrap(),
            (instance!(3, "i3", "R3", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:1".into(), addresses!["addr:1"]),
        );

        assert_eq!(
            join!(topology, Some("I4"), None, "addr:1").unwrap(),
            (instance!(4, "I4", "r3", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:1".into(), addresses!["addr:1"]),
        );

        let mut topology = Topology::new(
            instances![(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0))]
        ).with_replication_factor(1);

        assert_eq!(
            join!(topology, None, None, "addr:1").unwrap(),
            (instance!(2, "i2", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:1".into(), addresses!["addr:1"]),
        );
    }

    #[test]
    fn test_override() {
        let mut topology = Topology::new(instances![
            (1, "i1", "r1", CurrentGrade::online(1), TargetGrade::online(1)),
            (2, "i2", "r2-original", CurrentGrade::offline(0), TargetGrade::offline(0)),
        ])
        .with_replication_factor(2);

        // join::Request with a given instance_id online.
        // - It must be an impostor, return an error.
        // - Even if it's a fair rebootstrap, it will be marked as
        //   unreachable soon (when we implement failover) an the error
        //   will be gone.
        assert_eq!(
            join!(topology, Some("i1"), None, "active:2")
                .unwrap_err()
                .to_string(),
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
            join!(topology, Some("i2"), None, "inactive:2").unwrap(),
            (instance!(3, "i2", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)), "inactive:2".into(), addresses!["who-cares.biz", "inactive:2"]),
            // Attention: generated replicaset_id differs from the
            // original one, as well as raft_id.
            // That's a desired behavior.
        );

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

    #[test]
    fn test_instance_id_collision() {
        let mut topology = Topology::new(instances![
            (1, "i1", "r1", CurrentGrade::online(1), TargetGrade::online(1)),
            (2, "i3", "r3", CurrentGrade::online(1), TargetGrade::online(1)),
            // Attention: i3 has raft_id=2
        ]);

        assert_eq!(
            join!(topology, None, Some("r2"), "addr:2").unwrap(),
            (instance!(3, "i3-2", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:2".into(), addresses!["addr:2"]),
        );
    }

    #[test]
    fn test_replication_factor() {
        let mut topology = Topology::new(instances![
            (9, "i9", "r9", CurrentGrade::online(1), TargetGrade::online(1)),
            (10, "i10", "r9", CurrentGrade::online(1), TargetGrade::online(1)),
        ])
        .with_replication_factor(2);

        assert_eq!(
            join!(topology, Some("i1"), None, "addr:1").unwrap(),
            (instance!(11, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:1".into(), addresses!["addr:1"]),
        );
        assert_eq!(
            join!(topology, Some("i2"), None, "addr:2").unwrap(),
            (instance!(12, "i2", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:2".into(), addresses!["addr:1", "addr:2"]),
        );
        assert_eq!(
            join!(topology, Some("i3"), None, "addr:3").unwrap(),
            (instance!(13, "i3", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:3".into(), addresses!["addr:3"]),
        );
        assert_eq!(
            join!(topology, Some("i4"), None, "addr:4").unwrap(),
            (instance!(14, "i4", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:4".into(), addresses!["addr:3", "addr:4"]),
        );
    }

    #[test]
    fn test_update_grade() {
        let mut topology = Topology::new(instances![
            (1, "i1", "r1", CurrentGrade::online(1), TargetGrade::online(1)),
        ])
        .with_replication_factor(1);

        // Current grade incarnation is allowed to go down,
        // governor has the authority over it
        assert_eq!(
            set_grade!(topology, "i1", CurrentGrade::offline(0)).unwrap(),
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(1)),
        );

        // idempotency
        assert_eq!(
            set_grade!(topology, "i1", CurrentGrade::offline(0)).unwrap(),
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(1)),
        );

        // TargetGradeVariant::Offline takes incarnation from current grade
        assert_eq!(
            set_grade!(topology, "i1", TargetGradeVariant::Offline).unwrap(),
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );

        // TargetGradeVariant::Online increases incarnation
        assert_eq!(
            set_grade!(topology, "i1", TargetGradeVariant::Online).unwrap(),
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(1)),
        );

        // No idempotency, incarnation goes up
        assert_eq!(
            set_grade!(topology, "i1", TargetGradeVariant::Online).unwrap(),
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(2)),
        );

        // TargetGrade::Expelled takes incarnation from current grade
        assert_eq!(
            set_grade!(topology, "i1", TargetGradeVariant::Expelled).unwrap(),
            instance!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::expelled(0)),
        );

        // Instance get's expelled
        assert_eq!(
            set_grade!(topology, "i1", CurrentGrade::expelled(69)).unwrap(),
            instance!(1, "i1", "r1", CurrentGrade::expelled(69), TargetGrade::expelled(0)),
        );

        // Updating expelled instances isn't allowed
        assert_eq!(
            set_grade!(topology, "i1", TargetGradeVariant::Online).unwrap_err().to_string(),
            "cannot update expelled instance \"i1\"",
        );
    }

    #[test]
    fn failure_domain() {
        let mut t = Topology::new(instances![]).with_replication_factor(3);

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Earth})
                .unwrap()
                .0
                .replicaset_id,
            "r1",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Earth})
                .unwrap()
                .0
                .replicaset_id,
            "r2",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Mars})
                .unwrap()
                .0
                .replicaset_id,
            "r1",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Earth, os: BSD})
                .unwrap()
                .0
                .replicaset_id,
            "r3",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Mars, os: BSD})
                .unwrap()
                .0
                .replicaset_id,
            "r2",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {os: Arch})
                .unwrap_err()
                .to_string(),
            "missing failure domain names: PLANET",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Venus, os: Arch})
                .unwrap()
                .0
                .replicaset_id,
            "r1",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Venus, os: Mac})
                .unwrap()
                .0
                .replicaset_id,
            "r2",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Mars, os: Mac})
                .unwrap()
                .0
                .replicaset_id,
            "r3",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {})
                .unwrap_err()
                .to_string(),
            "missing failure domain names: OS, PLANET",
        );
    }

    #[test]
    fn reconfigure_failure_domain() {
        let mut t = Topology::new(instances![]).with_replication_factor(3);

        // first instance
        let (instance, ..) = join!(t, Some("i1"), None, "-", faildoms! {planet: Earth}).unwrap();
        assert_eq!(instance.failure_domain, faildoms! {planet: Earth});
        assert_eq!(instance.replicaset_id, "r1");

        // reconfigure single instance, fail
        assert_eq!(
            set_faildoms!(t, "i1", faildoms! {owner: Ivan})
                .unwrap_err()
                .to_string(),
            "missing failure domain names: PLANET",
        );

        // reconfigure single instance, success
        let instance = set_faildoms!(t, "i1", faildoms! {planet: Mars, owner: Ivan}).unwrap();
        assert_eq!(instance.failure_domain, faildoms! {planet: Mars, owner: Ivan});
        assert_eq!(instance.replicaset_id, "r1"); // same replicaset

        // second instance won't be joined without the newly added required
        // failure domain subdivision of "OWNER"
        assert_eq!(
            join!(t, Some("i2"), None, "-", faildoms! {planet: Mars})
                .unwrap_err()
                .to_string(),
            "missing failure domain names: OWNER",
        );

        // second instance
        #[rustfmt::skip]
        let (instance, ..) = join!(t, Some("i2"), None, "-", faildoms! {planet: Mars, owner: Mike})
            .unwrap();
        assert_eq!(instance.failure_domain, faildoms! {planet: Mars, owner: Mike});
        // doesn't fit into r1
        assert_eq!(instance.replicaset_id, "r2");

        // reconfigure second instance, success
        let instance = set_faildoms!(t, "i2", faildoms! {planet: Earth, owner: Mike}).unwrap();
        assert_eq!(instance.failure_domain, faildoms! {planet: Earth, owner: Mike});
        // replicaset doesn't change automatically
        assert_eq!(instance.replicaset_id, "r2");

        // add instance with new subdivision
        #[rustfmt::skip]
        let (instance, ..) = join!(t, Some("i3"), None, "-", faildoms! {planet: B, owner: V, dimension: C137})
            .unwrap();
        assert_eq!(
            instance.failure_domain,
            faildoms! {planet: B, owner: V, dimension: C137}
        );
        assert_eq!(instance.replicaset_id, "r1");

        // even though the only instance with failure domain subdivision of
        // `DIMENSION` is inactive, we can't add an instance without that
        // subdivision
        #[rustfmt::skip]
        assert_eq!(
            join!(t, Some("i4"), None, "-", faildoms! {planet: Theia, owner: Me})
                .unwrap_err()
                .to_string(),
            "missing failure domain names: DIMENSION",
        );
    }
}
