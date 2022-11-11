use std::collections::{BTreeMap, HashMap, HashSet};

use crate::traft::instance_uuid;
use crate::traft::replicaset_uuid;
use crate::traft::Address;
use crate::traft::FailureDomain;
use crate::traft::Peer;
use crate::traft::UpdatePeerRequest;
use crate::traft::{CurrentGrade, CurrentGradeVariant, Grade, TargetGrade, TargetGradeVariant};
use crate::traft::{InstanceId, RaftId, ReplicasetId};
use crate::util::Uppercase;

use raft::INVALID_INDEX;

pub struct Topology {
    replication_factor: u8,
    max_raft_id: RaftId,

    failure_domain_names: HashSet<Uppercase>,
    instance_map: HashMap<InstanceId, (Peer, Address)>,
    replicaset_map: BTreeMap<ReplicasetId, HashSet<InstanceId>>,
}

impl Topology {
    #[inline(always)]
    pub fn from_peers(peers: impl IntoIterator<Item = (Peer, Address)>) -> Self {
        let mut ret = Self {
            replication_factor: 1,
            max_raft_id: 0,
            failure_domain_names: Default::default(),
            instance_map: Default::default(),
            replicaset_map: Default::default(),
        };

        for (peer, address) in peers {
            ret.put_peer(peer, address);
        }

        ret
    }

    pub fn with_replication_factor(mut self, replication_factor: u8) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    fn put_peer(&mut self, peer: Peer, address: Address) {
        self.max_raft_id = std::cmp::max(self.max_raft_id, peer.raft_id);

        let instance_id = peer.instance_id.clone();
        let replicaset_id = peer.replicaset_id.clone();

        if let Some((old_peer, ..)) = self.instance_map.remove(&instance_id) {
            self.replicaset_map
                .get_mut(&old_peer.replicaset_id)
                .map(|r| r.remove(&old_peer.instance_id));
        }

        self.failure_domain_names
            .extend(peer.failure_domain.names().cloned());
        self.instance_map
            .insert(instance_id.clone(), (peer, address));
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
        'next_replicaset: for (replicaset_id, peers) in self.replicaset_map.iter() {
            if peers.len() < self.replication_factor as usize {
                for peer_id in peers {
                    let (peer, ..) = self.instance_map.get(peer_id).unwrap();
                    if peer.failure_domain.intersects(failure_domain) {
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
    ) -> Result<(Peer, Address), String> {
        if let Some(id) = &instance_id {
            let existing_peer = self.instance_map.get(id);
            if matches!(existing_peer, Some((peer, ..)) if peer.is_online()) {
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

        let peer = Peer {
            instance_id,
            instance_uuid,
            raft_id,
            replicaset_id,
            replicaset_uuid,
            commit_index: INVALID_INDEX,
            current_grade: CurrentGrade::offline(0),
            target_grade: TargetGrade::offline(0),
            failure_domain,
        };

        self.put_peer(peer.clone(), advertise.clone());
        Ok((peer, advertise))
    }

    pub fn update_peer(&mut self, req: UpdatePeerRequest) -> Result<Peer, String> {
        let this = self as *const Self;

        let (peer, ..) = self
            .instance_map
            .get_mut(&req.instance_id)
            .ok_or_else(|| format!("unknown instance {}", req.instance_id))?;

        if peer.current_grade == CurrentGradeVariant::Expelled
            && !matches!(
                req,
                UpdatePeerRequest {
                    target_grade: None,
                    current_grade: Some(current_grade),
                    failure_domain: None,
                    ..
                } if current_grade == CurrentGradeVariant::Expelled
            )
        {
            return Err(format!(
                "cannot update expelled peer \"{}\"",
                peer.instance_id
            ));
        }

        if let Some(fd) = req.failure_domain {
            // SAFETY: this is safe, because rust doesn't complain if you inline
            // the function
            unsafe { &*this }.check_required_failure_domain(&fd)?;
            self.failure_domain_names.extend(fd.names().cloned());
            peer.failure_domain = fd;
        }

        if let Some(value) = req.current_grade {
            peer.current_grade = value;
        }

        if let Some(variant) = req.target_grade {
            let incarnation = match variant {
                TargetGradeVariant::Online => peer.target_grade.incarnation + 1,
                _ => peer.current_grade.incarnation,
            };
            peer.target_grade = Grade {
                variant,
                incarnation,
            };
        }

        Ok(peer.clone())
    }
}

// Create first peer in the cluster
pub fn initial_peer(
    instance_id: Option<InstanceId>,
    replicaset_id: Option<ReplicasetId>,
    advertise: Address,
    failure_domain: FailureDomain,
) -> (Peer, Address) {
    let mut topology = Topology::from_peers(vec![]);
    let (mut peer, advertise) = topology
        .join(instance_id, replicaset_id, advertise, failure_domain)
        .unwrap();
    peer.commit_index = 1;
    (peer, advertise)
}

#[rustfmt::skip]
#[cfg(test)]
mod tests {
    use super::Topology;

    use crate::traft::instance_uuid;
    use crate::traft::replicaset_uuid;
    use crate::traft::FailureDomain;
    use crate::traft::Peer;
    use crate::traft::UpdatePeerRequest;
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

    trait ModifyUpdatePeerRequest {
        fn modify(self, req: UpdatePeerRequest) -> UpdatePeerRequest;
    }

    impl ModifyUpdatePeerRequest for CurrentGrade {
        fn modify(self, req: UpdatePeerRequest) -> UpdatePeerRequest {
            req.with_current_grade(self)
        }
    }

    impl ModifyUpdatePeerRequest for TargetGradeVariant {
        fn modify(self, req: UpdatePeerRequest) -> UpdatePeerRequest {
            req.with_target_grade(self)
        }
    }

    macro_rules! peers {
        [ $( ( $($peer:tt)+ ) ),* $(,)? ] => {
            vec![$( (peer!($($peer)+), "who-cares.biz".into()) ),*]
        };
    }

    macro_rules! peer {
        (
            $raft_id:expr,
            $instance_id:literal,
            $replicaset_id:literal,
            $current_grade:expr,
            $target_grade:expr
            $(, $failure_domain:expr)?
            $(,)?
        ) => {
            Peer {
                raft_id: $raft_id,
                instance_id: $instance_id.into(),
                replicaset_id: $replicaset_id.into(),
                instance_uuid: instance_uuid($instance_id),
                replicaset_uuid: replicaset_uuid($replicaset_id),
                commit_index: raft::INVALID_INDEX,

                current_grade: $current_grade.into_grade(),
                target_grade: $target_grade.into_grade(),
                failure_domain: {
                    let _f = FailureDomain::default();
                    $( let _f = $failure_domain; )?
                    _f
                },
                .. Peer::default()
            }
        };
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
            $topology.update_peer(
                {
                    let req = UpdatePeerRequest::new($instance_id.into(), "".into());
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
            $topology.update_peer(
                UpdatePeerRequest::new($instance_id.into(), "".into())
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
        let mut topology = Topology::from_peers(vec![]).with_replication_factor(1);

        assert_eq!(
            join!(topology, None, None, "addr:1").unwrap(),
            (peer!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:1".into()),
        );

        assert_eq!(
            join!(topology, None, None, "addr:1").unwrap(),
            (peer!(2, "i2", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:1".into()),
        );

        assert_eq!(
            join!(topology, None, Some("R3"), "addr:1").unwrap(),
            (peer!(3, "i3", "R3", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:1".into()),
        );

        assert_eq!(
            join!(topology, Some("I4"), None, "addr:1").unwrap(),
            (peer!(4, "I4", "r3", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:1".into()),
        );

        let mut topology = Topology::from_peers(
            peers![(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0))]
        ).with_replication_factor(1);

        assert_eq!(
            join!(topology, None, None, "addr:1").unwrap(),
            (peer!(2, "i2", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:1".into()),
        );
    }

    #[test]
    fn test_override() {
        let mut topology = Topology::from_peers(peers![
            (1, "i1", "r1", CurrentGrade::online(1), TargetGrade::online(1)),
            (2, "i2", "r2-original", CurrentGrade::offline(0), TargetGrade::offline(0)),
        ])
        .with_replication_factor(2);

        // JoinRequest with a given instance_id online.
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

        // JoinRequest with a given instance_id offline (or unreachable).
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
            (peer!(3, "i2", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)), "inactive:2".into()),
            // Attention: generated replicaset_id differs from the
            // original one, as well as raft_id.
            // That's a desired behavior.
        );

        // TODO
        //
        // JoinRequest with a given instance_id bootstrtapping.
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
        let mut topology = Topology::from_peers(peers![
            (1, "i1", "r1", CurrentGrade::online(1), TargetGrade::online(1)),
            (2, "i3", "r3", CurrentGrade::online(1), TargetGrade::online(1)),
            // Attention: i3 has raft_id=2
        ]);

        assert_eq!(
            join!(topology, None, Some("r2"), "addr:2").unwrap(),
            (peer!(3, "i3-2", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:2".into()),
        );
    }

    #[test]
    fn test_replication_factor() {
        let mut topology = Topology::from_peers(peers![
            (9, "i9", "r9", CurrentGrade::online(1), TargetGrade::online(1)),
            (10, "i10", "r9", CurrentGrade::online(1), TargetGrade::online(1)),
        ])
        .with_replication_factor(2);

        assert_eq!(
            join!(topology, Some("i1"), None, "addr:1").unwrap(),
            (peer!(11, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:1".into()),
        );
        assert_eq!(
            join!(topology, Some("i2"), None, "addr:2").unwrap(),
            (peer!(12, "i2", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:2".into()),
        );
        assert_eq!(
            join!(topology, Some("i3"), None, "addr:3").unwrap(),
            (peer!(13, "i3", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:3".into()),
        );
        assert_eq!(
            join!(topology, Some("i4"), None, "addr:4").unwrap(),
            (peer!(14, "i4", "r2", CurrentGrade::offline(0), TargetGrade::offline(0)), "addr:4".into()),
        );
    }

    #[test]
    fn test_update_grade() {
        let mut topology = Topology::from_peers(peers![
            (1, "i1", "r1", CurrentGrade::online(1), TargetGrade::online(1)),
        ])
        .with_replication_factor(1);

        // Current grade incarnation is allowed to go down,
        // governor has the authority over it
        assert_eq!(
            set_grade!(topology, "i1", CurrentGrade::offline(0)).unwrap(),
            peer!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(1)),
        );

        // idempotency
        assert_eq!(
            set_grade!(topology, "i1", CurrentGrade::offline(0)).unwrap(),
            peer!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(1)),
        );

        // TargetGradeVariant::Offline takes incarnation from current grade
        assert_eq!(
            set_grade!(topology, "i1", TargetGradeVariant::Offline).unwrap(),
            peer!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::offline(0)),
        );

        // TargetGradeVariant::Online increases incarnation
        assert_eq!(
            set_grade!(topology, "i1", TargetGradeVariant::Online).unwrap(),
            peer!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(1)),
        );

        // No idempotency, incarnation goes up
        assert_eq!(
            set_grade!(topology, "i1", TargetGradeVariant::Online).unwrap(),
            peer!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::online(2)),
        );

        // TargetGrade::Expelled takes incarnation from current grade
        assert_eq!(
            set_grade!(topology, "i1", TargetGradeVariant::Expelled).unwrap(),
            peer!(1, "i1", "r1", CurrentGrade::offline(0), TargetGrade::expelled(0)),
        );

        // Peer get's expelled
        assert_eq!(
            set_grade!(topology, "i1", CurrentGrade::expelled(69)).unwrap(),
            peer!(1, "i1", "r1", CurrentGrade::expelled(69), TargetGrade::expelled(0)),
        );

        // Updating expelled peers isn't allowed
        assert_eq!(
            set_grade!(topology, "i1", TargetGradeVariant::Online).unwrap_err().to_string(),
            "cannot update expelled peer \"i1\"",
        );
    }

    #[test]
    fn failure_domain() {
        let mut t = Topology::from_peers(peers![]).with_replication_factor(3);

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
        let mut t = Topology::from_peers(peers![]).with_replication_factor(3);

        // first instance
        let (peer, ..) = join!(t, Some("i1"), None, "-", faildoms! {planet: Earth}).unwrap();
        assert_eq!(peer.failure_domain, faildoms! {planet: Earth});
        assert_eq!(peer.replicaset_id, "r1");

        // reconfigure single instance, fail
        assert_eq!(
            set_faildoms!(t, "i1", faildoms! {owner: Ivan})
                .unwrap_err()
                .to_string(),
            "missing failure domain names: PLANET",
        );

        // reconfigure single instance, success
        let peer = set_faildoms!(t, "i1", faildoms! {planet: Mars, owner: Ivan}).unwrap();
        assert_eq!(peer.failure_domain, faildoms! {planet: Mars, owner: Ivan});
        assert_eq!(peer.replicaset_id, "r1"); // same replicaset

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
        let (peer, ..) = join!(t, Some("i2"), None, "-", faildoms! {planet: Mars, owner: Mike})
            .unwrap();
        assert_eq!(peer.failure_domain, faildoms! {planet: Mars, owner: Mike});
        // doesn't fit into r1
        assert_eq!(peer.replicaset_id, "r2");

        // reconfigure second instance, success
        let peer = set_faildoms!(t, "i2", faildoms! {planet: Earth, owner: Mike}).unwrap();
        assert_eq!(peer.failure_domain, faildoms! {planet: Earth, owner: Mike});
        // replicaset doesn't change automatically
        assert_eq!(peer.replicaset_id, "r2");

        // add instance with new subdivision
        #[rustfmt::skip]
        let (peer, ..) = join!(t, Some("i3"), None, "-", faildoms! {planet: B, owner: V, dimension: C137})
            .unwrap();
        assert_eq!(
            peer.failure_domain,
            faildoms! {planet: B, owner: V, dimension: C137}
        );
        assert_eq!(peer.replicaset_id, "r1");

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
