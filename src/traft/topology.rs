use std::collections::{BTreeMap, HashMap, HashSet};

use crate::traft::instance_uuid;
use crate::traft::replicaset_uuid;
use crate::traft::FailureDomain;
use crate::traft::Peer;
use crate::traft::{Grade, TargetGrade};
use crate::traft::{InstanceId, RaftId, ReplicasetId};
use crate::traft::{PeerChange, UpdatePeerRequest};
use crate::util::Uppercase;

use raft::INVALID_INDEX;

pub struct Topology {
    replication_factor: u8,
    max_raft_id: RaftId,

    failure_domain_names: HashSet<Uppercase>,
    instance_map: HashMap<InstanceId, Peer>,
    replicaset_map: BTreeMap<ReplicasetId, HashSet<InstanceId>>,
}

impl Topology {
    pub fn from_peers(mut peers: Vec<Peer>) -> Self {
        let mut ret = Self {
            replication_factor: 1,
            max_raft_id: 0,
            failure_domain_names: Default::default(),
            instance_map: Default::default(),
            replicaset_map: Default::default(),
        };

        for peer in peers.drain(..) {
            ret.put_peer(peer);
        }

        ret
    }

    pub fn with_replication_factor(mut self, replication_factor: u8) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    #[allow(dead_code)]
    pub fn get_peer(&mut self, instance_id: &str) -> Result<Peer, String> {
        let peer = self
            .instance_map
            .get_mut(instance_id)
            .ok_or_else(|| format!("unknown instance {}", instance_id))?;

        Ok(peer.clone())
    }

    fn put_peer(&mut self, peer: Peer) {
        self.max_raft_id = std::cmp::max(self.max_raft_id, peer.raft_id);

        let instance_id = peer.instance_id.clone();
        let replicaset_id = peer.replicaset_id.clone();

        if let Some(old_peer) = self.instance_map.remove(&instance_id) {
            self.replicaset_map
                .get_mut(&old_peer.replicaset_id)
                .map(|r| r.remove(&old_peer.instance_id));
        }

        self.failure_domain_names
            .extend(peer.failure_domain.names().cloned());
        self.instance_map.insert(instance_id.clone(), peer);
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

    fn choose_replicaset_id(&self, failure_domain: &FailureDomain) -> String {
        'next_replicaset: for (replicaset_id, peers) in self.replicaset_map.iter() {
            if peers.len() < self.replication_factor as usize {
                for peer_id in peers {
                    let peer = self.instance_map.get(peer_id).unwrap();
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
            let replicaset_id = format!("r{i}");
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
        replicaset_id: Option<String>,
        advertise: String,
        failure_domain: FailureDomain,
    ) -> Result<Peer, String> {
        if let Some(id) = instance_id.as_ref() {
            let existing_peer: Option<&Peer> = self.instance_map.get(id);

            if matches!(existing_peer, Some(peer) if peer.is_active()) {
                let e = format!("{} is already joined", id);
                return Err(e);
            }
        }

        self.check_required_failure_domain(&failure_domain)?;

        // Anyway, `join` always produces a new raft_id.
        let raft_id = self.max_raft_id + 1;
        let instance_id = instance_id.unwrap_or_else(|| self.choose_instance_id(raft_id));
        let instance_uuid = instance_uuid(&instance_id);
        let replicaset_id: String =
            replicaset_id.unwrap_or_else(|| self.choose_replicaset_id(&failure_domain));
        let replicaset_uuid = replicaset_uuid(&replicaset_id);

        let peer = Peer {
            instance_id,
            instance_uuid,
            raft_id,
            peer_address: advertise,
            replicaset_id,
            replicaset_uuid,
            commit_index: INVALID_INDEX,
            // Mark instance already active when it joins.
            // It prevents a disruption in case of the
            // instance_id collision.
            grade: Grade::Offline,
            target_grade: TargetGrade::Offline,
            failure_domain,
        };

        self.put_peer(peer.clone());
        Ok(peer)
    }

    #[allow(unused)]
    pub fn set_advertise(
        &mut self,
        instance_id: InstanceId,
        peer_address: String,
    ) -> Result<Peer, String> {
        let mut peer = self
            .instance_map
            .get_mut(&instance_id)
            .ok_or_else(|| format!("unknown instance {}", instance_id))?;

        peer.peer_address = peer_address;
        Ok(peer.clone())
    }

    pub fn update_peer(&mut self, req: UpdatePeerRequest) -> Result<Peer, String> {
        let this = self as *const Self;

        let mut peer = self
            .instance_map
            .get_mut(&req.instance_id)
            .ok_or_else(|| format!("unknown instance {}", req.instance_id))?;

        for change in req.changes {
            match change {
                PeerChange::Grade(grade) => {
                    if peer.grade != Grade::Expelled {
                        peer.grade = grade;
                    }
                }
                PeerChange::TargetGrade(target_grade) => {
                    if peer.target_grade != TargetGrade::Expelled {
                        peer.target_grade = target_grade;
                    }
                }
                PeerChange::FailureDomain(fd) => {
                    // SAFETY: this is safe, because rust doesn't complain if you inline
                    // the function
                    unsafe { &*this }.check_required_failure_domain(&fd)?;
                    self.failure_domain_names.extend(fd.names().cloned());
                    peer.failure_domain = fd;
                }
            }
        }

        Ok(peer.clone())
    }
}

// Create first peer in the cluster
pub fn initial_peer(
    instance_id: Option<InstanceId>,
    replicaset_id: Option<String>,
    advertise: String,
    failure_domain: FailureDomain,
) -> Peer {
    let mut topology = Topology::from_peers(vec![]);
    let mut peer = topology
        .join(instance_id, replicaset_id, advertise, failure_domain)
        .unwrap();
    peer.commit_index = 1;
    peer
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
    use crate::traft::{Grade, TargetGrade};
    use pretty_assertions::assert_eq;

    macro_rules! peers {
        [ $( (
            $raft_id:expr,
            $instance_id:literal,
            $replicaset_id:literal,
            $peer_address:literal,
            $grade:expr,
            $target_grade:expr
            $(, $failure_domain:expr)?
            $(,)?
        ) ),* $(,)? ] => {
            vec![$(
                peer!($raft_id, $instance_id, $replicaset_id, $peer_address, $grade, $target_grade $(,$failure_domain)?)
            ),*]
        };
    }

    macro_rules! peer {
        (
            $raft_id:expr,
            $instance_id:literal,
            $replicaset_id:literal,
            $peer_address:literal,
            $grade:expr,
            $target_grade:expr
            $(, $failure_domain:expr)?
            $(,)?
        ) => {
            Peer {
                raft_id: $raft_id,
                peer_address: $peer_address.into(),
                instance_id: $instance_id.into(),
                replicaset_id: $replicaset_id.into(),
                instance_uuid: instance_uuid($instance_id),
                replicaset_uuid: replicaset_uuid($replicaset_id),
                commit_index: raft::INVALID_INDEX,
                grade: $grade,
                target_grade: $target_grade,
                failure_domain: {
                    let _f = FailureDomain::default();
                    $( let _f = $failure_domain; )?
                    _f
                },
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
                $replicaset_id.map(str::to_string),
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
            $instance_id:expr,
            $grade:expr $(,)?
        ) => {
            $topology.update_peer(
                UpdatePeerRequest::new($instance_id.into(), "".into()).with_grade($grade),
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
                    .with_grade(Grade::Online)
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
            peer!(1, "i1", "r1", "addr:1", Grade::Offline, TargetGrade::Offline)
        );

        assert_eq!(
            join!(topology, None, None, "addr:1").unwrap(),
            peer!(2, "i2", "r2", "addr:1", Grade::Offline, TargetGrade::Offline)
        );

        assert_eq!(
            join!(topology, None, Some("R3"), "addr:1").unwrap(),
            peer!(3, "i3", "R3", "addr:1", Grade::Offline, TargetGrade::Offline)
        );

        assert_eq!(
            join!(topology, Some("I4"), None, "addr:1").unwrap(),
            peer!(4, "I4", "r3", "addr:1", Grade::Offline, TargetGrade::Offline)
        );

        let mut topology = Topology::from_peers(
            peers![(1, "i1", "r1", "addr:1", Grade::Offline, TargetGrade::Offline)]
        ).with_replication_factor(1);

        assert_eq!(
            join!(topology, None, None, "addr:1").unwrap(),
            peer!(2, "i2", "r2", "addr:1", Grade::Offline, TargetGrade::Offline)
        );
    }

    #[test]
    fn test_override() {
        let mut topology = Topology::from_peers(peers![
            (1, "i1", "r1", "active:1", Grade::Online, TargetGrade::Online),
            (2, "i2", "r2-original", "inactive:1", Grade::Offline, TargetGrade::Offline),
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
            peer!(3, "i2", "r1", "inactive:2", Grade::Offline, TargetGrade::Offline),
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
            (1, "i1", "r1", "addr:1", Grade::Online, TargetGrade::Online),
            (2, "i3", "r3", "addr:3", Grade::Online, TargetGrade::Online),
            // Attention: i3 has raft_id=2
        ]);

        assert_eq!(
            join!(topology, None, Some("r2"), "addr:2").unwrap(),
            peer!(3, "i3-2", "r2", "addr:2", Grade::Offline, TargetGrade::Offline),
        );
    }

    #[test]
    fn test_replication_factor() {
        let mut topology = Topology::from_peers(peers![
            (9, "i9", "r9", "nowhere", Grade::Online, TargetGrade::Online),
            (10, "i10", "r9", "nowhere", Grade::Online, TargetGrade::Online),
        ])
        .with_replication_factor(2);

        assert_eq!(
            join!(topology, Some("i1"), None, "addr:1").unwrap(),
            peer!(11, "i1", "r1", "addr:1", Grade::Offline, TargetGrade::Offline),
        );
        assert_eq!(
            join!(topology, Some("i2"), None, "addr:2").unwrap(),
            peer!(12, "i2", "r1", "addr:2", Grade::Offline, TargetGrade::Offline),
        );
        assert_eq!(
            join!(topology, Some("i3"), None, "addr:3").unwrap(),
            peer!(13, "i3", "r2", "addr:3", Grade::Offline, TargetGrade::Offline),
        );
        assert_eq!(
            join!(topology, Some("i4"), None, "addr:4").unwrap(),
            peer!(14, "i4", "r2", "addr:4", Grade::Offline, TargetGrade::Offline),
        );
    }

    #[test]
    fn test_set_active() {
        let mut topology = Topology::from_peers(peers![
            (1, "i1", "r1", "nowhere", Grade::Online, TargetGrade::Online),
            (2, "i2", "r2", "nowhere", Grade::Online, TargetGrade::Online),
        ])
        .with_replication_factor(1);

        assert_eq!(
            set_grade!(topology, "i1", Grade::Offline).unwrap(),
            peer!(1, "i1", "r1", "nowhere", Grade::Offline, TargetGrade::Online),
        );

        // idempotency
        assert_eq!(
            set_grade!(topology, "i1", Grade::Offline).unwrap(),
            peer!(1, "i1", "r1", "nowhere", Grade::Offline, TargetGrade::Online),
        );

        assert_eq!(
            set_grade!(topology, "i2", Grade::Offline).unwrap(),
            peer!(2, "i2", "r2", "nowhere", Grade::Offline, TargetGrade::Online),
        );

        // idempotency
        assert_eq!(
            set_grade!(topology, "i2", Grade::Offline).unwrap(),
            peer!(2, "i2", "r2", "nowhere", Grade::Offline, TargetGrade::Online),
        );
    }

    #[test]
    fn failure_domain() {
        let mut t = Topology::from_peers(peers![]).with_replication_factor(3);

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Earth})
                .unwrap()
                .replicaset_id,
            "r1",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Earth})
                .unwrap()
                .replicaset_id,
            "r2",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Mars})
                .unwrap()
                .replicaset_id,
            "r1",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Earth, os: BSD})
                .unwrap()
                .replicaset_id,
            "r3",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Mars, os: BSD})
                .unwrap()
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
                .replicaset_id,
            "r1",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Venus, os: Mac})
                .unwrap()
                .replicaset_id,
            "r2",
        );

        assert_eq!(
            join!(t, None, None, "-", faildoms! {planet: Mars, os: Mac})
                .unwrap()
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
        let peer = join!(t, Some("i1"), None, "-", faildoms! {planet: Earth}).unwrap();
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
        let peer = join!(t, Some("i2"), None, "-", faildoms! {planet: Mars, owner: Mike})
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
        let peer = join!(t, Some("i3"), None, "-", faildoms! {planet: B, owner: V, dimension: C137})
            .unwrap();
        assert_eq!(
            peer.failure_domain,
            faildoms! {planet: B, owner: V, dimension: C137}
        );
        assert_eq!(peer.replicaset_id, "r1");

        assert_eq!(
            set_grade!(t, "i3", Grade::Offline).unwrap().failure_domain,
            faildoms! {planet: B, owner: V, dimension: C137},
        );

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
