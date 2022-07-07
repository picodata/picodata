use std::collections::{BTreeMap, HashMap, HashSet};

use crate::traft::instance_uuid;
use crate::traft::replicaset_uuid;
use crate::traft::FailureDomains;
use crate::traft::Health;
use crate::traft::Peer;
use crate::traft::{InstanceId, RaftId, ReplicasetId};
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

    fn put_peer(&mut self, peer: Peer) {
        self.max_raft_id = std::cmp::max(self.max_raft_id, peer.raft_id);

        let instance_id = peer.instance_id.clone();
        let replicaset_id = peer.replicaset_id.clone();

        if let Some(old_peer) = self.instance_map.remove(&instance_id) {
            self.replicaset_map
                .entry(old_peer.replicaset_id)
                .or_default()
                .remove(&old_peer.instance_id);
        }

        self.failure_domain_names
            .extend(peer.failure_domains.names().cloned());
        self.instance_map.insert(instance_id.clone(), peer);
        self.replicaset_map
            .entry(replicaset_id)
            .or_default()
            .insert(instance_id);
    }

    fn choose_instance_id(&self, raft_id: u64) -> String {
        let mut suffix: Option<u64> = None;
        loop {
            let ret = match suffix {
                None => format!("i{raft_id}"),
                Some(x) => format!("i{raft_id}-{x}"),
            };

            if !self.instance_map.contains_key(&ret) {
                return ret;
            }

            suffix = Some(suffix.map_or(2, |x| x + 1));
        }
    }

    fn choose_replicaset_id(&self, failure_domains: &FailureDomains) -> String {
        'next_replicaset: for (replicaset_id, peers) in self.replicaset_map.iter() {
            if peers.len() < self.replication_factor as usize {
                for peer_id in peers {
                    let peer = self.instance_map.get(peer_id).unwrap();
                    if peer.failure_domains.intersects(failure_domains) {
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

    pub fn check_required_failure_domains(&self, fd: &FailureDomains) -> Result<(), String> {
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
        instance_id: Option<String>,
        replicaset_id: Option<String>,
        advertise: String,
        failure_domains: FailureDomains,
    ) -> Result<Peer, String> {
        if let Some(id) = instance_id.as_ref() {
            let existing_peer: Option<&Peer> = self.instance_map.get(id);

            if matches!(existing_peer, Some(peer) if peer.is_active()) {
                let e = format!("{} is already joined", id);
                return Err(e);
            }
        }

        self.check_required_failure_domains(&failure_domains)?;

        // Anyway, `join` always produces a new raft_id.
        let raft_id = self.max_raft_id + 1;
        let instance_id: String = instance_id.unwrap_or_else(|| self.choose_instance_id(raft_id));
        let instance_uuid = instance_uuid(&instance_id);
        let replicaset_id: String =
            replicaset_id.unwrap_or_else(|| self.choose_replicaset_id(&failure_domains));
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
            health: Health::Online,
            failure_domains,
        };

        self.put_peer(peer.clone());
        Ok(peer)
    }

    #[allow(unused)]
    pub fn set_advertise(
        &mut self,
        instance_id: String,
        peer_address: String,
    ) -> Result<Peer, String> {
        let mut peer = self
            .instance_map
            .get_mut(&instance_id)
            .ok_or_else(|| format!("unknown instance {}", instance_id))?;

        peer.peer_address = peer_address;
        Ok(peer.clone())
    }

    pub fn set_active(&mut self, instance_id: &str, health: Health) -> Result<Peer, String> {
        let mut peer = self
            .instance_map
            .get_mut(instance_id)
            .ok_or_else(|| format!("unknown instance {}", instance_id))?;

        peer.health = health;
        Ok(peer.clone())
    }
}

// Create first peer in the cluster
pub fn initial_peer(
    instance_id: Option<String>,
    replicaset_id: Option<String>,
    advertise: String,
    failure_domains: FailureDomains,
) -> Peer {
    let mut topology = Topology::from_peers(vec![]);
    let mut peer = topology
        .join(instance_id, replicaset_id, advertise, failure_domains)
        .unwrap();
    peer.commit_index = 1;
    peer
}

#[cfg(test)]
mod tests {
    use super::Topology;

    use crate::traft::instance_uuid;
    use crate::traft::replicaset_uuid;
    use crate::traft::FailureDomains;
    use crate::traft::Health::{Offline, Online};
    use crate::traft::Peer;
    use pretty_assertions::assert_eq;

    macro_rules! peers {
        [ $( (
            $raft_id:expr,
            $instance_id:literal,
            $replicaset_id:literal,
            $peer_address:literal,
            $health:expr
            $(, $failure_domains:expr)?
            $(,)?
        ) ),* $(,)? ] => {
            vec![$(
                peer!($raft_id, $instance_id, $replicaset_id, $peer_address, $health $(,$failure_domains)?)
            ),*]
        };
    }

    macro_rules! peer {
        (
            $raft_id:expr,
            $instance_id:literal,
            $replicaset_id:literal,
            $peer_address:literal,
            $health:expr
            $(, $failure_domains:expr)?
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
                health: $health,
                failure_domains: {
                    let _f = FailureDomains::default();
                    $( let _f = $failure_domains; )?
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
            $(, $failure_domains:expr )?
            $(,)?
        ) => {
            $topology.join(
                $instance_id.map(str::to_string),
                $replicaset_id.map(str::to_string),
                $advertise_address.into(),
                {
                    let _f = FailureDomains::default();
                    $(let _f = $failure_domains; )?
                    _f
                },
            )
        };
    }

    macro_rules! faildoms {
        ($(,)?) => { FailureDomains::default() };
        ($($k:tt : $v:tt),+ $(,)?) => {
            FailureDomains::from([$((stringify!($k), stringify!($v))),+])
        }
    }

    #[test]
    fn test_simple() {
        let mut topology = Topology::from_peers(vec![]).with_replication_factor(1);

        assert_eq!(
            join!(topology, None, None, "addr:1").unwrap(),
            peer!(1, "i1", "r1", "addr:1", Online)
        );

        assert_eq!(
            join!(topology, None, None, "addr:1").unwrap(),
            peer!(2, "i2", "r2", "addr:1", Online)
        );

        assert_eq!(
            join!(topology, None, Some("R3"), "addr:1").unwrap(),
            peer!(3, "i3", "R3", "addr:1", Online)
        );

        assert_eq!(
            join!(topology, Some("I4"), None, "addr:1").unwrap(),
            peer!(4, "I4", "r3", "addr:1", Online)
        );

        let mut topology = Topology::from_peers(peers![(1, "i1", "r1", "addr:1", Online)])
            .with_replication_factor(1);

        assert_eq!(
            join!(topology, None, None, "addr:1").unwrap(),
            peer!(2, "i2", "r2", "addr:1", Online)
        );
    }

    #[test]
    fn test_override() {
        let mut topology = Topology::from_peers(peers![
            (1, "i1", "r1", "active:1", Online),
            (2, "i2", "r2-original", "inactive:1", Offline),
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
        //      not, if replication_factor / failure_domains were edited.
        // - Even if it's an impostor, rely on auto-expel policy.
        //   Disruption isn't destructive if auto-expel allows (TODO).
        assert_eq!(
            join!(topology, Some("i2"), None, "inactive:2").unwrap(),
            peer!(3, "i2", "r1", "inactive:2", Online),
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
            (1, "i1", "r1", "addr:1", Online),
            (2, "i3", "r3", "addr:3", Online),
            // Attention: i3 has raft_id=2
        ]);

        assert_eq!(
            join!(topology, None, Some("r2"), "addr:2").unwrap(),
            peer!(3, "i3-2", "r2", "addr:2", Online),
        );
    }

    #[test]
    fn test_replicaset_mismatch() {
        let mut topology = Topology::from_peers(peers![(3, "i3", "R-A", "x:1", Online)]);
        assert_eq!(
            join!(topology, Some("i3"), Some("R-B"), "x:2")
                .unwrap_err()
                .to_string(),
            "i3 is already joined",
        );

        let mut topology = Topology::from_peers(vec![peer!(2, "i2", "R-A", "nowhere", Online)]);
        assert_eq!(
            join!(topology, Some("i3"), Some("R-A"), "y:1").unwrap(),
            peer!(3, "i3", "R-A", "y:1", Online),
        );
        assert_eq!(
            join!(topology, Some("i3"), Some("R-B"), "y:2")
                .unwrap_err()
                .to_string(),
            "i3 is already joined",
        );
    }

    #[test]
    fn test_replication_factor() {
        let mut topology = Topology::from_peers(peers![
            (9, "i9", "r9", "nowhere", Online),
            (10, "i10", "r9", "nowhere", Online),
        ])
        .with_replication_factor(2);

        assert_eq!(
            join!(topology, Some("i1"), None, "addr:1").unwrap(),
            peer!(11, "i1", "r1", "addr:1", Online),
        );
        assert_eq!(
            join!(topology, Some("i2"), None, "addr:2").unwrap(),
            peer!(12, "i2", "r1", "addr:2", Online),
        );
        assert_eq!(
            join!(topology, Some("i3"), None, "addr:3").unwrap(),
            peer!(13, "i3", "r2", "addr:3", Online),
        );
        assert_eq!(
            join!(topology, Some("i4"), None, "addr:4").unwrap(),
            peer!(14, "i4", "r2", "addr:4", Online),
        );
    }

    #[test]
    fn test_set_active() {
        let mut topology = Topology::from_peers(peers![
            (1, "i1", "r1", "nowhere", Online),
            (2, "i2", "r2", "nowhere", Offline),
        ])
        .with_replication_factor(1);

        assert_eq!(
            topology.set_active("i1", Offline).unwrap(),
            peer!(1, "i1", "r1", "nowhere", Offline),
        );

        // idempotency
        assert_eq!(
            topology.set_active("i1", Offline).unwrap(),
            peer!(1, "i1", "r1", "nowhere", Offline),
        );

        assert_eq!(
            topology.set_active("i2", Online).unwrap(),
            peer!(2, "i2", "r2", "nowhere", Online),
        );

        // idempotency
        assert_eq!(
            topology.set_active("i2", Online).unwrap(),
            peer!(2, "i2", "r2", "nowhere", Online),
        );
    }

    #[test]
    fn failure_domains() {
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
}
