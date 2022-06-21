use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::traft::instance_uuid;
use crate::traft::replicaset_uuid;
use crate::traft::FailureDomains;
use crate::traft::Health;
use crate::traft::Peer;
use crate::traft::{InstanceId, RaftId, ReplicasetId};

use raft::INVALID_INDEX;

pub struct Topology {
    replication_factor: u8,
    max_raft_id: RaftId,

    instance_map: BTreeMap<InstanceId, Peer>,
    replicaset_map: BTreeMap<ReplicasetId, BTreeSet<InstanceId>>,
}

impl Topology {
    pub fn from_peers(mut peers: Vec<Peer>) -> Self {
        let mut ret = Self {
            replication_factor: 2,
            max_raft_id: 0,
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

        self.instance_map.insert(instance_id.clone(), peer);
        self.replicaset_map
            .entry(replicaset_id)
            .or_default()
            .insert(instance_id);
    }

    fn choose_instance_id(&self, raft_id: u64) -> String {
        // FIXME: what if generated instance_id is already taken?
        format!("i{raft_id}")
    }

    fn choose_replicaset_id(&self, failure_domains: &FailureDomains) -> String {
        // TODO: implement logic
        let _ = failure_domains;
        for (replicaset_id, peers) in self.replicaset_map.iter() {
            if peers.len() < self.replication_factor as usize {
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

        // Anyway, `join` always produces a new raft_id.
        let raft_id = self.max_raft_id + 1;
        let instance_id: String = instance_id.unwrap_or_else(|| self.choose_instance_id(raft_id));
        let instance_uuid = instance_uuid(&instance_id);
        let replicaset_id: String =
            replicaset_id.unwrap_or_else(|| self.choose_replicaset_id(&failure_domains));
        let replicaset_uuid = replicaset_uuid(&replicaset_id);

        // TODO: store it in peer
        let _ = failure_domains;

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
            $health:expr $(,)?
        ) ),* $(,)? ] => {
            vec![$(
                peer!($raft_id, $instance_id, $replicaset_id, $peer_address, $health)
            ),*]
        };
    }

    macro_rules! peer {
        (
            $raft_id:expr,
            $instance_id:literal,
            $replicaset_id:literal,
            $peer_address:literal,
            $health:expr $(,)?
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
            }
        };
    }

    macro_rules! join {
        (
            $topology:expr,
            $instance_id:expr,
            $replicaset_id:expr,
            $advertise_address:literal $(,)?
        ) => {
            $topology.join(
                $instance_id.map(str::to_string),
                $replicaset_id.map(str::to_string),
                $advertise_address.into(),
                FailureDomains::default(),
            )
        };
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
    fn test_override_existing() {
        let mut topology = Topology::from_peers(peers![
            (1, "i1", "R1", "active:1", Online),
            (2, "i2", "R2", "inactive:1", Offline),
        ]);

        assert_eq!(
            join!(topology, Some("i1"), None, "active:2")
                .unwrap_err()
                .to_string(),
            "i1 is already joined",
        );

        assert_eq!(
            join!(topology, Some("i2"), None, "inactive:2").unwrap(),
            peer!(3, "i2", "R1", "inactive:2", Online),
        );
    }

    #[test]
    fn test_override_joined() {
        let mut topology = Topology::from_peers(vec![]);

        assert_eq!(
            join!(topology, Some("i1"), Some("R1"), "addr:1").unwrap(),
            peer!(1, "i1", "R1", "addr:1", Online),
        );

        assert_eq!(
            join!(topology, Some("i1"), None, "addr:2")
                .unwrap_err()
                .to_string(),
            "i1 is already joined",
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
}
