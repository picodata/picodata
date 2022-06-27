use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::traft::instance_uuid;
use crate::traft::replicaset_uuid;
use crate::traft::DeactivateRequest;
use crate::traft::JoinRequest;
use crate::traft::Peer;
use crate::traft::RaftId;
use crate::traft::TopologyRequest;

use raft::INVALID_INDEX;

pub struct Topology {
    peers: BTreeMap<RaftId, Peer>,
    diff: BTreeSet<RaftId>,
    to_replace: BTreeMap<RaftId, RaftId>,
    replication_factor: u8,

    max_raft_id: RaftId,
    instance_id_map: BTreeMap<String, RaftId>,
    replicaset_map: BTreeMap<String, BTreeSet<RaftId>>,
}

impl Topology {
    pub fn from_peers(mut peers: Vec<Peer>) -> Self {
        let mut ret = Self {
            peers: Default::default(),
            diff: Default::default(),
            to_replace: Default::default(),
            replication_factor: 2,

            max_raft_id: 0,
            instance_id_map: Default::default(),
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
        self.peers.insert(peer.raft_id, peer.clone());

        self.max_raft_id = std::cmp::max(self.max_raft_id, peer.raft_id);
        self.instance_id_map.insert(peer.instance_id, peer.raft_id);
        self.replicaset_map
            .entry(peer.replicaset_id.clone())
            .or_default()
            .insert(peer.raft_id);
    }

    fn peer_by_instance_id(&self, instance_id: &str) -> Option<Peer> {
        let raft_id = self.instance_id_map.get(instance_id)?;
        self.peers.get(raft_id).cloned()
    }

    fn choose_replicaset_id(&self) -> String {
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

    fn choose_instance_id(instance_id: Option<String>, raft_id: u64) -> String {
        match instance_id {
            Some(v) => v,
            None => format!("i{raft_id}"),
        }
    }

    pub fn join(&mut self, req: &JoinRequest) -> Result<Peer, String> {
        match &req.instance_id {
            Some(instance_id) => match self.peer_by_instance_id(instance_id) {
                Some(peer) => self.modify_existing_instance(peer, req),
                None => self.join_new_instance(req),
            },
            None => self.join_new_instance(req),
        }
    }

    fn modify_existing_instance(&mut self, peer: Peer, req: &JoinRequest) -> Result<Peer, String> {
        match &req.replicaset_id {
            Some(replicaset_id) if replicaset_id != &peer.replicaset_id => {
                let e = format!(
                    std::concat!(
                        "{} already joined with a different replicaset_id,",
                        " requested: {},",
                        " existing: {}.",
                    ),
                    peer.instance_id, replicaset_id, peer.replicaset_id
                );
                return Err(e);
            }
            _ => (),
        }

        let mut peer = peer;
        peer.peer_address = req.advertise_address.clone();
        peer.voter = req.voter;
        peer.is_active = true;

        if req.voter {
            self.diff.insert(peer.raft_id);
        } else {
            let old_raft_id = peer.raft_id;
            peer.raft_id = self.max_raft_id + 1;

            self.to_replace.insert(peer.raft_id, old_raft_id);
        }
        self.put_peer(peer.clone());
        Ok(peer)
    }

    fn join_new_instance(&mut self, req: &JoinRequest) -> Result<Peer, String> {
        let raft_id = self.max_raft_id + 1;
        let replicaset_id = match &req.replicaset_id {
            Some(v) => v.clone(),
            None => self.choose_replicaset_id(),
        };
        let replicaset_uuid = replicaset_uuid(&replicaset_id);
        let instance_id = Self::choose_instance_id(req.instance_id.clone(), raft_id);

        let peer = Peer {
            raft_id,
            instance_id: instance_id.clone(),
            replicaset_id,
            commit_index: INVALID_INDEX,
            instance_uuid: instance_uuid(&instance_id),
            replicaset_uuid,
            peer_address: req.advertise_address.clone(),
            voter: req.voter,
            is_active: true,
        };

        self.diff.insert(raft_id);
        self.put_peer(peer.clone());
        Ok(peer)
    }

    pub fn deactivate(&mut self, req: &DeactivateRequest) -> Result<Peer, String> {
        let peer = match self.peer_by_instance_id(&req.instance_id) {
            Some(peer) => peer,
            None => {
                return Err(format!(
                    "request to deactivate unknown instance {}",
                    &req.instance_id
                ))
            }
        };

        let peer = Peer {
            voter: false,
            is_active: false,
            ..peer
        };

        self.diff.insert(peer.raft_id);
        // no need to call put_peer, as the peer was already in the cluster
        self.peers.insert(peer.raft_id, peer.clone());

        Ok(peer)
    }

    pub fn process(&mut self, req: &TopologyRequest) -> Result<Peer, String> {
        match req {
            TopologyRequest::Join(join) => self.join(join),
            TopologyRequest::Deactivate(deactivate) => self.deactivate(deactivate),
        }
    }

    pub fn diff(&self) -> Vec<Peer> {
        self.diff
            .iter()
            .map(|id| self.peers.get(id).expect("peers must contain all peers"))
            .cloned()
            .collect()
    }

    pub fn to_replace(&self) -> Vec<(RaftId, Peer)> {
        self.to_replace
            .iter()
            .map(|(new_id, &old_id)| {
                let peer = self
                    .peers
                    .get(new_id)
                    .expect("peers must contain all peers")
                    .clone();
                (old_id, peer)
            })
            .collect()
    }
}

// Create first peer in the cluster
pub fn initial_peer(
    cluster_id: String,
    instance_id: Option<String>,
    replicaset_id: Option<String>,
    advertise_address: String,
) -> Peer {
    let mut topology = Topology::from_peers(vec![]);
    let req = JoinRequest {
        cluster_id,
        instance_id,
        replicaset_id,
        advertise_address,
        voter: true,
    };
    topology.join(&req).unwrap();
    topology.diff().pop().unwrap()
}

#[cfg(test)]
mod tests {
    use super::Topology;
    use crate::traft::instance_uuid;
    use crate::traft::replicaset_uuid;
    use crate::traft::Peer;

    macro_rules! peers {
        [ $( (
            $raft_id:expr,
            $instance_id:literal,
            $replicaset_id:literal,
            $peer_address:literal,
            $voter:literal
            $(, $is_active:literal)?
            $(,)?
        ) ),* $(,)? ] => {
            vec![$(
                peer!($raft_id, $instance_id, $replicaset_id, $peer_address, $voter $(, $is_active)?)
            ),*]
        };
    }

    macro_rules! peer {
        (
            $raft_id:expr,
            $instance_id:literal,
            $replicaset_id:literal,
            $peer_address:literal,
            $voter:literal
            $(, $is_active:literal)?
            $(,)?
        ) => {{
            let peer = Peer {
                raft_id: $raft_id,
                peer_address: $peer_address.into(),
                voter: $voter,
                instance_id: $instance_id.into(),
                replicaset_id: $replicaset_id.into(),
                instance_uuid: instance_uuid($instance_id),
                replicaset_uuid: replicaset_uuid($replicaset_id),
                commit_index: raft::INVALID_INDEX,
                is_active: true,
            };
            $( let peer = Peer { is_active: $is_active, ..peer }; )?
            peer
        }};
    }

    macro_rules! join {
        (
            $instance_id:literal,
            $replicaset_id:expr,
            $advertise_address:literal,
            $voter:literal
        ) => {
            &crate::traft::TopologyRequest::Join(crate::traft::JoinRequest {
                cluster_id: "cluster1".into(),
                instance_id: Some($instance_id.into()),
                replicaset_id: $replicaset_id.map(|v: &str| v.into()),
                advertise_address: $advertise_address.into(),
                voter: $voter,
            })
        };
    }

    macro_rules! deactivate {
        ($instance_id:literal) => {
            &crate::traft::TopologyRequest::Deactivate(crate::traft::DeactivateRequest {
                instance_id: $instance_id.into(),
                cluster_id: "cluster1".into(),
            })
        };
    }

    macro_rules! test_reqs {
        (
            replication_factor: $replication_factor:literal,
            init: $peers:expr,
            req: [ $( $req:expr ),* $(,)?],
            expected_diff: $expected:expr,
            $( expected_to_replace: $expected_to_replace:expr, )?
        ) => {
            let mut t = Topology::from_peers($peers)
                .with_replication_factor($replication_factor);
            $( t.process($req).unwrap(); )*

            pretty_assertions::assert_eq!(t.diff(), $expected);
            $( pretty_assertions::assert_eq!(t.to_replace(), $expected_to_replace); )?
        };
    }

    #[test]
    fn test_simple() {
        assert_eq!(Topology::from_peers(vec![]).diff(), vec![]);

        let peers = peers![(1, "i1", "R1", "addr:1", true)];
        assert_eq!(Topology::from_peers(peers).diff(), vec![]);

        test_reqs!(
            replication_factor: 1,
            init: peers![],
            req: [
                join!("i1", None, "nowhere", true),
                join!("i2", None, "nowhere", true),
            ],
            expected_diff: peers![
                (1, "i1", "r1", "nowhere", true),
                (2, "i2", "r2", "nowhere", true),
            ],
        );

        test_reqs!(
            replication_factor: 1,
            init: peers![
                (1, "i1", "R1", "addr:1", true),
            ],
            req: [
                join!("i2", Some("R2"), "addr:2", false),
            ],
            expected_diff: peers![
                (2, "i2", "R2", "addr:2", false),
            ],
        );
    }

    #[test]
    fn test_override() {
        test_reqs!(
            replication_factor: 1,
            init: peers![
                (1, "i1", "R1", "addr:1", false),
            ],
            req: [
                join!("i1", None, "addr:2", true),
            ],
            expected_diff: peers![
                (1, "i1", "R1", "addr:2", true),
            ],
        );
    }

    #[test]
    fn test_batch_overlap() {
        test_reqs!(
            replication_factor: 1,
            init: peers![],
            req: [
                join!("i1", Some("R1"), "addr:1", false),
                join!("i1", None, "addr:2", true),
            ],
            expected_diff: peers![
                (1, "i1", "R1", "addr:2", true),
            ],
        );
    }

    #[test]
    fn test_replicaset_mismatch() {
        let expected_error = concat!(
            "i3 already joined with a different replicaset_id,",
            " requested: R-B,",
            " existing: R-A.",
        );

        let peers = peers![(3, "i3", "R-A", "x:1", false)];
        let mut topology = Topology::from_peers(peers);
        topology
            .process(join!("i3", Some("R-B"), "x:2", true))
            .map_err(|e| assert_eq!(e, expected_error))
            .unwrap_err();
        assert_eq!(topology.diff(), vec![]);
        assert_eq!(topology.to_replace(), vec![]);

        let peers = peers![(2, "i2", "R-A", "nowhere", true)];
        let mut topology = Topology::from_peers(peers);
        topology
            .process(join!("i3", Some("R-A"), "y:1", false))
            .unwrap();
        topology
            .process(join!("i3", Some("R-B"), "y:2", true))
            .map_err(|e| assert_eq!(e, expected_error))
            .unwrap_err();
        assert_eq!(topology.diff(), peers![(3, "i3", "R-A", "y:1", false)]);
        assert_eq!(topology.to_replace(), vec![]);
    }

    #[test]
    fn test_replication_factor() {
        test_reqs!(
            replication_factor: 2,
            init: peers![
                (9, "i9", "r9", "nowhere", false),
                (10, "i9", "r9", "nowhere", false),
            ],
            req: [
                join!("i1", None, "addr:1", true),
                join!("i2", None, "addr:2", false),
                join!("i3", None, "addr:3", false),
                join!("i4", None, "addr:4", false),
            ],
            expected_diff: peers![
                (11, "i1", "r1", "addr:1", true),
                (12, "i2", "r1", "addr:2", false),
                (13, "i3", "r2", "addr:3", false),
                (14, "i4", "r2", "addr:4", false),
            ],
        );
    }

    #[test]
    fn test_replace() {
        test_reqs!(
            replication_factor: 2,
            init: peers![
                (1, "i1", "r1", "nowhere", false),
            ],
            req: [
                join!("i1", None, "addr:2", false),
            ],
            expected_diff: peers![],
            expected_to_replace: vec![
                (1, peer!(2, "i1", "r1", "addr:2", false)),
            ],
        );
    }

    #[test]
    fn test_deactivation() {
        test_reqs!(
            replication_factor: 1,
            init: peers![
                (1, "deactivate", "r1", "nowhere", true, true),
                (2, "activate_learner", "r2", "nowhere", false, false),
                (3, "activate_voter", "r3", "nowhere", false, false),
            ],
            req: [
                deactivate!("deactivate"),
                join!("activate_learner", None, "nowhere", false),
                join!("activate_voter", None, "nowhere", true),
            ],
            expected_diff: peers![
                (1, "deactivate", "r1", "nowhere", false, false),
                (3, "activate_voter", "r3", "nowhere", true, true),
            ],
            expected_to_replace: vec![
                (2, peer!(4, "activate_learner", "r2", "nowhere", false, true)),
            ],
        );

        test_reqs!(
            replication_factor: 1,
            init: peers![],
            req: [
                join!("deactivate", Some("r1"), "nowhere", true),
                deactivate!("deactivate"),
                deactivate!("deactivate"),
                deactivate!("deactivate"),
            ],
            expected_diff: peers![
                (1, "deactivate", "r1", "nowhere", false, false),
            ],
        );
    }
}
