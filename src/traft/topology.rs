use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::traft::instance_uuid;
use crate::traft::JoinRequest;
use crate::traft::Peer;
use crate::traft::RaftId;

use raft::INVALID_INDEX;

pub struct Topology {
    peers: BTreeMap<RaftId, Peer>,
    diff: BTreeMap<RaftId, Peer>,

    max_raft_id: RaftId,
    instance_id_map: BTreeMap<String, RaftId>,
    replicaset_map: BTreeMap<String, BTreeSet<RaftId>>,
}

impl Topology {
    pub fn from_peers(mut peers: Vec<Peer>) -> Self {
        let mut ret = Self {
            peers: Default::default(),
            diff: Default::default(),

            max_raft_id: 0,
            instance_id_map: Default::default(),
            replicaset_map: Default::default(),
        };

        for peer in peers.drain(..) {
            ret.put_peer(peer);
        }

        ret
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

    fn peer_by_instance_id(&self, instance_id: &str) -> Option<&Peer> {
        let raft_id = self.instance_id_map.get(instance_id)?;
        self.peers.get(raft_id)
    }

    fn choose_replicaset_id(&self) -> String {
        let mut i = 0u64;
        loop {
            i += 1;
            let replicaset_id = format!("r{i}");
            if self.replicaset_map.get(&replicaset_id).is_none() {
                return replicaset_id;
            }
        }
    }

    pub fn process(&mut self, req: &JoinRequest) -> Result<(), String> {
        if let Some(peer) = self.peer_by_instance_id(&req.instance_id) {
            match &req.replicaset_id {
                Some(replicaset_id) if replicaset_id != &peer.replicaset_id => {
                    let e = format!(
                        std::concat!(
                            "{} already joined with a different replicaset_id,",
                            " requested: {},",
                            " existing: {}.",
                        ),
                        req.instance_id, replicaset_id, peer.replicaset_id
                    );
                    return Err(e);
                }
                _ => (),
            }

            let mut peer = peer.clone();
            peer.peer_address = req.advertise_address.clone();
            peer.voter = req.voter;

            self.diff.insert(peer.raft_id, peer.clone());
            self.put_peer(peer);

            return Ok(());
        } else {
            let raft_id = self.max_raft_id + 1;
            let replicaset_id = match &req.replicaset_id {
                Some(v) => v.clone(),
                None => self.choose_replicaset_id(),
            };

            let peer = Peer {
                raft_id,
                instance_id: req.instance_id.clone(),
                replicaset_id,
                commit_index: INVALID_INDEX,
                instance_uuid: instance_uuid(&req.instance_id),
                peer_address: req.advertise_address.clone(),
                voter: req.voter,
            };

            self.diff.insert(raft_id, peer.clone());
            self.put_peer(peer);
        }

        Ok(())
    }

    pub fn diff(self) -> Vec<Peer> {
        self.diff.into_values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::Topology;
    use crate::traft::instance_uuid;
    use crate::traft::JoinRequest;
    use crate::traft::Peer;

    macro_rules! peers {
        [ $( (
            $raft_id:expr,
            $instance_id:literal,
            $replicaset_id:literal,
            $peer_address:literal,
            $voter:literal
        ) ),* $(,)? ] => {
            vec![$(
                Peer {
                    raft_id: $raft_id,
                    peer_address: $peer_address.into(),
                    voter: $voter,
                    instance_id: $instance_id.into(),
                    replicaset_id: $replicaset_id.into(),
                    commit_index: raft::INVALID_INDEX,
                    instance_uuid: instance_uuid($instance_id),
                }
            ),*]
        };
    }

    macro_rules! req {
        (
            $instance_id:literal,
            $replicaset_id:expr,
            $advertise_address:literal,
            $voter:literal
        ) => {
            &JoinRequest {
                instance_id: $instance_id.into(),
                replicaset_id: $replicaset_id.map(|v: &str| v.into()),
                advertise_address: $advertise_address.into(),
                voter: $voter,
            }
        };
    }

    macro_rules! test {
        (
            init: $peers:expr,
            req: [ $( $req:expr ),* $(,)?],
            expected_diff: $expected:expr
        ) => {
            let mut t = Topology::from_peers($peers);
            $( t.process($req).unwrap(); )*

            assert_eq!(t.diff(), $expected);
        };
    }

    #[test]
    fn test_simple() {
        assert_eq!(Topology::from_peers(vec![]).diff(), vec![]);

        let peers = peers![(1, "i1", "R1", "addr:1", true)];
        assert_eq!(Topology::from_peers(peers).diff(), vec![]);

        test!(
            init: peers![],
            req: [
                req!("i1", None, "nowhere", true),
                req!("i2", None, "nowhere", true),
            ],
            expected_diff: peers![
                (1, "i1", "r1", "nowhere", true),
                (2, "i2", "r2", "nowhere", true),
            ]
        );

        test!(
            init: peers![
                (1, "i1", "R1", "addr:1", true),
            ],
            req: [
                req!("i2", Some("R2"), "addr:2", false),
            ],
            expected_diff: peers![
                (2, "i2", "R2", "addr:2", false),
            ]
        );
    }

    #[test]
    fn test_override() {
        test!(
            init: peers![
                (1, "i1", "R1", "addr:1", false),
            ],
            req: [
                req!("i1", None, "addr:2", true),
            ],
            expected_diff: peers![
                (1, "i1", "R1", "addr:2", true),
            ]
        );
    }

    #[test]
    fn test_batch_overlap() {
        test!(
            init: peers![],
            req: [
                req!("i1", Some("R1"), "addr:1", false),
                req!("i1", None, "addr:2", true),
            ],
            expected_diff: peers![
                (1, "i1", "R1", "addr:2", true),
            ]
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
            .process(req!("i3", Some("R-B"), "x:2", true))
            .map_err(|e| assert_eq!(e, expected_error))
            .unwrap_err();
        assert_eq!(topology.diff(), vec![]);

        let peers = peers![(2, "i2", "R-A", "nowhere", true)];
        let mut topology = Topology::from_peers(peers);
        topology
            .process(req!("i3", Some("R-A"), "y:1", false))
            .unwrap();
        topology
            .process(req!("i3", Some("R-B"), "y:2", true))
            .map_err(|e| assert_eq!(e, expected_error))
            .unwrap_err();
        assert_eq!(topology.diff(), peers![(3, "i3", "R-A", "y:1", false)]);
    }
}
