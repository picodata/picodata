use std::collections::BTreeMap;

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
}

impl Topology {
    pub fn from_peers(mut peers: Vec<Peer>) -> Self {
        let mut ret = Self {
            peers: Default::default(),
            diff: Default::default(),

            max_raft_id: 0,
            instance_id_map: Default::default(),
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
    }

    fn peer_by_instance_id(&self, instance_id: &str) -> Option<&Peer> {
        let raft_id = self.instance_id_map.get(instance_id)?;
        self.peers.get(raft_id)
    }

    pub fn process(&mut self, req: &JoinRequest) -> Result<(), String> {
        if let Some(peer) = self.peer_by_instance_id(&req.instance_id) {
            // TODO check replicaset_id didn't change

            let mut peer = peer.clone();
            peer.peer_address = req.advertise_address.clone();
            peer.voter = req.voter;

            self.diff.insert(peer.raft_id, peer.clone());
            self.put_peer(peer);

            return Ok(());
        } else {
            let raft_id = self.max_raft_id + 1;

            let peer = Peer {
                raft_id,
                instance_id: req.instance_id.clone(),
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
            $peer_address:literal,
            $voter:literal
        ) ),* $(,)? ] => {
            vec![$(
                Peer {
                    raft_id: $raft_id,
                    peer_address: $peer_address.into(),
                    voter: $voter,
                    instance_id: $instance_id.into(),
                    commit_index: raft::INVALID_INDEX,
                    instance_uuid: instance_uuid($instance_id),
                }
            ),*]
        };
    }

    macro_rules! req {
        (
            $instance_id:literal,
            $advertise_address:literal,
            $voter:literal
        ) => {
            &JoinRequest {
                instance_id: $instance_id.into(),
                replicaset_id: None,
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

        let peers = peers![(1, "i1", "addr:1", true)];
        assert_eq!(Topology::from_peers(peers).diff(), vec![]);

        test!(
            init: peers![],
            req: [
                req!("i1", "nowhere", true),
            ],
            expected_diff: peers![
                (1, "i1", "nowhere", true),
            ]
        );

        test!(
            init: peers![
                (1, "i1", "addr:1", true),
            ],
            req: [
                req!("i2", "addr:2", false),
            ],
            expected_diff: peers![
                (2, "i2", "addr:2", false),
            ]
        );
    }

    #[test]
    fn test_override() {
        test!(
            init: peers![
                (1, "i1", "addr:1", false),
            ],
            req: [
                req!("i1", "addr:2", true),
            ],
            expected_diff: peers![
                (1, "i1", "addr:2", true),
            ]
        );
    }

    #[test]
    fn test_batch_overlap() {
        test!(
            init: peers![],
            req: [
                req!("i1", "addr:1", false),
                req!("i1", "addr:2", true),
            ],
            expected_diff: peers![
                (1, "i1", "addr:2", true),
            ]
        );
    }
}
