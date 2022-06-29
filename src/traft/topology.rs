use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::traft::instance_uuid;
use crate::traft::replicaset_uuid;
use crate::traft::Peer;
use crate::traft::{
    InstanceId,
    // type aliases
    RaftId,
    ReplicasetId,
};

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

        // FIXME remove old peer
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

    pub fn join(
        &mut self,
        instance_id: Option<String>,
        replicaset_id: Option<String>,
        advertise: String,
    ) -> Result<Peer, String> {
        if let Some(id) = instance_id.as_ref() {
            let existing_peer: Option<&Peer> = self.instance_map.get(id);

            if matches!(existing_peer, Some(peer) if peer.is_active) {
                let e = format!("{} is already joined", id);
                return Err(e);
            }
        }

        // Anyway, `join` always produces a new raft_id.
        let raft_id = self.max_raft_id + 1;
        let instance_id: String = instance_id.unwrap_or_else(|| self.choose_instance_id(raft_id));
        let instance_uuid = instance_uuid(&instance_id);
        let replicaset_id: String = replicaset_id.unwrap_or_else(|| self.choose_replicaset_id());
        let replicaset_uuid = replicaset_uuid(&replicaset_id);

        let peer = Peer {
            instance_id,
            instance_uuid,
            raft_id,
            peer_address: advertise.into(),
            replicaset_id,
            replicaset_uuid,
            commit_index: INVALID_INDEX,
            is_active: true,
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

    pub fn set_active(&mut self, instance_id: String, active: bool) -> Result<Peer, String> {
        let mut peer = self
            .instance_map
            .get_mut(&instance_id)
            .ok_or_else(|| format!("unknown instance {}", instance_id))?;

        peer.is_active = active;
        Ok(peer.clone())
    }
}

// Create first peer in the cluster
pub fn initial_peer(
    instance_id: Option<String>,
    replicaset_id: Option<String>,
    advertise: String,
) -> Peer {
    let mut topology = Topology::from_peers(vec![]);
    let mut peer = topology
        .join(instance_id, replicaset_id, advertise)
        .unwrap();
    peer.commit_index = 1;
    peer
}

// #[cfg(test)]
// mod tests {
//     use super::Topology;
//     use crate::traft::instance_uuid;
//     use crate::traft::replicaset_uuid;
//     use crate::traft::Peer;

//     // macro_rules! peers {
//     //     [ $( (
//     //         $raft_id:expr,
//     //         $instance_id:literal,
//     //         $replicaset_id:literal,
//     //         $peer_address:literal,
//     //         $(, $is_active:literal)?
//     //         $(,)?
//     //     ) ),* $(,)? ] => {
//     //         vec![$(
//     //             peer!($raft_id, $instance_id, $replicaset_id, $peer_address $(, $is_active)?)
//     //         ),*]
//     //     };
//     // }

//     macro_rules! peer {
//         (
//             $raft_id:expr,
//             $instance_id:literal,
//             $replicaset_id:literal,
//             $peer_address:literal,
//             $(, $active:literal)?
//             $(,)?
//         ) => {{
//             let peer = Peer {
//                 raft_id: $raft_id,
//                 peer_address: $peer_address,
//                 instance_id: $instance_id.into(),
//                 replicaset_id: $replicaset_id.into(),
//                 instance_uuid: instance_uuid($instance_id),
//                 replicaset_uuid: replicaset_uuid($replicaset_id),
//                 commit_index: raft::INVALID_INDEX,
//                 active: true,
//             };
//             $( let peer = Peer { active: $active, ..peer }; )?
//             peer
//         }};
//     }

//     // macro_rules! join {
//     //     (
//     //         $instance_id:literal,
//     //         $replicaset_id:expr,
//     //         $advertise_address:literal,
//     //         $voter:literal
//     //     ) => {
//     //         &crate::traft::TopologyRequest::Join(crate::traft::JoinRequest {
//     //             cluster_id: "cluster1".into(),
//     //             instance_id: Some($instance_id.into()),
//     //             replicaset_id: $replicaset_id.map(|v: &str| v.into()),
//     //             advertise_address: $advertise_address.into(),
//     //             voter: $voter,
//     //         })
//     //     };
//     // }

//     // macro_rules! deactivate {
//     //     ($instance_id:literal) => {
//     //         &crate::traft::TopologyRequest::Deactivate(crate::traft::DeactivateRequest {
//     //             instance_id: $instance_id.into(),
//     //             cluster_id: "cluster1".into(),
//     //         })
//     //     };
//     // }

//     // macro_rules! test_reqs {
//     //     (
//     //         replication_factor: $replication_factor:literal,
//     //         init: $peers:expr,
//     //         req: [ $( $req:expr ),* $(,)?],
//     //         expected_diff: $expected:expr,
//     //         $( expected_to_replace: $expected_to_replace:expr, )?
//     //     ) => {
//     //         let mut t = Topology::from_peers($peers)
//     //             .with_replication_factor($replication_factor);
//     //         $( t.process($req).unwrap(); )*

//     //         pretty_assertions::assert_eq!(t.diff(), $expected);
//     //         $( pretty_assertions::assert_eq!(t.to_replace(), $expected_to_replace); )?
//     //     };
//     // }

//     #[test]
//     fn test_simple() {
//         let mut topology = Topology::from_peers(vec![]);

//         assert_eq!(
//             topology.join(None, None, "addr:1".into()).unwrap(),
//             peer!(1, "i1", "R1", "addr:1", true) // { Peer::default() }
//         )

//         // assert_eq!(
//         //     topology.join(None, Some("R2"), "addr:1").unwrap(),
//         //     peer!(1, "i1", "R1", "addr:1", true)
//         // )

//         // let peers = peers![(1, "i1", "R1", "addr:1", true)];
//         // assert_eq!(Topology::from_peers(peers).diff(), vec![]);

//         // test_reqs!(
//         //     replication_factor: 1,
//         //     init: peers![],
//         //     req: [
//         //         join!("i1", None, "nowhere", true),
//         //         join!("i2", None, "nowhere", true),
//         //     ],
//         //     expected_diff: peers![
//         //         (1, "i1", "r1", "nowhere", true),
//         //         (2, "i2", "r2", "nowhere", true),
//         //     ],
//         // );

//         // test_reqs!(
//         //     replication_factor: 1,
//         //     init: peers![
//         //         (1, "i1", "R1", "addr:1", true),
//         //     ],
//         //     req: [
//         //         join!("i2", Some("R2"), "addr:2", false),
//         //     ],
//         //     expected_diff: peers![
//         //         (2, "i2", "R2", "addr:2", false),
//         //     ],
//         // );
//     }

//     // #[test]
//     // fn test_override() {
//     //     test_reqs!(
//     //         replication_factor: 1,
//     //         init: peers![
//     //             (1, "i1", "R1", "addr:1", false),
//     //         ],
//     //         req: [
//     //             join!("i1", None, "addr:2", true),
//     //         ],
//     //         expected_diff: peers![
//     //             (1, "i1", "R1", "addr:2", true),
//     //         ],
//     //     );
//     // }

//     // #[test]
//     // fn test_batch_overlap() {
//     //     test_reqs!(
//     //         replication_factor: 1,
//     //         init: peers![],
//     //         req: [
//     //             join!("i1", Some("R1"), "addr:1", false),
//     //             join!("i1", None, "addr:2", true),
//     //         ],
//     //         expected_diff: peers![
//     //             (1, "i1", "R1", "addr:2", true),
//     //         ],
//     //     );
//     // }

//     // #[test]
//     // fn test_replicaset_mismatch() {
//     //     let expected_error = concat!(
//     //         "i3 already joined with a different replicaset_id,",
//     //         " requested: R-B,",
//     //         " existing: R-A.",
//     //     );

//     //     let peers = peers![(3, "i3", "R-A", "x:1", false)];
//     //     let mut topology = Topology::from_peers(peers);
//     //     topology
//     //         .process(join!("i3", Some("R-B"), "x:2", true))
//     //         .map_err(|e| assert_eq!(e, expected_error))
//     //         .unwrap_err();
//     //     assert_eq!(topology.diff(), vec![]);
//     //     assert_eq!(topology.to_replace(), vec![]);

//     //     let peers = peers![(2, "i2", "R-A", "nowhere", true)];
//     //     let mut topology = Topology::from_peers(peers);
//     //     topology
//     //         .process(join!("i3", Some("R-A"), "y:1", false))
//     //         .unwrap();
//     //     topology
//     //         .process(join!("i3", Some("R-B"), "y:2", true))
//     //         .map_err(|e| assert_eq!(e, expected_error))
//     //         .unwrap_err();
//     //     assert_eq!(topology.diff(), peers![(3, "i3", "R-A", "y:1", false)]);
//     //     assert_eq!(topology.to_replace(), vec![]);
//     // }

//     // #[test]
//     // fn test_replication_factor() {
//     //     test_reqs!(
//     //         replication_factor: 2,
//     //         init: peers![
//     //             (9, "i9", "r9", "nowhere", false),
//     //             (10, "i9", "r9", "nowhere", false),
//     //         ],
//     //         req: [
//     //             join!("i1", None, "addr:1", true),
//     //             join!("i2", None, "addr:2", false),
//     //             join!("i3", None, "addr:3", false),
//     //             join!("i4", None, "addr:4", false),
//     //         ],
//     //         expected_diff: peers![
//     //             (11, "i1", "r1", "addr:1", true),
//     //             (12, "i2", "r1", "addr:2", false),
//     //             (13, "i3", "r2", "addr:3", false),
//     //             (14, "i4", "r2", "addr:4", false),
//     //         ],
//     //     );
//     // }

//     // #[test]
//     // fn test_replace() {
//     //     test_reqs!(
//     //         replication_factor: 2,
//     //         init: peers![
//     //             (1, "i1", "r1", "nowhere", false),
//     //         ],
//     //         req: [
//     //             join!("i1", None, "addr:2", false),
//     //         ],
//     //         expected_diff: peers![],
//     //         expected_to_replace: vec![
//     //             (1, peer!(2, "i1", "r1", "addr:2", false)),
//     //         ],
//     //     );
//     // }

//     // #[test]
//     // fn test_deactivation() {
//     //     test_reqs!(
//     //         replication_factor: 1,
//     //         init: peers![
//     //             (1, "deactivate", "r1", "nowhere", true, true),
//     //             (2, "activate_learner", "r2", "nowhere", false, false),
//     //             (3, "activate_voter", "r3", "nowhere", false, false),
//     //         ],
//     //         req: [
//     //             deactivate!("deactivate"),
//     //             join!("activate_learner", None, "nowhere", false),
//     //             join!("activate_voter", None, "nowhere", true),
//     //         ],
//     //         expected_diff: peers![
//     //             (1, "deactivate", "r1", "nowhere", false, false),
//     //             (3, "activate_voter", "r3", "nowhere", true, true),
//     //         ],
//     //         expected_to_replace: vec![
//     //             (2, peer!(4, "activate_learner", "r2", "nowhere", false, true)),
//     //         ],
//     //     );

//     //     test_reqs!(
//     //         replication_factor: 1,
//     //         init: peers![],
//     //         req: [
//     //             join!("deactivate", Some("r1"), "nowhere", true),
//     //             deactivate!("deactivate"),
//     //             deactivate!("deactivate"),
//     //             deactivate!("deactivate"),
//     //         ],
//     //         expected_diff: peers![
//     //             (1, "deactivate", "r1", "nowhere", false, false),
//     //         ],
//     //     );
//     // }
// }
