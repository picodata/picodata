use ::raft::prelude as raft;

use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::FromIterator as _;

use crate::traft::failover;
use crate::traft::Peer;
use crate::traft::RaftId;
use crate::traft::RaftSpaceAccess;

fn conf_change_single(node_id: RaftId, is_voter: bool) -> raft::ConfChangeSingle {
    let change_type = if is_voter {
        raft::ConfChangeType::AddNode
    } else {
        raft::ConfChangeType::AddLearnerNode
    };
    raft::ConfChangeSingle {
        change_type,
        node_id,
        ..Default::default()
    }
}

pub(crate) fn raft_conf_change(
    storage: &RaftSpaceAccess,
    peers: &[Peer],
) -> Option<raft::ConfChangeV2> {
    let voter_ids: HashSet<RaftId> =
        HashSet::from_iter(storage.voters().unwrap().unwrap_or_default());
    let learner_ids: HashSet<RaftId> =
        HashSet::from_iter(storage.learners().unwrap().unwrap_or_default());
    let peer_is_active: HashMap<RaftId, bool> = peers
        .iter()
        .map(|peer| (peer.raft_id, peer.is_online()))
        .collect();

    let (active_voters, to_demote): (Vec<RaftId>, Vec<RaftId>) = voter_ids
        .iter()
        .partition(|id| peer_is_active.get(id).copied().unwrap_or(false));

    let active_learners: Vec<RaftId> = learner_ids
        .iter()
        .copied()
        .filter(|id| peer_is_active.get(id).copied().unwrap_or(false))
        .collect();

    let new_peers: Vec<RaftId> = peer_is_active
        .iter()
        .map(|(&id, _)| id)
        .filter(|id| !voter_ids.contains(id) && !learner_ids.contains(id))
        .collect();

    let mut changes: Vec<raft::ConfChangeSingle> = Vec::new();

    const VOTER: bool = true;
    const LEARNER: bool = false;

    changes.extend(
        to_demote
            .into_iter()
            .map(|id| conf_change_single(id, LEARNER)),
    );

    let total_active = active_voters.len() + active_learners.len() + new_peers.len();

    if total_active == 0 {
        return None;
    }

    let new_peers_to_promote;
    match failover::voters_needed(active_voters.len(), total_active) {
        0 => {
            new_peers_to_promote = 0;
        }
        pos @ 1..=i64::MAX => {
            let pos = pos as usize;
            if pos < active_learners.len() {
                for &raft_id in &active_learners[0..pos] {
                    changes.push(conf_change_single(raft_id, VOTER))
                }
                new_peers_to_promote = 0;
            } else {
                for &raft_id in &active_learners {
                    changes.push(conf_change_single(raft_id, VOTER))
                }
                new_peers_to_promote = pos - active_learners.len();
                assert!(new_peers_to_promote <= new_peers.len());
                for &raft_id in &new_peers[0..new_peers_to_promote] {
                    changes.push(conf_change_single(raft_id, VOTER))
                }
            }
        }
        neg @ i64::MIN..=-1 => {
            let neg = -neg as usize;
            assert!(neg < active_voters.len());
            for &raft_id in &active_voters[0..neg] {
                changes.push(conf_change_single(raft_id, LEARNER))
            }
            new_peers_to_promote = 0;
        }
    }

    for &raft_id in &new_peers[new_peers_to_promote..] {
        changes.push(conf_change_single(raft_id, LEARNER))
    }

    if changes.is_empty() {
        return None;
    }

    let conf_change = raft::ConfChangeV2 {
        transition: raft::ConfChangeTransition::Auto,
        changes: changes.into(),
        ..Default::default()
    };

    Some(conf_change)
}
