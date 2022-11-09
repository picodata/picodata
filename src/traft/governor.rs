use ::raft::prelude as raft;

use std::collections::HashMap;
use std::collections::HashSet;

use crate::traft::failover;
use crate::traft::Peer;
use crate::traft::RaftId;

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
    peers: &[Peer],
    voters: &[RaftId],
    learners: &[RaftId],
) -> Option<raft::ConfChangeV2> {
    let voters: HashSet<RaftId> = voters.iter().cloned().collect();
    let learners: HashSet<RaftId> = learners.iter().cloned().collect();

    let peer_is_active: HashMap<RaftId, bool> = peers
        .iter()
        .map(|peer| (peer.raft_id, peer.is_online()))
        .collect();

    let (active_voters, to_demote): (Vec<RaftId>, Vec<RaftId>) = voters
        .iter()
        .partition(|id| peer_is_active.get(id).copied().unwrap_or(false));

    let active_learners: Vec<RaftId> = learners
        .iter()
        .copied()
        .filter(|id| peer_is_active.get(id).copied().unwrap_or(false))
        .collect();

    let new_peers: Vec<RaftId> = peer_is_active
        .iter()
        .map(|(&id, _)| id)
        .filter(|id| !voters.contains(id) && !learners.contains(id))
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

    // for the sake of test stability
    changes.sort_by(|l, r| Ord::cmp(&l.node_id, &r.node_id));

    let conf_change = raft::ConfChangeV2 {
        transition: raft::ConfChangeTransition::Auto,
        changes: changes.into(),
        ..Default::default()
    };

    Some(conf_change)
}

#[cfg(test)]
mod tests {
    use ::raft::prelude as raft;

    use super::raft_conf_change as cc;
    use crate::traft;

    macro_rules! p {
        (
            $raft_id:literal,
            $current_grade:ident ->
            $target_grade:ident
        ) => {
            traft::Peer {
                raft_id: $raft_id,
                current_grade: traft::CurrentGrade::$current_grade,
                target_grade: traft::TargetGrade::$target_grade,
                ..traft::Peer::default()
            }
        };

        (
            $raft_id:literal,
            $grade:ident
        ) => {
            p!($raft_id, $grade -> $grade)
        };
    }

    macro_rules! cc {
        [$(
            $change:ident($raft_id:literal)
        ),*] => {{
            Some(raft::ConfChangeV2 {
                changes: vec![$(
                    raft::ConfChangeSingle {
                        change_type: raft::ConfChangeType::$change,
                        node_id: $raft_id,
                        ..Default::default()
                    }
                ),*].into(),
                transition: raft::ConfChangeTransition::Auto,
                ..Default::default()
            })
        }};
    }

    #[test]
    fn conf_change() {
        let p1 = || p!(1, Online);
        let p2 = || p!(2, Online);
        let p3 = || p!(3, Online);
        let p4 = || p!(4, Online);
        let p5 = || p!(5, Online);

        assert_eq!(
            cc(&[p1(), p!(2, Offline)], &[1], &[]),
            // FIXME
            // cc![AddLearnerNode(2)]
            cc![AddNode(2)]
        );

        assert_eq!(
            cc(&[p1(), p!(2, Offline -> Online)], &[1], &[]),
            // FIXME
            // cc![AddLearnerNode(2)]
            cc![AddNode(2)]
        );

        assert_eq!(
            cc(&[p1(), p!(2, RaftSynced -> Online)], &[1], &[2]),
            // nothing to do until p2 attains current_grade online
            None
        );

        assert_eq!(
            cc(&[p1(), p!(2, Online)], &[1], &[2]),
            // promote p2 as soon as it attains current_grade online
            cc![AddNode(2)]
        );

        assert_eq!(
            cc(&[p1(), p!(2, Online -> Offline)], &[1, 2], &[]),
            // don't reduce total voters number
            None
        );

        assert_eq!(
            cc(&[p1(), p!(2, Replicated -> Offline)], &[1], &[2]),
            // p2 went offline even before being promoted.
            None
        );

        assert_eq!(
            cc(&[p1(), p2(), p3(), p4()], &[1, 2, 3], &[4]),
            // 4 instances -> 3 voters
            None
        );

        assert_eq!(
            cc(&[p1(), p2(), p3(), p4(), p5()], &[1, 2, 3], &[4, 5]),
            // 5 and more instances -> 5 voters
            cc![AddNode(4), AddNode(5)]
        );

        assert_eq!(
            cc(
                &[p1(), p2(), p!(3, Online -> Offline), p4()],
                &[1, 2, 3],
                &[4]
            ),
            // FIXME
            // failover a voter
            // cc![AddLearnerNode(3), AddNode(4)]
            None
        );

        assert_eq!(
            cc(
                &[p1(), p2(), p3(), p4(), p5(), p!(6, Online -> Offline)],
                &[1, 2, 3, 4, 5],
                &[6]
            ),
            None
        );

        assert_eq!(
            cc(
                &[
                    p!(1, Online -> Offline),
                    p!(2, Online -> Offline),
                    p!(3, Online -> Offline),
                    p!(4, Online -> Offline),
                    p!(5, Online -> Offline)
                ],
                &[1, 2, 3, 4, 5],
                &[]
            ),
            None
        );

        assert_eq!(
            cc(&[p1()], &[1, 99], &[]),
            // FIXME
            // Unknown voters should be removed
            // cc![RemoveNode(99)]
            cc![AddLearnerNode(99)]
        );

        assert_eq!(
            cc(&[p1()], &[1], &[99]),
            // FIXME
            // Unknown learners are removed as well
            // cc![RemoveNode(99)]
            None
        );

        assert_eq!(
            cc(&[p1(), p!(2, Online -> Expelled)], &[1, 2], &[]),
            // FIXME
            // Expelled voters should be removed
            // cc![RemoveNode(2)]
            None
        );

        assert_eq!(
            cc(&[p1(), p!(2, Offline -> Expelled)], &[1], &[2]),
            // FIXME
            // Expelled learners are removed too
            // cc![RemoveNode(2)]
            None
        );

        assert_eq!(
            cc(
                &[p1(), p2(), p3(), p4(), p!(5, Online -> Expelled)],
                &[1, 2, 3, 4, 5],
                &[]
            ),
            // FIXME
            // Tricky case.
            // When one of five voters is expelled,
            // only 3 voters should remain there.
            // cc![AddLearnerNode(4), RemoveNode(5)]
            None
        );

        assert_eq!(
            cc(
                &[p1(), p!(2, Online -> Offline), p!(3, RaftSynced -> Online)],
                &[1, 2],
                &[3]
            ),
            // Tricky case.
            // Voter p2 goes offline, but there's no replacement.
            None
        );
    }
}
