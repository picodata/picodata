use ::raft::prelude as raft;
use ::raft::prelude::ConfChangeType::*;

use std::collections::{BTreeMap, BTreeSet};

use crate::has_grades;
use crate::instance::grade::TargetGradeVariant;
use crate::instance::Instance;
use crate::traft::{Distance, RaftId};

struct RaftConf<'a> {
    all: BTreeMap<RaftId, &'a Instance>,
    voters: BTreeSet<RaftId>,
    learners: BTreeSet<RaftId>,
}

impl<'a> RaftConf<'a> {
    fn change_single(
        &mut self,
        change_type: raft::ConfChangeType,
        node_id: RaftId,
    ) -> raft::ConfChangeSingle {
        // Find the reference at
        // https://github.com/tikv/raft-rs/blob/v0.6.0/src/confchange/changer.rs#L162

        match change_type {
            AddNode => {
                self.voters.insert(node_id);
                self.learners.remove(&node_id);
            }
            AddLearnerNode => {
                self.voters.remove(&node_id);
                self.learners.insert(node_id);
            }
            RemoveNode => {
                self.voters.remove(&node_id);
                self.learners.remove(&node_id);
            }
        }

        raft::ConfChangeSingle {
            change_type,
            node_id,
            ..Default::default()
        }
    }
}

/// Sum of failure domain distances between `a` and each of `bs`.
fn sum_distance(all: &BTreeMap<RaftId, &Instance>, a: &RaftId, bs: &BTreeSet<RaftId>) -> Distance {
    let Some(a) = all.get(a) else { return 0 };
    bs.iter()
        .filter_map(|raft_id| all.get(raft_id))
        .map(|b| b.failure_domain.distance(&a.failure_domain))
        .sum()
}

/// Among `candidates` find one with maximum total distance to `voters`.
fn find_farthest(
    all: &BTreeMap<RaftId, &Instance>,
    voters: &BTreeSet<RaftId>,
    candidates: &BTreeSet<RaftId>,
) -> Option<(RaftId, Distance)> {
    candidates
        .iter()
        .filter(|&&raft_id| !voters.contains(&raft_id))
        .map(|&raft_id| (raft_id, sum_distance(all, &raft_id, voters)))
        .reduce(|acc, item| if item.1 > acc.1 { item } else { acc })
}

pub(crate) fn raft_conf_change(
    instances: &[Instance],
    voters: &[RaftId],
    learners: &[RaftId],
) -> Option<raft::ConfChangeV2> {
    let mut raft_conf = RaftConf {
        all: instances.iter().map(|p| (p.raft_id, p)).collect(),
        voters: voters.iter().cloned().collect(),
        learners: learners.iter().cloned().collect(),
    };
    let mut changes: Vec<raft::ConfChangeSingle> = vec![];

    let not_expelled = |instance: &&Instance| has_grades!(instance, * -> not Expelled);

    let cluster_size = instances.iter().filter(not_expelled).count();
    let voters_needed = match cluster_size {
        // five and more nodes -> 5 voters
        5.. => 5,
        // three or four nodes -> 3 voters
        3..=4 => 3,
        // two nodes -> 2 voters
        // one node -> 1 voter
        // zero nodes -> 0 voters (almost unreachable)
        x => x,
    };

    // Remove / replace voters
    let mut next_voters: BTreeSet<&RaftId> = BTreeSet::new();
    for voter_id in raft_conf.voters.clone().iter() {
        let Some(instance) = raft_conf.all.get(voter_id) else {
            // Nearly impossible, but rust forces me to check it.
            let ccs = raft_conf.change_single(RemoveNode, *voter_id);
            changes.push(ccs);
            continue;
        };
        match instance.target_grade.variant {
            TargetGradeVariant::Online => {
                // Do nothing
            }
            TargetGradeVariant::Offline => {
                // A voter goes offline. Replace it with
                // another online instance if possible.
                let Some(next_voter) = instances.iter().find(|instance| {
                    has_grades!(instance, Online -> Online)
                    && !raft_conf.voters.contains(&instance.raft_id)
                    && !next_voters.contains(&instance.raft_id)
                }) else { continue };
                next_voters.insert(&next_voter.raft_id);
                let ccs = raft_conf.change_single(AddLearnerNode, instance.raft_id);
                changes.push(ccs);
            }
            TargetGradeVariant::Expelled => {
                // Expelled instance is removed unconditionally.
                let ccs = raft_conf.change_single(RemoveNode, instance.raft_id);
                changes.push(ccs);
            }
        }
    }

    for voter_id in raft_conf.voters.clone().iter().skip(voters_needed) {
        // If threre're more voters that needed, remove excess ones.
        // That may be the case when one of 5 instances is expelled.
        let ccs = raft_conf.change_single(AddLearnerNode, *voter_id);
        changes.push(ccs);
    }

    // Remove unknown / expelled learners
    for learner_id in raft_conf.learners.clone().iter() {
        let Some(instance) = raft_conf.all.get(learner_id) else {
            // Nearly impossible, but rust forces me to check it.
            let ccs = raft_conf.change_single(RemoveNode, *learner_id);
            changes.push(ccs);
            continue;
        };
        match instance.target_grade.variant {
            TargetGradeVariant::Online | TargetGradeVariant::Offline => {
                // Do nothing
            }
            TargetGradeVariant::Expelled => {
                // Expelled instance is removed unconditionally.
                let ccs = raft_conf.change_single(RemoveNode, instance.raft_id);
                changes.push(ccs);
            }
        }
    }

    let remembered_voters = raft_conf.voters.clone();

    // Promote more voters
    let candidates: BTreeSet<_> = instances
        .iter()
        .filter(|instance| has_grades!(instance, Online -> Online))
        .map(|p| p.raft_id)
        .collect();
    while raft_conf.voters.len() < voters_needed {
        if let Some((new_voter_id, _)) =
            find_farthest(&raft_conf.all, &raft_conf.voters, &candidates)
        {
            let ccs = raft_conf.change_single(AddNode, new_voter_id);
            changes.push(ccs);
        } else {
            break;
        }
    }

    // Redistributing existing voters according to failure domains
    for voter_id in remembered_voters {
        let mut other_voters = raft_conf.voters.clone();
        other_voters.remove(&voter_id);
        if let Some((new_voter_id, new_distance)) =
            find_farthest(&raft_conf.all, &other_voters, &candidates)
        {
            if new_distance > sum_distance(&raft_conf.all, &voter_id, &other_voters) {
                let ccs1 = raft_conf.change_single(AddLearnerNode, voter_id);
                let ccs2 = raft_conf.change_single(AddNode, new_voter_id);
                changes.push(ccs1);
                changes.push(ccs2);
            }
        } else {
            break;
        }
    }

    // Promote remaining instances as learners
    for instance in instances.iter().filter(not_expelled) {
        if !raft_conf.voters.contains(&instance.raft_id)
            && !raft_conf.learners.contains(&instance.raft_id)
        {
            let ccs = raft_conf.change_single(AddLearnerNode, instance.raft_id);
            changes.push(ccs);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    use ::raft::prelude as raft;
    use std::collections::{BTreeMap, BTreeSet};

    use crate::failure_domain::FailureDomain;
    use crate::instance::grade::{CurrentGradeVariant, Grade};
    use crate::traft::RaftId;

    macro_rules! fd {
        ($(,)?) => { FailureDomain::default() };
        ($($k:tt : $v:tt),+ $(,)?) => {
            FailureDomain::from([$((stringify!($k), stringify!($v))),+])
        }
    }

    macro_rules! p {
        (
            $raft_id:literal,
            $current_grade:ident -> $target_grade:ident
            $(, $failure_domain:expr)?
        ) => {
            Instance {
                raft_id: $raft_id,
                current_grade: Grade {
                    variant: CurrentGradeVariant::$current_grade,
                    // raft_conf_change doesn't care about incarnations
                    incarnation: 0,
                },
                target_grade: Grade {
                    variant: TargetGradeVariant::$target_grade,
                    // raft_conf_change doesn't care about incarnations
                    incarnation: 0,
                },
                $(failure_domain: $failure_domain,)?
                ..Instance::default()
            }
        };

        (
            $raft_id:literal,
            $grade:ident
            $(, $failure_domain:expr)?

        ) => {
            p!($raft_id, $grade -> $grade $(, $failure_domain)?)
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

    fn cc(p: &[Instance], v: &[RaftId], l: &[RaftId]) -> Option<raft::ConfChangeV2> {
        let mut cc = super::raft_conf_change(p, v, l)?;
        cc.changes.sort_by(|l, r| Ord::cmp(&l.node_id, &r.node_id));
        Some(cc)
    }

    #[test]
    fn test_sum_distance() {
        assert_eq!(
            3,
            sum_distance(
                &BTreeMap::from([
                    (1, &p!(1, Online, fd! {x: A, y: B})),
                    (2, &p!(2, Online, fd! {x: A, y: C})),
                    (3, &p!(3, Online, fd! {x: D, y: C}))
                ]),
                &1,
                &BTreeSet::from([2, 3])
            )
        );
        assert_eq!(
            0,
            sum_distance(
                &BTreeMap::from([(1, &p!(1, Online, fd! {x: A, y: B}))]),
                &1,
                &BTreeSet::new()
            )
        );
    }

    #[test]
    fn test_find_farthest() {
        let instances = [
            p!(1, Online, fd! {dc: Msk, srv: Msk1}),
            p!(2, Online, fd! {dc: Msk, srv: Msk2}),
            p!(3, Online, fd! {dc: Msk, srv: Msk3}),
            p!(4, Online, fd! {dc: Spb, srv: Spb1}),
            p!(5, Online, fd! {dc: Spb, srv: Spb2}),
            p!(6, Online, fd! {dc: Spb, srv: Spb3}),
            p!(7, Online, fd! {dc: Arb, srv: Arb1}),
        ];
        let map: BTreeMap<RaftId, &Instance> = instances.iter().map(|p| (p.raft_id, p)).collect();
        let (found_raft_id, found_distance) = find_farthest(
            &map,
            &BTreeSet::from([7, 1]),
            &BTreeSet::from([2, 3, 4, 5, 6]),
        )
        .unwrap();
        let expected = [4, 5, 6];
        assert!(BTreeSet::<RaftId>::from(expected).contains(&found_raft_id));
        assert_eq!(4, found_distance);

        assert_eq!(
            None,
            find_farthest(&map, &BTreeSet::from([7, 1]), &BTreeSet::from([]))
        );
    }

    #[test]
    fn test_conf_change() {
        let p1 = || p!(1, Online);
        let p2 = || p!(2, Online);
        let p3 = || p!(3, Online);
        let p4 = || p!(4, Online);
        let p5 = || p!(5, Online);

        assert_eq!(
            cc(&[p1(), p!(2, Offline)], &[1], &[]),
            cc![AddLearnerNode(2)]
        );

        assert_eq!(
            cc(&[p1(), p!(2, Offline -> Online)], &[1], &[]),
            cc![AddLearnerNode(2)]
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
            // failover a voter
            cc![AddLearnerNode(3), AddNode(4)]
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
            // Unknown voters should be removed
            cc![RemoveNode(99)]
        );

        assert_eq!(
            cc(&[p1()], &[1], &[99]),
            // Unknown learners are removed as well
            cc![RemoveNode(99)]
        );

        assert_eq!(
            cc(&[p1(), p!(2, Online -> Expelled)], &[1, 2], &[]),
            // Expelled voters should be removed
            cc![RemoveNode(2)]
        );

        assert_eq!(
            cc(&[p1(), p!(2, Offline -> Expelled)], &[1], &[2]),
            // Expelled learners are removed too
            cc![RemoveNode(2)]
        );

        assert_eq!(
            cc(
                &[p1(), p2(), p3(), p4(), p!(5, Online -> Expelled)],
                &[1, 2, 3, 4, 5],
                &[]
            ),
            // Tricky case.
            // When one of five voters is expelled,
            // only 3 voters should remain there.
            cc![AddLearnerNode(4), RemoveNode(5)]
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

        assert_eq!(
            cc(
                &[
                    p!(1, Online, fd! {dc: Arb}),
                    p!(2, Online, fd! {dc: Msk}),
                    p!(3, Online, fd! {dc: Msk}),
                    p!(4, Online, fd! {dc: Msk}),
                    p!(5, Online, fd! {dc: Spb}),
                    p!(6, Offline, fd! {dc: Spb}),
                    p!(7, Online, fd! {dc: Spb}),
                ],
                &[1, 2, 3, 5],
                &[4, 7]
            ),
            // New voter should respect failure domain
            cc![AddLearnerNode(6), AddNode(7)]
        );

        assert_eq!(
            cc(
                &[
                    p!(1, Online, fd! {dc: Arb}),
                    p!(2, Online, fd! {dc: Msk}),
                    p!(3, Online, fd! {dc: Msk}),
                    p!(4, Online, fd! {dc: Msk}),
                    p!(5, Online, fd! {dc: Msk}),
                    p!(6, Online, fd! {dc: Spb}),
                    p!(7, Online, fd! {dc: Spb}),
                    p!(8, Online, fd! {dc: Spb}),
                    p!(9, Online, fd! {dc: Spb}),
                ],
                &[1, 2, 3, 4, 5],
                &[6, 7, 8, 9]
            ),
            // Existing voters should be redistributed according to failure domains
            cc![AddLearnerNode(2), AddLearnerNode(3), AddNode(6), AddNode(7)]
        );

        assert_eq!(
            cc(
                &[
                    p!(1, Online, fd! {x: _1, y: _5}),
                    p!(2, Online, fd! {x: _2, y: _2}),
                    p!(3, Online, fd! {x: _3, y: _3}),
                    p!(4, Online, fd! {x: _4, y: _4}),
                    p!(5, Online, fd! {x: _5, y: _5}),
                    p!(6, Online, fd! {x: _1, y: _2}),
                ],
                &[2, 3, 4, 5, 6],
                &[1]
            ),
            // Voter should not be replaced if candidate has same distance to other voters
            None
        );

        assert_eq!(
            cc(
                &[
                    p!(1, Offline, fd! {dc: msk}),
                    p!(2, Offline, fd! {dc: msk}),
                    p!(3, Online, fd! {dc: spb}),
                    p!(4, Online, fd! {dc: spb}),
                ],
                &[1, 2, 3],
                &[4]
            ),
            // Vouters number should not fall below the optimal number
            cc![AddLearnerNode(1), AddNode(4)]
        );
    }
}
