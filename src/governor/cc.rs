use ::raft::prelude as raft;
use ::raft::prelude::ConfChangeType::*;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::has_grades;
use crate::instance::grade::TargetGradeVariant;
use crate::instance::Instance;
use crate::traft::RaftId;

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
                let Some(replacement) = instances.iter().find(|instance| {
                    has_grades!(instance, Online -> Online)
                    && !raft_conf.voters.contains(&instance.raft_id)
                }) else { continue };

                let ccs1 = raft_conf.change_single(AddLearnerNode, instance.raft_id);
                let ccs2 = raft_conf.change_single(AddNode, replacement.raft_id);
                changes.extend_from_slice(&[ccs1, ccs2]);
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

    // Promote more voters
    for instance in instances
        .iter()
        .filter(|instance| has_grades!(instance, Online -> Online))
    {
        if raft_conf.voters.len() >= voters_needed {
            break;
        }

        if !raft_conf.voters.contains(&instance.raft_id) {
            let ccs = raft_conf.change_single(AddNode, instance.raft_id);
            changes.push(ccs);
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

    use crate::instance::grade::{CurrentGradeVariant, Grade};
    use crate::traft::RaftId;

    macro_rules! p {
        (
            $raft_id:literal,
            $current_grade:ident ->
            $target_grade:ident
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
                ..Instance::default()
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

    fn cc(p: &[Instance], v: &[RaftId], l: &[RaftId]) -> Option<raft::ConfChangeV2> {
        let mut cc = super::raft_conf_change(p, v, l)?;
        cc.changes.sort_by(|l, r| Ord::cmp(&l.node_id, &r.node_id));
        Some(cc)
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
    }
}
