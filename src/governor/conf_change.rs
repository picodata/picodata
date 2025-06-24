use crate::instance::Instance;
use crate::tier::Tier;
use crate::traft::{Distance, RaftId};
use crate::{has_states, tlog};
use ::raft::prelude as raft;
use ::raft::prelude::ConfChangeType::*;
use std::collections::HashMap;
// TODO: do not use BTreeSet, it's very bad for performance.
// Instead we should use a sorted array. All operations will be faster because
// of cache locality. Inserting into/removing from a sorted array is simple:
// find the location using binary search and shift the tail of the array.
// For our purposes this is going to be much more performant that the binary tree,
// because there will be dramatically fewer memory allocations and cache misses.
use std::collections::{BTreeMap, BTreeSet};

struct RaftConf<'a> {
    all: BTreeMap<RaftId, &'a Instance>,
    voters: BTreeSet<RaftId>,
    learners: BTreeSet<RaftId>,
}

impl RaftConf<'_> {
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
            change_type: change_type.into(),
            node_id,
            ..Default::default()
        }
    }
}

/// Sum of distances between failure domains of `a` and each of `bs`.
fn sum_distance(all: &BTreeMap<RaftId, &Instance>, bs: &BTreeSet<RaftId>, a: &RaftId) -> Distance {
    let Some(a) = all.get(a) else { return 0 };
    bs.iter()
        .filter_map(|raft_id| all.get(raft_id))
        .map(|b| b.failure_domain.distance(&a.failure_domain))
        .sum()
}

/// Among `candidates` finds one with maximum total distance to `voters`.
///
/// Returns its `RaftId` and the calculated distance.
/// Returns `None` if `candidates` set is empty.
fn find_farthest(
    all: &BTreeMap<RaftId, &Instance>,
    voters: &BTreeSet<RaftId>,
    candidates: &BTreeSet<RaftId>,
) -> Option<(RaftId, Distance)> {
    candidates
        .iter()
        .map(|&raft_id| (raft_id, sum_distance(all, voters, &raft_id)))
        .reduce(|acc, item| if item.1 > acc.1 { item } else { acc })
}

pub(crate) fn raft_conf_change(
    instances: &[Instance],
    voters: &[RaftId],
    learners: &[RaftId],
    tiers: &HashMap<&str, &Tier>,
) -> Option<raft::ConfChangeV2> {
    let mut raft_conf = RaftConf {
        all: instances.iter().map(|p| (p.raft_id, p)).collect(),
        voters: voters.iter().cloned().collect(),
        learners: learners.iter().cloned().collect(),
    };
    let mut changes: Vec<raft::ConfChangeSingle> = vec![];

    // Only an instance from tier with `can_vote = true` can be considered as voter.
    let tier_can_vote = |instance: &&Instance| {
        tiers
            .get(instance.tier.as_str())
            .expect("tier for instance should exists")
            .can_vote
    };

    let number_of_eligible_for_vote = instances
        .iter()
        .filter(|instance| has_states!(instance, * -> not Expelled))
        .filter(tier_can_vote)
        .count();

    let voters_needed = match number_of_eligible_for_vote {
        5.. => 5,
        3..=4 => 3,
        x => x,
    };

    // A list of instances that can be safely promoted to voters.
    let mut promotable: BTreeSet<RaftId> = instances
        .iter()
        // Only an instance with current state online can be promoted
        .filter(|instance| has_states!(instance, Online -> Online))
        .filter(tier_can_vote)
        .map(|instance| instance.raft_id)
        // Exclude those who is already a voter.
        .filter(|raft_id| !raft_conf.voters.contains(raft_id))
        .collect();

    let next_farthest = |raft_conf: &RaftConf, promotable: &BTreeSet<RaftId>| {
        find_farthest(&raft_conf.all, &raft_conf.voters, promotable)
    };

    // Remove / replace voters
    for voter_id in raft_conf.voters.clone().iter() {
        let Some(instance) = raft_conf.all.get(voter_id) else {
            // unknown instance which is not in configuration of cluster, nearly impossible
            #[rustfmt::skip]
            tlog!(Warning, "no info found for instance which is a raft voter: raft_id = {voter_id}");
            let ccs = raft_conf.change_single(RemoveNode, *voter_id);
            changes.push(ccs);
            continue;
        };

        if !tier_can_vote(instance) {
            // case when bootstrap leader from tier with `can_vote = false`
            #[rustfmt::skip]
            tlog!(Warning, "instance with name '{}' from tier '{}' with 'can_vote' = false is in raft configuration as voter, making it follower",
                  instance.name, instance.tier);
            let ccs = raft_conf.change_single(RemoveNode, *voter_id);
            changes.push(ccs);
            continue;
        }

        if has_states!(instance, Expelled -> *) {
            // Instance was already expelled, remove it entirely.
            let ccs = raft_conf.change_single(RemoveNode, instance.raft_id);
            changes.push(ccs);
            continue;
        }

        if has_states!(instance, * -> Offline) || has_states!(instance, * -> Expelled) {
            // A voter is shutting down or getting expelled.
            // First try to replace it with another online instance.
            if let Some((next_voter_id, _)) = next_farthest(&raft_conf, &promotable) {
                let ccs1 = raft_conf.change_single(AddLearnerNode, instance.raft_id);
                let ccs2 = raft_conf.change_single(AddNode, next_voter_id);
                changes.extend_from_slice(&[ccs1, ccs2]);
                promotable.remove(&next_voter_id);
                continue;
            }

            if has_states!(instance, * -> Expelled) {
                // If no replacement found, but instance is getting expelled, demote it anyway
                let ccs = raft_conf.change_single(AddLearnerNode, instance.raft_id);
                changes.push(ccs);
                continue;
            }

            let have_excess_voters = raft_conf.voters.len() > voters_needed;
            if have_excess_voters && has_states!(instance, * -> Offline) {
                // If no replacement found, but there's already an excess in
                // voters, the Offline instance should be the one being demoted
                let ccs = raft_conf.change_single(AddLearnerNode, instance.raft_id);
                changes.push(ccs);
                continue;
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
            // unknown instance which is not in configuration of cluster, nearly impossible
            #[rustfmt::skip]
            tlog!(Warning, "no info found for instance which is a raft voter: raft_id = {learner_id}");
            let ccs = raft_conf.change_single(RemoveNode, *learner_id);
            changes.push(ccs);
            continue;
        };

        if has_states!(instance, Expelled -> *) {
            // Instance was already expelled => remove it unconditionally.
            let ccs = raft_conf.change_single(RemoveNode, instance.raft_id);
            changes.push(ccs);
        }
    }

    // Promote more voters
    while raft_conf.voters.len() < voters_needed {
        let Some((new_voter_id, _)) = next_farthest(&raft_conf, &promotable) else {
            break;
        };
        let ccs = raft_conf.change_single(AddNode, new_voter_id);
        changes.push(ccs);
        promotable.remove(&new_voter_id);
    }

    // Redistribute existing voters according to their failure domains
    for voter_id in raft_conf.voters.clone().iter() {
        let mut other_voters = raft_conf.voters.clone();
        other_voters.remove(voter_id);
        let other_voters = other_voters;

        let Some((new_voter_id, new_distance)) =
            find_farthest(&raft_conf.all, &other_voters, &promotable)
        else {
            break;
        };

        if new_distance > sum_distance(&raft_conf.all, &other_voters, voter_id) {
            let ccs1 = raft_conf.change_single(AddLearnerNode, *voter_id);
            let ccs2 = raft_conf.change_single(AddNode, new_voter_id);
            changes.extend_from_slice(&[ccs1, ccs2]);
            promotable.remove(&new_voter_id);
        }
    }

    // Promote remaining instances as learners
    for instance in instances {
        if has_states!(instance, Expelled -> *)
            || raft_conf.voters.contains(&instance.raft_id)
            || raft_conf.learners.contains(&instance.raft_id)
        {
            continue;
        }
        // Non-expelled instance which is neither a learner nor a voter.
        // It should become a learner.
        let ccs = raft_conf.change_single(AddLearnerNode, instance.raft_id);
        changes.push(ccs);
    }

    if changes.is_empty() {
        return None;
    }

    let conf_change = raft::ConfChangeV2 {
        transition: raft::ConfChangeTransition::Auto.into(),
        changes,
        ..Default::default()
    };

    Some(conf_change)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failure_domain::FailureDomain;
    use crate::instance::State;
    use crate::instance::StateVariant::*;
    use crate::traft::RaftId;
    use ::raft::prelude as raft;
    use std::collections::{BTreeMap, BTreeSet};

    macro_rules! fd {
        ($(,)?) => { FailureDomain::default() };
        ($($k:tt : $v:tt),+ $(,)?) => {
            FailureDomain::from([$((stringify!($k), stringify!($v))),+])
        }
    }

    macro_rules! p {
        (
            $raft_id:literal,
            $current_state:ident -> $target_state:ident
            $(, $failure_domain:expr)?
        ) => {
            Instance {
                raft_id: $raft_id,
                current_state: State {
                    variant: $current_state,
                    // raft_conf_change doesn't care about incarnations
                    incarnation: 0,
                },
                target_state: State {
                    variant: $target_state,
                    // raft_conf_change doesn't care about incarnations
                    incarnation: 0,
                },
                tier: crate::tier::DEFAULT_TIER.into(),
                $(failure_domain: $failure_domain,)?
                ..Instance::default()
            }
        };

        (
            $raft_id:literal,
            $state:ident
            $(, $failure_domain:expr)?

        ) => {
            p!($raft_id, $state -> $state $(, $failure_domain)?)
        };
    }

    macro_rules! p_with_tier {
        (
            $raft_id:literal,
            $tier:ident,
            $state:ident

        ) => {{
            let mut instance = p!($raft_id, $state -> $state);
            instance.tier = $tier.to_string();
            instance
        }}
    }

    macro_rules! cc {
        [$(
            $change:ident($raft_id:literal)
        ),*] => {{
            Some(raft::ConfChangeV2 {
                changes: vec![$(
                    raft::ConfChangeSingle {
                        change_type: raft::ConfChangeType::$change.into(),
                        node_id: $raft_id,
                        ..Default::default()
                    }
                ),*].into(),
                transition: raft::ConfChangeTransition::Auto.into(),
                ..Default::default()
            })
        }};
    }

    fn cc(
        all_instances: &[Instance],
        voter_ids: &[RaftId],
        learner_ids: &[RaftId],
    ) -> Option<raft::ConfChangeV2> {
        let mut all_tiers = HashMap::new();
        let default_tier = Tier::default();
        all_tiers.insert(default_tier.name.as_str(), &default_tier);
        cc_with_tiers(all_instances, voter_ids, learner_ids, &all_tiers)
    }

    fn cc_with_tiers(
        all_instances: &[Instance],
        voter_ids: &[RaftId],
        learner_ids: &[RaftId],
        all_tiers: &HashMap<&str, &Tier>,
    ) -> Option<raft::ConfChangeV2> {
        let mut cc = super::raft_conf_change(all_instances, voter_ids, learner_ids, all_tiers)?;
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
                &BTreeSet::from([2, 3]),
                &1,
            )
        );
        assert_eq!(
            0,
            sum_distance(
                &BTreeMap::from([(1, &p!(1, Online, fd! {x: A, y: B}))]),
                &BTreeSet::new(),
                &1,
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
            cc(&[p1(), p!(2, Offline -> Online)], &[1], &[2]),
            // nothing to do until p2 attains current_state online
            None
        );

        assert_eq!(
            cc(&[p1(), p!(2, Online)], &[1], &[2]),
            // promote p2 as soon as it attains current_state online
            cc![AddNode(2)]
        );

        assert_eq!(
            cc(&[p1(), p!(2, Online -> Offline)], &[1, 2], &[]),
            // don't reduce total voters number
            None
        );

        assert_eq!(
            cc(&[p1(), p!(2, Online -> Offline)], &[1], &[2]),
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
            // If a voters starts getting expelled, it becomes a learner
            cc![AddLearnerNode(2)]
        );

        assert_eq!(
            cc(&[p1(), p!(2, Online -> Expelled)], &[1], &[2]),
            // If a learner starts getting expelled, nothing happens
            None
        );

        assert_eq!(
            cc(&[p1(), p!(2, Expelled -> Expelled)], &[1, 2], &[]),
            // Expelled voters should be removed
            cc![RemoveNode(2)]
        );

        assert_eq!(
            cc(&[p1(), p!(2, Expelled -> Expelled)], &[1], &[2]),
            // Expelled learners are removed too
            cc![RemoveNode(2)]
        );

        assert_eq!(
            cc(
                &[p1(), p2(), p3(), p4(), p!(5, Expelled -> Expelled)],
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
                &[p1(), p!(2, Online -> Offline), p!(3, Offline -> Online)],
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

        assert_eq!(
            cc(
                &[
                    p!(1, Online),
                    p!(2, Online -> Expelled),
                    p!(3, Online -> Offline),
                    p!(4, Online),
                    p!(5, Online),
                ],
                &[1, 2, 3, 4, 5],
                &[]
            ),
            // One instance is getting expelled, now total voter count is 3, and
            // we demote the 2 degenerate instances
            cc![AddLearnerNode(2), AddLearnerNode(3)]
        );

        let mut tiers = HashMap::new();
        const VOTER_TIER_NAME: &str = "voter";
        const FOLLOWER_TIER_NAME: &str = "follower";
        let voter_tier = Tier {
            name: VOTER_TIER_NAME.into(),
            replication_factor: 1,
            can_vote: true,
            ..Default::default()
        };
        let router_tier = Tier {
            name: FOLLOWER_TIER_NAME.into(),
            replication_factor: 1,
            can_vote: false,
            ..Default::default()
        };
        tiers.insert(voter_tier.name.as_str(), &voter_tier);
        tiers.insert(router_tier.name.as_str(), &router_tier);

        assert_eq!(
            cc_with_tiers(
                &[
                    p_with_tier!(1, VOTER_TIER_NAME, Online),
                    p_with_tier!(2, FOLLOWER_TIER_NAME, Online),
                ],
                &[1],
                &[2],
                &tiers
            ),
            // instance from router tier couldn't be upgraded to voter
            None
        );

        assert_eq!(
            cc_with_tiers(
                &[
                    p_with_tier!(1, VOTER_TIER_NAME, Online),
                    p_with_tier!(2, FOLLOWER_TIER_NAME, Online),
                ],
                &[2],
                &[1],
                &tiers
            ),
            // case when bootstrap leader from tier with falsy `can_vote`
            cc![AddNode(1), RemoveNode(2), AddLearnerNode(2)]
        );

        assert_eq!(
            cc_with_tiers(
                &[
                    p_with_tier!(1, VOTER_TIER_NAME, Expelled),
                    p_with_tier!(2, FOLLOWER_TIER_NAME, Online),
                ],
                &[1],
                &[2],
                &tiers
            ),
            // remove expelled voter
            cc![RemoveNode(1)]
        );

        assert_eq!(
            cc_with_tiers(
                &[
                    p_with_tier!(1, FOLLOWER_TIER_NAME, Expelled),
                    p_with_tier!(2, FOLLOWER_TIER_NAME, Online),
                ],
                &[1],
                &[2],
                &tiers
            ),
            // remove expelled follower
            cc![RemoveNode(1)]
        );

        assert_eq!(
            cc_with_tiers(
                &[
                    p_with_tier!(1, VOTER_TIER_NAME, Online),
                    p_with_tier!(2, VOTER_TIER_NAME, Online),
                ],
                &[],
                &[1, 2],
                &tiers
            ),
            cc![AddNode(1), AddNode(2)]
        );
    }
}
