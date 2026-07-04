//! Derive `MotionPolicy` for a join's inner child via [`EqualityFacts`].
//!
//! Decision priority:
//!
//! 1. `bucket_id` ↔ `bucket_id` (or `bucket_id` ↔ const) co-location detected
//!    via classes — strongest signal, independent of distribution keys.
//! 2. Outer's distribution key positionally matches some inner's distribution
//!    key via classes — both sides already on the same bucket. A class match
//!    accounts for types: the analyzer unions only same-`UnrestrictedType`
//!    slots.
//! 3. Outer's distribution key has class-matching inner positions — repartition
//!    inner by those positions.
//! 4. Otherwise — `Full`.
//!
//! Constants are folded into the same logic for free: when both sides equal
//! the same literal, the analyzer puts them in one class, so the positional
//! match in step 2/3 just works.
//!
//! Inner and `LEFT OUTER` joins are both handled. For inner joins the global
//! equality graph already carries all ON-condition equalities. For left
//! joins the ON condition is unsafe to apply globally (NULL-extension), so
//! the analyzer stashes those equalities into a per-join scope; this module
//! queries that scope by passing the join's `NodeId` as `at_rel` to the
//! adapter-style API ([`EqualityFacts::are_equal`],
//! [`EqualityFacts::find_equal_position`]).

use crate::errors::{Entity, SbroadError};
use crate::ir::distribution::{Distribution, Key};
use crate::ir::node::relational::Relational;
use crate::ir::node::{Join, NodeId};
use crate::ir::transformation::equality_facts::{EqualityFacts, Slot};
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::Plan;
use smol_str::format_smolstr;

impl Plan {
    /// Decide a motion policy for the inner child of a join using equality
    /// facts.  Works for both `Inner` and `LeftOuter` joins: the global
    /// equality graph carries inner-join ON-condition facts, and the
    /// per-join scope (built by the analyzer for left joins) carries the
    /// left join's cross-side equalities — both are consulted by passing
    /// `join_id` as the scope context to the adapter API.
    pub fn motion_policy_for_inner_via_facts(
        &self,
        join_id: NodeId,
    ) -> Result<MotionPolicy, SbroadError> {
        let Relational::Join(Join {
            left: outer_id,
            right: inner_id,
            ..
        }) = self.get_relation_node(join_id)?
        else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("expected join")),
            ));
        };
        let (outer_id, inner_id) = (*outer_id, *inner_id);

        let Some(facts) = self.facts.as_ref() else {
            return Err(SbroadError::Other(format_smolstr!(
                "facts are not available for this plan"
            )));
        };

        if self.bucket_id_co_located_via_facts(join_id, outer_id, inner_id, facts)? {
            return Ok(MotionPolicy::None);
        }

        let outer_dist = self.get_rel_distribution(outer_id)?;
        let inner_dist = self.get_rel_distribution(inner_id)?;

        match (outer_dist, inner_dist) {
            (
                Distribution::Segment { keys: outer_keys },
                Distribution::Segment { keys: inner_keys },
            ) => {
                for outer_key in outer_keys.iter() {
                    for inner_key in inner_keys.iter() {
                        if keys_co_located_via_facts(
                            join_id, outer_id, outer_key, inner_id, inner_key, facts,
                        ) {
                            return Ok(MotionPolicy::None);
                        }
                    }
                }
                for outer_key in outer_keys.iter() {
                    if let Some(inner_positions) = derive_inner_positions_via_facts(
                        join_id, outer_id, outer_key, inner_id, facts,
                    ) {
                        return Ok(MotionPolicy::Segment(Key::new(inner_positions).into()));
                    }
                }
                Ok(MotionPolicy::Full)
            }

            // Outer is segmented, inner is unsegmented (Any) — try to
            // repartition inner so its rows land on outer's buckets.
            (Distribution::Segment { keys: outer_keys }, Distribution::Any) => {
                for outer_key in outer_keys.iter() {
                    if let Some(inner_positions) = derive_inner_positions_via_facts(
                        join_id, outer_id, outer_key, inner_id, facts,
                    ) {
                        return Ok(MotionPolicy::Segment(Key::new(inner_positions).into()));
                    }
                }
                Ok(MotionPolicy::Full)
            }

            // Outer is Any (unsegmented) and inner is Segment. We cannot
            // re-place outer here (the join pipeline only motions inner +
            // subqueries), so the safe choice is to broadcast inner.
            //
            // Note: even when inner's key columns are class-equal to outer
            // slots, we cannot leverage that without motioning outer.
            (Distribution::Any, Distribution::Segment { .. }) => Ok(MotionPolicy::Full),

            // (Any, Any): no known sharding key on either side; class-equality between
            // positions doesn't imply bucket co-location. Bucket_id co-location was
            // already ruled out above. Broadcast inner.
            (Distribution::Any, Distribution::Any) => Ok(MotionPolicy::Full),

            // Outer or Inner is replicated everywhere; opposite stays where it is and the
            // join is local on each bucket that already holds inner data.
            (Distribution::Global, _) | (_, Distribution::Global) => Ok(MotionPolicy::None),

            (Distribution::Single, _) | (_, Distribution::Single) => {
                unreachable!("single-node distribution is handled by the caller")
            }
        }
    }

    fn bucket_id_co_located_via_facts(
        &self,
        at_rel: NodeId,
        outer_id: NodeId,
        inner_id: NodeId,
        facts: &EqualityFacts,
    ) -> Result<bool, SbroadError> {
        let outer_positions = {
            let mut ctx = self.context_mut();
            ctx.get_shard_columns_positions(outer_id, self)?.copied()
        };
        let Some(outer_positions) = outer_positions else {
            return Ok(false);
        };
        let inner_positions = {
            let mut ctx = self.context_mut();
            ctx.get_shard_columns_positions(inner_id, self)?.copied()
        };
        let Some(inner_positions) = inner_positions else {
            return Ok(false);
        };

        for outer_opt in outer_positions {
            let Some(o_pos) = outer_opt else { continue };
            for inner_opt in inner_positions {
                let Some(i_pos) = inner_opt else { continue };
                if facts.are_equal(
                    at_rel,
                    Slot::new(outer_id, o_pos),
                    Slot::new(inner_id, i_pos),
                ) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

/// Both keys describe identical buckets when each positional pair shares an
/// equivalence class. Order matters: `bucket_id` is computed from the key
/// tuple in order.
fn keys_co_located_via_facts(
    at_rel: NodeId,
    outer_id: NodeId,
    outer_key: &Key,
    inner_id: NodeId,
    inner_key: &Key,
    facts: &EqualityFacts,
) -> bool {
    if outer_key.positions.len() != inner_key.positions.len() {
        return false;
    }
    outer_key
        .positions
        .iter()
        .zip(inner_key.positions.iter())
        .all(|(o_pos, i_pos)| {
            facts.are_equal(
                at_rel,
                Slot::new(outer_id, *o_pos),
                Slot::new(inner_id, *i_pos),
            )
        })
}

/// For each position in `outer_key`, find a class-equivalent output position
/// on `inner_id`.  Picks the lowest matching position to keep the choice
/// deterministic for tests.
///
/// When several outer key positions share an equivalence class (so they're
/// all class-equal to the same inner position), the result repeats that
/// position — e.g. `Key([q, q])`.  This is semantically correct: hash
/// equality on matched rows follows from class equality of the outer
/// positions, and `keys_co_located_via_facts` checks each (outer_pos,
/// inner_pos) pair independently via [`EqualityFacts::are_equal`], so
/// repeated inner positions don't affect co-location detection.
pub(crate) fn derive_inner_positions_via_facts(
    at_rel: NodeId,
    outer_id: NodeId,
    outer_key: &Key,
    inner_id: NodeId,
    facts: &EqualityFacts,
) -> Option<Vec<usize>> {
    if facts.slot_count(inner_id) == 0 {
        return None;
    }
    let mut result = Vec::with_capacity(outer_key.positions.len());
    for o_pos in &outer_key.positions {
        let slot = Slot::new(outer_id, *o_pos);
        let matched = facts.find_equal_position(at_rel, slot, inner_id)?;
        result.push(matched);
    }
    Some(result)
}
