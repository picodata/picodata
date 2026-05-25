//! Dynamic-filter pushdown resolver.
//!
//! Runs after motion insertion and before slice calculation. For each
//! INNER JOIN whose both direct children are `Motion(Full)` and whose
//! ON-condition reduces to a pure `=`-conjunction over outer/inner
//! columns, the pass records a `DynamicFilterSpec` in the plan's
//! sidecar list. The IR itself is left untouched — no new relational
//! nodes are inserted, no edges are rewritten. The executor, slice
//! scheduler, and EXPLAIN all read filter information from
//! `Plan::dynamic_filters`.
//!
//! Gated by `Options::sql_dynamic_filter_pushdown`. Disabled by default;
//! when off, this pass is a no-op and the resulting IR (and DQL wire
//! packet) are bit-for-bit identical to pre-feature behavior.

use ahash::AHashSet;

use crate::errors::SbroadError;
use crate::ir::dynamic_filter_spec::DynamicFilterSpec;
use crate::ir::node::relational::Relational;
use crate::ir::node::{Join, Motion, NodeId, NullPolicy};
use crate::ir::operator::JoinKind;
use crate::ir::transformation::redistribution::eq_cols::EqualityCols;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::tree::traversal::{LevelNode, PostOrder, REL_CAPACITY};
use crate::ir::Plan;

/// One join candidate captured during the read-only traversal so the
/// recording phase can push specs without iterating-while-mutating.
struct Candidate {
    /// The build-side `Motion(Full)` — the right child of `Join`.
    build_motion: NodeId,
    /// The probe-side `Motion(Full)` — the left child of `Join`.
    probe_motion: NodeId,
    /// Equality pairs `(inner_col_pos, outer_col_pos)` extracted from
    /// the join condition. Guaranteed non-empty.
    pairs: Vec<(usize, usize)>,
}

impl Plan {
    /// Walk the subtree rooted at `top_id` and append a
    /// `DynamicFilterSpec` to `plan.dynamic_filters` for each eligible
    /// INNER JOIN. See module docs for the precondition.
    ///
    /// # Errors
    /// Propagates any IR-level error encountered while inspecting the
    /// join condition.
    pub fn insert_dynamic_filters_for_inner_joins(
        &mut self,
        top_id: NodeId,
    ) -> Result<(), SbroadError> {
        let candidates = self.collect_dynamic_filter_candidates(top_id)?;
        for (next_filter_id, cand) in (0_u32..).zip(candidates.into_iter()) {
            self.dynamic_filters.push(DynamicFilterSpec {
                filter_id: next_filter_id,
                build_motion_id: cand.build_motion,
                probe_motion_id: cand.probe_motion,
                build_columns: cand.pairs.iter().map(|p| p.0).collect(),
                probe_columns: cand.pairs.iter().map(|p| p.1).collect(),
                null_policy: NullPolicy::Skip,
            });
        }
        Ok(())
    }

    /// Read-only pass: find each eligible inner join and resolve its
    /// equality pairs. Returns an empty vector when nothing qualifies.
    fn collect_dynamic_filter_candidates(
        &self,
        top_id: NodeId,
    ) -> Result<Vec<Candidate>, SbroadError> {
        let dfs = PostOrder::new(|node| self.nodes.rel_iter(node), REL_CAPACITY);
        let mut seen: AHashSet<NodeId> = AHashSet::with_capacity(REL_CAPACITY);
        let mut candidates: Vec<Candidate> = Vec::new();

        for LevelNode(_, id) in dfs.traverse_into_iter(top_id) {
            if !seen.insert(id) {
                continue;
            }
            let Some(cand) = self.try_extract_candidate(id)? else {
                continue;
            };
            candidates.push(cand);
        }
        Ok(candidates)
    }

    /// Verify the v1 precondition (inner join + both direct children
    /// are `Motion(Full)`) and reduce the ON-condition to pure
    /// equality pairs via `EqualityCols::from_join_condition`.
    fn try_extract_candidate(&self, id: NodeId) -> Result<Option<Candidate>, SbroadError> {
        let rel = self.get_relation_node(id)?;
        let Relational::Join(Join {
            left,
            right,
            condition,
            kind,
            ..
        }) = rel
        else {
            return Ok(None);
        };
        if !matches!(kind, JoinKind::Inner) {
            return Ok(None);
        }
        let (probe_motion, build_motion, condition_id) = (*left, *right, *condition);

        match self.get_relation_node(probe_motion)? {
            Relational::Motion(Motion {
                policy: MotionPolicy::Full,
                child: Some(_),
                ..
            }) => {}
            _ => return Ok(None),
        };
        match self.get_relation_node(build_motion)? {
            Relational::Motion(Motion {
                policy: MotionPolicy::Full,
                child: Some(_),
                ..
            }) => {}
            _ => return Ok(None),
        };

        let Some(eq_cols) =
            EqualityCols::from_join_condition(self, id, build_motion, condition_id)?
        else {
            return Ok(None);
        };
        if eq_cols.pairs.is_empty() {
            return Ok(None);
        }
        Ok(Some(Candidate {
            build_motion,
            probe_motion,
            pairs: eq_cols.pairs,
        }))
    }
}

#[cfg(feature = "mock")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::expression::ColumnWithScan;
    use crate::ir::operator::Bool;
    use crate::ir::relation::{SpaceEngine, Table};
    use crate::ir::tests::{column_user_non_null, sharding_column};
    use crate::ir::transformation::redistribution::{MotionPolicy, Program};
    use crate::ir::types::UnrestrictedType;
    use rand::random;
    use smol_str::SmolStr;

    /// Programmatic IR shaped like:
    /// `JOIN(Motion(Full)(scan_t1), Motion(Full)(scan_t2)) ON t1.a = t2.d`.
    /// Returns `(plan, join_id, probe_motion_id, build_motion_id)`.
    fn make_two_motion_inner_join() -> (Plan, NodeId, NodeId, NodeId) {
        let mut plan = Plan::default();

        let t1 = Table::new_sharded(
            random(),
            "t1",
            vec![
                column_user_non_null(SmolStr::from("a"), UnrestrictedType::Integer),
                column_user_non_null(SmolStr::from("b"), UnrestrictedType::Integer),
                sharding_column(),
            ],
            &["a"],
            &["a"],
            SpaceEngine::Memtx,
        )
        .unwrap();
        plan.add_rel(t1);
        let scan_t1 = plan.add_scan("t1", None).unwrap();

        let t2 = Table::new_sharded(
            random(),
            "t2",
            vec![
                column_user_non_null(SmolStr::from("c"), UnrestrictedType::Integer),
                column_user_non_null(SmolStr::from("d"), UnrestrictedType::Integer),
                sharding_column(),
            ],
            &["d"],
            &["d"],
            SpaceEngine::Memtx,
        )
        .unwrap();
        plan.add_rel(t2);
        let scan_t2 = plan.add_scan("t2", None).unwrap();

        let motion_t1 = plan
            .add_motion(scan_t1, &MotionPolicy::Full, Program::default())
            .unwrap();
        let motion_t2 = plan
            .add_motion(scan_t2, &MotionPolicy::Full, Program::default())
            .unwrap();

        let a_ref = plan
            .add_ref_from_left_branch(motion_t1, motion_t2, ColumnWithScan::new("a", None))
            .unwrap();
        let d_ref = plan
            .add_ref_from_right_branch(motion_t1, motion_t2, ColumnWithScan::new("d", None))
            .unwrap();
        let condition = plan.nodes.add_bool(a_ref, Bool::Eq, d_ref).unwrap();
        let join = plan
            .add_join(motion_t1, motion_t2, condition, JoinKind::Inner)
            .unwrap();
        plan.top = Some(join);
        (plan, join, motion_t1, motion_t2)
    }

    #[test]
    fn records_spec_for_inner_join_two_motions() {
        let (mut plan, join, probe_motion, build_motion) = make_two_motion_inner_join();

        plan.insert_dynamic_filters_for_inner_joins(join).unwrap();

        // The resolver must leave the IR untouched: Join.left/right
        // still point at the original Motions, no new relational nodes
        // are inserted.
        let (left, right) = match plan.get_relation_node(join).unwrap() {
            Relational::Join(Join { left, right, .. }) => (*left, *right),
            _ => panic!("expected Join"),
        };
        assert_eq!(left, probe_motion);
        assert_eq!(right, build_motion);

        // Exactly one spec is recorded, with the natural column
        // mapping: t1.a (position 0) on the probe side, t2.d
        // (position 1) on the build side.
        assert_eq!(plan.dynamic_filters.len(), 1);
        let spec = &plan.dynamic_filters[0];
        assert_eq!(spec.filter_id, 0);
        assert_eq!(spec.build_motion_id, build_motion);
        assert_eq!(spec.probe_motion_id, probe_motion);
        assert_eq!(spec.build_columns, vec![1]);
        assert_eq!(spec.probe_columns, vec![0]);
        assert!(matches!(spec.null_policy, NullPolicy::Skip));
    }

    #[test]
    fn build_slice_strictly_precedes_probe_slice() {
        // §5.5: the sidecar spec must force `calculate_slices` to
        // schedule the build-side Motion in a strictly earlier slice
        // than the probe-side Motion via the post-pass that consults
        // `plan.dynamic_filters`.
        let (mut plan, join, probe_motion, build_motion) = make_two_motion_inner_join();
        plan.insert_dynamic_filters_for_inner_joins(join).unwrap();
        let slices = plan.calculate_slices(join).unwrap();

        let build_slice = slices
            .iter()
            .position(|s| s.contains(&build_motion))
            .expect("build_motion present in some slice");
        let probe_slice = slices
            .iter()
            .position(|s| s.contains(&probe_motion))
            .expect("probe_motion present in some slice");
        assert!(
            build_slice < probe_slice,
            "expected build_slice ({build_slice}) < probe_slice ({probe_slice}), slices={slices:?}",
        );
    }

    #[test]
    fn or_chain_condition_is_skipped() {
        // §5.7: condition shaped as `t1.a = t2.d OR t1.b = t2.c` is not
        // a pure equality conjunction. EqualityCols::from_join_condition
        // returns None for it, so the resolver must leave the IR
        // untouched.
        let mut plan = Plan::default();

        let t1 = Table::new_sharded(
            random(),
            "t1",
            vec![
                column_user_non_null(SmolStr::from("a"), UnrestrictedType::Integer),
                column_user_non_null(SmolStr::from("b"), UnrestrictedType::Integer),
                sharding_column(),
            ],
            &["a"],
            &["a"],
            SpaceEngine::Memtx,
        )
        .unwrap();
        plan.add_rel(t1);
        let scan_t1 = plan.add_scan("t1", None).unwrap();

        let t2 = Table::new_sharded(
            random(),
            "t2",
            vec![
                column_user_non_null(SmolStr::from("c"), UnrestrictedType::Integer),
                column_user_non_null(SmolStr::from("d"), UnrestrictedType::Integer),
                sharding_column(),
            ],
            &["d"],
            &["d"],
            SpaceEngine::Memtx,
        )
        .unwrap();
        plan.add_rel(t2);
        let scan_t2 = plan.add_scan("t2", None).unwrap();

        let motion_t1 = plan
            .add_motion(scan_t1, &MotionPolicy::Full, Program::default())
            .unwrap();
        let motion_t2 = plan
            .add_motion(scan_t2, &MotionPolicy::Full, Program::default())
            .unwrap();

        let a_ref = plan
            .add_ref_from_left_branch(motion_t1, motion_t2, ColumnWithScan::new("a", None))
            .unwrap();
        let d_ref = plan
            .add_ref_from_right_branch(motion_t1, motion_t2, ColumnWithScan::new("d", None))
            .unwrap();
        let b_ref = plan
            .add_ref_from_left_branch(motion_t1, motion_t2, ColumnWithScan::new("b", None))
            .unwrap();
        let c_ref = plan
            .add_ref_from_right_branch(motion_t1, motion_t2, ColumnWithScan::new("c", None))
            .unwrap();
        let eq_ad = plan.nodes.add_bool(a_ref, Bool::Eq, d_ref).unwrap();
        let eq_bc = plan.nodes.add_bool(b_ref, Bool::Eq, c_ref).unwrap();
        let or = plan.nodes.add_bool(eq_ad, Bool::Or, eq_bc).unwrap();
        let join = plan
            .add_join(motion_t1, motion_t2, or, JoinKind::Inner)
            .unwrap();
        plan.top = Some(join);

        plan.insert_dynamic_filters_for_inner_joins(join).unwrap();

        // Nothing changed: Join.left/right are still the original
        // Motions, and no spec was pushed.
        match plan.get_relation_node(join).unwrap() {
            Relational::Join(Join { left, right, .. }) => {
                assert_eq!(*left, motion_t1);
                assert_eq!(*right, motion_t2);
            }
            _ => unreachable!(),
        }
        assert!(plan.dynamic_filters.is_empty());
    }

    #[test]
    fn outer_join_is_skipped() {
        let (mut plan, join, _, _) = make_two_motion_inner_join();
        // Flip to LeftOuter — resolver must skip the candidate.
        if let crate::ir::node::MutNode::Relational(
            crate::ir::node::relational::MutRelational::Join(j),
        ) = plan.get_mut_node(join).unwrap()
        {
            j.kind = JoinKind::LeftOuter;
        } else {
            unreachable!();
        }
        plan.insert_dynamic_filters_for_inner_joins(join).unwrap();
        // Nothing changed and no spec recorded.
        let right = match plan.get_relation_node(join).unwrap() {
            Relational::Join(Join { right, .. }) => *right,
            _ => unreachable!(),
        };
        assert!(matches!(
            plan.get_relation_node(right).unwrap(),
            Relational::Motion(_)
        ));
        assert!(plan.dynamic_filters.is_empty());
    }
}
