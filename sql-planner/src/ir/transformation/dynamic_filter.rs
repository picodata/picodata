//! Dynamic-filter pushdown resolver.
//!
//! Runs after motion insertion and before slice calculation. For each
//! INNER JOIN whose both direct children are `Motion(Full)` and whose
//! ON-condition reduces to a pure `=`-conjunction over outer/inner
//! columns, the pass inserts a paired `BuildFilter` / `ApplyFilter`:
//!
//! * `BuildFilter` becomes the new parent of the build-side `Motion`
//!   (sits between the build `Motion` and the `Join`).
//! * `ApplyFilter` is spliced **inside** the probe-side `Motion`
//!   subtree, above the `Motion`'s current child. Together with the
//!   `filter_source` edge — pointing from `ApplyFilter` back to the
//!   matching `BuildFilter` — this guarantees that `calculate_slices`
//!   schedules the build slice strictly before the probe slice.
//!
//! Gated by `Options::sql_dynamic_filter_pushdown`. Disabled by default;
//! when off, this pass is a no-op and the resulting IR (and DQL wire
//! packet) are bit-for-bit identical to pre-feature behavior.

use ahash::AHashSet;

use crate::errors::SbroadError;
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    ApplyFilter, BuildFilter, Join, Motion, NodeId, NullPolicy, ReferenceTarget,
};
use crate::ir::operator::JoinKind;
use crate::ir::transformation::redistribution::eq_cols::EqualityCols;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::tree::traversal::{LevelNode, PostOrder, REL_CAPACITY};
use crate::ir::Plan;

/// One join candidate captured during the read-only traversal so the
/// mutation phase can splice nodes without iterating-while-mutating.
struct Candidate {
    join_id: NodeId,
    /// The build-side `Motion(Full)` — the right child of `Join`.
    build_motion: NodeId,
    /// The probe-side `Motion(Full)` — the left child of `Join`.
    probe_motion: NodeId,
    /// The current child of `probe_motion`; `ApplyFilter` will be
    /// spliced between this node and `probe_motion`.
    probe_subroot: NodeId,
    /// Equality pairs `(inner_col_pos, outer_col_pos)` extracted from
    /// the join condition. Guaranteed non-empty.
    pairs: Vec<(usize, usize)>,
}

impl Plan {
    /// Walk the subtree rooted at `top_id` and splice `BuildFilter` /
    /// `ApplyFilter` pairs around eligible INNER JOINs. See module
    /// docs for the precondition, splice shape, and rationale.
    ///
    /// # Errors
    /// Propagates any IR-level error from node construction, output
    /// row creation, or distribution resolution.
    pub fn insert_dynamic_filters_for_inner_joins(
        &mut self,
        top_id: NodeId,
    ) -> Result<(), SbroadError> {
        let candidates = self.collect_dynamic_filter_candidates(top_id)?;
        let mut next_filter_id: u32 = 0;
        for cand in candidates {
            self.splice_filter_pair(&cand, next_filter_id)?;
            next_filter_id += 1;
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

        let probe_subroot = match self.get_relation_node(probe_motion)? {
            Relational::Motion(Motion {
                policy: MotionPolicy::Full,
                child: Some(child),
                ..
            }) => *child,
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
            join_id: id,
            build_motion,
            probe_motion,
            probe_subroot,
            pairs: eq_cols.pairs,
        }))
    }

    /// Mutation phase: build the BuildFilter/ApplyFilter pair for one
    /// candidate and splice them into the IR.
    fn splice_filter_pair(
        &mut self,
        cand: &Candidate,
        filter_id: u32,
    ) -> Result<(), SbroadError> {
        let build_keys = self.make_key_refs(cand.build_motion, cand.pairs.iter().map(|p| p.0))?;
        let apply_keys = self.make_key_refs(cand.probe_subroot, cand.pairs.iter().map(|p| p.1))?;

        // BuildFilter: new parent of the build Motion. The filter is a
        // pure passthrough, so its output distribution equals its
        // child's — copy it directly rather than resolving via
        // references, which avoids depending on every intermediate
        // node's distribution already being set.
        let build_output = self.add_row_for_output(cand.build_motion, &[], true, None)?;
        let build_child_dist = self
            .get_distribution(self.get_relation_node(cand.build_motion)?.output())?
            .clone();
        let build_filter = BuildFilter {
            child: cand.build_motion,
            keys: build_keys,
            filter_id,
            null_policy: NullPolicy::Skip,
            output: build_output,
        };
        let build_filter_id = self.add_relational(build_filter.into())?;
        self.set_dist(build_output, build_child_dist)?;

        // ApplyFilter: spliced inside the probe Motion subtree.
        let apply_output = self.add_row_for_output(cand.probe_subroot, &[], true, None)?;
        let apply_child_dist = self
            .get_distribution(self.get_relation_node(cand.probe_subroot)?.output())?
            .clone();
        let apply_filter = ApplyFilter {
            child: cand.probe_subroot,
            keys: apply_keys,
            filter_id,
            null_policy: NullPolicy::Skip,
            filter_source: build_filter_id,
            output: apply_output,
        };
        let apply_filter_id = self.add_relational(apply_filter.into())?;
        self.set_dist(apply_output, apply_child_dist)?;

        // Rewire Join.right: build_motion -> BuildFilter, and patch
        // references in Join.output / Join.condition that targeted
        // build_motion so they now resolve through BuildFilter.
        self.change_child(cand.join_id, cand.build_motion, build_filter_id)?;
        self.replace_target_in_relational(cand.join_id, cand.build_motion, build_filter_id)?;

        // Rewire probe_motion.child: probe_subroot -> ApplyFilter, and
        // patch probe_motion's output references the same way.
        self.change_child(cand.probe_motion, cand.probe_subroot, apply_filter_id)?;
        self.replace_target_in_relational(
            cand.probe_motion,
            cand.probe_subroot,
            apply_filter_id,
        )?;

        Ok(())
    }

    /// Build a `Vec<NodeId>` of `Reference` expressions, one per
    /// requested column position in `child`'s output. Each reference
    /// targets `Single(child)` and inherits the column's derived type.
    fn make_key_refs<I: IntoIterator<Item = usize>>(
        &mut self,
        child: NodeId,
        positions: I,
    ) -> Result<Vec<NodeId>, SbroadError> {
        let child_output = self.get_relation_node(child)?.output();
        let row_list = self.get_row_list(child_output)?.clone();
        let mut keys: Vec<NodeId> = Vec::new();
        for pos in positions {
            let alias_id = *row_list.get(pos).ok_or_else(|| {
                SbroadError::Invalid(
                    crate::errors::Entity::Plan,
                    Some(smol_str::format_smolstr!(
                        "dynamic-filter key position {pos} out of range"
                    )),
                )
            })?;
            let col_type = self.get_expression_node(alias_id)?.calculate_type(self)?;
            let ref_id =
                self.nodes
                    .add_ref(ReferenceTarget::Single(child), pos, col_type, None, false);
            keys.push(ref_id);
        }
        Ok(keys)
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
    fn inserts_build_apply_pair_for_inner_join_two_motions() {
        let (mut plan, join, probe_motion, build_motion) = make_two_motion_inner_join();
        let probe_subroot = match plan.get_relation_node(probe_motion).unwrap() {
            Relational::Motion(Motion { child: Some(c), .. }) => *c,
            _ => unreachable!(),
        };
        let build_subroot = match plan.get_relation_node(build_motion).unwrap() {
            Relational::Motion(Motion { child: Some(c), .. }) => *c,
            _ => unreachable!(),
        };

        // In production the resolver runs after all distributions are
        // already set on intermediate nodes. The test fixture only sets
        // distributions on the Motions, so we lazily compute the Scan
        // (sharded → Segment) and Projection (passthrough) distributions
        // here in topological order.
        let probe_scan = match plan.get_relation_node(probe_subroot).unwrap() {
            crate::ir::node::relational::Relational::Projection(p) => p.child.unwrap(),
            _ => unreachable!(),
        };
        let build_scan = match plan.get_relation_node(build_subroot).unwrap() {
            crate::ir::node::relational::Relational::Projection(p) => p.child.unwrap(),
            _ => unreachable!(),
        };
        plan.set_rel_output_distribution(probe_scan).unwrap();
        plan.set_rel_output_distribution(build_scan).unwrap();
        plan.set_rel_output_distribution(probe_subroot).unwrap();
        plan.set_rel_output_distribution(build_subroot).unwrap();

        plan.insert_dynamic_filters_for_inner_joins(join).unwrap();

        // Join.right should now point at a BuildFilter wrapping the
        // original build-side Motion.
        let join_node = plan.get_relation_node(join).unwrap();
        let (left, right) = match join_node {
            Relational::Join(Join { left, right, .. }) => (*left, *right),
            _ => panic!("expected Join"),
        };
        assert_eq!(left, probe_motion, "probe motion preserved as Join.left");
        let build_filter = match plan.get_relation_node(right).unwrap() {
            Relational::BuildFilter(bf) => bf.clone(),
            _ => panic!("expected BuildFilter at Join.right"),
        };
        assert_eq!(build_filter.child, build_motion);
        assert_eq!(build_filter.keys.len(), 1);
        assert!(matches!(build_filter.null_policy, NullPolicy::Skip));

        // probe_motion.child should now be an ApplyFilter wrapping the
        // original probe subroot, with filter_source -> BuildFilter.
        let apply_filter_id = match plan.get_relation_node(probe_motion).unwrap() {
            Relational::Motion(Motion { child: Some(c), .. }) => *c,
            _ => panic!("expected Motion(left)"),
        };
        let apply_filter = match plan.get_relation_node(apply_filter_id).unwrap() {
            Relational::ApplyFilter(af) => af.clone(),
            _ => panic!("expected ApplyFilter inside probe motion"),
        };
        assert_eq!(apply_filter.child, probe_subroot);
        assert_eq!(apply_filter.keys.len(), 1);
        assert!(matches!(apply_filter.null_policy, NullPolicy::Skip));
        assert_eq!(apply_filter.filter_source, right);
        assert_eq!(apply_filter.filter_id, build_filter.filter_id);

        // Relational::subqueries(apply_id) must expose the filter_source
        // edge so `calculate_slices` can schedule the build slice first.
        let apply_rel = plan.get_relation_node(apply_filter_id).unwrap();
        let sqs: Vec<NodeId> = apply_rel.subqueries().iter().copied().collect();
        assert_eq!(sqs, vec![right], "filter_source exposed via subqueries()");
    }

    #[test]
    fn build_slice_strictly_precedes_probe_slice() {
        // §5.5: the filter_source edge from ApplyFilter to BuildFilter
        // must force calculate_slices to schedule the build-side Motion
        // in a strictly earlier slice than the probe-side Motion.
        let (mut plan, join, probe_motion, build_motion) = make_two_motion_inner_join();
        let probe_subroot = match plan.get_relation_node(probe_motion).unwrap() {
            Relational::Motion(Motion { child: Some(c), .. }) => *c,
            _ => unreachable!(),
        };
        let build_subroot = match plan.get_relation_node(build_motion).unwrap() {
            Relational::Motion(Motion { child: Some(c), .. }) => *c,
            _ => unreachable!(),
        };
        let probe_scan = match plan.get_relation_node(probe_subroot).unwrap() {
            crate::ir::node::relational::Relational::Projection(p) => p.child.unwrap(),
            _ => unreachable!(),
        };
        let build_scan = match plan.get_relation_node(build_subroot).unwrap() {
            crate::ir::node::relational::Relational::Projection(p) => p.child.unwrap(),
            _ => unreachable!(),
        };
        plan.set_rel_output_distribution(probe_scan).unwrap();
        plan.set_rel_output_distribution(build_scan).unwrap();
        plan.set_rel_output_distribution(probe_subroot).unwrap();
        plan.set_rel_output_distribution(build_subroot).unwrap();

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

        // Nothing changed: Join.left/right are still the original Motions.
        match plan.get_relation_node(join).unwrap() {
            Relational::Join(Join { left, right, .. }) => {
                assert_eq!(*left, motion_t1);
                assert_eq!(*right, motion_t2);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn outer_join_is_skipped() {
        let (mut plan, join, _, _) = make_two_motion_inner_join();
        // Flip to LeftOuter — resolver must skip the candidate.
        if let crate::ir::node::MutNode::Relational(crate::ir::node::relational::MutRelational::Join(j)) =
            plan.get_mut_node(join).unwrap()
        {
            j.kind = JoinKind::LeftOuter;
        } else {
            unreachable!();
        }
        plan.insert_dynamic_filters_for_inner_joins(join).unwrap();
        // Nothing changed: Join.right is still the original Motion.
        let right = match plan.get_relation_node(join).unwrap() {
            Relational::Join(Join { right, .. }) => *right,
            _ => unreachable!(),
        };
        assert!(matches!(
            plan.get_relation_node(right).unwrap(),
            Relational::Motion(_)
        ));
    }
}
