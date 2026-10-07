//! Derived domain checks repeated on Motion inputs.
//!
//! Enrichment places each [`DomainCheck`] at a host where FALSE or UNKNOWN
//! empties the domain: a `$param = pin` equality, or a `false` for a domain
//! proven empty at planning time. A sending Motion can run before that host
//! applies the check. This pass adds the check to eligible Motion inputs:
//!
//! ```text
//! Motion
//!   Projection (*)
//!     Selection ($param = pin AND ... | false)
//!       input
//! ```
//!
//! Inputs that cannot appear directly in FROM are wrapped in a subquery.
//! Binding folds the parameter checks so bucket discovery can prune the
//! Motion input; a `false` needs no binding.
//!
//! The original host keeps the check. Filtering another input in the same
//! domain preserves the result: a passing check changes nothing, and a
//! failing check already empties the domain. Domain membership is taken
//! from the analysis before redistribution. Checks do not cross its
//! boundaries, including aggregates, set operations, nullable LEFT JOIN
//! operands, expression subqueries and CTE bodies.

use crate::errors::{Entity, SbroadError};
use crate::ir::distribution::Distribution;
use crate::ir::node::relational::Relational;
use crate::ir::node::{Delete, NodeId};
use crate::ir::operator::Bool;
use crate::ir::transformation::equality_facts::{DomainCheck, DomainCheckKind, Pin};
use crate::ir::transformation::redistribution::{MotionPolicy, Program};
use crate::ir::tree::traversal::REL_CAPACITY;
use crate::ir::types::DerivedType;
use crate::ir::value::Value;
use crate::ir::Plan;
use ahash::AHashSet;
use smol_str::format_smolstr;

impl Plan {
    /// Build the pin as a detached constant or typed parameter.
    pub(crate) fn make_pin(&mut self, pin: &Pin) -> NodeId {
        match pin {
            Pin::Const(value) => self.add_const(value.clone()),
            Pin::Param(index, ty) => self.add_param(*index, *ty),
        }
    }

    /// Build `Parameter($param) = <pin>` as a detached, column-less SQL conjunct.
    ///
    /// # Errors
    /// - Building the comparison fails.
    pub(crate) fn make_param_check(
        &mut self,
        param: u16,
        param_type: DerivedType,
        pin: &Pin,
    ) -> Result<NodeId, SbroadError> {
        let pin_id = self.make_pin(pin);
        let param_id = self.add_param(param, param_type);
        self.add_bool(param_id, Bool::Eq, pin_id)
    }

    /// When domain checks exist, verify that every node with output in
    /// `nodes` has a domain before redistribution starts.
    /// Nodes created during redistribution are handled separately.
    ///
    /// # Errors
    /// - A node outside the analysis while some domain has checks.
    pub(super) fn check_motion_inputs_analyzed(&self, nodes: &[NodeId]) -> Result<(), SbroadError> {
        let Some(facts) = self
            .facts
            .as_ref()
            .filter(|facts| facts.has_domain_checks())
        else {
            return Ok(());
        };
        for &id in nodes {
            // The analysis records no domain for a node without output.
            if matches!(
                self.get_relation_node(id)?,
                Relational::Delete(Delete { child: None, .. })
            ) {
                continue;
            }
            if !facts.is_analyzed(id) {
                debug_assert!(false, "add_motions: {id:?} has no domain");
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "add_motions: relational node {id:?} has no domain"
                    )),
                ));
            }
        }
        Ok(())
    }

    /// Add a Motion over `input` like [`Plan::add_motion`], first filtering
    /// `input` by the checks of its domain that it does not already apply.
    /// The Motion keeps the alias, the output columns and their order that
    /// it would have over `input` itself.
    ///
    /// # Errors
    /// - Reading a node or building the filter or a node fails.
    pub(crate) fn add_motion_with_domain_checks(
        &mut self,
        input: NodeId,
        policy: &MotionPolicy,
        program: Program,
    ) -> Result<NodeId, SbroadError> {
        let checks = self.motion_input_checks(input, policy)?;
        if checks.is_empty() {
            return self.add_motion(input, policy, program);
        }
        let alias = self.motion_alias(input)?;
        let carrier = self.add_domain_check_carrier(input, &checks)?;
        self.add_motion_with_alias(carrier, alias, policy, program)
    }

    /// Return checks whose hosts are absent from the Motion input subtree,
    /// stopping the search at nested Motions.
    ///
    /// Only `Segment` and `Full` policies with wrappable inputs qualify.
    /// Local policies run on storage. VALUES use separate materialization,
    /// and adjacent Motions are squashed.
    ///
    /// # Errors
    /// - Reading a relational node fails.
    fn motion_input_checks(
        &self,
        input: NodeId,
        policy: &MotionPolicy,
    ) -> Result<Vec<DomainCheck>, SbroadError> {
        let Some(facts) = self
            .facts
            .as_ref()
            .filter(|facts| facts.has_domain_checks())
        else {
            return Ok(Vec::new());
        };
        if !matches!(policy, MotionPolicy::Segment(_) | MotionPolicy::Full) {
            return Ok(Vec::new());
        }
        if self.check_carrier_from(input)?.is_none() {
            return Ok(Vec::new());
        }
        // The pre-pass check validates existing nodes. Nodes created during
        // redistribution, such as local aggregate stages, have no domain
        // entry and receive no additional checks.
        let checks = facts.domain_checks_of(input);
        if checks.is_empty() {
            return Ok(Vec::new());
        }
        let local = self.motion_query_nodes(input);
        // A domain with a proven `false` keeps only those (see
        // `EqualityFacts::set_domain_checks`). One already in the input's
        // query empties it; otherwise a single `false` is enough.
        if checks[0].kind == DomainCheckKind::AlwaysFalse {
            if checks.iter().any(|check| local.contains(&check.host)) {
                return Ok(Vec::new());
            }
            return Ok(vec![checks[0].clone()]);
        }
        Ok(checks
            .iter()
            .filter(|check| !local.contains(&check.host))
            .cloned()
            .collect())
    }

    /// Collect relational nodes under `input`, stopping at nested Motions.
    fn motion_query_nodes(&self, input: NodeId) -> AHashSet<NodeId> {
        let mut seen: AHashSet<NodeId> = AHashSet::with_capacity(REL_CAPACITY);
        let mut stack = vec![input];
        while let Some(node) = stack.pop() {
            if !seen.insert(node) {
                continue;
            }
            if matches!(self.get_relation_node(node), Ok(Relational::Motion(_))) {
                continue;
            }
            stack.extend(self.nodes.rel_iter(node));
        }
        seen
    }

    /// How a filter attaches over `input`: directly, for a `FROM` item
    /// (`Some(false)`), or through a new subquery (`Some(true)`). `None`
    /// when `input` is not wrapped at all.
    ///
    /// # Errors
    /// - Reading a relational node fails.
    fn check_carrier_from(&self, input: NodeId) -> Result<Option<bool>, SbroadError> {
        Ok(match self.get_relation_node(input)? {
            Relational::ScanRelation(_) | Relational::ScanSubQuery(_) | Relational::Join(_) => {
                Some(false)
            }
            Relational::Projection(_)
            | Relational::SelectWithoutScan(_)
            | Relational::Limit(_)
            | Relational::Union(_)
            | Relational::UnionAll(_)
            | Relational::Except(_)
            | Relational::Intersect(_) => Some(true),
            // Motions are squashed; VALUES and CTE scans have separate paths.
            // Other node kinds are not supported by this wrapper.
            _ => None,
        })
    }

    /// `SELECT * FROM input WHERE <checks>`, as a star `Projection` over `Selection`
    /// over `input` (or over a new subquery on it). The new nodes copy
    /// `input`'s output and its distribution, so Motion keys keep their
    /// positions. Return the projection.
    ///
    /// # Errors
    /// - Building a node or the filter fails.
    fn add_domain_check_carrier(
        &mut self,
        input: NodeId,
        checks: &[DomainCheck],
    ) -> Result<NodeId, SbroadError> {
        let Some(needs_subquery) = self.check_carrier_from(input)? else {
            unreachable!("motion_input_checks returns no checks for this input");
        };
        let dist: Option<Distribution> = self.rel_distr_ref(input).ok().cloned();
        let mut added = Vec::with_capacity(3);
        let from = if needs_subquery {
            let name = self.context_mut().get_unnamed_subquery_name();
            let sq = self.add_sub_query(input, Some(&name))?;
            added.push(sq);
            sq
        } else {
            input
        };
        let mut filter = None;
        for check in checks {
            let check_id = match &check.kind {
                DomainCheckKind::ParamEq {
                    param,
                    param_type,
                    pin,
                } => self.make_param_check(*param, *param_type, pin)?,
                DomainCheckKind::AlwaysFalse => self.add_const(Value::Boolean(false)),
            };
            filter = Some(match filter {
                None => check_id,
                Some(filter) => self.concat_and(filter, check_id)?,
            });
        }
        let filter = filter.expect("checks are not empty");
        let sel = self.add_select(from, filter)?;
        // Preserve duplicate names with `SELECT *` and retain the shard column.
        // The SQL backend expands references for sources with system columns.
        let proj = self.add_proj_star(sel, true)?;
        added.extend([sel, proj]);
        if let Some(dist) = dist {
            for id in added {
                self.set_rel_distr(id, dist.clone())?;
            }
        }
        Ok(proj)
    }
}
