//! The witness of the facts-based 0–1 bucket proof.
//!
//! AddMotions proves a `Selection` / INNER `Join` single-node from the pins
//! of `EqualityFacts`, without reading its condition, and stores each pinned
//! key it used as a [`FactsRoute`]. The executor narrows the node by the
//! bound value of that key, so the routing keeps the promise the plan made
//! even where the condition tree walk does not see the pin.

use ahash::{AHashMap, AHashSet};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::cell::RefCell;
use std::rc::Rc;

use super::{conjunct_into, conjunct_key_buckets, BucketAlgebra, BucketsResolver, HashedKeys};
use crate::errors::SbroadError;
use crate::ir::node::expression::Expression;
use crate::ir::node::{Constant, NodeId};
use crate::ir::types::DerivedType;
use crate::ir::value::Value;
use crate::ir::Plan;

/// One column of a pinned key: a planning-time value, or a `Parameter` /
/// `CURRENT_USER` node that bind replaces in place with its constant of the
/// given type.
#[derive(Clone, Debug, PartialEq)]
pub enum FactsRoutePart {
    Value(Value),
    Bound(NodeId, DerivedType),
}

/// What the facts pin for every row a node contributes to its domain.
#[derive(Clone, Debug, PartialEq)]
pub enum FactsRoute {
    /// The whole sharding key of an input, in hash order.
    Key(Vec<FactsRoutePart>),
    /// The `bucket_id` column of an input.
    BucketId(FactsRoutePart),
    /// An unsatisfiable condition: an `AlwaysFalse` domain check placed at
    /// the node itself or, without enrichment, a node of `facts.dead_rels()`.
    Empty,
    /// Values equal for every contributing row: unequal or NULL after bind
    /// leave no row. Never narrows on its own. The hosts of one class share
    /// the list, and bucket discovery checks it once, see [`SameChecked`].
    Same(Rc<[FactsRoutePart]>),
}

/// The verdicts of the `FactsRoute::Same` lists one bucket discovery has
/// checked, by the address of the list: whether its values conflict. An
/// `FxHashMap` costs nothing until a route needs it, unlike a seeded hash.
pub type SameChecked = FxHashMap<*const FactsRoutePart, bool>;

/// Planner output of the proof the executor relies on.
#[derive(Clone, Debug, Default)]
pub struct FactsProof {
    /// `Selection` / INNER `Join` -> the routes its proof used.
    pub routes: AHashMap<NodeId, Vec<FactsRoute>>,
    /// Nodes whose subtree AddMotions proved single-node. The executor fails
    /// a stage that routes one of them wider, e.g. after a lost route.
    pub single_node: AHashSet<NodeId>,
}

impl FactsProof {
    /// Give the copy of each node in `old_new` the proof of its original:
    /// a copy of a planned subtree runs without the motions the proof
    /// removed, so it needs the same routes and the same check.
    pub(crate) fn copy_to_clone(&mut self, old_new: &AHashMap<NodeId, NodeId>) {
        for (old, new) in old_new {
            if let Some(routes) = self.routes.get(old).cloned() {
                self.routes.insert(*new, routes);
            }
            if self.single_node.contains(old) {
                self.single_node.insert(*new);
            }
        }
    }

    pub(crate) fn add_route(&mut self, rel_id: NodeId, route: FactsRoute) {
        let routes = self.routes.entry(rel_id).or_default();
        // A class has one list of values: compare it by address, not value
        // by value, as each key of a host may add it again.
        let known = routes.iter().any(|known| match (known, &route) {
            (FactsRoute::Same(known), FactsRoute::Same(new)) => Rc::ptr_eq(known, new),
            (known, new) => known == new,
        });
        if !known {
            routes.push(route);
        }
    }
}

impl Plan {
    fn bound_value<'a>(&'a self, part: &'a FactsRoutePart) -> Result<&'a Value, SbroadError> {
        Ok(match part {
            FactsRoutePart::Value(value) => value,
            FactsRoutePart::Bound(id, ty) => match self.get_expression_node(*id)? {
                // A value of another type hashes to another bucket than the
                // pin promised. pgproto decodes by the declared type and
                // iproto types the parameter by its value, so it never is.
                Expression::Constant(Constant { value })
                    if !matches!(value, Value::Null) && value.get_type() != *ty =>
                {
                    return Err(SbroadError::other(format!(
                        "facts route expects {ty} for {id:?}, bound {value:?}"
                    )))
                }
                Expression::Constant(Constant { value }) => value,
                other => {
                    return Err(SbroadError::other(format!(
                        "facts route reads unbound {id:?}: {other:?}"
                    )))
                }
            },
        })
    }

    /// The witness still names the plan after optimize: every host and proven
    /// node is reachable from one of `top_ids`, each host is a `Selection` / INNER
    /// `Join`, and each bound part is the `Parameter` / `CURRENT_USER` that
    /// bind replaces in place. Passes after AddMotions keep the relational
    /// node ids, so a failure means a rewrite moved the proof off its nodes.
    #[cfg(debug_assertions)]
    pub(crate) fn debug_assert_facts_proof_attached(&self, top_ids: &[NodeId]) {
        use crate::ir::node::relational::Relational;
        use crate::ir::node::Join;
        use crate::ir::operator::JoinKind;
        use crate::ir::tree::traversal::{NodeIter, PostOrder};

        let reachable: AHashSet<NodeId> = top_ids
            .iter()
            .flat_map(|top_id| {
                PostOrder::new(&self.nodes, |node| self.nodes.rel_iter(node), *top_id)
                    .relational()
                    .map(|(id, _)| id)
            })
            .collect();
        for (host, routes) in &self.facts_proof.routes {
            assert!(
                reachable.contains(host),
                "facts route host {host:?} is detached"
            );
            assert!(
                matches!(
                    self.get_relation_node(*host),
                    Ok(Relational::Selection(_)
                        | Relational::Join(Join {
                            kind: JoinKind::Inner,
                            ..
                        }))
                ),
                "facts route host {host:?} is not a Selection or INNER Join"
            );
            let parts = routes.iter().flat_map(|route| match route {
                FactsRoute::Key(parts) => parts.iter().collect::<Vec<_>>(),
                FactsRoute::BucketId(part) => vec![part],
                FactsRoute::Same(parts) => parts.iter().collect(),
                FactsRoute::Empty => vec![],
            });
            for part in parts {
                if let FactsRoutePart::Bound(id, _) = part {
                    assert!(
                        matches!(
                            self.get_expression_node(*id),
                            Ok(Expression::Parameter(_) | Expression::CurrentUser(_))
                        ),
                        "facts route of {host:?} binds {id:?}, not a parameter"
                    );
                }
            }
        }
        for id in &self.facts_proof.single_node {
            assert!(
                reachable.contains(id),
                "proven single-node {id:?} is detached"
            );
        }
    }

    /// The buckets `walk` of `rel_id`, narrowed to those its stored facts
    /// routes allow after bind; `walk` itself if it has none. `col = NULL` is
    /// never TRUE, so a NULL pin leaves no contributing row.
    ///
    /// Without `hash_keys`, for a `walk` of no bucket, a key or `bucket_id`
    /// route only checks its bound values: such a route has nothing left to
    /// narrow.
    ///
    /// `hashed` holds the keys the discovery has hashed, by the condition
    /// walk or by the routes of other nodes: the hosts of a key repeat its
    /// route (`Key($1)` on each join of a chain), see [`conjunct_key_buckets`].
    /// `same` holds the `Same` lists it has checked: the hosts of a class
    /// share one.
    ///
    /// # Errors
    /// - A route reads a node that bind did not replace with a constant.
    pub fn facts_route_buckets<'p, R: BucketsResolver>(
        &'p self,
        rel_id: NodeId,
        resolver: &R,
        hash_keys: bool,
        hashed: &RefCell<HashedKeys<'p, R::Buckets>>,
        same: &RefCell<SameChecked>,
        walk: R::Buckets,
    ) -> Result<R::Buckets, SbroadError> {
        let Some(routes) = self.facts_proof.routes.get(&rel_id) else {
            return Ok(walk);
        };
        let tier = self.tier.as_ref();
        let mut result = Some(walk);
        for route in routes {
            let buckets = match route {
                FactsRoute::Same(parts) => {
                    let checked = same.borrow().get(&parts.as_ptr()).copied();
                    let conflict = match checked {
                        Some(conflict) => conflict,
                        None => {
                            let values = parts
                                .iter()
                                .map(|part| self.bound_value(part))
                                .collect::<Result<SmallVec<[&Value; 4]>, _>>()?;
                            let conflict = values_conflict(&values);
                            same.borrow_mut().insert(parts.as_ptr(), conflict);
                            conflict
                        }
                    };
                    if !conflict {
                        continue;
                    }
                    R::Buckets::new_empty()
                }
                FactsRoute::Empty => R::Buckets::new_empty(),
                FactsRoute::BucketId(part) => match self.bound_value(part)? {
                    Value::Null => R::Buckets::new_empty(),
                    _ if !hash_keys => continue,
                    value => resolver.buckets_from_bucket_id(value, tier)?,
                },
                FactsRoute::Key(parts) => {
                    // Every part is read, past a NULL too: each checks its type.
                    let mut values: SmallVec<[&Value; 4]> = SmallVec::new();
                    let mut has_null = false;
                    for part in parts {
                        let value = self.bound_value(part)?;
                        has_null |= matches!(value, Value::Null);
                        values.push(value);
                    }
                    if has_null {
                        R::Buckets::new_empty()
                    } else if !hash_keys {
                        continue;
                    } else {
                        conjunct_key_buckets(resolver, &values, tier, Some(hashed), &mut result)?;
                        continue;
                    }
                }
            };
            conjunct_into(&mut result, buckets);
        }
        Ok(result.expect("conjunctions keep the walk"))
    }
}

/// A NULL, or two values of the same kind that differ. Values of different
/// kinds are left undecided: `Value::eq` is not symmetric across them.
/// Each value is compared with the first of its kind only: equality within
/// one kind is transitive, so a long `a = $1 AND .. AND a = $P` stays O(P).
fn values_conflict(values: &[&Value]) -> bool {
    let mut firsts: SmallVec<[&Value; 2]> = SmallVec::new();
    for &value in values {
        if matches!(value, Value::Null) {
            return true;
        }
        let kind = std::mem::discriminant(value);
        match firsts
            .iter()
            .find(|first| std::mem::discriminant(**first) == kind)
        {
            Some(first) if *first != value => return true,
            Some(_) => {}
            None => firsts.push(value),
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::values_conflict;
    use crate::ir::value::Value;

    fn conflict(values: &[Value]) -> bool {
        values_conflict(&values.iter().collect::<Vec<_>>())
    }

    fn decimal(text: &str) -> Value {
        Value::Decimal(Box::new(text.parse().unwrap()))
    }

    #[test]
    fn values_conflict_by_kind() {
        let one = Value::from(1);
        assert!(!conflict(&[one.clone(), one.clone(), one.clone()]));
        assert!(conflict(&[one.clone(), one.clone(), Value::from(2)]));
        assert!(conflict(&[one.clone(), Value::Null]));
        // Kinds are not compared with each other, each only within itself.
        assert!(!conflict(&[one.clone(), decimal("2"), one.clone()]));
        assert!(conflict(&[one.clone(), decimal("2"), decimal("3")]));
        // `1` and `1.0` are one decimal value.
        assert!(!conflict(&[decimal("1"), decimal("1.0")]));
    }

    /// One pass over a long list of equal bindings, see `values_conflict`.
    #[test]
    fn values_conflict_on_many_equal_bindings() {
        let values = vec![Value::from(1); 100_000];
        assert!(!conflict(&values));
        let mut values = values;
        values.push(Value::from(2));
        assert!(conflict(&values));
    }
}
