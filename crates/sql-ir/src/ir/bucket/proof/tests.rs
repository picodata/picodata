use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hasher};

use super::*;
use crate::ir::bucket::{BucketSet, Buckets, UnboundKeyPart};

fn key(value: i64) -> BucketProof {
    StaticResolver
        .buckets_from_values(&[&Value::from(value)], None)
        .unwrap()
}

fn param(index: u16) -> BucketProof {
    StaticResolver
        .buckets_from_unbound_key(&[UnboundKeyPart::Param(index)], None)
        .unwrap()
}

/// `bucket_of` gives the bucket of a key, or `None` for a `NULL` parameter.
fn check_keys(
    proof: &BucketProof,
    actual: &Buckets,
    bucket_of: &dyn Fn(&RoutingKey) -> Option<u64>,
) {
    match proof {
        BucketProof::Unknown => (),
        BucketProof::MaybeAny { upper } => match actual {
            Buckets::Any => (),
            Buckets::Filtered(BucketSet::Exact(buckets)) => {
                assert!(buckets.len() <= *upper, "{proof:?}: {actual:?}")
            }
            _ => panic!("{proof:?} does not cover {actual:?}"),
        },
        BucketProof::All => assert_eq!(actual, &Buckets::All),
        BucketProof::Any => assert_eq!(actual, &Buckets::Any),
        BucketProof::Filtered {
            upper,
            nonempty,
            keys,
        } => {
            let Buckets::Filtered(BucketSet::Exact(buckets)) = actual else {
                panic!("{proof:?} does not cover {actual:?}");
            };
            assert!(buckets.len() <= *upper, "{proof:?}: {actual:?}");
            assert!(!nonempty || !buckets.is_empty(), "{proof:?}: {actual:?}");
            for k in keys {
                match bucket_of(k) {
                    Some(bucket) => assert!(buckets.iter().all(|b| *b == bucket)),
                    None => assert!(buckets.is_empty(), "{proof:?}: {actual:?}"),
                }
            }
        }
    }
}

fn check(proof: &BucketProof, actual: &Buckets, mapping: &[u64; 4]) {
    check_keys(proof, actual, &|k| {
        let i = (0..4)
            .find(|&i| *k == RoutingKey::new(&[&Value::from(i as i64)]).unwrap())
            .unwrap();
        Some(mapping[i])
    });
}

/// Exercise collisions independently of the production hash: every mapping of
/// four keys into three buckets, including nested AND/OR and Any.
#[test]
fn bounds_cover_all_small_bucket_mappings() {
    for mut code in 0..3_usize.pow(4) {
        let mut mapping = [0; 4];
        for bucket in &mut mapping {
            *bucket = (code % 3 + 1) as u64;
            code /= 3;
        }
        let mut leaves = vec![
            (BucketProof::new_empty(), Buckets::new_empty()),
            (BucketProof::All, Buckets::All),
            (BucketProof::Any, Buckets::Any),
        ];
        for (i, bucket) in mapping.iter().enumerate() {
            leaves.push((
                key(i as i64),
                Buckets::new_filtered([*bucket].into_iter().collect()),
            ));
        }
        // The same unbound parameter may become NULL or a concrete key.
        leaves.push((BucketProof::new_estimated(1), Buckets::new_empty()));
        leaves.push((
            BucketProof::new_estimated(1),
            Buckets::new_filtered([mapping[0]].into_iter().collect()),
        ));
        let mut expressions = leaves.clone();
        for (a, x) in &leaves {
            for (b, y) in &leaves {
                expressions.push((a.conjunct(b), x.conjunct(y)));
                expressions.push((a.disjunct(b), x.disjunct(y)));
            }
        }
        for (a, x) in &expressions {
            check(a, x, &mapping);
            for (b, y) in &expressions {
                check(&a.conjunct(b), &x.conjunct(y), &mapping);
                check(&a.disjunct(b), &x.disjunct(y), &mapping);
            }
        }
    }
}

/// Every binding of two parameters to one of two literals or to `NULL`, with
/// literals that collide or not.
#[test]
fn parameter_bounds_cover_all_small_bindings() {
    let literal = |i: i64| RoutingKey::new(&[&Value::from(i)]).unwrap();
    let parameter = |i: u16| RoutingKey::unbound(&[UnboundKeyPart::Param(i)]).unwrap();
    let choices = [None, Some(0), Some(1)];
    for mapping in [[1_u64, 1], [1, 2]] {
        for first in choices {
            for second in choices {
                let bucket_of = |k: &RoutingKey| -> Option<u64> {
                    if *k == literal(0) {
                        Some(mapping[0])
                    } else if *k == literal(1) {
                        Some(mapping[1])
                    } else if *k == parameter(1) {
                        first.map(|i: usize| mapping[i])
                    } else if *k == parameter(2) {
                        second.map(|i: usize| mapping[i])
                    } else {
                        panic!("unexpected key {k:?}")
                    }
                };
                let bound = |binding: Option<usize>| match binding {
                    Some(i) => Buckets::new_filtered([mapping[i]].into_iter().collect()),
                    None => Buckets::new_empty(),
                };
                let leaves = vec![
                    (BucketProof::new_empty(), Buckets::new_empty()),
                    (BucketProof::All, Buckets::All),
                    (BucketProof::Any, Buckets::Any),
                    (key(0), bound(Some(0))),
                    (key(1), bound(Some(1))),
                    (param(1), bound(first)),
                    (param(2), bound(second)),
                ];
                let mut expressions = leaves.clone();
                for (a, x) in &leaves {
                    for (b, y) in &leaves {
                        expressions.push((a.conjunct(b), x.conjunct(y)));
                        expressions.push((a.disjunct(b), x.disjunct(y)));
                    }
                }
                for (a, x) in &expressions {
                    check_keys(a, x, &bucket_of);
                    for (b, y) in &expressions {
                        check_keys(&a.conjunct(b), &x.conjunct(y), &bucket_of);
                        check_keys(&a.disjunct(b), &x.disjunct(y), &bucket_of);
                    }
                }
            }
        }
    }
}

#[test]
fn parameter_keys_compare_by_index_and_arity() {
    let unbound = |parts: &[UnboundKeyPart]| RoutingKey::unbound(parts).unwrap();
    let one = Value::from(1);
    let two = Value::from(2);
    assert_eq!(
        unbound(&[UnboundKeyPart::Param(1)]),
        unbound(&[UnboundKeyPart::Param(1)])
    );
    assert_ne!(
        unbound(&[UnboundKeyPart::Param(1)]),
        unbound(&[UnboundKeyPart::Param(2)])
    );
    assert_ne!(
        unbound(&[UnboundKeyPart::Param(1)]),
        unbound(&[UnboundKeyPart::Value(&one)])
    );
    assert_ne!(
        unbound(&[UnboundKeyPart::Param(1), UnboundKeyPart::Value(&two)]),
        unbound(&[UnboundKeyPart::Param(1)])
    );
    assert_ne!(
        unbound(&[UnboundKeyPart::Param(1), UnboundKeyPart::Value(&two)]),
        unbound(&[UnboundKeyPart::Value(&two), UnboundKeyPart::Param(1)])
    );
    assert_eq!(
        unbound(&[UnboundKeyPart::Value(&one), UnboundKeyPart::Value(&two)]),
        RoutingKey::new(&[&one, &two]).unwrap()
    );
}

#[test]
fn shared_parameter_keys_read_one_bucket() {
    assert!(param(1).disjunct(&param(1)).is_single_node());
    assert!(!param(1).disjunct(&param(2)).is_single_node());
    assert!(!param(1).disjunct(&key(1)).is_single_node());
    // `a = $1 AND a = $2` reads the bucket of each of them, if any.
    let both = param(1).conjunct(&param(2));
    assert!(both.disjunct(&param(1)).is_single_node());
    assert!(both.disjunct(&param(2)).is_single_node());
    // A parameter may bind to NULL, so its set is never proven nonempty.
    assert!(!param(1).is_nonempty_filtered());
    assert!(param(1).shares_param_key(&param(1)));
    assert!(param(1).shares_param_key(&both));
    assert!(!param(1).shares_param_key(&param(2)));
    assert!(!key(1).shares_param_key(&key(1)));
    assert!(!BucketProof::Any.shares_param_key(&param(1)));
}

#[test]
fn contradictory_keys_keep_common_constraints() {
    let contradictory = key(1).conjunct(&key(2));
    assert!(contradictory.is_single_node());
    assert!(!contradictory.is_nonempty_filtered());
    assert!(contradictory.disjunct(&key(1)).is_single_node());
    assert!(!contradictory.disjunct(&key(3)).is_single_node());
    assert!(key(1).conjunct(&key(1)).is_nonempty_filtered());
}

#[test]
fn any_must_not_keep_a_possibly_empty_anchor() {
    let maybe_empty = key(1).conjunct(&key(2));
    let union = BucketProof::Any.disjunct(&maybe_empty);
    assert_eq!(union, BucketProof::MaybeAny { upper: 1 });
    assert!(union.is_single_node());
    // When 1 and 2 route apart, the executor's union is Any. The subsequent
    // filter selects key 3; it must not inherit keys 1 or 2 from the union.
    let filtered = union.conjunct(&key(3));
    assert!(filtered.disjunct(&key(3)).is_single_node());
    assert!(!filtered.disjunct(&key(1)).is_single_node());
    assert_eq!(
        BucketProof::Any.disjunct(&BucketProof::new_empty()),
        BucketProof::Any
    );
    assert_eq!(BucketProof::Any.disjunct(&key(1)), key(1));
}

#[test]
fn unknown_parameters_have_no_shared_key() {
    let parameter = BucketProof::new_estimated(1);
    assert!(parameter.is_single_node());
    assert!(!parameter.is_nonempty_filtered());
    assert!(!parameter.disjunct(&parameter).is_single_node());
    assert!(parameter.conjunct(&key(1)).is_single_node());
    assert!(!parameter.conjunct(&key(1)).is_nonempty_filtered());
    assert!(!BucketProof::new_at_most_one().is_nonempty_filtered());
    // A NULL binding turns this union into Any, so a later filter may widen
    // it to both buckets. The parameter's upper bound must not survive.
    let global_join = BucketProof::Any.disjunct(&parameter);
    assert!(global_join.is_single_node());
    assert!(!global_join
        .conjunct(&key(1).disjunct(&key(2)))
        .is_single_node());
}

#[test]
fn whole_false_filter_differs_from_a_false_conjunct() {
    use crate::ir::operator::Bool;
    use crate::ir::Plan;

    for value in [Value::Boolean(false), Value::Null] {
        let mut plan = Plan::default();
        let reject = plan.nodes.add_const(value);
        let pass = plan.nodes.add_const(Value::Boolean(true));
        let conjunction = plan.add_bool(reject, Bool::And, pass).unwrap();
        let resolve = |id| {
            plan.get_expression_tree_buckets(id, &[], &[], &StaticResolver)
                .unwrap()
        };
        assert_eq!(resolve(reject), BucketProof::new_empty());
        assert_eq!(resolve(conjunction), BucketProof::Any);
    }
}

#[derive(Default)]
struct CollidingHasher;
impl Hasher for CollidingHasher {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, _: &[u8]) {}
}

#[test]
fn key_identity_survives_hash_collisions() {
    let one = Value::from(1);
    let two = Value::from(2);
    let double = Value::from(1.0);
    let a = RoutingKey::new(&[&one, &two]).unwrap();
    let b = RoutingKey::new(&[&two, &one]).unwrap();
    let c = RoutingKey::new(&[&double, &two]).unwrap();
    let mut keys = HashSet::<_, BuildHasherDefault<CollidingHasher>>::default();
    assert!(keys.insert(a.clone()));
    assert!(keys.insert(b));
    assert!(keys.insert(c));
    assert!(!keys.insert(RoutingKey::new(&[&one, &two]).unwrap()));
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&a));
}
