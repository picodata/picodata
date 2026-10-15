use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hasher};

use super::*;
use crate::ir::bucket::{BucketSet, Buckets};

fn key(value: i64) -> BucketProof {
    StaticResolver
        .buckets_from_values(&[&Value::from(value)], None)
        .unwrap()
}

fn check(proof: &BucketProof, actual: &Buckets, mapping: &[u64; 4]) {
    match proof {
        BucketProof::Unknown => (),
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
                let i = (0..4)
                    .find(|&i| *k == RoutingKey::new(&[&Value::from(i as i64)]).unwrap())
                    .unwrap();
                assert!(buckets.iter().all(|b| *b == mapping[i]));
            }
        }
    }
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
    assert_eq!(union, BucketProof::Unknown);
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
